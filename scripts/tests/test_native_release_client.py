"""Real stdio child fault injection. No network, real credentials or service writes."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
# In standalone development the module is adjacent to this test file.
sys.path[:0] = [str(SCRIPTS), str(pathlib.Path(__file__).resolve().parent)]
import native_release_client as native

FAKE = r'''
import json,pathlib,sys,time
config=json.loads(pathlib.Path(sys.argv[1]).read_text());root=pathlib.Path(config['state_dir']);root.mkdir()
for line in sys.stdin:
    req=json.loads(line)
    with (root.parent/'received.jsonl').open('a') as out:out.write(json.dumps(req)+'\n')
    if 'id' not in req:continue
    if req['method']=='initialize':
        print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{'protocolVersion':'2025-11-25','capabilities':{'tools':{}}}}),flush=True);continue
    mode=req['params'].get('mode','ok')
    rid='req_'+format(req['id'],'032x')
    (root/(rid+'.json')).write_text(json.dumps({'request_id':rid,'operation_id':'original-operation','state':'connector_received'}))
    if mode=='drop':sys.exit(0)
    if mode=='timeout':time.sleep(5);continue
    if mode=='invalid':print('not-json-secret-fixture-token',flush=True);continue
    if mode=='huge':print('X'*2200000,flush=True);continue
    if mode=='partial':sys.stdout.write('{');sys.stdout.flush();sys.exit(0)
    if mode=='notification':print(json.dumps({'jsonrpc':'2.0','method':'notifications/progress','params':{}}),flush=True)
    if mode=='flood':
        for _ in range(80):print(json.dumps({'jsonrpc':'2.0','method':'notifications/progress'}),flush=True)
        time.sleep(5);continue
    ident=req['id']+100 if mode=='mismatch' else req['id']
    if mode=='protocol':
        print(json.dumps({'jsonrpc':'2.0','id':ident,'error':{'code':-32602,'message':'secret-fixture-token'}}),flush=True);continue
    value={'request_id':rid,'operation_id':'original-operation','state':'completed','answer':42}
    if mode=='rejected':value={'error':'upstream_call_unconfirmed','request_id':rid,'execution_state':'unknown'}
    print(json.dumps({'jsonrpc':'2.0','id':ident,'result':{'structuredContent':value,'isError':mode=='rejected'}}),flush=True)
    if mode=='exit_after_response':sys.exit(7)
'''


class NativeStdioTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name).resolve()
        self.script = self.root/'fake.py'
        self.script.write_text(FAKE)
        self.sessions = []
        real_popen = subprocess.Popen

        def spawn(args, **kwargs):
            # Execute a real peer process, but do not run any installed Agent.
            self.assertEqual(args[1:3], ['adapter', '--config'])
            self.assertNotIn('secret-fixture-token', ' '.join(args))
            return real_popen([sys.executable, str(self.script), args[-1]], **kwargs)

        self.patch = mock.patch.object(native.subprocess, 'Popen', side_effect=spawn)
        self.spawn = self.patch.start()

    def tearDown(self):
        for session in self.sessions:
            session.close()
        self.patch.stop()
        self.temp.cleanup()

    def session(self, timeout=10):
        s = native.NativeSession('https://fixture.example', 'secret-fixture-token',
                                 pathlib.Path(sys.executable), self.root/'evidence', timeout=timeout)
        self.sessions.append(s)
        return s

    def received(self, s):
        return [json.loads(x) for x in (s.directory/'received.jsonl').read_text().splitlines()]

    def test_one_process_for_multiple_calls_and_private_credentials(self):
        s = self.session()
        for i in range(3):
            result = s.rpc('tools/call', {'mode': 'ok'})
            self.assertEqual(result['structuredContent']['answer'], 42)
        self.assertEqual(self.spawn.call_count, 1)
        self.assertEqual(sum(r.get('method')=='initialize' for r in self.received(s)), 1)
        self.assertTrue(any(r.get('method')=='notifications/initialized' for r in self.received(s)))
        if os.name != 'nt':
            self.assertEqual(s._token.stat().st_mode & 0o777, 0o600)
            self.assertEqual(s.directory.stat().st_mode & 0o777, 0o700)
        self.assertTrue(s.close())
        self.assertFalse(s._token.exists())
        self.assertFalse(s._config.exists())
        self.assertEqual(len(list(s.receipts.glob('req_*.json'))), 3)
        self.assertTrue(json.loads((s.directory/'session.json').read_text())['process_reaped'])
        self.assertNotIn('secret-fixture-token', (s.directory/'session.json').read_text())

    def test_broken_output_poisoned_once_and_preserves_recovery(self):
        for mode in ('drop', 'timeout', 'invalid', 'huge', 'partial', 'mismatch', 'protocol', 'flood'):
            with self.subTest(mode=mode):
                s = self.session()
                s.start()
                if mode == 'timeout':
                    s.timeout = .6
                with self.assertRaises(native.NativeTransportError) as caught:
                    s.rpc('tools/call', {'mode': mode})
                self.assertTrue(s.poisoned)
                self.assertTrue(s.closed)
                self.assertIsNotNone(s.process.poll())
                self.assertFalse(s._token.exists())
                self.assertNotIn('secret-fixture-token', str(caught.exception))
                self.assertEqual(len(caught.exception.requests), 1)
                self.assertEqual(caught.exception.requests[0]['operation_id'], 'original-operation')
                before = self.spawn.call_count
                with self.assertRaisesRegex(RuntimeError, 'no restart or replay'):
                    s.rpc('tools/call', {'mode': mode})
                self.assertEqual(self.spawn.call_count, before)
                self.assertEqual(sum(r.get('method')=='tools/call' for r in self.received(s)), 1)

    def test_prior_acknowledged_receipt_not_confused_with_current_failure(self):
        s = self.session()
        previous = s.rpc('tools/call', {'mode':'ok'})['structuredContent']['request_id']
        with self.assertRaises(native.NativeTransportError) as caught:
            s.rpc('tools/call', {'mode':'drop'})
        self.assertEqual(len(caught.exception.requests), 1)
        self.assertNotEqual(caught.exception.requests[0]['request_id'], previous)

    def test_structured_upstream_uncertainty_is_returned_not_replayed(self):
        s = self.session()
        value = s.rpc('tools/call', {'mode':'rejected'})
        self.assertTrue(value['isError'])
        self.assertEqual(value['structuredContent']['execution_state'], 'unknown')
        self.assertEqual(sum(r.get('method')=='tools/call' for r in self.received(s)), 1)

    def test_notification_does_not_consume_response_identity(self):
        s = self.session()
        self.assertEqual(s.rpc('tools/call', {'mode':'notification'})['structuredContent']['answer'],42)

    def test_request_budget_failure_does_not_send_tool(self):
        s = self.session()
        s.start()
        with mock.patch.object(native, 'MAX_FRAME', 200):
            with self.assertRaises(native.NativeTransportError):
                s.rpc('tools/call', {'huge':'x'*400})
        self.assertFalse(any(r.get('method')=='tools/call' for r in self.received(s)))

    def test_binary_spawn_failure_removes_credentials(self):
        s = self.session()
        self.spawn.side_effect = OSError('secret-fixture-token')
        with self.assertRaises(native.NativeTransportError) as caught:
            s.start()
        self.assertNotIn('secret-fixture-token', str(caught.exception))
        self.assertFalse(s._token.exists())
        self.assertTrue(s.closed)

    def test_no_symlink_state_or_insecure_origin_or_missing_executable(self):
        (self.root/'link').symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'state_symlink'):
            native.NativeSession('https://fixture.example','token',pathlib.Path(sys.executable),self.root/'link'/'state')
        for url in ('http://fixture.example','https://user@fixture.example','https://fixture.example/path','https://fixture.example?x=1'):
            with self.assertRaises(ValueError):
                native.NativeSession(url,'token',pathlib.Path(sys.executable),self.root/'evidence')
        with self.assertRaisesRegex(ValueError, 'binary_unavailable'):
            native.NativeSession('https://fixture.example','token',self.root/'missing',self.root/'evidence')

    def test_nonzero_adapter_exit_is_not_a_successful_close(self):
        s = self.session()
        self.assertEqual(s.rpc('tools/call', {'mode':'exit_after_response'})['structuredContent']['answer'],42)
        self.assertFalse(s.close())
        self.assertFalse(s.close())
        self.assertEqual(s.status['adapter_exit_code'],7)
        self.assertTrue(s.status['credentials_removed'])

    def test_preflight_write_failure_removes_prepared_credentials(self):
        original = native.NativeSession._write_private
        def write(path, payload):
            if path.name == 'adapter.json':
                raise OSError('synthetic disk full')
            return original(path, payload)
        with mock.patch.object(native.NativeSession, '_write_private', side_effect=write):
            with self.assertRaisesRegex(RuntimeError, 'credentials_prepare_failed'):
                self.session()
        self.assertFalse(list((self.root/'evidence').glob('session-*/access.txt')))
        self.spawn.assert_not_called()

    def test_initial_receipt_failure_removes_credentials_before_start(self):
        with mock.patch.object(native.NativeSession, '_persist_status', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(RuntimeError, 'state_unavailable_before_start'):
                self.session()
        self.assertFalse(list((self.root/'evidence').glob('session-*/access.txt')))
        self.spawn.assert_not_called()

    def test_post_dispatch_metadata_failure_keeps_original_uncertain_handle(self):
        s = self.session()
        s.start()
        with mock.patch.object(s, '_persist_status', side_effect=OSError('synthetic disk full')):
            with self.assertRaises(native.NativeTransportError) as caught:
                s.rpc('tools/call', {'mode':'drop'})
        self.assertEqual(caught.exception.requests[0]['operation_id'], 'original-operation')
        self.assertFalse(s._token.exists())
        self.assertFalse(s._config.exists())
        self.assertIsNotNone(s.process.poll())
        self.assertFalse(s.status['metadata_persisted'])
        self.assertFalse(s.close())
        self.assertEqual(sum(r.get('method')=='tools/call' for r in self.received(s)), 1)

    def test_bootstrap_failure_is_not_an_uncertain_remote_tool(self):
        s = self.session()
        s.receipts.mkdir()
        rid = 'req_' + 'a' * 32
        record = {'kind': 'adapter_bootstrap', 'request_id': rid, 'method': 'initialize',
                  'state': 'bootstrap_failed', 'error_code': 'upstream_collection_incomplete',
                  'attempts': [{'number': 1}], 'tool_calls_submitted': 0}
        (s.receipts/(rid+'.json')).write_text(json.dumps(record))
        with self.assertRaises(native.NativeTransportError) as caught:
            s._fail('native_adapter_stdout_closed')
        self.assertEqual(caught.exception.requests, [])
        self.assertEqual(s.status['execution_state'], 'not_started')
        self.assertEqual(s.status['bootstrap_receipts'][0]['request_id'], rid)
        self.spawn.assert_not_called()

    def test_bootstrap_receipt_is_separate_from_later_uncertain_tool(self):
        s = self.session()
        s.start()
        rid = 'req_' + 'f' * 32
        (s.receipts/(rid+'.json')).write_text(json.dumps({'kind':'adapter_bootstrap',
            'request_id':rid,'method':'tools/list','state':'bootstrap_step_completed'}))
        with self.assertRaises(native.NativeTransportError) as caught:
            s.rpc('tools/call', {'mode':'drop'})
        self.assertEqual(len(caught.exception.requests), 1)
        self.assertNotEqual(caught.exception.requests[0]['request_id'], rid)
        self.assertEqual(s.status['execution_state'], 'unknown')
        self.assertEqual(s.status['tool_calls_submitted'], 1)
        self.assertEqual(len(s.status['bootstrap_receipts']), 1)

    def test_two_sessions_do_not_share_credentials_or_processes(self):
        a, b = self.session(), self.session()
        a.start(); b.start()
        self.assertNotEqual(a.directory,b.directory)
        self.assertNotEqual(a.process.pid,b.process.pid)
        a.close()
        self.assertIsNone(b.process.poll())
        self.assertEqual(b.rpc('tools/call',{})['structuredContent']['answer'],42)


if __name__ == '__main__':
    unittest.main()

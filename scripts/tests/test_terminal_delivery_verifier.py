"""Verifier classification and no-replay tests; no network or Agent processes."""
import contextlib
import importlib.util
import io
import itertools
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('terminal_delivery_verifier', SCRIPTS/'check-terminal-delivery.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class FakeClient:
    def __init__(self, mode, root):
        self.mode, self.root, self.calls = mode, root, []
        self.refresh = 'synthetic'
    def login(self):
        pass
    def raw(self, name, args):
        self.calls.append(name)
        if name == 'devices_list':
            return {'devices':[{'device_id':'device','name':'fixture','online':True,
                'capabilities':{'version':'0.10.13','platform':'linux'}}]}
        if self.mode == 'lost':
            raise verifier.NativeTransportError('unconfirmed', self.root,
                [{'request_id':'original-request','operation_id':'original-operation','state':'unknown'}])
        case = args['idempotency_key'].split('-')[-1]
        text = 'RH-DELIVERY-fixture-' + case + '\n'
        if case == 'delayed':
            text += 'RH-DELIVERY-fixture-delayed-end\n'
        code = 7 if case == 'failure' else 0
        if self.mode == 'wrong_exit':
            code = 9
        return {'operation_id':case,'state':'exited','output':text,
            'receipt':{'evidence_complete':True,'durable':True},
            'terminal':{'state':'exited','exit_code':code,'output_complete':True}}
    def close(self):
        if self.mode != 'bad_cleanup':
            self.refresh = None
            return True
        return False
    def transport_info(self):
        return {'mode':'synthetic','auto_replay':False}


class TerminalDeliveryVerifierTests(unittest.TestCase):
    def run_fixture(self, mode='ok', slow=False):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            report = root/'report.json'
            client = FakeClient(mode, root)
            args = ['check','--origin','https://fixture.example','--password-file',str(root/'unused'),
                '--version','0.10.13','--workspace','device:workspace','--run-id','fixture','--report',str(report)]
            with mock.patch.object(sys, 'argv', args), mock.patch.object(verifier, 'Client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
                if slow:
                    ticks = itertools.count()
                    with mock.patch.object(verifier.time, 'monotonic', side_effect=lambda:next(ticks)*3):
                        code = verifier.main()
                else:
                    code = verifier.main()
            return code, json.loads(report.read_text()), client
    def test_exact_function_and_fast_samples_pass(self):
        code, result, _ = self.run_fixture()
        self.assertEqual(code, 0)
        self.assertEqual(result['state'], 'passed')
        self.assertTrue(result['oauth_grant_revoked'])
        self.assertEqual(len(result['cases']), 3)
    def test_slow_correct_results_are_not_reported_as_complete_success(self):
        code, result, _ = self.run_fixture(slow=True)
        self.assertEqual(code, 2)
        self.assertEqual(result['state'], 'needs_improvement')
        self.assertTrue(result['functional_passed'])
        self.assertFalse(result['absolute_latency_target_met'])
    def test_wrong_exit_is_a_functional_failure(self):
        code, result, _ = self.run_fixture('wrong_exit')
        self.assertEqual(code, 1)
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['active_operation'], 'short')
    def test_cleanup_failure_is_not_success(self):
        code, result, _ = self.run_fixture('bad_cleanup')
        self.assertEqual(code, 1)
        self.assertEqual(result['state'], 'cleanup_unconfirmed')
        self.assertFalse(result['oauth_grant_revoked'])
    def test_uncertain_command_is_never_replayed_and_keeps_original_identity(self):
        code, result, client = self.run_fixture('lost')
        self.assertEqual(code, 1)
        self.assertEqual(client.calls.count('terminal_exec'), 1)
        self.assertEqual(result['original_request_recovery'][0]['operation_id'], 'original-operation')
        self.assertEqual(result['state'], 'failed')


if __name__ == '__main__':
    unittest.main()

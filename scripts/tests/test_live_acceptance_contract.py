"""Live acceptance client contract: no network, credentials or deployed changes."""
import copy
import base64
import importlib.util
import pathlib
import shlex
import sys
import unittest
import urllib.error
from unittest import mock

PATH = pathlib.Path(__file__).resolve().parents[1] / 'check-code-gateway.py'
sys.path.insert(0, str(PATH.parent))
from release_client import Client
SPEC = importlib.util.spec_from_file_location('live_acceptance_contract', PATH)
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)


class LiveAcceptanceContractTests(unittest.TestCase):
    def test_transient_transfer_resumes_original_and_observes_exact_rejection(self):
        paused={'operation_id':'original','state':'paused','next_action':'transfer_resume',
                'diagnostic':{'code':'gateway_connection'}}
        failed={'operation_id':'original','state':'failed','error':'transfer_failed',
                'message':'sha256_mismatch','destination_changed':False}
        invoke=mock.Mock(side_effect=[{'operation_id':'original','next_action':'operation_get'},failed])
        with mock.patch.object(acceptance.time,'sleep'):
            result=acceptance.settle_operation('file_upload',paused,invoke)
        acceptance.assert_transfer_rejected(result,'sha256_mismatch')
        self.assertEqual([c.args[0] for c in invoke.call_args_list],['transfer_resume','operation_get'])
        self.assertTrue(all(c.args[1]['operation_id']=='original' for c in invoke.call_args_list))

    def test_recovery_is_bounded_and_does_not_replay_arbitrary_mutations(self):
        paused={'operation_id':'original','state':'paused','next_action':'transfer_resume',
                'diagnostic':{'code':'gateway_connection'}}
        invoke=mock.Mock(return_value=paused)
        with self.assertRaisesRegex(RuntimeError,'inspect original: original'):
            acceptance.settle_operation('file_upload',paused,invoke)
        self.assertEqual(invoke.call_count,2)
        for name in ('terminal_exec','code_apply_edits'):
            invoke.reset_mock()
            with self.assertRaises(RuntimeError):
                acceptance.settle_operation(name,paused,invoke)
            invoke.assert_not_called()
        invoke=mock.Mock(side_effect=TimeoutError())
        with self.assertRaisesRegex(RuntimeError,'observe original: original'):
            acceptance.settle_operation('file_upload',paused,invoke)
        self.assertEqual(invoke.call_count,1)
        invoke=mock.Mock(return_value={'operation_id':'replacement','state':'completed'})
        with self.assertRaisesRegex(RuntimeError,'changed operation identity'):
            acceptance.settle_operation('file_upload',paused,invoke)

    def test_only_expected_failure_with_unchanged_destination_counts_as_rejection(self):
        failed={'state':'failed','error':'transfer_failed','message':'sha256_mismatch',
                'destination_changed':False}
        acceptance.assert_transfer_rejected(failed,'sha256_mismatch')
        for field,value in [('state','paused'),('message','network_failure'),
                            ('destination_changed',True),('error',None)]:
            with self.assertRaises(AssertionError):
                acceptance.assert_transfer_rejected(dict(failed,**{field:value}),'sha256_mismatch')

    def test_receipt_v2_does_not_fail_the_legacy_health_assertion(self):
        for value in (1, 2):
            acceptance.validate_receipt_protocol(value)
        for value in (None, True, 0, 3, '2'):
            with self.assertRaises(ValueError):
                acceptance.validate_receipt_protocol(value)

    def test_platform_commands_preserve_quotes_newlines_and_unicode(self):
        code = 'value = "a\'b 中文"\nassert value.endswith("中文")\n'
        for platform in ('linux', 'macos'):
            self.assertEqual(shlex.split(acceptance.python_probe_command(code, platform)),
                             ['python3', '-c', code])
        command = acceptance.python_probe_command(code, 'windows')
        encoded = command.split("b64decode('", 1)[1].split("')", 1)[0]
        self.assertEqual(base64.b64decode(encoded).decode(), code)
        # The only double quotes delimit the whole PowerShell argument.
        self.assertEqual(command.count('"'), 2)

    def test_all_tool_requests_explicitly_use_full_machine_view(self):
        for name in ('code_search', 'terminal_read', 'operation_get', 'code_apply_edits', 'file_upload'):
            arguments = {'idempotency_key': 'original', 'response_mode': 'compact'}
            before = dict(arguments)
            request = acceptance.machine_tool_params(name, arguments)
            self.assertEqual(request['arguments']['response_mode'], 'full')
            self.assertEqual(arguments, before)
            self.assertEqual(request['name'], name)
            self.assertEqual(request['arguments']['idempotency_key'], 'original')

    def test_observer_identity_is_not_operation_identity(self):
        original = {'operation_id': 'same', 'request_id': 'first', 'sha256': 'a'*64,
                    'receipt': {'operation_id': 'same', 'request_id': 'first', 'observed_at': 1,
                                'execution_state': 'completed', 'evidence_complete': True,
                                'retry_policy': 'do_not_replay_completed_operation'}}
        replay = copy.deepcopy(original)
        replay['request_id'] = 'second'
        replay['receipt'].update(request_id='second', observed_at=2)
        replay['operation_lifecycle'] = {'queue_ms': 100}
        self.assertEqual(acceptance.stable_operation_receipt(original),
                         acceptance.stable_operation_receipt(replay))
        self.assertEqual(original['receipt']['request_id'], 'first')
        for key, changed in (('execution_state', 'unknown'), ('evidence_complete', False),
                             ('retry_policy', 'inspect_original')):
            altered = copy.deepcopy(replay)
            altered['receipt'][key] = changed
            self.assertNotEqual(acceptance.stable_operation_receipt(original),
                                acceptance.stable_operation_receipt(altered))
        replay['operation_id'] = 'different'
        self.assertNotEqual(acceptance.stable_operation_receipt(original),
                            acceptance.stable_operation_receipt(replay))

    def test_v2_query_retention_does_not_change_durable_operation_evidence(self):
        original={'operation_id':'same','receipt':{'protocol':1,'durable':True,
                  'execution_state':'completed','evidence_complete':True}}
        query=copy.deepcopy(original)
        query['receipt'].update(protocol=2,durable=False,evidence_durable=True,
            request_record_persisted=False,durability_scope='observed_facts_only_not_this_query',
            request_retention='not_retained_use_original_operation')
        query['request_retention']='not_retained_use_original_operation'
        self.assertEqual(acceptance.stable_operation_receipt(original),
                         acceptance.stable_operation_receipt(query))
        for key,value in [('evidence_durable',False),('durability_scope','unknown'),
                          ('execution_state','outcome_unknown'),('evidence_complete',False)]:
            invalid=copy.deepcopy(query);invalid['receipt'][key]=value
            self.assertNotEqual(acceptance.stable_operation_receipt(original),
                                acceptance.stable_operation_receipt(invalid))

    def test_lost_mutation_response_is_never_automatically_replayed(self):
        client = Client('https://example.com', access='temporary-test-token', transport='legacy')
        for tool in ('terminal_exec', 'code_apply_edits', 'file_upload', 'workspace_open'):
            with mock.patch.object(client, 'parsed', side_effect=urllib.error.URLError('lost')) as call:
                with self.assertRaises(urllib.error.URLError):
                    client.rpc('tools/call', {'name': tool})
                self.assertEqual(call.call_count, 1)
        for method, params in (('tools/list', {}), ('initialize', {}),
                               ('tools/call', {'name': 'operation_get'})):
            with mock.patch.object(client, 'parsed', side_effect=urllib.error.URLError('lost')) as call, mock.patch('release_client.time.sleep'):
                with self.assertRaises(urllib.error.URLError):
                    client.rpc(method, params)
                self.assertEqual(call.call_count, 5)

    def test_current_release_requires_task_recovery_capability(self):
        self.assertIn('task_context', acceptance.required_tool_names('0.10.4'))
        self.assertNotIn('task_context', acceptance.required_tool_names('0.10.3'))


if __name__ == '__main__':
    unittest.main()

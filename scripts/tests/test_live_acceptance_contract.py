"""Live acceptance client contract: no network, credentials or deployed changes."""
import copy
import importlib.util
import pathlib
import unittest

PATH = pathlib.Path(__file__).resolve().parents[1] / 'check-code-gateway.py'
SPEC = importlib.util.spec_from_file_location('live_acceptance_contract', PATH)
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)


class LiveAcceptanceContractTests(unittest.TestCase):
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

    def test_lost_mutation_response_is_never_automatically_replayed(self):
        for tool in ('terminal_exec', 'code_apply_edits', 'file_upload', 'workspace_open'):
            self.assertEqual(acceptance.rpc_attempt_limit('tools/call', {'name': tool}), 1)
        for method, params in (('tools/list', {}), ('initialize', {}),
                               ('tools/call', {'name': 'operation_get'})):
            self.assertEqual(acceptance.rpc_attempt_limit(method, params), 5)

    def test_current_release_requires_task_recovery_capability(self):
        self.assertIn('task_context', acceptance.required_tool_names('0.10.4'))
        self.assertNotIn('task_context', acceptance.required_tool_names('0.10.3'))


if __name__ == '__main__':
    unittest.main()

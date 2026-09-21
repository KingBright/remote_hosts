"""Release client regression checks; no network, credentials or service changes."""
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client, evidence_is_durable


class ReleaseClientContractTests(unittest.TestCase):
    def test_query_trace_and_original_evidence_have_separate_durability(self):
        observed = {'protocol': 2, 'durable': False, 'evidence_durable': True,
                    'request_record_persisted': False,
                    'durability_scope': 'observed_facts_only_not_this_query'}
        self.assertTrue(evidence_is_durable(observed))
        for key in observed:
            incomplete = dict(observed)
            incomplete.pop(key)
            self.assertFalse(evidence_is_durable(incomplete), key)
        for key, value in [('durable', True), ('evidence_durable', False),
                           ('durability_scope', 'observation_error_record_only')]:
            self.assertFalse(evidence_is_durable(dict(observed, **{key: value})))
        self.assertFalse(evidence_is_durable({'protocol': 2, 'durable': True}))
        self.assertTrue(evidence_is_durable({'protocol': 1, 'durable': True}))
        self.assertFalse(evidence_is_durable({'protocol': 1, 'durable': False}))

    def test_unknown_or_malformed_protocol_is_not_durable_evidence(self):
        observed = {'protocol': 2, 'durable': False, 'evidence_durable': True,
                    'request_record_persisted': False,
                    'durability_scope': 'observed_facts_only_not_this_query'}
        for protocol in (0, 3, 99, None, True, False, '2', 2.0):
            with self.subTest(protocol=protocol):
                self.assertFalse(evidence_is_durable(dict(observed, protocol=protocol)))
        for value in (None, [], 'receipt'):
            self.assertFalse(evidence_is_durable(value))

    def test_persisted_request_must_not_contradict_durability(self):
        saved = {'protocol': 2, 'evidence_durable': True,
                 'request_record_persisted': True, 'durable': True}
        self.assertTrue(evidence_is_durable(saved))
        for value in (False, None, 'true', 1):
            with self.subTest(durable=value):
                self.assertFalse(evidence_is_durable(dict(saved, durable=value)))
        self.assertFalse(evidence_is_durable({'protocol': 1, 'durable': 'true'}))
        self.assertTrue(evidence_is_durable({}))

    def test_machine_view_is_full_and_does_not_mutate_caller_arguments(self):
        client = Client('https://fixture.example', access='synthetic', transport='legacy')
        arguments = {'operation_id': 'fixture'}
        with mock.patch.object(client, 'rpc', return_value={'structuredContent': {'answer': 42}}) as rpc:
            self.assertEqual(client.raw('operation_get', arguments), {'answer': 42})
        self.assertNotIn('response_mode', arguments)
        self.assertEqual(rpc.call_args.args[1]['arguments']['response_mode'], 'full')

    def test_transport_failure_does_not_replay_execution(self):
        client = Client('https://fixture.example', access='synthetic', transport='legacy')
        with mock.patch.object(client, 'parsed', side_effect=TimeoutError) as request, mock.patch('release_client.time.sleep'):
            with self.assertRaises(TimeoutError):
                client.rpc('tools/call', {'name': 'terminal_exec', 'arguments': {'idempotency_key': 'original'}})
        self.assertEqual(request.call_count, 1)

    def test_readonly_observation_retry_is_bounded(self):
        client = Client('https://fixture.example', access='synthetic', transport='legacy')
        with mock.patch.object(client, 'parsed', side_effect=[TimeoutError, {'result': {'pending': False}}]) as request, mock.patch('release_client.time.sleep'):
            result = client.rpc('tools/call', {'name': 'operation_get', 'arguments': {'operation_id': 'original'}})
        self.assertEqual(request.call_count, 2)
        self.assertFalse(result['pending'])

    def test_missing_empty_output_is_tolerated_but_truncation_is_not(self):
        client = Client('https://fixture.example', access='synthetic', transport='legacy')
        first = {'terminal_id': 'original', 'cursor': 0, 'terminal': {'exit_code': None, 'output_complete': False}}
        last = {'receipt': {'evidence_complete': True}, 'cursor': 0, 'terminal': {'exit_code': 0, 'output_complete': True, 'output_truncated': False}, 'has_more': False}
        with mock.patch.object(client, 'tool', side_effect=[first, last]), mock.patch('release_client.time.sleep'):
            self.assertEqual(client.terminal('w', 'true', 'key'), '')
        last['terminal']['output_truncated'] = True
        with mock.patch.object(client, 'tool', return_value=dict(last, terminal_id='original')):
            with self.assertRaisesRegex(RuntimeError, 'terminal_evidence_incomplete'):
                client.terminal('w', 'true', 'key')

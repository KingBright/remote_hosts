"""RH-009: actual Client/coordinator boundary, synthetic RPCs only."""
import importlib.util
import json
import pathlib
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from release_client import Client, OperationIncomplete
spec = importlib.util.spec_from_file_location('rh009_fleet', SCRIPTS/'fleet-upgrade.py')
fleet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fleet)

SHA = 'a' * 64


def paused(ident='original', state='paused', revision=0, **fields):
    return dict(operation_id=ident, state=state, pending=False, resumable=True,
                transfer_revision=revision, next_action='transfer_resume', **fields)


def complete(ident='original', **fields):
    return {**dict(operation_id=ident, state='completed', sha256=SHA,
                   artifact_id='artifact', download_url='https://fixture.example/files/artifact'), **fields}


class TransferRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.client = Client('https://fixture.example', access='fixture', transport='legacy')
        self.bundle = pathlib.Path('bundle.tgz')
        self.exported = complete()

    def export(self, value, **kwargs):
        return fleet.verified_export(self.client, value, self.bundle, SHA, '0.10.24', **kwargs)

    def imported(self, value, **kwargs):
        return fleet.verified_import(self.client, value, self.exported, self.bundle,
                                     SHA, '0.10.24', 'device', **kwargs)

    def test_explicit_capture_returns_pause_without_resuming_or_claiming_success(self):
        for state in ('paused', 'awaiting_source'):
            with self.subTest(state=state), mock.patch.object(self.client, 'raw', return_value=paused(state=state)) as raw:
                value = self.client.tool('file_upload', {'idempotency_key': 'initial'}, allow_incomplete=True)
                self.assertEqual(value['state'], state)
                self.assertEqual(raw.call_count, 1)
                self.assertEqual(raw.call_args.args[0], 'file_upload')

    def test_default_client_still_raises_for_paused_transfer(self):
        with mock.patch.object(self.client, 'raw', return_value=paused()) as raw:
            with self.assertRaises(OperationIncomplete):
                self.client.tool('file_upload', {'idempotency_key': 'initial'})
        self.assertEqual(raw.call_count, 1)

    def test_capture_flag_cannot_apply_to_shell_mutation(self):
        with mock.patch.object(self.client, 'raw') as raw:
            with self.assertRaises(ValueError):
                self.client.tool('terminal_exec', {}, allow_incomplete=True)
        raw.assert_not_called()

    def test_pending_then_pause_keeps_original_handle(self):
        with mock.patch.object(self.client, 'raw', side_effect=[
            {'pending': True, 'operation_id': 'original'}, paused()]) as raw:
            value = self.client.tool('file_upload', {}, allow_incomplete=True)
        self.assertEqual(value['operation_id'], 'original')
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['file_upload', 'operation_get'])
        self.assertEqual(raw.call_args.args[1]['operation_id'], 'original')

    def test_pending_observation_identity_change_stops(self):
        with mock.patch.object(self.client, 'raw', side_effect=[
            {'pending': True, 'operation_id': 'original'}, paused('different')]) as raw:
            with self.assertRaisesRegex(RuntimeError, 'identity'):
                self.client.tool('file_upload', {}, allow_incomplete=True)
        self.assertEqual(raw.call_count, 2)

    def test_real_client_export_resumes_once_then_observes_original(self):
        with mock.patch.object(self.client, 'raw', side_effect=[
            {'operation_id': 'original', 'state': 'resume_queued', 'next_action': 'operation_get'}, complete()]) as raw:
            self.assertEqual(self.export(paused()), complete())
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['transfer_resume', 'operation_get'])
        self.assertTrue(all(c.args[1]['operation_id'] == 'original' for c in raw.call_args_list))

    def test_awaiting_source_import_refreshes_only_original_file_identity(self):
        with mock.patch.object(self.client, 'raw', return_value=complete()) as raw:
            self.assertEqual(self.imported(paused(state='awaiting_source')), complete())
        self.assertEqual(raw.call_args.args[0], 'transfer_resume')
        self.assertEqual(raw.call_args.args[1]['file']['file_id'], self.exported['artifact_id'])
        self.assertEqual(raw.call_args.args[1]['operation_id'], 'original')

    def test_export_awaiting_source_needs_explicit_source_authorization(self):
        with mock.patch.object(self.client, 'raw') as raw:
            with self.assertRaisesRegex(RuntimeError, 'source authorization'):
                self.export(paused(state='awaiting_source'))
        raw.assert_not_called()

    def test_complete_transfer_does_not_resume(self):
        with mock.patch.object(self.client, 'raw') as raw:
            self.assertEqual(self.export(complete()), complete())
            self.assertEqual(self.imported(complete()), complete())
        raw.assert_not_called()

    def test_source_diagnosis_never_resumes_or_refreshes_authorization(self):
        value = dict(paused(), next_action='diagnose_original_source',
                     diagnostic={'code':'source_address_policy_rejected'},
                     source_authorization={'state':'available'})
        with mock.patch.object(self.client, 'raw') as raw:
            with self.assertRaises(RuntimeError): self.export(value)
            with self.assertRaises(RuntimeError): self.imported(value)
        raw.assert_not_called()

    def test_checksum_or_export_artifact_identity_mismatch_never_passes(self):
        with mock.patch.object(self.client, 'raw') as raw:
            for result in (complete(sha256='b'*64), complete(artifact_id='')):
                with self.subTest(result=result), self.assertRaises(RuntimeError):
                    self.export(result)
            with self.assertRaises(RuntimeError): self.imported(complete(sha256='b'*64))
        raw.assert_not_called()

    def test_failed_unknown_cancelled_and_nonresumable_states_stop(self):
        with mock.patch.object(self.client, 'raw') as raw:
            for state in ('failed', 'outcome_unknown', 'cancelled', 'unknown'):
                with self.subTest(state=state), self.assertRaises(RuntimeError):
                    self.export({'state': state, 'operation_id': 'original'})
            with self.assertRaises(RuntimeError): self.export(dict(paused(), resumable=False))
        raw.assert_not_called()

    def test_unknown_receipt_cannot_be_resumed_even_if_pause_is_reported(self):
        with mock.patch.object(self.client, 'raw') as raw:
            with self.assertRaisesRegex(RuntimeError, 'uncertain'):
                self.export(dict(paused(), receipt={'execution_state': 'outcome_unknown'}))
        raw.assert_not_called()

    def test_legacy_already_finished_observes_without_resuming(self):
        with mock.patch.object(self.client, 'raw', return_value=complete()) as raw:
            self.assertEqual(self.export({'state': 'already_finished', 'operation_id': 'original',
                                         'next_action': 'operation_get'}), complete())
        self.assertEqual(raw.call_args.args[0], 'operation_get')

    def test_resume_ack_loss_preserves_exception_and_never_retries_mutation(self):
        for error in (TimeoutError('ack unavailable'), ConnectionError('stream dropped')):
            with self.subTest(error=type(error).__name__), mock.patch.object(self.client, 'raw', side_effect=error) as raw:
                with self.assertRaises(type(error)) as caught: self.export(paused())
                self.assertTrue(any('original' in n for n in caught.exception.__notes__))
                self.assertEqual([c.args[0] for c in raw.call_args_list], ['transfer_resume'])

    def test_observation_disconnect_does_not_create_resume_or_transfer(self):
        with mock.patch.object(self.client, 'raw', side_effect=ConnectionError('dropped')) as raw:
            with self.assertRaises(ConnectionError):
                self.export({'operation_id': 'original', 'state': 'resume_queued', 'next_action': 'operation_get'})
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['operation_get'])

    def test_failed_resume_does_not_hide_error_or_retry(self):
        failed={'state':'failed','operation_id':'original','error':'transfer_failed',
                'error_code':'SOURCE_CHANGED','resumable':False}
        with mock.patch.object(self.client, 'raw', return_value=failed) as raw:
            with self.assertRaisesRegex(RuntimeError, 'SOURCE_CHANGED.*original'):
                self.imported(paused())
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['transfer_resume'])

    def test_expired_authorization_exception_does_not_reexport_or_reupload(self):
        expired=RuntimeError('source_authorization_required')
        with mock.patch.object(self.client, 'raw', side_effect=expired) as raw:
            with self.assertRaisesRegex(RuntimeError, 'source_authorization_required') as caught:
                self.imported(paused(state='awaiting_source', source_authorization={'state':'expired'}))
        self.assertIs(caught.exception, expired)
        self.assertTrue(any('original' in n for n in caught.exception.__notes__))
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['transfer_resume'])

    def test_uncertain_resume_result_stops_before_second_resume(self):
        with mock.patch.object(self.client, 'raw', return_value={'operation_id':'original','state':'outcome_unknown'}) as raw:
            with self.assertRaisesRegex(RuntimeError, 'uncertain.*original'):
                self.export(paused())
        self.assertEqual(raw.call_count, 1)

    def test_pending_observation_timeout_keeps_original_without_new_mutation(self):
        with mock.patch.object(self.client, 'raw', return_value={'operation_id':'original','pending':True}) as raw, \
             mock.patch('release_client.time.monotonic', return_value=11):
            with self.assertRaisesRegex(RuntimeError, 'observation_timeout:original'):
                self.client.tool('operation_get', {'operation_id':'original'}, deadline=10, allow_incomplete=True)
        self.assertEqual([c.args[0] for c in raw.call_args_list], ['operation_get'])

    def test_invalid_resume_budget_does_not_call_network(self):
        with mock.patch.object(self.client, 'raw') as raw:
            for budget in (-1, 6, True):
                with self.subTest(budget=budget), self.assertRaises(ValueError):
                    self.export(paused(), max_resumes=budget)
        raw.assert_not_called()

    def test_unchanged_revision_does_not_rotate_key_when_retry_budget_exhausts(self):
        with mock.patch.object(self.client, 'raw', return_value=paused(revision=7)) as raw:
            with self.assertRaisesRegex(RuntimeError, 'resume limit'):
                self.export(paused(revision=7))
        self.assertEqual(raw.call_count, 2)
        self.assertEqual(raw.call_args_list[0], raw.call_args_list[1])

    def test_deadline_and_resume_budget_stop_on_original(self):
        with mock.patch.object(self.client, 'raw') as raw:
            with self.assertRaisesRegex(RuntimeError, 'timeout.*original'):
                self.export(paused(), deadline=time.monotonic()-1)
        raw.assert_not_called()
        with mock.patch.object(self.client, 'raw', side_effect=[paused(revision=1), paused(revision=2)]) as raw:
            with self.assertRaisesRegex(RuntimeError, 'resume limit.*original'):
                self.export(paused(), max_resumes=2)
        self.assertEqual(raw.call_count, 2)
        self.assertNotEqual(raw.call_args_list[0].args[1]['idempotency_key'], raw.call_args_list[1].args[1]['idempotency_key'])

    def test_resume_key_reuses_authoritative_revision_across_invocations(self):
        with mock.patch.object(self.client, 'raw', return_value=complete()) as raw:
            self.export(paused(revision=7)); first = raw.call_args.args[1]
            self.export(paused(revision=7)); second = raw.call_args.args[1]
            self.export(paused(revision=8)); third = raw.call_args.args[1]
        self.assertEqual(first, second)
        self.assertNotEqual(first['idempotency_key'], third['idempotency_key'])

    def test_changed_recovery_handle_stops_before_any_second_mutation(self):
        with mock.patch.object(self.client, 'raw', return_value=complete('other')) as raw:
            with self.assertRaisesRegex(RuntimeError, 'identity'):
                self.export(paused())
        self.assertEqual(raw.call_count, 1)

    def test_coordinator_initial_export_and_import_reach_recovery_with_real_client(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); package = root/'package'; package.mkdir()
            (package/'manifest.json').write_text('{"version":"0.10.24"}')
            bundle = root/'bundle.tgz'; bundle.write_bytes(b'fixture')
            config = root/'config.json'; config.write_text(json.dumps({'origin': 'https://fixture.example',
                'password_file': str(root/'unused-password'), 'controller_device_id': 'device'}))
            device = {'device_id': 'device', 'name': 'fixture', 'online': True, 'converged': False,
                      'capabilities': {'platform': 'macos', 'roots': [str(root)], 'home_dir': str(root)}}
            replies = {'file_download': [paused('export')], 'file_upload': [paused('import')],
                'transfer_resume': [complete('export'), complete('import')],
                'workspace_open': [{'workspace': {'id': 'device:controller'}}, {'workspace': {'id': 'device:worker'}}]}
            def response(name, args):
                if name == 'fleet_status': return {'devices': [device], 'summary': {'gateway_converged': True}}
                return replies[name].pop(0)
            self.client.login = lambda: self.client
            argv = ['fleet-upgrade.py', '--version', '0.10.24', '--package', str(package),
                    '--config', str(config), '--report-dir', str(root/'report')]
            with mock.patch.object(fleet, 'Client', return_value=self.client), \
                 mock.patch.object(fleet, 'verify_package', return_value={'artifacts': {'remote-hosts-code-macos-arm64': {'sha256': SHA}}}), \
                 mock.patch.object(fleet, 'ensure_bundle', return_value=(bundle, SHA)), \
                 mock.patch.object(fleet, 'gateway_upgrade_api', return_value={'state': 'fixture'}), \
                 mock.patch.object(fleet, 'finish_observed_fleet', side_effect=[False, True]), \
                 mock.patch.object(self.client, 'terminal', return_value='staged'), \
                 mock.patch.object(self.client, 'raw', side_effect=response) as raw, mock.patch.object(sys, 'argv', argv):
                fleet.main()
            names = [c.args[0] for c in raw.call_args_list]
            self.assertEqual(names.count('file_upload'), 1)
            self.assertEqual(names.count('file_download'), 1)
            self.assertEqual(names.count('transfer_resume'), 2)


if __name__ == '__main__': unittest.main()

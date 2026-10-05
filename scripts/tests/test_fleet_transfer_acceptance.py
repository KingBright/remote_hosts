"""Existing-transfer acceptance never invokes deployment or OAuth bootstrap."""
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from release_client import Client

spec_module=importlib.util.spec_from_file_location('fleet_transfer_acceptance',ROOT/'scripts/fleet-upgrade.py')
fleet=importlib.util.module_from_spec(spec_module)
spec_module.loader.exec_module(fleet)
SHA='a'*64
ORIGIN='https://fixture.invalid'


def specification(**changes):
    value={'schema_version':1,'origin':ORIGIN,'kind':'export','operation_id':'original',
           'sha256':SHA,'resume_key':'original-resume'}
    value.update(changes)
    return value


def complete(**changes):
    value={'operation_id':'original','state':'completed','sha256':SHA,
           'artifact_id':'same-file','download_url':'https://fixture.invalid/private?signature=omit'}
    value.update(changes)
    return value


class TransferAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.report=pathlib.Path(self.directory.name)/'report.json'

    def client(self,*replies):
        client=Client(ORIGIN,access='fixture-borrowed',transport='legacy')
        client.rpc=mock.Mock(side_effect=[{'structuredContent':v} for v in replies])
        self.addCleanup(client.close)
        return client

    def names(self,client):
        return [call.args[1]['name'] for call in client.rpc.call_args_list]

    def test_completed_original_only_observed_and_private_url_omitted(self):
        client=self.client(complete())
        value=fleet.accept_existing_transfer(client,specification(),self.report)
        self.assertEqual(self.names(client),['operation_get'])
        self.assertEqual(value['state'],'accepted')
        self.assertNotIn('signature',self.report.read_text())
        self.assertNotIn('download_url',self.report.read_text())

    def test_pause_recovery_uses_original_operation_and_no_producer(self):
        paused={'operation_id':'original','state':'paused','next_action':'transfer_resume','transfer_revision':7}
        client=self.client(paused,{'operation_id':'original','state':'queued','next_action':'operation_get'},complete())
        fleet.accept_existing_transfer(client,specification(),self.report)
        self.assertEqual(self.names(client),['operation_get','transfer_resume','operation_get'])
        for call in client.rpc.call_args_list:
            self.assertEqual(call.args[1]['arguments']['operation_id'],'original')

    def test_expired_source_stops_then_explicit_same_file_refresh_can_accept(self):
        waiting={'operation_id':'original','state':'awaiting_source','next_action':'transfer_resume','transfer_revision':3}
        client=self.client(waiting)
        with self.assertRaisesRegex(RuntimeError,'refreshed source'):
            fleet.accept_existing_transfer(client,specification(),self.report)
        self.assertEqual(self.names(client),['operation_get'])
        file={'file_id':'same-file','download_url':'https://fixture.invalid/refreshed?signature=private','file_name':'bundle.tgz'}
        client=self.client(waiting,complete())
        spec=specification(kind='import',file=file)
        fleet.accept_existing_transfer(client,spec,self.report)
        args=client.rpc.call_args_list[1].args[1]['arguments']
        self.assertEqual(args['file'],file)
        self.assertEqual(args['operation_id'],'original')
        self.assertNotIn('private',self.report.read_text())

    def test_uncertain_original_never_resumes(self):
        client=self.client({'operation_id':'original','state':'outcome_unknown'})
        with self.assertRaisesRegex(RuntimeError,'uncertain'):
            fleet.accept_existing_transfer(client,specification(),self.report)
        self.assertEqual(self.names(client),['operation_get'])
        self.assertEqual(json.loads(self.report.read_text())['state'],'needs_recovery')

    def test_wrong_operation_sha_and_missing_artifact_never_accept(self):
        for response in [complete(operation_id='other'),complete(sha256='b'*64),complete(artifact_id=None)]:
            with self.subTest(response=response):
                client=self.client(response)
                with self.assertRaises(RuntimeError):
                    fleet.accept_existing_transfer(client,specification(),self.report)
                self.assertEqual(self.names(client),['operation_get'])
                self.assertEqual(json.loads(self.report.read_text())['state'],'needs_recovery')

    def test_origin_and_invalid_budget_rejected_before_rpc(self):
        for spec in [specification(origin='https://other.invalid'),specification(max_resumes=6),
                     specification(deadline_seconds=0),specification(sha256='wrong')]:
            client=self.client()
            with self.assertRaises(ValueError):
                fleet.accept_existing_transfer(client,spec,self.report)
            client.rpc.assert_not_called()

    def test_lost_resume_response_preserves_cause_and_private_error_not_saved(self):
        failure=OSError('transport signature=private')
        paused={'operation_id':'original','state':'paused','next_action':'transfer_resume','transfer_revision':0}
        client=self.client(paused)
        client.rpc.side_effect=[{'structuredContent':paused},failure]
        with self.assertRaises(OSError) as caught:
            fleet.accept_existing_transfer(client,specification(),self.report)
        self.assertIs(caught.exception,failure)
        self.assertEqual(self.names(client),['operation_get','transfer_resume'])
        self.assertNotIn('private',self.report.read_text())

    def test_coordinator_process_reentry_observes_original_without_deployment(self):
        # Two independent Client instances model coordinator shutdown/reentry;
        # actual backend process kills are covered by the fixed Rust fixture suite.
        client=self.client({'operation_id':'original','state':'awaiting_source','next_action':'transfer_resume'})
        with self.assertRaises(RuntimeError):
            fleet.accept_existing_transfer(client,specification(),self.report)
        client.close()
        resumed=self.client(complete())
        fleet.accept_existing_transfer(resumed,specification(),self.report)
        self.assertEqual(self.names(resumed),['operation_get'])

    def test_cli_borrows_grant_and_bypasses_all_deployment_and_login_paths(self):
        spec=pathlib.Path(self.directory.name)/'input.json'
        spec.write_text(json.dumps(specification()))
        client=self.client(complete())
        with mock.patch.object(fleet,'Client',return_value=client) as factory, \
             mock.patch.object(Client,'login',side_effect=AssertionError('no login')), \
             mock.patch.object(fleet,'verify_package',side_effect=AssertionError('no package')), \
             mock.patch.object(fleet,'gateway_upgrade_api',side_effect=AssertionError('no upgrade')), \
             mock.patch.object(fleet,'gateway_upgrade_ssh',side_effect=AssertionError('no SSH')), \
             mock.patch.object(sys,'argv',['fleet-upgrade.py','--acceptance-only',str(spec),'--report-dir',self.directory.name,'--access-token-stdin']), \
             mock.patch.object(sys,'stdin',io.StringIO('fixture-borrowed')),mock.patch.object(sys,'stdout',io.StringIO()):
            fleet.main()
        factory.assert_called_once_with(ORIGIN,access='fixture-borrowed')
        self.assertEqual(self.names(client),['operation_get'])
        self.assertIsNone(client.refresh)

    def test_cli_rejects_mixed_deployment_inputs_before_token_or_client(self):
        with mock.patch.object(sys,'argv',['fleet-upgrade.py','--acceptance-only','unused','--version','0.10.24','--report-dir',self.directory.name,'--access-token-stdin']), \
             mock.patch.object(fleet,'Client') as factory,mock.patch.object(sys,'stderr',io.StringIO()):
            with self.assertRaises(SystemExit):
                fleet.main()
        factory.assert_not_called()

    def test_cli_transport_exception_never_prints_private_capability_or_bearer(self):
        spec=pathlib.Path(self.directory.name)/'input.json'
        spec.write_text(json.dumps(specification()))
        client=self.client()
        client.rpc.side_effect=OSError('https://fixture.invalid/source?signature=private fixture-borrowed')
        with mock.patch.object(fleet,'Client',return_value=client), \
             mock.patch.object(sys,'argv',['fleet-upgrade.py','--acceptance-only',str(spec),'--report-dir',self.directory.name,'--access-token-stdin']), \
             mock.patch.object(sys,'stdin',io.StringIO('fixture-borrowed')):
            with self.assertRaises(SystemExit) as caught:
                fleet.main()
        self.assertIn('operation=original',str(caught.exception))
        self.assertNotIn('private',str(caught.exception))
        self.assertNotIn('fixture-borrowed',str(caught.exception))


if __name__=='__main__':
    unittest.main()

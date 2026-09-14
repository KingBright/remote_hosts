#!/usr/bin/env python3
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock
import sys

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import build_slot
import release_client


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fleet = load('fleet_upgrade_0103', SCRIPTS/'fleet-upgrade.py')
linux_upgrade = load('linux_upgrade_0103', SCRIPTS/'upgrade-code-agent-linux.py')


class FakeClient:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def tool(self, name, args):
        self.calls.append((name, args))
        return self.result


class ReleaseHardening0103Tests(unittest.TestCase):
    def test_controller_device_id_falls_back_to_workspace_prefix(self):
        self.assertEqual(fleet.controller_device_id({'controller_workspace':'device-1:workspace-2'}), 'device-1')
        self.assertEqual(fleet.controller_device_id({'controller_device_id':'explicit','controller_workspace':'other:ws'}), 'explicit')
        self.assertIsNone(fleet.controller_device_id({}))

    def test_verified_import_resumes_publication_recovery(self):
        sha = 'a'*64
        imported = {'state':'paused','next_action':'transfer_resume','operation_id':'op-1','publication_pending':True}
        completed = {'state':'completed','sha256':sha,'operation_id':'op-1'}
        client = FakeClient(completed)
        exported = {'artifact_id':'source-1','download_url':'https://example.invalid/file'}
        bundle = pathlib.Path('remote-hosts-code-0.10.3-bundle.tgz')
        result = fleet.verified_import(client, imported, exported, bundle, sha, '0.10.3', 'device-1')
        self.assertEqual(result, completed)
        self.assertEqual(client.calls[0][0], 'transfer_resume')
        self.assertEqual(client.calls[0][1]['operation_id'], 'op-1')
        self.assertEqual(client.calls[0][1]['file']['file_id'], 'source-1')

    def test_verified_import_rejects_unverified_result(self):
        client = FakeClient({'state':'completed','sha256':'b'*64})
        with self.assertRaisesRegex(RuntimeError, 'bundle import not verified'):
            fleet.verified_import(client, {'state':'completed','sha256':'b'*64},
                {'artifact_id':'source','download_url':'https://example.invalid/file'},
                pathlib.Path('bundle.tgz'), 'a'*64, '0.10.3', 'device-1')
        self.assertEqual(client.calls, [])

    def test_linux_readiness_retries_transient_gateway_error(self):
        error = linux_upgrade.GatewayObservationError({
            'stage':'gateway_readiness','category':'network_timeout','retryable':True,
            'next_action':'retry_same_readonly_probe_with_backoff','attempts':1})
        samples = [
            error,
            {'agent_version':'0.10.3','ready':True,'last_seen':11,'session':'session-1'},
            {'agent_version':'0.10.3','ready':True,'last_seen':12,'session':'session-1'},
            {'agent_version':'0.10.3','ready':True,'last_seen':13,'session':'session-1'},
        ]
        with mock.patch.object(linux_upgrade, 'gateway_observation', side_effect=samples), \
             mock.patch.object(linux_upgrade.time, 'sleep'):
            result = linux_upgrade.wait_gateway({}, '0.10.3', 10, timeout=5)
        self.assertTrue(result['gateway_verified'])
        self.assertEqual(result['samples'], 3)
        self.assertEqual(result['last_seen'], 13)

    def test_release_client_retries_same_rpc_identity(self):
        client = release_client.Client('https://example.invalid', access='token')
        success = {'result': {'tools': []}}
        with mock.patch.object(client, 'parsed', side_effect=[TimeoutError(), success]) as parsed, \
             mock.patch.object(release_client.time, 'sleep'):
            result = client.rpc('tools/list', {})
        self.assertEqual(result, {'tools': []})
        first = parsed.call_args_list[0].args[1]
        second = parsed.call_args_list[1].args[1]
        self.assertEqual(first['id'], second['id'])
        self.assertEqual(first, second)

    def test_stale_build_owner_recovers_only_with_dead_recorded_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory).resolve()
            slot = base/'slot'; slot.mkdir()
            (slot/'slot.json').write_text(json.dumps({'schema_version':1,'purpose':'remote-hosts-release-slot'}))
            old_report = base/'old.json'; old_report.write_text(json.dumps({'state':'running','child_pid':2222}))
            (slot/'owner.json').write_text(json.dumps({'pid':1111,'report':str(old_report),'snapshot_id':'old','state':'running'}))
            with mock.patch.object(build_slot, 'process_alive', return_value=False):
                with build_slot.lease(slot, base/'new.json', 'new'):
                    owner = json.loads((slot/'owner.json').read_text())
                    self.assertEqual(owner['snapshot_id'], 'new')
                    self.assertEqual(owner['state'], 'running')
            self.assertEqual(json.loads((slot/'owner.json').read_text())['state'], 'released')

    def test_stale_build_owner_refuses_live_process_group(self):
        previous = {'pid':1111,'report':'/tmp/report'}
        with mock.patch.object(build_slot, 'process_alive', side_effect=[False, True]), \
             mock.patch.object(pathlib.Path, 'read_text', return_value=json.dumps({'child_pid':2222})):
            self.assertFalse(build_slot.stale_owner_recoverable(previous))

    def test_windows_updater_discovers_installed_paths_and_ignores_console_hosts(self):
        text = (SCRIPTS/'upgrade-code-agent-windows.ps1').read_text()
        self.assertIn("Get-ScheduledTask -TaskName $TaskName", text)
        self.assertIn("--config\\s+", text)
        self.assertIn("$env:LOCALAPPDATA", text)
        self.assertIn("'conhost.exe','OpenConsole.exe'", text)


if __name__ == '__main__':
    unittest.main()

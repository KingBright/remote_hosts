"""Updater regression tests: temporary files and mocked services only."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'upgrade-code-agent.py'
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location('upgrade_code_agent', SCRIPT)
UPGRADE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(UPGRADE)


class UpdaterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.tmp.name)
        self.base = self.home / '.local/share/remote-hosts-code'
        (self.base / 'bin').mkdir(parents=True)
        self.binary = self.base / 'bin/remote-hosts-code'
        self.binary.write_bytes(b'new-candidate-fixture')
        self.candidate = self.home / 'candidate'
        self.candidate.write_bytes(self.binary.read_bytes())
        self.result = self.home / 'deployment.json'
        self.prior = b'{"state":"upgraded","backup":"verified-old-backup"}'
        self.result.write_bytes(self.prior)
        self.checksum = UPGRADE.sha(self.candidate)
        self.args = argparse.Namespace(candidate=self.candidate, sha256=self.checksum,
                                       version='0.3.1', result=self.result, idle_timeout=0)
        self.mask = os.umask(0o077)
        self.gateway_patch = mock.patch.object(UPGRADE, 'gateway_observation', return_value={'last_seen':0})
        self.readiness_patch = mock.patch.object(UPGRADE, 'wait_ready', return_value={'pid':2020,'started_at':100,
            'gateway_verified':True,'readiness':'stable_authenticated_control_plane'})
        self.gateway_mock = self.gateway_patch.start()
        self.readiness_mock = self.readiness_patch.start()
        self.maintenance_patch=mock.patch.object(UPGRADE,"MaintenanceLease")
        self.maintenance_mock=self.maintenance_patch.start()
        self.maintenance_mock.return_value.close.return_value={"released":True}

    def tearDown(self):
        self.maintenance_patch.stop()
        self.gateway_patch.stop()
        self.readiness_patch.stop()
        os.umask(self.mask)
        self.tmp.cleanup()

    def invoke(self):
        argv = [str(SCRIPT), '--candidate', str(self.candidate), '--sha256', self.checksum,
                '--version', '0.3.1', '--result', str(self.result), '--idle-timeout', '0']
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(pathlib.Path, 'home', return_value=self.home):
            UPGRADE.main()

    def database(self):
        state = self.base / 'state with ? and #'
        state.mkdir()
        (self.base / 'agent.json').write_text(json.dumps({'state_dir': str(state)}))
        with contextlib.closing(sqlite3.connect(state / 'state.sqlite')) as db, db:
            db.execute('CREATE TABLE kv (kind TEXT,key TEXT,value TEXT)')
            db.execute('INSERT INTO kv VALUES(?,?,?)', ('runtime', 'build', json.dumps(
                {'version': '0.3.1', 'pid': 2020, 'started_at': 100})))

    def test_repeated_identical_candidate_does_not_restart_or_replace_receipt(self):
        # No config exists: a no-op must not enter health checks or idle waiting.
        output = io.StringIO()
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1\n'), \
             mock.patch.object(UPGRADE.subprocess, 'run') as run, \
             mock.patch.object(UPGRADE.sqlite3, 'connect', side_effect=AssertionError('must not query live state')), \
             contextlib.redirect_stdout(output):
            for _ in range(3):
                self.invoke()
        run.assert_not_called()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(records), 3)
        for record in records:
            self.assertEqual(record['state'], 'no_change')
            self.assertEqual(record['health'], 'not_evaluated')
            self.assertFalse(record['service_changed'])
        self.assertEqual(self.result.read_bytes(), self.prior)
        self.assertFalse((self.base / 'releases').exists())
        self.assertFalse((self.home / 'deployment-attempts').exists())

    def test_no_change_has_a_receipt_when_job_has_no_previous_result(self):
        self.result.unlink()
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1'), \
             mock.patch.object(UPGRADE.subprocess, 'run') as run, contextlib.redirect_stdout(io.StringIO()):
            self.invoke()
        run.assert_not_called()
        self.assertEqual(json.loads(self.result.read_text())['state'], 'no_change')
        self.assertFalse(json.loads(self.result.read_text())['service_changed'])

    def test_wrong_hash_never_executes_candidate_or_restarts_service(self):
        self.candidate.write_bytes(b'changed-after-manifest')
        with mock.patch.object(UPGRADE.subprocess, 'check_output') as version, \
             mock.patch.object(UPGRADE.subprocess, 'run') as run, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.invoke()
        version.assert_not_called()
        run.assert_not_called()
        self.assertEqual(json.loads(self.result.read_text())['state'], 'failed')
        self.assertEqual(self.binary.read_bytes(), b'new-candidate-fixture')

    def test_version_mismatch_never_restarts_service(self):
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 9.9.9'), \
             mock.patch.object(UPGRADE.subprocess, 'run') as run, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.invoke()
        run.assert_not_called()
        self.assertIn('version mismatch', json.loads(self.result.read_text())['error'])

    def test_exclusive_upgrade_lock_is_released_after_exception(self):
        with self.assertRaisesRegex(ValueError, 'fixture failure'):
            with UPGRADE.upgrade_lock(self.base):
                with self.assertRaisesRegex(RuntimeError, 'another upgrade'):
                    with UPGRADE.upgrade_lock(self.base):
                        self.fail('second upgrade entered')
                raise ValueError('fixture failure')
        with UPGRADE.upgrade_lock(self.base):
            pass

    def test_busy_updater_does_not_execute_or_overwrite_receipt(self):
        with UPGRADE.upgrade_lock(self.base), mock.patch.object(UPGRADE.subprocess, 'run') as run, \
             mock.patch.object(UPGRADE.subprocess, 'check_output') as version:
            with self.assertRaisesRegex(RuntimeError, 'another upgrade'):
                self.invoke()
        version.assert_not_called()
        run.assert_not_called()
        self.assertEqual(self.result.read_bytes(), self.prior)

    def test_nested_resource_group_does_not_count_as_running_agent(self):
        nested = '\tresource group = {\n\t\tstate = running\n\t\tpid = 700\n\t}\n'
        self.assertIsNone(UPGRADE.current_launchd_pid('service = {\n\tstate = spawn scheduled\n' + nested + '}'))
        self.assertIsNone(UPGRADE.current_launchd_pid('service = {\n\tstate = running\n' + nested + '}'))
        self.assertEqual(UPGRADE.current_launchd_pid('service = {\n\tstate = running\n\tpid = 1927\n' + nested + '}'), 1927)
        self.assertIsNone(UPGRADE.current_launchd_pid('\tstate = running\n\tpid = 0\n'))

    def test_attempt_history_is_not_overwritten_by_next_attempt(self):
        first = {'state': 'failed', 'error': 'first'}
        second = {'state': 'upgraded', 'readiness': 'local_process_only'}
        UPGRADE.save_attempt(self.result, first)
        first_bytes = pathlib.Path(first['attempt_receipt']).read_bytes()
        UPGRADE.save_attempt(self.result, second)
        self.assertNotEqual(first['attempt_receipt'], second['attempt_receipt'])
        self.assertEqual(pathlib.Path(first['attempt_receipt']).read_bytes(), first_bytes)
        self.assertEqual(json.loads(self.result.read_text()), second)
        self.assertEqual(len(list((self.home / 'deployment-attempts').glob('*.json'))), 2)

    def test_changed_candidate_retains_backup_and_marks_local_only_readiness(self):
        self.binary.write_bytes(b'old-verified-binary')
        self.database()
        def run(command, **kwargs):
            text = 'service = {\n\tstate = running\n\tpid = 2020\n}\n' if command[:2] == ['launchctl', 'print'] else ''
            return subprocess.CompletedProcess(command, 0, stdout=text, stderr='')
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1'), \
             mock.patch.object(UPGRADE.subprocess, 'run', side_effect=run) as runs, contextlib.redirect_stdout(io.StringIO()):
            self.invoke()
        record = json.loads(self.result.read_text())
        self.assertEqual(record['state'], 'upgraded')
        self.assertEqual(record['readiness'], 'stable_authenticated_control_plane')
        self.assertTrue(record['gateway_verified'])
        self.readiness_mock.assert_called_once()
        self.assertEqual(pathlib.Path(record['backup']).read_bytes(), b'old-verified-binary')
        self.assertEqual(self.binary.read_bytes(), self.candidate.read_bytes())
        self.assertEqual(sum(c.args[0][:2] == ['launchctl', 'kickstart'] for c in runs.call_args_list), 1)

    def test_failed_restart_restores_old_binary_under_the_same_lock(self):
        self.binary.write_bytes(b'old-verified-binary')
        self.database()
        kicks = []
        def run(command, **kwargs):
            if command[:2] == ['launchctl', 'kickstart']:
                kicks.append(command)
                if len(kicks) == 1:
                    raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1'), \
             mock.patch.object(UPGRADE.subprocess, 'run', side_effect=run), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.invoke()
        record = json.loads(self.result.read_text())
        self.assertEqual(record['state'], 'failed')
        self.assertEqual(record['rollback'], 'restored_previous_binary_and_verified_gateway')
        self.assertEqual(self.binary.read_bytes(), b'old-verified-binary')
        self.assertEqual(len(kicks), 2)

    def test_gateway_not_ready_does_not_replace_binary(self):
        self.binary.write_bytes(b'old-verified-binary')
        self.database()
        self.gateway_mock.side_effect = RuntimeError('gateway_readiness_unavailable')
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1'), \
             mock.patch.object(UPGRADE.subprocess, 'run') as run, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.invoke()
        run.assert_not_called()
        self.assertEqual(self.binary.read_bytes(), b'old-verified-binary')

    def test_failed_network_readiness_restores_and_checks_previous_release(self):
        self.binary.write_bytes(b'old-verified-binary')
        self.database()
        self.readiness_mock.side_effect = [RuntimeError('readiness_timeout'), {'gateway_verified':True}]
        with mock.patch.object(UPGRADE.subprocess, 'check_output', return_value='remote-hosts-code 0.3.1'), \
             mock.patch.object(UPGRADE.subprocess, 'run', return_value=subprocess.CompletedProcess([],0)), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.invoke()
        self.assertEqual(self.binary.read_bytes(), b'old-verified-binary')
        self.assertEqual(self.readiness_mock.call_count, 2)
        self.assertEqual(json.loads(self.result.read_text())['rollback'], 'restored_previous_binary_and_verified_gateway')

    def test_invalid_version_is_rejected_before_lock_or_execution(self):
        argv = [str(SCRIPT), '--candidate', str(self.candidate), '--sha256', self.checksum,
                '--version', '../invalid', '--result', str(self.result)]
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(UPGRADE, 'upgrade_lock') as lock, \
             contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                UPGRADE.main()
        lock.assert_not_called()


if __name__ == '__main__':
    unittest.main()

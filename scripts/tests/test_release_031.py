"""Release lifecycle tests use fake clocks, temporary files and mocked launchd."""
import argparse
import copy
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import agent_upgrade_support as support

spec = importlib.util.spec_from_file_location('launcher031', SCRIPTS / 'launch-code-upgrade.py')
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


class ReadyTests(unittest.TestCase):
    def setUp(self):
        self.local = {'running': True, 'pid': 42, 'runs': '1',
                      'build': {'pid': 42, 'version': '0.3.1', 'started_at': 100},
                      'readiness': {'pid': 42, 'version': '0.3.1', 'session': 'a' * 64,
                                    'phase': 'polling', 'lanes': {x: 119 for x in support.LANES}}}
        self.remote = {'ready': True, 'agent_version': '0.3.1', 'last_seen': 120, 'observed_at': 120, 'session': 'a' * 64}

    def test_matching_pid_alone_never_passes(self):
        local = copy.deepcopy(self.local)
        local.pop('readiness')
        self.assertIsNone(support.sample_identity(local, self.remote, '0.3.1', 100, 99, now=120))
        self.assertIsNotNone(support.sample_identity(self.local, self.remote, '0.3.1', 100, 99, now=120))

    def test_stale_missing_future_lanes_or_wrong_session_never_pass(self):
        for change in ('stale', 'missing', 'future', 'pid', 'session', 'phase'):
            local = copy.deepcopy(self.local)
            if change == 'stale': local['readiness']['lanes']['read'] = 100
            elif change == 'missing': del local['readiness']['lanes']['control']
            elif change == 'future': local['readiness']['lanes']['read'] = 999
            elif change == 'pid': local['pid'] = 99
            elif change == 'session': local['readiness']['session'] = 'b' * 64
            else: local['readiness']['phase'] = 'gateway_wait'
            self.assertIsNone(support.sample_identity(local, self.remote, '0.3.1', 100, 99, now=160), change)

    def test_gateway_must_advance_since_preflight(self):
        self.assertIsNone(support.sample_identity(self.local, self.remote, '0.3.1', 100, 120, now=120))
        remote = dict(self.remote, ready=False)
        self.assertIsNone(support.sample_identity(self.local, remote, '0.3.1', 100, 99, now=120))

    def test_stability_window_survives_repeated_checks_without_mutations(self):
        clock = [0.0]
        def advancing_local(_base):
            value = copy.deepcopy(self.local)
            value['readiness']['lanes'] = {lane: 120+int(clock[0]) for lane in support.LANES}
            return value
        def advancing_remote(_config, **_options):
            return dict(self.remote, last_seen=120+int(clock[0]), observed_at=120+int(clock[0]))
        with mock.patch.object(support.time, 'monotonic', side_effect=lambda: clock[0]), \
             mock.patch.object(support.time, 'time', side_effect=lambda: 120+int(clock[0])), \
             mock.patch.object(support.time, 'sleep', side_effect=lambda dt: clock.__setitem__(0, clock[0] + dt)), \
             mock.patch.object(support, 'local_observation', side_effect=advancing_local), \
             mock.patch.object(support, 'gateway_observation', side_effect=advancing_remote):
            result = support.wait_ready(pathlib.Path('/unused'), {}, '0.3.1', 100, 99, timeout=20, stable_seconds=5)
        self.assertGreaterEqual(result['stable_seconds'], 5)
        self.assertTrue(result['gateway_verified'])
        self.assertTrue(result['all_lanes_verified'])
        self.assertEqual(result['functional_acceptance'], 'separate_required_gate')

    def test_changing_pid_session_times_out_instead_of_flapping_success(self):
        clock = [0.0]
        def changing(_base):
            local = copy.deepcopy(self.local)
            pid = 42 + int(clock[0])
            local['pid'] = local['build']['pid'] = local['readiness']['pid'] = pid
            return local
        with mock.patch.object(support.time, 'monotonic', side_effect=lambda: clock[0]), \
             mock.patch.object(support.time, 'time', return_value=120), \
             mock.patch.object(support.time, 'sleep', side_effect=lambda dt: clock.__setitem__(0, clock[0] + dt)), \
             mock.patch.object(support, 'local_observation', side_effect=changing), \
             mock.patch.object(support, 'gateway_observation', return_value=self.remote):
            with self.assertRaisesRegex(RuntimeError, 'readiness_timeout'):
                support.wait_ready(pathlib.Path('/unused'), {}, '0.3.1', 100, 99, timeout=10, stable_seconds=5)


class LauncherTests(unittest.TestCase):
    def test_plist_has_no_reentry_triggers_or_shell_wrapper(self):
        p = pathlib.Path('/absolute')
        value = launcher.make_plist('label', p/'python', p/'script', p/'candidate', 'a'*64, '0.3.1', p/'result', p)
        self.assertIs(value['RunAtLoad'], True)
        self.assertIs(value['KeepAlive'], False)
        for key in ('StartInterval', 'StartCalendarInterval', 'WatchPaths', 'QueueDirectories', 'Sockets'):
            self.assertNotIn(key, value)
        self.assertEqual(value['ProgramArguments'][0], '/absolute/python')
        self.assertNotIn('sh', value['ProgramArguments'])

    def test_prepare_snapshots_updater_and_reuses_same_job(self):
        with tempfile.TemporaryDirectory() as d:
            base = pathlib.Path(d)
            candidate = base/'candidate'
            candidate.write_bytes(b'candidate')
            args = argparse.Namespace(candidate=candidate, sha256=launcher.sha(candidate), version='0.3.1')
            record = launcher.prepare(args, base)
            self.assertEqual(record, launcher.prepare(args, base))
            directory = pathlib.Path(record['directory'])
            self.assertEqual((directory/'upgrade-code-agent.py').read_bytes(), (SCRIPTS/'upgrade-code-agent.py').read_bytes())
            (directory/'upgrade-code-agent.py').write_text('changed')
            with self.assertRaisesRegex(ValueError, 'changed'):
                launcher.prepare(args, base)

    def test_bootstrap_is_requested_once_even_after_completion(self):
        with tempfile.TemporaryDirectory() as d:
            candidate = pathlib.Path(d)/'candidate'
            candidate.write_bytes(b'fixture')
            record = launcher.prepare(argparse.Namespace(candidate=candidate, sha256=launcher.sha(candidate), version='0.3.1'), pathlib.Path(d))
            with mock.patch.object(launcher.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
                self.assertEqual(launcher.start_once(record)['state'], 'started')
                pathlib.Path(record['result']).write_text('{"state":"upgraded"}')
                self.assertEqual(launcher.start_once(record)['state'], 'already_requested')
            run.assert_called_once()
            self.assertEqual(run.call_args.args[0][1], 'bootstrap')

    def test_uncertain_bootstrap_outcome_is_not_automatically_replayed(self):
        with tempfile.TemporaryDirectory() as d:
            candidate = pathlib.Path(d)/'candidate'
            candidate.write_bytes(b'fixture')
            record = launcher.prepare(argparse.Namespace(candidate=candidate, sha256=launcher.sha(candidate), version='0.3.1'), pathlib.Path(d))
            with mock.patch.object(launcher.subprocess, 'run', side_effect=subprocess.TimeoutExpired('launchctl', 10)) as run:
                self.assertEqual(launcher.start_once(record)['state'], 'bootstrap_outcome_unknown')
                self.assertEqual(launcher.start_once(record)['state'], 'already_requested')
            run.assert_called_once()


if __name__ == '__main__':
    unittest.main()

"""Fail-closed release gates; no real launchd or production credentials."""
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
# Test is uploaded under scripts/tests in the actual project.
sys.path.insert(0, str(SCRIPTS))
import agent_upgrade_support as support

spec = importlib.util.spec_from_file_location('guardrail_launcher', SCRIPTS/'launch-code-upgrade.py')
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


class ReadinessGuardrails(unittest.TestCase):
    def setUp(self):
        self.local = {'running': True, 'pid': 42, 'runs': '1',
            'build': {'pid': 42, 'version': '0.3.1', 'started_at': 100},
            'readiness': {'pid': 42, 'version': '0.3.1', 'session': 'a'*64,
                'phase': 'polling', 'lanes': {lane: 119 for lane in support.LANES}}}
        self.remote = {'ready': True, 'agent_version': '0.3.1', 'last_seen': 120,
                       'observed_at': 120, 'session': 'a'*64}

    def test_cached_observations_cannot_count_as_continued_network_progress(self):
        clock = [0.0]
        with mock.patch.object(support.time, 'monotonic', side_effect=lambda: clock[0]), \
             mock.patch.object(support.time, 'time', side_effect=lambda: 120+int(clock[0])), \
             mock.patch.object(support.time, 'sleep', side_effect=lambda dt: clock.__setitem__(0, clock[0]+dt)), \
             mock.patch.object(support, 'local_observation', return_value=self.local), \
             mock.patch.object(support, 'gateway_observation', return_value=self.remote):
            with self.assertRaisesRegex(RuntimeError, 'readiness_timeout'):
                support.wait_ready(pathlib.Path('/unused'), {}, '0.3.1', 100, 99,
                                   timeout=12, stable_seconds=5)

    def test_stale_gateway_or_future_seen_timestamp_cannot_pass(self):
        for change in ('stale', 'future', 'boolean'):
            remote = dict(self.remote)
            if change == 'stale':
                remote.update(last_seen=101, observed_at=170)
            elif change == 'future':
                remote.update(last_seen=121, observed_at=120)
            else:
                remote['last_seen'] = True
            self.assertIsNone(support.sample_identity(self.local, remote, '0.3.1', 100, 99, now=120), change)

    def test_all_lanes_must_continue_not_just_one_lane(self):
        clock = [0.0]
        def local(_base):
            value = copy.deepcopy(self.local)
            value['readiness']['lanes']['read'] = 120+int(clock[0])
            return value
        def remote(_config, **_options):
            return dict(self.remote, last_seen=120+int(clock[0]), observed_at=120+int(clock[0]))
        with mock.patch.object(support.time, 'monotonic', side_effect=lambda: clock[0]), \
             mock.patch.object(support.time, 'time', side_effect=lambda: 120+int(clock[0])), \
             mock.patch.object(support.time, 'sleep', side_effect=lambda dt: clock.__setitem__(0, clock[0]+dt)), \
             mock.patch.object(support, 'local_observation', side_effect=local), \
             mock.patch.object(support, 'gateway_observation', side_effect=remote):
            with self.assertRaisesRegex(RuntimeError, 'readiness_timeout'):
                support.wait_ready(pathlib.Path('/unused'), {}, '0.3.1', 100, 99,
                                   timeout=12, stable_seconds=5)


class LauncherGuardrails(unittest.TestCase):
    def prepared(self, root):
        candidate = root/'candidate'
        candidate.write_bytes(b'fixture candidate')
        import argparse
        return launcher.prepare(argparse.Namespace(candidate=candidate, sha256=launcher.sha(candidate), version='0.3.1'), root)

    def test_bootstrap_timeout_has_durable_unknown_result_and_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self.prepared(pathlib.Path(tmp))
            unloaded={'loaded':False,'running':False,'service':'gui/501/com.remote-hosts.code-upgrade'}
            with mock.patch.object(launcher, 'service_state', side_effect=[unloaded, unloaded]), \
                 mock.patch.object(launcher, 'cleanup_legacy_jobs', return_value=0), \
                 mock.patch.object(launcher.subprocess, 'run', side_effect=subprocess.TimeoutExpired('launchctl', 10)) as run:
                result = launcher.start_once(record, require_identity=False)
                self.assertEqual(result['state'], 'kickstart_outcome_unknown')
                saved = json.loads((pathlib.Path(record['directory'])/'bootstrap-result.json').read_text())
                self.assertEqual(saved['state'], 'kickstart_outcome_unknown')
                self.assertEqual(launcher.start_once(record, require_identity=False)['state'], 'already_requested')
            run.assert_called_once()

    def test_changed_plist_cannot_be_started_after_prepare(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self.prepared(pathlib.Path(tmp))
            (pathlib.Path(record['directory'])/'job.plist').write_text('changed')
            with mock.patch.object(launcher.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
                with self.assertRaisesRegex(ValueError, 'changed'):
                    launcher.start_once(record)
            run.assert_not_called()
            self.assertFalse((pathlib.Path(record['directory'])/'start-requested.json').exists())

    def test_failed_bootstrap_is_not_reported_as_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self.prepared(pathlib.Path(tmp))
            unloaded={'loaded':False,'running':False,'service':'gui/501/com.remote-hosts.code-upgrade'}
            with mock.patch.object(launcher, 'service_state', side_effect=[unloaded, unloaded]), \
                 mock.patch.object(launcher, 'cleanup_legacy_jobs', return_value=0), \
                 mock.patch.object(launcher.subprocess, 'run', return_value=subprocess.CompletedProcess([], 5)) as run:
                self.assertEqual(launcher.start_once(record, require_identity=False)['state'], 'bootstrap_failed')
                self.assertEqual(launcher.start_once(record, require_identity=False)['state'], 'already_requested')
            run.assert_called_once()

    def test_missing_signing_authorization_never_writes_launch_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self.prepared(pathlib.Path(tmp))
            signing={'state':'authorization_required','certificate_sha1':'a'*40}
            with mock.patch.object(launcher.macos_code_identity, 'status', return_value=signing), \
                 mock.patch.object(launcher.subprocess, 'run') as run:
                result = launcher.start_once(record)
            self.assertEqual(result['state'], 'authorization_required')
            self.assertEqual(result['signing'], signing)
            self.assertFalse((pathlib.Path(record['directory'])/'start-requested.json').exists())
            run.assert_not_called()


if __name__ == '__main__':
    unittest.main()

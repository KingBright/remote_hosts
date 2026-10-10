"""Real child exit/cancellation and detached launch boundaries; never run Cargo."""
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import build_process

SPEC = importlib.util.spec_from_file_location('supervised_release', SCRIPTS/'release-code.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@unittest.skipUnless(os.name == 'posix', 'owned process sessions require POSIX')
class SupervisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def run_child(self, code, *, active=False, update=None, idle=5):
        original = subprocess.Popen
        child = []
        counter = [0.0]

        def spawn(*args, **kwargs):
            value = original(*args, **kwargs)
            child.append(value)
            return value

        def observe():
            if not child or child[0].poll() is not None:
                return {}
            if active:
                counter[0] += 0.02
            pid = child[0].pid
            return {pid: {'parent': os.getpid(), 'group': pid, 'state': 'S',
                          'cpu': counter[0], 'name': 'python'}}

        with mock.patch.object(build_process.subprocess, 'Popen', side_effect=spawn), \
             mock.patch.object(build_process, 'process_table', side_effect=observe):
            return build_process.run([sys.executable, '-c', code], self.root, self.root/'child.log',
                                     dict(os.environ), idle_timeout=idle, on_update=update, poll_interval=0.02)

    def test_quiet_activity_outlives_idle_budget_and_keeps_nonzero_exit(self):
        result = self.run_child('import time; time.sleep(.3); print("完整输出"); raise SystemExit(7)',
                                active=True, idle=0.05)
        self.assertGreater(result['elapsed_seconds'], result['idle_timeout_seconds'])
        self.assertEqual(result['state'], 'finished')
        self.assertEqual(result['raw_exit_code'], 7)
        self.assertIsNone(result['wall_timeout_seconds'])
        self.assertTrue(result['output_complete'])
        self.assertIn('完整输出', (self.root/'child.log').read_text())
        self.assertEqual(json.loads(pathlib.Path(result['exit_receipt']).read_text())['exit_code'], 7)

    def test_live_log_and_heartbeat_do_not_manufacture_progress(self):
        seen = []

        def update(value):
            seen.append(value)
            if value['state'] == 'running' and value['log_bytes']:
                self.assertIn('pulse', (self.root/'child.log').read_text())

        result = self.run_child('import time; print("pulse",flush=True); time.sleep(.3)', update=update)
        running = [x for x in seen if x['state'] == 'running' and x['log_bytes']]
        self.assertGreater(len(running), 1)
        self.assertGreater(len({x['heartbeat']['observed_at'] for x in running}), 1)
        self.assertLess(len({x['progress']['last_observed_at'] for x in running}), len(running))
        self.assertEqual(result['raw_exit_code'], 0)

    def test_final_status_failure_does_not_erase_child_exit(self):
        def broken(value):
            if value.get('exit_code') == 7:
                raise OSError('fixture receipt publication failure')
        result = self.run_child('raise SystemExit(7)', update=broken)
        self.assertEqual(result['state'], 'supervision_failed')
        self.assertEqual(result['raw_exit_code'], 7)
        self.assertEqual(json.loads(pathlib.Path(result['exit_receipt']).read_text())['raw_exit_code'], 7)

    def test_confirmed_idle_stops_owned_child_and_keeps_signal_exit(self):
        result = self.run_child('import time; time.sleep(30)', idle=0.06)
        self.assertEqual(result['state'], 'stalled')
        self.assertLess(result['raw_exit_code'], 0)
        self.assertEqual(result['cleanup']['state'], 'complete')
        self.assertTrue(result['output_complete'])
        self.assertIn('unchanged_log_bytes', result['stop_evidence'])

    def test_start_claim_never_spawns_twice(self):
        snapshot = self.root/'snapshot';snapshot.mkdir()
        report = self.root/'report.json'
        with mock.patch.object(runner.source_snapshot, 'check', return_value={'snapshot_id':'fixture'}), \
             mock.patch.object(runner.os, 'posix_spawn', return_value=12345) as spawn, \
             contextlib.redirect_stdout(io.StringIO()):
            runner.start_pipeline(snapshot, report)
            runner.start_pipeline(snapshot, report)
        self.assertEqual(spawn.call_count, 1)
        self.assertTrue(spawn.call_args.kwargs['setsid'])
        launch = json.loads((self.root/'report-launch.json').read_text())
        self.assertEqual(launch['worker_pid'], 12345)
        self.assertEqual(launch['state'], 'submitted')

    def test_explicit_interrupt_cleans_only_owned_child_and_keeps_exit(self):
        def interrupt(value):
            if value['state'] == 'running':
                raise KeyboardInterrupt
        result = self.run_child('import time; time.sleep(30)', update=interrupt)
        self.assertEqual(result['state'], 'interrupted')
        self.assertLess(result['raw_exit_code'], 0)
        self.assertEqual(result['cleanup']['state'], 'complete')

    def test_leader_exit_does_not_claim_success_with_owned_children(self):
        code = ('import subprocess,sys,time; '
                'subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                'time.sleep(.2)')
        result = build_process.run([sys.executable, '-c', code], self.root,
                                   self.root/'orphan.log', dict(os.environ),
                                   idle_timeout=10, poll_interval=.03)
        self.assertEqual(result['state'], 'supervision_failed')
        self.assertEqual(result['raw_exit_code'], 0)
        self.assertEqual(result['failure_type'], 'OwnedChildrenAfterLeaderExit')
        self.assertEqual(result['cleanup']['state'], 'complete')

    def test_macos_pending_ps_name_never_resets_build_progress(self):
        original = subprocess.Popen
        children = []
        samples = [0]
        def spawn(*args, **kwargs):
            child = original(*args, **kwargs);children.append(child);return child
        def sample(*args, **kwargs):
            if not children or children[0].poll() is not None:
                return subprocess.CompletedProcess(args, 0, '', '')
            pid = children[0].pid;samples[0] += 1
            # A verifier's ps observer is visible to the outer stage sampler.
            stdout = (f'{pid} {os.getpid()} {pid} S 0:00.00 Python\n'
                      f'{100000+samples[0]} {pid} {pid} R 0:00.01 (ps)\n')
            return subprocess.CompletedProcess(args, 0, stdout, '')
        with mock.patch.object(build_process.subprocess, 'Popen', side_effect=spawn), \
             mock.patch.object(build_process.subprocess, 'run', side_effect=sample):
            result = build_process.run([sys.executable, '-c', 'import time; time.sleep(30)'],
                                       self.root, self.root/'sampling.log', dict(os.environ),
                                       idle_timeout=.06, poll_interval=.02, exclude_root_cpu=True)
        self.assertEqual(result['state'], 'stalled')
        self.assertLess(result['raw_exit_code'], 0)
        self.assertEqual(result['cleanup']['state'], 'complete')

    def test_observer_group_exit_cannot_cancel_detached_worker(self):
        snapshot = self.root/'snapshot';snapshot.mkdir()
        report = self.root/'report.json'
        marker = self.root/'worker-finished'
        worker = self.root/'worker.py'
        worker.write_text('import pathlib,time\ntime.sleep(.4)\npathlib.Path('+repr(str(marker))+').write_text("done")\nprint("worker finished",flush=True)\n')
        wrapper = ('import importlib.util,pathlib,sys,time\n'
                   'sys.path.insert(0,'+repr(str(SCRIPTS))+')\n'
                   'spec=importlib.util.spec_from_file_location("launch_fixture",'+repr(str(SCRIPTS/'release-code.py'))+')\n'
                   'module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n'
                   'module.__file__='+repr(str(worker))+'\n'
                   'module.source_snapshot.check=lambda _: {"snapshot_id":"fixture"}\n'
                   'module.start_pipeline(pathlib.Path('+repr(str(snapshot))+'),pathlib.Path('+repr(str(report))+'))\n'
                   'time.sleep(30)\n')
        launch_path = self.root/'report-launch.json'
        observer = subprocess.Popen([sys.executable, '-c', wrapper], stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, start_new_session=True)
        worker_pid = None
        try:
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                try:
                    launch = json.loads(launch_path.read_text())
                    if launch.get('state') == 'submitted':
                        worker_pid = launch['worker_pid'];break
                except (OSError, ValueError):
                    pass
                time.sleep(.01)
            self.assertIsNotNone(worker_pid)
            self.assertNotEqual(os.getpgid(worker_pid), os.getpgid(observer.pid))
            os.killpg(observer.pid, signal.SIGTERM);observer.wait(timeout=5)
            deadline = time.monotonic()+10
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertEqual(marker.read_text(), 'done')
            self.assertIn('worker finished', (self.root/'report-driver.log').read_text())
        finally:
            if observer.poll() is None:
                os.killpg(observer.pid, signal.SIGKILL);observer.wait(timeout=5)
            if worker_pid:
                try:
                    os.killpg(worker_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


if __name__ == '__main__':
    unittest.main()

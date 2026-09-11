"""Opt-in macOS probe. Runs only a disposable counter job, never the real updater."""
import importlib.util
import os
import pathlib
import plistlib
import re
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

SPEC = importlib.util.spec_from_file_location('oneshot_probe_launcher', pathlib.Path(__file__).resolve().parents[1]/'launch-code-upgrade.py')
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


@unittest.skipUnless(sys.platform == 'darwin' and os.environ.get('REMOTE_HOSTS_LAUNCHD_PROBE') == '1',
                     'requires explicit disposable macOS launchd probe opt-in')
class LiveOneShotTest(unittest.TestCase):
    def test_exited_counter_job_is_not_restarted(self):
        label = 'com.remote-hosts.test.oneshot.'+uuid.uuid4().hex
        service = 'gui/%d/%s' % (os.getuid(), label)
        with tempfile.TemporaryDirectory(prefix='remote-hosts-oneshot-fixture-') as tmp:
            directory = pathlib.Path(tmp)
            script = directory/'counter.py'
            script.write_text('import pathlib\np=pathlib.Path(__file__).with_name("runs.txt")\nwith p.open("a") as f: f.write("run\\n")\n')
            plist = launcher.make_plist(label, pathlib.Path(sys.executable).resolve(), script,
                                       directory/'unused-candidate', 'a'*64, '0.3.1', directory/'unused-result', directory)
            job = directory/'job.plist'
            job.write_bytes(plistlib.dumps(plist))
            job.chmod(0o600)
            result = subprocess.run(['launchctl', 'bootstrap', 'gui/%d' % os.getuid(), str(job)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, 'disposable probe bootstrap failed')
            try:
                marker = directory/'runs.txt'
                deadline = time.monotonic()+15
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.1)
                self.assertEqual(marker.read_text(), 'run\n')
                time.sleep(12)  # Longer than the old updater's 10-second throttle.
                observed = subprocess.run(['launchctl', 'print', service], capture_output=True, text=True, timeout=5)
                self.assertEqual(observed.returncode, 0)
                fields = dict(re.findall(r'^\t(runs|last exit code) = ([^\n]+)$', observed.stdout, re.MULTILINE))
                self.assertEqual(marker.read_text(), 'run\n', 'one-shot job unexpectedly ran again')
                self.assertEqual(fields.get('runs'), '1')
                self.assertEqual(fields.get('last exit code'), '0')
                print('disposable launchd probe: runs=1, exit=0, no re-entry after 12 seconds', flush=True)
            finally:
                removed = subprocess.run(['launchctl', 'bootout', service], capture_output=True, timeout=10)
                self.assertEqual(removed.returncode, 0, 'disposable probe cleanup failed: '+label)


if __name__ == '__main__':
    unittest.main()

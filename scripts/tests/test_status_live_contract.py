"""Exercise the status client without a browser, network or extra npm dependencies."""
import pathlib
import shutil
import subprocess
import unittest


class StatusClientTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is a build-test dependency, not an Agent runtime requirement')
    def test_read_only_refresh_and_failure_contract(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        result = subprocess.run(
            ['node', '--test', str(root / 'crates/remote-hosts-code/tests/status_live_test.cjs')],
            capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()

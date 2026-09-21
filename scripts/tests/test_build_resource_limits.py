"""Builder limits are tested with a fake resource API, never global host changes."""
import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location('resource_limit_runner', SCRIPTS/'release-code.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class BuildResourceLimitTests(unittest.TestCase):
    def api(self, before, after=None):
        return types.SimpleNamespace(RLIMIT_NOFILE=7, RLIM_INFINITY=-1,
            getrlimit=mock.Mock(side_effect=[before, after or before]), setrlimit=mock.Mock())

    def test_launchd_default_is_corrected_for_child_builds(self):
        api = self.api((256, -1), (4096, -1))
        result = runner.prepare_build_limits(api, 'darwin')
        api.setrlimit.assert_called_once_with(7, (4096, -1))
        self.assertEqual(result['after_soft'], 4096)
        self.assertFalse(result['global_limits_changed'])

    def test_larger_limit_is_not_lowered(self):
        api = self.api((8192, 16384))
        self.assertEqual(runner.prepare_build_limits(api, 'darwin')['after_soft'], 8192)
        api.setrlimit.assert_not_called()

    def test_unlimited_soft_limit_is_preserved(self):
        api = self.api((-1, -1))
        self.assertEqual(runner.prepare_build_limits(api, 'darwin')['after_soft'], -1)
        api.setrlimit.assert_not_called()

    def test_insufficient_hard_limit_fails_before_compilation(self):
        api = self.api((256, 1024))
        with self.assertRaisesRegex(ValueError, 'hard limit below 4096'):
            runner.prepare_build_limits(api, 'darwin')
        api.setrlimit.assert_not_called()

    def test_limit_adjustment_failure_is_not_hidden(self):
        api = self.api((256, -1))
        api.setrlimit.side_effect = OSError('synthetic denied adjustment')
        with self.assertRaises(OSError):
            runner.prepare_build_limits(api, 'darwin')

    def test_result_is_read_back_not_assumed(self):
        api = self.api((256, -1))
        with self.assertRaisesRegex(ValueError, 'unconfirmed'):
            runner.prepare_build_limits(api, 'darwin')

    def test_other_platforms_are_untouched(self):
        api = self.api((256, 1024))
        self.assertEqual(runner.prepare_build_limits(api, 'linux')['state'], 'not_required')
        api.getrlimit.assert_not_called()
        api.setrlimit.assert_not_called()


if __name__ == '__main__':
    unittest.main()

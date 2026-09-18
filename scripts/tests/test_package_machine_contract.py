"""Release packaging must consume the native Rust contract instead of copied protocol constants."""
import importlib.util
import json
import pathlib
import stat
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    'package_code_release', pathlib.Path(__file__).resolve().parents[1] / 'package-code-release.py')
package = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(package)


class MachineContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def binary(self, payload):
        path = self.root / 'candidate'
        path.write_text(
            '#!' + sys.executable + '\n'
            'import json,sys\n'
            'assert sys.argv[1:] == ["release-manifest"]\n'
            'print(json.dumps(' + repr(payload) + '))\n'
        )
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return path

    def valid(self):
        return {
            'version': '0.10.4',
            'machine_contract_protocol': 1,
            'tool_count': 23,
            'tool_schema_revision': 'a' * 64,
            'tools_sha256': 'a' * 64,
            'terminal_observation_protocol': 2,
            'request_receipt_protocol': 1,
        }

    def test_contract_is_read_from_candidate_binary(self):
        expected = self.valid()
        self.assertEqual(package.machine_contract(self.binary(expected), '0.10.4'), expected)

    def test_session_specific_or_mismatched_contract_is_rejected(self):
        for mutate in (
            lambda value: value.update(host_schema_status='current'),
            lambda value: value.update(version='0.10.3'),
            lambda value: value.update(tools_sha256='b' * 64),
            lambda value: value.update(tool_count=0),
        ):
            value = self.valid()
            mutate(value)
            with self.assertRaises(ValueError):
                package.machine_contract(self.binary(value), '0.10.4')


if __name__ == '__main__':
    unittest.main()

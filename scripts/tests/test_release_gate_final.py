"""The authoritative finished verifier replaces a stale sampled progress tick."""
import contextlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location('release_gate_runner', SCRIPTS/'release-code.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class FinalGateTests(unittest.TestCase):
    def test_fast_verification_cannot_finish_with_a_running_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root/'snapshot'
            crate = snapshot/'crates/remote-hosts-code'
            crate.mkdir(parents=True)
            (crate/'Cargo.toml').write_text('[package]\nversion="0.0.1"\n')
            checkout = root/'checkout'
            checkout.mkdir()
            manifest = {'snapshot_id': 'a'*64, 'source_inputs': {}}
            proof = {'state':'passed', 'functional_tests': {'passed':1, 'failed':0}}

            @contextlib.contextmanager
            def lease(*_):
                yield checkout

            def finish_stage(name, args, execution, directory, report, state, env, **kwargs):
                self.assertEqual(name, 'verification')
                state['verification_gate'] = 'running'
                (directory/'verification.json').write_text(json.dumps(proof))

            with mock.patch.object(runner.source_snapshot, 'check', return_value=manifest), \
                 mock.patch.object(runner.source_snapshot, 'verifier', return_value=types.SimpleNamespace(receipt_current=lambda *_: True)), \
                 mock.patch.object(runner.build_slot, 'lease', side_effect=lease), \
                 mock.patch.object(runner.build_slot, 'synchronize', return_value={}), \
                 mock.patch.object(runner.subprocess, 'check_output', return_value='synthetic compiler'), \
                 mock.patch.object(runner, 'run_stage', side_effect=finish_stage):
                report = root/'result.json'
                value = runner.run_pipeline(snapshot, report, root/'slot', verify_only=True)
            self.assertEqual(value['state'], 'passed')
            self.assertEqual(value['verification_gate'], 'passed')
            self.assertEqual(json.loads(report.read_text())['verification_gate'], 'passed')


if __name__ == '__main__':
    unittest.main()

"""Verification evidence tests run tiny disposable child processes, never Cargo."""
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location('source_check', pathlib.Path(__file__).resolve().parents[1]/'check-code-source.py')
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class SourceEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        for directory in ('crates/remote-hosts-code/src', 'crates/dependency/src', 'scripts/tests'):
            (self.root/directory).mkdir(parents=True)
        (self.root/'Cargo.toml').write_text('[workspace]\n')
        (self.root/'Cargo.lock').write_text('# fixture\n')
        (self.root/'crates/remote-hosts-code/Cargo.toml').write_text('[package]\nversion="0.3.1"\n')
        (self.root/'crates/remote-hosts-code/src/lib.rs').write_text('// fixture\n')
        self.dependency = self.root/'crates/dependency/src/lib.rs'
        self.dependency.write_text('// dependency\n')
        self.report = self.root/'docs/proof.json'

    def tearDown(self):
        self.tmp.cleanup()

    def gates(self):
        return [(name, [sys.executable, '-c', 'print('+repr(
            'test result: ok. 2 passed; 0 failed; 0 ignored;' if name == 'rust_tests' else
            'Ran 2 tests in 0.001s\n\nOK' if name == 'python_tests' else 'ok')+')'])
                for name in ('fmt', 'clippy', 'rust_tests', 'python_tests', 'workspace')]

    def run_check(self, gates=None, timeout=5):
        with contextlib.redirect_stdout(io.StringIO()):
            return checker.run_verification(self.root, self.report, gates if gates is not None else self.gates(), timeout)

    def test_unchanged_inputs_have_complete_machine_receipt(self):
        result = self.run_check()
        self.assertEqual(result['state'], 'passed')
        self.assertEqual(result['functional_tests']['passed'], 4)
        self.assertEqual(result['functional_tests']['rust_executed'], 2)
        self.assertEqual(result['functional_tests']['python_executed'], 2)
        self.assertTrue(result['functional_tests']['evidence_complete'])
        self.assertTrue(checker.receipt_current(result, self.root))
        self.assertEqual(json.loads(self.report.read_text()), result)
        for entry in result['checks'].values():
            self.assertIsInstance(entry['command'], list)
            self.assertEqual(entry['working_directory'], str(self.root.resolve()))
            self.assertEqual(checker.digest(self.root/entry['log']), entry['sha256'])

    def test_native_build_gates_have_explicit_bounded_compile_budgets(self):
        observed = {}
        commands = {}
        def fake_run(argv, root, path, env, timeout):
            observed[path.stem] = timeout
            commands[path.stem] = list(argv)
            path.write_text('test result: ok. 2 passed; 0 failed; 0 ignored;' if path.stem == 'rust_tests' else
                            'Ran 2 tests in 0.001s\n\nOK' if path.stem == 'python_tests' else 'ok')
            return {'state':'finished', 'exit_code':0}
        with mock.patch.object(checker, 'run_command', side_effect=fake_run), contextlib.redirect_stdout(io.StringIO()):
            proof = checker.run_verification(self.root, self.report)
        self.assertEqual(observed, {'fmt':900, 'clippy':900, 'rust_tests':2700, 'python_tests':900, 'workspace':1800})
        self.assertTrue(checker.receipt_current(proof, self.root))
        self.assertEqual(proof['checks']['rust_tests']['timeout_seconds'], 2700)
        # Serialize independent cases, not the actors inside each concurrency test.
        # Preserve both packages, all normal tests, and collection of every failure.
        self.assertEqual(commands['rust_tests'], [
            'cargo', 'test', '-p', 'remote-hosts-code', '-p', 'remote-hosts-mcp',
            '-p', 'remote-hosts-release', '-p', 'remote-hosts-token-output',
            '--locked', '--no-fail-fast', '--', '--test-threads=1', '--color', 'never'])

    def test_existing_receipt_is_rejected_before_executing_commands(self):
        self.report.parent.mkdir()
        self.report.write_text('original receipt')
        with mock.patch.object(checker.subprocess, 'Popen') as process:
            with self.assertRaises(FileExistsError):
                self.run_check()
        process.assert_not_called()
        self.assertEqual(self.report.read_text(), 'original receipt')

    def test_dependency_edits_and_new_files_invalidate_previous_success(self):
        result = self.run_check()
        self.dependency.write_text('// changed dependency')
        self.assertFalse(checker.receipt_current(result, self.root))
        self.dependency.write_text('// dependency\n')
        self.assertTrue(checker.receipt_current(result, self.root))
        added = self.dependency.with_name('added.rs')
        added.write_text('// newly added unverified file')
        self.assertFalse(checker.receipt_current(result, self.root))
        added.unlink()
        self.dependency.unlink()
        self.assertFalse(checker.receipt_current(result, self.root))

    def test_source_change_during_gate_produces_stale_not_passed(self):
        code = 'import pathlib; pathlib.Path('+repr(str(self.dependency))+').write_text("changed")'
        result = self.run_check([('fmt', [sys.executable, '-c', code])])
        self.assertEqual(result['state'], 'stale')
        self.assertIn('crates/dependency/src/lib.rs', result['changed_inputs'])
        self.assertFalse(result['source_inputs_unchanged'])

    def test_python_failures_and_errors_are_counted(self):
        logs = self.root/'logs'
        logs.mkdir()
        (logs/'rust_tests.log').write_text('test result: FAILED. 2 passed; 1 failed; 0 ignored;\n')
        (logs/'python_tests.log').write_text('Ran 5 tests in 0.02s\n\nFAILED (failures=2, errors=1)\n')
        count = checker.summarize({'rust_tests': {'exit_code': 1}, 'python_tests': {'exit_code': 1}}, logs)
        self.assertEqual(count['failed'], 4)
        self.assertEqual(count['python_failed'], 3)
        self.assertEqual(count['passed'], 4)
        self.assertFalse(count['test_gates_completed_successfully'])

    def test_zero_selected_tests_cannot_be_reported_as_verified(self):
        gates = [
            ('rust_tests', [sys.executable, '-c', 'print("test result: ok. 0 passed; 0 failed; 0 ignored;")']),
            ('python_tests', [sys.executable, '-c', 'print("Ran 0 tests in 0.001s\\n\\nOK")']),
        ]
        result = self.run_check(gates)
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['failure_type'], 'IncompleteTestEvidence')
        self.assertFalse(result['functional_tests']['evidence_complete'])
        self.assertEqual(result['functional_tests']['rust_executed'], 0)
        self.assertEqual(result['functional_tests']['python_executed'], 0)
        self.assertFalse(checker.receipt_current(result, self.root))

    def test_all_ignored_or_skipped_tests_never_count_as_executed(self):
        gates = [
            ('rust_tests', [sys.executable, '-c', 'print("test result: ok. 0 passed; 0 failed; 3 ignored;")']),
            ('python_tests', [sys.executable, '-c', 'print("Ran 2 tests in 0.001s\\n\\nOK (skipped=2)")']),
        ]
        result = self.run_check(gates)
        count = result['functional_tests']
        self.assertEqual(result['state'], 'failed')
        self.assertEqual((count['rust_selected'], count['python_selected']), (3, 2))
        self.assertEqual((count['rust_executed'], count['python_executed']), (0, 0))
        self.assertFalse(count['evidence_complete'])

    def test_exit_zero_does_not_hide_failed_or_truncated_test_evidence(self):
        logs = self.root/'logs'; logs.mkdir()
        (logs/'rust_tests.log').write_text('test result: FAILED. 1 passed; 1 failed; 0 ignored;\n')
        (logs/'python_tests.log').write_text('Ran 1 test in 0.01s\n\nOK\n')
        checks = {name: {'state':'finished', 'exit_code':0, 'output_complete':True}
                  for name in ('rust_tests', 'python_tests')}
        self.assertFalse(checker.summarize(checks, logs)['evidence_complete'])
        (logs/'rust_tests.log').write_text('test result: ok. 1 passed; 0 failed; 0 ignored;\n')
        checks['rust_tests']['output_truncated'] = True
        self.assertFalse(checker.summarize(checks, logs)['evidence_complete'])
        checks['rust_tests']['output_truncated'] = False
        checks['python_tests']['output_complete'] = False
        self.assertFalse(checker.summarize(checks, logs)['evidence_complete'])

    def test_failed_gate_is_saved_and_stops_following_commands(self):
        sentinel = self.root/'should-not-exist'
        gates = [('fmt', [sys.executable, '-c', 'raise SystemExit(7)']),
                 ('clippy', [sys.executable, '-c', 'open('+repr(str(sentinel))+',"w").close()'])]
        result = self.run_check(gates)
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['checks']['fmt']['exit_code'], 7)
        self.assertNotIn('clippy', result['checks'])
        self.assertFalse(sentinel.exists())

    def test_missing_interpreter_has_a_durable_failure_receipt(self):
        result = self.run_check([('fmt', [str(self.root/'missing-command')])])
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['checks']['fmt']['state'], 'start_or_collection_failed')
        self.assertEqual(result['checks']['fmt']['failure_type'], 'FileNotFoundError')

    @unittest.skipUnless(os.name == 'posix', 'process group cleanup is a POSIX guarantee')
    def test_timeout_stops_owned_child_group_and_preserves_log(self):
        sentinel = self.root/'orphan-wrote-file'
        child = 'import time,pathlib; time.sleep(0.8); pathlib.Path('+repr(str(sentinel))+').touch()'
        ready = self.root/'child-group-created'
        parent = 'import subprocess,sys,time,pathlib; subprocess.Popen([sys.executable,"-c",'+repr(child)+']); print("started",flush=True); pathlib.Path('+repr(str(ready))+').touch(); time.sleep(30)'
        # Synchronize process startup before measuring the cancellation window.
        # Under compiler load, interpreter startup can exceed 200ms; an empty
        # startup log is not evidence that process-group cancellation is broken.
        original_popen = checker.subprocess.Popen
        def started_process(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            deadline = time.monotonic()+5
            while not ready.exists() and time.monotonic()<deadline:
                time.sleep(0.01)
            if not ready.exists():
                import signal
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
                raise RuntimeError('fixture child did not start')
            return process
        with mock.patch.object(checker.subprocess, 'Popen', side_effect=started_process):
            result = self.run_check([('fmt', [sys.executable, '-c', parent])], timeout=0.2)
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['checks']['fmt']['state'], 'timed_out')
        self.assertIn('started', (self.root/result['checks']['fmt']['log']).read_text())
        time.sleep(0.9)
        self.assertFalse(sentinel.exists())


if __name__ == '__main__':
    unittest.main()

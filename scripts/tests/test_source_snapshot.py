"""Snapshot regression tests: temporary files, no compiler/deployment required."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
def module(name, file):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS/file)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value
snapshot = module('snapshot', 'source_snapshot.py')
checker = module('snapshot_checker', 'check-code-source.py')

class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)/'repository'
        self.dest = pathlib.Path(self.tmp.name)/'captured'
        (self.root/'crates/remote-hosts-code/src').mkdir(parents=True)
        (self.root/'scripts').mkdir()
        (self.root/'Cargo.toml').write_text('[workspace]\n')
        (self.root/'Cargo.lock').write_text('# fixture\n')
        (self.root/'crates/remote-hosts-code/Cargo.toml').write_text('[package]\nversion="0.3.2"\n')
        self.source = self.root/'crates/remote-hosts-code/src/lib.rs'
        self.source.write_text('// stable\n')
        self.script = self.root/'scripts/check-code-source.py'
        self.script.write_text('# fixture input, not executed\n')
    def tearDown(self):
        self.tmp.cleanup()
    def capture(self):
        return snapshot.create(self.root, self.dest, checker.inputs)
    def test_editor_changes_do_not_mutate_independent_snapshot(self):
        proof = self.capture()
        copied = self.dest/self.source.relative_to(self.root)
        self.assertNotEqual(self.source.stat().st_ino, copied.stat().st_ino)
        self.source.write_text('// new editor work\n')
        self.assertEqual(copied.read_text(), '// stable\n')
        self.assertEqual(snapshot.check(self.dest, checker.inputs)['snapshot_id'], proof['snapshot_id'])
        self.assertNotEqual(checker.inputs(self.root), proof['source_inputs'])
    def test_input_change_during_capture_does_not_publish(self):
        original = snapshot.copy_checked
        def racing(src, dst, expected):
            size = original(src, dst, expected)
            self.source.write_text('// racing edit\n')
            return size
        with mock.patch.object(snapshot, 'copy_checked', side_effect=racing):
            with self.assertRaises(ValueError): self.capture()
        self.assertFalse(self.dest.exists())
    def test_existing_snapshot_is_not_overwritten(self):
        self.capture()
        with self.assertRaises(FileExistsError): self.capture()
        snapshot.check(self.dest, checker.inputs)
    def test_new_and_deleted_inputs_break_snapshot_identity(self):
        self.capture()
        added = self.dest/'scripts/new.py'; added.write_text('# unverified\n')
        with self.assertRaises(ValueError): snapshot.check(self.dest, checker.inputs)
        added.unlink(); snapshot.check(self.dest, checker.inputs)
        (self.dest/'Cargo.lock').unlink()
        with self.assertRaises(ValueError): snapshot.check(self.dest, checker.inputs)
    @unittest.skipUnless(os.name=='posix', 'POSIX file identity')
    def test_executable_mode_is_captured_and_rechecked(self):
        self.script.chmod(0o755); proof=self.capture()
        self.assertIn('scripts/check-code-source.py', proof['executable_files'])
        (self.dest/'scripts/check-code-source.py').chmod(0o644)
        with self.assertRaises(ValueError): snapshot.check(self.dest, checker.inputs)
    @unittest.skipUnless(os.name=='posix', 'symlink fixture')
    def test_linked_directory_is_not_silently_omitted(self):
        elsewhere=pathlib.Path(self.tmp.name)/'external'; elsewhere.mkdir()
        (self.root/'crates/external').symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(ValueError): self.capture()
    def test_root_migrations_are_copied_and_new_migration_invalidates_identity(self):
        migration = self.root/'migrations/0001.sql'
        migration.parent.mkdir()
        migration.write_text('CREATE TABLE test(id INTEGER);')
        proof = self.capture()
        self.assertIn('migrations/0001.sql', proof['source_inputs'])
        self.assertEqual((self.dest/'migrations/0001.sql').read_text(), migration.read_text())
        (self.dest/'migrations/0002.sql').write_text('ALTER TABLE test ADD name TEXT;')
        with self.assertRaises(ValueError): snapshot.check(self.dest, checker.inputs)

    def test_build_outputs_credentials_and_reports_are_not_copied(self):
        for file in ('target/debug/private.bin','dist/old.bin','.git/config','agent.json'):
            p=self.root/file;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('not a source input')
        proof=self.capture()
        for file in ('target/debug/private.bin','dist/old.bin','.git/config','agent.json'):
            self.assertNotIn(file,proof['source_inputs']);self.assertFalse((self.dest/file).exists())
    def test_tests_can_run_on_copy_while_original_tree_changes(self):
        import sys
        self.capture()
        code='import pathlib; pathlib.Path('+repr(str(self.source))+').write_text("edited outside snapshot")'
        gates=[('fmt',[sys.executable,'-c',code])]
        with contextlib.redirect_stdout(io.StringIO()):
            proof=checker.run_verification(self.dest,self.dest/'proof.json',gates,5)
        self.assertEqual(proof['state'],'passed')
        self.assertTrue(proof['source_inputs_unchanged'])
        self.assertNotEqual(proof['source_inputs'],checker.inputs(self.root))
        snapshot.check(self.dest,checker.inputs)

if __name__=='__main__': unittest.main()

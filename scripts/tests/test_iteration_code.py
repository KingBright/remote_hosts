"""Pure policy tests for the iteration driver. No Git push or deployment."""
import importlib.util
import pathlib
import sys
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS/'iteration-code.py'
spec = importlib.util.spec_from_file_location('iteration_code', SCRIPT)
iteration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(iteration)


class IterationPolicyTests(unittest.TestCase):
    def test_docs_only_uses_lightest_gate(self):
        self.assertEqual(iteration.classify(['README.md','docs/product/NEXT.md']), 'docs')

    def test_skill_only_uses_lightest_gate(self):
        self.assertEqual(iteration.classify([
            'skills/remote-hosts-agent/SKILL.md',
            'skills/remote-hosts-agent/references/setup-and-runtime.md',
        ]), 'docs')

    def test_release_python_does_not_force_rust_rebuild(self):
        self.assertEqual(iteration.classify(['scripts/check-code-gateway.py',
                                             'scripts/tests/test_acceptance_scope_032.py',
                                             'docs/releases/0.7.1/RELEASE.md',
                                             'skills/remote-hosts-agent/SKILL.md']), 'release_python')

    def test_runtime_change_requires_full_release_pipeline(self):
        for paths in (['crates/remote-hosts-code/src/agent.rs'], ['Cargo.lock'],
                      ['migrations/0001_initial.sql'], ['scripts/a.py','crates/x/src/lib.rs']):
            self.assertEqual(iteration.classify(paths), 'runtime', paths)

    def test_empty_tree_is_not_an_iteration(self):
        self.assertEqual(iteration.classify([]), 'clean')

    def test_fingerprint_is_stable_for_same_files(self):
        import tempfile
        old_root=iteration.ROOT
        try:
            with tempfile.TemporaryDirectory() as tmp:
                iteration.ROOT=pathlib.Path(tmp)
                (iteration.ROOT/'a').write_text('one')
                first=iteration.fingerprint(['a','missing'])
                self.assertEqual(first, iteration.fingerprint(['a','missing']))
                (iteration.ROOT/'a').write_text('two')
                self.assertNotEqual(first, iteration.fingerprint(['a','missing']))
        finally:
            iteration.ROOT=old_root

    def test_validated_is_distinct_from_fully_passed_policy(self):
        self.assertNotEqual('validated', 'passed')

    def test_commit_audit_rejects_release_blobs_and_bearer_urls(self):
        import tempfile
        old_root=iteration.ROOT
        try:
            with tempfile.TemporaryDirectory() as tmp:
                iteration.ROOT=pathlib.Path(tmp)
                (iteration.ROOT/'artifact.tgz').write_bytes(b'x')
                with self.assertRaisesRegex(ValueError, 'artifact'):
                    iteration.audit_commit_scope(['artifact.tgz'])
                (iteration.ROOT/'Cargo.lock').write_text('version = 4')
                iteration.audit_commit_scope(['Cargo.lock'])
                (iteration.ROOT/'publish.lock').write_text('temporary')
                with self.assertRaisesRegex(ValueError, 'artifact'):
                    iteration.audit_commit_scope(['publish.lock'])
                (iteration.ROOT/'evidence.json').write_text(
                    '{"download_url":"https://example.test/files/'+'a'*64+'/x"}')
                with self.assertRaisesRegex(ValueError, 'bearer'):
                    iteration.audit_commit_scope(['evidence.json'])
                (iteration.ROOT/'safe.md').write_text('no private capability here')
                iteration.audit_commit_scope(['safe.md'])
        finally:
            iteration.ROOT=old_root

    def test_publish_status_is_compact_and_does_not_require_secrets(self):
        import json, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp)
            (root/'deployment.json').write_text(json.dumps({
                'version':'1.2.3','state':'partial','phase':'finished','all_targets_accepted':False,
                'gateway':{'state':'upgraded','version':'1.2.3','private':'discard'},
                'agents':{'d':{'name':'Mac','state':'needs_recovery','phase':'acceptance','secret':'discard'}}}))
            value=iteration.compact_publish_status(root)
            self.assertEqual(value['gateway'], {'state':'upgraded','version':'1.2.3'})
            self.assertEqual(value['agents']['d'], {'name':'Mac','state':'needs_recovery','phase':'acceptance'})
            self.assertNotIn('private', json.dumps(value))
            self.assertNotIn('secret', json.dumps(value))


if __name__ == '__main__':
    unittest.main()

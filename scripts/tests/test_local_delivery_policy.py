"""Local delivery is authoritative; disabling cloud builds never removes tests."""
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]

class LocalDeliveryPolicyTests(unittest.TestCase):
    def test_no_automatic_github_build_workflows(self):
        directory = ROOT / '.github' / 'workflows'
        self.assertEqual(sorted(directory.glob('*.yml')), [])
        self.assertEqual(sorted(directory.glob('*.yaml')), [])

    def test_local_pipeline_still_verifies_and_builds_all_platforms(self):
        code = (ROOT / 'scripts' / 'release-code.py').read_text()
        for value in ('scripts/check-code-source.py', 'x86_64-unknown-linux-musl',
                      'x86_64-pc-windows-msvc', 'zigbuild', 'xwin', 'package-code-release.py'):
            self.assertIn(value, code)
        for value in ('gh run', 'gh workflow', 'actions/runs', 'api.github.com'):
            self.assertNotIn(value, code)

    def test_direct_distribution_has_no_github_artifact_dependency(self):
        code = (ROOT / 'scripts' / 'fleet-upgrade.py').read_text()
        self.assertIn('gateway_upgrade_ssh(config,package,manifest,args.version,directory)', code)
        self.assertIn("client.tool('file_download'", code)
        self.assertIn("client.tool('file_upload'", code)
        self.assertNotIn('releases/download/', code)
        self.assertNotIn('gh release', code)
        self.assertNotIn('actions/runs', code)

if __name__ == '__main__':
    unittest.main()

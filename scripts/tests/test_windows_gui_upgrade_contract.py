"""Controller contract regression for the native Windows GUI-subsystem agent.
Native process execution remains a separate Windows acceptance requirement.
"""
import pathlib
import unittest


class WindowsGuiUpgradeContract(unittest.TestCase):
    def test_gui_version_probe_is_a_pipeline_and_checks_native_exit(self):
        source = (pathlib.Path(__file__).resolve().parents[1] /
                  'upgrade-code-agent-windows.ps1').read_text()
        self.assertIn('$candidateVersion = (& $Candidate --version | Out-String).Trim()', source)
        self.assertIn('$LASTEXITCODE -ne 0 -or $candidateVersion -ne', source)
        self.assertNotIn('(& $Candidate --version).Trim()', source)
        probe = source.index('$candidateVersion =')
        acquire = source.index("Maintenance-Call 'acquire'")
        swap = source.index('Copy-Item -LiteralPath $Candidate')
        self.assertLess(probe, acquire)
        self.assertLess(acquire, swap)


if __name__ == '__main__':
    unittest.main()

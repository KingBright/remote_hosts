import os
import pathlib
import stat
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "remote-hosts-systemd-service"


class SystemdServiceTests(unittest.TestCase):
    def test_render_is_user_scoped_private_and_loopback_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            home = root / "home"
            home.mkdir()
            env = os.environ.copy()
            env.update(
                REMOTE_HOSTS_ALLOW_NON_LINUX_TEST="1",
                HOME=str(home),
                XDG_CONFIG_HOME=str(root / "config"),
                XDG_DATA_HOME=str(root / "data"),
                XDG_STATE_HOME=str(root / "state"),
            )
            subprocess.run([str(SCRIPT), "render"], cwd=ROOT, env=env, check=True)
            config = root / "config" / "remote-hosts" / "service.env"
            vault = root / "config" / "remote-hosts" / "vault-master-password"
            api = root / "config" / "systemd" / "user" / "remote-hosts-api.service"
            connector = root / "config" / "systemd" / "user" / "remote-hosts-connector.service"
            api_wrapper = root / "data" / "remote-hosts" / "run" / "remote-hosts-api"
            connector_wrapper = root / "data" / "remote-hosts" / "run" / "remote-hosts-connector"
            self.assertIn("REMOTE_HOSTS_BIND=127.0.0.1:8787", config.read_text())
            self.assertEqual(stat.S_IMODE(vault.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(api_wrapper.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE(connector_wrapper.stat().st_mode), 0o755)
            self.assertIn(f"ExecStart={api_wrapper}", api.read_text())
            self.assertIn(f"ExecStart={connector_wrapper}", connector.read_text())
            for unit in (api.read_text(), connector.read_text()):
                self.assertIn("WantedBy=default.target", unit)
                self.assertIn("NoNewPrivileges=true", unit)
                self.assertIn("PrivateTmp=true", unit)
                self.assertNotIn("WantedBy=multi-user.target", unit)


if __name__ == "__main__":
    unittest.main()

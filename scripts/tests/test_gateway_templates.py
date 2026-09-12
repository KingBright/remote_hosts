"""Generic Gateway deployment templates contain no private deployment defaults."""
import importlib.util
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GatewayTemplateTests(unittest.TestCase):
    def test_caddy_template_renders_public_host_and_loopback(self):
        installer = load("gateway_installer_test", "install-code-gateway.py")
        rendered = installer.render(
            SCRIPTS / "code-gateway.caddy",
            {"PUBLIC_HOST": "mcp.example.com", "GATEWAY_BIND": "127.0.0.1:18787"},
        ).decode()
        self.assertIn("mcp.example.com", rendered)
        self.assertIn("reverse_proxy 127.0.0.1:18787", rendered)
        self.assertNotIn("{{", rendered)

    def test_systemd_template_requires_explicit_deployment_values(self):
        installer = load("gateway_service_test", "install-code-gateway.py")
        rendered = installer.render(
            SCRIPTS / "remote-hosts-code-gateway.service",
            {
                "SERVICE_USER": "remote-hosts-code",
                "SERVICE_GROUP": "remote-hosts-code",
                "BINARY_PATH": "/usr/local/bin/remote-hosts-code",
                "CONFIG_PATH": "/etc/remote-hosts-code/gateway.json",
                "STATE_DIR": "/var/lib/remote-hosts-code",
            },
        ).decode()
        self.assertIn("User=remote-hosts-code", rendered)
        self.assertIn("ReadWritePaths=/var/lib/remote-hosts-code", rendered)
        self.assertNotIn("{{", rendered)

    def test_publish_config_requires_gateway_deployment_paths(self):
        publish = load("publish_config_test", "publish-code.py")
        config = {
            "agents": [{
                "device_id": "00000000-0000-0000-0000-000000000001",
                "home": "/home/device-a",
                "root": "/home/device-a/projects",
            }],
            "controller_device_id": "00000000-0000-0000-0000-000000000001",
            "gateway": {
                "ssh_host": "ops@example.test",
                "ssh_port": 22,
                "root": "/srv/remote-hosts-code",
                "binary_path": "/usr/local/bin/remote-hosts-code",
                "config_path": "/etc/remote-hosts-code/gateway.json",
                "backup_root": "/var/lib/remote-hosts-code/releases",
                "service_name": "remote-hosts-code-gateway.service",
                "gateway_bind": "127.0.0.1:18787",
            },
        }
        self.assertIs(publish.validate(config), config)
        del config["gateway"]["binary_path"]
        with self.assertRaises(ValueError):
            publish.validate(config)

    def test_legacy_numeric_uid_wrapper_is_not_public_source(self):
        self.assertFalse((SCRIPTS / "run-code-gateway.py").exists())


if __name__ == "__main__":
    unittest.main()

"""NAS discovery safety tests; synthetic proc tree and mocked public metadata only."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gateway_release as release
import gateway_release_stage as stage
from test_gateway_release_entry import PackageFixture


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.proc = self.root / "proc"
        self.proc.mkdir()
        (self.proc / "123").mkdir()
        self.install = self.root / "install"
        self.install.mkdir()
        self.binary = self.install / "remote-hosts-code"
        self.binary.write_bytes(b"synthetic previous")
        self.identity = {"root": self.install, "binary": self.binary, "pid": 123,
                         "running": True, "installed_sha256": stage.BINARY_IDENTITIES["0.10.25"]}

    def discover(self, *, extra=False, listener=True, deleted=False, cwd=None):
        if extra:
            (self.proc / "124").mkdir()
        def readlink(path):
            return str(cwd or self.install) if str(path).endswith("/cwd") else str(self.binary) + (" (deleted)" if deleted else "")
        with mock.patch.object(stage, "PROC_ROOT", self.proc), \
             mock.patch.object(stage, "NAS_ROOT", self.install), \
             mock.patch.object(stage.os, "readlink", side_effect=readlink), \
             mock.patch.object(stage, "checked_identity", return_value=self.identity), \
             mock.patch.object(stage, "owns_gateway_listener", return_value=listener), \
             mock.patch.object(stage, "nas_public_identity", return_value={"version": "0.10.25"}):
            return stage.discover_nas()

    def test_unique_live_process_listener_and_canonical_directory(self):
        result = self.discover()
        self.assertEqual(result["pid"], 123)
        self.assertEqual(result["discovery"], "verified_entware_process_listener_resource")

    def test_multiple_processes_are_not_guessed(self):
        with self.assertRaises(stage.StageError) as caught:
            self.discover(extra=True)
        self.assertEqual(caught.exception.code, "gateway_service_identity_ambiguous")

    def test_wrong_listener_deleted_binary_and_working_directory_stop(self):
        for kwargs, code in (({"listener": False}, "gateway_listener_identity_mismatch"),
                             ({"deleted": True}, "gateway_executable_deleted"),
                             ({"cwd": self.root}, "gateway_working_directory_mismatch")):
            with self.subTest(code=code), self.assertRaises(stage.StageError) as caught:
                self.discover(**kwargs)
            self.assertEqual(caught.exception.code, code)

    def test_systemctl_unavailable_uses_guarded_discovery(self):
        with mock.patch.object(stage.os, "geteuid", return_value=0), \
             mock.patch.object(stage.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, b"", b"private")), \
             mock.patch.object(stage, "discover_nas", return_value=self.identity) as discover:
            self.assertEqual(stage.identify(), self.identity)
        discover.assert_called_once()

    def test_symlinked_installation_is_rejected_even_under_allowed_alias(self):
        alias = self.root / "alias"
        alias.symlink_to(self.install, target_is_directory=True)
        with mock.patch.object(stage, "NAS_ROOT", alias), \
             mock.patch.object(stage.os, "readlink", return_value=str(alias / "remote-hosts-code")), \
             self.assertRaises(stage.StageError) as caught:
            stage.checked_identity(123)
        self.assertEqual(caught.exception.code, "symlink_in_installation")

    def test_owned_listening_socket_required(self):
        fd = self.proc / "123" / "fd"
        fd.mkdir()
        (fd / "1").symlink_to("socket:[456]")
        net = self.proc / "123" / "net"
        net.mkdir()
        (net / "tcp").write_text("header\n0: 0100007F:4963 00000000:0000 0A 0 0 0 0 0 456\n")
        (net / "tcp6").write_text("header\n")
        with mock.patch.object(stage, "PROC_ROOT", self.proc):
            self.assertTrue(stage.owns_gateway_listener(123))
            (net / "tcp").write_text("header\n0: 0100007F:4963 00000000:0000 0A 0 0 0 0 0 999\n")
            self.assertFalse(stage.owns_gateway_listener(123))

    def test_resource_runtime_and_installed_hash_must_all_match(self):
        metadata = {"resource": stage.PUBLIC_ORIGIN + "/mcp",
                    "authorization_servers": [stage.PUBLIC_ORIGIN]}
        health = dict(stage.RUNTIME_IDENTITIES["0.10.25"], version="0.10.25",
                      wire_protocol=2, file_transfer=True)
        with mock.patch.object(stage, "local_public_json", side_effect=[metadata, health]):
            self.assertEqual(stage.nas_public_identity(self.identity)["version"], "0.10.25")
        for meta, status, identity in (
            (dict(metadata, resource="https://other.invalid/mcp"), health, self.identity),
            (metadata, dict(health, tools_sha256="0"*64), self.identity),
            (metadata, health, dict(self.identity, installed_sha256="0"*64)),
        ):
            with mock.patch.object(stage, "local_public_json", side_effect=[meta, status]), \
                 self.assertRaises(stage.StageError):
                stage.nas_public_identity(identity)

    def test_no_runner_or_unconfirmed_service_never_reports_publication_ready(self):
        for runner, rc, pid in ((None, 0, b"123"), ("/usr/bin/systemd-run", 1, b""),
                                ("/usr/bin/systemd-run", 0, b"0")):
            with mock.patch.object(stage.shutil, "which", return_value=runner), \
                 mock.patch.object(stage.subprocess, "run", return_value=subprocess.CompletedProcess([], rc, pid, b"private")):
                self.assertFalse(stage.publication_controller()["ready"])


    def test_systemd219_property_format_is_supported_without_value(self):
        with mock.patch.object(stage.subprocess, "run",
                return_value=subprocess.CompletedProcess([], 0, b"MainPID=123\n", b"")) as run:
            self.assertEqual(stage.systemctl_main_pid(), 123)
        self.assertNotIn("--value", run.call_args.args[0])

    def test_ambiguous_or_malformed_mainpid_is_not_accepted(self):
        for output in (b"123", b"MainPID=12\nMainPID=13", b"MainPID=private"):
            with mock.patch.object(stage.subprocess, "run",
                    return_value=subprocess.CompletedProcess([], 0, output, b"private")):
                self.assertIsNone(stage.systemctl_main_pid())

    def test_completion_is_atomic_and_original_identity_is_preserved(self):
        plan = dict(PackageFixture(self.root).identity()["plan"], stage_operation_id="owned-stage-01")
        directory = self.root / "receipt"
        directory.mkdir()
        first = stage.stage_completion(directory, plan, write=True)
        self.assertEqual(stage.stage_completion(directory, plan), first)
        self.assertEqual(list(directory.glob(".bundle-stage-complete-*")), [])
        with self.assertRaises(stage.StageError):
            stage.stage_completion(directory, dict(plan, stage_operation_id="other-operation"), write=True)


class PreOAuthTests(unittest.TestCase):
    def test_controller_failure_stops_before_oauth_and_persists_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = PackageFixture(root).identity()
            report = root / "report"
            ssh = mock.Mock()
            ssh.last_outcome = {"exit_code": 0, "category": "connected"}
            ssh.request.return_value = {"publication_controller": {"ready": False}}
            with mock.patch.object(release.sys, "argv", ["gateway_release.py", "--report-dir",
                 str(report), "--execute", "--oauth-browser"]), \
                 mock.patch.object(release.sys.stdin, "isatty", return_value=True), \
                 mock.patch.object(release.sys.stderr, "isatty", return_value=True), \
                 mock.patch.object(release, "verify_release", return_value=identity), \
                 mock.patch.object(release, "OwnerSSH", return_value=ssh), \
                 mock.patch.object(release, "Client") as client, \
                 mock.patch.object(release, "BrowserOAuth") as oauth, \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(release.main(), 2)
            client.assert_not_called()
            oauth.assert_not_called()
            ssh.request.assert_called_once_with("probe", identity["plan"])
            self.assertTrue((report / "gateway-preflight.json").is_file())
            failure = json.loads(output.getvalue())
            self.assertEqual(failure["error_code"], "gateway_publication_controller_unavailable")
            self.assertFalse(failure["import_attempted"])
            self.assertFalse(failure["request_attempted"])


if __name__ == "__main__":
    unittest.main()

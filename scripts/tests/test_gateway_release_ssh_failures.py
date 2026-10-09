"""Isolated SSH/entry failure tests; no network, real key, token or password."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gateway_release as release
from gateway_release_ssh_diagnostics import StderrCodes, classify
from test_gateway_release_entry import PackageFixture


class DiagnosticsTests(unittest.TestCase):
    def test_known_codes_and_sensitive_text_are_never_returned(self):
        for text, expected in (
            (b"Permission denied (publickey). SYNTHETIC_PASSWORD_TOKEN", "authentication_rejected"),
            (b"Host key verification failed. SYNTHETIC_PASSWORD_TOKEN", "host_identity_verification_failed"),
            (b"Connection timed out SYNTHETIC_PASSWORD_TOKEN", "connection_timed_out"),
            (b"Could not resolve hostname SYNTHETIC_PASSWORD_TOKEN", "dns_resolution_failed"),
            (b"SYNTHETIC_PASSWORD_TOKEN", "ssh_failed_unclassified"),
        ):
            with self.subTest(expected=expected):
                value = classify(text, 255)
                self.assertEqual(value["category"], expected)
                self.assertEqual(value["exit_code"], 255)
                self.assertNotIn("SYNTHETIC", json.dumps(value))
                self.assertFalse(value["password_prompt_capture"])

    def test_stream_classifies_split_lines_without_retaining_bytes(self):
        pipe = io.BytesIO(b"x" * 1015 + b"Permission denied (publickey). SYNTHETIC_TOKEN")
        reader = StderrCodes(pipe)
        value = reader.result(255)
        self.assertEqual(value["category"], "authentication_rejected")
        self.assertNotIn("SYNTHETIC", json.dumps(value))
        self.assertFalse(hasattr(reader, "raw_stderr"))

    def test_success_does_not_wait_for_background_master_stderr_eof(self):
        class IdlePipe:
            def read1(self, size):
                time.sleep(0.2)
                return b""
        reader = StderrCodes(IdlePipe())
        start = time.monotonic()
        self.assertEqual(reader.result(0)["category"], "connected")
        self.assertLess(time.monotonic() - start, 0.1)
        reader.thread.join(timeout=1)

    def test_signal_failure_is_not_mislabeled_bad_password(self):
        self.assertEqual(classify(b"", -2)["category"], "interrupted_by_signal")


class SSHTests(unittest.TestCase):
    def test_connect_uses_existing_key_config_without_password_prompt(self):
        ssh = release.OwnerSSH("owner@gateway.example", 222)
        process = mock.Mock()
        process.stderr = io.BytesIO(b"")
        process.wait.return_value = 0
        process.poll.return_value = 0
        with mock.patch.object(release.sys.stdin, "isatty", return_value=True), \
             mock.patch.object(release.sys.stderr, "isatty", return_value=True), \
             mock.patch.object(release.subprocess, "Popen", return_value=process) as popen, \
             mock.patch.object(release.subprocess, "run"):
            ssh.connect()
            args = popen.call_args.args[0]
            self.assertNotIn("-F", args)
            for forbidden in ("IdentityFile=none", "IdentityAgent=none", "PubkeyAuthentication=no"):
                self.assertNotIn(forbidden, args)
            for required in ("BatchMode=yes", "PasswordAuthentication=no",
                             "KbdInteractiveAuthentication=no", "StrictHostKeyChecking=yes"):
                self.assertIn(required, args)
            self.assertIn("222", args)
            self.assertEqual(args[-1], "owner@gateway.example")
            self.assertNotIn("stdin", popen.call_args.kwargs)
            self.assertNotIn("stdout", popen.call_args.kwargs)
            ssh.close()

    def test_failed_connect_preserves_code_without_secret(self):
        ssh = release.OwnerSSH("owner@gateway.example", 222)
        process = mock.Mock()
        process.stderr = io.BytesIO(b"Permission denied (publickey). SYNTHETIC_SECRET")
        process.wait.return_value = 255
        process.poll.return_value = 255
        with mock.patch.object(release.sys.stdin, "isatty", return_value=True), \
             mock.patch.object(release.sys.stderr, "isatty", return_value=True), \
             mock.patch.object(release.subprocess, "Popen", return_value=process), \
             mock.patch.object(release.subprocess, "run"):
            with self.assertRaises(release.PublishError):
                ssh.connect()
            self.assertEqual(ssh.last_outcome["exit_code"], 255)
            self.assertEqual(ssh.last_outcome["category"], "authentication_rejected")
            self.assertNotIn("SYNTHETIC", json.dumps(ssh.last_outcome))
            ssh.close()

    def test_known_remote_preflight_error_is_not_hidden_as_ssh_auth_error(self):
        ssh = release.OwnerSSH("owner@gateway.example", 222)
        ssh.socket = "/synthetic/socket"
        value = subprocess.CompletedProcess([], 1,
            stdout=json.dumps({"state": "blocked", "error_code": "gateway_service_identity_unavailable",
                               "private": "SYNTHETIC_SECRET"}).encode(),
            stderr=b"SYNTHETIC_SECRET")
        with mock.patch.object(release.subprocess, "run", return_value=value):
            with self.assertRaises(release.PublishError) as caught:
                ssh.request("probe", {})
        self.assertEqual(caught.exception.code, "gateway_service_identity_unavailable")
        self.assertEqual(ssh.last_outcome["category"], "gateway_preflight_failed")
        self.assertNotIn("SYNTHETIC", json.dumps(ssh.last_outcome))


class EntryFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = PackageFixture(self.root).identity()
        self.report = self.root / "report"

    def run_failure(self, *, interrupted=False):
        ssh = mock.Mock()
        ssh.last_outcome = {"exit_code": 255, "category": "authentication_rejected",
                            "raw_stderr_retained": False, "password_prompt_capture": False}
        ssh.connect.side_effect = KeyboardInterrupt() if interrupted else release.PublishError("owner_ssh_connection_not_confirmed")
        with contextlib.redirect_stdout(io.StringIO()) as output, \
             mock.patch.object(release.sys, "argv", ["gateway_release.py", "--report-dir",
                 str(self.report), "--execute", "--oauth-browser",
                 "--origin", release.ORIGIN, "--ssh-account", "owner@gateway.example",
                 "--ssh-port", "222", "--installation-root", "/srv/remote-hosts-code"]), \
             mock.patch.object(release.sys.stdin, "isatty", return_value=True), \
             mock.patch.object(release.sys.stderr, "isatty", return_value=True), \
             mock.patch.object(release, "verify_release", return_value=self.identity), \
             mock.patch.object(release, "OwnerSSH", return_value=ssh), \
             mock.patch.object(release, "Client") as client, \
             mock.patch.object(release, "BrowserOAuth") as oauth:
            result = release.main()
        client.assert_not_called()
        oauth.assert_not_called()
        ssh.close.assert_called_once()
        receipt_paths = list(self.report.glob("gateway-release-failure-*.json"))
        self.assertEqual(len(receipt_paths), 1)
        value = json.loads(receipt_paths[0].read_text())
        self.assertEqual(value, json.loads(output.getvalue()))
        self.assertFalse(value["request_attempted"])
        self.assertFalse(value["import_attempted"])
        self.assertEqual(value["entry_phase"], "existing_ssh_connection")
        return result, value

    def test_failed_batch_connection_is_durable_before_oauth(self):
        code, value = self.run_failure()
        self.assertEqual(code, 2)
        self.assertEqual(value["ssh_outcome"]["exit_code"], 255)

    def test_owner_interrupt_is_durable_and_never_reauthenticates(self):
        code, value = self.run_failure(interrupted=True)
        self.assertEqual(code, 130)
        self.assertEqual(value["error_code"], "owner_interrupted")


class TargetConfigurationTests(unittest.TestCase):
    """Explicit deployment identity is checked before any SSH or OAuth action."""

    def test_missing_target_stops_execute_and_observe_before_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = PackageFixture(root).identity()
            for mode in ("--execute", "--observe"):
                with self.subTest(mode=mode), \
                     mock.patch.object(release.sys, "argv", ["gateway_release.py",
                         "--report-dir", str(root / mode[2:]), mode, "--oauth-browser"]), \
                     mock.patch.object(release.sys.stdin, "isatty", return_value=True), \
                     mock.patch.object(release.sys.stderr, "isatty", return_value=True), \
                     mock.patch.object(release, "verify_release", return_value=identity), \
                     mock.patch.object(release, "OwnerSSH") as ssh, \
                     mock.patch.object(release, "Client") as client, \
                     mock.patch.object(release, "BrowserOAuth") as oauth, \
                     contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(release.main(), 2)
                self.assertEqual(json.loads(output.getvalue())["error_code"],
                                 "deployment_target_required")
                ssh.assert_not_called()
                client.assert_not_called()
                oauth.assert_not_called()

    def test_malformed_target_cannot_reach_authentication(self):
        valid = ["https://gateway.example", "owner@gateway.example", 2201,
                 "/srv/remote-hosts-code"]
        for index, value in ((0, "http://gateway.example"),
                             (0, "https://user:password@gateway.example"),
                             (0, "https://gateway.example/path"),
                             (0, "https://gateway.example?query=1"),
                             (0, "https://gateway.example\\\\other"),
                             (1, "-oProxyCommand=bad"), (1, "owner@host\nother"),
                             (2, 0), (2, True), (3, "relative/root"),
                             (3, "/srv/../other"), (3, "/"), (3, "/srv/\x00root")):
            args = list(valid)
            args[index] = value
            with self.subTest(index=index, value=repr(value)), \
                 self.assertRaises((release.PublishError, release.BrowserOAuthError)):
                release.deployment_target(*args)

    def test_one_explicit_ssh_target_and_port_are_used_throughout(self):
        ssh = release.OwnerSSH("owner@gateway.example", 2201)
        process = mock.Mock(stderr=io.BytesIO(b""))
        process.wait.return_value = process.poll.return_value = 0
        completed = subprocess.CompletedProcess([], 0, stdout=b"{}", stderr=b"")
        with mock.patch.object(release.subprocess, "Popen", return_value=process) as popen, \
             mock.patch.object(release.subprocess, "run", return_value=completed) as run:
            try:
                ssh.connect()
                ssh.request("probe", {})
            finally:
                ssh.close()
        commands = [popen.call_args.args[0]] + [c.args[0] for c in run.call_args_list]
        self.assertEqual(len(commands), 3)
        for command in commands:
            self.assertIn("owner@gateway.example", command)
            self.assertEqual(command[command.index("-p") + 1], "2201")
        self.assertIn("StrictHostKeyChecking=yes", commands[0])
        self.assertIn("PubkeyAuthentication=no", commands[1])

    def test_changed_deployment_cannot_reuse_original_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = PackageFixture(root).identity()
            target = release.deployment_target("https://gateway.example",
                        "owner@gateway.example", 2201, "/srv/remote-hosts-code")
            journal = root / "journal.json"
            release.GatewayRelease(identity, None, mock.Mock(), journal, target=target)
            for key, value in (("origin", "https://other.example"),
                               ("ssh_account", "other@gateway.example"),
                               ("ssh_port", 2202),
                               ("installation_root", "/srv/other-gateway")):
                with self.subTest(key=key), self.assertRaises(release.PublishError) as caught:
                    release.GatewayRelease(identity, None, mock.Mock(), journal,
                                           target=dict(target, **{key: value}))
                self.assertEqual(caught.exception.code, "journal_release_identity_conflict")

    def test_oauth_metadata_cannot_substitute_another_configured_origin(self):
        import gateway_release_oauth as oauth
        client = mock.Mock(origin="https://gateway.example")
        client.parsed.side_effect = [
            {"resource": oauth.ORIGIN + "/mcp"},
            {"issuer": oauth.ORIGIN,
             "authorization_endpoint": oauth.ORIGIN + "/oauth/authorize",
             "token_endpoint": oauth.ORIGIN + "/oauth/token",
             "registration_endpoint": oauth.ORIGIN + "/oauth/register",
             "code_challenge_methods_supported": ["S256"]}]
        with tempfile.TemporaryDirectory() as directory, \
             self.assertRaises(oauth.BrowserOAuthError):
            oauth.BrowserOAuth(client, directory).metadata()

    def test_configured_installation_scope_is_exact(self):
        import gateway_release_stage as stage
        with mock.patch.object(stage, "NAS_ROOT"), \
             mock.patch.object(stage, "PUBLIC_ORIGIN"), \
             mock.patch.object(stage, "PUBLIC_HOST"):
            stage.configure_target({"origin": "https://gateway.example",
                                    "installation_root": "/srv/remote-hosts-code"})
            self.assertTrue(stage.approved_root(Path("/srv/remote-hosts-code")))
            self.assertFalse(stage.approved_root(Path("/srv/remote-hosts-code/other")))
            self.assertFalse(stage.approved_root(stage.APPROVED_ROOT))
            self.assertFalse(stage.approved_root(Path("/srv/other-gateway")))

    def test_staging_without_target_stops_before_process_probe(self):
        import gateway_release_stage as stage
        raw = json.dumps({"action": "probe", "plan": {}}).encode() + b"\n"
        stdin = mock.Mock(buffer=io.BytesIO(raw))
        with mock.patch.object(stage.sys, "stdin", stdin), \
             mock.patch.object(stage, "probe", return_value={}) as probe, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(stage.main(), 1)
        self.assertEqual(json.loads(output.getvalue())["error_code"],
                         "deployment_target_required")
        probe.assert_not_called()


if __name__ == "__main__":
    unittest.main()

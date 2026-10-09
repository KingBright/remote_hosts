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
        ssh = release.OwnerSSH()
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
            self.assertEqual(args[-1], "root@hackerlife.fun")
            self.assertNotIn("stdin", popen.call_args.kwargs)
            self.assertNotIn("stdout", popen.call_args.kwargs)
            ssh.close()

    def test_failed_connect_preserves_code_without_secret(self):
        ssh = release.OwnerSSH()
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
        ssh = release.OwnerSSH()
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
                 str(self.report), "--execute", "--oauth-browser"]), \
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


if __name__ == "__main__":
    unittest.main()

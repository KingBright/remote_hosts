"""Task policy rollback gates; all services, binaries and data are isolated fixtures."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "task_policy_upgrade", Path(__file__).resolve().parents[1] / "upgrade-code-gateway.py")
UPGRADE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(UPGRADE)


class UpgradePolicyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.database = self.state / "state.sqlite"
        with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("CREATE TABLE kv(kind TEXT,key TEXT,value TEXT,expires INTEGER)")
        self.binary = self.root / "gateway"
        self.binary.write_bytes(b"previous")
        self.candidate = self.root / "candidate"
        self.candidate.write_bytes(b"candidate")
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"state_dir": str(self.state),
                                          "public_url": "https://fixture.invalid"}))
        self.result = self.root / "result.json"

    def policy(self, kind="task_authorization", value=None):
        with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("INSERT INTO kv VALUES(?,?,?,?)",
                               (kind, "fixture", json.dumps(value or {"enabled": False}), 2**63-1))

    def invoke(self, check_call, manifest=None, previous_manifest=None):
        argv = ["upgrade-code-gateway.py", "--candidate", str(self.candidate),
                "--sha256", UPGRADE.checksum(self.candidate), "--version", "0.10.99",
                "--result", str(self.result), "--binary-path", str(self.binary),
                "--config-path", str(self.config), "--backup-root", str(self.root / "backup")]
        def output(command, **kwargs):
            if command[-1] == "--version":
                return "remote-hosts-code 0.10.99"
            self.assertEqual(command[-1], "release-manifest")
            return json.dumps((manifest or {}) if command[0] == str(self.candidate)
                              else (previous_manifest if previous_manifest is not None else (manifest or {})))
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(UPGRADE.os, "getuid", return_value=0), \
                mock.patch.object(UPGRADE.os, "umask"), \
                mock.patch.object(UPGRADE.subprocess, "check_output", side_effect=output), \
                mock.patch.object(UPGRADE.subprocess, "check_call", side_effect=check_call), \
                mock.patch.object(UPGRADE.time, "monotonic", side_effect=[0, 40]), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                UPGRADE.main()
        return json.loads(self.result.read_text())

    def test_legacy_without_policy_needs_no_new_capability_or_database(self):
        self.assertFalse(UPGRADE.task_authorization_required(self.database))
        missing = self.root / "absent.sqlite"
        with mock.patch.object(UPGRADE, "task_authorization_supported") as supported:
            UPGRADE.require_task_authorization_support(missing, self.binary, "preflight")
            supported.assert_not_called()
        self.assertFalse(missing.exists())

    def test_grants_and_orphan_bindings_block_unsupported_binary(self):
        for kind in ("task_authorization", "operation_task_authorization"):
            with self.subTest(kind=kind):
                with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
                    connection.execute("DELETE FROM kv")
                self.policy(kind)
                with mock.patch.object(UPGRADE, "task_authorization_supported", return_value=False):
                    with self.assertRaisesRegex(RuntimeError, "task_authorization_policy_required"):
                        UPGRADE.require_task_authorization_support(self.database, self.binary, "rollback")

    def test_supported_binary_preserves_private_policy_rows(self):
        self.policy()
        before = self.database.read_bytes()
        with mock.patch.object(UPGRADE.subprocess, "check_output",
                               return_value='{"task_authorization_protocol":1}'):
            UPGRADE.require_task_authorization_support(self.database, self.binary, "cutover")
        self.assertEqual(self.database.read_bytes(), before)

    def test_absent_malformed_boolean_or_unknown_protocol_is_not_support(self):
        for value in ('{}', '[]', 'null', 'broken', '{"task_authorization_protocol":true}',
                      '{"task_authorization_protocol":3}'):
            with self.subTest(value=value), mock.patch.object(
                    UPGRADE.subprocess, "check_output", return_value=value):
                self.assertFalse(UPGRADE.task_authorization_supported(self.binary))
        with mock.patch.object(UPGRADE.subprocess, "check_output",
                               side_effect=subprocess.TimeoutExpired("fixture", 10)):
            self.assertFalse(UPGRADE.task_authorization_supported(self.binary))

    def test_corrupt_policy_store_fails_closed(self):
        self.database.write_bytes(b"not-sqlite")
        with self.assertRaises(sqlite3.DatabaseError):
            UPGRADE.task_authorization_required(self.database)

    def test_downgrade_with_policy_stops_before_service_or_binary_changes(self):
        self.policy()
        calls = []
        result = self.invoke(lambda command, **kwargs: calls.append(command))
        self.assertIn("task_authorization_policy_required:preflight", result["error"])
        self.assertEqual([c for c in calls if c[0] == "systemctl"], [])
        self.assertFalse((self.root / "backup").exists())
        self.assertEqual(self.binary.read_bytes(), b"previous")

    def test_grant_racing_preflight_is_rechecked_after_stop_and_original_restarts(self):
        calls = []
        def control(command, **kwargs):
            calls.append(command)
            if command[1] == "stop":
                self.policy()
        result = self.invoke(control)
        self.assertIn("task_authorization_policy_required:cutover", result["error"])
        self.assertEqual([c[1] for c in calls if c[0] == "systemctl"], ["stop", "start"])
        self.assertEqual(self.binary.read_bytes(), b"previous")
        self.assertFalse(result["service_stopped"])

    def test_grant_created_by_new_runtime_blocks_legacy_rollback(self):
        calls = []
        def control(command, **kwargs):
            calls.append(command)
            if command[:2] == ["systemctl", "start"]:
                self.policy()
        result = self.invoke(control)
        self.assertEqual(result["rollback"],
                         "blocked_task_authorization_policy; service stopped; live database preserved")
        self.assertEqual(self.binary.read_bytes(), b"candidate")
        self.assertTrue(result["service_stopped"])
        self.assertEqual([c[1] for c in calls if c[0] == "systemctl"], ["stop", "start", "stop"])

    def test_compatible_rollback_restores_binary_and_keeps_new_grant(self):
        started = 0
        def control(command, **kwargs):
            nonlocal started
            if command[:2] == ["systemctl", "start"]:
                started += 1
                if started == 1:
                    self.policy()
        result = self.invoke(control, {"task_authorization_protocol": 1})
        self.assertEqual(result["rollback"], "restored_previous_binary; live database preserved")
        self.assertEqual(self.binary.read_bytes(), b"previous")
        self.assertTrue(UPGRADE.task_authorization_required(self.database))
        self.assertFalse(result["service_stopped"])


    def test_expiry_policy_requires_protocol_two_even_after_expiry_or_revocation(self):
        for kind in ("task_authorization", "operation_task_authorization"):
            for value in ({"protocol": 2, "enabled": False},
                          {"protocol": 2, "expires_at": 1, "enabled": True},
                          {"expires_at": 1, "enabled": False}):
                with self.subTest(kind=kind, value=value):
                    with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
                        connection.execute("DELETE FROM kv")
                    self.policy(kind, value)
                    self.assertEqual(UPGRADE.task_authorization_required_protocol(self.database), 2)
                    before = self.database.read_bytes()
                    with mock.patch.object(UPGRADE.subprocess, "check_output",
                                           return_value='{"task_authorization_protocol":1}'):
                        with self.assertRaisesRegex(RuntimeError, "protocol 2"):
                            UPGRADE.require_task_authorization_support(self.database, self.binary, "rollback")
                    with mock.patch.object(UPGRADE.subprocess, "check_output",
                                           return_value='{"task_authorization_protocol":2}'):
                        UPGRADE.require_task_authorization_support(self.database, self.binary, "cutover")
                    self.assertEqual(self.database.read_bytes(), before)

    def test_unknown_future_policy_and_malformed_metadata_fail_closed(self):
        self.policy(value={"protocol": 3})
        with mock.patch.object(UPGRADE.subprocess, "check_output",
                               return_value='{"task_authorization_protocol":2}'):
            with self.assertRaisesRegex(RuntimeError, "protocol 3"):
                UPGRADE.require_task_authorization_support(self.database, self.binary, "cutover")
        with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("UPDATE kv SET value='broken'")
        with self.assertRaisesRegex(RuntimeError, "metadata_invalid"):
            UPGRADE.task_authorization_required_protocol(self.database)

    def test_invalid_policy_metadata_never_permits_downgrade(self):
        for value in ([], None, {"protocol": "2"}, {"protocol": True},
                      {"protocol": 0}, {"expires_at": "1"}, {"expires_at": True}):
            with self.subTest(value=value):
                with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
                    connection.execute("DELETE FROM kv")
                    connection.execute("INSERT INTO kv VALUES(?,?,?,?)",
                                       ("task_authorization", "fixture", json.dumps(value), 2**63-1))
                with self.assertRaisesRegex(RuntimeError, "metadata_invalid"):
                    UPGRADE.task_authorization_required_protocol(self.database)

    def test_new_expiry_policy_blocks_rollback_to_protocol_one(self):
        calls = []
        def control(command, **kwargs):
            calls.append(command)
            if command[:2] == ["systemctl", "start"]:
                self.policy(value={"protocol": 2, "expires_at": 1})
        result = self.invoke(control, {"task_authorization_protocol": 2},
                             previous_manifest={"task_authorization_protocol": 1})
        self.assertEqual(result["rollback"],
                         "blocked_task_authorization_policy; service stopped; live database preserved")
        self.assertTrue(result["service_stopped"])
        self.assertEqual(self.binary.read_bytes(), b"candidate")
        self.assertEqual(UPGRADE.task_authorization_required_protocol(self.database), 2)


    def test_active_legacy_grant_blocks_expiry_migration_before_service_changes(self):
        for value in ({"enabled": True}, {"protocol": 1, "enabled": True, "expires_at": 1},
                      {"protocol": 2, "enabled": True}):
            with self.subTest(value=value):
                with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
                    connection.execute("DELETE FROM kv")
                self.policy(value=value)
                calls = []
                result = self.invoke(lambda command, **kwargs: calls.append(command),
                                     {"task_authorization_protocol": 2})
                self.assertIn("owner renewal required", result["error"])
                self.assertEqual([c for c in calls if c[0] == "systemctl"], [])
                self.assertEqual(self.binary.read_bytes(), b"previous")
                self.assertFalse((self.root / "backup").exists())

    def test_legacy_grant_racing_stop_keeps_original_permissions_and_binary(self):
        calls = []
        def control(command, **kwargs):
            calls.append(command)
            if command[:2] == ["systemctl", "stop"]:
                self.policy(value={"enabled": True})
        result = self.invoke(control, {"task_authorization_protocol": 2})
        self.assertIn("owner renewal required", result["error"])
        self.assertEqual([c[1] for c in calls if c[0] == "systemctl"], ["stop", "start"])
        self.assertEqual(self.binary.read_bytes(), b"previous")
        self.assertFalse(result["service_stopped"])
        with contextlib.closing(sqlite3.connect(self.database)) as connection:
            value = json.loads(connection.execute("SELECT value FROM kv").fetchone()[0])
        self.assertTrue(value["enabled"])

    def test_protocol_two_expiring_grant_is_not_an_unbounded_migration(self):
        self.policy(value={"protocol": 2, "enabled": True, "expires_at": 1})
        with mock.patch.object(UPGRADE.subprocess, "check_output",
                               return_value='{"task_authorization_protocol":2}'):
            UPGRADE.require_task_authorization_support(self.database, self.binary, "cutover")


if __name__ == "__main__":
    unittest.main()

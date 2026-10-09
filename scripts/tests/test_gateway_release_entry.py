"""Gateway-only release tests: temporary files and loopback fake HTTP only."""
import contextlib
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import socket
import sys
import tarfile
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gateway_release as release
import gateway_release_stage as stage
import release_receipts as rr
from release_client import Client, NoRedirect


def encode(value):
    return (json.dumps(value, sort_keys=True) + "\n").encode()


class PackageFixture:
    def __init__(self, root):
        self.package = root / "remote-hosts-code-0.10.26"
        self.package.mkdir()
        names = ["remote-hosts-code-macos-arm64", "remote-hosts-code-linux-amd64",
                 "remote-hosts-code-windows-amd64.exe", "upgrade-code-agent.py",
                 "agent_upgrade_support.py", "launch-code-upgrade.py",
                 "code_upgrade_runner.py", "macos_code_identity.py",
                 "upgrade-code-gateway.py", "check-code-gateway.py"]
        artifacts = {}
        for name in names:
            data = ("fixture:" + name).encode()
            (self.package / name).write_bytes(data)
            artifacts[name] = {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
        snapshot = "f" * 64
        inputs = {"fixture": "b" * 64}
        proof = {"state": "passed", "version": release.VERSION,
                 "source_inputs_unchanged": True, "snapshot_id": snapshot,
                 "source_inputs": inputs,
                 "checks": {name: {"exit_code": 0, "state": "finished"} for name in rr.GATES},
                 "functional_tests": {"passed": 5, "failed": 0,
                                      "test_gates_completed_successfully": True}}
        (self.package / "source-verification.json").write_bytes(encode(proof))
        self.manifest = {"version": release.VERSION, "artifacts": artifacts,
                         "snapshot_id": snapshot, "source_inputs": inputs,
                         "source_verification_sha256": rr.digest(self.package / "source-verification.json"),
                         "task_authorization_protocol": 1, "wire_protocol": 2,
                         "tool_count": 25, "tools_sha256": "c" * 64, "skill_revision": "d" * 64}
        (self.package / "manifest.json").write_bytes(encode(self.manifest))
        (self.package / "README.md").write_text("synthetic fixture")
        (self.package / "SHA256SUMS").write_text("synthetic fixture")
        self.bundle = self.package.parent / (self.package.name + "-bundle.tgz")
        self.archive()
        self.report = root / "build.json"
        report = {"state": "passed", "version": release.VERSION, "verify_only": False,
                  "source_inputs_unchanged": True, "snapshot_id": snapshot,
                  "stages": {name: {"exit_code": 0, "state": "finished"} for name in rr.STAGES},
                  "package": {"path": str(self.package),
                              "manifest_sha256": rr.digest(self.package / "manifest.json")},
                  "verification": {"sha256": rr.digest(self.package / "source-verification.json")}}
        self.report.write_bytes(encode(report))
        self.approved = {"build_sha256": rr.digest(self.report),
                         "manifest_sha256": rr.digest(self.package / "manifest.json"),
                         "bundle_sha256": rr.digest(self.bundle), "bundle_bytes": self.bundle.stat().st_size,
                         "candidate_sha256": artifacts["remote-hosts-code-linux-amd64"]["sha256"]}

    def archive(self, extra=None):
        with tarfile.open(self.bundle, "w:gz") as archive:
            for path in sorted(self.package.iterdir()):
                archive.add(path, arcname=path.name)
            if extra:
                member, data = extra
                archive.addfile(member, io.BytesIO(data) if member.isfile() else None)

    def identity(self):
        return release.verify_release(self.report, self.approved)


class FakeGateway:
    """Stateful existing API, with no Agent action and no real credential."""
    def __init__(self, identity):
        self.identity = identity
        self.token = "synthetic-owner-bearer"
        self.authorized = True
        self.write_scope = True
        self.mode = "success"
        self.health_change = {}
        self.requests = []
        self.posts = 0
        self.staged = False
        self.marker = False
        self.result = None
        self.installed = "e" * 64
        self.running = True
        self.current_version = "0.10.25"
        self.ssh_actions = []
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def response(self, value, status=200):
                data = encode(value)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def permitted(self):
                return owner.authorized and self.headers.get("Authorization") == "Bearer " + owner.token
            def do_GET(self):
                owner.requests.append(("GET", self.path))
                if self.path == "/healthz":
                    value = dict(owner.identity["runtime"], file_transfer=True)
                    value["version"] = release.VERSION if owner.installed == owner.identity["plan"]["candidate_sha256"] else owner.current_version
                    value.update(owner.health_change)
                    return self.response(value)
                if self.path.startswith("/status/task-authorization?"):
                    self.send_response(303)
                    self.send_header("Location", "/status")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if self.path == "/admin/status":
                    if not self.permitted():
                        return self.response({"error": "unauthorized"}, 401)
                    return self.response({"gateway": {"version": "0.10.25"},
                                          "devices": ["unrelated fixture; must never be saved"],
                                          "private_extra": "SYNTHETIC_DO_NOT_SAVE"})
                return self.response({"error": "unsupported"}, 404)
            def do_POST(self):
                owner.requests.append(("POST", self.path))
                if not self.permitted():
                    return self.response({"error": "unauthorized"}, 401)
                data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
                if self.path == "/mcp":
                    if data.get("method") != "tools/list":
                        return self.response({"error": "Agent actions forbidden in this fixture"}, 400)
                    names = ["code_read"] + (["code_apply_edits"] if owner.write_scope else [])
                    return self.response({"jsonrpc": "2.0", "id": data["id"],
                                          "result": {"tools": [{"name": n} for n in names]}})
                if self.path != "/admin/gateway-upgrade":
                    return self.response({"error": "OAuth creation forbidden"}, 400)
                owner.posts += 1
                expected = {"version": release.VERSION,
                            "bundle_sha256": owner.identity["plan"]["bundle_sha256"]}
                if data != expected or not owner.staged or not owner.write_scope:
                    return self.response({"error": "rejected"}, 403)
                owner.marker = True
                if owner.mode in ("success", "lost_ack"):
                    owner.installed = owner.identity["plan"]["candidate_sha256"]
                    owner.result = {"state": "upgraded", "version": release.VERSION,
                                    "installed_sha256": owner.installed,
                                    "candidate_sha256": owner.installed}
                elif owner.mode == "rollback":
                    owner.result = {"state": "failed", "version": release.VERSION,
                                    "candidate_sha256": owner.identity["plan"]["candidate_sha256"],
                                    "rollback": "restored_previous_binary; live database preserved",
                                    "error": "SYNTHETIC_DO_NOT_SAVE"}
                elif owner.mode == "policy_blocked":
                    owner.running = False
                    owner.result = {"state": "failed", "version": release.VERSION,
                                    "candidate_sha256": owner.identity["plan"]["candidate_sha256"],
                                    "rollback": "blocked_task_authorization_policy; service stopped; live database preserved",
                                    "service_stopped": True}
                if owner.mode in ("lost_ack", "pending"):
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                return self.response({"state": "started", "version": release.VERSION}, 202)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def client(self, token=None):
        client = Client("https://fixture.invalid", access=token or self.token, transport="legacy")
        owner = self
        class LoopbackOnlyOpener:
            def open(self, request, timeout):
                parsed = urllib.parse.urlsplit(request.full_url)
                if parsed.scheme != "https" or parsed.netloc != "fixture.invalid":
                    raise AssertionError("test request escaped synthetic origin")
                mapped = urllib.request.Request(
                    "http://127.0.0.1:" + str(owner.server.server_port) + parsed.path
                    + (("?" + parsed.query) if parsed.query else ""),
                    data=request.data, headers=dict(request.headers), method=request.get_method())
                # This isolated adapter translates only synthetic HTTPS to loopback HTTP.
                # Production Client HTTPS/TLS policy is unchanged; no test keys are needed.
                return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(mapped, timeout=timeout)
        client.opener = LoopbackOnlyOpener()
        return client

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, action, plan, bundle=None):
        self.ssh_actions.append(action)
        if action == "stage":
            self.staged = True
        value = {"version": release.VERSION, "gateway_pid": 123 if self.running else 0,
                 "gateway_executable": "/opt/remote-hosts-code/remote-hosts-code",
                 "installed_sha256": self.installed, "running": self.running,
                 "bundle_staged": self.staged, "marker_present": self.marker,
                 "result_present": self.result is not None}
        if self.staged:
            value.update(bundle_sha256=plan["bundle_sha256"], bundle_bytes=plan["bundle_bytes"])
        if self.result is not None:
            value["result"] = stage.safe_result(self.result)
        return value


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.fixture = PackageFixture(self.root)
        self.identity = self.fixture.identity()
        self.gateway = FakeGateway(self.identity)
        self.addCleanup(self.gateway.close)
        self.client = self.gateway.client()
        self.addCleanup(self.client.close)
        self.journal = self.root / "gateway-release.json"

    def coordinator(self, client=None):
        return release.GatewayRelease(self.identity, client or self.client, self.gateway,
                                      self.journal, timeout=0.03, interval=0.01)

    def test_full_gateway_only_loop_checks_exact_hash_health_and_no_secrets(self):
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "healthy")
        self.assertEqual(result["agent_actions"], 0)
        self.assertEqual(self.gateway.posts, 1)
        self.assertNotIn("SYNTHETIC_DO_NOT_SAVE", self.journal.read_text())
        self.assertNotIn(self.gateway.token, self.journal.read_text())
        self.assertEqual(set(self.gateway.requests),
                         {("GET", "/admin/status"), ("POST", "/mcp"),
                          ("POST", "/admin/gateway-upgrade"), ("GET", "/healthz"),
                      ("GET", "/status/task-authorization?task_id=01a1184e-4ede-76a2-aad4-3141c7b4c03c")})

    def test_status_cookie_value_cannot_replace_bearer_and_no_import_occurs(self):
        client = self.gateway.client("a" * 64)
        self.addCleanup(client.close)
        with self.assertRaises(release.PublishError):
            self.coordinator(client).execute()
        self.assertEqual(self.gateway.ssh_actions, [])
        self.assertEqual(self.gateway.posts, 0)

    def test_read_only_bearer_stops_before_management_authentication(self):
        self.gateway.write_scope = False
        with self.assertRaises(release.PublishError):
            self.coordinator().execute()
        self.assertEqual(self.gateway.ssh_actions, [])
        self.assertEqual(self.gateway.posts, 0)

    def test_lost_ack_is_verified_by_original_receipt_without_post_replay(self):
        self.gateway.mode = "lost_ack"
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "healthy")
        self.assertEqual(self.gateway.posts, 1)
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "healthy")
        self.assertEqual(self.gateway.posts, 1)

    def test_crash_during_import_never_retransfers_when_result_is_unknown(self):
        first = self.coordinator()
        first.save(state="importing", import_attempted=True, import_verified=False)
        self.assertEqual(self.coordinator().execute()["state"], "observation_pending")
        self.assertEqual(self.coordinator().execute()["state"], "observation_pending")
        self.assertNotIn("stage", self.gateway.ssh_actions)
        self.assertEqual(self.gateway.posts, 0)

    def test_already_verified_staged_bundle_is_reused_without_byte_transfer(self):
        self.gateway.staged = True
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "healthy")
        self.assertNotIn("stage", self.gateway.ssh_actions)
        self.assertEqual(self.gateway.posts, 1)

    def test_unexpected_newer_runtime_is_not_downgraded_or_imported(self):
        self.gateway.current_version = "0.10.27"
        with self.assertRaises(release.PublishError):
            self.coordinator().execute()
        self.assertNotIn("stage", self.gateway.ssh_actions)
        self.assertEqual(self.gateway.posts, 0)

    def test_pending_unknown_is_not_replayed_across_process_restart(self):
        self.gateway.mode = "pending"
        self.assertEqual(self.coordinator().execute()["state"], "outcome_unknown")
        self.assertEqual(self.coordinator().execute()["state"], "outcome_unknown")
        self.assertEqual(self.gateway.posts, 1)
        self.assertEqual(self.gateway.ssh_actions.count("stage"), 1)

    def test_crash_after_durable_intent_before_send_never_submits(self):
        first = self.coordinator()
        first.save(state="requesting", request_attempted=True)
        self.assertEqual(self.coordinator().execute()["state"], "outcome_unknown")
        self.assertEqual(self.gateway.posts, 0)
        self.assertNotIn("stage", self.gateway.ssh_actions)

    def test_wrong_health_schema_prevents_success_and_does_not_replay(self):
        self.gateway.health_change["tools_sha256"] = "0" * 64
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "outcome_unknown")
        self.assertFalse(result["deployment_confirmed"])
        self.assertEqual(self.gateway.posts, 1)

    def test_existing_remote_marker_causes_observation_only(self):
        self.gateway.marker = True
        self.assertEqual(self.coordinator().execute()["state"], "observation_pending")
        self.assertEqual(self.gateway.posts, 0)
        self.assertNotIn("stage", self.gateway.ssh_actions)

    def test_rollback_receipt_is_failed_and_private_error_is_excluded(self):
        self.gateway.mode = "rollback"
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["updater"]["rollback"],
                         "restored_previous_binary_live_database_preserved")
        self.assertNotIn("SYNTHETIC_DO_NOT_SAVE", self.journal.read_text())
        self.assertEqual(self.gateway.posts, 1)

    def test_policy_blocked_rollback_is_preserved_without_service_bypass(self):
        self.gateway.mode = "policy_blocked"
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "failed")
        self.assertTrue(result["updater"]["service_stopped"])
        self.assertEqual(result["updater"]["rollback"], "blocked_task_authorization_policy")
        self.assertEqual(self.gateway.posts, 1)

    def test_observe_only_never_imports_or_posts(self):
        result = self.coordinator().observe()
        self.assertEqual(result["state"], "observation_pending")
        self.assertEqual(self.gateway.posts, 0)
        self.assertNotIn("stage", self.gateway.ssh_actions)

    def test_verified_original_import_can_continue_without_retransfer(self):
        first = self.coordinator()
        first.save(state="import_outcome_unknown", import_attempted=True, import_verified=False)
        self.gateway.staged = True
        result = first.observe()
        self.assertEqual(result["state"], "import_verified_request_not_submitted")
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "healthy")
        self.assertEqual(self.gateway.posts, 1)
        self.assertNotIn("stage", self.gateway.ssh_actions)

    def test_real_owner_status_guard_401_confirms_route_without_new_login(self):
        health = dict(self.identity["runtime"], file_transfer=True)
        with mock.patch.object(self.client, "call", side_effect=[
                (200, {}, encode(health)),
                (401, {}, b"<a href='/status'>Sign in as the Gateway owner</a>")]):
            self.coordinator().health()
        saved = json.loads(self.journal.read_text())
        self.assertTrue(saved["task_authorization_route"]["login_required"])
        self.assertFalse(saved["task_authorization_route"]["grant_created"])
        self.assertEqual(self.gateway.posts, 0)

    def test_unrelated_401_is_not_accepted_as_task_authorization_route(self):
        health = dict(self.identity["runtime"], file_transfer=True)
        with mock.patch.object(self.client, "call", side_effect=[
                (200, {}, encode(health)), (401, {}, b"unrelated proxy rejection")]), \
                self.assertRaises(release.PublishError) as caught:
            self.coordinator().health()
        self.assertEqual(caught.exception.code, "task_authorization_route_not_confirmed")
        self.assertEqual(self.gateway.posts, 0)

    def test_changed_semantic_identity_cannot_reuse_journal(self):
        first = self.coordinator()
        other = dict(self.identity, plan=dict(self.identity["plan"], bundle_sha256="0" * 64))
        with self.assertRaises(release.PublishError):
            release.GatewayRelease(other, self.client, self.gateway, self.journal)
        self.assertEqual(first.state["request_attempted"], False)
        self.assertEqual(self.gateway.posts, 0)

    def test_concurrent_journal_owner_is_rejected(self):
        with release.journal_lock(self.root / "lock"):
            with self.assertRaises(release.PublishError):
                with release.journal_lock(self.root / "lock"):
                    self.fail("duplicate owner acquired lock")

    def test_noninteractive_authentication_never_reads_disk_or_prompts(self):
        with mock.patch.object(release.sys.stdin, "isatty", return_value=False), \
                mock.patch.object(release.getpass, "getpass") as prompt:
            with self.assertRaises(release.PublishError):
                release.borrowed_bearer()
            prompt.assert_not_called()

    def test_boolean_task_protocol_is_not_a_valid_runtime_identity(self):
        self.gateway.health_change["task_authorization_protocol"] = True
        result = self.coordinator().execute()
        self.assertEqual(result["state"], "outcome_unknown")
        self.assertEqual(self.gateway.posts, 1)

    def test_default_cli_is_local_plan_only_with_no_auth_or_ssh(self):
        report_dir = self.root / "plan"
        args = ["gateway_release.py", "--report-dir", str(report_dir)]
        with mock.patch.object(sys, "argv", args), \
                mock.patch.object(release, "verify_release", return_value=self.identity), \
                mock.patch.object(release, "Client") as client, \
                mock.patch.object(release, "OwnerSSH") as ssh, \
                mock.patch.object(release, "borrowed_bearer") as bearer, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(release.main(), 2)
        client.assert_not_called()
        ssh.assert_not_called()
        bearer.assert_not_called()
        result = json.loads((report_dir / "gateway-release-plan.json").read_text())
        self.assertEqual(result["state"], "authentication_required")
        self.assertEqual(result["agent_actions"], 0)
        self.assertEqual(self.gateway.posts, 0)

    def test_oauth_flag_without_execution_remains_local_plan(self):
        report_dir = self.root / "oauth-plan"
        with mock.patch.object(sys, "argv", ["gateway_release.py", "--report-dir",
                str(report_dir), "--oauth-browser"]), \
                mock.patch.object(release, "verify_release", return_value=self.identity), \
                mock.patch.object(release, "BrowserOAuth") as oauth_flow, \
                mock.patch.object(release, "Client") as client, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(release.main(), 2)
        client.assert_not_called()
        oauth_flow.assert_not_called()
        self.assertEqual(self.gateway.posts, 0)

    def test_noninteractive_oauth_execution_stops_before_client_creation(self):
        report_dir = self.root / "noninteractive"
        with mock.patch.object(sys, "argv", ["gateway_release.py", "--report-dir",
                str(report_dir), "--execute", "--oauth-browser"]), \
                mock.patch.object(release, "verify_release", return_value=self.identity), \
                mock.patch.object(release.sys.stdin, "isatty", return_value=False), \
                mock.patch.object(release, "Client") as client, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(release.main(), 2)
        client.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["error_code"], "owner_terminal_required")

    def test_no_new_oauth_claims_shorter_expiry_than_server_protocol(self):
        auth = release.auth_requirement()["new_authorization_if_no_existing_bearer"]
        self.assertEqual(auth["scopes"], ["code:read", "code:write"])
        self.assertEqual(auth["access_seconds"], 3600)
        self.assertEqual(auth["refresh_seconds"], 2592000)
        self.assertFalse(auth["refresh_issuance_can_be_disabled_by_current_protocol"])


class StageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.fixture = PackageFixture(self.root)
        self.plan = self.fixture.identity()["plan"]
        self.install = self.root / "install"
        self.install.mkdir()
        self.binary = self.install / "remote-hosts-code"
        self.binary.write_bytes(b"previous")
        self.identity = {"root": self.install, "binary": self.binary,
                         "pid": 123, "running": True, "installed_sha256": rr.digest(self.binary)}
        self.patch = mock.patch.object(stage, "identify", return_value=self.identity)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.bundle = self.install / "releases" / release.VERSION / self.fixture.bundle.name

    def receive(self, data=None):
        return stage.stage(self.plan, io.BytesIO(self.fixture.bundle.read_bytes() if data is None else data))

    def test_import_is_atomic_verified_and_same_hash_is_idempotent(self):
        value = self.receive()
        self.assertTrue(value["bundle_staged"])
        self.assertEqual(rr.digest(self.bundle), self.plan["bundle_sha256"])
        before = self.bundle.stat().st_mtime_ns
        self.receive()
        self.assertEqual(self.bundle.stat().st_mtime_ns, before)
        self.assertEqual(list(self.bundle.parent.glob("*.part")), [])

    def test_different_existing_bundle_is_never_overwritten(self):
        self.bundle.parent.mkdir(parents=True)
        self.bundle.write_bytes(b"published different bytes")
        with self.assertRaises(stage.StageError):
            self.receive()
        self.assertEqual(self.bundle.read_bytes(), b"published different bytes")

    def test_truncated_transport_publishes_nothing_and_cleans_owned_partial(self):
        with self.assertRaises(stage.StageError):
            self.receive(self.fixture.bundle.read_bytes()[:-1])
        self.assertFalse(self.bundle.exists())
        self.assertEqual(list(self.bundle.parent.glob("*.part")), [])

    def test_low_disk_reserve_blocks_before_directory_or_file_creation(self):
        with mock.patch.object(stage.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(1, 1, stage.RESERVE)):
            with self.assertRaises(stage.StageError):
                self.receive()
        self.assertFalse(self.bundle.parent.exists())

    def test_unknown_existing_upgrade_marker_blocks_import(self):
        self.bundle.parent.mkdir(parents=True)
        (self.bundle.parent / "self-upgrade-request.json").write_bytes(
            encode({"version": release.VERSION, "bundle_sha256": self.plan["bundle_sha256"]}))
        with self.assertRaises(stage.StageError):
            self.receive()
        self.assertFalse(self.bundle.exists())

    def test_symlink_release_directory_is_rejected(self):
        other = self.root / "other"
        other.mkdir()
        (self.install / "releases").symlink_to(other, target_is_directory=True)
        with self.assertRaises(stage.StageError):
            self.receive()
        self.assertEqual(list(other.iterdir()), [])

    def test_wrong_version_and_manifest_identity_block_before_import(self):
        for change in ({"version": "0.10.27"}, {"manifest_sha256": "0" * 64}):
            with self.subTest(change=change), self.assertRaises(stage.StageError):
                stage.stage(dict(self.plan, **change), io.BytesIO(self.fixture.bundle.read_bytes()))
            self.assertFalse(self.bundle.exists())

    def test_traversal_duplicate_and_symlink_members_are_rejected(self):
        for name, kind in (("../escape", tarfile.REGTYPE),
                           ("README.md", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)):
            with self.subTest(name=name):
                item = tarfile.TarInfo(name)
                item.type = kind
                item.size = 1 if kind == tarfile.REGTYPE else 0
                item.linkname = "README.md" if kind == tarfile.SYMTYPE else ""
                self.fixture.archive((item, b"x"))
                altered = dict(self.plan, bundle_sha256=rr.digest(self.fixture.bundle),
                               bundle_bytes=self.fixture.bundle.stat().st_size)
                with self.assertRaises(stage.StageError):
                    stage.inspect_bundle(self.fixture.bundle, altered)
                self.assertFalse((self.root / "escape").exists())

    def test_stopped_gateway_reads_only_pinned_original_policy_failure_receipt(self):
        self.receive()
        result = {"state": "failed", "version": release.VERSION,
                  "candidate_sha256": self.plan["candidate_sha256"],
                  "rollback": "blocked_task_authorization_policy; service stopped; live database preserved",
                  "service_stopped": True, "error": "SYNTHETIC_DO_NOT_SAVE"}
        (self.bundle.parent / "self-upgrade-result.json").write_bytes(encode(result))
        with mock.patch.object(stage, "identify", side_effect=stage.StageError("gateway_not_running")), \
                mock.patch.object(stage, "APPROVED_ROOT", self.install):
            value = stage.probe(dict(self.plan, gateway_root=str(self.install)))
        self.assertFalse(value["running"])
        self.assertEqual(value["result"]["rollback"], "blocked_task_authorization_policy")
        self.assertNotIn("SYNTHETIC_DO_NOT_SAVE", json.dumps(value))

    def test_mutated_build_receipt_and_artifact_are_rejected_locally(self):
        self.fixture.report.write_text("{}")
        with self.assertRaises(release.PublishError):
            self.fixture.identity()


if __name__ == "__main__":
    unittest.main()

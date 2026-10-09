#!/usr/bin/env python3
"""Gateway-only 0.10.26 publication through the owner API and direct human login."""
import argparse
from contextlib import contextmanager
import fcntl
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile
import time

import gateway_release_stage as staging
import release_receipts as rr
from release_client import Client
from gateway_release_oauth import BrowserOAuth, BrowserOAuthError, checked_origin
from gateway_release_ssh_diagnostics import StderrCodes, classify

VERSION = "0.10.26"
ORIGIN = "https://mcp.example.com"  # Reserved example; execution requires an explicit target.
BUILD_REPORT = "build.json"
APPROVED = {
    "build_sha256": "a0706aaa562d0965c47a2349e2b5ce910821ddee6041595ff0cc5238a55262fb",
    "manifest_sha256": "4bb5e301c122b3f7276ba9426d239728fd9c7438126147efd00372becaf8fbf6",
    "bundle_sha256": "00da35fbbf1d0e768988320cd481544eb3ca5690463e4e90ee8f23209bdfc0cd",
    "bundle_bytes": 31956930,
    "candidate_sha256": "2c9d259c73f436384b9e3c2d562f5e7d4c5a88515be69df567383a0480943523",
}
SCOPES = ["code:read", "code:write"]


class PublishError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__()


def require(condition, code):
    if not condition:
        raise PublishError(code)


def ssh_target(account, port):
    require(isinstance(account, str) and bool(re.fullmatch(
        r"[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9.-]*", account)),
        "invalid_ssh_target")
    require(type(port) is int and 1 <= port <= 65535, "invalid_ssh_port")


def deployment_target(origin, account, port, installation_root):
    require(all(value is not None for value in (origin, account, installation_root)),
            "deployment_target_required")
    origin = checked_origin(origin)
    ssh_target(account, port)
    require(isinstance(installation_root, str) and len(installation_root) <= 4096
            and not any(ord(c) < 32 or ord(c) == 127 for c in installation_root),
            "invalid_installation_root")
    root = Path(installation_root)
    require(root.is_absolute() and root != Path("/") and ".." not in root.parts
            and "." not in installation_root.split("/")
            and str(root) == installation_root, "invalid_installation_root")
    return {"origin": origin, "ssh_account": account, "ssh_port": port,
            "installation_root": installation_root}


def auth_requirement(target=None):
    origin = target["origin"] if target else ORIGIN
    return {
        "state": "owner_session_required",
        "available_modes": ["oauth-browser", "borrow-existing-bearer"],
        "browser_flow_approved_in_this_task": False,
        "status_login_usable": False,
        "existing_session": "Borrow an already authorized owner Bearer directly in the operator Terminal; never send it to the model.",
        "resource": origin + "/mcp",
        "scopes": SCOPES,
        "automatic_registration_or_authorization": False,
        "new_authorization_if_no_existing_bearer": {
            "approval_required": True,
            "type": "OAuth authorization_code with PKCE S256; existing registered client preferred",
            "resource": origin + "/mcp",
            "scopes": SCOPES,
            "authorization_pending_seconds": 600,
            "code_seconds": 120,
            "access_seconds": 3600,
            "refresh_seconds": 2592000,
            "refresh_issuance_can_be_disabled_by_current_protocol": False,
            "persistent_client_or_refresh_storage_by_this_entry": False,
            "client_registration_if_no_reusable_registered_client": {
                "approval_required": True,
                "type": "OAuth public client registration, token_endpoint_auth_method=none; no client secret",
                "server_registration_expiry": "none; stored with i64::MAX expiry",
                "created": False,
            },
            "limitation": "These scopes are owner resource scopes, not a Gateway-only token audience. This entry itself never dispatches Agent actions.",
        },
        "borrowed_session_lifetime": "Existing server expiry is unchanged; memory only for this bounded run, no refresh, revoke or credential-file write.",
        "ssh": {"account": target["ssh_account"] if target else "YOUR_USER@example.com",
                "port": target["ssh_port"] if target else 22,
                "purpose": "Verified import and read-only receipt observation only",
                "owner_authentication": "Existing NAS key or agent in BatchMode; no password prompt",
                "idle_timeout_seconds": 600, "closed_on_exit": True,
                "model_reads_private_key_password_or_token_contents": False,
                "existing_ssh_key_or_agent_reuse_authorized": False},
    }


def verify_release(report, approved=APPROVED):
    report = Path(report)
    require(not report.is_symlink() and rr.digest(report) == approved["build_sha256"],
            "approved_build_receipt_mismatch")
    proof = rr.verified_build(report, VERSION)
    package = Path(proof["package"])
    require(proof["manifest_sha256"] == approved["manifest_sha256"],
            "approved_manifest_mismatch")
    manifest = json.loads((package / "manifest.json").read_text())
    require(type(manifest.get("task_authorization_protocol")) is int
            and manifest["task_authorization_protocol"] == 1, "task_policy_protocol_required")
    require(manifest["artifacts"]["remote-hosts-code-linux-amd64"]["sha256"]
            == approved["candidate_sha256"], "approved_gateway_candidate_mismatch")
    bundle = package.parent / (package.name + "-bundle.tgz")
    plan = {"version": VERSION, **{k: approved[k] for k in
            ("manifest_sha256", "bundle_sha256", "bundle_bytes", "candidate_sha256")}}
    staging.inspect_bundle(bundle, plan)
    # Verify every archived byte against the already gated immutable local package.
    with tarfile.open(bundle, "r:gz") as archive:
        for member in archive.getmembers():
            local = package / member.name
            require(not local.is_symlink() and local.is_file()
                    and local.stat().st_size == member.size, "bundle_local_inventory_mismatch")
            h = hashlib.sha256()
            with archive.extractfile(member) as stream:
                for block in iter(lambda: stream.read(1048576), b""):
                    h.update(block)
            require(h.hexdigest() == rr.digest(local), "bundle_local_bytes_mismatch")
    return {"plan": plan, "bundle": str(bundle), "snapshot_id": proof["snapshot_id"],
            "manifest_sha256": proof["manifest_sha256"], "tests": proof["tests"],
            "runtime": {k: manifest[k] for k in
                        ("version", "wire_protocol", "tool_count", "tools_sha256",
                         "skill_revision", "task_authorization_protocol")}}


@contextmanager
def journal_lock(directory):
    directory = Path(directory)
    require(not directory.is_symlink(), "unsafe_journal_directory")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / "gateway-release.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PublishError("gateway_release_already_owned") from None
        yield directory / "gateway-release.json"
    finally:
        os.close(fd)


class OwnerSSH:
    """One explicitly owner-authenticated, short-lived SSH management transport."""
    def __init__(self, account, port=22):
        ssh_target(account, port)
        self.account, self.port = account, port
        self.directory = None
        self.socket = None
        self.auth_process = None
        self.stderr_codes = None
        self.last_outcome = None

    def connect(self):
        # Passwordless read-only preflight may run without a Terminal; owner OAuth still requires one.
        self.directory = tempfile.TemporaryDirectory(prefix="rh-gw-", dir="/tmp")
        os.chmod(self.directory.name, 0o700)
        self.socket = str(Path(self.directory.name) / "c")
        args = ["ssh", "-p", str(self.port), "-S", self.socket,
                "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=15",
                "-o", "BatchMode=yes", "-o", "NumberOfPasswordPrompts=0",
                "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no",
                "-o", "ControlPersist=600", "-M", "-N", "-f", self.account]
        # Existing configuration, keys and agent authenticate in BatchMode. No prompt.
        try:
            self.auth_process = subprocess.Popen(args, stderr=subprocess.PIPE)
            self.stderr_codes = StderrCodes(self.auth_process.stderr)
            returncode = self.auth_process.wait()
            self.last_outcome = self.stderr_codes.result(returncode)
        except OSError as error:
            self.last_outcome = {"exit_code": None, "category": "ssh_client_start_failed",
                                 "error_type": type(error).__name__,
                                 "raw_stderr_retained": False, "password_prompt_capture": False}
            raise PublishError("owner_ssh_connection_not_confirmed") from None
        require(returncode == 0, "owner_ssh_connection_not_confirmed")

    def request(self, action, plan, bundle=None):
        require(self.socket is not None, "owner_ssh_not_connected")
        source = Path(staging.__file__).read_text()
        args = ["ssh", "-F", "/dev/null", "-p", str(self.port), "-S", self.socket,
                "-o", "ControlMaster=no", "-o", "ProxyCommand=false",
                "-o", "StrictHostKeyChecking=yes", "-o", "IdentityFile=none",
                "-o", "IdentityAgent=none", "-o", "PubkeyAuthentication=no",
                "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no",
                "-o", "BatchMode=yes", self.account,
                "python3 -c " + shlex.quote(source)]
        payload = (json.dumps({"action": action, "plan": plan}) + "\n").encode()
        if action == "stage":
            require(bundle is not None and Path(bundle).stat().st_size == plan["bundle_bytes"]
                    and rr.digest(bundle) == plan["bundle_sha256"], "bundle_changed_before_import")
            payload += Path(bundle).read_bytes()
        value = subprocess.run(args, input=payload, capture_output=True,
                               timeout=600 if action == "stage" else 90, check=False)
        require(len(value.stdout) <= 65536, "staging_receipt_budget")
        # stderr may contain private transport detail; never persist or print it.
        if value.returncode != 0:
            self.last_outcome = classify(value.stderr, value.returncode)
            try:
                blocked = json.loads(value.stdout)
                code = blocked.get("error_code") if isinstance(blocked, dict) else None
            except (ValueError, TypeError):
                code = None
            known = {"existing_management_account_required", "gateway_service_identity_unavailable",
                     "gateway_not_running", "gateway_executable_deleted", "unexpected_gateway_executable",
                     "gateway_outside_existing_installation", "symlink_in_installation", "not_regular_file"}
            if code in known:
                self.last_outcome.update(category="gateway_preflight_failed", gateway_error_code=code)
                raise PublishError(code)
        require(value.returncode == 0, "staging_or_observation_not_confirmed")
        result = json.loads(value.stdout)
        require(isinstance(result, dict) and result.get("state") != "blocked",
                "staging_or_observation_blocked")
        return result

    def close(self):
        if self.auth_process is not None and self.auth_process.poll() is None:
            self.auth_process.terminate()
            try:
                self.auth_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.auth_process.kill()
                self.auth_process.wait(timeout=2)
        if self.socket is not None:
            try:
                subprocess.run(["ssh", "-F", "/dev/null", "-p", str(self.port), "-S", self.socket,
                                "-O", "exit", self.account],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=10, check=False)
            except (OSError, subprocess.SubprocessError):
                pass  # ControlPersist bounds the transport if close is unconfirmed.
            self.socket = None
        if self.stderr_codes is not None:
            self.stderr_codes.thread.join(timeout=1)
        if self.directory is not None:
            self.directory.cleanup()
            self.directory = None


class GatewayRelease:
    def __init__(self, identity, client, ssh, journal, *, clock=time.monotonic,
                 sleep=time.sleep, timeout=300, interval=2, target=None):
        require(0 < timeout <= 600 and 0 < interval <= 10, "invalid_observation_budget")
        self.identity, self.client, self.ssh = identity, client, ssh
        self.journal = Path(journal)
        self.clock, self.sleep, self.timeout, self.interval = clock, sleep, timeout, interval
        self.plan = dict(identity["plan"])
        self.origin = target["origin"] if target else ORIGIN
        if target:
            self.plan.update(origin=self.origin, installation_root=target["installation_root"])
        semantic = {"origin": self.origin, "target": "gateway", "plan": dict(self.plan)}
        if target:
            semantic["deployment"] = dict(target)
        ident = rr.identity(semantic)
        if self.journal.exists():
            require(not self.journal.is_symlink(), "unsafe_journal_file")
            self.state = json.loads(self.journal.read_text())
            require(self.state.get("semantic_id") == ident, "journal_release_identity_conflict")
            if self.state.get("gateway_root"):
                self.plan["gateway_root"] = self.state["gateway_root"]
        else:
            self.state = {"protocol": 1, "semantic_id": ident, **semantic,
                          "state": "verified_local", "request_attempted": False,
                          "import_attempted": False, "import_verified": False, "agent_actions": 0}
            self.save()

    def save(self, **updates):
        self.state.update(updates)
        self.state["updated_at"] = int(time.time())
        rr.atomic_json(self.journal, self.state)
        directory_fd = os.open(self.journal.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def authenticate(self):
        status, _, body = self.client.call("/admin/status", auth=True)
        require(status == 200, "existing_owner_code_read_session_required")
        # Do not persist the snapshot's Agent inventory or unrelated operations.
        json.loads(body)
        tools = self.client.rpc("tools/list", {})
        names = {item.get("name") for item in tools.get("tools", [])}
        require("code_apply_edits" in names, "existing_code_write_scope_required")
        self.save(authenticated_scopes=SCOPES, credential_storage=False)

    def probe(self):
        value = self.ssh.request("probe", self.plan)
        executable = value.get("gateway_executable", "")
        require(isinstance(executable, str) and Path(executable).name == "remote-hosts-code",
                "gateway_identity_unavailable")
        root = str(Path(executable).parent)
        if "gateway_root" in self.plan:
            require(root == self.plan["gateway_root"], "gateway_root_changed")
        self.plan["gateway_root"] = root
        self.save(gateway_root=root)
        # Keep only the staging module's bounded public metadata.
        self.save(observation=value)
        return value

    def health(self):
        status, _, body = self.client.call("/healthz")
        require(status == 200, "gateway_health_not_confirmed")
        value = json.loads(body)
        require(value.get("file_transfer") is True, "gateway_readiness_not_confirmed")
        for key, expected in self.identity["runtime"].items():
            require(type(value.get(key)) is type(expected) and value.get(key) == expected,
                    "gateway_runtime_identity_mismatch")
        route_status, route_headers, route_body = self.client.call(
            "/status/task-authorization?task_id=gateway-release-route-check")
        owner_guard = (route_status == 401 and route_body ==
                       b"<a href=\'/status\'>Sign in as the Gateway owner</a>")
        require(route_status == 200 or owner_guard or (route_status == 303
                and route_headers.get("Location") == "/status"),
                "task_authorization_route_not_confirmed")
        self.save(task_authorization_route={"available": True, "http_status": route_status,
                  "login_required": route_status in (303, 401), "grant_created": False})
        return {key: value[key] for key in
                (*self.identity["runtime"], "file_transfer")}

    def observe(self):
        deadline = self.clock() + self.timeout
        while True:
            try:
                value = self.probe()
                result = value.get("result") or {}
                if (not self.state["request_attempted"] and value.get("bundle_staged") is True
                        and not value.get("marker_present") and not value.get("result_present")
                        and (self.state.get("import_attempted") or
                             self.state.get("state") in ("importing", "import_outcome_unknown"))
                        and not self.state.get("import_verified")):
                    self.save(state="import_verified_request_not_submitted", import_verified=True,
                              recovery="Original import is verified. An explicit --execute may continue with the same journal.")
                    return self.state
                if result.get("state") == "failed":
                    self.save(state="failed", updater=result,
                              recovery="Inspect original updater receipt; rollback policy remains authoritative.")
                    return self.state
                if value.get("running", True) is True and (
                        result.get("state") == "upgraded" or
                        value.get("installed_sha256") == self.plan["candidate_sha256"]):
                    require(value.get("installed_sha256") == self.plan["candidate_sha256"]
                            and result.get("installed_sha256", self.plan["candidate_sha256"])
                            == self.plan["candidate_sha256"], "installed_gateway_hash_mismatch")
                    health = self.health()
                    self.save(state="healthy", updater=result, health=health,
                              deployment_confirmed=True, request_replayed=False)
                    return self.state
            except Exception as error:
                self.save(last_observation_error_type=type(error).__name__)
            if self.clock() >= deadline:
                self.save(state="outcome_unknown" if self.state["request_attempted"] else "observation_pending",
                          deployment_confirmed=False, request_replayed=False,
                          recovery="Use --observe with this same report directory. Never resubmit an unknown request.")
                return self.state
            self.sleep(self.interval)

    def execute(self):
        self.authenticate()
        # Any prior request, including an interrupted/unknown one, is observation only.
        if self.state["request_attempted"]:
            return self.observe()
        if ((self.state.get("import_attempted") or self.state.get("state") in
             ("importing", "import_outcome_unknown")) and not self.state.get("import_verified")):
            return self.observe()
        status, _, body = self.client.call("/healthz")
        require(status == 200, "current_gateway_health_unavailable")
        current_version = json.loads(body).get("version")
        require(current_version in ("0.10.25", VERSION), "unexpected_current_gateway_version")
        before = self.probe()
        require(current_version != VERSION or before.get("installed_sha256") ==
                self.plan["candidate_sha256"], "published_version_binary_mismatch")
        if before.get("marker_present") or before.get("result_present"):
            return self.observe()
        if before.get("installed_sha256") == self.plan["candidate_sha256"]:
            return self.observe()
        if before.get("bundle_staged"):
            require(before.get("bundle_sha256") == self.plan["bundle_sha256"]
                    and before.get("bundle_bytes") == self.plan["bundle_bytes"],
                    "existing_staged_bundle_identity_mismatch")
            self.save(state="import_verified", import_verified=True)
        else:
            self.save(state="importing", import_attempted=True, import_verified=False)
            try:
                imported = self.ssh.request("stage", self.plan, self.identity["bundle"])
                require(imported.get("bundle_staged") is True
                        and imported.get("bundle_sha256") == self.plan["bundle_sha256"]
                        and imported.get("bundle_bytes") == self.plan["bundle_bytes"],
                        "import_not_verified")
                self.save(state="import_verified", import_verified=True)
            except Exception as error:
                self.save(state="import_outcome_unknown", error_type=type(error).__name__)
                raise PublishError("observe_original_import_before_continuing") from None
        before_post = self.probe()
        if before_post.get("marker_present") or before_post.get("result_present"):
            return self.observe()
        require(before_post.get("bundle_staged") is True
                and before_post.get("bundle_sha256") == self.plan["bundle_sha256"]
                and before_post.get("bundle_bytes") == self.plan["bundle_bytes"],
                "staged_bundle_changed_before_request")
        # Persist intent BEFORE the single effectful API call, even if it never reaches the server.
        self.save(state="requesting", request_attempted=True,
                  request={"version": VERSION, "bundle_sha256": self.plan["bundle_sha256"]},
                  request_replayed=False)
        try:
            status, _, body = self.client.call(
                "/admin/gateway-upgrade", self.state["request"], auth=True)
            if status in (401, 403):
                self.save(state="authentication_rejected", http_status=status,
                          deployment_confirmed=False)
                return self.state
            if status not in (200, 202):
                self.save(state="outcome_unknown", http_status=status)
            else:
                result = json.loads(body)
                require(result.get("version") == VERSION, "upgrade_ack_identity_mismatch")
                ack = result.get("state")
                self.save(state="observing", acknowledged_state=ack if ack in
                          ("started", "upgraded", "no_change", "needs_recovery", "failed") else "unrecognized")
        except Exception as error:
            # Never stringify an HTTP/transport exception, body, Authorization header or URL.
            self.save(state="outcome_unknown", request_error_type=type(error).__name__)
        return self.observe()


def borrowed_bearer():
    require(sys.stdin.isatty() and sys.stderr.isatty(), "owner_terminal_required")
    value = getpass.getpass("既有 owner Bearer（仅内存；不会登录或创建 OAuth）：", stream=sys.stderr)
    require(bool(re.fullmatch(r"[A-Za-z0-9._~+/=-]{1,4096}", value)),
            "invalid_borrowed_bearer")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-report", type=Path, default=Path(BUILD_REPORT))
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--origin", help="Exact HTTPS Gateway origin; no credentials or path")
    parser.add_argument("--ssh-account", help="Existing management account in USER@HOST form")
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--installation-root", help="Exact canonical existing Gateway directory")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--observe", action="store_true")
    auth_mode = parser.add_mutually_exclusive_group()
    auth_mode.add_argument("--oauth-browser", action="store_true",
                          help="Use an explicitly owner-approved read/write PKCE grant; owner logs in directly in the default browser")
    auth_mode.add_argument("--borrow-existing-bearer", action="store_true",
                        help="Owner enters an already authorized Bearer at a hidden Terminal prompt; no OAuth or password-file login")
    args = parser.parse_args()
    os.umask(0o077)
    client, ssh, release = None, None, None
    entry_phase, journal_owned = "local_verification", False
    try:
        identity = verify_release(args.build_report)
        if not (args.execute or args.observe) or not (args.borrow_existing_bearer or args.oauth_browser):
            result = {"state": "authentication_required", "version": VERSION,
                      "local_verified": identity, "authentication": auth_requirement(),
                      "configuration_required": ["--origin", "--ssh-account", "--installation-root"],
                      "deployed": False, "agent_actions": 0}
            require(not args.report_dir.is_symlink(), "unsafe_journal_directory")
            args.report_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            rr.atomic_json(args.report_dir / "gateway-release-plan.json", result)
            print(json.dumps(result, ensure_ascii=False))
            return 2
        with journal_lock(args.report_dir) as journal:
            journal_owned = True
            require(sys.stdin.isatty() and sys.stderr.isatty(), "owner_terminal_required")
            entry_phase = "deployment_target_validation"
            target = deployment_target(args.origin, args.ssh_account, args.ssh_port,
                                       args.installation_root)
            # Read-only existing key/service preflight precedes any new OAuth grant.
            ssh = OwnerSSH(target["ssh_account"], target["ssh_port"])
            release = GatewayRelease(identity, None, ssh, journal, target=target)
            entry_phase = "existing_ssh_connection"
            ssh.connect()
            entry_phase = "gateway_identity_preflight"
            preflight = ssh.request("probe", release.plan)
            rr.atomic_json(args.report_dir / "gateway-preflight.json", preflight)
            if args.execute and not release.state["request_attempted"]:
                require(preflight.get("publication_controller", {}).get("ready") is True,
                        "gateway_publication_controller_unavailable")
            entry_phase = "owner_oauth_authentication"
            client = Client(target["origin"], access=None, transport="legacy")
            token = (BrowserOAuth(client, args.report_dir).authenticate() if args.oauth_browser
                     else borrowed_bearer())
            client.access = token
            client.refresh = None
            token = None
            release.client = client
            entry_phase = "owner_application_authority"
            release.authenticate()
            release.save(entry_phase="ssh_authenticated", ssh_outcome=ssh.last_outcome)
            entry_phase = "observe_original" if args.observe else "gateway_publication"
            result = release.observe() if args.observe else release.execute()
            print(json.dumps(result, ensure_ascii=False))
            return 0 if result["state"] == "healthy" else 2
    except (Exception, KeyboardInterrupt) as error:
        exit_code = 130 if isinstance(error, KeyboardInterrupt) else 2
        failure = {"state": "blocked", "entry_phase": entry_phase,
                   "error_type": type(error).__name__,
                   "error_code": "owner_interrupted" if isinstance(error, KeyboardInterrupt) else
                        error.code if isinstance(error, (PublishError, BrowserOAuthError)) else "publication_not_confirmed",
                   "entry_exit_code": exit_code, "observed_at": int(time.time()),
                   "import_attempted": bool(release and release.state["import_attempted"]),
                   "request_attempted": bool(release and release.state["request_attempted"]),
                   "deployed": False, "raw_error_retained": False}
        if ssh is not None and isinstance(ssh.last_outcome, dict):
            failure["ssh_outcome"] = ssh.last_outcome
        if journal_owned and args.report_dir.is_dir() and not args.report_dir.is_symlink():
            path = args.report_dir / ("gateway-release-failure-" + str(os.getpid()) + "-" + str(int(time.time())) + ".json")
            rr.atomic_json(path, failure)
        print(json.dumps(failure, ensure_ascii=False))
        return exit_code
    finally:
        if ssh is not None:
            ssh.close()
        if client is not None:
            client.close()  # no retained refresh token; access remains only in this process
            client.access = None


if __name__ == "__main__":
    sys.exit(main())

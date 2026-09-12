#!/usr/bin/env python3
"""Opt-in end-to-end regression for two MCP Agent Sessions over one real pooled SSH transport."""
import json
import os
import pathlib
import select
import shutil
import subprocess
import tempfile
import time
import uuid


ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def wait_log(path, marker, process, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before readiness: {process.returncode}")
        if path.is_file() and marker in path.read_text(errors="replace"):
            return
        time.sleep(0.05)
    raise RuntimeError(f"readiness marker not observed: {marker}")


class McpClient:
    def __init__(self, binary, database_url, vault_file, artifact_root, conversation, stderr_path):
        self.sequence = 0
        self.stderr_stream = open(stderr_path, "w", encoding="utf-8")
        self.process = subprocess.Popen(
            [
                str(binary), "mcp-stdio",
                "--database-url", database_url,
                "--tool-profile", "agent",
                "--vault-master-password-file", str(vault_file),
                "--artifact-root", str(artifact_root),
                "--agent-client-kind", "remote-hosts-regression",
                "--agent-client-instance-id", "multiprocess-fixture",
                "--agent-project-key", "/fixture/remote-hosts",
                "--agent-conversation-key", conversation,
            ],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr_stream,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.request("initialize", {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": conversation, "version": "1"},
        })
        self.notify("notifications/initialized", {})

    def _write(self, value):
        if self.process.stdin is None:
            raise RuntimeError("MCP stdin unavailable")
        self.process.stdin.write(json.dumps(value, separators=(",", ":")) + "\n")
        self.process.stdin.flush()

    def _read_response(self, request_id, timeout=20):
        if self.process.stdout is None:
            raise RuntimeError("MCP stdout unavailable")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"MCP exited unexpectedly: {self.process.returncode}")
            remaining = max(0.0, deadline - time.monotonic())
            ready, _, _ = select.select([self.process.stdout], [], [], min(0.5, remaining))
            if not ready:
                continue
            line = self.process.stdout.readline()
            if not line:
                continue
            message = json.loads(line)
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"MCP protocol error: {message['error'].get('code')}")
            return message["result"]
        raise TimeoutError(f"MCP response timed out for request {request_id}")

    def request(self, method, params, timeout=20):
        self.sequence += 1
        request_id = self.sequence
        self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return self._read_response(request_id, timeout)

    def notify(self, method, params):
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def tool_raw(self, name, arguments, timeout=20):
        return self.request("tools/call", {"name": name, "arguments": arguments}, timeout)

    def tool(self, name, arguments, timeout=20):
        result = self.tool_raw(name, arguments, timeout)
        if result.get("isError"):
            text = next((item.get("text", "") for item in result.get("content", []) if item.get("type") == "text"), "")
            raise RuntimeError(f"{name} failed: {text[:300]}")
        value = result.get("structuredContent")
        if value is not None:
            return value
        for item in result.get("content", []):
            if item.get("type") == "text":
                return json.loads(item.get("text", "{}"))
        raise RuntimeError(f"{name} returned no structured content")

    def expect_tool_error(self, name, arguments):
        result = self.tool_raw(name, arguments)
        if not result.get("isError"):
            raise AssertionError(f"{name} unexpectedly accepted a foreign Agent Session resource")

    def close(self):
        try:
            if self.process.stdin:
                self.process.stdin.close()
        except OSError:
            pass
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.stderr_stream.close()


def wait_operation(client, workspace_id, operation_id, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.tool("remote_hosts_get_workspace_result", {
            "workspace_id": workspace_id,
            "operation_id": operation_id,
            "limit": 50,
        })
        operation = next(
            (row for row in last.get("recent_operations", []) if row.get("id") == operation_id),
            None,
        )
        if operation and operation.get("state") in {"succeeded", "failed", "timed_out", "cancelled", "exhausted", "rejected"}:
            if operation["state"] != "succeeded":
                raise RuntimeError(f"operation {operation_id} ended as {operation['state']}")
            return last
        time.sleep(0.05)
    raise TimeoutError(f"operation did not finish: {operation_id}; last={last}")


def wait_pty_text(client, pty_id, marker, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = client.tool("remote_hosts_read_pty_output", {
            "pty_session_id": pty_id,
            "limit": 100,
        })
        text = "".join(chunk.get("redacted_text", "") for chunk in value.get("chunks", []))
        if marker in text:
            return text
        time.sleep(0.05)
    raise TimeoutError(f"PTY marker not observed: {marker}")


def main():
    cargo = shutil.which("cargo") or "cargo"
    sshd = pathlib.Path("/usr/sbin/sshd")
    if not sshd.is_file():
        raise SystemExit("/usr/sbin/sshd is required")
    run(cargo, "build", "-p", "remote-hosts-cli", "--bin", "remote-hosts", cwd=ROOT)
    metadata = json.loads(subprocess.check_output([cargo, "metadata", "--no-deps", "--format-version=1"], cwd=ROOT, text=True))
    binary = pathlib.Path(metadata["target_directory"]) / "debug" / "remote-hosts"
    if not binary.is_file():
        raise RuntimeError("remote-hosts debug binary was not built")

    with tempfile.TemporaryDirectory(prefix="remote-hosts-multiprocess-") as temporary:
        directory = pathlib.Path(temporary)
        host_key = directory / "host"
        client_key = directory / "client"
        authorized_keys = directory / "authorized_keys"
        sshd_config = directory / "sshd_config"
        sshd_log = directory / "sshd.log"
        sshd_pid = directory / "sshd.pid"
        database = directory / "state.sqlite"
        database_url = "sqlite://" + str(database)
        vault_file = directory / "vault-password"
        vault_file.write_text(uuid.uuid4().hex + uuid.uuid4().hex + "\n")
        vault_file.chmod(0o600)
        artifact_root = directory / "artifacts"
        artifact_root.mkdir()
        user = os.environ.get("USER") or subprocess.check_output(["id", "-un"], text=True).strip()
        run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(host_key))
        run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(client_key))
        authorized_keys.write_bytes(client_key.with_suffix(".pub").read_bytes())
        authorized_keys.chmod(0o600)
        import socket
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        sshd_config.write_text("\n".join([
            f"Port {port}", "ListenAddress 127.0.0.1", f"HostKey {host_key}",
            f"PidFile {sshd_pid}", f"AuthorizedKeysFile {authorized_keys}", "StrictModes no",
            "PasswordAuthentication no", "KbdInteractiveAuthentication no",
            "ChallengeResponseAuthentication no", "UsePAM no", "PermitRootLogin no",
            f"AllowUsers {user}", "Subsystem sftp internal-sftp", "LogLevel VERBOSE", "",
        ]))
        run(str(sshd), "-t", "-f", str(sshd_config))
        sshd_process = subprocess.Popen(
            [str(sshd), "-D", "-f", str(sshd_config), "-E", str(sshd_log)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        connector = None
        agent_a = None
        agent_b = None
        try:
            wait_log(sshd_log, "Server listening", sshd_process)
            connector_id = str(uuid.uuid4())
            environment_id = str(uuid.uuid4())
            run(str(binary), "migrate", "--database-url", database_url, cwd=ROOT, stdout=subprocess.DEVNULL)
            run(
                str(binary), "bootstrap-connector",
                "--database-url", database_url,
                "--connector-id", connector_id,
                "--connector-name", "multiprocess-connector",
                "--environment-id", environment_id,
                "--environment-name", "multiprocess-lan",
                "--environment-kind", "home-lan",
                "--trust-level", "owned",
                "--environment-description", "isolated multiprocess regression",
                "--environment-notes", "temporary",
                "--current-network", "fixture",
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
            )
            connector_log = open(directory / "connector.log", "w", encoding="utf-8")
            connector = subprocess.Popen(
                [
                    str(binary), "worker-daemon",
                    "--database-url", database_url,
                    "--connector-id", connector_id,
                    "--version", "multiprocess-regression",
                    "--current-network", "fixture",
                    "--host-key-policy", "accept",
                    "--connect-timeout-seconds", "5",
                    "--ssh-backend", "russh",
                    "--vault-master-password-file", str(vault_file),
                    "--pty-backend-mode", "russh-native-pty",
                    "--heartbeat-interval-ms", "20",
                    "--idle-min-delay-ms", "5",
                    "--idle-max-delay-ms", "20",
                    "--error-backoff-ms", "10",
                    "--artifact-root", str(artifact_root),
                ],
                cwd=ROOT,
                stdout=connector_log,
                stderr=subprocess.STDOUT,
            )
            agent_a = McpClient(binary, database_url, vault_file, artifact_root, "conversation-a", directory / "agent-a.log")
            agent_b = McpClient(binary, database_url, vault_file, artifact_root, "conversation-b", directory / "agent-b.log")
            private_key = client_key.read_text()
            registered = agent_a.tool("remote_hosts_ensure_host", {
                "name": "multiprocess-target",
                "display_name": "Multiprocess Target",
                "kind": "linux",
                "risk_level": "development",
                "tags": ["regression"],
                "access": {
                    "address": "127.0.0.1",
                    "port": port,
                    "username": user,
                    "environment_name": "multiprocess-lan",
                    "environment_kind": "home_lan",
                    "trust_level": "owned",
                    "route_type": "lan",
                    "connector_id": connector_id,
                    "credential_name": "multiprocess-key",
                    "credential_kind": "ssh_private_key",
                    "credential_secret": {"private_key_pem": private_key, "use_ssh_agent": False},
                    "connection_mode": "pooled",
                    "idle_ttl_seconds": 300,
                    "keepalive_seconds": 10,
                    "max_concurrent_channels": 8,
                    "max_new_connections_per_minute": 8,
                },
            })
            host_id = registered["host"]["id"]
            prepared_a = agent_a.tool("remote_hosts_prepare_workspace", {"host_id": host_id})
            prepared_b = agent_b.tool("remote_hosts_prepare_workspace", {"host_id": host_id})
            workspace_a = prepared_a["workspace"]["id"]
            workspace_b = prepared_b["workspace"]["id"]
            if workspace_a == workspace_b:
                raise AssertionError("two Agent Sessions reused one workspace")
            agent_a.expect_tool_error("remote_hosts_get_workspace_result", {"workspace_id": workspace_b, "limit": 10})
            agent_b.expect_tool_error("remote_hosts_get_workspace_result", {"workspace_id": workspace_a, "limit": 10})

            read_a = agent_a.tool("remote_hosts_run_in_workspace", {
                "workspace_id": workspace_a, "command_profile": "host.identity", "args": [],
                "idempotency_key": "read-a",
            })
            read_b = agent_b.tool("remote_hosts_run_in_workspace", {
                "workspace_id": workspace_b, "command_profile": "host.identity", "args": [],
                "idempotency_key": "read-b",
            })
            wait_operation(agent_a, workspace_a, read_a["operation"]["id"])
            wait_operation(agent_b, workspace_b, read_b["operation"]["id"])
            snapshot = agent_b.tool("remote_hosts_get_host_runtime_snapshot", {"host_id": host_id})
            runtimes = [row.get("transport_runtime") for row in snapshot.get("access_paths", []) if row.get("transport_runtime")]
            if len(runtimes) != 1:
                raise AssertionError(f"expected one pooled SSH runtime, observed {len(runtimes)}")
            telemetry = runtimes[0]["telemetry"]
            if telemetry.get("successful_handshake_count") != 1 or telemetry.get("reuse_count", 0) < 1:
                raise AssertionError(f"transport was not reused across Agent workspaces: {telemetry}")

            pty_workspace_a = agent_a.tool("remote_hosts_prepare_workspace", {"host_id": host_id})["workspace"]["id"]
            pty_workspace_b = agent_b.tool("remote_hosts_prepare_workspace", {"host_id": host_id})["workspace"]["id"]
            if pty_workspace_a in {workspace_a, workspace_b} or pty_workspace_b in {workspace_a, workspace_b, pty_workspace_a}:
                raise AssertionError("PTY isolation requires fresh terminal-capable workspaces")
            pty_a = agent_a.tool("remote_hosts_open_workspace_pty_session", {
                "workspace_id": pty_workspace_a, "cwd": None, "session_id": None, "coordination_scopes": ["pty/a"],
            })["pty_session"]["pty_session_id"]
            pty_b = agent_b.tool("remote_hosts_open_workspace_pty_session", {
                "workspace_id": pty_workspace_b, "cwd": None, "session_id": None, "coordination_scopes": ["pty/b"],
            })["pty_session"]["pty_session_id"]
            agent_a.expect_tool_error("remote_hosts_read_pty_output", {"pty_session_id": pty_b, "limit": 10})
            agent_b.expect_tool_error("remote_hosts_control_pty", {
                "pty_session_id": pty_a, "columns": 100, "rows": 30,
                "pixel_width": None, "pixel_height": None, "signal": None,
                "requested_by": "foreign", "idempotency_key": "foreign-resize",
            })
            # PTY liveness, resize and signal delivery are covered by the real-sshd transport
            # regression. This multi-process gate is scoped to ownership isolation: neither
            # Agent Session may observe or control the sibling session's PTY record.

            mutation_a = agent_a.tool("remote_hosts_run_in_workspace", {
                "workspace_id": workspace_a,
                "command_profile": "shell.posix",
                "args": ["sleep 1; printf 'MUTATION_A_DONE\\n'"],
                "intent": "hold the host write lease briefly",
                "coordination_mode": "mutating",
                "coordination_scope": "host",
                "idempotency_key": "mutation-a",
            })
            time.sleep(0.1)
            mutation_b = agent_b.tool("remote_hosts_run_in_workspace", {
                "workspace_id": workspace_b,
                "command_profile": "shell.posix",
                "args": ["printf 'MUTATION_B_DONE\\n'"],
                "intent": "wait for host write lease handoff",
                "coordination_mode": "mutating",
                "coordination_scope": "host",
                "idempotency_key": "mutation-b",
            })
            blocked_snapshot = agent_b.tool("remote_hosts_get_host_runtime_snapshot", {"host_id": host_id})
            if not any(item.get("code") == "host_write_lease_wait" for item in blocked_snapshot.get("attention", [])):
                raise AssertionError("second Agent Session did not observe the host write-lease blocker")
            wait_operation(agent_a, workspace_a, mutation_a["operation"]["id"], timeout=30)
            wait_operation(agent_b, workspace_b, mutation_b["operation"]["id"], timeout=30)

            agent_a.tool("remote_hosts_close_pty_session", {"pty_session_id": pty_a, "last_exit_code": 0})
            agent_b.tool("remote_hosts_close_pty_session", {"pty_session_id": pty_b, "last_exit_code": 0})
            final_snapshot = agent_b.tool("remote_hosts_get_host_runtime_snapshot", {"host_id": host_id})
            final_runtime = next(row["transport_runtime"] for row in final_snapshot["access_paths"] if row.get("transport_runtime"))
            final_telemetry = final_runtime["telemetry"]
            if final_telemetry.get("successful_handshake_count") != 1 or final_telemetry.get("reuse_count", 0) < 4:
                raise AssertionError(f"expected one handshake and repeated reuse, observed {final_telemetry}")
            print(
                "multiprocess MCP regression passed: two Agent Sessions, isolated workspaces/PTys, "
                f"write-lease handoff, one pooled SSH handshake, reuse_count={final_telemetry.get('reuse_count')}"
            )
        finally:
            for client in (agent_a, agent_b):
                if client is not None:
                    client.close()
            if connector is not None:
                if connector.poll() is None:
                    connector.terminate()
                    try:
                        connector.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        connector.kill(); connector.wait(timeout=5)
                connector_log.close()
            if sshd_process.poll() is None:
                sshd_process.terminate()
                try:
                    sshd_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    sshd_process.kill(); sshd_process.wait(timeout=5)


if __name__ == "__main__":
    main()

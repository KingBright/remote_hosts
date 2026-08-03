# Deployment and Operations

This guide covers the local macOS service installation and production route practices. Architecture
and protocol details live in [Architecture and Runtime](architecture-and-runtime.md).

## Local Paths

The service manager uses these defaults:

| Purpose | Path |
| --- | --- |
| Binary | `~/.local/bin/remote-hosts` |
| Configuration | `~/.config/remote-hosts/service.env` |
| Vault master key | `~/.config/remote-hosts/vault-master-password` |
| SQLite database | `~/.local/share/remote-hosts/remote-hosts.sqlite` |
| Output artifacts | `~/.local/share/remote-hosts/artifacts` |
| Hot-swappable admin UI | `~/.local/share/remote-hosts/ui/admin.html` |
| Logs | `~/.local/state/remote-hosts/logs` |

## launchd Services

Install and start everything with:

```bash
scripts/remote-hosts-service install
```

The script builds a release binary, runs migrations, creates a default local connector, writes user
LaunchAgents, installs the admin UI, synchronizes the Agent Skill, and starts:

- `com.remote-hosts.api`: HTTP API on `127.0.0.1:8787`.
- `com.remote-hosts.connector`: connector daemon for operations, transfers, and PTY input.

Both services start automatically at user login. MCP stdio is not a daemon; Codex or Antigravity
starts a child process with the shared database and vault-key path.

## Lifecycle Commands

```bash
scripts/remote-hosts-service status
scripts/remote-hosts-service logs
scripts/remote-hosts-service stage
scripts/remote-hosts-service update
scripts/remote-hosts-service restart
scripts/remote-hosts-service skills
scripts/remote-hosts-service ui
scripts/remote-hosts-service stop
```

- `stage` rebuilds and installs the binary, runs migrations, and refreshes launchd, Skill, and UI
  files without restarting running processes.
- `update` performs staging and restarts only after the drain gate reports no queued or running
  operations, active or pending PTYs, queued PTY input, or unexpired write leases.
- `restart` applies the same drain gate to the installed version.
- `--force` intentionally interrupts active conversations and should be used only when the operator
  has accepted that loss of PTY continuity.
- `skills` synchronizes `skills/remote-hosts-agent` into Codex and Antigravity.
- `ui` atomically replaces only the external administration page; the API and connector do not
  restart.

The API reads `REMOTE_HOSTS_ADMIN_HTML_PATH` on every `/admin` request and falls back to its embedded
page. After a UI-only update, reload the browser.

## Configuration

The generated `service.env` configures the database, artifact root, connector identity, current
network, SSH backend, host-key policy, timeouts, and worker concurrency. Local installation defaults
to native `russh`, PTY backend mode `auto`, and a bounded operation worker pool.

Do not put plaintext credentials in `service.env`. Host passwords and private keys belong in the
encrypted credential tools. The vault-key file unlocks those encrypted rows and remains local with
mode `0600`.

## Normal Agent Operation

1. Resolve or register the canonical host and intended access path.
2. Read runtime snapshot version 8 before reasoning about transport or channel state.
3. Prepare a Workspace with a stable coordination scope.
4. Run `shell.posix`, `shell.powershell`, or open a persistent PTY.
5. Reuse the Workspace and semantic idempotency key while waiting or retrying.
6. Use the transfer tools for files and artifact tools for large command output.

Do not open a raw `ssh` process beside Remote Hosts. That bypasses pooling, Agent Session isolation,
state reporting, rate protection, and audit.

## Bastions and Internal Assets

Register the real externally reachable endpoint. When it exposes an interactive asset menu, use a
`bastion` route with `requires_tty=true` and operate through one persistent PTY per conversation.
Several canonical hosts may share that physical endpoint while retaining distinct inventory IDs.

Never infer a direct-login username or register an internal address as externally reachable because
another environment happens to allow it. A `username/server/account` gateway convention is valid
only when the owner documents it and a bounded read-only probe confirms it.

`remote_hosts_run_in_workspace` rejects routes that require a TTY. For those routes:

1. Open the Workspace PTY.
2. Read the banner or menu.
3. Select the intended asset.
4. Keep commands and interactive file transfers in that PTY.

Real `proxy_jump` routes and non-empty proxy chains fail before handshake until a verified proxy-aware
implementation is configured. The connector never silently bypasses a declared chain.

## Files and Release Artifacts

Use Remote Hosts for bounded commands, configuration, diagnostics, and moderate file transfer. Use a
resumable object store or OCI registry for large release assets that many targets will consume, then
run only the placement or deployment command through the pooled Workspace.

When a transfer is interrupted, read its existing operation result first. A retained partial reports
`resumed_bytes`; an already-completed matching destination converges to `completed`. Do not start
MinIO, Harbor, web upload, and SSH fallback concurrently for the same destination.

## Retry Policy

| Operation | Automatic retry |
| --- | --- |
| Read-only check | Allowed within bounded operation policy |
| Verified transfer initialization or chunk | Allowed because remote state and digest are checked |
| Already-placed transfer destination | Allowed to converge after size and SHA-256 verification |
| Arbitrary mutation with unknown result | Stop and inspect current state |
| Deploy, restart, migration, or destructive cleanup | Never replay only because the SSH response was lost |

Keep the same semantic idempotency key and exact payload when retrying one logical action. A timeout
while waiting does not justify a new key or a parallel operation.

## Handshake and Capacity Diagnosis

`local_handshake_budget_exhausted` is local protection, not proof that the target SSH daemon rejected
the client. Respect the exact `retry_after_seconds` and keep the same Workspace. Creating another
Workspace or conversation does not bypass the shared connector budget.

`channel_capacity.state=saturated` means the authenticated transport is busy. Wait for the existing
reservation to clear; do not mark the route unhealthy or open another connection. Increase
`max_concurrent_channels` only after confirming the target and bastion support it.

Transport evidence is the source of truth:

- `reused` proves a pooled authenticated connection served the channel.
- `first_handshake` proves a new connection was established.
- `reconnected` means a failed connection was replaced inside the same runtime.
- `attempt_failed` records a real failed attempt.

A logical session row alone does not prove a transport is alive.

## PTY Recovery

After connector restart, old live shells cannot be recovered from database state. They are marked
`blocked/failed`, input is disabled, and the agent must prepare or activate a new Workspace/PTY as
appropriate. The service never sends queued input to a replacement shell with unknown context.

If activation remains pending, inspect channel capacity and follow `poll_after_ms`. If it becomes
failed, read the PTY output and runtime snapshot; do not loop on the same PTY ID.

## Host-Key Policy

`strict` requires a known matching key. `add` accepts a new host and rejects changes. `accept` is an
explicit productivity tradeoff for controlled VPN-contained routes. Relaxing host-key verification
does not relax host identity, environment separation, mutation scoping, or artifact digest checks.

## Operational Checks

```bash
scripts/remote-hosts-service status
curl -fsS http://127.0.0.1:8787/v1/command-profiles
scripts/remote-hosts-service logs
```

For a fresh MCP smoke test, start a new MCP stdio child and verify that the Agent profile exposes 18
tools, including upload and download, and that a read-only runtime snapshot reports version 8.

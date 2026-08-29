# Deployment and Operations

This guide covers local service installation and production route practices. Architecture and
protocol details live in [Architecture and Runtime](architecture-and-runtime.md); native Windows
packaging and Task Scheduler operations live in [Windows Installation and Operations](windows.md).

## macOS Local Paths

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

## Instance Sync Listener

The normal API remains loopback-only. To let another approved Remote Hosts installation synchronize
with this one, set `REMOTE_HOSTS_PEER_SYNC_BIND` in `~/.config/remote-hosts/service.env` to a
separate address such as `0.0.0.0:8788`, then apply it during the next planned service update. The
additional listener exposes only `/healthz`, `/v1/health`, instance identity, and authenticated
instance-sync export/receive routes. It cannot access the admin UI, credential inspection or
management routes, topology bindings, Workspaces, PTYs, commands, files, or activity records. A
credential selected for sync remains peer-sealed in the envelope and is re-encrypted by the
receiver's local vault.

Use a trusted LAN, VPN, or an SSH tunnel for the peer listener. Use HTTPS before exposing it through
any routed or public network. Leave the setting empty when no direct peer needs to reach the
instance; that remains the default on macOS and Windows.

## Windows Services

The Windows package runs the same API, connector, SQLite database, encrypted vault, admin UI, and
Agent Skill. It uses current-user Task Scheduler jobs and the native `russh` backend. Versioned
release directories avoid replacing a running `.exe`, while a small stable Rust launcher lets new
MCP children select the current release without a permanent PowerShell proxy. Installation and
lifecycle commands are documented in [Windows Installation and Operations](windows.md).

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

Before a planned restart, run `restart-readiness` and identify the Agent Sessions that own every
reported operation, PTY, queued input, and write lease. Notify only those active Codex or
Antigravity conversations, ask them to finish their current atomic step, close their PTY/Workspace,
and stop creating new Remote Hosts work. The notice must also tell them to reload MCP after the
upgrade and follow the compact Agent response contract. Wait for the normal drain gate to pass;
never use `--force` merely because notification or draining takes time.

An explicitly `closed` Workspace is an authoritative cancellation boundary. Migration `0023`, the
restart-readiness command, and Connector lifecycle reconciliation cancel any queued/running
operation still attached to it, clear its connector claim, and release its active-work write lease.
The scheduler also refuses to reclaim that work. This cleanup is idempotent and does not touch PTYs,
operations, or leases owned by another Workspace. Do not edit SQLite or use `--force` to clear a
closed-Workspace residue. `done`, `failed`, and `throttled` are not treated as explicit closure:
they retain existing concurrent-operation and bounded lease-handoff behavior.

The API reads `REMOTE_HOSTS_ADMIN_HTML_PATH` on every `/admin` request and falls back to its embedded
page. After a UI-only update, reload the browser.

The admin page separates infrastructure topology from `Agent 活动`. The activity view reads the
bounded `/v1/admin/activity` feed and shows target host, Agent/project identity, redacted command or
PTY input preview, status, exit code, duration, result, and expandable transport details. It is the
operator-facing audit surface; raw MCP JSON is not intended for routine human inspection.

## Configuration

The generated macOS `service.env` or Windows `service.json` configures the database, artifact root,
connector identity, current network, SSH backend, host-key policy, timeouts, worker concurrency, and
an optional restricted peer-sync bind. `REMOTE_HOSTS_PEER_SYNC_BIND` on macOS and `PeerSyncBind` on
Windows are empty by default.
Local installation defaults to native `russh`, PTY backend mode `auto`, and a bounded operation
worker pool. Connector bootstrap and restart readiness are implemented by the Rust CLI, so neither
platform needs an external `sqlite3` executable for service management.

Lifecycle defaults are `REMOTE_HOSTS_PTY_IDLE_TTL_SECONDS=3600` and
`REMOTE_HOSTS_PTY_BUSY_TTL_SECONDS=86400`. The first applies when no foreground process is
declared; the second applies to a quiet long-running PTY that agents heartbeat truthfully through
`remote_hosts_heartbeat_pty_session`. Output and accepted input refresh activity automatically.
Set one value to zero only to disable that expiry class. Each access path's `idle_ttl_seconds`
independently controls zero-channel SSH transport retention; keepalive probes do not extend it.

Do not put plaintext credentials in the service config. Host passwords and private keys belong in
the encrypted credential tools. The vault-key file unlocks those encrypted rows and remains local
with mode `0600` on macOS or a current-user ACL on Windows.

## Normal Agent Operation

1. Resolve or register the canonical host and intended access path.
2. Read runtime snapshot version 11 before reasoning about state. Treat host-level logical `workspace_capacity` and per-access-path SSH `channel_capacity` as independent limits; inspect exact `coordination_scopes`, and treat `pty_input_required` as an active PTY waiting for input rather than an SSH failure.
3. Prepare a Workspace with a stable coordination scope.
4. Run `shell.posix`, `shell.powershell`, or open a persistent PTY.
5. Heartbeat a quiet long-running PTY with its truthful foreground process; do not send dummy input as keepalive.
6. Reuse the Workspace and semantic idempotency key while waiting or retrying.
7. Use the transfer tools for files and artifact tools for large command output.

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

## Observed macOS Resource Footprint

The following sample was measured on the owner's Mac on 2026-08-03 with the release build managed by
launchd. It represents an active, history-heavy installation rather than an empty database:

| Process | RSS | CPU sample | Threads |
| --- | ---: | ---: | ---: |
| API | 8.8 MiB | 0.0% | 14 |
| Connector | 47.1 MiB | 1.38% five-sample average | 23 |
| Combined | about 55.9 MiB | mostly connector activity | 37 |

The sample contained 88 hosts, 53 access paths, 1,555 Workspaces, 1,791 operations, 379 PTYs, 384
topology nodes, and 1,207 topology edges. The release binary was 23 MiB. The data directory used 141
MiB, including a 127 MiB SQLite database, a 7 MiB WAL, and 3.8 MiB of artifacts; logs used 192 KiB.

Most disk use came from retained PTY and command output; host, route, topology, credential, and
knowledge records were a small minority. These numbers are useful as a real daily-use reference,
not a clean-install minimum or a guaranteed Windows footprint. Live SSH transports, active PTYs,
large output retention, tracing level, and concurrent MCP children will change memory, CPU, and
storage use.

## Compressed Output Storage

Current releases support low-latency compressed PTY batches. Each segment stores repeated Session
and Workspace metadata once, encodes logical chunks with Postcard, and compresses the result with
Zstandard level 9. Command output uses the same format and appends small chunks to a bounded segment.
The API and MCP repositories decompress transparently, preserving chunk ids, sequence cursors,
timestamps, redaction, stream identity, and truncation flags.

A binary update leaves compressed-only writes disabled. New API and Connector processes therefore
continue writing the legacy tables until the migration is explicitly activated, so resident older
MCP children do not lose output. Reload every Codex and Antigravity MCP child, wait for operations,
PTYs, queued input, and write leases to drain, then run:

```bash
scripts/remote-hosts-service optimize-storage
```

The service helper supplies the CLI's explicit `--activate-compressed-writes` confirmation. The
command processes one Session or operation per short transaction, verifies the encoded payload
before deleting exact legacy row ids, updates planner statistics, vacuums the database, then enables
compressed writes and prints before/after JSON counters. It does not restart either service. A
failed migration never activates compressed-only writes. Physical reclamation refuses to run while
operations, live PTYs, queued PTY input, or write leases remain. `--force` exists for an intentional
maintenance window, not routine use. Running the command again is idempotent.

Migration `0018` also terminalizes queued or claimed input for a PTY or Workspace that can no longer
deliver it. It clears the raw input payload and keeps redacted failure metadata, preventing stale
input from blocking a later service restart.

On a consistent copy of the 2026-08-03 daily-use database, 111,480 PTY chunks and 15,921 command
chunks contained 67.2 MB of redacted text. They became 2,199 compressed segments with 11.67 MB of
payload. After `VACUUM`, the SQLite file fell from 139.7 MB to 26.2 MB, an 81.3% physical reduction;
the complete migration took about 14 seconds with the Release binary on the sampled Mac. The source
database and running services were not modified during this benchmark.

## Operational Checks

```bash
scripts/remote-hosts-service status
curl -fsS http://127.0.0.1:8787/v1/command-profiles
scripts/remote-hosts-service logs
```

For a fresh MCP smoke test, start a new MCP stdio child and verify that the Agent profile exposes 18
tools, including upload and download, that `prepare_workspace` returns compact identity plus
`next_action`, and that a read-only runtime snapshot reports version 10.

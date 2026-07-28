# Remote Hosts

Remote Hosts is a Rust-based remote SSH host knowledge, state, access, and execution center designed for AI agents and human operators.

The project is built around four production planes:

- Knowledge Plane: hosts, environments, facts, software installs, runbooks, and operation history.
- Access Plane: connector-aware access paths, credentials, proxy chains, and route selection.
- State Plane: connector, access path, SSH session, workspace, operation, and host health state.
- Execution Plane: guarded remote checks, connection/session reuse, server protection, redaction, and auditability.

The product boundary is intentionally narrow. Remote Hosts owns reliable SSH transport reuse, generic POSIX/PowerShell execution, PTY, file streams, output artifacts, state, credentials, and audit. It does not add separate MCP tools for Kubernetes, Harbor, databases, GPU frameworks, or each operational product. Agents and users run those tools through `shell.posix`, `shell.powershell`, or PTY on the reused connection; repeatable domain workflows can live as optional runbooks outside the core tool surface.

The implementation priority is transport-first: keep one healthy SSH transport per access path, expose its state to the agent, and run arbitrary commands through short exec channels or a persistent PTY without another handshake. Host registry, credentials, files, artifacts, and knowledge support that channel. Narrow read-only command profiles are optional shortcuts, not the primary extensibility mechanism.

See [REMOTE_HOSTS_PRODUCT_TECH_PLAN.md](REMOTE_HOSTS_PRODUCT_TECH_PLAN.md) for the full product and technical plan.

## Release Highlights (2026-07-27)

This release turns the recent topology and transport work into one coherent Agent-facing product:

- Reliable pooled execution: POSIX commands carry an in-band completion frame so the connector
  recovers the real exit status even when a gateway omits or falsifies SSH exit-status messages.
  Healthy commands keep the authenticated transport pooled; a timeout, channel failure, or missing
  completion frame invalidates it before one bounded reconnect.
- Resilient file operations: direct routes use pooled SFTP, non-interactive single-hop bastions
  can use bounded exec frames, and interactive asset-menu bastions reuse the workspace's already
  selected PTY. Stable partial files, prefix verification, idempotent chunks, atomic placement,
  progress events, and connector-restart recovery prevent duplicate bytes and blind restarts.
- Observable long work: transfers publish verified byte progress and a 30-second operation
  heartbeat, while the connector daemon continues its own health heartbeat during long commands.
  Progress persistence is diagnostic and cannot cancel an otherwise healthy data channel.
- Agent isolation with physical reuse: each Codex or Antigravity task owns its Agent Session,
  workspace, PTY, operations, artifacts, and idempotency keys. Tasks still share one connector-owned
  SSH transport per access path and coordinate mutations through a host write lease.
- Infrastructure inventory: the loopback admin console and topology API model clusters, hosts,
  services, dependencies, inactive history, and encrypted credential bindings without adding
  product-specific MCP tools.
- Reproducible local operation: launchd keeps the API and connector available after login,
  install/update passes the local vault key correctly, and the repository-owned Agent Skill is
  synchronized into Codex and Antigravity.

## Current Workspace

```text
crates/
  remote-hosts-domain      Shared IDs, entities, state enums, operation/workspace types
  remote-hosts-core        Command profiles, connector state, workspace supervision, policy, redaction, transport trait
  remote-hosts-vault       Internal Argon2id + XChaCha20-Poly1305 credential vault
  remote-hosts-db          SQLx migrations and repositories
  remote-hosts-connector   Guarded transports, OpenSSH provider cache, connector operation worker
  remote-hosts-api         Axum HTTP API for host/access/state/workspace surfaces
  remote-hosts-mcp         MCP tool contract constants and request schemas
  remote-hosts-cli         Admin CLI
migrations/                SQLite/Postgres-oriented schema migrations
```

## Development

The workspace pins Rust `1.94.1`.

```bash
cargo fmt --all
cargo test --workspace
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Run the CLI:

```bash
cargo run -p remote-hosts-cli -- doctor
cargo run -p remote-hosts-cli -- migrate --database-url sqlite://remote-hosts.sqlite
cargo run -p remote-hosts-cli -- serve --bind 127.0.0.1:8787 --vault-master-password-file ~/.config/remote-hosts/vault-master-password
cargo run -p remote-hosts-cli -- mcp-stdio --database-url sqlite://remote-hosts.sqlite --vault-master-password-file ~/.config/remote-hosts/vault-master-password
cargo run -p remote-hosts-cli -- worker-once --database-url sqlite://remote-hosts.sqlite --connector-id <uuid>
cargo run -p remote-hosts-cli -- worker-daemon --database-url sqlite://remote-hosts.sqlite --connector-id <uuid> --pty-backend-mode auto
cargo run -p remote-hosts-cli -- worker-daemon --database-url sqlite://remote-hosts.sqlite --connector-id <uuid> --ssh-backend russh --vault-master-password-file ~/.config/remote-hosts/vault-master-password
```

## macOS launchd Deployment

For a local macOS operator machine, use the launchd service manager script:

```bash
scripts/remote-hosts-service install
scripts/remote-hosts-service status
scripts/remote-hosts-service logs
scripts/remote-hosts-service stage
scripts/remote-hosts-service update
scripts/remote-hosts-service restart
scripts/remote-hosts-service skills
scripts/remote-hosts-service stop
```

`install` builds the release binary, installs it to `~/.local/bin/remote-hosts`, creates `~/.config/remote-hosts/service.env`, generates a mode-`0600` local vault key, migrates the SQLite database at `~/.local/share/remote-hosts/remote-hosts.sqlite`, creates a default local connector record, writes user LaunchAgents, installs the repository-owned `remote-hosts-agent` skill into Codex and Antigravity, and starts both services:

- `com.remote-hosts.api`: HTTP API on `127.0.0.1:8787`.
- `com.remote-hosts.connector`: long-running worker daemon for queued operations and PTY input.

The services start automatically when the user logs in. `stage` rebuilds the release binary, reruns migrations, and refreshes launchd and Skill files without interrupting active conversations. `update` performs the same staging work and restarts only when there are no queued/running operations, active PTYs, queued PTY inputs, or write leases; `restart` applies the same drain gate. `--force` is reserved for an intentional interruption. MCP stdio is not kept as a daemon; normal agent clients launch it with the database and the generated `--vault-master-password-file`. The task-level `remote_hosts_ensure_host` tool handles normal host registration, route updates, and optional encrypted credential capture; `remote_hosts_store_host_credential` rotates credentials for existing hosts. Use `admin` for low-level registry repair and `full` only for development/debugging.

Connector workers store large redacted stdout/stderr streams as file-backed artifacts instead of flooding agent context. The defaults can be tuned with `--artifact-root`, `--artifact-threshold-bytes`, and `--artifact-preview-bytes` on `worker-once` and `worker-daemon`. The daemon runs up to four queued operations concurrently by default; tune that bounded pool with `--max-concurrent-operations` or `REMOTE_HOSTS_MAX_CONCURRENT_OPERATIONS`. It also attaches a PTY backend factory and input pump by default; tune queued PTY input delivery with `--pty-input-lease-seconds` and `--pty-input-max-attempts`. The daemon defaults to `--pty-backend-mode auto`: `openssh` operations use `control-master-tty`, while `russh` operations use `russh-native-pty`. `control-master-tty` starts a persistent `ssh -tt` child through the existing OpenSSH ControlMaster socket, `russh-native-pty` opens a native SSH channel with `request-pty` on the pooled `russh` session, and `pipe-shell` remains the lower-overhead compatibility fallback.

Queued operation execution can use `--ssh-backend openssh` or `--ssh-backend russh`; local service installs default to `russh`. The native backend tries a stored key, at most two SSH-agent identities, default local keys when the agent is empty, and then the encrypted password. After password authentication it schedules an idempotent local-public-key install with its own 10-second timeout. The authenticated pooled session is available immediately and remains valid when bootstrap is denied, read-only, unsupported, timed out, or aborted. Bootstrap state and cooldown are persisted per access path and key fingerprint, capped at three transient failures, and exposed in MCP runtime snapshot version 6. Both transport pools reload route metadata before returning cached transports. `proxy_jump` and non-empty proxy chains are rejected before handshake until a proxy-aware implementation is configured, while an empty-chain `bastion` route is treated as one physical SSH connection for gateway usernames or interactive asset menus. The connector never silently bypasses a configured jump chain, validates host keys through `strict`, `add`, or `accept`, reuses a cached native session per access path, and never returns plaintext credential material through HTTP or MCP.

Current HTTP surfaces:

- `GET /` and `GET /admin` (embedded infrastructure management console)
- `GET /v1/admin/overview`
- `GET /v1/hosts`
- `GET /v1/hosts/{host_id}`
- `GET /v1/topology`
- `POST /v1/topology/sync`
- `GET /v1/topology/credential-bindings`
- `POST /v1/topology/nodes/{node_id}/credentials`
- `GET /v1/credentials`
- `GET /v1/hosts/{host_id}/access-paths`
- `GET /v1/hosts/{host_id}/resolve-access`
- `GET /v1/hosts/{host_id}/state`
- `GET /v1/command-profiles`
- `POST /v1/connectors/{connector_id}/heartbeat`
- `GET /v1/connectors/{connector_id}/events`
- `POST /v1/runtime-events/wait`
- `GET /v1/hosts/{host_id}/workspaces`
- `POST /v1/hosts/{host_id}/workspaces`
- `GET /v1/workspaces/{workspace_id}`
- `GET /v1/workspaces/{workspace_id}/operations`
- `POST /v1/workspaces/{workspace_id}/operations`
- `GET /v1/workspaces/{workspace_id}/output`
- `GET /v1/workspaces/{workspace_id}/output-artifacts`
- `GET /v1/output-artifacts/{artifact_id}`
- `POST /v1/workspaces/{workspace_id}/wait`
- `POST /v1/workspaces/{workspace_id}/close`
- `POST /v1/workspaces/{workspace_id}/state`
- `GET /v1/workspaces/{workspace_id}/pty-sessions`
- `POST /v1/workspaces/{workspace_id}/pty-sessions`
- `POST /v1/pty-sessions/{pty_session_id}/heartbeat`
- `GET /v1/pty-sessions/{pty_session_id}/output`
- `POST /v1/pty-sessions/{pty_session_id}/input`
- `GET /v1/pty-sessions/{pty_session_id}/input-events`
- `POST /v1/pty-sessions/{pty_session_id}/close`
- `POST /v1/pty-sessions/reap-expired`

## Infrastructure topology and credentials

The management console at `http://127.0.0.1:8787/admin` combines the host registry,
access-path health, connectors, infrastructure topology, and public credential metadata. The
topology is a generic directed graph, so hosts, virtual machines, clusters, reverse proxies,
middleware, databases, caches, queues, storage, and business services can be represented without
adding one product-specific table for every technology.

`POST /v1/topology/sync` accepts an authoritative snapshot for one `scope_key + source`. Repeating
the same snapshot is idempotent. Nodes and edges omitted by a later snapshot are marked inactive
for that source and scope, but are not deleted; another source can keep the same graph object
active. Use globally stable external keys such as `host:<host-id>`,
`cluster:<cluster-name>`, or `service:<cluster-name>:<service-name>`.

Topology metadata rejects secret-like keys. Store passwords, API tokens, database credentials,
service accounts, private keys, and arbitrary internal secrets through the node credential
endpoint or management form. They are encrypted with the existing local vault, and HTTP responses
contain metadata and bindings only. An unlocked HTTP vault is restricted to a loopback bind; use
an SSH tunnel for remote access to the console.

Example snapshot:

```json
{
  "scope_key": "cluster:factory-a",
  "source": "inventory-agent",
  "nodes": [
    {
      "external_key": "proxy:factory-a",
      "name": "Factory ingress",
      "kind": "reverse_proxy",
      "address": "10.20.0.10",
      "ports": [443],
      "metadata": {"software": "nginx"}
    },
    {
      "external_key": "service:factory-a:api",
      "name": "Factory API",
      "kind": "business_service",
      "address": "10.20.0.21",
      "ports": [8080]
    }
  ],
  "edges": [
    {
      "external_key": "factory-ingress-api",
      "from": "proxy:factory-a",
      "to": "service:factory-a:api",
      "relation": "proxies_to"
    }
  ]
}
```

### Agent topology workflow

The compact MCP profile remains transport-first and does not add product-specific cluster,
Kubernetes, Harbor, database, or middleware tool families. Agents discover those resources with
their existing remote CLIs through `shell.posix`, `shell.powershell`, or a persistent PTY, then use
the loopback topology API as the normalized inventory plane:

1. Read `GET /v1/topology` or `GET /v1/admin/overview` before changing inventory.
2. Build one complete snapshot for a single `scope_key + source`. Never submit a partial discovery
   result as authoritative, because omitted members become inactive for that scope and source.
3. Keep `external_key` values globally stable and link physical machines with `host_id` when a
   canonical Host record exists.
4. Submit the snapshot to `POST /v1/topology/sync`, then verify active and inactive counts through
   `GET /v1/topology?include_inactive=true`.
5. Keep passwords, tokens, private keys, and service accounts out of topology metadata. Bind them
   through the encrypted credential form or node credential endpoint. The launchd API wrapper
   unlocks the local vault only on its loopback bind.

The canonical Agent instructions live in `skills/remote-hosts-agent`. Running
`scripts/remote-hosts-service skills` synchronizes that directory to both
`~/.codex/skills/remote-hosts-agent` and
`~/.gemini/config/skills/remote-hosts-agent`. Existing Agent tasks may retain their already-loaded
Skill and MCP child until the client starts a new task or reloads its MCP/Skill state.

The default `agent` MCP profile exposes 18 task-oriented tools:

- `remote_hosts_list_hosts`
- `remote_hosts_ensure_host`
- `remote_hosts_store_host_credential`
- `remote_hosts_search_knowledge`
- `remote_hosts_record_knowledge`
- `remote_hosts_get_host_runtime_snapshot`
- `remote_hosts_prepare_workspace`
- `remote_hosts_run_in_workspace`
- `remote_hosts_upload_file`
- `remote_hosts_download_file`
- `remote_hosts_wait_workspace_state`
- `remote_hosts_get_workspace_result`
- `remote_hosts_read_output_artifact_content`
- `remote_hosts_open_workspace_pty_session`
- `remote_hosts_queue_pty_input`
- `remote_hosts_read_pty_output`
- `remote_hosts_close_pty_session`
- `remote_hosts_wait_runtime_events`

The `admin` profile exposes 36 tools by adding low-level host deduplication/upsert, environment, credential-reference, access-path, fact, connector, and workspace maintenance. The `full` profile exposes all 45 registered tools. Hidden tools are removed from both discovery and dispatch, so a caller cannot invoke them by guessing a name.

`remote_hosts_ensure_host` matches by stable slug, display name, and SSH endpoint, preserves the canonical slug, merges tags, rejects ambiguous cross-host matches, creates missing environment/access records, and can create a named encrypted password credential in the same call without returning the secret. Existing environment classification wins over a caller guess, and correcting only the route type for the same endpoint updates the canonical access path instead of creating a duplicate. `remote_hosts_store_host_credential` updates the only route automatically or requires an explicit access path when several exist, preserving secret fields not being replaced. `remote_hosts_prepare_workspace` reuses only an `idle` or `working` workspace owned by the current Agent Session before creating one; it never returns another conversation's workspace or a `throttled`, `blocked`, `failed`, or closed workspace. Runtime snapshot version 6 includes connector-local `transport_runtime` identity, backend, connection generation, handshake/reuse counters, timestamps, route capabilities, current Agent Session identity, and privacy-aware host `write_lease` state. Connection-session output keeps active/resolving sessions plus the latest and latest reusable session per enabled path, so restart history remains in the database without inflating every agent call or producing stale attention. `remote_hosts_get_workspace_result` returns bounded output chunks, recent operations, and artifact metadata in one call; `remote_hosts_read_output_artifact_content` reads the complete redacted artifact in bounded offset-based chunks.

Workspace execution is queue-first and connector-driven. `run_in_workspace` validates a built-in command profile, applies host protection policy, creates a queued `operation_run`, marks the workspace `working`, and writes an initial system output chunk. `shell.posix` and `shell.powershell` are the normal general-purpose path for deployment, diagnosis, and maintenance; narrow read-only profiles are shortcuts for common checks. Shell profiles use the same queue, pooled transport, connection limits, explicit intent, configurable timeout up to two hours, output cap, redaction, and bounded command summary. The connector daemon claims several eligible operations concurrently, while the host write lease and running-write guard keep mutating work strictly serial. Each operation binds to a reusable logical connection session, renews its lease during long remote work, executes through a cached guarded OpenSSH `ControlMaster` transport or native `russh` session, writes redacted output, and finishes only while it still owns the claim. OpenSSH and `russh` consume access-path keepalive settings, validate cached sessions before reuse, and rate-limit replacement SSH handshakes with independent per-path buckets plus one connector-wide shared budget. Cached raw and guarded transports are replaced when endpoint, route, host kind, keepalive, or connection-limit metadata changes. Operation and PTY backends share one access-path transport and one channel semaphore, so multiple conversations reuse one authentication lifecycle without exceeding the configured channel cap. Repeated connection failures open a persisted access-path circuit that blocks new work until cooldown instead of creating another session.

Logical connection sessions and real connector-local SSH transports are intentionally separate. Every OpenSSH and `russh` transport now owns a unique runtime id and records its lifecycle, successful connection generation, actual connection attempts, authenticated handshakes, cached-connection validations, and capabilities. Each exec, file-transfer, and PTY channel persists `transport_evidence` with one mutually exclusive `connection_use` value: `unchanged`, `reused`, `first_handshake`, `reconnected`, or `attempt_failed`. The independent `runtime_replaced` flag reports that a route change or connector restart created a different runtime object. Connector startup marks persisted runtimes `runtime_lost`; historical counters remain inspectable but can never masquerade as a live connection.

Conversation isolation is logical, not physical. MCP launchers create an Agent Session for each client process or derive a stable one from an explicit client-instance/conversation key. Workspaces, PTYs, queued inputs, operations, idempotency keys, output, artifacts, and temporary context belong to that Agent Session; the normal `agent` profile cannot inspect or operate another conversation's state. Logical workspaces have an independent host cap (32 by default) and do not consume SSH channels until used. All conversations share the connector's pooled SSH transport per access path; the production defaults allow four exec operations and eight persistent PTYs, further bounded by each access path's channel limit. Mutating shell operations, uploads, and PTY input coordinate through a crash-safe host write lease, while read-only operations remain eligible. Exact retries use a caller-supplied semantic `idempotency_key` scoped to the Agent Session. PTY input keeps the lease for 300 seconds, output activity renews it, and close, backend exit, or restart reconciliation shortens it to a bounded handoff period. The connector continuously reconciles externally closed PTY records with its in-memory backend handles so shell processes, channel permits, and logical open-channel counts cannot leak after a conversation closes its PTY.

File transfer is a first-class queued operation. `remote_hosts_upload_file` and `remote_hosts_download_file` bind to a workspace and access path, enforce configurable limits, reject unsafe local sources and path traversal, verify SHA-256 at both ends, and place data through a same-directory temporary file before rename. Direct routes use native SFTP. Non-interactive empty-chain POSIX bastions may use bounded exec frames. When a bastion exposes an interactive asset menu, uploads use the active connector-owned PTY from that same workspace after target selection; they never open an unrelated exec channel. The connector disables terminal echo before sending bounded Base64 chunks, while MCP and persisted audit records receive only stage, size, offset, and digest metadata. Stable partial files and prefix verification make chunk replay and later retries idempotent. Every chunk and final placement require an explicit remote marker plus integrity checks, and terminal settings are restored on success, failure, or timeout. A gateway that drops input, output, or markers is never treated as success. Transfer progress records the stage, verified bytes, total bytes, resumed bytes, retry count, and elapsed time; the worker writes a 30-second active heartbeat so long transfers do not disappear into a silent wait.

## Production Intranet Operations

Use Remote Hosts as the reusable control plane, not as a replacement for an artifact registry:

- Register the real externally reachable bastion endpoint. When that endpoint exposes an interactive asset menu, use `route_type=bastion`, `requires_tty=true`, and one persistent PTY per active conversation as the normal control path. Those PTYs remain logically isolated while sharing the bounded access-path transport.
- Use a `username/server/account` direct-login form only when the bastion owner explicitly documents that capability and a read-only exec-channel probe succeeds. Never infer it from another environment, and never register an internal asset address as an externally reachable route.
- Run bounded commands, checks, and small configuration transfers through the pooled Remote Hosts workspace.
- Publish large or repeatedly consumed release assets through resumable MinIO multipart upload or Harbor OCI layers. Once the object is verified, run only the small deployment command through Remote Hosts.
- Use `remote_hosts_upload_file` as a verified fallback for smaller artifacts or when the object-storage path is unavailable. Reusing the same local content and destination resumes the stable partial file instead of starting from zero.

Retry behavior is deliberately asymmetric:

| Operation | Automatic retry |
| --- | --- |
| Read-only check or command | Allowed within the bounded operation policy |
| Upload initialization, verified chunk, or final placement | Allowed, because each stage is idempotent and SHA-256 checked |
| MinIO multipart part or Harbor layer | Allowed only after remote size/digest verification |
| Arbitrary remote mutation with an unknown result | Not allowed; inspect state before deciding whether to continue |
| Deploy, restart, migration, or destructive cleanup | Never replay solely because the SSH response was lost |

For a VPN-contained route, operators may set `REMOTE_HOSTS_HOST_KEY_POLICY=accept` when the productivity tradeoff is explicitly accepted. This relaxes only SSH host-key verification. The access path must still be bound to the intended hostname/IP and production environment, mutating work must verify remote `hostname`/address first, release artifacts must retain SHA-256 verification, and Test/Prod credentials and destinations must remain separate. Do not identify a bastion asset by a mutable menu sequence number.

When a transfer is interrupted, read the workspace result before starting another path. A retained partial upload reports `resumed_bytes`; an already-completed matching destination converges to `stage=completed` with the full size retained in `resumed_bytes`. If neither appears, verify the local artifact, remote destination, and access-path identity before retrying. Do not run MinIO, Harbor, Web upload, and SSH fallback concurrently for the same destination.

Replacement handshakes are protected by atomic sliding-window budgets: each access path keeps its own conservative connection rate while the connector-wide default allows 60 new SSH handshakes per 10 minutes across independent conversations and hosts. A rejected connector-wide attempt does not consume the access path's next connection slot, and retry delays round up so an agent does not wake one fraction of a second too early and get throttled again. Exhausting either budget is reported as `local_handshake_budget_exhausted` with the original `retry_after_seconds`; it is not recorded as target sshd rate limiting, does not increase target/session failure counters, and does not expand into a one-hour host circuit. Once the exact cooldown expires, runtime snapshots expose `local_handshake_budget_ready`, normalize the stale path/session throttle to `unknown`, and allow one normal retry instead of continuing to tell the agent to wait.

Persistent PTY session records are policy-guarded, heartbeat-driven, closeable, and reapable; heartbeats synchronize foreground process, cwd, output reference, input allowance, workspace state, `backend_state`, and `backend_capabilities`. The connector supports OpenSSH `control-master-tty`, OpenSSH `pipe-shell`, and native `russh-native-pty` persistent PTY modes. Opening a PTY creates a pending record that the connector proactively activates before any input, allowing the agent to read a banner or interactive asset menu before choosing a response. PTY activation runs as a connector-owned task whose handshake cannot be cancelled by a competing heartbeat, operation completion, or queue poll after it consumes a connection budget. MCP returns `backend_ready`, `recommended_action`, and `poll_after_ms` so pending activation is not mistaken for an unsupported terminal. Only pending PTYs whose workspace and PTY state are `idle` or `working` enter the activation queue. A successfully opened PTY marks both its logical session and access path connected, clearing expired local-handshake throttle attention. Native PTYs reserve their shared transport lifecycle before channel activation: closing one conversation's PTY leaves other active PTYs untouched, while closing the final PTY on an interactive empty-chain bastion invalidates only that PTY generation's cached login session so the next conversation performs a clean handshake instead of reusing a gateway that answers `Please re-login`. If the backing connection becomes unusable, activation makes the PTY `blocked/failed`, disables input, records the failure on its logical connection and access-path health, writes a redacted system recovery message, and consumes the queue item once instead of degrading the connector through an infinite retry loop. PTY input remains DB-backed and connector-owned: API/MCP enqueue input and expose only redacted metadata/status, while the connector daemon claims input with a lease, writes to the local active backend, then marks the event delivered or failed. Agents may omit `session_id`; the service reuses a valid logical session or creates a `resolving` one. Connector startup marks all previously connected/resolving logical sessions `unknown` with zero open channels because connector-local SSH transports cannot survive process death. It also reconciles stale `active` PTY records to `blocked/failed` and never silently replaces a lost runtime with a new shell. Backend exit updates PTY, workspace, and open-channel state. `remote_hosts_get_host_runtime_snapshot` returns the host, connector snapshots, enabled access paths and their current connection sessions, workspaces, PTYs, recent operations, actionable attention items, and an event cursor in one call; sessions belonging only to disabled routes no longer create misleading current-health attention. `remote_hosts_wait_runtime_events` and `POST /v1/runtime-events/wait` require explicit `live_only` or `after_cursor` behavior, so retained history cannot be mistaken for a new transition and snapshot generation races can be resumed from the captured cursor. The sequenced event log currently receives connector state transitions; authoritative connection, workspace, and PTY lifecycle emitters remain follow-up work.

Expired operations that have exhausted their claim budget are marked `exhausted`, their workspaces become `blocked`, and a recovery hint is written to system output so agents do not retry blindly. The long-running connector daemon adds heartbeat emission, idle backoff, infrastructure-error backoff, Ctrl-C graceful shutdown, PTY startup reconciliation, input pump scheduling, and offline state recording. This keeps agents on an existing workspace instead of repeatedly opening SSH sessions.

## Security Posture

The default product avoids third-party vault coupling. Credentials are encrypted inside the local database using an automatically generated local master key. Agents may accept credentials the user explicitly supplies and pass them once to the dedicated credential tools, but cannot read them back; plaintext is not returned in MCP responses or stored in knowledge and metadata fields.

Remote execution passes through command profiles, policy gates, output limits, redaction, connector heartbeat/state tracking, and workspace/session supervision. The default agent surface permits explicit POSIX or PowerShell scripts through `run_in_workspace`; it does not permit bypassing the managed workspace with a separate raw SSH connection.

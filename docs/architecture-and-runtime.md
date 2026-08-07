# Architecture and Runtime

This document describes the implementation details behind Remote Hosts. For product positioning and
the shortest setup path, start with the [README](../README.md).

## System Planes

Remote Hosts separates four concerns while keeping them in one deployable Rust workspace:

- The **Knowledge Plane** stores canonical hosts, facts, software installs, durable notes, and
  operation history.
- The **Access Plane** resolves environments, connectors, SSH endpoints, credentials, and route
  policy.
- The **State Plane** reports connector, route, transport, logical session, Workspace, PTY, and
  operation state.
- The **Execution Plane** validates and queues commands, files, and PTY input for connector-owned
  execution.

SQLite stores durable logical state. The API and MCP processes create work; the connector daemon
owns live SSH objects and completes that work. Connector-local transports are never inferred to be
alive solely because an old database row says a logical session was connected.

## Rust Workspace

| Crate | Responsibility |
| --- | --- |
| `remote-hosts-domain` | Shared IDs, entities, state enums, and operation types |
| `remote-hosts-core` | Command profiles, policy, redaction, state aggregation, and supervision |
| `remote-hosts-vault` | Argon2id and XChaCha20-Poly1305 local credential encryption |
| `remote-hosts-db` | SQLx migrations and repositories |
| `remote-hosts-connector` | OpenSSH and `russh` transports, pooling, PTYs, and workers |
| `remote-hosts-api` | Axum HTTP API and administration UI |
| `remote-hosts-mcp` | MCP profiles, schemas, handlers, and Agent Session identity |
| `remote-hosts-cli` | Doctor, migration, API, MCP, and connector entry points |

## Transport Reuse and Capacity

Each enabled access path has a connector-owned transport runtime. OpenSSH uses a ControlMaster-based
provider; the native backend caches an authenticated `russh` session. Exec commands, file transfer,
and PTYs reserve channels from the same access-path capacity budget.

The default path capacity is eight channels. Connector workers skip saturated paths instead of
occupying global worker slots while waiting. Runtime snapshot version 9 exposes configured,
reserved, and available channels, active and pending PTYs, and current operation pressure.

Logical connection sessions and physical transport runtimes are separate. Every runtime records a
runtime ID, backend, generation, real connection attempts, authenticated handshakes, validations,
and reuse count. Each command, transfer, and PTY persists transport evidence that distinguishes:

- `first_handshake`: this runtime established its first authenticated connection.
- `reused`: an existing authenticated connection served the channel.
- `reconnected`: the same runtime replaced a failed connection.
- `attempt_failed`: a real connection attempt failed.
- `runtime_replaced`: route change or connector restart created a different runtime object.

Connector startup marks persisted runtimes as lost and resets logical open-channel counts. Historical
telemetry remains inspectable but cannot masquerade as a live SSH transport.

Replacement handshakes use independent access-path and connector-wide sliding-window budgets. Local
budget exhaustion returns the original `retry_after_seconds`; it does not mark the target SSH server
as rate limited or inflate target failure counters.

## Agent Sessions and Workspaces

Every MCP client process creates or resumes an Agent Session. Workspaces, PTYs, operations, input
events, artifacts, and idempotency keys belong to that session. The default Agent profile cannot
operate another conversation's logical state.

Physical reuse deliberately crosses that logical boundary: all conversations using the same access
path share the connector's bounded transport pool. This allows isolation without paying for another
SSH authentication lifecycle.

Workspace preparation reuses only an `idle` or `working` Workspace owned by the current Agent
Session, on the requested access path, with the same coordination scope. It never revives a
`throttled`, `blocked`, `failed`, `done`, or closed Workspace.

## Scoped Mutation Coordination

Mutating commands, uploads, downloads, and PTY input inherit a stable lowercase
`coordination_scope` from their Workspace:

- `host` is the compatibility default and conflicts with every mutation on that host.
- Equal scopes conflict.
- A parent conflicts with every descendant.
- Sibling scopes may run concurrently.

For example, `k8s/prod/datatool-dev` conflicts with
`k8s/prod/datatool-dev/service/file-gateway`, while that service scope can run beside
`k8s/prod/datatool-dev/service/report-api`.

Leases are crash-safe and visible in runtime snapshot `write_lease.active_leases`. Scope names are
resource identity, not a mechanism for evading a legitimate conflict. A task that spans several
resources uses their real common parent.

## Command Execution

`shell.posix` and `shell.powershell` are the normal extension surface. Narrow read-only profiles are
convenience shortcuts, not the product boundary. `run_in_workspace` validates the profile, applies
host policy, creates a queued operation, and lets the connector execute it on the pooled transport.

POSIX execution carries an in-band completion frame. This preserves the real exit status when a
gateway omits or falsifies the SSH exit-status message. Healthy completion keeps the transport
pooled; timeout, channel failure, or a missing completion frame invalidates it before one bounded
reconnect.

Output is redacted and bounded. Large output moves to file-backed artifacts with previews and a
SHA-256 digest. MCP clients read full artifact content in bounded offset-based chunks.

Semantic `idempotency_key` values are scoped to the Agent Session. Retrying the same key and payload
returns the original operation; changing the payload under the same key is rejected.

## Persistent PTYs

The connector supports OpenSSH `control-master-tty`, OpenSSH `pipe-shell`, and native
`russh-native-pty` backends. Opening a PTY first creates a pending logical record. The connector
activates it proactively so an agent can read a banner or asset menu before sending input.

Activation uses a nonblocking channel reservation. A saturated path leaves the PTY pending without
blocking input delivery to already-active PTYs. MCP reports `backend_ready`, `recommended_action`,
and `poll_after_ms` so pending activation is not confused with an unsupported shell.

PTY input is database-backed and connector-owned. The API and MCP enqueue redacted input metadata;
the connector claims, delivers, and terminalizes each event. Backend exit, explicit close, or
connector restart converges PTY, Workspace, lease, and channel state. A lost runtime is marked
`blocked/failed` rather than silently replaced with a fresh shell that has different context.

## File Transfer

`remote_hosts_upload_file` and `remote_hosts_download_file` are queued Workspace operations. They
enforce size, timeout, overwrite, destination, and mode policy; verify SHA-256; and atomically place
data from a same-directory temporary file.

The connector chooses a route-compatible implementation:

- Direct routes use pooled SFTP.
- Non-interactive single-hop POSIX bastions may use bounded framed exec channels.
- Interactive asset-menu bastions reuse the Workspace's already-selected active PTY.

Exec and PTY transfer bodies never enter MCP output or persisted PTY audit rows. Uploads support
stable partial files, prefix verification, idempotent chunks, and already-placed convergence.
Interactive downloads capture bounded Base64 frames in connector memory, verify every chunk and the
whole file, compare remote metadata before and after transfer, and atomically place the local file.

Transfers publish verified bytes, total bytes, resumed bytes, retry count, elapsed time, and a
30-second active heartbeat. Missing frames, output, markers, or integrity proof is failure even when
the SSH channel claims exit status zero.

## Credentials and Authentication

The local vault encrypts password, private-key, passphrase, and sudo-password fields. Plaintext is
accepted only by dedicated credential tools and is never returned by HTTP or MCP.

Native SSH authentication tries a stored key, bounded SSH-agent identities, default local keys when
the agent is empty, and then the encrypted password. After password authentication, the connector
may perform one bounded, idempotent public-key bootstrap. Bootstrap failure never invalidates the
already-authenticated pooled session or forces the user to type the password again.

Bootstrap state, cooldown, failure classification, and key fingerprint are persisted per access
path. Unsupported or repeatedly failing routes become deferred or skipped instead of entering a
loop. Real proxy chains are rejected before handshake until a proxy-aware implementation is
configured; an empty-chain `bastion` route remains one physical SSH endpoint.

## Runtime State

Runtime snapshot version 9 returns one consistent view containing:

- current Agent Session identity;
- host-level logical Workspace capacity, including recorded, effective, expired/reapable, and per-session counts;
- connector health and freshness;
- enabled access paths, route health, key-bootstrap state, transport runtime, and channel capacity;
- current logical connection sessions;
- session-owned Workspaces, PTYs, and recent operations;
- active scoped write leases;
- actionable attention records and an event cursor.

Workspace TTL is enforced automatically. MCP/API creation first closes expired `idle`/`working`
records only when they own neither a queued/running operation nor an active PTY. The Connector
performs the same bounded reconciliation at startup and on heartbeats. Agent Session ownership is
not relaxed: one task never reuses another task's Workspace even when both share one pooled SSH
transport.

`remote_hosts_wait_runtime_events` and the HTTP runtime wait endpoint require explicit `live_only` or
`after_cursor` behavior. This prevents retained history from being mistaken for a new event and
allows callers to resume from the snapshot cursor without losing transitions.

## MCP Profiles

The default `agent` profile exposes 18 task-oriented tools:

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

The `admin` profile adds low-level host, environment, credential-reference, access-path, fact,
connector, and Workspace maintenance. The `full` profile exists for development and debugging.
Hidden tools are removed from discovery and dispatch.

## HTTP Surfaces

The loopback API groups its endpoints by responsibility:

- Management: `/`, `/admin`, `/v1/admin/overview`
- Registry and state: `/v1/hosts`, host access paths, access resolution, and host state
- Topology and credentials: `/v1/topology`, topology sync, credential bindings, and credentials
- Connector runtime: connector heartbeat/events and `/v1/runtime-events/wait`
- Workspace execution: Workspace lifecycle, operations, output, artifacts, and waits
- PTY lifecycle: open, heartbeat, output, input, input events, close, and expiry reaping
- Command catalog: `/v1/command-profiles`

The source routes and request schemas in `remote-hosts-api` are authoritative; this grouped list
avoids duplicating every route signature in overview documentation.

## Security Boundaries

Remote Hosts intentionally avoids third-party vault coupling. Its local vault key is generated with
mode `0600`; encrypted credentials live in the database, and only metadata leaves credential APIs.
An HTTP process with an unlocked vault is restricted to a loopback bind.

Managed execution applies command-profile validation, explicit intent, policy gates, output limits,
redaction, connector state, Workspaces, and audit. The default Agent surface can run arbitrary
POSIX or PowerShell scripts, but it cannot bypass the managed transport with a separate raw SSH
connection.

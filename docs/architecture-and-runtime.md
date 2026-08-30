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
| `remote-hosts-sync` | Direct instance-sync protocol for inventory, knowledge, and peer-sealed credentials; receipts, conflicts, and HTTP client |
| `remote-hosts-cli` | Doctor, migration, API, MCP, and connector entry points |

## Transport Reuse and Capacity

Each enabled access path has a connector-owned transport runtime. OpenSSH uses a ControlMaster-based
provider; the native backend caches an authenticated `russh` session. Exec commands, file transfer,
and PTYs reserve channels from the same access-path capacity budget.

The default path capacity is eight channels. Connector workers skip saturated paths instead of
occupying global worker slots while waiting. A pending PTY blocked by local channel capacity writes
one system output explaining that its remote menu has not started; agents keep that PTY and wait
rather than reconnecting or queuing input. Runtime snapshot version 11 exposes configured,
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

Completed exec and transfer channels close immediately. OpenSSH ControlMaster and native `russh`
keep the authenticated transport only while its access path idle TTL permits. Native transport
eviction requires every channel permit to be free; keepalive probes do not refresh business
activity. Intentional eviction persists `state=idle`, which is a normal cold boundary rather than
a route, credential, or target-server failure.

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

## Scoped Operation Coordination

Arbitrary shell requests declare one coordination mode:

- `read_only` is an explicit caller attestation that the complete script only observes remote state;
  it does not acquire a write lease and can run across conversations up to queue and SSH channel
  capacity.
- `mutating` acquires a hierarchical write lease.
- `auto` is the compatibility default. Narrow read-only profiles remain read-only, while arbitrary
  POSIX and PowerShell scripts remain conservatively mutating.

Each mutating operation selects one or more stable lowercase resources within its Workspace
boundary:

- `coordination_scope` names one resource subtree.
- `coordination_scopes` names up to 16 disjoint resource subtrees acquired atomically.
- `host` is the compatibility default for genuinely machine-wide or uncertain effects and conflicts
  with every mutation on that host.
- Equal scopes conflict.
- A parent conflicts with every descendant.
- Sibling scopes may run concurrently.

The default Workspace boundary is `host`, so one conversation can submit precise operation scopes
without creating a new Workspace for each resource. A narrow Workspace accepts only its own scope or
descendants and therefore cannot jump to a sibling resource.

For example, a rejected-data cleanup can atomically coordinate
`prod/datatool-dev/storage/minio/rejected-data`,
`prod/datatool-dev/database/mysql/rejected-data`, and
`prod/datatool-dev/search/elasticsearch/rejected-data`. It can run beside
`prod/datatool-dev/deployment/lichtblick` and `prod/datatool-dev/pipeline-recovery/clean`, while a
second task touching any cleanup resource or its parent/child waits. A request cannot mix singular
and plural fields or include a parent and its child in the same exact set.

Leases are crash-safe and visible in runtime snapshot `write_lease.active_leases`; compact operation
results expose `requires_write_lease`, exact `coordination_scopes`, and a singular common-ancestor
summary for older clients. Scope names are resource identity, not a mechanism for evading a
legitimate conflict. The connector acquires the complete exact set in one transaction or retains
none. Uploads and PTY input remain mutating; downloads only read the remote host and do not take its
write lease. A PTY fixes its exact scope set when opened, and input, output activity, close, failure,
and idle reaping all use that immutable set rather than the broader Workspace boundary.

## Command Execution

`shell.posix` and `shell.powershell` are the normal extension surface. Narrow read-only profiles are
convenience shortcuts, not the product boundary. `run_in_workspace` validates the profile, applies
host policy, creates a queued operation, and lets the connector execute it on the pooled transport.
For short commands, `wait_timeout_ms` atomically submits and observes that exact operation for up
to 60 seconds, removing the queue-to-wait race without adding another MCP tool.

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
The connector always services queued input for active PTYs before starting another activation.
Native `russh` bounds the complete channel-open, terminal-allocation, and shell-start sequence by
the configured SSH connect timeout; a stuck target therefore terminalizes only that pending PTY
and cannot hold the connector-wide PTY pump indefinitely.

PTY input is database-backed and connector-owned. The API and MCP enqueue redacted input metadata;
the connector claims, delivers, and terminalizes each event. Redacted output-tail detection exposes
password, sudo-password, host-key confirmation, confirmation, pager, and menu prompts as an
`interaction` on the live PTY. The owning Workspace becomes `blocked` to prevent unrelated work,
but the active input-capable PTY retains its channel reservation and can receive the response.
Runtime snapshots emit `pty_input_required` rather than treating that case as a failed connection.
Backend exit, explicit close, or connector restart clears the interaction and converges PTY,
Workspace, lease, and channel state. A lost runtime is marked `blocked/failed` rather than silently
replaced with a fresh shell that has different context.

The daemon also performs PTY lifecycle maintenance without an external cron or API caller. An
ordinary PTY is closed after one hour without output, accepted input, or heartbeat. A truthful
non-empty `foreground_process` heartbeat protects quiet work with the longer one-day busy TTL.
Queued or claimed input prevents reaping. Internal polling while a pending PTY waits for SSH
channel capacity does not refresh business activity, so an abandoned queue entry still expires.
Closing an expired PTY terminates the local backend, releases its SSH channel, shortens its scoped
write lease, and then allows Workspace expiry.

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
accepted only by dedicated credential tools and is never returned by HTTP or MCP. When an active
PTY reports a live sudo prompt, the existing PTY-input tool can request
`use_stored_sudo_password=true` without a text payload. The connector rechecks that prompt,
decrypts only the access path's dedicated sudo field in memory, and writes it directly to that
same PTY. Queue records retain only the `stored_sudo_password` type, a fixed redacted summary, and
zero payload bytes; the password never enters MCP arguments, output artifacts, audit rows, or the
SQLite input payload. Canonical `[sudo] password ...` prompts and macOS's bare `Password:` prompt
are supported, but no password is injected automatically merely because a prompt was detected. For
the bare macOS form, the installed Agent skill requires an explicit `sudo` command immediately
before the prompt on the same PTY, with no intervening input.

For a registered target reachable only from the active PTY host, the caller may send the exact
connector-verified `/usr/bin/ssh` command and then request
`use_stored_password_from_host_id=<target-host-id>` on the resulting generic password prompt. Pin the target host key first and use `StrictHostKeyChecking=yes` so this also works with older OpenSSH clients while still failing closed on unknown or changed keys. The
target must have one enabled SSH path. The public input event stores only
`stored_ssh_password` metadata, while its private payload contains only the target access-path id.
Before and after vault decryption, the connector verifies the live prompt, unchanged target path,
same Agent Session, exact immediately preceding SSH command, and a two-minute prompt window. The SSH
password exists only in zeroizing connector memory and fake or intervening prompts fail closed.

Native SSH authentication tries a stored key, bounded SSH-agent identities, default local keys when
the agent is empty, and then the encrypted password. After password authentication, the connector
may perform one bounded, idempotent public-key bootstrap. Bootstrap failure never invalidates the
already-authenticated pooled session or forces the user to type the password again.

Bootstrap state, cooldown, failure classification, and key fingerprint are persisted per access
path. Unsupported or repeatedly failing routes become deferred or skipped instead of entering a
loop. Real proxy chains are rejected before handshake until a proxy-aware implementation is
configured; an empty-chain `bastion` route remains one physical SSH endpoint.

## Runtime State

Runtime snapshot version 11 returns one consistent view containing:

- current Agent Session identity;
- host-level logical Workspace capacity, including recorded, effective, expired/reapable, and per-session counts;
- connector health and freshness;
- enabled access paths, route health, key-bootstrap state, transport runtime, and channel capacity;
- current logical connection sessions;
- session-owned Workspaces, PTYs (including live `interaction` state), and recent operations;
- active exact-resource write leases and multi-resource operation/PTY declarations;
- actionable attention records and an event cursor.

Workspace TTL is enforced automatically. MCP/API creation first closes expired `idle`/`working`/`blocked`
records only when they own neither a queued/running operation nor an active PTY. The Connector
performs the same bounded reconciliation at startup and on heartbeats. Agent Session ownership is
not relaxed: one task never reuses another task's Workspace even when both share one pooled SSH
transport.

`remote_hosts_wait_runtime_events` and the HTTP runtime wait endpoint require explicit `live_only` or
`after_cursor` behavior. This prevents retained history from being mistaken for a new event and
allows callers to resume from the snapshot cursor without losing transitions.

Agent Work Context version 1 adds a session-scoped decision surface without changing runtime
snapshot version 11. `remote_hosts_get_agent_work_context` implicitly binds the current MCP Agent
Session and supports either `snapshot` or cursor-based `wait`. It returns active work across hosts,
new terminal results after the supplied cursor, type-only PTY interaction, exact coordination
scopes, bounded transfer progress, active-host route/transport/channel digests, and one
deterministic primary action. It never accepts a session id, executes an action, returns raw PTY
input, or exposes foreign-session command/output identifiers. The loopback/admin HTTP equivalents
are `GET /v1/agent-sessions/{id}/work-context` and
`POST /v1/agent-sessions/{id}/work-context/wait`. A timed-out wait returns a compact
`changed=false` acknowledgement with the unchanged cursor and empty host/item arrays; callers keep
the last changed context, and the unchanged cursor makes a racing lifecycle event replayable.

Workspace, operation, PTY, input, transfer-progress, and connection lifecycle hooks append
session/host/workspace linkage to the existing monotonic event sequence after the business state is
durable. Event publication is best effort: SQLite contention or an event-write failure cannot
cancel, retry, restart, or rewrite remote work. Legacy event rows remain readable.

## MCP Profiles

The default `agent` profile exposes 22 task-oriented tools:

- `remote_hosts_list_hosts`
- `remote_hosts_ensure_host`
- `remote_hosts_store_host_credential`
- `remote_hosts_search_knowledge`
- `remote_hosts_record_knowledge`
- `remote_hosts_get_host_runtime_snapshot`
- `remote_hosts_get_agent_work_context`
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
- `remote_hosts_configure_instance_sync_peer`
- `remote_hosts_sync_instance_peer`

The `admin` profile adds low-level host, environment, credential-reference, access-path, fact,
connector, and Workspace maintenance. The `full` profile exists for development and debugging.
Hidden tools are removed from discovery and dispatch.

## HTTP Surfaces

The loopback API groups its endpoints by responsibility:

- Management: `/`, `/admin`, `/v1/admin/overview`, `/v1/admin/activity`
- Registry and state: `/v1/hosts`, host access paths, access resolution, and host state
- Topology and credentials: `/v1/topology`, topology sync, credential bindings, and credentials
- Connector runtime: connector heartbeat/events and `/v1/runtime-events/wait`
- Workspace execution: Workspace lifecycle, operations, output, artifacts, and waits
- PTY lifecycle: open, heartbeat, output, input, input events, close, and expiry reaping
- Command catalog: `/v1/command-profiles`
- Instance Sync: `/v1/instance-sync/identity`, authenticated export, and authenticated receive

The source routes and request schemas in `remote-hosts-api` are authoritative; this grouped list
avoids duplicating every route signature in overview documentation.

## Compact Agent Responses and Operator Activity

MCP remains structured JSON at the transport boundary, but the normal `agent` profile returns
task-oriented compact views. Workspace preparation omits the repeated runtime snapshot and command
catalog; command submission omits full policy, Workspace, and output objects; result and PTY reads
return incremental chunks without repeated foreign keys and timestamps. Stable `workspace.id`,
`operation.id`, state, next action, retry delay, command preview, exit code, and summaries remain.
The `admin` and `full` profiles retain detailed records for maintenance and debugging.

Command visibility is stored as a bounded preview after secret redaction. Managed shell scripts and
normal PTY input are readable in audit records, while detected password/sudo input and encrypted
credential injection remain type-only. The admin page's `Agent 活动` view combines command runs and
PTY input into a newest-first timeline linked to host, Workspace, Agent session, project, result,
duration, and optional transport evidence. `/v1/admin/activity` is bounded to 200 records and never
returns raw private input payloads or secret material.

## Security Boundaries

Remote Hosts intentionally avoids third-party vault coupling. Its local vault key is generated with
mode `0600`; encrypted credentials live in the database, and only metadata leaves credential APIs.
An HTTP process with an unlocked vault is restricted to a loopback bind.

Managed execution applies command-profile validation, explicit intent, policy gates, output limits,
redaction, connector state, Workspaces, and audit. The default Agent surface can run arbitrary
POSIX or PowerShell scripts, but it cannot bypass the managed transport with a separate raw SSH
connection.

# Remote Hosts Product and Technical Plan

Last updated: 2026-07-23

## 1. Product Goal

Build a self-contained remote SSH machine management service for AI agents and human operators.

The system should manage machines across home LANs, company LANs, customer delivery sites, public networks, FRP/VPN paths, and jump hosts. It should expose convenient, structured interfaces to AI agents, while accumulating durable knowledge about every machine: access methods, environment facts, installed software, historical operations, runbooks, failures, and known caveats.

The product starts with one job: keep a reliable remote command channel available to agents without
repeated SSH logins. Host knowledge, access paths, credentials, files, state, and history support
that channel; they are not reasons to grow a domain-specific operations platform.

## 2. Primary Users and Scenarios

Primary users:

- Human operator managing personal, company, and customer machines.
- AI coding or ops agent that needs reliable machine context before acting.
- Future local web UI user who wants to inspect topology, status, history, and knowledge.

Representative machines:

- Home Mac Studio.
- Home Windows AMD host.
- Company 4090 GPU servers in an isolated LAN.
- Customer deployment servers at delivery sites.
- Public servers and machines reachable through FRP, VPN, or jump hosts.

Core scenarios:

- Ask an agent: "Which machines do I have that can run CUDA workloads?"
- Ask an agent: "How can I currently reach company-4090-a from this environment?"
- Ask an agent: "Before running anything, show me whether the connector, access path, SSH session, and host health are OK."
- Ask an agent: "Check GPU status on the 4090 server without repeatedly opening new SSH connections."
- Record that a server has Docker, CUDA, a specific application path, a known service restart SOP, and a previous troubleshooting history.
- Keep credentials self-contained but not visible to agents.

### 2.1 Product Boundary

The primary product is a reliable, observable, reusable SSH execution channel for agents. Core priorities are:

- pooled SSH transport and cheap short-lived channels;
- cached-session validation, bounded reconnect, keepalive, backoff, and exact retry state;
- arbitrary POSIX and PowerShell scripts through the generic workspace command tool;
- interactive PTY when command execution genuinely needs a terminal;
- pooled upload/download, bounded output artifacts, credentials, state, and audit;
- host identity, access paths, environment knowledge, and operation history.

Kubernetes, Harbor, databases, middleware, GPU frameworks, package managers, and deployment products are not separate core MCP tool families. Users and agents invoke their existing CLIs through `shell.posix`, `shell.powershell`, or PTY. A repeated workflow may later be packaged as a declarative, optional runbook that compiles to the same generic command/file primitives. Schedulers, alerts, approvals, and notifications remain external orchestration concerns.

This boundary prevents tool-count growth and keeps connection reuse useful for new software without Remote Hosts learning every domain.

### 2.2 P0: Persistent Command Channel

The highest-priority product capability is one reusable command channel per selected access path:

- Keep one healthy physical SSH transport and open cheap logical channels on it.
- Use `shell.posix` or `shell.powershell` for arbitrary non-interactive commands. Command content is
  not domain-allowlisted; the service still bounds queue depth, concurrency, timeout, output, and
  audit metadata.
- Reuse one persistent PTY when commands need terminal semantics, shared shell state, an interactive
  gateway menu, or a gateway makes repeated exec channels unreliable.
- Expose physical transport, logical session, workspace, exec channel, PTY backend, reconnect count,
  reuse count, and exact retry state separately. An agent must know which layer failed.
- Validate a cached transport before use. Permit one bounded replacement handshake when stale, then
  reuse the replacement across workspaces.
- Never claim a transfer or command stage succeeded from exit status alone when a gateway can drop
  stdin, EOF, stdout, stderr, or completion markers.

P0 is successful when an agent can stay on the same host for a long task, issue arbitrary commands,
observe the real channel state, survive a stale transport with one controlled reconnect, and stop
cleanly instead of entering a retry loop when the route is incompatible.

## 3. Design Principles

1. Separate host identity from access paths.
   A host is the real machine. An access path is one way to reach it from one connector or network.

2. Keep secrets self-contained but not plain text.
   Avoid third-party vault coupling. Use an internal encrypted vault table. Agents can use credentials through the service but cannot read secrets.

3. Treat connection reuse as a first-class feature.
   Agents must not create a fresh SSH login for every small query. The connector should reuse SSH transports and open short-lived channels.

4. Give agents state awareness.
   Agents need to know connector state, access path health, SSH session state, operation state, and host health. A single online/offline flag is not enough.

5. Keep the execution surface generic.
   Narrow read-only profiles are conveniences, not the product boundary. `shell.posix`, `shell.powershell`, PTY, and file transfer must support real arbitrary work through the reused connection without requiring a new MCP tool for each command or domain.

6. Keep security simple and practical.
   The goal is not bank-grade security. The goal is to prevent common accidents: plaintext credential leakage, repeated SSH retries, unbounded command execution, and secret exposure in logs.

7. Store knowledge with source and time.
   Machine facts can become stale. Every fact should have `observed_at`, `source`, and `confidence`.

## 4. System Planes

The system should be organized around four planes.

### 4.1 Knowledge Plane

Stores durable knowledge about machines and their environments.

Responsibilities:

- Host registry.
- Environment and network topology metadata.
- Machine facts such as OS, CPU, GPU, CUDA, Docker, disk, services, open ports.
- Software install records.
- Runbooks and SOPs.
- Operation summaries and historical notes.
- Searchable knowledge items linked to hosts, software, access paths, and operations.

### 4.2 Access Plane

Resolves how to reach a host.

Responsibilities:

- Host-to-access-path mapping.
- Connector-aware route selection.
- Credential references.
- Proxy chains, jump hosts, FRP/VPN/public paths.
- Access path priority and fallback.
- Policy metadata such as read-only-only, production-priority, customer-site-sensitive.

### 4.3 State Plane

Gives agents eyes.

Responsibilities:

- Connector liveness.
- Access path reachability.
- SSH session pool status.
- Channel and operation lifecycle.
- Host health snapshots.
- Failure reasons, retry windows, circuit breaker state, and agent hints.

Important: do not compress this into one boolean. A host can be healthy while a specific access path is degraded, or the connector can be offline while a public fallback still works.

### 4.4 Execution Plane

Runs controlled actions.

Responsibilities:

- Arbitrary one-shot POSIX and PowerShell scripts through the managed workspace channel.
- Interactive PTY sessions.
- Read-only convenience profiles such as `nvidia-smi`, `systemctl status`, and `df -h`.
- Pooled file transfers over SFTP or a route-compatible SSH exec data stream.
- Port forwards.
- Optional declarative runbooks compiled to the same generic primitives.
- Audit logs and redacted outputs.

## 5. High-Level Architecture

```text
Agent / Codex / Other MCP Client
        |
        | MCP tools
        v
MCP Gateway
        |
        v
Core API Service
  - Host registry
  - Access resolver
  - Credential vault facade
  - Knowledge service
  - State service
  - Operation audit service
        |
        +--> Database
        |     - normal metadata
        |     - encrypted credential blobs
        |     - state snapshots and events
        |
        +--> Connector(s)
              - home connector
              - company LAN connector
              - customer site connector
              - public connector
              |
              v
        Connection Manager
              |
              +--> SSH transport pool
              +--> OpenSSH ControlMaster fallback
              +--> SFTP channels
              +--> Port forwarding channels
```

Connectors can run on machines inside each network. They should call back to the core service where possible, so machines behind NAT or customer networks do not require inbound exposure.

## 6. Core Domain Model

### 6.1 Host

Represents a real machine.

Fields:

- `id`
- `name`
- `display_name`
- `kind`: `macos`, `windows`, `linux`, `gpu_server`, `jump_host`, `customer_server`, etc.
- `owner`
- `tags`
- `description`
- `risk_level`: `personal`, `development`, `production`, `customer_site`
- `created_at`
- `updated_at`

### 6.2 Environment

Represents a network or physical context.

Fields:

- `id`
- `name`
- `kind`: `home_lan`, `company_lan`, `customer_site`, `public_internet`, `vpn`, `frp`
- `description`
- `trust_level`
- `notes`

### 6.3 Connector

Represents an agent/daemon that can access machines from one environment.

Fields:

- `id`
- `name`
- `environment_id`
- `host_id`: optional, when the connector runs on a managed host
- `version`
- `state`
- `last_seen_at`
- `current_network`

### 6.4 AccessPath

Represents one way to reach a host.

Fields:

- `id`
- `host_id`
- `environment_id`
- `connector_id`: optional preferred connector
- `protocol`: initially `ssh`
- `address`
- `port`
- `username`
- `credential_id`
- `route_type`: `lan`, `public`, `frp`, `vpn`, `proxy_jump`, `bastion`
- `proxy_chain`
- `priority`
- `enabled`
- `connection_mode`: `pooled` or `one_shot`
- `idle_ttl_seconds`
- `keepalive_seconds`
- `max_concurrent_channels`
- `max_new_connections_per_minute`
- `requires_tty`
- `notes`

### 6.5 Credential

Secrets are self-contained in the local system but not exposed to agents.

Fields:

- `id`
- `name`
- `type`: `ssh_password`, `ssh_private_key`, `ssh_private_key_with_passphrase`, `sudo_password`, `windows_password`
- `username_hint`
- `encrypted_blob`
- `kdf_params`
- `created_at`
- `updated_at`
- `last_used_at`

The decrypted blob can contain:

```json
{
  "password": "...",
  "private_key_pem": "...",
  "private_key_passphrase": "...",
  "sudo_password": "..."
}
```

Agents never receive this JSON. They only receive whether a credential exists, whether the vault is unlocked, and whether a credential can be used.

### 6.6 HostFact

Represents a time-aware fact.

Fields:

- `id`
- `host_id`
- `namespace`: `os`, `hardware`, `gpu`, `docker`, `network`, `service`, `software`
- `key`
- `value_json`
- `source`: `manual`, `probe`, `operation`, `import`
- `observed_at`
- `expires_at`
- `confidence`

### 6.7 SoftwareInstall

Fields:

- `id`
- `host_id`
- `name`
- `version`
- `install_path`
- `config_paths`
- `service_names`
- `ports`
- `installed_by_operation_id`
- `notes`

### 6.8 OperationRun

Fields:

- `id`
- `host_id`
- `access_path_id`
- `connector_id`
- `session_id`
- `workspace_id`
- `operation_type`: `probe`, `readonly_exec`, `sftp`, `port_forward`, `runbook`
- `intent`
- `state`: `queued`, `running`, `succeeded`, `failed`, `timed_out`, `cancelled`, `rejected`, `exhausted`
- `started_at`
- `finished_at`
- `exit_code`
- `timeout_seconds`
- `redacted_command_summary`
- `redacted_output_summary`
- `log_ref`
- `attempt_count`
- `claim_token`
- `claimed_at`
- `lease_expires_at`
- `last_error`

### 6.9 KnowledgeItem

Fields:

- `id`
- `title`
- `body`
- `source`: `manual`, `agent_summary`, `operation_postmortem`, `probe`
- `linked_host_ids`
- `linked_access_path_ids`
- `linked_software_ids`
- `linked_operation_ids`
- `tags`
- `created_at`
- `updated_at`

### 6.10 Infrastructure Topology

The topology layer is a generic directed graph rather than a collection of product-specific
tables.

`TopologyNode` represents registered hosts, virtual machines, clusters, containers, reverse
proxies, load balancers, middleware, databases, caches, message queues, storage, networks,
endpoints, and business services. A node has a globally stable `external_key`, optional primary
`host_id`, status, address, ports, non-secret metadata, observation timestamps, and derived active
state.

`TopologyEdge` connects two nodes with relationships such as `contains`, `member_of`, `runs_on`,
`proxies_to`, `routes_to`, `depends_on`, `connects_to`, `replicates_to`, `exposes`, or
`managed_by`.

Topology synchronization is authoritative within one `scope_key + source`. A repeated snapshot
upserts stable nodes and edges in a transaction. Memberships omitted by the next snapshot become
inactive for that source and scope instead of being deleted. This preserves history and allows
manual inventory, host probes, cluster inventory agents, and imported sources to contribute to the
same graph safely.

`CredentialBinding` links any topology node to encrypted credential metadata with a purpose such
as `admin`, `readonly`, `database`, or `automation`. Secret-like keys are forbidden in topology
metadata; secret material belongs in the credential vault.

## 7. Credential Vault Design

No Keychain, 1Password, or external Vault dependency in the default product.

Default production approach:

- Use an internal encrypted credential table.
- Keep normal metadata and encrypted blobs in the same database.
- Use a master password to unlock the vault at service startup.
- Derive an encryption key from the master password.
- Keep the derived key only in process memory.
- Never log secrets.
- Never return secret material through MCP/API.

Recommended crypto choices:

- AEAD: `ChaCha20Poly1305` or `XChaCha20Poly1305`.
- KDF: `Argon2id`.
- Secret memory handling: `secrecy` and `zeroize`.

Stability note:

- If the newest `argon2` crate release is a release candidate, prefer the latest stable `0.5.x` line for the first production implementation, then upgrade after explicit testing.

## 8. Connection Manager and Multiplexing

The connection manager is mandatory, not an optimization.

Agent calls should not create raw SSH connections. The flow should be:

```text
Agent request
  -> MCP/API tool
    -> Access Resolver
      -> Connector
        -> Connection Manager
          -> reused SSH transport
            -> short-lived exec/file/forward channel or persistent PTY
```

### 8.1 Session Key

Pool SSH transports by:

```text
connector_id
+ access_path_id
+ username
+ credential_id
+ host_key_fingerprint
+ proxy_chain_hash
```

### 8.2 Runtime Policy

Default policy:

- `idle_ttl`: 10 minutes.
- `max_session_age`: 4 hours.
- `keepalive_interval`: 30 seconds.
- `keepalive_failures_before_reconnect`: 3.
- `max_concurrent_channels_per_host`: 2 by default, configurable 1-4.
- `max_new_connections_per_minute`: 1-2 per access path.
- Independent connector-wide handshake budget across all access paths.
- Circuit breaker for repeated auth/network failures.
- Deduplicate identical short probes when possible.

The per-access-path and connector-wide handshake budgets are independent. A local denial returns
the exact limiting bucket's `retry_after_seconds`; it does not mark the target unhealthy and must
not be expanded to the longer window of the other bucket. Once that exact cooldown expires, the
agent state changes from `local_handshake_budget_exhausted` to `local_handshake_budget_ready` and
permits one normal retry; it must not remain in a zero-second wait loop.

### 8.3 Implementation Strategy

Phase 1 fallback:

- Support OpenSSH ControlMaster / ControlPersist for compatibility.
- Useful for complex ProxyJump chains and environments where OpenSSH config already works.

Phase 2 native pool:

- Use Rust-native SSH sessions with a transport pool.
- Open a new SSH channel per command or SFTP operation.
- Keep transport health and channel lifecycle observable.

The public internal interface should be independent of the backend implementation:

```rust
trait RemoteTransport {
    async fn check(&self, request: CheckRequest) -> Result<CheckResult>;
    async fn exec(&self, request: ExecRequest) -> Result<ExecResult>;
    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult>;
    async fn open_forward(&self, request: ForwardRequest) -> Result<ForwardHandle>;
}
```

### 8.4 Agent Session Supervisor

Herdr is a useful reference for this part of the design: it keeps terminal sessions persistent, exposes agent-aware states such as blocked/working/done/idle, and provides a socket/API control surface so agents can create, inspect, wait on, and clean up workspaces instead of blindly driving panes.

For this product, the lesson is not to depend on Herdr directly. The lesson is to add a first-class `Agent Session Supervisor` inside each connector.

An Agent Session represents one Codex/Antigravity conversation or one explicitly identified client instance. The boundary is logical: all conversations share the same connector-owned SSH transport pool for an access path, but each conversation exclusively owns its Workspaces, PTYs, queued inputs, operations, idempotency namespace, output, artifacts, and temporary context. The normal Agent profile cannot inspect or operate another Agent Session's resources; Admin/Full can inspect legacy or cross-session state for recovery.

Mutations coordinate through crash-safe hierarchical write leases rather than by creating another SSH connection. Every Workspace has a stable `coordination_scope`, defaulting to `host`. Mutating shell operations, uploads, and PTY input inherit that scope; read-only operations and downloads remain eligible. `host` conflicts with every scope, equal and parent/child scopes conflict, and siblings may mutate concurrently. This lets independent resources on one management host progress without weakening protection for the same Kubernetes object, namespace, filesystem subtree, or machine-wide state. Active scoped leases expire automatically and are all visible in runtime snapshot version 8. PTY input retains its scope for 300 seconds, PTY output activity renews it, and PTY close, backend exit, or connector restart reconciliation shortens it to a bounded handoff grace. Session-scoped semantic idempotency keys return the original operation/input event on exact retry and reject key reuse with a different payload.

The supervisor should manage three different execution shapes:

1. Short structured command
   - For bounded read-only checks such as `uname`, `nvidia-smi`, `docker ps`, `df -h`.
   - Runs through a reused SSH transport and a short-lived channel.
   - Subject to command profiles, output limits, and timeouts.
   - `shell.posix` and `shell.powershell` cover real build, deployment, and maintenance scripts when a narrow profile is insufficient. They still run through the workspace queue, pooled transport, explicit intent, timeout, output cap, redaction, and audit history.

2. Persistent remote shell or PTY session
   - For interactive work, shared shell context, or gateways whose repeated exec channels lose
     stdin, EOF, stdout, stderr, or completion status.
   - One long-lived session per `agent_session + host + access_path + workspace`.
   - Commands are queued and streamed through the existing session.
   - The supervisor tracks cwd, foreground process, idle/working/blocked/done state, recent output, and last activity.
   - Lifecycle records are policy-guarded and expose open, heartbeat, close, and idle reap operations through API/MCP.
   - The current connector implementation attaches lifecycle records to managed long-lived remote shells over reused OpenSSH ControlMaster transports or native pooled `russh` sessions, and persists redacted output chunks for API/MCP polling.
   - PTY session records expose `backend_state` and `backend_capabilities` so agents can distinguish a pending backend, a pipe-shell compatibility backend, and a true SSH PTY backend.
   - The OpenSSH connector supports `pipe-shell` compatibility mode and `control-master-tty` mode. `control-master-tty` runs a persistent local `ssh -tt` child against the existing ControlMaster socket, giving the remote side TTY semantics without creating a new SSH handshake for every agent input.
   - The native `russh-native-pty` mode opens a session channel on the pooled `russh` transport, sends `request-pty`, starts a remote shell, and streams persistent input/output without repeated SSH handshakes.
   - Opening a PTY creates a pending record that the connector proactively activates before any input. This allows the agent to read a banner or asset menu before selecting a response. MCP returns `backend_ready`, `recommended_action`, and `poll_after_ms`.
   - PTY input uses a DB-backed event queue: API/MCP enqueue bounded input and expose only redacted metadata/status; the connector daemon input pump claims input with a lease, writes it to the local active backend, and marks it delivered or failed.
   - The next hardening step is exposing resize/signal controls through API/MCP and validating the PTY backends against local/containerized SSHD integration tests.
   - Connector restart reconciliation now marks connector-local `active` PTY records as `blocked/failed` and records a recovery hint. It deliberately does not start a fresh shell under the old PTY id, because that would falsely claim runtime continuity.
   - Connector startup also marks old connector-local SSH sessions `unknown` and clears their
     open-channel count. A database row cannot claim that an in-memory transport survived a process
     restart.
   - Pending activation is limited to PTYs and workspaces in `idle` or `working`. A missing or unusable backing connection terminalizes that PTY once, disables input, records redacted system output, and removes it from automatic activation instead of retrying forever and degrading the connector.
   - Opening a PTY no longer requires an agent to invent or discover a connection-session id. API/MCP reuse a compatible logical session or create a `resolving` session for connector-owned activation.
   - When route capability probing shows reliable exec channels, one-shot shell operations remain
     the cleaner default. When exec behavior is incomplete, prefer one verified persistent PTY and
     do not keep opening replacement exec channels.

3. Remote worker session
   - Optional phase for high-frequency use.
   - Start a small self-contained Rust worker on the target host through one SSH session.
   - Communicate over stdio, a Unix socket, or a Windows named pipe inside the existing trusted session.
   - The worker owns local command execution, PTYs, and output streaming, while the connector keeps policy decisions and credential control.

This gives us a Herdr-like control plane without making terminal UI or third-party tooling part of the core dependency chain.

### 8.5 Server Protection Policy

The multiplexer must protect target servers from agent behavior.

Default policy:

- One active SSH transport per access path.
- One active persistent PTY workspace per host by default.
- Strict queueing for repeated agent commands.
- Token bucket for new SSH handshakes.
- Token bucket for new remote processes.
- Per-host queue depth limit.
- Per-operation CPU/output/time budgets.
- Cooldown after SSHD disconnects, rate-limit messages, auth failures, or connection resets.
- Automatic downgrade from active probing to passive cached state when a host shows overload symptoms.

Recommended defaults:

```text
max_new_ssh_handshakes_per_10_min = 10
max_parallel_exec_channels_per_host = 1
max_parallel_probe_jobs_per_host = 1
max_persistent_ptys_per_host = 1
max_operation_queue_depth_per_host = 20
default_exec_timeout_seconds = 30
default_output_limit_bytes = 256 KiB
overload_cooldown_seconds = 300
```

The agent should see overload as a first-class state, not as a generic SSH error.

Example:

```json
{
  "state": "throttled",
  "reason_code": "target_sshd_rate_limited",
  "retry_after_seconds": 300,
  "agent_hint": "use_cached_state_or_wait"
}
```

### 8.6 Session and Workspace Model

Add an explicit workspace/session layer above raw SSH.

```text
Host
  AccessPath
    ConnectionSession
      AgentWorkspace
        AgentPane / PtySession / Operation
```

`AgentWorkspace` fields:

- `id`
- `host_id`
- `access_path_id`
- `connector_id`
- `label`
- `cwd`
- `state`: `idle`, `working`, `blocked`, `done`, `failed`, `throttled`
- `created_at`
- `last_activity_at`
- `ttl_seconds`
- `policy_profile`
- `coordination_scope`: hierarchical mutation boundary, default `host`

`PtySession` fields:

- `id`
- `workspace_id`
- `session_id`
- `state`
- `foreground_process`
- `cwd`
- `recent_output_ref`
- `last_exit_code`
- `input_allowed`
- `created_at`
- `last_activity_at`

The state plane should expose these objects to agents so they can inspect and wait instead of opening more SSH connections.

### 8.7 MCP Tools for Session Supervision

Add these tools to the MCP surface when the connection manager is implemented:

- `remote_hosts_list_workspaces`
- `remote_hosts_get_workspace`
- `remote_hosts_create_workspace`
- `remote_hosts_close_workspace`
- `remote_hosts_run_in_workspace`
- `remote_hosts_read_workspace_output`
- `remote_hosts_list_workspace_pty_sessions`
- `remote_hosts_open_workspace_pty_session`
- `remote_hosts_heartbeat_pty_session`
- `remote_hosts_read_pty_output`
- `remote_hosts_queue_pty_input`
- `remote_hosts_list_pty_input_events`
- `remote_hosts_close_pty_session`
- `remote_hosts_reap_expired_pty_sessions`
- `remote_hosts_wait_workspace_state`
- `remote_hosts_get_server_protection_state`
- `remote_hosts_get_host_runtime_snapshot`
- `remote_hosts_wait_runtime_events`

These tools should return structured state and recovery hints. They should not expose raw sockets or credentials.

### 8.8 MCP Tool Profiles and Facade

The full MCP implementation can grow without forcing every agent to inspect every low-level schema. Tool registration is separated from tool visibility:

- `agent`: 18 task-oriented tools for host discovery/registration, encrypted credential updates, snapshot/knowledge, workspace preparation, structured execution, pooled file transfer, bounded artifact reads, PTY interaction, and runtime event waits.
- `admin`: 36 tools, adding host deduplication/upsert, environment, credential-reference, access-path, fact, connector, and workspace maintenance.
- `full`: all 45 tools for development, migration, and debugging.

The normal workflow is:

```text
list_hosts
  -> get_host_runtime_snapshot (state-only request)
  -> prepare_workspace (execution request; reuses before creating)
  -> run_in_workspace / upload_file / download_file
  -> wait_workspace_state
  -> get_workspace_result
  -> read_output_artifact_content (when an artifact is larger than the normal result)
```

`prepare_workspace` returns an `idle` or `working` workspace, a post-preparation runtime snapshot, and available command profiles. It never reuses `throttled`, `blocked`, `failed`, or closed workspaces. `get_workspace_result` returns bounded output chunks, recent operations, and large-output artifact metadata; artifact content can be consumed with bounded offset-based reads. Hidden profile tools are removed from the router rather than merely omitted from documentation, so direct calls are rejected.

### 8.9 Clean-Room Herdr Source Audit

Herdr was inspected locally at commit `66be0b655fe922867f1eed100a41d67038b6ffd6`. It is AGPL software, so Remote Hosts adopts independently implemented behavior and architecture lessons rather than copying source.

The highest-value findings are:

1. Bootstrap with a complete snapshot, then observe change events. A snapshot without a cursor can miss a transition that occurs while the snapshot is being assembled.
2. Make subscription start behavior explicit. A new live subscriber must not begin at sequence zero and replay retained history; replay is valid only when the caller supplies a cursor.
3. Share one SSH control connection across bootstrap, status, command, and bridge-like activity. Creating separate multiplexers per subsystem defeats handshake reduction.
4. Treat persisted session metadata, a live remote process, and an agent application's own resume token as separate kinds of continuity. A replacement shell must never be presented as the original lost runtime.
5. Negotiate protocol version and capabilities before target-side update, restart, detach, or handoff behavior. An older worker that cannot survive transport loss must be reported honestly.
6. Prefer authoritative lifecycle hooks and completed transitions over screen-text heuristics. If heuristics are later added, debounce them and use them only as lower-confidence fallback evidence.
7. Keep the agent API narrower than the internal socket or process API. Unrestricted command-bearing socket methods turn a convenience interface into a policy bypass.

Implemented from these lessons:

- The daemon injects one `OpenSshTransportPool` into both queued operation and PTY backends.
- Host runtime snapshots capture `event_cursor` before reading child state.
- Runtime event waits require `live_only` or `after_cursor`; both HTTP and MCP return normal timeout results with the next usable cursor.
- Connector-local PTY loss is reconciled to `blocked/failed` and never silently replaced.
- Runtime snapshots exclude connection sessions whose access path is disabled, while historical operations remain attached to their workspaces.
- Snapshot version 6 persists connector-local SSH runtime identity, backend, connection generation,
  attempts, successful handshakes, cached-connection reuse, capabilities, Agent Session ownership,
  and privacy-aware host write-lease state.
- Every exec, file-transfer, and PTY channel records structured transport evidence. Its mutually
  exclusive `connection_use` is `unchanged`, `reused`, `first_handshake`, `reconnected`, or
  `attempt_failed`; the orthogonal `runtime_replaced` flag distinguishes a new connector-local
  runtime after route change or connector restart.

Still required:

- Authoritative state-event emission for connection, workspace, operation, and PTY transitions.
- A target-side worker with explicit protocol/capability negotiation for process continuity across connector restarts.
- Graceful drain or handoff during connector updates.

## 9. State Plane Design

Agent-visible state is a core product feature.

### 9.1 State Layers

```text
Connector state
AccessPath state
SSH Session state
Channel / Operation state
Host Health state
```

### 9.2 State Values

Use specific states instead of only `online` and `offline`:

- `unknown`
- `not_configured`
- `connector_offline`
- `resolving`
- `tcp_unreachable`
- `ssh_handshake_failed`
- `auth_failed`
- `host_key_changed`
- `connected`
- `degraded`
- `rate_limited`
- `throttled`
- `target_overloaded`
- `circuit_open`
- `maintenance`
- `healthy`

Every state response should include:

- `observed_at`
- `state_age_seconds`
- `confidence`
- `reason_code`
- `human_message`
- `agent_hint`
- `retry_after`

Examples of `agent_hint`:

- `use_alternate_access_path`
- `wait_before_retry`
- `ask_user_to_unlock_vault`
- `connector_offline_try_public_path`
- `auth_failed_do_not_retry`
- `refresh_facts_before_execution`
- `use_existing_workspace`
- `use_cached_state_or_wait`
- `reduce_probe_frequency`

### 9.3 State Tables

```text
connector_status
  connector_id
  state
  last_seen_at
  version
  current_network
  last_error

access_path_health
  access_path_id
  state
  last_checked_at
  latency_ms
  failure_count
  last_error_code
  next_retry_at

connection_sessions
  session_id
  access_path_id
  connector_id
  state
  created_at
  last_used_at
  open_channels
  reused_count
  failure_count
  last_error

agent_workspaces
  workspace_id
  host_id
  access_path_id
  connector_id
  label
  cwd
  state
  policy_profile
  created_at
  last_activity_at
  ttl_seconds

pty_sessions
  pty_session_id
  workspace_id
  session_id
  state
  foreground_process
  cwd
  recent_output_ref
  last_exit_code
  input_allowed
  created_at
  last_activity_at

operation_runs
  operation_id
  host_id
  access_path_id
  session_id
  channel_id
  state
  started_at
  finished_at
  exit_code
  timeout_seconds
  redacted_summary

state_events
  id
  entity_type
  entity_id
  old_state
  new_state
  reason_code
  observed_at
```

### 9.4 Refresh Levels

`remote_hosts_refresh_state` should support:

- `passive`: read cached state only.
- `tcp`: test TCP reachability.
- `ssh`: perform SSH handshake/auth check.
- `facts`: run lightweight read-only probes.

## 10. MCP Tool Surface

The MCP server should expose task-oriented tools with structured outputs.

### 10.1 Read-Only Knowledge and Access Tools

- `remote_hosts_list_hosts`
- `remote_hosts_get_host`
- `remote_hosts_search_hosts`
- `remote_hosts_search_knowledge`
- `remote_hosts_get_host_facts`
- `remote_hosts_get_software_installs`
- `remote_hosts_get_recent_operations`
- `remote_hosts_resolve_access`
- `remote_hosts_find_by_capability`

### 10.2 State Tools

- `remote_hosts_health_summary`
- `remote_hosts_get_host_state`
- `remote_hosts_get_access_path_state`
- `remote_hosts_list_connection_sessions`
- `remote_hosts_get_operation_state`
- `remote_hosts_wait_for_operation`
- `remote_hosts_refresh_state`
- `remote_hosts_explain_unreachable`
- `remote_hosts_get_server_protection_state`

### 10.3 Controlled Execution Tools

Start with read-only or narrow tools:

- `remote_hosts_probe`
- `remote_hosts_run_check`
- `remote_hosts_exec_readonly_profile`
- `remote_hosts_record_knowledge`
- `remote_hosts_run_in_workspace`
- `remote_hosts_upload_file`
- `remote_hosts_download_file`
- `remote_hosts_read_workspace_output`
- `remote_hosts_read_output_artifact_content`
- `remote_hosts_read_pty_output`
- `remote_hosts_queue_pty_input`
- `remote_hosts_list_pty_input_events`
- `remote_hosts_wait_workspace_state`

Later add:

- `remote_hosts_execute_runbook`
- `remote_hosts_open_port_forward`

Do not expose:

- `get_password`
- `show_private_key`
- unmanaged shell execution that bypasses workspace reuse, policy, timeout, output bounds, and audit

### 10.4 Operator Management HTTP Surface

- `GET /admin`
- `GET /v1/admin/overview`
- `GET /v1/topology`
- `POST /v1/topology/sync`
- `GET /v1/topology/credential-bindings`
- `POST /v1/topology/nodes/{node_id}/credentials`

The embedded console visualizes hosts, access-path health, clusters, services, and topology
relationships. Credential responses are metadata-only. A service process with an unlocked HTTP
vault binds only to loopback; operators reach it remotely through their existing SSH tunnel.

## 11. Execution Safety

Execution policy should be simple, strict, and explainable.

Levels:

- L0: database-only query.
- L1: network check, no auth.
- L2: SSH auth and read-only probes.
- L3: approved runbook with bounded mutation.
- L4: production/customer/destructive action requiring explicit human approval.

Rules:

- Prefer structured checks over free-form shell; use managed shell profiles for real operations that cannot be expressed by a narrow profile.
- Represent commands as argument arrays where possible.
- Validate command profile, target host, timeout, environment, and output size.
- Redact secrets before storing output.
- Block repeated auth retries after failure.
- Block repeated SSH handshakes after disconnects, overload symptoms, or server rate-limit messages.
- Keep an audit trail for every remote action.
- Avoid multi-layer shell quoting where possible.
- For nontrivial Windows PowerShell, use encoded scripts rather than nested quotes.

## 12. Rust Technical Scheme

Research timestamp: 2026-07-08.

Local toolchain observed in this workspace:

- `rustc 1.94.1`
- `cargo 1.94.1`

The project should pin a Rust toolchain in `rust-toolchain.toml` once initialized. Because some current libraries require Rust 1.85+ and `sqlx 0.9.0` reports Rust 1.94.0, target Rust 1.94+ for the initial implementation.

### 12.1 Recommended Crates

Current versions observed via `cargo search` / `cargo info` on 2026-07-08:

| Area | Crate | Observed Version | Recommendation |
| --- | --- | ---: | --- |
| Async runtime | `tokio` | `1.52.3` | Primary async runtime. |
| HTTP API | `axum` | `0.8.9` | Core API service. |
| Middleware | `tower`, `tower-http` | `tower-http 0.7.0` | Timeouts, tracing, CORS, compression later. |
| MCP SDK | `rmcp` | `2.1.0` | MCP gateway/server. |
| SSH native | `russh` | `0.62.2` | Long-term SSH transport pool. |
| OpenSSH fallback | `openssh` | `0.11.6` | Phase 1 compatibility backend. |
| Native SFTP | `russh-sftp` | `2.3.0` | SFTP channels over the pooled native SSH session. |
| OpenSSH SFTP | `openssh-sftp-client` | `0.15.7` | SFTP channels over the existing ControlMaster session. |
| Database | `sqlx` | `0.9.0` | SQLite MVP, Postgres later. Compile-time checked SQL where practical. |
| Serialization | `serde` | `1.0.228` | JSON schemas and API payloads. |
| IDs | `uuid` | `1.23.4` | Stable IDs. |
| CLI | `clap` | `4.6.1` | Admin CLI and connector commands. |
| Tracing | `tracing` | `0.1.44` | Structured logs and spans. |
| OpenAPI | `utoipa` | `5.5.0` | Optional HTTP API docs. |
| AEAD | `chacha20poly1305` | `0.11.0` | Credential vault encryption. |
| KDF | `argon2` | `0.6.0-rc.8` latest search result; docs.rs stable latest may still show `0.5.x` | Use the latest stable non-RC line for MVP; test RC separately before adopting. |
| Secret handling | `secrecy`, `zeroize` | verify at implementation | Prevent accidental secret formatting and clear memory where practical. |

### 12.2 Workspace Layout

```text
remote-hosts/
  crates/
    remote-hosts-domain/
    remote-hosts-db/
    remote-hosts-core/
    remote-hosts-connector/
    remote-hosts-mcp/
    remote-hosts-api/
    remote-hosts-cli/
  migrations/
  docs/
  tests/
```

Suggested responsibility split:

- `remote-hosts-domain`: entities, IDs, state enums, policy types.
- `remote-hosts-db`: SQLx models, migrations, repository implementations.
- `remote-hosts-core`: access resolver, state service, vault facade, operation service.
- `remote-hosts-connector`: SSH connection manager, transport backends, probe execution.
- `remote-hosts-mcp`: MCP tool definitions and handlers.
- `remote-hosts-api`: Axum HTTP API for UI and admin.
- `remote-hosts-cli`: bootstrap, vault unlock, import/export, connector registration.

### 12.3 Data Storage Strategy

MVP:

- SQLite with SQLx.
- WAL mode.
- One local database for metadata, encrypted credentials, events, and operation summaries.
- File-based log artifacts for large outputs.

Growth path:

- Postgres for multi-user or multi-connector deployments.
- Optional vector search or full-text indexing after basic knowledge search works.

### 12.4 SSH Strategy

Use two transport implementations behind one trait:

1. `OpenSshTransport`
   - Uses local OpenSSH and ControlMaster/ControlPersist.
   - Best long-term compatibility path for real-world SSH config, jump hosts, and ProxyJump.
   - The current connector rejects `proxy_jump` and non-empty proxy chains before handshake because `proxy_chain` is not yet a typed, validated OpenSSH destination model. This prevents a configured jump route from being silently treated as direct. An empty-chain `bastion` route is one physical SSH connection and supports interactive asset menus or gateway usernames such as `username/server/account`. First-class OpenSSH `SessionBuilder::jump_hosts` wiring follows the typed route schema.

2. `RusshTransport`
   - Uses native Rust async SSH.
   - Best for long-term pooling, channel control, metrics, cancellation, and high-performance connector behavior.
   - Local inspection of `russh 0.62.2` confirms client APIs for `channel_open_session`, `request_pty`, `request_shell`, `exec`, `data`, `window_change`, `signal`, and keepalive/ping.
   - Current implementation includes a shared cached native `russh` pool for queued operations, SFTP channels, and PTY backends. It resolves password or private-key material from the internal encrypted vault inside the connector process, tries a bounded set of local SSH-agent/default-key identities before password fallback, and schedules an idempotent public-key install after password authentication. Installation runs outside the connection critical path with a 10-second timeout; attempting/installed/deferred/skipped state, key fingerprint, failure count, and cooldown survive connector restarts. It applies `strict`/`add`/`accept` host-key policy via known_hosts, validates command profiles and file-transfer specifications, bounds output capture, reuses the operation queue, lease renewal, redaction, and artifact pipeline, and can run persistent `request-pty` shell channels for agent workspaces. Native multi-hop paths fail before handshake until direct-tcpip channel chaining is implemented.
   - Transport runtime metrics now distinguish the logical connection-session record from the real connector-local SSH object. Each runtime has a unique id, connection generation, attempt/handshake/reuse counters, state, timestamps, and capabilities. Exec, file-transfer, and PTY records capture the exact runtime/generation and one mutually exclusive `connection_use` classification. Runtime replacement remains an independent fact because a newly allocated runtime can still perform its first handshake.
   - Remaining native work: HTTP/MCP resize and signal controls, direct TCP/IP forwarding, and integration tests against a local/containerized SSHD.

The MVP can ship `OpenSshTransport` first if needed, but the architecture should treat `RusshTransport` as the long-term default.

### 12.5 Performance Considerations

- Reuse SSH transports; do not reconnect for each command.
- Open bounded concurrent channels per host.
- Cache state snapshots with freshness metadata.
- Deduplicate identical short probes within a small time window.
- Stream large command outputs to log artifacts instead of loading everything into memory.
- Apply operation timeout and output byte limits.
- Use backpressure queues per host/access path.
- Separate connector event loop from CPU-heavy parsing tasks.

### 12.6 Reliability Considerations

- Circuit breaker per access path.
- Retry only when reason code is retryable.
- Do not retry `auth_failed` aggressively.
- Persist operation state transitions.
- Record state events for later explanation.
- Revalidate host key fingerprints.
- Use connector heartbeats.
- Make vault locked/unlocked state explicit.
- Design all MCP errors with recovery hints.

Current connection hardening includes access-path keepalive and idle TTL propagation, cached-session liveness checks before reuse, a `governor` token bucket for replacement SSH handshakes, one shared OpenSSH pool across operation and PTY backends, operation-to-session binding, connection reuse/open-channel/failure metrics, access-path cooldown enforcement, explicit PTY runtime-loss reconciliation, and monotonic state-event waits with explicit live-only/replay semantics. The next reliability layer is authoritative lifecycle emission for all runtime entities and a target-side worker for continuity that cannot be provided by a connector-local SSH channel alone.

The connector now applies independent per-access-path and connector-wide handshake budgets. Cached
raw and guarded transports are replaced whenever endpoint, route, host kind, keepalive, or
connection policy changes, so a stale direct transport cannot survive a route edit. Empty-chain
POSIX bastion file fallback requires explicit initialization, per-chunk, and final integrity
markers. Missing markers are a route capability failure even when an exit code is zero.

## 13. API and Agent Interaction Examples

### 13.1 Host State Response

```json
{
  "host_id": "company-4090-a",
  "overall": "degraded",
  "best_access_path_id": "ap_company_lan_ssh",
  "connector": {
    "state": "online",
    "last_seen_at": "2026-07-03T20:01:12+08:00"
  },
  "access_path": {
    "state": "reachable",
    "latency_ms": 18,
    "last_checked_at": "2026-07-03T20:00:55+08:00"
  },
  "ssh_session": {
    "state": "connected",
    "pooled": true,
    "open_channels": 1,
    "idle_seconds": 42,
    "reused_count": 17
  },
  "host_health": {
    "state": "healthy",
    "facts_age_seconds": 3600,
    "gpu_count": 4,
    "docker_state": "running"
  },
  "retry": {
    "allowed": true,
    "retry_after_seconds": 0
  }
}
```

### 13.2 Access Path Example

```yaml
host: amd-windows-home
environment: home-lan
access_paths:
  - name: from-macstudio-lan
    from_connector: macstudio-home
    protocol: ssh
    address: 192.168.31.20
    port: 22
    username: sshuser
    credential_id: cred_amd_windows_sshuser
    route_type: lan

  - name: public-frp
    from_connector: local
    protocol: ssh
    address: example.com
    port: 6004
    username: sshuser
    credential_id: cred_amd_windows_sshuser
    route_type: frp
```

## 14. Phased Roadmap

### Phase 0: Documentation and Skeleton

- Finalize this product and technical plan.
- Initialize Rust workspace.
- Add formatting, linting, basic CI/local checks.
- Create initial migrations.

### Phase 1: Local Registry and Vault

- Host, environment, connector, access path CRUD.
- Internal encrypted credential vault.
- Automatically generated local vault key shared by MCP credential writes and the connector.
- Agent-profile tools to register or update user-supplied credentials without returning plaintext.
- Key-first authentication with encrypted-password fallback and automatic public-key bootstrap.
- SQLite persistence.

Success criteria:

- A user can register hosts, access paths, and credentials without plaintext secret storage.

### Phase 2: State-Aware Access Resolution

- Connector heartbeat.
- Passive/tcp/ssh/facts refresh levels.
- Access resolver chooses best route.
- State snapshots and state events.

Success criteria:

- An agent can ask whether a host is reachable and why not.

### Phase 3: Connection Manager MVP

- OpenSSH ControlMaster backend.
- Session pool state tracking.
- Per-host concurrency and rate limits.
- Server protection policy with handshake token bucket, process token bucket, queue depth, cooldowns, and overload states.
- Read-only command profiles.

Success criteria:

- Repeated checks reuse SSH connections, do not repeatedly login, and return `throttled` or `target_overloaded` instead of hammering the server.

### Phase 4: Agent Session Supervisor

- Persistent workspace model.
- One isolated PTY/workspace per Agent Session and host when interactive continuity is needed.
- One shared physical SSH transport per access path across Agent Sessions.
- Session-scoped idempotency and hierarchical resource write leases for conflicting mutations.
- Workspace state: idle, working, blocked, done, failed, throttled.
- Recent output snapshots and wait tools.
- MCP tools for workspace creation, read, wait, and close.

Success criteria:

- An agent can continue work through an existing workspace instead of opening more SSH sessions, and can tell whether the workspace is blocked, busy, done, or throttled.

### Phase 5: MCP Gateway

- Read-only MCP tools for hosts, facts, access, state, and knowledge.
- Controlled probe tools.
- Structured error recovery hints.

Success criteria:

- Codex or another MCP client can query and diagnose hosts without seeing secrets.

### Phase 6: Native Rust SSH Pool

- Shared `russh` backend for check/exec and persistent PTY channels.
- Native channel lifecycle and cancellation.
- PTY resize/signal API and MCP controls.
- Pooled OpenSSH/native SFTP upload and download with bounded size/time, SHA-256 verification, temporary placement, and overwrite policy.
- Interactive asset-menu PTY upload and download with connector-local frame capture, per-chunk verification, whole-file source stability checks, and no file bodies in persisted audit output.
- Port forward support.

Success criteria:

- Connector can run high-volume checks with bounded, observable channel reuse.

### Phase 7: Knowledge and Runbooks

- Operation summary generation.
- Knowledge items linked to operations and hosts.
- Approved runbook execution.
- Web UI can be added after core agent flows are stable.

Success criteria:

- The system becomes a durable operational memory, not just a connection tool.

## 15. Verification Strategy

Unit tests:

- Domain state transitions.
- Access path resolution.
- Credential encryption/decryption.
- Policy decisions.
- Command profile validation.

Integration tests:

- SQLite migrations.
- MCP tool handler success/failure.
- Connector heartbeat.
- Mock SSH transport.
- Circuit breaker behavior.
- Local/containerized SSHD coverage for pooled exec, persistent PTY, SFTP, stale-session replacement,
  and cross-workspace reuse.
- Real gateway coverage for dropped stdin, EOF, stdout, stderr, exit status, and completion markers.
- Independent per-path and connector-wide handshake-budget cooldowns.

Manual/local smoke tests:

- Register a host and access path.
- Add a credential.
- Unlock vault.
- Resolve access.
- Run passive/tcp/ssh refresh.
- Run a read-only check twice and confirm session reuse.

Security checks:

- Ensure secrets never appear in API/MCP responses.
- Ensure operation logs are redacted.
- Ensure `auth_failed` does not enter rapid retry loops.
- Ensure arbitrary shell commands remain bounded by explicit intent, timeout, output, concurrency,
  and audit controls without requiring a domain-specific MCP tool.

## 16. Open Questions

- Should the first connector and core service run in one process for MVP, or separate from day one?
- Should the first database be SQLite only, or should Postgres migrations be maintained from the start?
- Which recurring workflows deserve optional runbooks without changing the generic channel contract?
- What is the minimum Web UI needed before the MCP flow is useful?
- How should multi-user ownership and permissions be handled if this grows beyond a personal system?

## 17. References Checked

- Model Context Protocol introduction: https://modelcontextprotocol.io/docs/getting-started/intro
- `rmcp` Rust SDK: https://docs.rs/rmcp/latest/rmcp/
- `tokio`: https://docs.rs/tokio/latest/tokio/
- `axum`: https://docs.rs/axum/latest/axum/
- `sqlx`: https://docs.rs/sqlx/latest/sqlx/
- `russh`: https://docs.rs/russh/latest/russh/
- `openssh`: https://docs.rs/openssh/latest/openssh/
- `chacha20poly1305`: https://docs.rs/chacha20poly1305/latest/chacha20poly1305/
- `argon2`: https://docs.rs/argon2/latest/argon2/
- Herdr article: https://sohanscript.vercel.app/note/i-ditched-tmux-for-herdr-but-why-though
- Herdr socket API: https://herdr.dev/docs/socket-api/
- Herdr source audit: local `git` checkout at `66be0b655fe922867f1eed100a41d67038b6ffd6`

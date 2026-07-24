# Task: Production Remote Hosts System

## Goal

Build the production-grade Rust implementation of the remote host knowledge, access, state, and execution center described in `REMOTE_HOSTS_PRODUCT_TECH_PLAN.md`.

## Completed

- [x] Product and technical plan.
- [x] Rust workspace pinned to Rust `1.94.1`.
- [x] Core crates for domain, core policy, vault, db, connector, API, MCP contracts, and CLI.
- [x] Initial database migration covering hosts, environments, credentials, access paths, facts, software installs, connection sessions, workspaces, PTYs, operations, knowledge, and state events.
- [x] Internal credential vault with Argon2id + XChaCha20-Poly1305.
- [x] Server protection baseline policy.
- [x] Command profile validation.
- [x] Secret redaction.
- [x] Guarded transport wrapper for validation, concurrency limiting, truncation, and redaction.
- [x] CLI doctor and migration commands.
- [x] Health API shell.
- [x] MCP tool name and request schema contracts.
- [x] Repositories for environments, connectors, credentials, access paths, access path health, facts, software installs, connection sessions, workspaces, PTYs, operations, knowledge, and state events.
- [x] Access resolver and host state aggregation service.
- [x] HTTP API endpoints for host listing, host detail, access path listing, access resolution, and host state.
- [x] OpenSSH native `ControlMaster` transport backend behind `RemoteTransport`.
- [x] Connector heartbeat service with state snapshots and state event writing.
- [x] Agent workspace supervisor core with default one-workspace-per-host protection.
- [x] HTTP API endpoints for connector heartbeat/events and workspace create/list/get/state/PTY listing.
- [x] MCP tool contract names and schemas for connector heartbeat/events and workspace state surfaces.
- [x] Repository-backed MCP server handlers for host/access/state/knowledge/heartbeat/workspace tools.
- [x] CLI MCP stdio entrypoint for local agent integration.
- [x] Built-in structured command profile catalog for agent-safe workspace operations.
- [x] Workspace operation queue planning with policy checks, operation records, and initial system output chunks.
- [x] Database migration and repositories for workspace-linked operations and redacted output chunks.
- [x] MCP handlers for command profile listing, run-in-workspace, output reading, workspace waiting, and close workspace.
- [x] HTTP API endpoints for command profiles, workspace operation queueing/listing/output/wait/close.
- [x] Operation claim leases with attempt counts, claim tokens, lease expiry, and last-error tracking.
- [x] Connector-side operation worker that claims queued operations, executes through `RemoteTransport`, stores redacted output chunks, and updates workspace/operation state.
- [x] Cached OpenSSH `ControlMaster` transport provider keyed by access path.
- [x] CLI `worker-once` command for executing one queued operation for a connector.
- [x] Long-running connector daemon loop with heartbeat emission, idle backoff, infrastructure-error backoff, Ctrl-C graceful shutdown, and offline state recording.
- [x] Lease renewal during long-running remote exec and claim-token-guarded operation finish.
- [x] File-backed large output artifacts with SHA-256 metadata, redacted previews, API/MCP metadata surfaces, and connector CLI tuning flags.
- [x] Recovery policy and agent-visible `exhausted` operation state for expired claims that reach `max_attempts`.
- [x] Policy-guarded persistent PTY lifecycle records with open/heartbeat/close/reap API and MCP tools.
- [x] Connector-local managed shell backend over reused OpenSSH transport with redacted PTY output chunks.
- [x] HTTP and MCP read surfaces for bounded PTY output polling.
- [x] DB-backed PTY input queue with API/MCP enqueue, status polling, connector claim leases, and connector-owned delivery pump.
- [x] Per-session/access-path OpenSSH PTY backend factory with existing-session activation and shipped `worker-daemon` input pump wiring.
- [x] Agent-visible PTY backend state and capability fields persisted in PTY session records.
- [x] OpenSSH ControlMaster true-TTY backend mode using a persistent `ssh -tt` child without repeated SSH handshakes.
- [x] Native `russh` check/exec transport provider with cached sessions, host-key policy, and internal vault-backed SSH credentials.
- [x] Shared native `russh` transport pool for operation and PTY backends.
- [x] Native `russh` persistent PTY backend with `request-pty`, shell startup, pooled session reuse, and persistent input/output streaming.
- [x] Access-path keepalive and idle TTL wired into OpenSSH ControlMaster and native `russh` sessions.
- [x] Cached-session health validation, bounded replacement handshake token bucket, connection-session metrics, access-path failure state, and circuit cooldown enforcement.
- [x] Connector startup PTY reconciliation and backend-exit state convergence without silently replacing a lost runtime with a new shell.
- [x] Automatic logical connection-session selection/creation for PTY open requests and operation-to-session binding.
- [x] Snapshot-first MCP runtime view across connector, access path, SSH session, workspace, PTY, and recent operation state.
- [x] Shared OpenSSH transport pool injected into daemon operation and PTY backends.
- [x] Monotonic runtime state-event cursors with explicit `live_only`/`after_cursor` HTTP and MCP waits.
- [x] MCP `agent`/`admin`/`full` tool profiles with an 18-tool default agent surface.
- [x] Agent-profile encrypted credential capture for registration and existing-host credential rotation.
- [x] Native key-first authentication with bounded SSH-agent/default-key attempts, password fallback, and idempotent POSIX/Windows public-key bootstrap.
- [x] Persistent authorized-key bootstrap state with independent timeout, crash cooldown, bounded retry suppression, permanent-failure classification, key-fingerprint reset, and agent-visible recovery hints.
- [x] Multi-hop route preflight that reloads route metadata, invalidates stale direct caches, and rejects unsupported jump chains before SSH handshake.
- [x] Automatic local vault-key generation and shared MCP/connector launch configuration.
- [x] Task-oriented `prepare_workspace` and combined `get_workspace_result` facade tools.
- [x] One-call named password credential creation through `ensure_host`, with canonical environment preservation and same-endpoint route reclassification.
- [x] Single-hop bastion endpoint semantics for interactive menus and gateway usernames, while real proxy chains still fail before handshake.
- [x] Managed `shell.posix` and `shell.powershell` profiles for real operations through the pooled workspace, with bounded summaries and configurable timeout/output limits.
- [x] Proactive connector-side PTY activation before first input plus MCP backend readiness and polling guidance.
- [x] Terminal-workspace PTY filtering and one-shot activation failure convergence with redacted recovery output.
- [x] Runtime snapshot filtering so disabled access paths cannot contribute stale connection-health warnings.
- [x] Managed OpenSSH and native `russh` SFTP upload/download through the existing pooled session, with bounded size/time, SHA-256 verification, same-directory temporary placement, atomic rename, mode, and overwrite policy.
- [x] Route-compatible POSIX exec-channel file transfer for empty-chain bastion endpoints that cannot carry SFTP writes, with bounded encrypted chunks, no file body in MCP/audit persistence, and mandatory per-stage completion markers.
- [x] Independent per-access-path and connector-wide shared SSH handshake budgets; local path cooldown is no longer expanded to the ten-minute global window.
- [x] Route-aware raw and guarded transport cache replacement after endpoint, route, host-kind, keepalive, or connection-policy changes.
- [x] Bounded MCP reads for complete redacted output artifacts with offset pagination, UTF-8 boundary handling, and artifact-root containment checks.
- [x] Local handshake-budget exhaustion reported separately from target sshd rate limiting, preserving the exact retry delay without increasing target failure counters or opening its circuit.
- [x] Workspace preparation reuses only `idle` or `working` workspaces and never returns `throttled`, `blocked`, `failed`, or closed workspaces for new work.
- [x] Successful PTY activation converges access-path health to connected and clears expired local-handshake throttle attention.
- [x] Runtime snapshots convert expired local-handshake throttles into `local_handshake_budget_ready` with one-retry guidance instead of a zero-second wait loop.
- [x] Connector startup invalidates connector-local `connected/healthy/resolving` sessions and clears open-channel counts so persisted history cannot masquerade as a live SSH transport.
- [x] Snapshot v6 connector-local SSH runtime telemetry and per-channel evidence for exec, file transfer, and PTY, distinguishing handshake, real transport reuse, same-runtime reconnect, and runtime replacement.
- [x] Durable Agent Session identity with session-owned Workspaces, PTYs, operations, output, artifacts, and legacy-state recovery rules.
- [x] Session-scoped semantic idempotency for commands, file transfers, and PTY input, including exact retry reuse and mismatched-payload rejection.
- [x] Host-scoped write leases that serialize cross-conversation mutations without blocking read-only work or creating another SSH transport.
- [x] PTY lease continuity with a 300-second post-input window, output/activity renewal, and bounded handoff after close, backend exit, or connector restart.
- [x] Arbitrary POSIX/PowerShell commands over reused exec channels plus persistent PTY state when shell context must survive between inputs.
- [x] CLI `worker-daemon --pty-backend-mode auto|control-master-tty|pipe-shell|russh-native-pty`.
- [x] macOS launchd service management script for install/update/start/stop/restart/status/logs.
- [x] Local launchd deployment running API and connector daemon with release binary, SQLite database, logs, and default connector bootstrap.
- [x] Verification: `cargo fmt --all`, `cargo test --workspace`, `cargo check --workspace`, strict clippy.
- [x] Release deployment smoke: migration applied, launchd API/connector healthy, fresh MCP stdio exposes 18 Agent tools and `snapshot_version=6` without opening a remote SSH connection.

## Next

- [ ] Add a real gateway/SSHD regression suite for pooled arbitrary commands, session invalidation, one bounded reconnect, cross-workspace reuse, SFTP and exec-channel file transfer, 1 GiB files, complete Artifact reads, and gateways that drop stdin/EOF/stdout/exit-status signals.
- [ ] Add local SSHD integration tests for `control-master-tty` and native `russh` PTY backend.
- [ ] Add a multi-process MCP integration test that drives two Agent Sessions through one real pooled SSH transport while proving workspace/PTY isolation and write-lease handoff.
- [ ] Emit authoritative connection, workspace, operation, and PTY lifecycle events into the sequenced runtime event log.
- [ ] Add agent-state explain output and authoritative lifecycle hooks before considering screen-output heuristics.
- [ ] Add a target-side worker mode for true PTY/process continuity across connector restarts.
- [ ] Add HTTP/MCP control methods for native `russh` PTY resize and signal delivery.
- [ ] Implement port forwarding behind the existing pooled transport trait.
- [ ] Define a typed proxy-chain schema and implement verified multi-hop routing without exposing jump-host credentials or bypassing route intent.
- [ ] Expand HTTP API endpoints for registry mutation, knowledge search, and operations.
- [ ] Add Linux systemd packaging and service templates.
- [ ] Add first-class CLI registry mutation commands for hosts/environments/connectors/access paths.

Domain-specific Kubernetes, Harbor, database, middleware, GPU, and deployment behavior is intentionally not a core MCP-tool roadmap. Use arbitrary shell/PowerShell or PTY on the pooled connection; package repeated workflows as optional runbooks.

## Verification Gates

- [x] `cargo fmt --all`
- [x] `cargo test --workspace`
- [x] `cargo check --workspace`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] CLI migration smoke test against a new SQLite file
- [x] Fresh installed-binary MCP initialize/tool-list/read-only snapshot smoke test
- [x] No plaintext secrets in logs/API/MCP responses

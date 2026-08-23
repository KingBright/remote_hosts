---
name: remote-hosts-agent
description: Operate and maintain the user's transport-first Remote Hosts system for managed SSH machines and infrastructure topology. Use for arbitrary POSIX/PowerShell commands over pooled connections, persistent PTYs, host inventory and deduplication, cluster and per-host service topology, encrypted credentials, password fallback and public-key bootstrap, direct/FRP/VPN/proxy/jump/bastion routes, file transfer, output artifacts, runtime health and errors, and MCP setup in Codex or Antigravity. Do not invent domain-specific tools when the remote CLI can run through the generic channel.
---

# Remote Hosts Agent Skill

Use this skill to interact with the user's Remote Hosts service instead of opening raw SSH sessions directly.

## Core Rules

1. Prefer `remote_hosts_*` MCP tools over shelling out to `ssh`, editing the SQLite database, or inventing host state.
2. When the user explicitly supplies an SSH password, private key, passphrase, or sudo password, store it immediately through `remote_hosts_ensure_host.access.credential_secret` or `remote_hosts_store_host_credential`. Never repeat it in replies, logs, notes, knowledge, shell commands, or non-credential tools. A stored sudo password can later answer only a live PTY sudo prompt through `remote_hosts_queue_pty_input` with `use_stored_sudo_password=true`. A stored SSH password for another registered Host can answer only a live nested SSH prompt through `use_stored_password_from_host_id`; first verify and pin the target host key in the current SSH user's `known_hosts`, then the immediately preceding delivered PTY input must be exactly `/usr/bin/ssh -o StrictHostKeyChecking=yes -o NumberOfPasswordPrompts=1 -p <port> <username>@<address>` plus Enter, derived from that Host's only enabled SSH path. Never paste either password into `input`.
3. Never create or suggest a duplicate host before running the duplicate checks in [host-registry.md](references/host-registry.md).
4. Reuse only `idle` or `working` workspaces owned by the current Agent Session and healthy PTY/access-path state. The one exception is an already-active PTY with visible `interaction`: keep that same blocked workspace and answer through the same PTY instead of creating another SSH session.
5. Treat `blocked`, `exhausted`, `throttled`, `rate_limited`, `connector_offline`, `auth_failed`, `host_key_changed`, and `ssh_route_unsupported` as stop-and-diagnose states, except `pty_input_required` with an active, input-allowed PTY, which requires a deliberate response on that PTY. For `pooled_transport_invalidated`, wait for its short cooldown, then make one fresh workspace/connection attempt; never restart the connector or interrupt an unrelated PTY solely to clear a discarded pooled transport.
6. Use `shell.posix` or `shell.powershell` as the normal path for domain work. Narrow read-only profiles are optional shortcuts, not an extensibility boundary. Prefer one persistent PTY when interaction, shared shell state, or an unreliable gateway makes repeated exec channels unsuitable.
7. Keep output bounded. Use output artifact tools for large logs and read full content only in bounded chunks.
8. For normal host registration or route updates, use the idempotent `remote_hosts_ensure_host`; it performs duplicate checks and preserves the canonical host identity.
9. The default MCP `agent` profile is intentionally compact but supports task-level host and encrypted credential management. Use `admin` only for low-level registry repair or manual entity maintenance; never bypass a missing tool by editing SQLite directly.
10. Prefer public-key authentication. When a stored password is needed, the connector tries local keys first, uses the password as fallback, and then schedules one bounded, idempotent local-public-key install. A failed install never invalidates the authenticated pooled session or requires the user to type the password again.
11. Inspect each runtime snapshot access path's `authorized_key_bootstrap` and related `attention` before retrying. `proxy_jump` or a non-empty `proxy_chain` is true multi-hop and must stop on `ssh_route_unsupported`. An empty-chain `bastion` route is a single SSH endpoint and may use an interactive menu or an explicitly documented gateway username. For Smart Mine production, the only SSH entry is `10.36.31.20`; all internal machines must be selected inside its interactive menu, and no direct-login username or internal-IP access path may be inferred.
12. Never loop on a pending or failed PTY. Only `idle` or `working` workspaces are activatable. `workspace=blocked` is still live only when the same PTY reports `backend_state=active`, `input_allowed=true`, and a non-null `interaction`; read its output and queue one intentional response rather than reopening it. For `interaction.kind=sudo_password`, send no `input`: queue `use_stored_sudo_password=true` with one stable idempotency key. For macOS's bare `Password:` form, do this only when this Agent Session just sent an explicit `sudo` command to that same PTY with no intervening input; otherwise leave the PTY blocked and diagnose. For `interaction.kind=password` created by the exact connector-verified nested SSH command in rule 2, send no plaintext input: queue `use_stored_password_from_host_id=<target-host-id>`. The target must have exactly one enabled SSH path. The connector rechecks the prompt, target path, Agent Session, immediately preceding command, and two-minute prompt window before decrypting in memory; a fake or intervening prompt fails closed. If stored injection reports unavailable, do not fall back to plaintext. If `backend_state=failed`, input is disabled, or system output says automatic retry is disabled, inspect the snapshot and output, recover or replace the workspace connection, and open a new PTY id.
13. Use `remote_hosts_upload_file` and `remote_hosts_download_file` for deployment files. They reuse the workspace's pooled SSH session, enforce size/timeout/overwrite policy, verify SHA-256 at both ends, and place through a same-directory temporary file. An interactive asset-menu bastion supports both directions through the already selected active PTY; transfer frames are captured in connector memory and file bodies do not enter MCP or PTY audit output. Exec-channel uploads use per-I/O no-progress timeouts, retain and verify partial bytes across reconnects, and emit progress plus 30-second active heartbeats. A successful command or transfer keeps the healthy SSH session pooled; a timeout or missing completion frame invalidates it before reuse. Let the connector perform its bounded idempotent-stage retries; do not create parallel transfer operations or encode file bodies into agent-visible shell arguments.
14. Handshake protection has independent per-access-path and connector-wide budgets. On `local_handshake_budget_exhausted`, respect the exact `retry_after_seconds`; do not create another workspace or session to bypass either bucket.
15. For gateway file fallback, require explicit initialization, per-chunk, and final markers plus integrity checks. Missing output or markers is failure even when the channel reports exit status zero. Initialization, chunk append, and final placement may retry internally up to the bounded stage limit because they verify remote state and are idempotent. Once that budget is exhausted, stop until route capability or configuration changes. Never generalize this retry permission to arbitrary shell writes.
16. Require `snapshot_version=10` when reasoning about reuse, conversation isolation, scoped mutation coordination, Workspace capacity, SSH channel pressure, or PTY interaction. Inspect host-level `workspace_capacity` separately from each access path's `channel_capacity`: logical Workspaces do not consume SSH channels until an operation or PTY opens one. `expired_reapable` is stale state that `prepare_workspace` and the connector reconcile automatically; a full `effective_active` count means live operations/PTYs or genuinely live Workspace owners still hold capacity. A logical connection session is not proof of a live SSH transport. Inspect each access path's `transport_runtime` and `channel_capacity`, plus each recent operation or PTY's `transport_evidence` and `interaction`. `transport_runtime=null` means cold/unobserved, not connected or failed; the next real channel may require a handshake.
17. Treat `channel_capacity.state=saturated|oversubscribed` and attention `ssh_channel_capacity_saturated` as local queue pressure, not SSH failure. Keep the same Workspace, operation, PTY, and idempotency key; wait for a reservation to clear. Raise `max_concurrent_channels` only when the target and bastion are known to support more channels.
18. Interpret `transport_evidence.connection_use` precisely: `reused` proves an existing authenticated SSH connection served the channel; `first_handshake` means this runtime opened its first connection; `reconnected` means the same runtime replaced a failed connection; `attempt_failed` means a real connection attempt did not authenticate. `runtime_replaced=true` separately means route change or connector restart created a different runtime.
19. Treat Agent Session boundaries as strict. Never reuse a Workspace or PTY id copied from another Codex/Antigravity task. If ownership is rejected, call `remote_hosts_prepare_workspace` in the current task; do not switch profiles or create raw SSH state to bypass isolation.
20. Use a stable semantic `idempotency_key` for every mutating shell step, upload/download placement, and PTY input. Keep the same key and exact payload for a retry; use a new key for a changed command, file specification, or input. Never generate a new key merely because a wait timed out.
21. Choose one stable lowercase `coordination_scope` when preparing a Workspace. Use `host` when the task may touch machine-wide state, several unrelated resources, or an uncertain boundary. Use the narrowest known canonical resource path for contained work, for example `k8s/prod/datatool-dev/service/file-gateway`; equal and parent/child scopes conflict, while siblings may mutate concurrently. If one task spans resources, use their real common parent. Never vary spelling, aliases, or scope depth to evade a conflict.
22. Inspect `write_lease.active_leases`, not only its aggregate state. Wait only when the queued mutation overlaps a foreign scope. `recommended_action=wait_for_overlapping_scope_or_refine_scope` permits a narrower Workspace only when the original scope was genuinely over-broad and no side effect has begun; it never permits inventing a sibling scope. Read-only work may continue when the queue accepts it.
23. Physical and logical reuse are different: Agent Sessions, Workspaces, PTYs, operations, artifacts, and temporary context are isolated per conversation, while all conversations intentionally share the connector's pooled SSH transport for the selected access path.
24. Treat topology synchronization as an authoritative inventory write. Read [topology-and-inventory.md](references/topology-and-inventory.md), use one globally stable `external_key` per real resource, and submit a complete snapshot for exactly one `scope_key + source`. Never publish a partial or failed discovery as complete because omitted members become inactive for that producer scope.
25. Treat Agent-profile MCP responses as compact decision records. Follow `next_action` and `retry_after_ms`, retain stable `workspace.id`, `operation.id`, and sequence cursors, and read only new chunks. Do not request a runtime snapshot or command-profile catalog after every call; fetch diagnostics explicitly only when state, attention, capacity, routing, or transport evidence is needed.
26. Command and PTY audit summaries expose bounded, redacted previews. Use `operation.command_preview` to confirm the submitted action, but never expect secrets to appear. Password and sudo interactions remain type-only. Operators can inspect the same activity in the HTTP admin page's `Agent 活动` view without reading raw MCP JSON.
27. Idle lifecycle is connector-owned. Ordinary PTYs with no business activity are closed after the configured idle TTL, while zero-channel pooled SSH transports are released after the access path's `idle_ttl_seconds`; SSH keepalive does not count as business activity. For a quiet long-running PTY command, call `remote_hosts_heartbeat_pty_session` with a truthful `foreground_process` before the normal idle TTL and refresh it while the process remains active. Clear it or close the PTY when work finishes. Never fake foreground work merely to retain a channel.

## Service Assumptions

The local Remote Hosts service is expected to be installed through the platform service manager.
On the owner's current macOS workstation:

- Binary: `/Users/jinliang/.local/bin/remote-hosts`
- Database: `sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite`
- HTTP API: `http://127.0.0.1:8787`
- Service helper: `/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service`
- Output artifacts: `/Users/jinliang/.local/share/remote-hosts/artifacts`

On Windows, the release defaults to:

- Stable MCP launcher: `%LOCALAPPDATA%\RemoteHosts\bin\remote-hosts-launcher.exe`
- Service manager: `%LOCALAPPDATA%\RemoteHosts\bin\remote-hosts-service.ps1`
- Configuration: `%LOCALAPPDATA%\RemoteHosts\config\service.json`
- Database: `%LOCALAPPDATA%\RemoteHosts\data\remote-hosts.sqlite`
- HTTP API: `http://127.0.0.1:8787`
- Output artifacts: `%LOCALAPPDATA%\RemoteHosts\data\artifacts`

macOS uses user LaunchAgents. Windows uses current-user Task Scheduler jobs so native `russh` can
use that user's OpenSSH Agent named pipe, Pageant, private keys, and configuration. Windows MCP
clients should call the stable Rust launcher; it reads the current version pointer and forwards
stdio without a persistent PowerShell proxy.

If MCP tools are unavailable, first check whether the service is running:

```bash
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service status
curl -sS http://127.0.0.1:8787/v1/command-profiles
```

On Windows PowerShell:

```powershell
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Status
Invoke-RestMethod http://127.0.0.1:8787/v1/command-profiles
```

## Workflow Router

- For host inventory, aliases, deduplication, environment/access-path maintenance, or stale facts: read [host-registry.md](references/host-registry.md).
- For normal remote execution, workspace reuse, PTY sessions, and output polling: read [mcp-workflows.md](references/mcp-workflows.md).
- For connection health, connector state, throttling, SSH failures, and reuse policy: read [connection-and-errors.md](references/connection-and-errors.md).
- For cluster topology, host-internal services, dependency edges, inactive history, or topology credential bindings: read [topology-and-inventory.md](references/topology-and-inventory.md).
- For Codex/Antigravity MCP configuration and local service operations: read [setup-and-runtime.md](references/setup-and-runtime.md).

## Fast Path

For a typical request like "check machine X":

1. `remote_hosts_list_hosts` and identify the intended host.
2. For state-only work, call `remote_hosts_get_host_runtime_snapshot`; inspect `attention`, each access path's `authorized_key_bootstrap`, and retain `event_cursor` when later changes must be observed.
3. For execution, choose the stable `coordination_scope` for the intended mutation boundary and call `remote_hosts_prepare_workspace`. Omit it to use the conservative `host` default. The tool reuses only an `idle` or `working` workspace owned by this Agent Session with the same scope before creating one, automatically closes expired foreign history that owns no queued/running operation or active PTY, and returns compact Agent-session and Workspace identity plus `next_action`. Call the explicit runtime snapshot tool when capacity, transport, attention, or route diagnosis is actually needed. Require `snapshot_version=10` for logical Workspace capacity, transport-runtime identity, SSH channel capacity, per-channel reuse evidence, conversation ownership, scoped write-lease state, single-hop bastion, managed shell, proactive PTY behavior, and explicit input requests.
4. Call `remote_hosts_run_in_workspace` with `shell.posix` or `shell.powershell`, one script argument, explicit intent, a semantic `idempotency_key`, and realistic timeout/output limits. Set `wait_timeout_ms` for short commands so submission and observing this exact operation are atomic; a narrow profile is a convenience for a matching read-only check.
5. When `completion.completed=true`, read incremental chunks, the exact compact operation, and artifacts with `remote_hosts_get_workspace_result`. When it is false or omitted, follow `next_action` and `retry_after_ms` without changing the operation id or idempotency key. Request the host runtime snapshot only when transport evidence or a failure needs diagnosis; do not infer real reuse only from logical session metadata.
6. Upload or download manifests, kubeconfig, certificates, installers, packages, and collected files with the file tools on that workspace. Supply one stable `idempotency_key` per intended placement, keep `overwrite=deny` unless replacement is intentional, and pass an expected SHA-256 when one is known. Follow `bytes_transferred`, `resumed_bytes`, `retry_count`, and the 30-second active heartbeat instead of starting another channel. Missing transfer markers or suppressed output after the bounded internal retries ends the transfer attempt.
7. For artifact output larger than the normal result, call `remote_hosts_read_output_artifact_content` from offset zero and continue only with the returned `next_offset` until `eof=true`.
8. If the task becomes interactive, needs shell context, or repeated exec channels are unreliable, open one PTY without guessing a `session_id`. When `backend_ready=false` and `recommended_action=wait_for_pty_activation`, wait `poll_after_ms` before reading output; no menu exists yet and no input or reconnect is appropriate. A system output saying the PTY is waiting for SSH channel capacity means keep the same PTY and wait for capacity or inspect the runtime snapshot. Once `backend_ready=true`, inspect real `backend_capabilities` and the banner/menu before queueing each response with a distinct semantic `idempotency_key`. For `pty_input_required`, first read the latest output, then respond through that same active PTY even though its workspace is `blocked`. If pending activation converges to failed, do not retry that PTY id.
9. For a quiet PTY command expected to exceed the normal idle TTL, heartbeat the same PTY with a truthful `foreground_process`; refresh before its busy TTL while it remains active, then clear or close it. Output and accepted input refresh activity automatically, so do not send dummy terminal traffic as keepalive.
10. For connector state changes after the snapshot, call `remote_hosts_wait_runtime_events` with `start_mode=after_cursor` and the retained cursor. Use `live_only` only when retained history is intentionally irrelevant.

For a typical request like "record/update machine X":

1. Call `remote_hosts_ensure_host` with the proposed slug, display name, kind, risk level, and any known SSH route.
2. Inspect `duplicate_signals`, the returned canonical `host`, `defaults_applied`, and `attention`. Existing environment classification is canonical, and correcting only the route type for the same endpoint reuses that access path instead of creating another.
3. If it reports ambiguous canonical hosts, stop and ask the user which identity to keep; do not create another host.
4. If the user supplied credential material during registration, include it only in `access.credential_secret`. For an existing host, call `remote_hosts_store_host_credential`; omit `access_path_id` only when exactly one route exists.
5. Reuse the returned host id for knowledge and later workspaces. Add another route by calling `remote_hosts_ensure_host` again with that canonical identity and new access details.
6. Store durable non-secret observations with `remote_hosts_record_knowledge`. Use the `admin` profile only when low-level facts or registry repair are actually required.

For a typical request like "discover or update the topology":

1. Use the managed shell or PTY on the relevant Host to run existing inventory commands. Do not add a Kubernetes-, Harbor-, Docker-, database-, or middleware-specific MCP tool.
2. Read the current graph from the loopback `GET /v1/topology?include_inactive=true` endpoint.
3. Normalize the successful discovery into one complete `scope_key + source` snapshot with stable node and edge keys. Abort without syncing when discovery is partial, timed out, or ambiguous.
4. POST the snapshot to `/v1/topology/sync`, then read the graph again and verify active/inactive counts and important relationships.
5. Keep secrets out of topology metadata. Use the encrypted management form or a secret-safe client for node credential binding; never place a topology credential in a shell command, URL, audit note, or knowledge record.

## Reporting

In final answers, include:

- which host/workspace/access path was used;
- whether the connector and backend were healthy;
- the command profile or PTY action used;
- any blocked/throttled/error state and recovery hint;
- file direction, bytes, verified SHA-256, and overwrite policy for transfers;
- any runtime snapshot `attention` item, especially `pty_runtime_lost` or `connection_unhealthy`;
- each relevant access path's public-key bootstrap state when password fallback, key installation, or retry behavior mattered;
- runtime `snapshot_version` when single-hop bastion, managed shell, or PTY activation behavior mattered;
- Agent Session ownership, Workspace `coordination_scope`, and overlapping `write_lease.active_leases` when multiple tasks touched the same host;
- transport runtime id/backend/generation and the relevant operation or PTY `transport_evidence` when connection reuse, reconnect, or replacement mattered;
- topology `scope_key`, `source`, active/inactive counts, and any resource whose status changed when topology was synchronized;
- any registry data gaps that should be cleaned up.

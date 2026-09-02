# Connection, Reuse, and Errors

Use this when diagnosing SSH access, connector health, PTY status, throttling, or server overload.

## Reuse Policy

Prefer reuse in this order:

1. Existing `idle` or `working` workspace for the host and access path.
2. Existing active PTY for interactive work.
3. Existing connector-owned SSH session/access path.
4. New workspace.
5. New PTY.
6. Direct SSH only when Remote Hosts is unavailable and the user explicitly approves.

Avoid loops that create new workspaces, PTYs, or SSH sessions after every failed check.
Successful exec and file channels keep the authenticated transport pooled. A command timeout,
channel error, or missing POSIX completion frame invalidates that cached session before another
operation can reuse it.

## Health Checks

Before remote execution, inspect:

- the host runtime snapshot and its `attention` list;
- each selected access path's `transport_runtime`;
- each selected access path's `channel_capacity`;
- host state;
- resolved access path;
- connector state and recent events;
- workspace state;
- active PTY backend state/capabilities;
- server protection state.

## Stop States

Before using this list, separate local control-plane errors from SSH evidence. `database is locked`,
`SQLITE_BUSY`, lifecycle publisher backlog, and heartbeat `metadata_persisted=false` describe the
current Remote Hosts instance only. They do not prove TCP, SSH, the bastion, or an already-running
inner PTY failed. Keep the same ids and cursor, follow the bounded metadata retry action, and inspect
inner output when Remote Hosts is nested. Never reconnect, restart, signal, close, or replay a
mutation solely because local metadata persistence was delayed.

Stop and diagnose instead of retrying when you see:

- `connector_offline`: local connector is not heartbeating; check the platform service manager and
  logs.
- `auth_failed`: first verify the selected host/access path. If the user supplies a replacement password or key, immediately store it with `remote_hosts_store_host_credential` and retry once; never repeat the value in the response.
- `host_key_changed`: possible MITM or rebuilt host; ask the user to verify.
- `rate_limited` or `throttled`: wait or reduce concurrency.
- `local_handshake_budget_exhausted`: this is connector-local protection, not target sshd health. Respect the exact `retry_after_seconds` and `recommended_action=wait_for_local_handshake_budget`; do not create another workspace or session to bypass it.
- `local_handshake_budget_ready`: the exact local cooldown has elapsed and target reachability is stale. Perform one normal workspace/connection attempt; do not fan out retries.
- `pooled_transport_invalidated`: a completed SSH handshake lost a later exec or file channel, so
  the connector already discarded its cached transport. This is neither TCP reachability nor a
  credential failure. Wait for the short `retry_after_seconds`, then create/prepare one fresh
  workspace and make one normal attempt. Do not restart the connector or interrupt an unrelated
  PTY merely to clear this state.
- `file_transfer_incompatible` or missing transfer markers after the connector's bounded stage retries: the route did not prove transfer completion. Initialization, verified chunks, and final placement may retry internally because replay is idempotent; the caller must not open parallel transfers. Once the internal budget is exhausted, stop until route capability or configuration changes.
- `exhausted`: automatic retry budget is spent; read operation output and recovery hint.
- `blocked`: inspect recent output and decide next action.
- `pty_runtime_lost`: the connector process no longer owns the original backend; open a new PTY explicitly and do not assume cwd/process context survived.
- connection session `state=unknown` after a connector restart: the persisted history remains, but the in-memory SSH transport did not survive. Prepare normally and allow one connector-owned reconnect; do not treat the old session as live.
- transport runtime `state=runtime_lost`: its id, generation, and counters are historical evidence only. The next successful command should report `runtime_replaced=true` and one new handshake; never count the old runtime as connected.
- PTY system output containing `automatic retry disabled`: activation reached a terminal failure such as a missing or unusable backing connection. Do not poll or queue input again for that PTY id; inspect the workspace/connection state and open a replacement only after recovery.
- `circuit_open`: do not create another session id to bypass cooldown; wait until `next_retry_at` or change the route.
- `host_write_lease_wait`: another Agent Session holds an equal, parent, child, or broad `host` scope that overlaps at least one exact resource in this queued mutation. First confirm the operation really has `requires_write_lease=true`. Observational shell work should have been submitted with `coordination_mode=read_only`; do not relabel a command after side effects or merely to escape the wait. For real mutations, compare every `operation.coordination_scopes` entry with `write_lease.active_leases`, wait, and refresh state; do not create another PTY, route, or SSH connection. The singular `operation.coordination_scope` may be only a common-ancestor compatibility summary. Use `wait_for_overlapping_scope_or_refine_scope` only when the exact declaration was genuinely over-broad before side effects began, never to invent a differently spelled sibling. A foreign non-overlapping sibling lease does not block this task and does not mark the connector or target unhealthy.
- `ssh_channel_capacity_saturated`: the selected path has no unreserved SSH channels. Follow `recommended_action=wait_for_channel_or_raise_limit`, keep the same logical task ids, and wait for an operation or PTY reservation to clear. Do not create another route or SSH connection. Increase the path limit only after verifying the target and bastion support it.
- `tcp_unreachable` or `ssh_handshake_failed`: check network, VPN, route, port, and connector environment. If the returned cooldown has elapsed, prepare one fresh workspace before escalating; a stale failure state must not be treated as permanent reachability evidence.
- `ssh_route_unsupported`: stop immediately. The configured route needs one or more jump hosts and was rejected before handshake; do not retry or substitute a direct connection.
- `authorized_key_bootstrap_deferred`: keep using the stored-password pooled session and wait until `next_retry_at`; do not force another install.
- `authorized_key_bootstrap_skipped`: keep using the stored password. Add a local key only for `no_local_public_key`; permission, read-only, shell, and exhausted states require an explicit route/host change before automatic retry.
- `authorized_key_bootstrap_stalled`: the connector likely restarted during bootstrap and its crash cooldown elapsed; allow one normal connection attempt, not a loop.

## Connector Service Checks

Local macOS service commands:

```bash
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service status
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service logs
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service restart
```

Local Windows service commands:

```powershell
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Status
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Logs
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Restart
```

Use restart only when the service appears stuck, crashed, or after config updates. Both platform
managers refuse a normal restart while active conversation work has not drained.

## PTY Backend Capabilities

Interpret `backend_capabilities`:

- `openssh_pipe_shell`: persistent shell over pipes; no real TTY.
- `openssh_control_master_tty`: real TTY via local `ssh -tt` reusing ControlMaster.
- `russh_native_pty`: real SSH PTY channel on a pooled native `russh` session.

If a task needs curses, sudo prompt behavior, interactive installers, or shell job control, prefer a backend with `allocates_tty: true`.

For repeated ordinary commands, prefer a verified persistent PTY when the gateway's short exec
channels suppress stdin, EOF, stdout, stderr, completion markers, or status. Reuse that PTY and
track output sequence; do not probe by opening a new SSH transport for every command.

## Transport Runtime Evidence

Snapshot version 11 separates the real connector-local SSH runtime from logical sessions and workspaces, exposes privacy-aware Agent Session ownership and exact multi-resource write leases, and reports scheduler-visible channel pressure, proactive PTY state, and explicit interaction requests.

Interpret access-path `channel_capacity`:

- `configured_limit`: bounded channel pool size for this access path.
- `running_operations`: non-expired operation claims reserving channels.
- `active_ptys`: PTY backends currently holding channels.
- `pending_ptys`: activatable PTYs reserving the next available channels.
- `reserved_channels`: total scheduler reservations; `available_channels` is the remaining capacity.
- `state=available`: another operation or PTY may be scheduled.
- `state=saturated`: reservations equal the limit; wait without creating another SSH session.
- `state=oversubscribed`: old or concurrent reservations exceed the configured limit; allow them to drain before changing configuration.

Interpret `transport_runtime.telemetry`:

- `transport_runtime=null`: no connector-local runtime has been observed for this path since the feature was installed or state was cleared. Report it as cold/unobserved; never infer reuse.
- `runtime_id`: identity of one in-memory OpenSSH or `russh` transport object.
- `generation`: successful authenticated SSH connection generation inside that runtime.
- `connection_attempt_count`: real network connection attempts that passed local budgets.
- `successful_handshake_count`: authenticated handshakes completed by that runtime.
- `reuse_count`: successful validations/channel opens on an existing authenticated connection.
- `state=cold`: a runtime object exists but has not attempted a connection.
- `state=connecting`: one budget-approved connection attempt is currently in progress.
- `state=ready`: the last observed connector-local transport was reusable.
- `state=idle`: the connector intentionally released a healthy zero-channel transport after the access path's idle TTL; the next real operation may perform one budgeted handshake. This is not a network or credential failure.
- `state=disconnected`: the runtime observed that its last connection is unusable; one later channel may reconnect subject to handshake budgets.
- `state=runtime_lost`: the owning connector restarted; counters remain historical.

Interpret operation and PTY `transport_evidence`:

- `connection_use=reused`: this exact channel used an existing authenticated SSH connection.
- `connection_use=first_handshake`: this runtime opened its first authenticated connection.
- `connection_use=reconnected`: the same runtime replaced a failed authenticated connection.
- `connection_use=attempt_failed`: a real connection attempt did not complete authentication.
- `connection_use=unchanged`: no connection attempt or cached-session validation was observed.
- `runtime_replaced=true`: a route change or connector restart created another runtime id.

Do not use logical `session_id`, `open_channels`, or `reused_count` alone as proof of transport reuse.

## Idle Lifecycle

- Completed exec and transfer channels close immediately; only the authenticated transport remains pooled.
- Native `russh` releases that cached transport only when every channel permit is free and the access path `idle_ttl_seconds` has elapsed. Server keepalive packets do not refresh this business-idle clock.
- The connector closes an ordinary PTY after `REMOTE_HOSTS_PTY_IDLE_TTL_SECONDS` without output, accepted input, or heartbeat. The default is 3600 seconds.
- A PTY heartbeat with a non-empty `foreground_process` uses `REMOTE_HOSTS_PTY_BUSY_TTL_SECONDS`, default 86400 seconds. Heartbeat it truthfully while quiet work continues; output also refreshes activity automatically.
- Queued or claimed PTY input prevents idle reaping. A PTY close releases its channel and shortens every exact resource lease declared when that PTY opened before expired Workspace reconciliation runs.
- Connector polling while a pending PTY waits for channel capacity is not business activity. Keep the same PTY while actively waiting, but abandoned pending entries still expire normally.
- Zero disables the corresponding PTY expiry class. Do not disable expiry globally merely to preserve one long task.

## Retry Discipline

- Retry once only after the state indicates a transient failure.
- Per-access-path and connector-wide handshake budgets are independent. For `local_handshake_budget_exhausted`, wait the exact reported duration. Continue only after the snapshot reports `local_handshake_budget_ready`, then retry one normal workspace preparation. Never substitute the other bucket's window or convert it to a one-hour target cooldown.
- Wait for backoff or cooldown when throttled.
- Do not retry authentication, host key, or route failures without changed inputs.
- Do not trust a gateway's exit status alone when expected stdout/stderr or completion markers are absent.
- Do not manually repeat authorized-key installation while bootstrap is attempting, deferred, installed, or skipped for the same fingerprint.
- Record what was retried and why.

## Authentication Order

The native connector reuses one pooled session and authenticates in this order:

1. A private key stored in the encrypted local vault, when present.
2. At most two local SSH-agent identities per handshake.
3. Default unencrypted local keys when the agent has no identities.
4. The stored SSH/Windows password as fallback.

After password authentication, the connector persists an `attempting` guard and schedules the chosen local public key install outside the connection critical path. The command has an independent 10-second timeout. Bootstrap failure does not discard the authenticated pooled session, so the stored password continues to work without user input.

Interpret `authorized_key_bootstrap` by access path and key fingerprint:

- `attempting`: one bounded install is active, or a connector crash cooldown is protecting it. Do not start another.
- `installed`: that exact local key fingerprint is complete and will not be reinstalled.
- `deferred`: a transient timeout or remote-command failure is cooling down. Respect `next_retry_at`.
- `skipped`: the failure is permanent for that route/key, no local key exists, the route is unsupported, or three transient attempts were exhausted.

A changed local key fingerprint resets eligibility. Raw stderr, key content, and passwords are never stored in bootstrap state.

## Bastion and Multi-Hop Routes

True multi-hop means `route_type=proxy_jump` or a non-empty `proxy_chain`. Those routes are currently rejected before TCP/SSH handshake by both connector backends until the proxy chain has a typed format and a verified proxy-aware implementation.

`route_type=bastion` with an empty `proxy_chain` is one physical SSH connection. Use it for a bastion's own interactive asset menu or for gateway login names such as `username/server/account`. Set `requires_tty=true` only when the endpoint actually presents an interactive menu.

For an interactive asset menu, ordinary exec channels are invalid even when a previous attempt happened to return output. The MCP layer rejects `remote_hosts_run_in_workspace` for `requires_tty=true`; open one PTY, read the menu, select the target, and reuse that PTY.

Smart Mine production is a fixed exception to any gateway-username inference: `10.36.31.20` is the only SSH endpoint, `requires_tty=true`, and every internal asset must be selected inside that persistent interactive session.

- Never reinterpret the final target as directly reachable.
- Never walk a proxy chain recursively in agent logic.
- Never retry `ssh_route_unsupported`; select a verified direct/FRP/VPN access path or wait for route configuration to change.
- Route metadata is reloaded before cached transports are returned, so a formerly direct path changed to multi-hop invalidates its stale cache.
- Do not relabel a single-hop bastion endpoint as `vpn` merely to make the connector accept it.

## Server Protection

When a host appears overloaded:

- Stop issuing new commands.
- Read existing output/state first.
- Prefer one lightweight profile such as uptime or process snapshot after cooldown.
- Avoid PTY spam and high-frequency polling.

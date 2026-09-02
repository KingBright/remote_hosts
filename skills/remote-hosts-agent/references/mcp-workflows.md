# MCP Workflows

Use these workflows when `remote_hosts_*` MCP tools are available.

## Default Agent Tools

The `agent` profile intentionally exposes only 18 task-level tools:

- `remote_hosts_list_hosts`
- `remote_hosts_ensure_host`
- `remote_hosts_store_host_credential`
- `remote_hosts_get_host_runtime_snapshot`
- `remote_hosts_search_knowledge`
- `remote_hosts_record_knowledge`
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

The `admin` profile adds host deduplication/upsert, environments, credential references, access paths, facts, and operational maintenance. The `full` profile is reserved for development and debugging.

Agent-profile responses are compact by default. High-frequency calls preserve stable nested paths
such as `workspace.id` and `operation.id`, while omitting repeated runtime snapshots, command
catalogs, protection decisions, full Workspace records, and unrelated recent operations. Follow
`next_action` and `retry_after_ms`; call the explicit runtime snapshot or admin/full profile only
for diagnostics and maintenance.

Shell and PTY audit records contain bounded, secret-redacted command previews. Incremental output
chunks contain sequence, stream, text, and truncation state without repeating Workspace and
operation ids in every chunk. Password-like PTY interactions remain type-only.

## Standard Remote Check

1. List hosts and identify exactly one target. If multiple records may be the same machine, stop and follow `host-registry.md`.
2. For state-only work, read `remote_hosts_get_host_runtime_snapshot` and inspect `attention`, `authorized_key_bootstrap`, and `transport_runtime` for each access path.
3. Call `remote_hosts_prepare_workspace`. Its default Workspace scope is `host`, which is a safe upper boundary for later operation-level scopes. Create a narrower Workspace only when every operation in it belongs to that resource subtree. The tool reuses only an `idle` or `working` workspace owned by the current Agent Session with the same scope before creating one and returns compact Agent-session and Workspace identity plus the next action. Request a runtime snapshot explicitly only when state or connection diagnosis needs it.
4. Do not execute if snapshot attention reports `auth_failed`, `host_key_changed`, `connector_offline`, `rate_limited`, `throttled`, `circuit_open`, `ssh_route_unsupported`, overload, `pty_runtime_lost`, or `connection_unhealthy` without a recovery action. `local_handshake_budget_ready` allows exactly one normal connection attempt after cooldown. Bootstrap deferred/skipped state does not block password-backed execution unless route attention also blocks it.
5. For normal remote work, use `shell.posix` or `shell.powershell` with exactly one arbitrary script argument. Always set `coordination_mode=read_only` for a fully observational script or `coordination_mode=mutating` for any possible side effect. A one-resource mutation sets the narrowest truthful `coordination_scope`; one indivisible action across several disjoint resources sets the complete `coordination_scopes` array instead. Omitted `auto` keeps the legacy behavior and treats arbitrary shell as mutating. Include explicit `intent` and a stable semantic `idempotency_key`; set `timeout_seconds` up to 7200 and `output_limit_bytes` up to 8 MiB when defaults are insufficient. Use `wait_timeout_ms` (up to 60000) for short commands so queueing and observing the exact operation are one agent-visible action. Do not put passwords, tokens, or private keys in the script.
   `remote_hosts_run_in_workspace` intentionally rejects access paths marked `requires_tty=true`; use the Interactive Session workflow for those routes.
6. Use a narrow profile only as a shortcut when it exactly matches a read-only check. Do not request or implement a Kubernetes, Harbor, database, GPU, package-manager, or deployment-specific MCP tool when the existing remote CLI can run through the generic shell or PTY.
7. When `remote_hosts_run_in_workspace.completion.completed=true`, use its exact compact operation result directly. Otherwise follow `next_action` and `retry_after_ms`; preserve the operation id and idempotency key.
8. Read bounded incremental chunks, the requested operation, and artifact metadata with `remote_hosts_get_workspace_result`. Request a runtime snapshot when transport evidence is needed to prove reuse or diagnose reconnection.

Read-only shell example:

```json
{
  "workspace_id": "<current-conversation-workspace>",
  "command_profile": "shell.posix",
  "args": ["kubectl get pods -A -o wide"],
  "intent": "inspect current pod placement",
  "coordination_mode": "read_only",
  "idempotency_key": "inspect-pod-placement"
}
```

Independent mutation example:

```json
{
  "workspace_id": "<current-conversation-workspace>",
  "command_profile": "shell.posix",
  "args": ["kubectl -n datatool-dev rollout restart deployment/report-worker"],
  "intent": "restart report-worker deployment",
  "coordination_mode": "mutating",
  "coordination_scope": "k8s/prod/datatool-dev/deployment/report-worker",
  "idempotency_key": "restart-report-worker-20260829"
}
```

Atomic multi-resource mutation example:

```json
{
  "workspace_id": "<current-conversation-workspace>",
  "command_profile": "shell.posix",
  "args": ["./cleanup-rejected-data"],
  "intent": "clean rejected records from MinIO, MySQL, and Elasticsearch",
  "coordination_mode": "mutating",
  "coordination_scopes": [
    "prod/datatool-dev/storage/minio/rejected-data",
    "prod/datatool-dev/database/mysql/rejected-data",
    "prod/datatool-dev/search/elasticsearch/rejected-data"
  ],
  "idempotency_key": "cleanup-rejected-data-20260829"
}
```

## Managed File Transfer

Use file tools for deployment manifests, kubeconfig, CA material, installers, packages, and collected diagnostics:

1. Prepare one healthy workspace for the intended host and access path.
2. Use `remote_hosts_upload_file` for a connector-local source and remote destination, or `remote_hosts_download_file` for the reverse. Supply a stable semantic `idempotency_key` for the intended placement.
3. Paths must be absolute. Remote paths use `/` separators, including drive-letter paths such as `C:/Users/name/file.zip`.
4. Leave `overwrite` as `deny` unless replacement is intentional. `replace` still rejects directory and symlink destinations.
5. Set `max_size_bytes` and `timeout_seconds` for large files; defaults are 512 MiB and 600 seconds, with hard caps of 4 GiB and 7200 seconds.
6. Supply `expected_sha256` when a release or source digest is known. Use `mode` such as `0600` or `0755` only when destination permissions matter.
7. Wait and inspect the operation through the normal workspace result flow. A successful summary includes direction, byte count, verified SHA-256, overwrite policy, and `pooled_session=true`.

Direct routes open an SFTP subsystem channel on the pooled SSH transport. Uploads and downloads use a same-directory temporary file, verify both endpoints, and rename only after verification. Empty-chain POSIX bastions may use one stdin stream with a per-I/O no-progress timeout, then fall back to bounded Base64 exec-channel chunks without putting file bodies in MCP input or audit records. When the route requires an interactive asset menu, select the target once in an active Workspace PTY; both upload and download then reuse that PTY while ordinary input waits behind a transfer lock. The connector diverts raw transfer frames into memory instead of persisted PTY output. Interactive downloads use explicit chunk start/end frames, verify each decoded chunk, compare remote size and whole-file SHA-256 before and after transfer, and atomically place the verified local temporary file. Exec and PTY uploads use an artifact-stable remote temporary path, verify the retained prefix SHA-256, make duplicate chunk replay harmless, and recognize a matching already-placed destination after connector restart. Every initialization, chunk, and final placement requires an explicit marker; never accept exit status alone. The connector may retry eligible idempotent stages after transient transport failure and publishes `bytes_transferred`, `resumed_bytes`, `retry_count`, elapsed time, and a 30-second active heartbeat. A progress-record write failure does not cancel the active data channel. Successful commands and transfers retain the healthy pooled session; timeouts or missing completion frames invalidate it before reuse. Missing markers or suppressed output after the bounded retry budget ends the transfer. Stop transfer attempts on that route until capability or configuration changes. Multi-hop routes still stop at `ssh_route_unsupported`; do not work around them with recursive shell or raw SSH loops.

The current runtime snapshot schema is version 11. Older snapshots do not provide the complete Agent Session ownership, exact multi-resource write-lease set, connector-local transport runtime, channel-capacity reservations, connection generation, handshake/reuse counters, per-channel transport evidence, or explicit live PTY interaction contract. Reload the MCP child before relying on isolation, scoped coordination, pressure, reuse, or prompt handling claims.

## Conversation Isolation and Idempotency

- One MCP client process gets one Agent Session. An explicit client-instance or conversation key can make that identity stable across MCP restarts; a project key alone is metadata and never merges conversations.
- Agent Session, Workspace, PTY, operation, input event, output, artifact, and temporary context are isolated. Never carry Workspace or PTY ids from another task into the current task.
- The SSH transport is deliberately shared per access path. Do not interpret a new Workspace as a new SSH connection or create another route to escape logical isolation.
- Use one semantic idempotency key per intended side effect, such as `release-1.4-upload-linux-amd64` or `db-migration-20260724-step-2`. An exact retry keeps the same key and payload. Any changed payload requires a new key.
- A Workspace owns one immutable lowercase coordination boundary. Each mutating command selects one descendant with `coordination_scope` or up to 16 disjoint descendants with `coordination_scopes`; a `host` Workspace can select any valid resource set. Singular and plural fields are mutually exclusive. The complete set is acquired atomically, and a set containing both a parent and its child is rejected. Equal and parent/child resources across tasks conflict; siblings do not. Use canonical resource identity such as `k8s/<cluster>/<namespace>/<kind>/<name>`, and use singular `host` only when impact is genuinely host-wide or uncertain.
- Declared `read_only` shell work has `requires_write_lease=false`, can proceed beside foreign mutations, and is limited only by queue and SSH channel capacity. The declaration covers the complete script; a command that creates temporary files, refreshes credentials/caches, signals a process, or otherwise changes remote state is `mutating`.
- Inspect `write_lease.active_leases` against every queued `operation.coordination_scopes` entry. The legacy singular `operation.coordination_scope` may only be their common-ancestor summary. `held_by_other_session` alone may describe a non-overlapping sibling. Wait on `host_write_lease_wait`; refine a declaration only when it was genuinely over-broad before side effects began. Never change spelling or invent a sibling to bypass another task.
- `channel_capacity.state=saturated|oversubscribed` is path-local queue pressure. Keep the current Workspace and operation/PTY ids, respect `wait_for_channel_or_raise_limit`, and let reservations drain. Existing active PTY input remains eligible while a new PTY waits.
- PTY input holds the immutable exact `coordination_scopes` selected when the PTY opened, falling back to the Workspace boundary only when no exact set was declared. Output activity renews the same set. Close the PTY when finished so the connector can shorten the handoff period.

## Runtime Event Waits

Prefer `remote_hosts_get_agent_work_context` for normal Agent recovery and waiting. Start with
`mode=snapshot`, retain its `cursor`, then call `mode=wait` with that exact `after_cursor`. The
response is bound to the current Agent Session and returns only active items plus terminal changes
not yet consumed by that cursor. Follow the one `primary_action`; use entity-specific output or
runtime tools only when it says to read a result, respond to typed interaction, or inspect runtime.
Never interpret `changed=false` as failure or as proof that the Session is idle, and never create
duplicate work merely because a wait timed out. A no-change wait returns `overall_state=waiting`,
`primary_action.kind=wait`, an unchanged cursor, and empty items; retain the last changed context
and wait again from that same cursor.
Passing the returned cursor to the next wait with the same `host_id` filter durably acknowledges
all earlier lifecycle rows for that Agent Session and filter. Do not reuse a cursor after changing
the Host filter, invent a larger cursor, or discard the returned one during recovery.
The response's `lifecycle_outbox` block is publisher health, not business completion state. Pending
rows remain authoritative and visible to snapshot/wait even when the secondary state-event stream
is unavailable. Report sustained pending age or `last_publish_error`; do not restart a connector,
retry a mutation, or discard the cursor to clear observability backlog.

Use `remote_hosts_wait_runtime_events` only with an explicit start mode:

- `after_cursor`: pass the `event_cursor` returned by a host runtime snapshot, or `next_cursor` returned by a previous wait. This permits replay and closes the snapshot-to-subscription race.
- `live_only`: ignore all retained events and wait only for transitions created after the call begins.

A timeout is a normal structured result with `timed_out=true`; it is not evidence that SSH or the connector failed. Continue from `next_cursor`. The low-level runtime-event tool remains useful for connector/admin diagnostics. Normal Agent operation, Workspace, PTY, input, and transfer lifecycle recovery should use Agent Work Context so session isolation, terminal-event consumption, blockers, and next-action priority stay centralized.

## Standard Registry Maintenance

Use this for adding or updating normal host data:

1. Call `remote_hosts_ensure_host` with one proposed machine identity and an optional SSH route.
2. Inspect the canonical host, `duplicate_signals`, defaults, and attention returned by the tool.
3. If identity is ambiguous, stop and ask which canonical host to keep. The tool writes nothing in this case.
4. Call it again with the canonical identity when adding another environment or route for the same machine. If only `route_type` changes for the same environment/address/port/username/proxy chain, the service treats it as a correction and updates the existing access path.
5. If the user supplied a password or key during registration, put it only in `access.credential_secret`. If the host already exists, use `remote_hosts_store_host_credential`; it selects the only access path automatically and otherwise requires `access_path_id`.
6. Use `remote_hosts_record_knowledge` for durable non-secret notes, install history, operational context, and maintenance decisions.

Low-level cleanup and registry repair require the `admin` profile:

1. Run `remote_hosts_find_host_duplicates` with stable name, display name, and access hints.
2. If a likely duplicate exists, reuse that host id. If identity evidence is ambiguous, stop and ask for confirmation.
3. Use `remote_hosts_upsert_host` for machine identity, not for every IP or username.
4. Use `remote_hosts_list_environments` / `remote_hosts_upsert_environment` for network scopes such as home LAN, company LAN, VPN, FRP, public, or customer site.
5. Use `remote_hosts_list_credentials` to find credential metadata. If a new reference is needed, use `remote_hosts_upsert_credential_ref` with only non-secret metadata.
6. Use `remote_hosts_upsert_access_path` for each route. Equivalent paths are reused by host, environment, address, port, username, route, and proxy chain.
7. Use `remote_hosts_record_host_fact` for structured observed facts.

Credential handling rules:

- Accept credential material the user explicitly provides and send it once to `remote_hosts_ensure_host.access.credential_secret` or `remote_hosts_store_host_credential`.
- Do not repeat credential values in replies or put them in knowledge, notes, facts, tags, shell commands, or low-level registry tools.
- Use `remote_hosts_store_host_credential` to rotate or repair credentials while preserving secret fields the user did not replace.
- Public-key authentication remains first choice. A stored password is the fallback and enables automatic, idempotent local-public-key installation after a successful login.
- Automatic key installation has its own timeout and persistent retry suppression. Never turn a bootstrap failure into a password prompt or raw SSH retry loop.
- Never request, read, or expose the service's vault master password.

## Interactive Session

Open a PTY only when:

- the command requires TTY semantics;
- the user is debugging an interactive installer, REPL, shell, or long-running service;
- repeated command profiles would lose necessary session context.
- a gateway drops exec-channel stdin, EOF, stdout, stderr, completion markers, or status, but a native PTY has been verified.

Process:

1. Reuse an active PTY only when it belongs to this Agent Session and its immutable `coordination_scopes` truthfully cover the intended mutations. Otherwise open one PTY with the complete exact set; omit the set only when the Workspace boundary itself is the truthful mutation resource.
2. Inspect `backend_state` and `backend_capabilities`.
3. Inspect `transport_evidence` after activation. It records the runtime id/generation and whether opening the PTY reused the authenticated SSH connection or performed a handshake.
4. Prefer `russh_native_pty` or, on Unix only, `openssh_control_master_tty` for true terminal behavior. Windows always uses `russh_native_pty`. Omit `session_id` when opening unless you have an explicit compatible session; the service owns session reuse/creation.
5. Opening is proactive: the connector activates the pending backend without requiring dummy input. When `recommended_action=wait_for_pty_activation`, wait `poll_after_ms` first: no menu is available and input must not be queued. If system output reports local SSH channel capacity, keep this same PTY and wait for capacity or inspect the runtime snapshot; do not reconnect. Once `backend_ready=true`, read the banner/menu before responding.
   The connector does not send an initial `cd` command on any `requires_tty=true` route, including when the logical target is recorded as a Linux host rather than a jump host.
6. Queue input in small chunks with one semantic `idempotency_key` per intentional input; include trailing newline only when the user intended Enter. When the live `interaction.kind` is `sudo_password`, do not provide `input`. Instead call `remote_hosts_queue_pty_input` with `use_stored_sudo_password=true` and a stable idempotency key. The connector rechecks the same active PTY prompt, decrypts the access path's dedicated sudo field only in connector memory, sends it with Enter, and records only `stored_sudo_password` metadata. This also recognizes the bare `Password:` prompt used by macOS sudo, but use that form only immediately after this Agent Session sent an explicit `sudo` command to that same PTY, with no intervening input. For a nested SSH route, first verify and pin the target host key in the current SSH user's `known_hosts`, then send exactly `/usr/bin/ssh -o StrictHostKeyChecking=yes -o NumberOfPasswordPrompts=1 -p <port> <username>@<address>` when the source PTY Host is recorded as POSIX or `C:\\Windows\\System32\\OpenSSH\\ssh.exe -o StrictHostKeyChecking=yes -o NumberOfPasswordPrompts=1 -p <port> <username>@<address>` when it is recorded as Windows, plus Enter using the target Host's only enabled SSH path. Cross-platform forms fail closed. Only when the resulting live interaction is `password`, call the same tool with `use_stored_password_from_host_id=<target-host-id>`, no `input`, and no `requested_by`. The connector requires that exact immediately preceding delivered command from the same Agent Session, a prompt observed within two minutes, and an unchanged enabled SSH target both before and after vault decryption. Any intervening input, unpinned or changed host key, or fake prompt fails closed. If either stored-password event fails as unavailable, do not paste a password into PTY input; record or rotate it through the credential tool only when the user explicitly provides it.
7. Poll output incrementally and remember the last seen sequence.
8. For a quiet command expected to exceed the ordinary PTY idle TTL, call `remote_hosts_heartbeat_pty_session` with a truthful `foreground_process` and refresh it before the configured busy TTL. Output and accepted input already refresh activity; do not send dummy terminal input as keepalive. Heartbeat is advisory control-plane metadata. `metadata_persisted=false` with `continue_pty_and_retry_heartbeat` means only the local metadata write was contended: keep the same PTY, continue bounded output/state polling, and retry the heartbeat after `poll_after_ms`. Do not exit the business wrapper, send a signal, close the PTY, reopen SSH, restart the Connector, or start a duplicate command. Outer and inner Remote Hosts instances report only their own Workspace/operation/PTY effects; an outer helper failure never proves that an already-delivered inner command stopped.
9. Close the PTY when the task is complete, unless the user asked to keep it alive.

When snapshot `attention` contains `pty_input_required`, the PTY is still live: require
`backend_state=active`, `input_allowed=true`, and a non-null `interaction`, then read its latest
output and queue one deliberate response on that same id. Its Workspace may be `blocked` solely to
prevent unrelated work. Do not reconnect, open another PTY, or treat this as `pty_runtime_lost`.

## State Semantics

- `idle`: workspace exists and can accept work.
- `working`: wait or inspect recent output; avoid issuing unrelated operations.
- `done`: read output and summarize.
- `blocked`: normally read output/error and propose recovery. Exception: a same-workspace active,
  input-allowed PTY with `interaction` and `pty_input_required` is waiting for a response, not
  broken; read output and queue input on that existing PTY.
- `failed`: inspect connector/access path state before retrying.
- `throttled`: stop creating work; wait or ask user to relax policy.
- `closed`: create a new workspace if further work is needed.

`backend_state=failed` plus snapshot attention `pty_runtime_lost` means connector-local runtime continuity was lost. Never send more input to that PTY id; inspect prior output and explicitly open a new PTY.

A PTY may be activated only while both its own state and the backing workspace are `idle` or `working`. When activation finds a missing or unusable connection, the connector marks the PTY `blocked/failed`, disables input, and writes a system chunk containing `automatic retry disabled`. Treat that queue item as complete: inspect the runtime snapshot, recover or replace the workspace connection, and open a new PTY instead of polling or resubmitting input to the old id.

## Output Discipline

- Prefer summaries over full logs.
- Never paste secrets even if they appear in remote output; rely on redaction and still be cautious.
- For repeated polling, track sequence or requested limit to avoid rereading old chunks.
- Use `remote_hosts_read_output_artifact_content` for complete redacted logs. Start at `offset=0`, keep each request bounded, and continue with exactly `next_offset` until `eof=true`.

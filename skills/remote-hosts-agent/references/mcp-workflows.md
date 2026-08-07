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

## Standard Remote Check

1. List hosts and identify exactly one target. If multiple records may be the same machine, stop and follow `host-registry.md`.
2. For state-only work, read `remote_hosts_get_host_runtime_snapshot` and inspect `attention`, `authorized_key_bootstrap`, and `transport_runtime` for each access path.
3. For execution, choose a stable `coordination_scope` and call `remote_hosts_prepare_workspace`. Omit it for the conservative `host` default. The tool reuses only an `idle` or `working` workspace owned by the current Agent Session with the same scope before creating one and returns a fresh runtime snapshot plus command profiles.
4. Do not execute if snapshot attention reports `auth_failed`, `host_key_changed`, `connector_offline`, `rate_limited`, `throttled`, `circuit_open`, `ssh_route_unsupported`, overload, `pty_runtime_lost`, or `connection_unhealthy` without a recovery action. `local_handshake_budget_ready` allows exactly one normal connection attempt after cooldown. Bootstrap deferred/skipped state does not block password-backed execution unless route attention also blocks it.
5. For normal remote work, use `shell.posix` or `shell.powershell` with exactly one arbitrary script argument. Include explicit `intent` and a stable semantic `idempotency_key`; set `timeout_seconds` up to 7200 and `output_limit_bytes` up to 8 MiB when defaults are insufficient. Use `wait_timeout_ms` (up to 60000) for short commands so queueing and observing the exact operation are one agent-visible action. Do not put passwords, tokens, or private keys in the script.
   `remote_hosts_run_in_workspace` intentionally rejects access paths marked `requires_tty=true`; use the Interactive Session workflow for those routes.
6. Use a narrow profile only as a shortcut when it exactly matches a read-only check. Do not request or implement a Kubernetes, Harbor, database, GPU, package-manager, or deployment-specific MCP tool when the existing remote CLI can run through the generic shell or PTY.
7. When `remote_hosts_run_in_workspace.completion.completed=true`, use its exact operation result directly. Otherwise wait for `done`, `failed`, `blocked`, `throttled`, or timeout with `remote_hosts_wait_workspace_state`; preserve the operation id and idempotency key.
8. Read bounded chunks, recent operations, and artifact metadata together with `remote_hosts_get_workspace_result`. Inspect the operation's `transport_evidence` to prove whether the channel reused an authenticated transport or opened a new handshake.

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

The current runtime snapshot schema is version 10. Older snapshots do not provide the complete Agent Session ownership, hierarchical write-lease set, connector-local transport runtime, channel-capacity reservations, connection generation, handshake/reuse counters, per-channel transport evidence, or explicit live PTY interaction contract. Reload the MCP child before relying on isolation, scoped coordination, pressure, reuse, or prompt handling claims.

## Conversation Isolation and Idempotency

- One MCP client process gets one Agent Session. An explicit client-instance or conversation key can make that identity stable across MCP restarts; a project key alone is metadata and never merges conversations.
- Agent Session, Workspace, PTY, operation, input event, output, artifact, and temporary context are isolated. Never carry Workspace or PTY ids from another task into the current task.
- The SSH transport is deliberately shared per access path. Do not interpret a new Workspace as a new SSH connection or create another route to escape logical isolation.
- Use one semantic idempotency key per intended side effect, such as `release-1.4-upload-linux-amd64` or `db-migration-20260724-step-2`. An exact retry keeps the same key and payload. Any changed payload requires a new key.
- A Workspace owns one immutable lowercase `coordination_scope`. `host` conflicts with every mutation; equal and parent/child scopes conflict; sibling scopes do not. Use a canonical hierarchy such as `k8s/<cluster>/<namespace>/<kind>/<name>`, use the real common parent when one task spans resources, and default to `host` when uncertain.
- Inspect `write_lease.active_leases` against the queued operation's scope. `held_by_other_session` alone may describe a non-overlapping sibling. Wait on `host_write_lease_wait`; refine a scope only when it was genuinely over-broad before side effects began. Never change spelling or invent a sibling to bypass another task.
- `channel_capacity.state=saturated|oversubscribed` is path-local queue pressure. Keep the current Workspace and operation/PTY ids, respect `wait_for_channel_or_raise_limit`, and let reservations drain. Existing active PTY input remains eligible while a new PTY waits.
- PTY input holds the Workspace's scoped write lease for about 300 seconds; output activity renews it. Close the PTY when finished so the connector can shorten the handoff period.

## Runtime Event Waits

Use `remote_hosts_wait_runtime_events` only with an explicit start mode:

- `after_cursor`: pass the `event_cursor` returned by a host runtime snapshot, or `next_cursor` returned by a previous wait. This permits replay and closes the snapshot-to-subscription race.
- `live_only`: ignore all retained events and wait only for transitions created after the call begins.

A timeout is a normal structured result with `timed_out=true`; it is not evidence that SSH or the connector failed. Continue from `next_cursor`. The sequenced log currently carries connector state transitions, so keep using workspace wait and PTY/output tools for operation and interactive lifecycle changes.

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

1. Reuse an active PTY if one exists for the workspace.
2. Inspect `backend_state` and `backend_capabilities`.
3. Inspect `transport_evidence` after activation. It records the runtime id/generation and whether opening the PTY reused the authenticated SSH connection or performed a handshake.
4. Prefer `russh_native_pty` or, on Unix only, `openssh_control_master_tty` for true terminal behavior. Windows always uses `russh_native_pty`. Omit `session_id` when opening unless you have an explicit compatible session; the service owns session reuse/creation.
5. Opening is proactive: the connector activates the pending backend without requiring dummy input. Follow `recommended_action=read_pty_output`, wait `poll_after_ms` when returned, and inspect the banner/menu before responding.
   The connector does not send an initial `cd` command on any `requires_tty=true` route, including when the logical target is recorded as a Linux host rather than a jump host.
6. Queue input in small chunks with one semantic `idempotency_key` per intentional input; include trailing newline only when the user intended Enter.
7. Poll output incrementally and remember the last seen sequence.
8. Close the PTY when the task is complete, unless the user asked to keep it alive.

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

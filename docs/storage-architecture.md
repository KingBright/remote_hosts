# Remote Hosts Code storage model

This document defines what is durable, what is temporary, and what is allowed on hot control paths. The goal is not to preserve every intermediate fact forever. The goal is to preserve exactly the state required for safety and recovery while keeping polling, readiness, and upgrades independent of historical volume.

## Rules

1. **One owner per lifecycle.** State moves from the Agent to the Gateway when the Gateway explicitly accepts the result. Do not retain duplicate permanent copies after ownership transfers.
2. **Live state never depends on audit history.** Polling, heartbeat, drain, readiness, and terminal observation must query current state directly. `work_events` is a bounded history feed only.
3. **Transient observations expire.** Online presence, progress, terminal observations, transfer progress, and readiness samples are caches. They are not business records.
4. **Recovery state is durable only while recovery is possible or required.** Running/paused operations, resumable transfers, undelivered receipts, partial edit journals, and upgrade receipts remain durable.
5. **History is bounded by construction.** A feature that writes an unbounded row per operation must define its compaction/ownership rule before shipping.
6. **Hot queries must ignore completed history.** Existing indexed state columns should be used before adding new tables or indexes. Avoid JSON scans across completed jobs.

## Physical schema roles

`Store::open()` creates only the shared `kv` table and its single shared state index. The Agent explicitly installs `work_events`; the Gateway explicitly installs `jobs`, `semantic_guards`, and the temporary compatibility `operation_timing` table. Agent databases no longer create Gateway dispatch tables, and Gateway databases no longer install Agent event triggers.

Existing databases are migrated non-destructively: unused legacy tables are not blindly dropped from the opposite role because an old installation may have been configured unusually. New processes simply stop depending on or recreating the wrong-role schema.

## Agent state

| State | Purpose | Lifetime | Hot path? |
| --- | --- | --- | --- |
| `workspace` | Authorized project identity/root | Durable; workspace GC only removes eligible child state, not the workspace identity | No |
| `local_operation` running/paused | Crash/replay protection before result acceptance | Until completion/receipt acceptance | Small point/state lookups only |
| `local_operation` done, normal tools | Result body until acceptance; then compact fingerprint receipt to reject late polls | Strip result body after Gateway `accepted=true`; discard delivered receipts on next Delivery startup | No |
| `local_operation` for `code_apply_edits` | Recovery anchor for `change_resume` | Until completed edit journal is GC'd; then compact receipt until next startup | No |
| `receipt_outbox` | Durable result delivery intent | Until Gateway explicitly accepts result | Delivery loop only |
| `terminal` | Current terminal state and ID needed by `terminal_read` | Durable while active; finished rows eligible for workspace GC | Heartbeat only; poll lanes do not rescan it |
| terminal log file | User-requestable terminal output | Until workspace GC | Direct read by terminal ID |
| `transfer_local` | Resumable transfer journal/checkpoint metadata | Through transfer terminal state and explicit GC | Transfer operations only |
| transfer data file | Resume checkpoint/body | Through transfer terminal state and explicit GC | Transfer operations only |
| edit journal | Atomic edit recovery / `change_resume` | Partial journals protected; completed journals eligible for GC | No |
| `work_events` | Workspace activity tail for context/replay | Latest 4096 transitions per workspace | **Never** used by poll/heartbeat scheduling |
| `runtime/*` | Current build/readiness/delivery summaries | Replace-in-place | Small point lookups |

### Terminal replication

Heartbeat is the single owner of terminal-state replication. Independent Read/Write/Transfer/Terminal/Control poll lanes do not each perform the same SQLite terminal snapshot. Terminal recency comes from the terminal row's `updated_at`; audit events are not joined into terminal queries.

### Result ownership transfer

`Delivery::complete` atomically stores the local result and the receipt intent. Until the Gateway returns a validated `accepted=true`, both remain. After acceptance, the outbox receipt is removed and the ordinary completed `local_operation` **result body is stripped** in the same transaction, **only if the acknowledgement owns the current receipt lease**. Its small fingerprint and `gateway_accepted=true` marker remain for this Agent runtime: a poll response dispatched before acceptance may arrive afterwards and must not execute the operation twice or overwrite the Gateway's result. On the next process startup, those delivered receipts are removed because old process-local poll responses can no longer arrive. A late acknowledgement from an older sender cannot delete a newer attempt's recovery anchor. `code_apply_edits` is intentionally retained because `change_resume` is a long-lived recovery contract.

### Workspace cleanup boundary

GC uses lifecycle timestamps from the retained terminal/transfer state rather than requiring a redundant completed result. Missing or non-positive lifecycle timestamps, unfinished terminal output, active/unknown local operations, partial edits, and undelivered receipts remain protected. The apply path selects and removes candidates within one bounded immediate transaction, so a receipt or recovery transition cannot race state-file deletion. GC also retains compact deduplication receipts until restart, including after explicitly collecting a completed edit journal. Preview remains read-only.

## Gateway state

| State | Purpose | Lifetime | Query rule |
| --- | --- | --- | --- |
| `jobs` queued/dispatched | Durable dispatch and at-most-once/idempotency authority | Until result transition | Dispatch hot path |
| `jobs` done | Idempotency / `operation_get` authority | Durable for now | Must not participate in dispatch scans |
| `operation_timing` | Queue/dispatch/result diagnostics | Compatibility-only in 0.10.4 | Never gates dispatch |
| `online` | Current device session | Replace-in-place / freshness based | Point lookup |
| `terminal_observation` | Recently replicated terminal state | TTL bounded | Point lookup by operation |
| `operation_progress` | Progress cache | TTL bounded | Point lookup/list only when requested |
| transfer control/source/blob state | Durable only while transfer protocol needs it | Protocol-specific TTL/terminal state | Transfer-only |
| auth/revocation state | Security authority | Durable per auth policy | Auth paths only |

Gateway dispatch separates `queued` and stale `dispatched` candidates so the existing `jobs(device,state,updated)` index can select active work without scanning accumulated `done` rows. Completed history is intentionally outside the dispatch candidate set.

### Completed exports versus temporary download availability

A completed export receipt in `jobs` outlives its temporary blob and bearer link. `operation_get` must keep returning the original completion state, checksum, size, and operation identity after the blob expires or is collected. It reports `download_available=false` and `artifact_expired_or_removed` separately, without minting a link, rewriting the durable result, or starting another export. Retrieving bytes again requires a new explicitly requested export with the expected source version; the source may have changed since the original snapshot. Principal, device-scope, database, and receipt-integrity checks remain enforced. Expiration is not an unknown execution outcome.

`operation_timing` is deliberately kept for **one compatibility release**. 0.10.4 changes job creation to use explicit column names so a later additive `jobs` schema remains rollback-safe. After the fleet is on 0.10.4, 0.10.5 can merge the three timing fields into `jobs` and remove the separate table without breaking rollback to the immediately previous binary.

## Work-event retention

`work_events` has one table and one `(workspace, seq)` index. There is no separate `work_event_floor` table. Each transition inserts one event and trims only the overflow older than the newest 4096 events for that workspace. The history floor is derived from the retained rows when read. Sequence numbers are global: below retention capacity, another workspace's events must not invalidate an empty workspace's cursor. At capacity, cursors older than the retained tail conservatively require a fresh snapshot.

## What is deliberately not added

- No larger SQLite pool as a substitute for slow queries.
- No per-feature forest of JSON expression indexes.
- No second terminal-history index derived from `work_events`.
- No duplicate Agent result body after the Gateway has accepted it; only a compact current-runtime deduplication receipt remains.
- No audit/event table in readiness, heartbeat, poll, or drain decisions.

## Next compaction boundary

Gateway `done` jobs are still the long-term idempotency and `operation_get` authority. They may grow on a busy Gateway, but 0.10.4 makes dispatch cost independent of that growth. A later compaction should only split `done` jobs into a slimmer receipt ledger if it preserves the exact idempotency, operation lookup, and recovery contracts. Do not introduce retention deletion until those contracts are explicit and tested.

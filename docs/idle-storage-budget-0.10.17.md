# 0.10.17: derived queue health and bounded expiry

Status: candidate under verification. No deployment or latency improvement is implied by this document.

## Removed work

The Agent no longer persists `runtime/receipt_delivery` on a two-second reporting loop (previously unchanged values were coalesced for ten seconds). Each outbound poll derives its queue counts from the original durable `receipt_outbox`, scoped by device and Gateway origin. A failed read produces unavailable telemetry, never a fabricated empty queue or a refreshed copy of an old process's status. No additional database, memory cache or write-behind queue is introduced. Existing obsolete telemetry rows are left untouched but are not read by the new Agent.

The delivery worker subscribes before reading the durable queue. New/finished receipt notifications wake it immediately; due retry times and a maximum five-second reconciliation interval replace four unconditional idle checks per second. Failed queue reads use a one-second recovery delay. HTTP deadlines, two-worker concurrency, lease binding, exact saved payload retries and original-command deduplication are unchanged. Timers remain recovery hints, not delivery authority.

## Bounded expiry

`kv_expiring(expires,kind,key)` is a partial index for finite-lived records only. Permanent execution evidence is excluded. `prune_batch` first checks for eligible records without acquiring the writer, then deletes at most 256 rows in one atomic statement. The deletion repeats expiry predicates after acquiring the writer; concurrent renewal is not based on a stale key list. Empty maintenance does not enter a write statement.

Expiration and physical reclamation remain distinct: every get/take checks expiry immediately even when reclamation is deferred. No new TTL is assigned to original operations, unknown results, paused transfers or audit evidence. No production history purge or VACUUM is part of this release. WAL and FULL durability are retained.

## Required proof

- Repeated queue-health samples cause zero kv writes while retaining delivery intent; old saved telemetry is ignored.
- Restart reconstructs queue counts from the outbox; different devices/destinations remain isolated; storage failure is explicit unavailability.
- A new receipt interrupts a long idle wait, rather than waiting for the timer. Retry/deadline calculations remain bounded.
- Empty expiry passes with a held SQLite writer. EXPLAIN QUERY PLAN uses the partial index without scanning permanent history or creating a sort.
- Multiple expiry batches respect 256-row limits; permanent evidence/live tokens survive; expired data is immediately invalid; injected deletion failure rolls back; concurrent renewal survives.
- Frozen-source local build, target-native verification and existing fleet read/edit/execute/transfer/recovery regression remain required.

SQL row counts, file/WAL bytes, commit timing and end-to-end tool latency are separate measurements. Avoid deriving fsync savings or percentile latency from row counts or a few samples.

## Deferred

Transaction grouping for durable command acceptance/results, large-body consolidation and cold-history retention remain separate changes. The current release removes redundant work first; it does not weaken commit acknowledgment or claim to fix the measured storage-device commit latency.

## Validation clarification

The first pooled EXPLAIN check returned an old schema plan. The diagnostic now uses one acquired connection, executes a read to refresh its schema, then prepares EXPLAIN without reusing an old statement. The exact SQLite 3.51.3 test showed the original expiry predicate uses the covering kv_expiring index with no table scan or temporary sort. No INDEXED BY hint, relaxed assertion or alternate production predicate was retained. Original failed reports remain available; this is a test-observation correction, not evidence of a released query regression.

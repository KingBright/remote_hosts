# 0.10.16: observation cost budget

Status: implementation under verification. This document is not proof of deployment.

## Scope

The default Gateway policy stops creating a start row and a final row update for every successful pure observation. The read still goes through current principal, original-operation and device authorization. The hot business database retains the original operation, output and recovery facts exactly once.

The lightweight paths are devices_list, fleet_status, task_context, and direct/batch operation_get whose jobs do not issue a download capability. Request-id resolution remains fully audited because a request may acquire a job binding concurrently. A file_download observation can issue a bearer link, so it retains the full journal. Other tools keep the existing durable pre-dispatch request binding and atomic result transaction.

No extra database, cache, message broker, write-behind queue, truncation of history, change to FULL durability or production cleanup is introduced.

## Evidence contract

A lightweight response explicitly returns receipt protocol 2:

- durable=false: the query response/request was not individually journaled in the Gateway database.
- evidence_durable=true: the underlying authorized facts came from durable storage. This is not a declaration that their output is complete, fresh or successful.
- request_record_persisted=false and durability_scope=observed_facts_only_not_this_query.
- The original operation_id, process exit code, stale flag, evidence/output completeness and retry boundary remain independent.

Clients must check the original evidence and output, not reject a successfully read durable result just because its query trace is not persisted. The bundled release client uses a strict helper for this split; unknown or contradictory protocol-2 shapes do not pass. Unchanged output must not erase failures or uncertainty.

A lookup for an unretained query request_id can return not_observed_or_expired. It explicitly states its lookup scope and the possibility of unretained observation; it must never be read as proof a command did not execute. Observe the original operation instead. A query cannot shadow an existing durable request ID.

A new query failure, including authorization or lookup failure, writes one compact error record instead of start/end records. If even that write fails, the error remains visible and the response reports that its diagnostic was not persisted. Observing an already recorded command failure does not copy the failure again.

## Audit policy

The operator can retain full per-call audit with REMOTE_HOSTS_OBSERVATION_AUDIT=full on the Gateway. Default minimal keeps the scoped policy above. Invalid configuration fails startup; it does not silently fall back. Mutation/request recovery guarantees are identical in both modes. The mode is not inferred from MCP readOnly annotations.

## Source preparation order

Generate adapter-contract.json with the candidate Gateway's existing adapter-contract command before freezing the final release snapshot. The generated catalog includes the embedded Skill revision; editing only the version field leaves a stale contract. Keep the compiled-catalog comparison enabled. Source archives must be checked at the directory that actually contains source-snapshot.json, not an assumed archive root.

The client accepts only known receipt protocol versions and explicit consistent durability fields. A protocol number greater than 2, a malformed version, or a persisted-request flag contradicting durable is not accepted as reliable evidence. The optional-timing failure regression runs under both minimal and full audit policies without changing its exit-code, output-completeness or authorization assertions.

## Acceptance

Measure at least 100 calls per lightweight tool against an isolated real Gateway. Count all kv/jobs INSERT/UPDATE/DELETE operations, not just request row count; require zero business writes, zero new jobs and no duplicate original result. Count errors and full-audit mode separately. Row writes do not equal physical flushes.

Regression must cover a held SQLite writer, unknown IDs, caller/device scope revocation, request-ID collisions, unchanged failed/stale/incomplete output, download capability exception, request-handle recovery, error-audit storage failure and explicit full audit. Use the exact packaged binary for native HTTP cost tests after source verification. Keep old/new reports and their binary hashes. Do not claim absolute latency from trigger-instrumented cost tests.

Historical cleanup, expiry indexes, idle telemetry and artifact consolidation remain separate follow-up work. Existing persistent records are not deleted by this change.

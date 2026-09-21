# Result persistence: one commit, original-operation recovery

## Problem and reproduced evidence

On the owner's HO5, the released 0.10.14 Linux executable was run against an isolated loopback Gateway with synthetic jobs only. No production table or device credential was modified.

- An identical already-committed result, while an independent SQLite writer was held, waited 10,025 ms and returned HTTP 500. Duplicate acknowledgement was unnecessarily competing for a writer.
- A trigger rejecting the operation timing update still produced HTTP 200, a published `done` result, released semantic guard, and missing timing. Re-delivering the same result did not repair that partial state.

This reproduction establishes the failure behavior of that path. It does not prove that every production slow request or collection error has the same cause.

## Change

New job prerequisites (request binding, semantic mapping, timing and file authorization) commit with the job. Result publication, timing, semantic reservation release and source cleanup also commit together. Every fallible write participates in rollback. A transport failure permits re-delivering the same result packet, not executing a new command.

Duplicate acknowledgement is a read-only path, validated by the original job/device and transfer generation. A first receipt rechecks the same conditions under `BEGIN IMMEDIATE`. Concurrent identical packets produce one publication and one duplicate acknowledgement. Stale transfer generations cannot finish a resumed transfer; unknown execution preserves its semantic reservation for explicit recovery.

Optional timing/queue telemetry is separate from durable execution evidence. Failure to read it is explicitly returned as `operation_lifecycle_unavailable`, while an independently authorized and durably confirmed result remains available. Original owner and operation scopes remain mandatory.

No new scheduler, database, extra state replica, weakened SQLite durability setting or external build service is introduced.

## Acceptance

Run the unit and integration failure-injection tests, then the same `native-result-atomicity.py` probe with the actual release executable. The candidate must acknowledge duplicates without acquiring the writer; reject injected partial publication; recover by accepting the same packet once; and preserve exactly two synthetic jobs with zero executed commands. Retain both old and new reports.

After release, verify installed binary identity, platform signing where required, configuration integrity, maintenance release, original request recovery, exact file/terminal output and latency using the same controller. Cached recovery results are excluded from performance claims. A passing functional test is not a guarantee of absolute latency or future availability.

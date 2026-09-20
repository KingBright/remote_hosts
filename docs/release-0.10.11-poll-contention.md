# 0.10.11: reduce empty-poll contention, preserve delivery evidence

## Observed problem

The deployed 0.10.10 Windows Agent already runs without a console and retains classified polling failures and recovery events. Investigation on the owner's NAS found slow queue UPDATE statements with zero affected rows and connection-acquisition warnings. New Windows logs include transient HTTP 500 and timeout failures followed by recovery. The old 14:54 warning lacked its original error classification, so this patch must not be described as a retrospective proof of that historical cause.

## Changes

- Empty queue observation uses a read-only candidate query rather than unconditionally acquiring SQLite's writer lock. Candidate selection is shared with the atomic UPDATE. Device session, lane, active-operation exclusion, maintenance and busy-resource filters are rechecked by the write.
- Actual job claiming and dispatch-timing persistence now commit in the same transaction. A failed timing write cannot leave a separately committed dispatched job without a delivered response.
- Job-change notifications are registered before the candidate read. Durable reads and the existing bounded recovery timer remain authoritative.
- Rebinding a repeated request to an existing operation acquires an IMMEDIATE transaction before the request read, avoiding a deferred read-to-write promotion race under concurrent heartbeat writes.
- Device poll/heartbeat storage failures produce bounded typed diagnostics. Busy/locked/pool-wait failures return HTTP 503 with Retry-After; non-transient storage errors retain HTTP 500. SQL text and credentials are never copied into the diagnostic. Retrying a poll never replays an accepted command.

## Delivery and validation

Build only on owned development machines with the pinned Rust toolchain. Generate macOS arm64, Linux x86_64 musl and Windows x86_64 MSVC artifacts using Cargo and the existing local linkers/SDKs. No GitHub Actions, cloud check-run gate or GitHub Release download is part of delivery.

Regression tests must prove empty and other-lane-only queues do not wait for an existing SQLite writer, concurrent polls deliver a job once, failed timing writes roll back the claim, maintenance/session/resource filters remain enforced, and diagnostics stay redacted. The existing Windows hidden-startup, pipe/exit-code, poll-recovery, OAuth, file-transfer and request-id tests remain required.

Verify runtime hashes and configuration preservation on each deployed node. Run native functional acceptance and collect post-deployment poll/SQL observations. Short observation windows cannot establish that all future transient failures are eliminated. Do not weaken FULL synchronous persistence, delete evidence, move data to temporary storage or retry ambiguous commands to make a benchmark appear better.

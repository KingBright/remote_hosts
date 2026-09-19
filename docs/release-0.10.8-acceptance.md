# 0.10.8: precise results and lower observation overhead

## Scope

Keep the 0.10.7 execution, OAuth/Gemini compatibility, device identities, durable receipts, and FULL SQLite durability. Do not replace the scheduler or add a second task database.

- Share terminal projection between operation results, task recovery and human status. Paused transfers must not appear completed.
- Return exactly the requested sanitized UTF-8 output range. Retain missing-range and stale evidence. Refresh compression metadata after replacing output; compact identical preview text by an explicit output_ref.
- Batch operation/status reads instead of per-operation database queries. Suppress unchanged final snapshots; coalesce identical live snapshots only within a five-second freshness renewal bound.
- Wait for a short command's own finalization notification instead of repeatedly reading SQLite every 20 ms. Persisted state remains authoritative.
- Conditional authenticated status refresh uses weak ETags, preserves unchanged cards and marks observation failures. Hidden pages stop polling. No npm dependencies or Agent-side Python/Node runtime is introduced.
- Update Gateway initialization instructions and embedded Skill together. Catalog generation is checked against the compiled Gateway. External final-host exposure remains unknown until the host reports it; do not claim this release can modify a third-party host's current tool schema.

## Required regression and release evidence

Run formatting, Clippy, Rust and existing Python suites on one frozen source identity. Tests include paused transfer recovery, byte-range consistency and invalid UTF-8 boundaries, compact output references, authenticated conditional HTTP responses, privilege checks, clock rollback, final snapshot deduplication and browser-client failure behavior.

Build one immutable macOS/Linux/Windows bundle. Compare each installed binary against its platform artifact, preserving host-local macOS signing evidence. Upgrade the Gateway before Agents. Release each node's maintenance lease after stable readiness.

Run the same native-shell success/failure/delayed-output, precise edit, idempotency and file roundtrip cases on all four authorized nodes. Separately verify the new cursor/ETag contracts through authenticated HTTP and the currently exposed host tools. Record actual call counts and latency, without calling UTF-8 bytes billing tokens or lab throughput production performance.

## Evidence boundary

This document is the acceptance plan, not a passing receipt. Build, publication and per-node acceptance reports must identify their exact commit, snapshot and artifact hashes. Fault injection runs in isolated test directories, not in the production Gateway database. Do not suppress errors or count skipped/ignored tests as passing executions.

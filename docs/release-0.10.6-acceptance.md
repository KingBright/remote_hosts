# 0.10.6 observable-response release

This patch follows the immutable 0.10.4 release at 6f258c104e5a11149e7cdefb3475a7f282c4483b. Never overwrite its tag or artifacts.

## Defect reproduced on the live connector

A completed process was reported as `exited` by terminal_observation and receipt while the response root and nested terminal retained `running` from the original submission. This caused extra reads and required the caller to reconcile contradictory states.

The new response projection uses the confirmed process observation without rewriting the durable submission. Tail previews are not promoted to full output; missing legacy previews remain explicit. Failure exit codes, incomplete output, stale observations and lack of business acceptance remain visible.

## Release acceptance

Use exactly one frozen source fingerprint for verification and three-platform packaging. Keep per-device deployment receipts including candidate hash, runtime version, original configuration identity, rollback backup, maintenance release, and fresh authenticated observations.

Run real commands through the same live connector: a successful short command, exit code 7, and a two-part delayed output command. Record every call and count separately from preparation; prefer at most two calls when the command has finished. Confirm the root state, nested terminal, observation and receipt agree whenever authoritative process evidence exists. Never turn transport completion or exit code zero into business acceptance.

Replay an identical idempotency key only as an explicit idempotence test: it must return the original operation. Conflicting parameters must be rejected. Preserve raw evidence off-model; report measured call counts and bytes without calling them tokenizer or billing measurements.

Check each device through the actual runtime and installed artifact, not only a version label. Verify file creation, exact read/edit, and binary upload/download hashes. Observe `/status` after login; never infer final host exposure from a generated adapter catalog. Missing host reports remain unknown and count as an unresolved integration boundary.

## Change discipline

No new scheduler or task database. No unrelated repository edits. No unconditional upgrade retries. A blocked host request is not a remote process failure. If live acceptance reveals another reproducible defect, preserve its evidence, fix and test it, publish a new immutable patch release, then repeat the same acceptance cases.

Version 0.10.5 is reserved by an older local Gemini candidate. That candidate was not overwritten; this release uses 0.10.6.

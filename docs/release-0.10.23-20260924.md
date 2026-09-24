# Remote Hosts Code 0.10.23

## Scope

This release includes the previously committed Agent liveness isolation and automatic completed-history retention (16a86a8, b126847), plus two bounded follow-ups found in the release review:

1. Gateway file blobs and expired receive sessions now participate in autonomous maintenance. Previously their cleanup depended on another incoming transfer. The collector only touches metadata-owned UUID cache paths after expiry, uses existing per-operation and allocation locks without waiting, preserves execution/receipt identities, retains failed unlinks for retry, and uses a restart-persistent keyset cursor with at most 256 scanned rows per tick. Unexpired caches, in-flight operations, unknown paths and symlinks are protected.
2. Malformed Gateway history JSON and non-object bodies are counted and preserved instead of aborting all later body reclamation or panicking on an invalid shape.

No new task database, filesystem-wide scanner, cron job, dependency or tool API is introduced. The ordinary 7/30-day history windows, one-day pressure floor and protected recovery evidence are unchanged. Transfer artifacts retain their existing cache/session expiry semantics; expiring a cache does not mean its original command should be replayed.

The added regression suite covers autonomous maintenance, live operation/allocation contention, malformed records, file deletion failures, missing payloads, retry after database failure, restart-persistent bounded pagination, zero-write idle scans, path/identity checks and symlink protection.

## Release gates

Final frozen-source verification PASSED on Mac Studio with the pinned Rust 1.94.1 toolchain. Formatting, strict Clippy, Rust tests, Python tests and the full workspace check all succeeded. Exactly 824 executed tests passed with zero failures: Rust 535 and Python 289. Two Rust benchmarks were ignored and three Python tests skipped; neither is counted as a pass. The ten added regression cases are included in the totals, not added twice. The earlier 814-test receipt belongs to b126847's snapshot and is not reused.

Verified source snapshot (267 declared inputs): `2f3e2bbc2bbda06ca57d2965295fc36d0775a0d4e81b13348c9ec708e3f38d3b`.

Original verification receipt SHA-256: `d992bc76e0cc796d431e4acc6f67208de09a41527ad2f4d38e037968aba4fd04`. Receipt: Mac Studio `target/retention-rollout-01023/native-r2-logs/verification.json`. The main-worktree input bytes and executable modes were matched to this frozen snapshot before staging the nine scoped release files.

Three-platform release builds and packaging are proceeding in the same exclusive persistent build slot. A new immutable 0.10.23 package must pass hash validation before Gateway-first deployment. Existing device identities, connection origins, signing, maintenance leases and rollback checks remain in force. Source verification is not a production acceptance receipt.

## Deployment

Not deployed at document creation. Actual runtime versions and automatic cleanup observations will be recorded after cutover; no production data was edited to simulate acceptance.

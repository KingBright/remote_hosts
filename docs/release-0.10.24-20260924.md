# Remote Hosts Code 0.10.24: bounded Gateway maintenance

## Observed production issue

After 0.10.23, Gateway history reclamation did retire old bodies (3,396 observed), but the complete sweep frequently encountered SQLite writer contention and returned before publishing its status. The NAS had 11 TB available, so the observed errors were not explained by a full disk. The old loop also restarted its scan from the beginning after each failure, competing unnecessarily with ordinary requests.

## Change

The same authorization, final-exit validation, result digest, legacy clock, semantic-guard and transaction predicates now run through a restartable stepper. Each production tick scans at most 256 body rows, admits at most eight body/legacy-clock writes, and checks a cooperative one-second wall budget. Once a commit is admitted it is not forcibly cancelled. Large bodies and command text are never copied into checkpoints; only bounded counters, a keyset position and at most eight oldest-candidate identities are retained.

Partial progress is published together with a restart checkpoint in the existing single latest runtime record, including sanitized failure classification. Failed status publication keeps the current in-memory position. A crash falls back to the last durable position; original idempotency digests prevent replay or duplicate retirement. Capacity candidates are reloaded and their current final state/age rechecked before retirement. Incomplete inventory counts are explicitly labelled partial estimates.

Body scanning, transfer-cache expiry and finite-cache pruning have separate error reporting so one stage cannot hide all other progress. The Gateway reuses its dedicated maintenance connection rather than creating a new pool on every tick. The 7/30-day retention windows, one-day pressure floor, original job identities, protected in-flight work and agent origin bindings do not change.

## Verification and rollout

Final source verification PASSED on Mac Studio using Rust 1.94.1 and the exclusive persistent release slot. Formatting, strict Clippy (-D warnings), Rust tests, Python tests and full workspace checks succeeded. Exactly 830 tests were executed and passed, with zero failures: Rust 541 and Python 289. Two Rust benchmarks were ignored and three Python tests skipped; neither is counted as passed. The six newly added maintenance regressions are included in these totals.

Verified snapshot (268 source inputs): `14ece8357505bb4158f3f30fb9f54509dd27b860a1861f5f25b314a05e838797`. Original verification receipt SHA-256: `7ef4cae1dfc989f44856f8db4a283cd37a34ca97b38465132a89d70062cb4650`. Receipt on Mac Studio: `target/retention-rollout-01024/native-r3-logs/verification.json`. MacBook's scoped source inputs and executable modes matched the frozen snapshot before staging.

Three-platform release builds follow verification from the same snapshot. Never reuse 0.10.23's test receipt or overwrite published binaries. Final delivery requires the new immutable package, Gateway-first deployment, existing signing and maintenance leases, per-device runtime identity checks, automatic-maintenance observations, and actual read/edit/execute/transfer acceptance. Build success alone is not deployment acceptance.

The live acceptance operator also checks Windows using native PowerShell instead of assuming that the WindowsApps Python launcher is an installed runtime. Offline devices must remain explicitly pending; successful acceptance of reachable devices must never be presented as all-device convergence.

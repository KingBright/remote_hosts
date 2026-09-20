# Terminal completion delivery

The Agent treats a terminal's durable state as the authority. A small in-process watch revision only signals that this state should be read again. It is not a second task database, a completion receipt, or permission to repeat a command.

After a final terminal state is successfully persisted, the terminal worker wakes the existing heartbeat loop. Accepted cancellation, failed process startup, and orphan reconciliation also wake it after their corresponding durable state changes. Concurrent changes coalesce for 50 milliseconds. One heartbeat is in flight at a time; an event arriving during an HTTP request remains unseen until the next iteration. Periodic reconciliation is retained, including retry after a failed upload. Ordinary streaming output still uses the bounded periodic path.

The change does not weaken FULL persistence, skip authorization, replace original operation IDs, or turn an unsuccessful state write into a successful result. No automatic command replay or transport failover is introduced.

## Regression scope

`cargo test -p remote-hosts-code --lib terminal_completion_tests` executes real native shell children with a loopback-only HTTP receiver and synthetic configuration. The event tests use a sixty-second fallback interval but require completion publication within a bounded five-second wait. This distinguishes event delivery from periodic polling without requiring a submillisecond scheduler benchmark. A blocked first request verifies that a completion is retained without a concurrent heartbeat; a rejected completion verifies retry of the same terminal state.

`scripts/check-terminal-delivery.py` uses the installed Rust adapter, explicit authorized workspace IDs and a unique run ID. It records fixed success, exit-code-seven and delayed-output cases. Exact output, durable evidence, operation identity and cleanup are verified. Slow cases remain in the report. The reports separate functional success, call-count targets and a five-second short-command threshold. The latter is a sample criterion, not proof of a fleet-wide percentile. The helper does not deploy software or restart services.

## Measurement boundaries

Compare one frozen source/build identity at a time. Client elapsed time uses its monotonic clock. Gateway timings and Agent timings retain their own provenance and must not be subtracted across machines. A terminal-execution RPC can finish before a long-running process exits; its execution timing is not the total process runtime. Retain failed baselines and original receipt references. Source tests, target-native tests and live deployment acceptance are different evidence.

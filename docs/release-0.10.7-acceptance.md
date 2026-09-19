# 0.10.7 durable heartbeat batching

Production observations on 2026-09-19 showed SQLite connection acquisition taking 2-5 seconds and intermittent OAuth/receipt HTTP 500 responses. The gateway uses WAL with synchronous FULL; that durability setting must remain unchanged.

Heartbeat and poll metadata previously committed online status, active-operation lease renewals, progress and terminal snapshots independently. They now share one BEGIN IMMEDIATE transaction and one durable commit. The transaction ends before the long-poll dispatch wait. Session and original-job authorization remain enforced within the transaction.

A fault-injection test rejects terminal observation persistence and verifies both heartbeat and poll roll back the session timestamp and operation lease together. Removing the injected fault permits the same authenticated observation to complete. No operation execution is replayed by observation recovery.

Release acceptance: complete source-bound tests, deploy immutable artifacts, then measure short/failing/delayed commands, state consistency, fresh OAuth, and database warnings under the same four-node load. Fewer source-level commits are not a claimed wall-clock speedup until measured. Existing 0.10.4 and 0.10.6 evidence is not this revision's acceptance.

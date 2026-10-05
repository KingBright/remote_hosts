# Transfer recovery acceptance

Two separate scopes are available. Neither entry deploys a Gateway or Agent.

## Fixed isolated protocol and process evidence

Run on an authorized POSIX development machine with loopback networking available:

```sh
python3 -B scripts/check-transfer-recovery.py \
  --report-dir /tmp/transfer-acceptance-UNIQUE-RUN \
  --build-slot target/transfer-acceptance-slot
```

The report directory must be new. The existing source snapshot and build-slot
helpers freeze inputs and serialize compilation. The script runs the actual
Client/coordinator boundary, temporary Rust Gateway/Agent state machines, a
loopback HTTP403 source, same-source authorization refresh, and owned child
process kill/restart. It checks that each exact Rust case executes once and
retains logs, hashes, source identity, passed/failed/skipped counts and cleanup.

These are fixture receipts. They do not establish installed fleet acceptance.
No production configuration, credentials, enrollment or service manager is used.
On failure inspect the same report and its owned child before a new isolated run.

## Existing original transfer only

`fleet-upgrade.py --acceptance-only ORIGINAL_TRANSFER_JSON --report-dir DIR
--access-token-stdin` bypasses package verification, login, producer submission,
SSH staging, Gateway upgrades and Agent launch. Supply an already authorized
bearer through private stdin; this mode neither creates nor revokes its grant.

The explicit JSON specification has:

```json
{
  "schema_version": 1,
  "origin": "https://fixture.invalid",
  "operation_id": "ORIGINAL-OPERATION-ID",
  "kind": "export",
  "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "resume_key": "ORIGINAL-SEMANTIC-RESUME-KEY",
  "deadline_seconds": 300,
  "max_resumes": 2
}
```

For an import, an optional `file` object contains the original `file_id`,
explicit refreshed `download_url` and `file_name`. The existing Gateway enforces
unchanged source identity. Do not put capability URLs or tokens in repository
documents. Without explicit source refresh, `awaiting_source` stops.

Only `operation_get` and `transfer_resume` are invoked, using the original ID.
Unknown outcomes, mismatched IDs/SHA, deterministic failures, missing export
identity and lost resume acknowledgements stop without a new upload/download.
Reports omit bearer values, URLs and arbitrary exception text. A completed
transfer proves that transfer only; it does not imply fleet convergence.

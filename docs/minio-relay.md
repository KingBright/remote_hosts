# Private MinIO relay for interactive bastions

## Scope

Normal SSH routes retain pooled SFTP (and the existing supported exec fallback). A MinIO relay
is eligible only for an explicitly configured canonical Host and interactive bastion AccessPath.
The relay moves file bytes over HTTP(S); the already selected managed PTY carries only bounded
control scripts, identity checks, and completion receipts. It does not add an SSH bypass.

Existing `remote_hosts_upload_file` and `remote_hosts_download_file` operations retain their
operation IDs, ownership, write leases, timeout, size bound, mode, SHA-256, and overwrite policy.
No public bucket policy is needed. The remote target receives short-lived, object-specific signed
URLs; long-lived MinIO credentials remain in the Remote Hosts vault.

## Configuration

The connector reads `minio-relays.json` beside its configured vault master password file. This
file contains only non-secret policy and encrypted credential IDs. Each profile pins the S3 API
endpoint, private relay bucket, canonical Host, AccessPath, and expected target hostname.
The Console endpoint is not an S3 API endpoint.

```json
{
  "profiles": [{
    "name": "customer-management",
    "host_id": "<registered-host-uuid>",
    "access_path_id": "<registered-interactive-path-uuid>",
    "expected_hostname": "<verified-target-hostname>",
    "endpoint": "https://<approved-s3-api>",
    "bucket": "remote-hosts-transfer",
    "credential_id": "<existing-encrypted-service-credential-uuid>",
    "threshold_bytes": 16777216,
    "allow_http": false
  }]
}
```

Only explicitly approved internal HTTP endpoints may set `allow_http=true`. No credentials,
signed URLs, business object keys, or transient operation IDs belong in this document or skill.

## Safety and recovery requirements

- Require one unambiguous active PTY and verify its actual target identity before moving bytes.
- Fail closed on wrong identity, missing target tooling, invalid credentials, or SHA mismatch.
- Verify the exact file and use a same-directory temporary destination before final placement.
- Relay objects use operation-specific private prefixes; clean only the exact owned object.
- A successful byte count is not a completion receipt. Placement and cleanup are separate stages.
- Never retry an arbitrary shell mutation when a transfer receipt is missing.
- Release clients still own the official release object and admission contract. This temporary
  relay is for Host-to-connector file transport, not a substitute for YWB object registration.

## Provision and verify

Run `remote-hosts minio-relay-check --database-url <existing-db-url> --config <policy-json>
--vault-master-password-file <existing-vault-file> --provision` once for the approved profile.
The command reuses the encrypted credential internally, creates only the dedicated relay bucket,
and installs one-day orphan-object retention with the required Content-MD5.
Some MinIO versions accept but do not retain the incomplete-multipart lifecycle field. The
Connector therefore also checks for stale multipart uploads before new native uploads and
during the check command: at most 20 per call, exact profile prefix, and older than one day.
Unknown timestamps and current/foreign uploads are never aborted. This is opportunistic cleanup,
not a scheduled guarantee when the Connector is idle.
An interrupted first provisioning can resume; existing unrelated lifecycle rules are not replaced.
Rerun without `--provision` for authenticated private-object CRUD, byte integrity, and HEAD-404
cleanup verification. The command prints only non-secret receipt fields.

The transfer terminal result includes `transport=minio_relay` and a warnings list. Cleanup and
PTY-restoration waits are bounded after verified placement; near the deadline they are deferred.
`minio_cleanup_pending` and `pty_restore_pending` are follow-up work, not a reason to replay a
successful file mutation. Failures before a placement receipt retain the operation-specific
object for diagnosis and expiration. An operation timeout does not prove remote curl stopped.

## Validation evidence (2026-09-05)

- Actual-size selection and exact route matching; direct/FRP routes excluded.
- Connector-level refusal of a selected terminal with the wrong target hostname.
- RFC 4231 HMAC vector, URI encoding, method/host/object/expiry signing boundaries.
- Private S3 object PUT/GET, anonymous GET denial, exact-byte verification, DELETE + HEAD 404.
- Live 33 MiB + 17 byte roundtrip: three-part native upload, streaming native GET, exact target
  curl GET script with a verified partial destination and quoted filename, atomic placement,
  exact target curl PUT script, and SHA mismatch rejection. Test objects were deleted.
- More than 20 transfer progress records now return the latest progress in Agent Work Context.
- Connector/Core/MCP regression tests and strict Clippy checks pass.

The live curl scripts were exercised locally against the real MinIO service. The production
interactive bastion timed out during activation, so production Host-to-MinIO reachability and
end-to-end transfer through that actual PTY remain unverified. Do not present local script
validation as acceptance of the production route.

The explicit live regression is ignored in normal offline test runs. To rerun it, set
`REMOTE_HOSTS_RELAY_TEST_CONFIG`, `REMOTE_HOSTS_RELAY_TEST_DATABASE`, and
`REMOTE_HOSTS_RELAY_TEST_MASTER_FILE` to non-secret configuration paths / database URL, then run
`cargo test -p remote-hosts-connector live_private_multipart_and_curl_roundtrip -- --ignored`.
It reads the existing Vault inside the test, uses an isolated local temporary directory and
operation-specific relay key, and never prints credentials or signed URLs.

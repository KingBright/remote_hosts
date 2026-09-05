# Private MinIO relay

Use the normal `remote_hosts_upload_file` and `remote_hosts_download_file` tools. The Connector
selects MinIO only for an exact configured Host + enabled interactive bastion AccessPath, and
only when the actual source size reaches the profile threshold (normally 16 MiB).
Direct LAN, FRP, VPN, and ordinary SSH keep pooled SFTP and the supported exec fallback.

Prepare your own Workspace, open one PTY, finish the bastion asset selection, and wait until
that PTY is active, input-ready, and idle. The Connector checks the expected target hostname
before transferring. Never reuse another task's PTY, infer an internal-IP SSH route, or turn an
FRP endpoint into a fake bastion. A failed relay does not silently stream a huge file as Base64.

Long-lived service credentials are references to the encrypted Vault. Non-secret endpoint,
bucket, target, threshold, and credential ID live in `minio-relays.json` beside the Connector's
vault master password file. Recover that profile and Host knowledge before searching old chats
or legacy `mc` aliases. Use the S3 API endpoint; the MinIO Console port is not interchangeable.

Uploads use bounded multipart HTTP, then target curl GET with verified partial-file reuse and
SHA-256-gated atomic placement. Downloads use target curl PUT, native streamed GET, and local
SHA-256-gated placement. Target requirements are POSIX sh, curl, and sha256sum or shasum; Python
is not required. Targets receive only short-lived single-object URLs. Do not copy those URLs
into agent messages, knowledge, command arguments, or public bucket policies.

Set the normal size ceiling and deadline for the complete operation (up to 4 GiB / 7200 seconds).
Read current stage/bytes through Agent Work Context. A terminal receipt reports
`transport=minio_relay`. `minio_cleanup_pending` means placement succeeded but the exact private
object still needs cleanup; orphan retention is a safety net. `pty_restore_pending` means
inspect the existing terminal before sending more input. Neither warning permits re-upload.
Missing placement receipts require diagnosis with the same operation and paths; do not replay
an arbitrary shell mutation or assume a remote curl stopped when the client timed out.

Provision/check the dedicated relay bucket using `remote-hosts minio-relay-check --config ...
--database-url ... --vault-master-password-file ... [--provision]`. Arguments are paths and
credential references only. Provisioning resumes missing retention setup but does not overwrite
an existing unrelated lifecycle policy. Some MinIO servers ignore multipart lifecycle fields;
the Connector separately aborts at most 20 profile-owned uploads older than one day on new native
uploads and checks. This is opportunistic cleanup, so do not promise scheduled cleanup while idle.
Keep official YWB release objects and registration
separate from temporary Host file transport.

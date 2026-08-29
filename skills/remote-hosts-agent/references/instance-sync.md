# Instance Sync

Use this reference when a user asks to share Remote Hosts data between independently installed
instances, for example a Mac Studio and a home Windows machine.

## Scope

Protocol v1 exchanges:

- `inventory`: host identity, labels, ownership, tags, description, and risk level;
- `knowledge`: redacted notes, source, tags, and links to synchronized hosts.
- `credentials`: user-authorized SSH password, private key, key passphrase, and sudo password,
  associated with one access path.

Credential records carry route metadata and peer-sealed ciphertext, never plaintext or a source
vault blob. The receiver decrypts it only in memory and re-encrypts it in its own local vault. It
first updates a matching local route, otherwise creates one under its most recently seen local
connector or its only local environment. Connector configuration, transport state, Agent Sessions,
Workspaces, PTYs, write leases, operations, command output, artifacts, raw SQLite files, and live
project files remain local.

## Flow

1. Make the peer API intentionally reachable. Prefer HTTPS outside a trusted private network.
2. Configure one peer direction with `remote_hosts_configure_instance_sync_peer`. The pairing token
   is encrypted locally and is never returned. Repeat on the opposite instance for bidirectional
   sharing.
3. Run `remote_hosts_sync_instance_peer`. Defaults include `credentials` and it sends at most 1,000 deterministic records,
   each with a SHA-256 digest.
4. Treat duplicate counts as successful idempotent replay, usually after a timeout or retry.
5. Treat conflicts as a request for review: the receiver retained a newer local record and stored
   a visible conflict. Do not retry to force an overwrite.
6. After importing a route on a differently connected instance, inspect its runtime state and make
   one normal connection attempt. Correct the route through `remote_hosts_ensure_host` only when
   the receiver's network genuinely needs a different endpoint.

## Boundaries

Do not create archives, copy `remote-hosts.sqlite`, or use upload/download to impersonate instance
sync. File transfer remains correct for deployment artifacts and explicitly requested files, but it
does not carry peer identity, record receipts, host deduplication, conflict handling, or apply
gating.

Topology and artifact collection names are intentionally reserved in v1. Do not request them until
their provenance, resumable-transfer, and explicit promotion semantics are available.

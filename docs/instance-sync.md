# Instance Sync

`Instance Sync` lets independently installed Remote Hosts instances exchange durable operational
knowledge and user-authorized SSH credentials directly. It replaces a handoff made from a SQLite
copy, Git bundles, ad-hoc archives, temporary incoming folders, and a second round of manual
reconstruction.

It is intentionally an instance-to-instance product capability, not a way to move an active
agent task between computers. A task must keep using the instance that owns its Workspace and PTY.

## The Boundary

The following data is eligible for v1 synchronization:

| Collection | Contents | Identity and conflict behavior |
| --- | --- | --- |
| `inventory` | Host identity, tags, owner, description, risk level | A host first converges by source id, then by stable host name. A newer local record is retained and a visible conflict is created. |
| `knowledge` | Redacted knowledge title, body, source, host links, tags | Keeps the source knowledge id. Host links are remapped through the peer-specific host mapping. |
| `credentials` | Each SSH access path's route metadata plus user-authorized password, private key, key passphrase, and sudo password | Implies `inventory`. The receiver matches an existing local route first; otherwise it creates a route under its most recently seen connector, or its only local environment. A newer local credential is retained as a visible conflict. |

Vault blobs and master keys always remain local. A credential sync record contains only route and
credential metadata plus an XChaCha20-Poly1305 ciphertext sealed from the explicitly configured
peer pairing token. The receiver opens that ciphertext only in memory and immediately encrypts the
same secret with its own local vault key. Plaintext passwords, private keys, passphrases, and sudo
passwords never enter MCP output, audit records, normal HTTP responses, or a SQLite dump.

Connector settings, SSH transport state, connection sessions, Agent Sessions, Workspaces, PTYs,
write leases, operations, command output, and queues remain local and never enter a sync envelope.

This boundary preserves task and transport isolation while removing repeated password entry across
Mac Studio, a home Windows machine, a company connector, and a customer-site connector. The
receiver deliberately binds an imported route to its own current connector/environment instead of
copying the sender's connector id. Verify route reachability after the first sync when the two
instances sit on different networks.

## Direct Protocol

Each installation lazily creates a durable v7 `instance_id`. An approved peer has a display name,
direct API endpoint, approved collection set, encrypted local outbound token, and an SHA-256
digest of the inbound token. The plaintext token is accepted only when configuring the peer; it is
never returned by the API, MCP tool, or peer listing.

The API exposes three endpoints:

- `GET /v1/instance-sync/identity`: read the instance identity and protocol version.
- `POST /v1/instance-sync/export`: authenticated export for pull workflows.
- `POST /v1/instance-sync/receive`: authenticated push and apply.

The token is carried in `x-remote-hosts-sync-token`. A peer must be explicitly configured on each
receiving side. The normal operator API remains loopback-only when the local vault is unlocked.
For a direct peer, start the restricted listener on a separate port, for example
`remote-hosts serve --bind 127.0.0.1:8787 --peer-sync-bind 0.0.0.0:8788`. That listener exposes
only health, identity, export, and receive routes; it cannot serve the admin UI, credential
inspection or management routes, Workspaces, PTYs, or command APIs. Use HTTPS for any routed or
public endpoint. HTTP is supported only for an explicitly trusted private network or an SSH/VPN
tunnel.

An envelope contains at most 1,000 records and no files. Every record has an immutable event id,
origin instance id, collection, source key, source update timestamp, canonical payload, and
SHA-256 payload digest. Receipts make retransmission idempotent after a timeout or interrupted
request. A receiver validates the digest, checks the peer's approved collections, and then applies
each record independently. Credential records use a strict schema: fixed route metadata and only
`aead`, `nonce_b64`, and `ciphertext_b64` inside the sealed payload. Plaintext or extra fields are
rejected. One bad record does not corrupt or invalidate already applied records.

## Agent Workflow

The compact MCP profile adds two tools:

1. `remote_hosts_configure_instance_sync_peer` stores one approved peer endpoint and pairing token
   locally. The default collection set is `inventory`, `knowledge`, and `credentials`.
2. `remote_hosts_sync_instance_peer` pushes the bounded durable envelope and returns only
   sent/applied/duplicate/conflict/rejected counts plus short actionable details.

Both sides need the same pairing token configured for the direction they accept. For bidirectional
exchange, configure each installation as a peer of the other. The normal first exchange creates
host-id mappings; later exchanges are safe retries because the receiver stores source payload
receipts. A task should inspect nonzero `conflicts` rather than repeatedly retrying the same push.

## What This Fixes

The Smart Mine handoff that motivated this work mixed a 6.3 GiB cache/archive package with a raw
`remote-hosts.sqlite` snapshot, active repositories, paused markers, checksums, and a manually
selected incoming directory. It could prove a historical snapshot, but it could not safely apply
live state and could not converge later changes.

Instance Sync moves current portable facts and user-authorized access credentials. It removes
database copying, archive rebuilding, manual checksum orchestration, stale-snapshot warnings, and
accidental application of files from an in-progress task. It also makes durable state visible as
per-record sync results and conflicts instead of hiding it inside a tarball.

## Deliberate Follow-ups

Protocol v1 reserves `topology` and `artifacts` collection names but rejects them until their
semantics are implemented. Topology needs stable node/edge provenance and host-id remapping.
Artifacts need manifest-first chunk transfer, resume cursors, same-directory temporary placement,
and explicit staged-versus-promoted application. Neither should be smuggled through a command
argument or folded into a database copy.

For peers reachable only through a bastion, the next transport extension should tunnel this same
HTTP protocol through an existing pooled SSH access path. The sync model stays unchanged: the
transport supplies reachability, while the peer protocol owns identity, receipts, conflict rules,
and apply gating.

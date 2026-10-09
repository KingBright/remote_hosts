# Gateway-only 0.10.26 release entry

This operator entry verifies the existing frozen build and publishes only Gateway
version 0.10.26 at https://mcp.hackerlife.fun. It never builds, exports another
bundle, upgrades an Agent, changes network/security configuration, or obtains an
OAuth grant. The immutable release is unchanged; these coordinator files run
outside it.

## Current authentication boundary

`/status/login` has an **Open status** button and issues the existing status-only
`rh_status` cookie: Secure, HttpOnly, SameSite=Strict, Path=/status, 12 hours.
`/admin/gateway-upgrade` is behind Bearer-only middleware. It requires the owner
and `code:write`. Read-only status observation requires `code:read`.
A status login alone cannot publish. This implementation preserves that boundary.

The entry accepts only a user-entered **already authorized owner Bearer** at a
hidden prompt in the user's own Terminal. It does not extract Codex OAuth,
read a credential file/environment variable, register/approve/refresh/revoke
OAuth, or store a password, key, token or cookie. Borrowed server expiry/scopes
are unchanged; the Bearer remains only in memory and is cleared on exit.

Without an existing bearer, the remaining action requires explicit approval for
an OAuth authorization-code / PKCE S256 session, preferably using an existing
registered client, resource `https://mcp.hackerlife.fun/mcp`, scopes
`code:read code:write`. Current server lifetimes are: pending approval 10 minutes,
code 2 minutes, access 1 hour, refresh 30 days. It always issues the refresh token;
there is no no-refresh parameter. If a reusable registered client is unavailable,
new public-client registration (`token_endpoint_auth_method=none`, no client secret)
also needs explicit approval: its server record has no automatic expiry.
These owner scopes cover the resource, not a
Gateway-only audience. No such session or persistent credential has been created.
A genuinely Gateway-specific, access-only authorization would require a separately
approved server protocol change; this entry does not claim to provide one.

## One entry, explicit authentication point

Inspect the exact local package and authentication requirement without network,
SSH authentication or publication:

```sh
python3 /Users/jinliang/Workspace/remote_hosts/scripts/gateway_release.py \
  --report-dir /Users/jinliang/Workspace/Codex/2026-10-08/task/task-auth-release-0.10.26/recovery-01/gateway-publish
```

Only if the owner already has an authorized Bearer, run the same entry:

```sh
python3 /Users/jinliang/Workspace/remote_hosts/scripts/gateway_release.py \
  --report-dir /Users/jinliang/Workspace/Codex/2026-10-08/task/task-auth-release-0.10.26/recovery-01/gateway-publish \
  --execute --borrow-existing-bearer
```

The owner enters the existing Bearer at the hidden application prompt, then
authenticates `root@hackerlife.fun:222` directly at the SSH prompt. Do not send
either secret to a model. The temporary private SSH control socket is owned by
this run, has a 600-second idle limit, and is closed on exit. Strict known-host
verification remains enabled; disk identity files, SSH-agent keys and unattended
password fallback are disabled. There is one password prompt. An absent pin,
missing session or unexpected identity stops the entry.

SSH performs only exact bundle import and public receipt/executable observation.
Upgrade execution always uses the original authenticated application endpoint.

## Verified publication and recovery

The entry pins the approved build receipt, manifest, bundle and Linux candidate
hashes; checks all original build/test gates, provenance, flat regular archive
inventory and every artifact; imports the bytes under the **running Gateway
executable's parent**. Service identity is
`remote-hosts-code-gateway.service`, matching the existing upgrade endpoint.
Different artifacts, symlink paths and unknown upgrade state are never overwritten.
Staging retains at least 4 GiB plus expansion headroom.

The running version must be 0.10.25 or this exact 0.10.26 candidate; another
version is not automatically downgraded. An already staged, matching bundle is
reused without retransmitting bytes. Import intent is durable before streaming;
a process crash during import also becomes observation only.

A private durable journal and exclusive local lock preserve one semantic release
identity. The upgrade intent is fsynced before one POST containing only version
and bundle SHA256. A lost reply never causes another POST. Existing markers,
prior attempts and interrupted requests enter observation of the original result.
Use the same report directory:

```sh
python3 /Users/jinliang/Workspace/remote_hosts/scripts/gateway_release.py \
  --report-dir /Users/jinliang/Workspace/Codex/2026-10-08/task/task-auth-release-0.10.26/recovery-01/gateway-publish \
  --observe --borrow-existing-bearer
```

Observation is bounded and does not import, launch or retry an upgrade. A completed
original import can be explicitly continued with the same journal. Partial/unknown
imports remain blocked. HTTP acknowledgement alone is not success: the running
Gateway SHA, version, wire protocol, tools hash/count, Skill, task authorization
protocol and file-transfer readiness must match the approved manifest.

Rollback remains the immutable package's existing updater responsibility. Its
binary/database backup, policy checks at preflight/cutover/rollback, and preservation
of the live authorization database are unchanged. A rollback blocked by task policy
is reported as failed with service-stopped evidence; no legacy binary or service
restart bypass is attempted.

## Validation limits

Tests use temporary files, loopback fake HTTP through an isolated synthetic-origin
adapter, and mocked SSH/service controls. Production HTTPS/TLS is unchanged.
The entry's default plan is also checked against the actual approved local bundle.
Neither is live publication evidence. Do not describe Gateway or Agents as deployed
until the real original updater and runtime checks pass.

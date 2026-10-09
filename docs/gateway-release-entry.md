# Gateway-only 0.10.26 release entry

This entry publishes only Gateway 0.10.26 at https://mcp.hackerlife.fun using
the original frozen bundle and owner API. It runs outside that release; it does
not rebuild it or upgrade Agents.

## Direct owner login

The owner explicitly approved on 2026-10-09 the OAuth authorization-code flow
with PKCE S256, resource `https://mcp.hackerlife.fun/mcp`, scopes
`code:read code:write`, access lifetime 1 hour and protocol-issued refresh lifetime
30 days. Necessary public-client registration was also approved; its server
record has no automatic expiry. This same grant does not need another approval.
Other scopes, resources and persistent TaskGrants require their own actual scope.

Run in the owner's native Terminal:

```sh
python3 /Users/jinliang/Workspace/remote_hosts/scripts/gateway_release.py \
  --report-dir /Users/jinliang/Workspace/Codex/2026-10-08/task/task-auth-release-0.10.26/recovery-01/gateway-publish \
  --execute --oauth-browser
```

The default browser opens the real Gateway OAuth page. The owner enters the
password directly and clicks **登录并授权**. The literal loopback callback verifies
state, issuer and Host, then exchanges the single code with its in-memory PKCE
verifier. No token copying is needed. The callback and token are never logged.
Only public client identity and safe progress metadata are written.

The entry retains the access token only in its process. It discards the refresh
token and never refreshes, revokes, extracts browser cookies or reads a password,
key, token or credential file. Registration and token exchange are submitted once.
An interrupted/unknown exchange or existing session is observed rather than
automatically repeating authorization.

After API authority is checked, the same Terminal prompts for the existing
`root@hackerlife.fun:222` SSH password. SSH performs verified bundle import and
bounded public executable/updater observation. Strict known-host verification
remains enabled; automatic disk-key and SSH-agent authentication are disabled.
Its private control socket has a 600-second idle bound and is closed on exit.
Upgrade execution always uses `POST /admin/gateway-upgrade`.

A status-page **Open status** login issues only the 12-hour status cookie
(Secure, HttpOnly, SameSite=Strict, Path=/status); it cannot authorize the
Bearer-only owner upgrade endpoint.

## Default plan and recovery

Without an explicit execute/observe mode and authentication option, the entry
only verifies the exact local package and writes a plan; it does not authenticate:

```sh
python3 /Users/jinliang/Workspace/remote_hosts/scripts/gateway_release.py \
  --report-dir /Users/jinliang/Workspace/Codex/2026-10-08/task/task-auth-release-0.10.26/recovery-01/gateway-publish
```

An already authorized Bearer remains an optional hidden Terminal input through
`--borrow-existing-bearer`; it is mutually exclusive with `--oauth-browser`.
There is no credential argument or environment-variable input.

The entry pins the build receipt, manifest, bundle and Linux candidate hashes;
verifies original gates/provenance and all archived bytes; stages under the
running Gateway executable's parent. Service identity remains
`remote-hosts-code-gateway.service`. Staging retains at least 4 GiB plus expansion
headroom. A matching staged bundle is reused; another version, artifact, symlink
or unknown release state stops the entry.

A durable private journal and exclusive lock preserve the original release.
Import intent and the single upgrade POST intent are fsynced first. Existing
attempts, markers and lost replies are observation only: never replay the POST.
Observe the original worker/journal first. If the worker has exited after an OAuth
exchange, no token can be recovered from disk, and the entry does not silently
create another grant. A separately available already-authorized session can use
`--observe --borrow-existing-bearer` with the same report directory.

Success requires the running Linux SHA, version, wire protocol, global tool
count/hash, Skill revision, TaskGrant protocol and file-transfer health to match
the manifest. The read-only TaskGrant route must exist or redirect to its login
guard; no TaskGrant is created. Scoped OAuth tools/list checks write authority
without broadening the grant to expose every tool.

Immutable updater backup/cutover/rollback checks remain authoritative. A rollback
blocked by persistent task policy remains failed with service-stopped evidence;
the entry does not bypass it.

## Validation limits

Tests use temporary files, loopback fake HTTP and mocked SSH/service controls.
Production HTTPS/TLS is unchanged. Local tests and plan verification are not live
publication evidence. Claim deployment only after original updater and running
binary/health checks succeed.

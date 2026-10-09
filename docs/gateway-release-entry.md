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

Before OAuth, SSH reuses the owner's existing configuration, key or agent in
BatchMode at the verified NAS endpoint `root@hackerlife.fun:222`; it never prompts
for an NAS password. The bare `hackerlife.fun` alias instead selects MacStudio
(port 2222, user jinliang), so the NAS port/user are explicit. Strict known-host
verification remains enabled. The private control socket has a 600-second idle
bound and closes on exit; slaves cannot silently reconnect or authenticate.

The read-only preflight verifies the running executable and release location
before creating an application session. On the verified Entware installation it
uses the canonical `/volume1/@entware-opt/remote-hosts-code`, a unique live process,
its working directory, owned TCP listener 18787, and loopback public health and
OAuth resource identity with the required Host header. It never reads process
arguments, environment, config, private keys or database contents. A failed
systemctl lookup is not proof that the Gateway is absent.

Publication also requires the existing API's systemd-run launcher and service
control to be verified. An unavailable controller blocks before OAuth. Observation
of an already attempted upgrade remains available. Upgrade execution always uses
`POST /admin/gateway-upgrade`; SSH does not run the updater.

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
running Gateway executable's parent. The configured systemd identity is
`remote-hosts-code-gateway.service`; verified NAS discovery is the guarded fallback.
Staging retains at least 4 GiB plus expansion
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

## Verified NAS checkpoint (2026-10-09)

Existing NAS SSH authentication succeeds without a password. Gateway 0.10.25
runs as PID 12270 from the canonical Entware directory, supervised by the existing
systemd 219 unit (not an Entware init script). At the first checkpoint the 0.10.26
release directory did not exist. Original transfer
`64b45061-9e0d-4b94-bd6a-31a1c709b120` completed with 31,956,930 bytes and SHA
`00da35fbbf1d0e768988320cd481544eb3ca5690463e4e90ee8f23209bdfc0cd`.
Its temporary receiver blob has since been removed, confirmed by both the original
operation and the exact file's absence. It is not a reusable staged release.
The owner subsequently authorized one new persistent placement of the same
immutable local bundle, without rebuilding or re-exporting it. Operation
`86cd17cf-3ed3-482a-ae7a-46797623bf57` completed: bundle size/SHA, archived files
and `bundle-stage-complete.json` were verified at
`/volume1/@entware-opt/remote-hosts-code/releases/0.10.26`.
Its semantic identity is `nas-gw-0.10.26-bundle-restore-01`. Local intent is fsynced
before sending, the final bundle and completion marker are exclusive and atomic,
and an uncertain outcome is observation only. This is staging, not deployment.

The original owner worker exited with neither import nor upgrade POST attempted.
Its exact exit cause is unavailable; do not infer a bad password. New failures
persist phase, process exit code, fixed error category and attempt flags in a
private receipt. Raw SSH stderr, Terminal input/history and credentials are never
saved. Password and keyboard-interactive SSH authentication are disabled.

The NAS SSH environment lacks systemd-run. The original MainPID query failed
because systemd 219 does not support `--value`; the supported
`--property=MainPID` query returns `MainPID=12270` and the existing unit is active.
The entry now parses that format. Both the 0.10.25 source and installed executable
contain the systemd-run self-upgrade launcher. The existing service's Python
launcher drops to UID/GID 18787; its configuration is owned by that identity with
mode 0400, while the release parent remains root-owned 0700. No ownership, mode,
unit, account, config or authentication change was made. The owner API publication
gate remains blocked; it was not called to discover these failures.

### Concrete compatible publication proposal (not executed)

The repository already supplies `gateway_upgrade_ssh` in `scripts/fleet-upgrade.py`.
It runs the packaged `upgrade-code-gateway.py` once as the existing root SSH
administrator, directly controlling the existing systemd unit. That updater uses
the supported MainPID property format and does not require systemd-run. Reuse
this maintenance path for NAS, after explicitly selecting this publication-mode
change; do not pretend the unsupported owner self-upgrade API succeeded.

Reuse the verified persistent bundle without another transfer. Extract only
verified regular members, preserve the candidate's packaged executable mode,
and pin all updater/candidate hashes. The exact existing binary/config roots are
`/volume1/@entware-opt/remote-hosts-code`; the config path is
`gateway.json` (metadata only was inspected). The unit continues to use its
existing `run-code-gateway.py` privilege drop and all hardening properties.

Run only Gateway's original updater under a bounded, separately journaled
maintenance operation: root/config authority and candidate checks; consistent
binary/database backup; existing service stop; decisive TaskGrant compatibility
check; atomic binary cutover; existing service start; exact PID/executable/public
health verification. On failure preserve the packaged rollback policy and live
database; do not restore a pre-grant database or bypass a policy-blocked rollback.
Finally apply the entry's stricter manifest/TaskGrant-route acceptance. Unknown
outcomes observe the original maintenance receipt, never rerun the updater.

This proposal adds no daemon, privilege grant, polkit/sudo rule, PATH shim or
systemd installation. It changes the selected publication channel to the existing
administrator SSH flow. No switch, new OAuth login or server authentication
modification is performed while that channel selection remains pending. The
0.10.26 owner self-upgrade API still needs a separately reviewed future platform
solution; bootstrapping this release does not fix that API.

## Future in-system iteration acceptance

Keep these gaps in the existing task and product acceptance, without a duplicate
task or broad refactor: durable task phase and recovery; reuse of valid existing
authorization; fixed verification and actual receipts; health checks and compatible
rollback; observation of unknown outcomes without replay; and continuation of
independent work. Durable release artifacts must be distinct from expiring transfer
blobs, and supported deployment controllers must be checked before OAuth. The
system must never grant itself higher privilege or bypass authentication.

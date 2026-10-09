# Owner task authorization with automatic expiry

## Source and deployed-runtime status

The NAS Gateway runs the independent 0.10.27 release with task authorization
protocol 2, wire protocol 2 and 25 MCP tools. Runtime acceptance on 2026-10-09
verified the installed and running executable SHA-256, UID 18787, local health,
owner-route authentication and existing-authority read/terminal/transfer receipts.
The 0.10.26 published bundle remains byte-identical. Only the Linux Gateway was
built and deployed; Agents retain their existing wire-compatible versions.

The binary is bound to commit `b55e061a1d88a9975125cbacc6761c1b321600c3` and the
declared working-tree snapshot
`0b1556c3ff430a9aca27946a832cda271a52e186eb1da0bd400566b53c4b9b6b`.
The initial package's Skill revision used the hash of one file; acceptance caught
that mismatch before service changes. A separate corrected package uses the
ordered nine-file embedded Skill revision; original artifacts and the rejection
receipt are retained. Its binary and updater bytes did not change or rebuild.
The accepted NAS package is in `releases/0.10.27/verified-02`; runtime evidence is
`task-auth-release-0.10.27/gateway-01/runtime-acceptance.json` in the task evidence
workspace. No production grant, OAuth change or other-product action occurred.

The conversation currently exposes 24 Remote Hosts tools against the Gateway's
25-tool catalog: `task_resume` is absent and its enqueue tools do not accept
`authorization_version`. The Gateway's advertised catalog does not establish host-side availability. There is no
supported connector action exposed here to refresh that schema. For a custom
MCP connection, use ChatGPT Plugins, select the existing connection, choose
Refresh, verify the metadata, then start a new conversation and inspect the
tools. Published plugins follow the host's catalog/review update process.
See [OpenAI's connection documentation](https://developers.openai.com/plugins/deploy/connect-chatgpt).
Never invent a version or submit a field/tool absent from the active schema.

## Owner approval and scope

The trusted entry point remains `/status/task-authorization` on the existing
Gateway HTTPS origin. The owner signs in directly and supplies the existing
password for each grant write. Same-origin POST, a session-bound CSRF value and
`expected_version` CAS remain required. MCP/OAuth credentials cannot create,
renew or revoke a grant. The form starts no operation and creates no credential.

A grant records the owner, exact task ID, version, enabled state, exact enrolled
device IDs, existing allowlisted scopes, protocol, update time and expiry.
Authorization requires a lifetime of 1–1440 minutes. The server computes
`expires_at = server_now + lifetime`; caller-provided absolute timestamps are
not accepted. At `server_now >= expires_at` the grant is expired. Revocation
increments the version, disables the grant and preserves its previous devices,
scopes and expiry even after enrollment changes. All versions remain in the
private history. An owner browser session ending does not end a valid grant;
server expiry does not depend on that session or a scheduled manual revocation.

The initial form leaves devices empty and proposes only `code:read`, with a
60-minute lifetime. These are inactive defaults until the owner submits approval.
A request must still satisfy current account scopes, enrolled-device scopes,
the grant and Agent local allow flags/path checks. Task/device bindings do not
enlarge that intersection. A bare `task_id` without a stored grant remains a
correlation label; unlabelled legacy calls retain existing API authority.
TaskGrant revocation therefore does not revoke the account's other API access.

TaskGrant has no directory, command or individual-tool allowlist. `code:read`
covers the existing read tools and their existing local path policy. Neither a
workspace ID, cwd nor a string prefix proves filesystem confinement.
`terminal:exec` permits shell execution with local-user authority, including
outside code roots. Do not call it a workspace sandbox or infer that TaskGrant
limits a command to the intended project.

## Enforcement and continuation

Expiry, revocation, exact owner/task/version, enrolled device and scope are
checked before queue creation, again while inserting the queue/binding in its
transaction, and atomically before an undispatched job is claimed. Expired,
revoked or changed grants block new/undispatched task work without blocking
independent work. Existing dispatched/running work can continue; expiry is not
a process-kill guarantee. Existing receipts remain readable under their current
account/operation authorization.

A protocol-1 grant without an expiry is not silently promoted. It reports
`expiry_required` and bound undispatched work waits for direct owner renewal
to a new version. Existing unbound jobs and receipt recovery keep their original
semantics. The deployed protocol-2 form provides automatic expiry. The 0.10.27
updater blocks migration while an effective unbounded legacy grant exists, so
cutover cannot silently invalidate the owner's current approval. The production
preflight found no grant, binding or history rows; no effective grant changed.

After direct owner renewal, `task_resume` requires the exact task ID, new
owner-issued version and one original request or operation ID. A durable
Gateway task-policy rejection must prove `not_started` with no operation.
Recovery retains the immutable original intent, file versions and idempotency
key. A queued job needs no result, no dispatch timestamp and an existing timing
record; renewal updates its binding while keeping its operation ID. Concurrent
resume must create/claim only one execution. An identical completed resume
observes that original operation.

Unknown outcomes, already dispatched work, platform approval, local policy
denials, source-address rejection, credentials/terminal input, transfer URLs
and generic access denial are not eligible. Observe their original handles.
Renewal does not repair another product's transport, Android executor,
Cloudflare browser-signature rejection, OAuth connection or host schema cache.

## Replacement and rollback

The updater reads only aggregate policy capability metadata. Protocol-1 rows
require support for at least protocol 1. Protocol-2 or expiry-bearing grant/binding
rows require at least protocol 2 even if expired, revoked or orphaned. Unknown
future versions and malformed metadata fail closed. Candidate manifests must
declare a recognized integer protocol supporting the persisted minimum.

The capability gate runs before service changes, again after stopping the policy
writer, and before rollback. An additional migration gate checks effective legacy
grants before replacement and after stopping that writer. If it blocks cutover,
the unchanged previous binary restarts; no grant is rewritten or erased. A new
expiry policy cannot be ignored by a protocol-1 predecessor. If compatible rollback is unavailable, the configured service stays
stopped and the live database is retained; use a verified compatible binary.
Do not clear policy or restore an older database to force downgrade. This source
change does not modify or supersede immutable previously packaged updaters.

## Minimum future production probe

The former proposal for three scopes plus manual revocation after one hour is
withdrawn. No production grant was created. Manual revocation is not automatic
expiry, and both the previous 0.10.26 acceptance and the 0.10.27 runtime acceptance
used existing account authority rather than a human-issued TaskGrant.

Protocol-2 release/runtime acceptance is complete. After the host schema is
updated, the inactive proposal is task `taskgrant-expiry-live-0.10.27-01`, enrolled
device `ba3bf113-2390-466e-88bc-40d5b4f02884` (MacBook-M2-Max), only `code:read`
and five minutes. It requests no terminal or write scope.
The owner must directly accept the actual read scope and issue the version.
Use one harmless read in an existing permitted workspace, close the owner
browser, observe its original receipt, then check that a new labelled read after
server expiry is rejected before queue creation while the original receipt
remains readable. Do not create a grant until the owner approves that concrete
scope. This probes expiry/intersection only; it does not establish exact-file
confinement or unattended assistant scheduling.

Keep terminal/write recovery and revocation races in isolated fixtures for this
minimum production probe. A future need for exact paths or shell confinement
requires an enforced capability/sandbox design and separate acceptance;
changing cwd or adding a path label is insufficient.

## Current-source verification

Use the existing registered release-slot target and its actual lease. Build
only the relevant library test harness with the pinned toolchain, locked offline
dependencies and one build job. Record declared source-input hashes before and
after compilation/testing, Cargo's generated executable, its checksum, test
count and sealed logs. A cached test executable without exact input provenance
does not establish current-source acceptance; zero selected tests are rejected.

The 0.10.27 frozen snapshot passed 17 TaskGrant library tests, 16 Python updater
tests, eight dispatcher tests and one release-manifest test (42 total), plus
format, Clippy and component check gates. It reused the registered target and
lease, built one Linux production executable and left more than 107 GiB free.
The updater tests include active-legacy preflight rejection, a grant appearing
before cutover, and valid protocol-2 approval metadata.

The isolated suite covers expiry boundaries, expiry between initial check and
queue binding, post-enqueue expiry, legacy grants, owner/task/device/scope
binding, current account/device/local policy, owner authentication/CAS,
revocation/renewal races, original-marker exactly-once recovery and rollback
downgrade faults. These fixtures do not establish live owner approval, fleet
rollout, platform tool refresh or RemotePlay/Pixel/Android recovery.

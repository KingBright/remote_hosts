# Owner task authorization with automatic expiry

## Source and deployed-runtime status

The current source implements task authorization protocol 2. The deployed
immutable 0.10.26 Gateway advertises protocol 1 and 25 MCP tools; its existing
bundle, updater and production state were not changed by this source work.
Protocol 2 must receive a new immutable release version and normal signed
release/runtime acceptance before production use. Do not replace the existing
0.10.26 artifacts with a build from this changed source.

The conversation currently has an older tool schema: `task_resume` is absent
and its enqueue tools do not accept `authorization_version`. The Gateway's
advertised catalog does not establish host-side availability. There is no
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
semantics. The source protocol-2 form provides automatic expiry; the deployed
protocol-1 form cannot provide it.

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

The gate runs before service changes, again after stopping the policy writer,
and before rollback. A new expiry policy cannot be ignored by a protocol-1
predecessor. If compatible rollback is unavailable, the configured service stays
stopped and the live database is retained; use a verified compatible binary.
Do not clear policy or restore an older database to force downgrade. This source
change does not modify or supersede immutable previously packaged updaters.

## Minimum future production probe

The former proposal for three scopes plus manual revocation after one hour is
withdrawn. No production grant was created. Manual revocation is not automatic
expiry, and the previous 0.10.26 acceptance used existing account authority,
not a human-issued TaskGrant.

After a separately verified protocol-2 release and host schema refresh, propose
one unique task ID, one existing test device, only `code:read` and five minutes.
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

The isolated suite covers expiry boundaries, expiry between initial check and
queue binding, post-enqueue expiry, legacy grants, owner/task/device/scope
binding, current account/device/local policy, owner authentication/CAS,
revocation/renewal races, original-marker exactly-once recovery and rollback
downgrade faults. These fixtures do not establish live owner approval, fleet
rollout, platform tool refresh or RemotePlay/Pixel/Android recovery.

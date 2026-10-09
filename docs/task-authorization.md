# Persistent owner task authorization

This candidate adds a persistent approval for one owner/task and an explicit
recovery path. It does not create Codex conversations, schedule an assistant,
refresh an external platform login, disable platform approval, or authorize root.

The trusted entry point is /status/task-authorization on the Gateway's existing
HTTPS origin. First use the existing owner status login. Each grant write also
requires the existing owner password, exact same-origin POST, a session-bound
CSRF value and expected_version CAS. OAuth/MCP credentials cannot establish or
change a grant. No new credential is created. The form does not execute work.

The grant records task_id, version, enabled, exact enrolled device IDs and the
allowlisted code:read/code:write/terminal:exec scopes. All versions are retained
in the existing private Gateway store. Approval persists until explicitly changed
or revoked; an owner browser session ending does not end an approved task.

A managed request supplies task_id and authorization_version. Its authority is
the intersection of the current OAuth account scopes, enrolled device scopes,
the task grant, Agent allow_write/allow_exec and existing path checks. Grants
cannot expand any of these. Terminal commands retain their existing local-user
authority outside code roots; they are not a project-path sandbox. Legacy calls
without a stored grant continue using existing account/device authorization;
a bare correlation label is never a grant.

Grant checks happen before queue creation, again in the queue transaction, and
atomically when a queued operation is claimed. A version change prevents queued
work from being handed to an Agent. Already dispatched/running work is not
retroactively undone and remains protected by the original outcome/idempotency
journal. Independent queued work remains eligible.

Use task_context to read the grant version, blocked queued operations and bounded
rejected-request metadata. The cursor includes grant/rejection changes. Never
infer successful execution from a task or workspace being present.

After an owner changes authorization, task_resume takes task_id,
authorization_version and exactly one original request_id or operation_id:

- A Gateway task-policy rejection must durably prove not_started and have no
  operation. Recovery uses its privately retained immutable original intent.
  Original commands, paths, expected file versions and idempotency keys remain
  fixed. One atomic claim prevents concurrent recovery from creating duplicates.
- A queued operation must have no result and no dispatch timestamp. Recovery
  updates its authorization binding and keeps the same operation ID.
- Unknown outcomes, running/completed work, platform prompts, local policy
  denials, source-address rejection, credentials/terminal input, transfer URLs
  and generic access_denied are not eligible. Observe existing handles instead.
- A crash after a recovery claim is unknown; it cannot be reclaimed by submitting
  another version or idempotency key.
- task_resume still requires the original operation's current account/device
  scopes. File CAS checks and Agent local flags remain authoritative.

Deferred intent is retained only for workspace_open, code_apply_edits,
files_sync and terminal_exec task-policy rejections. It is separate from public
receipts; task_context/operation_get expose metadata, never these arguments.
Rejected unrelated sources (including EnGraph) must not use this path.

This change is source-only until the verified candidate is deployed through the
existing signed release workflow, Gateway first. A rollout must explicitly name
the target Gateway/Agents, new catalog/Skill identities, original build receipt,
maintenance lease and runtime acceptance. It does not require changing roots,
local allow flags, OAuth scopes, credentials, sudoers, VPN or DNS.

## Defaults and upgrade scope

Deployment creates no grant. The form initially leaves device IDs empty and
prefills the three existing scopes; these are proposed values, not active
permission. The owner must select enrolled devices and submit authenticated
approval. No-grant legacy calls retain the existing account/device/local policy.
An owner-created grant makes subsequent enqueued calls with that exact task_id
require its current version. Unlabelled calls remain the existing API authority;
a TaskGrant is not a replacement for account-wide revocation or a command/path
sandbox. Observation and existing control APIs keep their existing authorization.

The new MCP catalog entry is task_resume (25th tool). The trusted browser entry
is GET/POST /status/task-authorization; login remains POST /status/login via
/status. A valid existing status session can load the form, but every grant write
requires the owner to enter the existing password directly. There are no new
credentials, OAuth scopes, device enrollments, local allow flags, roots, sudoers,
VPN/DNS rules or platform approval exemptions. Persistent grant/version/history,
operation bindings, deferred intent and recovery audit are new private records
in the existing Gateway store. They persist until explicit owner cleanup.

Revocation preserves the prior devices/scopes and works even after enrollment
or device scopes change. A missing timing record is not proof of no dispatch;
task_resume refuses such a queued operation.

## Deployment and rollback boundary

Use one new immutable version and the existing verified release bundle:
release-code.py on the existing leased Mac Studio target, then fleet-upgrade.py
with the established private deployment configuration. The narrow authenticated
/admin/gateway-upgrade path upgrades the Gateway first. Existing signed Agent
updaters keep their maintenance leases, hashes and host signing checks; controller
cutover stays last. Android requires its separate signed APK workflow.

The Gateway updater backs up the binary and SQLite state, checks candidate hash,
version and config, stops only its configured service, then rechecks policy before
replacement. It starts the candidate and requires bounded health plus executable
identity. Ordinary compatible rollback restores the previous binary and preserves
the live database, including owner changes made during the attempt.

Once task grants or task-bound operations exist, replacement and rollback binaries
must declare task_authorization_protocol=1. This is checked again after stopping
the policy writer. It prevents a rollback to 0.10.25 from dispatching a task-blocked
queue without checking the grant. If a new runtime creates policy and then fails
health while its predecessor lacks support, automatic rollback is blocked and
the service stays stopped; the receipt names that recovery state. Recover with
a verified compatible binary. Do not clear policy, weaken authentication or
restore an old database to force rollback.

## Live owner-offline acceptance (pending deployment)

1. Verify deployed Gateway protocol 1, 25-tool catalog and candidate Skill identity,
   plus selected Agent/runtime convergence. Host exposure must report task_resume;
   a server catalog alone does not establish that the conversation has it.
2. Choose one unique acceptance task_id and the existing enrolled test device.
   The owner opens /status, signs in directly, then opens
   /status/task-authorization?task_id=<id>. Choose only the needed existing scopes
   and authorize version 1. Verify operations_started=false.
3. The owner logs out/closes that browser. Through the already-authorized connector,
   use task_id/version 1 to create a small isolated project under existing roots,
   apply a version-checked test file, run a bounded content check and local Git
   commit, and read back the file, commit and original terminal receipts.
4. Require file versions, original operation IDs, exit 0, complete captured output
   and commit identity. OAuth refresh/connection and the active assistant remain
   platform prerequisites; this feature does not schedule or wake the assistant.
5. The owner revokes to version 2. Submit one harmless marker command for that task;
   require durable task-policy rejection, not_started and no operation. Reauthorize
   to version 3, then task_resume that exact original request. Require unchanged
   command/idempotency identity and one execution. Repeat resume only to observe
   the same operation; marker count and operation count must remain one.
6. Clean up only the acceptance project while its grant remains valid, then have
   the owner revoke it. Read grant history and receipts without exposing deferred
   command arguments. Do not change production flags/scopes to manufacture tests.

The isolated suite covers queued races, missing evidence, account/device/local
denials, platform/source refusals and rollback faults. Those tests are not live
fleet, human owner consent, client tool refresh or target-platform evidence.

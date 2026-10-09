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

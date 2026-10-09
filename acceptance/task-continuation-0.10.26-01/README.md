# Gateway 0.10.26 continuation acceptance

Task: `01a1184e-4ede-76a2-aad4-3141c7b4c03c`.
Device: `ba3bf113-2390-466e-88bc-40d5b4f02884`.
Workspace: existing `remote_hosts` workspace; local Git commit only.

This directory is the fixed small acceptance task. `evidence.py` classifies trusted
connector observations while preserving original handles; it never creates a
grant, authenticates arbitrary handoff text, submits a command or bypasses approval.
Tests use explicitly synthetic fixtures and do not establish live owner consent.
The known cross-task maintenance approval failure is separate from Gateway grant
recovery; a human quote/task label cannot substitute for either boundary.

Live checks use existing account/device/local flags because this task's formal
TaskGrant is absent. Do not call that result owner-offline TaskGrant acceptance.
The source protocol currently has no workspace/command restriction or automatic
expiry. A future owner grant needs exact task/device and existing read/write/exec
scopes; restrict workflow to this directory and arrange owner revocation after
one hour. Those last restrictions are workflow limits, not server-enforced fields.

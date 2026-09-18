# Remote Hosts Code: observable, recoverable tool use

## Scope

This is the source-level 0.10.4 experience contract. It does not install or upgrade the
Gateway, an Agent, a ChatGPT connector, or a Gemini app. Source verification, the generated
adapter artifact, deployed process identity, and a real client connection are separate gates.
Gemini OAuth client support remains in the same Gateway and its existing regression suite.

## One catalog, three evidence boundaries

`tools::catalog()` owns the schemas and honest tool annotations. `tools/list` filters this
catalog using the authenticated principal's scopes. It must not advertise execution tools
to a read-only OAuth grant. Downloading a file creates a Gateway artifact and is not marked
read-only merely because it reads the source device.

The compiled executable generates `crates/remote-hosts-code/adapter-contract.json`:

```sh
remote-hosts-code adapter-contract --output crates/remote-hosts-code/adapter-contract.json
remote-hosts-code adapter-contract --output crates/remote-hosts-code/adapter-contract.json --check
```

Generation is an explicit development action; CI checks the checked-in artifact and never
regenerates it to conceal drift. Server and adapter tools hashes refer to the same catalog.
`skill_revision` hashes the complete embedded Skill bundle, including referenced files, and
is checked against its own expected revision, not against a tool-schema hash.

The Rust stdio adapter uses that compiled catalog, verifies the upstream authorized tools,
and automatically sends `x-rh-adapter-catalog-sha256`, `x-rh-adapter-skill-revision` and a stable
per-call `x-rh-request-id`. A request's final-host report is separate. No report means
`unknown_not_reported`; the server never infers final host exposure from device permissions,
its own catalog, or a model-supplied `known_tools_sha256` comparison hint. Contradictory reported
hashes and tool names are not treated as a confirmed match. Reports do not grant authority.

An external host must supply its actual final tool list to report that layer. This repository
cannot mutate a running ChatGPT conversation's tool schema or manufacture a host attestation.
The adapter's optional host-report input is an integration point, not proof that any specific
external host currently implements it.

## Durable calls and safe recovery

Gateway request receipts are inserted before dispatch. Creating a remote job binds request ID,
operation ID and optional task association in the same database transaction. A crash after
commit can therefore be queried by the original request ID without creating another job.
A failed pre-dispatch receipt write prevents execution. A post-dispatch save/read failure
preserves uncertainty and the original durable binding; it is never reported as permission
to rerun an arbitrary command.

Receipt fields distinguish transport completion, process outcome, output completeness and
business acceptance. In particular:

- A terminal handle alone is not evidence that the process has exited.
- A stale active snapshot means the process outcome is unknown, not failed.
- Exit zero plus missing or truncated output does not imply complete evidence.
- Successful execution does not imply that the business acceptance criteria were correct.
- `not_observed_or_expired` does not mean that a host safety check rejected the request.

Exact retry of an observed request returns its original operation. Conflicting payloads are
rejected. Observation and task recovery never replay work. Access is checked again against
current owner, account scope and device scope; a handle is not an authorization token.

## Run and observe

A short noninteractive `terminal_exec` has a bounded default wait. Longer work retains one
operation ID. `operation_get` reads Gateway-side replicated state and up to 2 KiB of sanitized
live output; `terminal_cursor` continues the UTF-8 byte stream when retained. It creates no
Agent read job. A preview gap is explicit; `terminal_read` remains the complete-history and
recovery path rather than the default polling loop.

Terminal snapshots merge transactionally and cannot regress a recorded terminal outcome.
Output cursors never go backward. Output-only changes invalidate the observation cursor;
heartbeats without a meaningful change do not. Repeated identical final log lines advance
output delivery without pretending that business progress has advanced.

An unchanged observation can omit payload while retaining its decision receipt. Batches
factor repeated ordinary completed-receipt defaults once when needed to meet a byte budget.
Errors, stale state, required user action, incomplete output and unknown outcomes retain
explicit per-operation evidence. An omitted payload is always marked incomplete.

## Independent status and task recovery

`/status` is the owner-password-protected, server-rendered status view. `/admin/status` is the
scoped API projection. Both reuse existing jobs, terminal observations, progress and fleet
records. Status reads do not generate download URLs, refresh credentials, or enqueue work.
Task cards expose device, workspace, initial working directory, observed process ID, process
state, transport state, meaningful progress time, staleness, blockers and redacted receipts.
A historical PID is evidence, never permission to signal that PID.

The page distinguishes active processes from completed submission RPCs, offline waiting,
uncertain outcomes, missing observations and genuinely empty active work. It does not show
an invented completion percentage. Limited or failed observations cannot become a global
claim that there is no work.

Pass the same optional `task_id` with related tool calls. `task_context` groups their durable
operation links across devices and workspaces. Follow its page cursor when `has_more` is true.
It provides the next existing handle rather than creating a replacement task. It never
reconstructs unknown Git commits or promotes a terminal exit into verified source evidence.
When no source verification was attached, `last_verified_source` remains null.

New request receipts, task associations and terminal observations have no automatic expiry.
The API reports `retention_seconds: null` and `until_explicit_owner_cleanup`; normal pruning
cannot silently remove them after seven days. Full process logs and release receipts stay at
their source until explicit owner cleanup. Existing expired historical records cannot be
reconstructed, and storage/backups still require normal operational care.

A request observation carries both the new call's `request_id` and the original
`observed_request_id`/`request_receipt`. Never follow the observation call's own audit ID in
place of the original task handle.

Lifecycle tests use `Agent::run_isolated()` so exercising an Agent cannot synchronize test
Skill files into the operator's real Codex or Gemini installation.

## Verification evidence

The existing source verifier and the Rust release executor distinguish selected, executed,
passed, failed and skipped/ignored tests. Ignored or skipped tests do not count as executions;
zero executed tests cannot produce verified success. Known failed cases cannot be masked by
a final successful shell exit. The Rust executor records each concrete command, source plan,
resolved toolchain fingerprints, working directory, environment, exit code and log hashes.
Completed steps are revalidated before reuse. The source verifier's scope includes Code, the
Rust release executor, MCP compatibility and the shared token-output crate.

## Acceptance and measurements

`experience_contract` exercises persistent request binding, injected store failures, terminal
ordering, scopes, task recovery, status aggregation and real authenticated MCP responses.
`experience_ab` runs real local child processes through authenticated in-process HTTP routers
and the actual Gateway/Agent implementations. It compares the compatibility workflow and
preferred workflow on the same candidate for short success, incremental output and a failing
command. Both paths must deliver identical output hashes and exit codes.

An optional `RH_EXPERIENCE_AB_REPORT` writes a new report without overwriting an earlier one.
The report records actual calls, read jobs, observations, serialized model-visible UTF-8 bytes
and elapsed time. Byte counts are not model token counts. This fixture does not measure an
external ChatGPT/Gemini host, the live fleet, or a previous deployed binary. Do not extrapolate
its results into an unsupported global percentage.

The `Code experience contract` CI workflow checks format, warnings, the generated catalog,
source tests and transitional evidence tests. It has read-only repository permissions and no
deployment step. A configured workflow is not a claim that a hosted CI run has completed.

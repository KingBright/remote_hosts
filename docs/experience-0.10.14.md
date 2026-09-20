# 0.10.14: initialization recovery and useful operation observation

## Scope

Use the owner's local Rust toolchain, existing build slot, source snapshot, immutable artifacts, signed platform updaters, and target-device acceptance. No cloud builds. Do not infer a performance pass from functional tests.

## Native adapter initialization

Only initialize, notifications/initialized, and tools/list use bounded bootstrap recovery. The entire bootstrap shares a 25-second deadline; each attempt has at most eight seconds and each step has at most three attempts. Only interrupted transport or body collection and attempt timeout are retryable. Authentication rejection, HTTP rate limiting, invalid protocol data and catalog mismatch are not retried.

Each step persists one request identity and its attempt history. Successful recovery retains its earlier failure. Bootstrap never invokes tools/call. An uncertain business call is never replayed and a broken established client remains poisoned. Bootstrap receipts are separate from operation recovery handles. Notification response bodies are consumed within a strict bound so reusable connections do not retain unread bodies.

## Operation observation

Without a cursor, observe waits within the requested budget for the actual process result, not just a transport submission or running-state transition. It also recognizes a running process from a durable submission when no replicated preview exists yet. Legacy single-operation calls retain their shape and bounded wait.

With a cursor, observe still returns the next state or output change. Zero wait still returns immediately. Stale or failed observations are not hidden behind a completion wait. Notifications are registered before reading durable state, preventing a lost-wakeup window. No observation creates another remote job.

## Evidence gates

Fault injection covers incomplete bootstrap response recovery, fixed retry budget, shared deadline, authorization and response-ID rejection, storage failure before sending, and rejection of tools/call by the bootstrap whitelist. Gateway tests cover intermediate transitions, missing previews, legacy calls, cursor changes, bounded waits, and stale or failed output. Native client tests separate bootstrap failures from uncertain tool execution.

Deployment and performance results must be recorded separately after testing real installed programs. Existing source tests alone do not certify the new runtime or an absolute latency target.

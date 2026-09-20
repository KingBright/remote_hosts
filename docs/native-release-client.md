# Local release client: native MCP transport

The local release client reuses the **installed Rust `remote-hosts-code adapter`** for the lifetime of each `Client`. Rust owns HTTPS connection reuse, generated tool catalog verification, per-call request identities, and durable operation receipts. The Python layer is only OAuth bootstrap, stdio framing, and existing release coordination. This is not a claim that the entire release pipeline is Python-free.

## Selection and evidence

`Client(...)` defaults to `auto`: use the canonical installed adapter when it is available. If it is absent, select the legacy client **before any MCP request** and report `native_absent_before_any_rpc`. Production checks may require `transport='native'`; this fails closed if the binary is missing. `transport='legacy'` is an explicit comparison/recovery option, not an automatic response to a timeout.

`REMOTE_HOSTS_RELEASE_TRANSPORT` accepts `auto`, `native`, or `legacy`. `REMOTE_HOSTS_NATIVE_ADAPTER` selects an explicit executable, and `REMOTE_HOSTS_NATIVE_STATE_DIR` selects the local evidence root. Explicit arguments override environment defaults. No binary is fetched from the network, no Agent is registered, and no service is restarted by selecting a client.

A `Client` keeps one owned adapter subprocess. `transport_info()` reports the chosen route, executable hash, startup duration, RPC count and evidence directory. The native adapter automatically reports its own authorized catalog; final host exposure remains unknown unless the actual host supplies a separate report.

The default evidence root is `~/.local/share/remote-hosts-code/release-clients/`. Each session has a private directory with the Rust adapter's original request receipts, an adapter stderr log, and session metadata. Credentials are passed through private files, never command arguments. Close the client in a `finally` block: normal and failed sessions remove temporary token/config files, retain receipts, and reap only their owned subprocess. Failed OAuth revocation is not reported as success and retains the refresh handle for an explicit cleanup retry.

## Failure and recovery

There is no automatic replay, process restart, or switch to legacy HTTP after native transport selection. Timeout, truncated framing, response identity mismatch, invalid JSON, or an over-budget frame poison that session. `NativeTransportError` retains the receipt directory and unacknowledged request/operation references. Reconnect explicitly, authorize normally, and observe those references. Do not resubmit a command merely because the stdio response was lost.

An upstream structured error is returned as an error, not translated into a success. Authentication, proxy selection, and TLS validation remain the responsibility of the existing Rust adapter. OAuth/admin/artifact requests retain the original urllib path and its existing cookie/redirect/TLS policy.

## Terminal observation

`Client.terminal(...)` follows the same `operation_id` with bounded `operation_get` waits. Ordinary short output does not require a second queued `terminal_read` operation. Tail-only, incomplete, or legacy previews use exact full-history recovery from byte zero. A tail is never appended to an unrelated prefix. UTF-8 byte cursor gaps, truncation, log errors, nonzero exit status, and unconfirmed final capture are failures.

## Validation and release boundaries

Run `python3 -W error::ResourceWarning -m unittest discover -s scripts/tests -v` on the owner-operated development machine. Tests include real stdio child fault injection, single-process reuse, no-replay behavior, token-file cleanup, request identity, routing, full-log fallback, and exact cursor validation. These tests are bridge/coordinator evidence, not a new Rust build.

The integration reuses the already verified native binary. Helper-only changes can be deployed to the development controller without rebuilding or replacing Agent/Gateway executables. Record the helper Git revision separately from the installed binary version/hash. Never modify an existing signed release bundle or move its tag. Future immutable packages include `native_release_client.py` alongside `release_client.py`.

Real acceptance must test both direct commands and the entire read/edit/transfer/recovery path through the selected transport. Report absolute latency separately from call count; connection reuse does not prove all network, storage, or command-start latency is eliminated.

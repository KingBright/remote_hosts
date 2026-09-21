# 0.10.18 readiness cost and edge diagnosis

Status: candidate. Final local build, installed identity and target-native acceptance remain separate gates.

## Readiness

Successful authenticated poll replies record five bounded lane timestamps in shared process-local memory. The existing heartbeat loop publishes merged readiness, normally at most once per ten seconds. Newly observed lanes may publish sooner, with a one-second attempt floor. Idle unchanged state produces no write. Failed lanes retain their actual last successful timestamp. The memory state is telemetry only, never command execution or authorization authority.

Each database publication matches both process ID and session and carries an increasing publication revision. A late timed-out write cannot overwrite a newer revision. Unconfirmed writes retain the pending facts and do not advance the local publication cursor. The heartbeat waits at most 250 ms for optional publication; the SQLite call might still complete later, which is why revision fencing is required. Critical command records, outbox acknowledgements, WAL and FULL settings are unchanged.

The workspace recovery view now reads queue health from the original durable outbox using the current device and origin. It no longer reads the obsolete runtime/receipt_delivery copy removed from active reporting in 0.10.17. Failure is explicit unavailable, not an empty healthy queue.

## HTTP evidence and retries

Release HTTP failures retain bounded status, request path without query, Cloudflare error code when explicitly returned, and validated CF-Ray. Response bodies, token values and untrusted free text are not returned. Only known public metadata GETs can retry explicitly transient HTTP errors, at most three attempts. A 403, declared non-retryable response, rate limit, OAuth write or MCP POST is never automatically replayed by this helper.

An ordinary diagnostic Python client received Cloudflare 1010 / HTTP 403. The original release client was checked separately without changing its existing identity and timed out; that timeout is not proven to be the same condition as the 1010, nor does 1010 explain older 520 responses. Origin loopback metadata returned HTTP 200. The configured Cloudflare DNS credential can read the zone and DNS record, but security-setting and configuration-rule reads returned HTTP 403 (codes 9109 and 10000). No security rule, DNS proxy flag or credential was changed to evade those denials.

Owner action for the proven 1010: review a narrowly scoped configuration rule for GET requests on mcp.hackerlife.fun to the OAuth discovery paths and /healthz, disabling only Browser Integrity Check where machine-readable discovery is intended. Keep authentication, rate limits, other WAF checks and other hostnames unchanged. Do not claim this resolves the original client's timeouts without new measurements.

Official references:
- https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-1xxx-errors/error-1010/
- https://developers.cloudflare.com/waf/tools/browser-integrity-check/

## Acceptance

Count logical readiness writes with the existing isolated native Agent fixture; verify both fresh startup lane evidence and no stale-lane refresh. Exercise held-writer timeouts, failed publication retry, late snapshots and session changes. Run the complete frozen-source suite, then cross-build locally, verify real process identity and repeat read/edit/execute/transfer/recovery tests. Preserve failed attempts; source-test success does not prove public OAuth bootstrap or low end-to-end latency.

# File source initialization diagnostics

An upload Agent first retrieves the original source capability from the Gateway,
then resolves and validates its hostname before starting the file HTTP request.
The older `source_dns_or_connection` label collapsed DNS timeout/failure,
non-public address rejection and HTTP client initialization failure. That label
alone cannot prove a failed TCP connection, an OAuth rejection, or a platform
network restriction. Older receipts cannot reconstruct a hostname or OS error
that was discarded; do not infer them from another device or a guessed domain.

The source initializer now preserves a stable, URL-free diagnostic:

| Code | Confirmed boundary |
| --- | --- |
| `source_dns_timeout` | Device DNS lookup exceeded its bounded deadline |
| `source_dns_lookup_failed` | Device resolver returned an error |
| `source_dns_no_addresses` | Resolver returned no addresses |
| `source_network_access_denied` | Resolver explicitly reported permission denied |
| `source_address_policy_rejected` | At least one resolved address failed the existing public-address policy |
| `source_client_initialization_failed` | HTTP client construction failed before a source request |

Only the source hostname, stage, side and numeric OS error (when available) are
reported. Signed paths, queries, tokens and raw exception chains are omitted.
A DNS failure without an explicit permission error does not establish whether
an OS or platform policy caused it. Public-address validation, pinned DNS,
no-proxy behavior, hostname allowlist and redirect rejection remain enforced.

These failures retain the original operation/checkpoint and report
`diagnose_original_source`. The receipt preserves that instruction rather than
automatically recommending another resume. Confirm actual connectivity and the
original source authorization before resuming the same operation. Do not change
DNS, proxy, VPN or device security, switch identities, or replace the endpoint to
bypass a rejection.

For paused and awaiting-source imports, `operation_get` now also reports current
source authorization as `available`, `expired`, or `required`. Its optional
`source_endpoint` contains only the validated scheme, hostname and port. The
query uses the same original owner/device scope checks and neither queues work
nor refreshes authorization. An expired source needs explicit authorization for
the original file and bytes; a new source must never be guessed.

`scripts/check-transfer-recovery.py` includes isolated classification/redaction,
real Agent/Gateway address-policy rejection, and source-expiry observation cases
alongside the existing recovery tests. They use temporary loopback servers and
synthetic grants. Source/test completion does not change installed runtimes or
prove a production network failure has recovered; installation follows the
normal authorized release workflow.

# Forge projects through the existing owner login

This optional read-only feature reuses the existing Gateway owner password and `rh_status` session. It adds no account, credential, OAuth scope, TaskGrant, cookie-path change or new listener. The existing public Caddy route already forwards this host to Gateway.

Configuration is disabled when `forge_browser` is absent:

```json
{
  "forge_browser": {
    "app_addr": "127.0.0.1:60594",
    "projects": ["existing-canonical-project-uuid"]
  }
}
```

Use only actual NAS project UUIDs, verified from the current catalog. The target must be a fixed IPv4 `127.0.0.1` address with a nonzero port; it cannot be a URL, hostname, remote address or browser parameter. Project IDs must be unique and bounded to 64. The App must separately opt in to the same IDs with `--owner-read-project`.

After owner login at `/status`, the enabled status page links to `/status/forge/`. Project deep links are `/status/forge/projects/<id>`, with `/issues`, `/experiments`, `/knowledge`, `/evidence`, and typed detail links `/issues/<resource-id>` or `/experiments/<resource-id>`. Missing/expired/revoked owner sessions are denied. An unsigned forwarded identity or an OAuth bearer token does not substitute for that session. Login uses the existing route; no automatic return-cookie or new login mechanism is added.

Only GET/HEAD read routes are accepted. Query strings, traversal, encoded separators, unlisted projects, wrong resource types and unknown routes are rejected. Gateway fetches only two fixed private App projection paths. It does not forward cookies, bearer tokens or request headers; redirects are disabled, response size/time are bounded and upstream error bodies are suppressed. There is no generic proxy, raw evidence/file endpoint, administrative RPC, writable route or model execution.

The read UI displays versions, source/evidence summaries, limitations, historical observations/runs, and current Issue coverage. It escapes project data and uses wrapping navigation and text at 320px. A positive route-only run does not establish a performance baseline or convert Unknown acceptance into a pass.

This candidate is isolated from the concurrently published Remote Hosts UI. Before a production release, coordinate with that release owner, integrate/rebase the two commits against its accepted source, run the same checks, assign a fresh immutable release identity and produce target Linux artifacts on the authorized release builder. Do not publish this development source as another 0.10.27 artifact or replace the other worker's candidate.

Production activation requires the pending explicit approval for the existing owner to read the selected NAS projects. Proposed changes are the tested Gateway/App artifacts, the optional `forge_browser` object in the existing private gateway config, and the exact App project allowlist arguments. Preserve configuration modes/ownership, listeners, login credentials, sessions, all other routes and daemon/data. Rollback removes only that optional object/arguments and uses preserved prior executables; it does not restore an old project database.

Local acceptance includes owner expiry/revocation, writes, traversal, whitelist/deep-link enforcement, redirect/error suppression, HTML escaping, no upstream credential forwarding, unchanged workspace data, existing owner OAuth/password regressions, and a real App/Daemon/Gateway headed Chrome navigation run at 320px and 1280px. Physical Android and public Cloudflare access are separate, unverified conditions.

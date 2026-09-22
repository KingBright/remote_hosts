# Desktop code access

Codex can connect directly to the same Code Gateway as ChatGPT and Gemini Spark.
This is separate from the older `remote-hosts mcp-stdio` SSH inventory/transport
service; keep that service for managed SSH and infrastructure operations.

Gateway 0.10.19 adds native OAuth callbacks for `http://127.0.0.1:<port>/callback`
and `http://[::1]:<port>/callback`, optionally with a bounded callback identifier.
Dynamic registration stores the concrete URI. Authorization and token exchange
still require that exact URI, owner consent, S256 PKCE and the correct resource.
Non-loopback HTTP, hostname aliases, userinfo, query/fragment redirects and
noncanonical paths are rejected. The consent page's CSP includes only the
current listener's exact origin; this does not add a browser CORS origin.

## Codex setup

Use the deployment's canonical public HTTPS origin, without an alternate port:

```sh
codex mcp add remote-hosts-code --url https://YOUR_GATEWAY/mcp
codex mcp login remote-hosts-code
```

If adding the server already completed login, a second login is unnecessary.
Codex stores and refreshes its own OAuth grant. Never put a Gateway owner password,
Agent credential or copied conversation token in `config.toml`. The existing
Rust stdio adapter accepts an externally managed access-token file and is useful
for controlled clients; it does not itself refresh OAuth and should not be
configured as an indefinitely authenticated desktop service.

The resulting user-level entry is:

```toml
[mcp_servers.remote-hosts-code]
url = "https://YOUR_GATEWAY/mcp"
startup_timeout_sec = 30
tool_timeout_sec = 60
```

Preserve other server entries and the client's existing approval policy. Reload
MCP or start a new task to load the new catalog; an old task's tools are a snapshot.
The desktop app and CLI share the Codex host's MCP configuration. See the
[official MCP documentation](https://developers.openai.com/codex/mcp).

## Working across devices

1. Call `devices_list` or `fleet_status`, and explicitly choose a device by ID.
2. Open the required project under a root that device advertises. Reuse its
   `workspace_id`; a workspace never moves to another device.
3. Use `code_search`, `code_symbols` and bounded `code_read` ranges to inspect.
4. Pass returned file versions to `code_apply_edits`; inspect `code_diff` and
   run the appropriate validation with `terminal_exec`.
5. Observe the original operation/terminal after a timeout. Resume only through
   the documented change-set/transfer recovery entry; never replay an uncertain
   mutation or silently route an offline device to another host.

Verify desktop exposure separately from server capability: initialize/list with
the configured desktop client, then perform read/edit/execute/transfer acceptance
on disposable files on each online device. Offline devices remain unverified.

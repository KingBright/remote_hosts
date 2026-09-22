# Gemini Spark / standard OAuth MCP clients

## Scope and release status

This change extends the existing `remote-hosts-code` Gateway. It does not introduce a second
Gateway, an operator API exposure, a new database, a browser automation bridge, or an Agent wire
protocol. The same tools and authorization scopes serve ChatGPT and other MCP clients.

**An in-process test pass is not a live Gemini connection acceptance.** Production rollout must
also configure the real user's exact callback and verify the real Gemini authorization + MCP
sequence. Do not describe this feature as deployed merely because code or an artifact exists.

The server supports:

- Streamable HTTP at the existing `/mcp` resource and the existing OAuth discovery endpoints.
- DCR and local pre-registration, sharing one registration implementation and client store.
- `none`, `client_secret_basic`, and `client_secret_post`. The registered method is enforced for
  code exchange, refresh, and confidential-client revocation. Missing DCR auth method defaults to
  `client_secret_basic` per RFC 7591; existing ChatGPT/public clients keep their `none` registration.
- S256 PKCE and an exact resource audience remain mandatory, including for confidential clients.
- New client secrets are returned once and stored only as hashes. The CLI writes a new mode-0600
  file atomically and refuses to overwrite an existing file. No secret is printed to stdout.
- Failed client authentication, wrong resource, wrong callback, or wrong PKCE cannot consume a
  legitimate authorization code. Refresh markers bind to the client and are committed atomically.

## Configuration

`redirect_uris` contains complete, exact OAuth callback URLs. `allowed_origins` contains exact
browser origins. These lists have different purposes and neither grants access by itself.

Older configuration files without `allowed_origins` enable both supported hosted MCP clients:
`https://chatgpt.com` and `https://gemini.google.com`. The Gateway's own origin is always included.
Both HTTP security layers use the same list.

OAuth callbacks remain exact for ordinary clients. From 0.10.20, Gemini Spark DCR accepts its
observed six-callback registration: HTTPS on the exact `oauth-redirect.googleusercontent.com`,
`oauth-redirect-sandbox.googleusercontent.com`, or `oauth-redirect-test.googleusercontent.com`
host, with `/r/user_bound_custom-mcp-` or `/a/user_bound_custom-mcp-` followed by one bounded
URL-safe suffix. The registration limit is six, and every entry must pass validation. The concrete callback is
then stored on that registered client and every authorize/token exchange still has to match it
exactly. This deliberately does **not** allow `*.googleusercontent.com`, arbitrary Google paths,
query-bearing callbacks, fragments, or browser origins as OAuth callbacks. The authorization CSP
adds the active callback's exact Google Account Linking origin; test/sandbox browser request
origins are not added to the CORS allowlist.

Manual/pre-registered credentials still use a complete exact callback from the real Gemini flow.
Callback URLs, account identifiers, credentials, and live deployment receipts belong in the
private ops profile, not in this public repository.

Validate the updated configuration before restarting:

```sh
remote-hosts-code check --config /private/setup/gateway.json
```

## Connect

Prefer dynamic registration: use the Gateway's public `/mcp` URL in Gemini's custom app dialog,
then complete the owner login and consent. Registration alone never grants access.

For the advanced-settings route, pre-register using the exact callback already configured:

```sh
remote-hosts-code register-oauth-client \
  --gateway-config /private/setup/gateway.json \
  --name 'Gemini Spark' \
  --redirect-uri "$EXACT_GEMINI_CALLBACK" \
  --auth-method client_secret_basic \
  --output /private/setup/gemini-oauth-client.json
```

Use the resulting client ID and client secret in Gemini's advanced settings. They are not a
Gemini API key, a Gateway owner password, or an Agent credential. The registration CLI modifies
only the selected Gateway state and the new credential file; it does not modify configuration,
restart services, or expose a public administrator registration endpoint.

The authorization page displays the client-provided name as unverified, the concrete client ID,
the exact redirect, scopes, and the terminal authority warning. Tool write annotations remain
honest. Host-side confirmation behavior is owned by Gemini, not bypassed by the Gateway.

## Acceptance

Run `cargo test -p remote-hosts-code --test oauth_clients` and the existing integration/regression
suite. The new tests exercise the actual in-process HTTP router, not mocked auth-success paths.
Then verify on an isolated staging state before the production cutover:

1. Real Spark discovery, dynamic registration (including its `user_bound_custom-mcp-*` callback),
   owner authorization, token exchange, `initialize`, `tools/list`, and `devices_list`.
2. On the explicitly selected device, open a test workspace and compare a known file range with
   the real file. With confirmation, edit only a disposable test file, execute a harmless command,
   and follow its existing operation/terminal rather than replaying it.
3. Refresh and revocation; no client can exchange another client's code, refresh its tokens, or
   revoke its grants. Confirm the existing ChatGPT connection still works.
4. Record artifact identity separately from the source tree, installed binary, running process,
   and live client acceptance. Agents do not need a fleet upgrade for this change alone.

Conversation attachment import is not declared Gemini-compatible by this change. Its host file
reference/authorization contract must be verified separately; do not expand the URL allowlist to
arbitrary external sources.

## Rollback boundary

Gateway configuration and OAuth state must be part of the rollback plan. Old binaries deny unknown
configuration keys and do not implement confidential-client authentication. New confidential-client
identities and grants therefore use `rh2_` opaque values and separate `oauth2_*` kinds in the same KV
store. A pre-feature binary cannot discover those grants and accidentally skip client authentication.
Public-client state retains its original kinds and refresh-replay string format; additional ownership
binding is a separate row committed in the same transaction. This requires no second database.

A rollback still needs a verified configuration without unsupported keys and an actual compatibility
test of the chosen old binary. Preserve unrelated live device/workspace work; do not restore an entire
old database blindly. An in-process namespace regression is not proof of a successful production rollback.

## References

- Google custom apps: https://support.google.com/gemini/answer/17209137
- MCP authorization: https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization
- OAuth registration/auth methods: https://www.rfc-editor.org/rfc/rfc7591
- HTTP Basic client authentication: https://www.rfc-editor.org/rfc/rfc6749#section-2.3.1

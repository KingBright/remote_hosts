# Setup and Runtime

Use this when configuring MCP or checking whether the local Remote Hosts service is usable.

## Local Service

Expected paths:

- Binary: `/Users/jinliang/.local/bin/remote-hosts`
- Service helper: `/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service`
- Config: `/Users/jinliang/.config/remote-hosts/service.env`
- Local vault key: `/Users/jinliang/.config/remote-hosts/vault-master-password` (generated automatically, mode `0600`)
- Database URL: `sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite`
- HTTP API: `http://127.0.0.1:8787`
- Logs: `/Users/jinliang/.local/state/remote-hosts/logs`
- Output artifacts: `/Users/jinliang/.local/share/remote-hosts/artifacts`

Useful commands:

```bash
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service status
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service update
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service restart
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service logs
curl -sS http://127.0.0.1:8787/v1/command-profiles
curl -sS http://127.0.0.1:8787/v1/admin/overview
```

The launchd API wrapper passes the same local vault password file used by MCP and the connector.
`GET /v1/admin/overview` should therefore report `vault_unlocked=true`. The HTTP API remains bound
to loopback; do not expose the unlocked vault on a public or LAN bind.

## MCP Server Command

Agents should launch the stdio MCP server on demand:

```bash
/Users/jinliang/.local/bin/remote-hosts mcp-stdio --database-url sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite --tool-profile agent --vault-master-password-file /Users/jinliang/.config/remote-hosts/vault-master-password --artifact-root /Users/jinliang/.local/share/remote-hosts/artifacts --agent-client-kind codex
```

Do not run MCP stdio as a launchd daemon. Keep API and connector as daemons; let the agent client own the stdio MCP child process.

## Updating and Reloading

`remote-hosts-service update` rebuilds the binary, applies migrations, synchronizes the
repository-owned Skill into Codex and Antigravity, and restarts the launchd-owned API and
connector. It does not replace MCP stdio children or already-loaded Skill context owned by an
existing Codex or Antigravity task; those processes keep the old binary, tool schema, and
instructions until the client reloads them.

- Complete any required calls on the current MCP transport before reloading it.
- Reload MCP servers or begin the next agent task after an update, then require runtime `snapshot_version=6`.
- Do not kill the MCP child mid-task and expect the same task transport to reconnect automatically.
- For deployment smoke tests, a separate freshly launched MCP stdio client may verify the installed binary without disturbing an active task.

## Codex MCP Config

Expected `~/.codex/config.toml` entry:

```toml
[mcp_servers.remote-hosts]
command = "/Users/jinliang/.local/bin/remote-hosts"
args = ["mcp-stdio", "--database-url", "sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite", "--tool-profile", "agent", "--vault-master-password-file", "/Users/jinliang/.config/remote-hosts/vault-master-password", "--artifact-root", "/Users/jinliang/.local/share/remote-hosts/artifacts", "--agent-client-kind", "codex"]
startup_timeout_sec = 30
```

## Antigravity MCP Config

Expected `~/.gemini/config/mcp_config.json` entry:

```json
{
  "mcpServers": {
    "remote-hosts": {
      "$typeName": "exa.cascade_plugins_pb.CascadePluginCommandTemplate",
      "command": "/Users/jinliang/.local/bin/remote-hosts",
      "args": [
        "mcp-stdio",
        "--database-url",
        "sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite",
        "--tool-profile",
        "agent",
        "--vault-master-password-file",
        "/Users/jinliang/.config/remote-hosts/vault-master-password",
        "--artifact-root",
        "/Users/jinliang/.local/share/remote-hosts/artifacts",
        "--agent-client-kind",
        "antigravity"
      ],
      "env": {}
    }
  }
}
```

Profiles:

- `agent`: default 18-tool transport-first surface for arbitrary remote shell/PTY work, managed file transfer, bounded artifact reads, idempotent host registration, and encrypted credential updates.
- `admin`: adds registry and operational maintenance tools.
- `full`: all tools for development and debugging.

Changing profiles requires restarting or reloading the MCP child process.

Infrastructure topology is currently an HTTP management-plane surface rather than another MCP
tool family. Agents use the existing shell/PTY tools for discovery and the loopback
`/v1/topology` API for normalized graph reads and authoritative snapshot synchronization. Follow
`topology-and-inventory.md` so a partial discovery cannot accidentally inactivate valid nodes.

Global client configuration should set only `--agent-client-kind`. Never place one static `--agent-client-instance-id`, `--agent-project-key`, or `--agent-conversation-key` in a global config, because that would merge unrelated tasks. When a launcher supports per-task interpolation, it may pass a stable client-instance or conversation key for restart continuity; without one, each MCP child receives a fresh isolated Agent Session.

## When Tools Are Missing

If the skill is loaded but no `remote_hosts_*` tools are visible:

1. Check MCP configuration.
2. Restart the agent client or reload MCP servers.
3. Verify the binary exists and `remote-hosts doctor` passes.
4. Verify the service database exists and migrations have run.
5. Use the HTTP API only for read-only checks unless the user explicitly asks for topology or
   administrative maintenance.

If `remote_hosts_ensure_host` is missing while other agent tools are visible, the MCP child is stale or the installed binary is outdated. Reload the MCP server after updating the service. Switch to `admin` only for low-level registry repair, not ordinary host registration.

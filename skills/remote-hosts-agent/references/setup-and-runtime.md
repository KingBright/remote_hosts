# Setup and Runtime

Use this when configuring MCP or checking whether the local Remote Hosts service is usable.

## macOS Local Service

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
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service stage
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service update
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service restart
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service ui
/Users/jinliang/Workspace/remote_hosts/scripts/remote-hosts-service logs
curl -sS http://127.0.0.1:8787/v1/command-profiles
curl -sS http://127.0.0.1:8787/v1/admin/overview
```

The launchd API wrapper passes the same local vault password file used by MCP and the connector.
`GET /v1/admin/overview` should therefore report `vault_unlocked=true`. The HTTP API remains bound
to loopback; do not expose the unlocked vault on a public or LAN bind.

## Windows Local Service

The native Windows package installs under `%LOCALAPPDATA%\RemoteHosts` and registers `Remote Hosts
API` plus `Remote Hosts Connector` as current-user Task Scheduler jobs. Use:

```powershell
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Status
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Update
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Restart
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Ui
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Skills
& "$env:LOCALAPPDATA\RemoteHosts\bin\remote-hosts-service.ps1" Logs
```

The Windows connector supports only the native `russh` backend. It retains pooled commands,
PowerShell, PTYs, upload/download, Windows SSH Agent named-pipe and Pageant authentication, password
fallback, and key bootstrap. The OpenSSH native-mux compatibility backend is Unix-only.

## MCP Server Command

Agents should launch the stdio MCP server on demand:

```bash
/Users/jinliang/.local/bin/remote-hosts mcp-stdio --database-url sqlite:///Users/jinliang/.local/share/remote-hosts/remote-hosts.sqlite --tool-profile agent --vault-master-password-file /Users/jinliang/.config/remote-hosts/vault-master-password --artifact-root /Users/jinliang/.local/share/remote-hosts/artifacts --agent-client-kind codex
```

Do not run MCP stdio as a launchd daemon or Scheduled Task. Keep API and connector in the platform
service manager; let the agent client own the stdio MCP child process.

On Windows, use the stable native launcher shown by `remote-hosts-service.ps1 PrintConfig`. It reads
`%LOCALAPPDATA%\RemoteHosts\config\current-binary.txt`, starts the current versioned binary, and
inherits MCP stdio directly. New tasks therefore pick up staged releases without changing the MCP
configuration; already-running MCP children remain on their original binary until reloaded.

## Updating and Reloading

`remote-hosts-service stage` rebuilds or stages the binary, applies migrations, and synchronizes the
repository-owned Skill into Codex and Antigravity without restarting local services.
`remote-hosts-service update` stages the same files and restarts only after its database drain
gate confirms there are no queued/running operations, active PTYs, queued PTY inputs, or
unexpired write leases. `restart` uses the same gate. Use `--force` only when interrupting all
reported conversations is intentional. Neither command replaces MCP stdio children or
already-loaded Skill context owned by an existing Codex or Antigravity task; those processes
keep the old binary, tool schema, and instructions until the client reloads them.

For a planned upgrade, first run the restart-readiness check and map each reported live PTY,
operation, input, or write lease to its owning Agent Session and client conversation. Notify only
those conversations. Ask them to finish the current atomic step, close their PTY/Workspace, stop
submitting new work, and reload MCP after the upgrade. Include the compact response rules: follow
`next_action` and `retry_after_ms`, preserve ids and sequence cursors, consume only new chunks, and
request runtime snapshots only for diagnostics. Wait until the normal drain gate passes; do not use
`--force` as a substitute for notification and orderly release.

Windows uses the same Rust `restart-readiness` query and `-Force` spelling. Its versioned release
directories avoid overwriting an executable that is still in use. Neither macOS nor Windows service
management depends on an external SQLite CLI.

Output storage supports compact Postcard plus Zstandard segments. A routine binary update remains in
legacy-compatible write mode so an already-running old MCP child keeps seeing new output. Reload all
Codex and Antigravity MCP children before running `remote-hosts-service optimize-storage` on macOS or
`remote-hosts-service.ps1 OptimizeStorage` on Windows. That explicit command migrates and verifies
all legacy PTY and command output, enables compressed writes, and reclaims physical SQLite pages
without restarting either service. It refuses migration while conversation work remains unless
interruption is explicitly forced. PTY or Workspace terminal transitions also fail and scrub any
input that can no longer be delivered, so stale queue entries do not block service lifecycle actions.

After the installed API has been restarted once with external admin UI support,
`remote-hosts-service ui` atomically updates only
`~/.local/share/remote-hosts/ui/admin.html`. Reloading `/admin` picks up that file without
restarting the API, connector, MCP children, PTYs, or active operations.

- Complete any required calls on the current MCP transport before reloading it.
- Reload MCP servers or begin the next agent task after an update, then require runtime `snapshot_version=11` and inspect host-level `workspace_capacity` independently from per-access-path `channel_capacity`, including exact `coordination_scopes` and `pty_input_required` before treating a blocked workspace as broken.
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

On Windows, keep the MCP path stable across versioned updates:

```toml
[mcp_servers.remote-hosts]
command = "C:/Users/<user>/AppData/Local/RemoteHosts/bin/remote-hosts-launcher.exe"
args = ["--", "mcp-stdio", "--database-url", "sqlite://C:/Users/<user>/AppData/Local/RemoteHosts/data/remote-hosts.sqlite", "--tool-profile", "agent", "--vault-master-password-file", "C:/Users/<user>/AppData/Local/RemoteHosts/config/vault-master-password", "--artifact-root", "C:/Users/<user>/AppData/Local/RemoteHosts/data/artifacts", "--agent-client-kind", "codex"]
startup_timeout_sec = 30
```

Use `PrintConfig` rather than guessing `<user>` or the configured root.

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

- `agent`: default 19-tool transport-first surface for arbitrary remote shell/PTY work, PTY long-task heartbeats, managed file transfer, bounded artifact reads, idempotent host registration, and encrypted credential updates.
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

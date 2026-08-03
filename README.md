# Remote Hosts

Remote Hosts is a Rust-based SSH control plane for AI agents and human operators. It keeps remote
host inventory, access routes, credentials, runtime state, reusable connections, command history,
files, and operational knowledge in one local system.

The product is transport-first: agents execute ordinary POSIX or PowerShell commands through a
pooled SSH connection instead of opening a new login for every action. Kubernetes, Harbor,
databases, GPU tools, and other domain CLIs run through that generic channel rather than requiring
one MCP tool family per product.

## Core Capabilities

- Canonical host registry with duplicate detection, multiple environments, and multiple access
  paths per host.
- Connector-owned native `russh` connection reuse on macOS, Linux, and Windows, plus an optional
  Unix OpenSSH compatibility backend, with bounded channel concurrency, keepalive, reconnect
  control, and handshake rate protection.
- Arbitrary POSIX and PowerShell execution, persistent PTYs, bounded output, large-output artifacts,
  and state visible to the calling agent.
- Verified upload and download over pooled SFTP, framed exec channels, or an already-selected
  interactive bastion PTY.
- Per-conversation Workspace, PTY, operation, artifact, and idempotency isolation while sharing the
  physical SSH transport.
- Hierarchical mutation scopes so unrelated resources on the same host can be changed concurrently
  without allowing conflicting work to overlap.
- Local encrypted credential storage, password fallback, and bounded public-key bootstrap without
  coupling the product to an external vault service.
- Generic infrastructure topology for clusters, hosts, services, dependencies, inactive history,
  and encrypted resource credential bindings.
- Compact MCP surface and an Agent Skill for Codex and Antigravity.

## How It Fits Together

Remote Hosts has four cooperating planes:

- **Knowledge:** hosts, facts, software, operation history, and durable notes.
- **Access:** environments, connectors, routes, credentials, and route selection.
- **State:** connector, access path, transport, session, Workspace, PTY, and operation health.
- **Execution:** guarded commands, pooled channels, file transfer, PTY continuity, output, and audit.

The API and MCP server persist logical work in SQLite. A long-running connector owns live SSH
transports and executes queued work. Agent conversations remain logically isolated, while the
connector safely reuses the same authenticated transport across those conversations.

See [Architecture and Runtime](docs/architecture-and-runtime.md) for the detailed model.

## Quick Start

### macOS

Install the release binary, database, launchd services, admin UI, and Agent Skills:

```bash
scripts/remote-hosts-service install
scripts/remote-hosts-service status
```

Open the local management console at <http://127.0.0.1:8787/admin>.

Common lifecycle commands:

```bash
scripts/remote-hosts-service update
scripts/remote-hosts-service restart
scripts/remote-hosts-service logs
scripts/remote-hosts-service ui
scripts/remote-hosts-service skills
```

Normal updates use a drain gate and will not interrupt active operations, PTYs, pending input, or
write leases. The `--force` option is reserved for an intentional interruption.

See [Deployment and Operations](docs/deployment-and-operations.md) for paths, configuration,
upgrade behavior, bastion guidance, retry policy, and troubleshooting.

### Windows

Build or download the Windows x64 ZIP, extract it, and run in Windows PowerShell:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\remote-hosts-service.ps1 Install
.\remote-hosts-service.ps1 Status
```

The native installer uses current-user Task Scheduler jobs for login startup and failure recovery,
keeps releases versioned for non-disruptive staging, and installs a stable Rust MCP launcher. See
[Windows Installation and Operations](docs/windows.md).

## Agent Integration

The default `agent` MCP profile exposes 18 task-oriented tools for host registration, credentials,
knowledge, runtime snapshots, Workspaces, generic commands, file transfer, PTYs, artifacts, and
event waits. Low-level registry repair stays in the `admin` profile.

The canonical instructions live in [`skills/remote-hosts-agent`](skills/remote-hosts-agent).
Running the following command synchronizes them into Codex and Antigravity:

```bash
scripts/remote-hosts-service skills
```

Existing conversations may keep their already-loaded Skill or MCP child until the client starts a
new conversation or reloads its MCP configuration.

## Development

The workspace pins Rust `1.94.1`.

```bash
cargo fmt --all
cargo test --workspace
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/build-windows-cross.sh
```

Useful local entry points:

```bash
cargo run -p remote-hosts-cli -- doctor
cargo run -p remote-hosts-cli -- migrate --database-url sqlite://remote-hosts.sqlite
cargo run -p remote-hosts-cli -- serve --bind 127.0.0.1:8787
cargo run -p remote-hosts-cli -- mcp-stdio --database-url sqlite://remote-hosts.sqlite
```

## Workspace Layout

```text
crates/
  remote-hosts-domain      Shared entities, IDs, and state types
  remote-hosts-core        Policy, supervision, redaction, and transport traits
  remote-hosts-vault       Local encrypted credential vault
  remote-hosts-db          SQLx migrations and repositories
  remote-hosts-connector   SSH transports, pools, PTYs, and operation workers
  remote-hosts-api         Axum HTTP API and administration console
  remote-hosts-mcp         MCP server, profiles, and request schemas
  remote-hosts-cli         Service and administration CLI
migrations/                Database schema migrations
skills/                    Repository-owned Agent Skill
```

## Documentation

- [Product and Technical Plan](REMOTE_HOSTS_PRODUCT_TECH_PLAN.md): product scope, domain model,
  architecture decisions, and roadmap.
- [Architecture and Runtime](docs/architecture-and-runtime.md): transport reuse, Agent Sessions,
  scoped coordination, commands, PTYs, transfers, MCP tools, API surfaces, and security behavior.
- [Deployment and Operations](docs/deployment-and-operations.md): local services, updates,
  configuration, resource reference, production routes, retry rules, and diagnosis.
- [Infrastructure Topology](docs/infrastructure-topology.md): graph model, authoritative snapshot
  synchronization, stable identity, and credential binding.
- [Windows Installation and Operations](docs/windows.md): native cross-compilation, Task Scheduler
  services, versioned updates, paths, MCP launcher, and troubleshooting.

## Current Release

The 2026-08-03 release adds native Windows runtime and packaging support, a stable low-overhead MCP
launcher, Rust-owned connector bootstrap and restart-readiness checks, runtime snapshot version 8,
hierarchical Workspace coordination, capacity-aware scheduling, resumable verified transfers,
transport evidence, grouped topology management, and an 18-tool Agent profile. Release-level
implementation details stay in the architecture and operations documents.

## Security Summary

Credentials are encrypted in the local database using a generated local master key. Dedicated
credential tools can store user-supplied secrets but never return plaintext. Remote execution stays
inside managed Workspaces with policy checks, bounded output, redaction, runtime state, and audit.

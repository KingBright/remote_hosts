# Host Registry Discipline

Use this before creating, updating, or reasoning about host records.

Normal registration and route updates use `remote_hosts_ensure_host` in the default `agent` profile. It combines duplicate detection, canonical host upsert, environment creation, encrypted credential storage, access-path upsert, and connector selection. Use `remote_hosts_store_host_credential` to update credentials on an existing route. Use `admin` only for low-level repair or entities that the task-level tools do not cover.

## Identity Model

Treat a host as the physical or virtual machine identity. Treat access paths as ways to reach that same host from different environments.

Do not create separate hosts merely because:

- the machine has multiple IP addresses;
- it is reachable from home LAN, company LAN, VPN, FRP, public IP, or customer network;
- it has multiple SSH usernames or ports;
- it appears under both hostname and IP;
- it was discovered by a different connector.

Create a new host only when evidence shows it is a distinct machine.

## Duplicate Check

Before adding or updating host data:

1. Search/list existing hosts.
2. Compare normalized names, display names, owner, tags, host kind, and risk level.
3. Compare known access paths: address, port, username, environment, connector, route type, and proxy chain.
4. Compare facts when available: hostname, machine-id, OS, kernel, CPU/GPU, MAC addresses, serial/model, installed software, service ports.
5. Compare history: operations, knowledge links, and software installs.
6. If two records probably represent the same machine, do not create another record. Report a merge/update recommendation.

## Update Rules

Prefer updating these entities instead of duplicating:

- Same machine, new IP or network: add/update an access path.
- Same machine, new SSH user/port/key: add/update access path and credential reference.
- Same machine, new environment: add/update environment and access path.
- Same machine, stale facts: update facts with observed time and source.
- Same software found in a different path/version: update software install record, preserve history in notes/knowledge.

## Maintenance Workflow

Use this loop for requests such as "record this server", "update host X", "clean duplicates", or "remember what is installed there":

1. Use `remote_hosts_ensure_host` for ordinary registration; it checks the proposed slug, exact display name, and SSH endpoint before writing.
2. Use the canonical host returned by the tool. If it reports ambiguity, make no further registry changes and ask for confirmation.
3. Separate machine identity from access paths. New network routes, ports, users, VPN/proxy paths, or connector scopes belong under the same host when the machine is the same.
4. Treat an existing environment's kind and trust level as canonical when a caller supplies different guesses. Inspect `defaults_applied` and continue; do not create a second environment name to bypass the mismatch.
5. Correcting `route_type` for the same environment/address/port/username/proxy chain updates the existing access path. A bastion gateway username remains one path and should not be duplicated as both `bastion` and `vpn`.
6. Record observations as facts or knowledge with source and time. Do not overwrite useful history with a single latest value unless the schema explicitly models current state.
7. Link software installs, operations, command outputs, and notes to the canonical host and relevant access path when possible.
8. When stale or duplicate data is found, produce a merge/update plan before mutating anything.

For observed facts, prefer stable identifiers over transient network data:

- Strong identity: machine-id, hostname plus OS install history, serial/model, stable MAC, GPU inventory, long-lived service identity.
- Medium identity: display name, project/owner, recurring IP, SSH host key fingerprint, installed software profile.
- Weak identity: one-off IP address, temporary hostname, NAT address, username, port, or current network location.

## Registry Write Safety

Use official MCP/HTTP/CLI registry mutation tools when they exist.

For the installed MCP surface, prefer:

- `remote_hosts_ensure_host` for normal create/update/access-path work in the `agent` profile
- `remote_hosts_store_host_credential` for encrypted credential create/update work in the `agent` profile
- `remote_hosts_record_knowledge` for durable observations in the `agent` profile

The following low-level tools require `admin` and are intended for repair or specialized maintenance:

- `remote_hosts_find_host_duplicates`
- `remote_hosts_upsert_host`
- `remote_hosts_upsert_environment`
- `remote_hosts_upsert_credential_ref`
- `remote_hosts_upsert_access_path`
- `remote_hosts_record_host_fact`

If `remote_hosts_ensure_host` is unavailable in a future client/session:

1. Do not directly edit SQLite unless the user explicitly asks for local database maintenance.
2. Present the exact intended change as a structured patch plan.
3. Explain that the MCP child is stale or the installed binary does not yet provide task-level registry mutation; do not switch profiles merely for a normal registration.
4. Prefer implementing or using first-class registry mutation endpoints instead of ad hoc database writes.

When a maintenance response cannot be applied automatically, include:

- canonical host to update or merge into;
- access paths to add, change, disable, or relabel;
- facts/knowledge/software records to add or refresh;
- duplicate host ids to archive only after relinking;
- confidence level and evidence.

## Minimum Host Record

For `remote_hosts_ensure_host`, collect:

- stable name: lowercase slug, not just a transient IP;
- display name: human-readable;
- host kind and risk level;
- owner or project;
- tags;
- environment;
- access path protocol/address/port/username/route;
- credential material only when the user explicitly supplied it, placed in `access.credential_secret` so the service encrypts it locally and never returns it;
- credential reference when a managed credential already exists; otherwise the connector uses local SSH-agent/default-key identities first;
- connector or network scope when multiple healthy connectors could reach the route;
- notes explaining ambiguous identity evidence.

## Merge Recommendations

When duplicate records are detected, recommend:

- canonical host id/name to keep;
- access paths to move or consolidate;
- facts/software/knowledge/operations to relink;
- stale record to disable/archive after relinking;
- evidence and confidence level.

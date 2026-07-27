# Topology and Inventory

Use this reference for cluster topology, services running inside a Host, reverse proxies,
middleware, databases, storage, endpoints, dependency relationships, and inactive history.

## Product Boundary

Remote Hosts owns a generic directed infrastructure graph. It does not replace Kubernetes,
Docker, Harbor, systemd, database, or cloud CLIs.

1. Discover reality through the existing managed `shell.posix`, `shell.powershell`, file, or PTY
   channel on a canonical Host.
2. Normalize the successful result into topology nodes and edges.
3. Synchronize the graph through the loopback HTTP API.

Do not request a new domain-specific MCP tool when a normal remote command can discover the data.

## Read Surfaces

- `GET http://127.0.0.1:8787/v1/topology`
- `GET http://127.0.0.1:8787/v1/topology?include_inactive=true`
- `GET http://127.0.0.1:8787/v1/admin/overview`
- `GET http://127.0.0.1:8787/v1/topology/credential-bindings`
- Human console: `http://127.0.0.1:8787/admin`

Prefer `/v1/topology` for graph work. Use `/v1/admin/overview` only when host, access-path,
connector, credential-metadata, and graph context are all needed together.

## Identity Model

`external_key` is the global durable identity of a node or edge. Reuse it across repeated
observations and producer scopes.

Recommended node keys:

- `host:<host-id>` for a canonical Remote Hosts machine.
- `cluster:<environment>:<name>` for a cluster.
- `vm:<environment>:<name>` for a virtual machine.
- `container:<host-or-cluster>:<runtime-id>` for a container.
- `service:<scope>:<name>` for a business service.
- `database:<scope>:<name>`, `cache:<scope>:<name>`, or `queue:<scope>:<name>`.
- `proxy:<scope>:<name>`, `storage:<scope>:<name>`, or `endpoint:<scope>:<name>`.

When a node represents a registered machine, set `host_id` to the canonical Host id. Never create
a second Host merely because another topology producer found the same address.

Keep edge keys stable and semantic, for example
`edge:<scope>:ingress-api:proxies-to` or `edge:<scope>:api-db:depends-on`.

## Authoritative Snapshot Contract

`POST /v1/topology/sync` reconciles one complete producer snapshot identified by
`scope_key + source`.

- Repeating the same snapshot is idempotent.
- Nodes and edges omitted by a later snapshot become inactive for that producer scope.
- Omitted objects are retained as history.
- Another active producer membership can keep the same graph object active.
- `external_key` is global, while membership and inactivity are scoped by `scope_key + source`.

Before syncing:

1. Complete the remote discovery without timeout, truncation, or parse errors.
2. Read the existing graph, including inactive entries.
3. Resolve ambiguous Host identity before creating topology keys.
4. Build all nodes first, then edges that reference those node keys.
5. Include every resource currently owned by this producer scope.

Abort without POSTing when discovery is partial. A partial result is evidence, not an authoritative
snapshot. Store a bounded diagnostic as operation output or knowledge instead.

After syncing, verify:

- returned active and inactive node/edge counts;
- important node status, address, and port values;
- every edge endpoint exists;
- `GET /v1/topology?include_inactive=true` shows expected history;
- no unrelated member became inactive.

## Node and Edge Semantics

Use the generic node kinds and relations already accepted by the API. Typical relations include:

- `contains` and `member_of`;
- `runs_on`;
- `proxies_to` and `routes_to`;
- `depends_on` and `connects_to`;
- `replicates_to`;
- `exposes`;
- `managed_by`.

Store only bounded, non-secret inventory attributes in `metadata`, such as software/version,
namespace, image digest, role, protocol, ownership label, or discovery evidence.

Do not put passwords, tokens, private keys, kubeconfig contents, connection strings containing
credentials, cookies, or service-account material in topology metadata. Secret-like metadata keys
are rejected.

## Credentials

Topology credential bindings attach encrypted credential metadata to any node with a purpose such
as `admin`, `readonly`, `database`, `automation`, or `registry`.

- Host SSH credentials still belong to `remote_hosts_ensure_host` or
  `remote_hosts_store_host_credential`.
- Service, database, registry, or API credentials may be bound through the management form or node
  credential endpoint.
- Responses expose metadata and bindings only, never plaintext.
- Do not embed a secret JSON body in a shell command, URL, persisted note, or knowledge record.
- Use the loopback management form or another client path that keeps the secret body out of command
  history and audit summaries.

If `vault_unlocked=false`, check the launchd API wrapper and service update before asking the user
to re-enter credentials.

## Discovery Patterns

Examples of commands that may run through the existing generic channel:

- Host services: `systemctl`, `launchctl`, `docker ps`, `podman ps`, `ss`, `lsof`, or PowerShell
  service/network commands.
- Kubernetes: `kubectl get nodes,namespaces,deployments,services,ingresses -o json`.
- Proxies: validated Nginx, HAProxy, Traefik, or application routing configuration.
- Databases and middleware: vendor CLIs that return bounded instance, role, replication, and
  endpoint metadata.

Use structured JSON output when the remote CLI supports it. Keep raw output bounded and store large
diagnostics as output artifacts. Normalize only facts actually observed; do not infer hidden
dependencies from naming alone.

## Reporting

Report the synchronized `scope_key`, `source`, active/inactive counts, important status changes,
and any ambiguity that prevented a complete snapshot. For discovery commands, also report the
Host, Workspace, access path, and transport evidence according to the main Skill.

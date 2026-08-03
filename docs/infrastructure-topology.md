# Infrastructure Topology

Remote Hosts models infrastructure as a generic directed graph. It can represent physical hosts,
virtual machines, clusters, reverse proxies, middleware, databases, caches, queues, storage, and
business services without introducing one schema or MCP tool family per technology.

The management console at <http://127.0.0.1:8787/admin> combines topology with the canonical host
registry, route health, connectors, and credential metadata.

## Identity and Reconciliation

`POST /v1/topology/sync` accepts one authoritative snapshot for a `scope_key + source` pair.
Repeating the same snapshot is idempotent. A later complete snapshot marks omitted memberships
inactive for that producer scope instead of deleting graph history. Another source may keep the same
node or edge active.

Every real resource needs a globally stable `external_key`, for example:

- `host:<host-id>`
- `cluster:<cluster-name>`
- `service:<cluster-name>:<service-name>`

Use `host_id` to bind a graph node to an existing canonical Host. Do not encode mutable display
names, menu sequence numbers, or ephemeral addresses as identity.

## Snapshot Example

```json
{
  "scope_key": "cluster:factory-a",
  "source": "inventory-agent",
  "nodes": [
    {
      "external_key": "proxy:factory-a",
      "name": "Factory ingress",
      "kind": "reverse_proxy",
      "address": "10.20.0.10",
      "ports": [443],
      "metadata": {"software": "nginx"}
    },
    {
      "external_key": "service:factory-a:api",
      "name": "Factory API",
      "kind": "business_service",
      "address": "10.20.0.21",
      "ports": [8080]
    }
  ],
  "edges": [
    {
      "external_key": "factory-ingress-api",
      "from": "proxy:factory-a",
      "to": "service:factory-a:api",
      "relation": "proxies_to"
    }
  ]
}
```

## Agent Workflow

Remote Hosts does not add Kubernetes-, Harbor-, database-, or middleware-specific MCP tools. Agents
use the relevant CLI through `shell.posix`, `shell.powershell`, or a persistent PTY and normalize a
successful discovery into the topology contract:

1. Read `GET /v1/topology` or `GET /v1/admin/overview`.
2. Discover the complete intended scope through the existing remote CLI.
3. Abort without syncing when discovery is partial, timed out, or ambiguous.
4. Build one complete snapshot with stable node and edge keys for one `scope_key + source`.
5. Submit it to `POST /v1/topology/sync`.
6. Verify active and inactive counts with `GET /v1/topology?include_inactive=true`.

Omitting a resource from an authoritative snapshot makes that producer membership inactive. Never
publish partial discovery results merely to preserve progress.

## Credential Bindings

Topology metadata rejects secret-like keys. Passwords, API tokens, service accounts, private keys,
and database credentials belong in the encrypted node credential binding endpoint or management
form. API responses return credential metadata and bindings, never plaintext.

An API process with the vault unlocked is restricted to loopback. Use an SSH tunnel when the local
management console must be accessed remotely.

## Relevant API Groups

- `GET /v1/topology`
- `POST /v1/topology/sync`
- `GET /v1/topology/credential-bindings`
- `POST /v1/topology/nodes/{node_id}/credentials`
- `GET /v1/admin/overview`

The route definitions in `remote-hosts-api` remain the authoritative request and response contract.

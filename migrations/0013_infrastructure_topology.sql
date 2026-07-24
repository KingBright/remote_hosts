CREATE TABLE topology_nodes (
    id TEXT PRIMARY KEY NOT NULL,
    external_key TEXT NOT NULL UNIQUE,
    host_id TEXT REFERENCES hosts(id),
    name TEXT NOT NULL,
    kind_json TEXT NOT NULL,
    status_json TEXT NOT NULL,
    address TEXT,
    ports_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL
);

CREATE INDEX idx_topology_nodes_host
    ON topology_nodes(host_id);
CREATE INDEX idx_topology_nodes_kind
    ON topology_nodes(kind_json);

CREATE TABLE topology_edges (
    id TEXT PRIMARY KEY NOT NULL,
    external_key TEXT NOT NULL UNIQUE,
    source_node_id TEXT NOT NULL REFERENCES topology_nodes(id),
    target_node_id TEXT NOT NULL REFERENCES topology_nodes(id),
    relation_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    CHECK(source_node_id <> target_node_id)
);

CREATE INDEX idx_topology_edges_source
    ON topology_edges(source_node_id);
CREATE INDEX idx_topology_edges_target
    ON topology_edges(target_node_id);

CREATE TABLE topology_sync_runs (
    id TEXT PRIMARY KEY NOT NULL,
    scope_key TEXT NOT NULL,
    source TEXT NOT NULL,
    active_node_count INTEGER NOT NULL,
    inactive_node_count INTEGER NOT NULL,
    active_edge_count INTEGER NOT NULL,
    inactive_edge_count INTEGER NOT NULL,
    completed_at TEXT NOT NULL
);

CREATE INDEX idx_topology_sync_runs_scope
    ON topology_sync_runs(scope_key, source, completed_at);

CREATE TABLE topology_node_memberships (
    scope_key TEXT NOT NULL,
    source TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES topology_nodes(id),
    last_sync_run_id TEXT NOT NULL REFERENCES topology_sync_runs(id),
    active INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(scope_key, source, node_id)
);

CREATE INDEX idx_topology_node_memberships_active
    ON topology_node_memberships(active, node_id);

CREATE TABLE topology_edge_memberships (
    scope_key TEXT NOT NULL,
    source TEXT NOT NULL,
    edge_id TEXT NOT NULL REFERENCES topology_edges(id),
    last_sync_run_id TEXT NOT NULL REFERENCES topology_sync_runs(id),
    active INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(scope_key, source, edge_id)
);

CREATE INDEX idx_topology_edge_memberships_active
    ON topology_edge_memberships(active, edge_id);

CREATE TABLE credential_bindings (
    id TEXT PRIMARY KEY NOT NULL,
    topology_node_id TEXT NOT NULL REFERENCES topology_nodes(id),
    credential_id TEXT NOT NULL REFERENCES credentials(id),
    purpose TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(topology_node_id, credential_id, purpose)
);

CREATE INDEX idx_credential_bindings_node
    ON credential_bindings(topology_node_id);

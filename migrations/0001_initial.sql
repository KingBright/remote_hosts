CREATE TABLE IF NOT EXISTS hosts (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    kind_json TEXT NOT NULL,
    owner TEXT,
    tags_json TEXT NOT NULL,
    description TEXT,
    risk_level_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    kind_json TEXT NOT NULL,
    description TEXT,
    trust_level_json TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS connectors (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    environment_id TEXT NOT NULL REFERENCES environments(id),
    host_id TEXT REFERENCES hosts(id),
    version TEXT NOT NULL,
    state_json TEXT NOT NULL,
    last_seen_at TEXT,
    current_network TEXT
);

CREATE TABLE IF NOT EXISTS credentials (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    kind_json TEXT NOT NULL,
    username_hint TEXT,
    encrypted_blob_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_used_at TEXT
);

CREATE TABLE IF NOT EXISTS access_paths (
    id TEXT PRIMARY KEY NOT NULL,
    host_id TEXT NOT NULL REFERENCES hosts(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    connector_id TEXT REFERENCES connectors(id),
    protocol_json TEXT NOT NULL,
    address TEXT NOT NULL,
    port INTEGER NOT NULL,
    username TEXT NOT NULL,
    credential_id TEXT NOT NULL REFERENCES credentials(id),
    route_type_json TEXT NOT NULL,
    proxy_chain_json TEXT NOT NULL,
    priority INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    connection_mode_json TEXT NOT NULL,
    idle_ttl_seconds INTEGER NOT NULL,
    keepalive_seconds INTEGER NOT NULL,
    max_concurrent_channels INTEGER NOT NULL,
    max_new_connections_per_minute INTEGER NOT NULL,
    requires_tty INTEGER NOT NULL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_access_paths_host_id ON access_paths(host_id);
CREATE INDEX IF NOT EXISTS idx_access_paths_environment_id ON access_paths(environment_id);

CREATE TABLE IF NOT EXISTS access_path_health (
    access_path_id TEXT PRIMARY KEY NOT NULL REFERENCES access_paths(id),
    state_json TEXT NOT NULL,
    last_checked_at TEXT,
    latency_ms INTEGER,
    failure_count INTEGER NOT NULL,
    last_error_code_json TEXT,
    next_retry_at TEXT
);

CREATE TABLE IF NOT EXISTS host_facts (
    id TEXT PRIMARY KEY NOT NULL,
    host_id TEXT NOT NULL REFERENCES hosts(id),
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    source_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    expires_at TEXT,
    confidence REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_host_facts_host_namespace ON host_facts(host_id, namespace);

CREATE TABLE IF NOT EXISTS software_installs (
    id TEXT PRIMARY KEY NOT NULL,
    host_id TEXT NOT NULL REFERENCES hosts(id),
    name TEXT NOT NULL,
    version TEXT,
    install_path TEXT,
    config_paths_json TEXT NOT NULL,
    service_names_json TEXT NOT NULL,
    ports_json TEXT NOT NULL,
    installed_by_operation_id TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_software_installs_host_id ON software_installs(host_id);

CREATE TABLE IF NOT EXISTS connection_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    access_path_id TEXT NOT NULL REFERENCES access_paths(id),
    connector_id TEXT NOT NULL REFERENCES connectors(id),
    state_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL,
    open_channels INTEGER NOT NULL,
    reused_count INTEGER NOT NULL,
    failure_count INTEGER NOT NULL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS agent_workspaces (
    workspace_id TEXT PRIMARY KEY NOT NULL,
    host_id TEXT NOT NULL REFERENCES hosts(id),
    access_path_id TEXT NOT NULL REFERENCES access_paths(id),
    connector_id TEXT NOT NULL REFERENCES connectors(id),
    label TEXT NOT NULL,
    cwd TEXT,
    state_json TEXT NOT NULL,
    policy_profile TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_activity_at TEXT NOT NULL,
    ttl_seconds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pty_sessions (
    pty_session_id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    session_id TEXT NOT NULL REFERENCES connection_sessions(session_id),
    state_json TEXT NOT NULL,
    foreground_process TEXT,
    cwd TEXT,
    recent_output_ref TEXT,
    last_exit_code INTEGER,
    input_allowed INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    last_activity_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_runs (
    id TEXT PRIMARY KEY NOT NULL,
    host_id TEXT NOT NULL REFERENCES hosts(id),
    access_path_id TEXT NOT NULL REFERENCES access_paths(id),
    connector_id TEXT NOT NULL REFERENCES connectors(id),
    session_id TEXT REFERENCES connection_sessions(session_id),
    operation_type_json TEXT NOT NULL,
    intent TEXT NOT NULL,
    state_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    exit_code INTEGER,
    timeout_seconds INTEGER NOT NULL,
    redacted_command_summary TEXT NOT NULL,
    redacted_output_summary TEXT,
    log_ref TEXT
);

CREATE INDEX IF NOT EXISTS idx_operation_runs_host_started ON operation_runs(host_id, started_at);

CREATE TABLE IF NOT EXISTS knowledge_items (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    source_json TEXT NOT NULL,
    linked_host_ids_json TEXT NOT NULL,
    linked_access_path_ids_json TEXT NOT NULL,
    linked_software_ids_json TEXT NOT NULL,
    linked_operation_ids_json TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_items_fts USING fts5(
    title,
    body,
    tags,
    content=''
);

CREATE TABLE IF NOT EXISTS state_events (
    id TEXT PRIMARY KEY NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    old_state_json TEXT NOT NULL,
    new_state_json TEXT NOT NULL,
    reason_code_json TEXT NOT NULL,
    observed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_state_events_entity_observed
    ON state_events(entity_type, entity_id, observed_at);

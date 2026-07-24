ALTER TABLE operation_runs
ADD COLUMN transport_evidence_json TEXT;

ALTER TABLE pty_sessions
ADD COLUMN transport_evidence_json TEXT;

CREATE TABLE ssh_transport_runtimes (
    access_path_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    backend_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    generation INTEGER NOT NULL,
    connection_attempt_count INTEGER NOT NULL,
    successful_handshake_count INTEGER NOT NULL,
    reuse_count INTEGER NOT NULL,
    last_handshake_at TEXT,
    last_validated_at TEXT,
    capabilities_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (access_path_id, connector_id),
    FOREIGN KEY (access_path_id) REFERENCES access_paths(id),
    FOREIGN KEY (connector_id) REFERENCES connectors(id)
);

CREATE INDEX idx_ssh_transport_runtimes_connector
    ON ssh_transport_runtimes(connector_id, updated_at);

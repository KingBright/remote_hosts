CREATE TABLE IF NOT EXISTS pty_input_events (
    id TEXT PRIMARY KEY NOT NULL,
    pty_session_id TEXT NOT NULL REFERENCES pty_sessions(pty_session_id),
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    connector_id TEXT NOT NULL REFERENCES connectors(id),
    state_json TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    input_text TEXT,
    redacted_input_summary TEXT NOT NULL,
    byte_len INTEGER NOT NULL,
    requested_by TEXT,
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    lease_expires_at TEXT,
    delivered_at TEXT,
    failed_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    last_error TEXT,
    UNIQUE(pty_session_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_pty_input_events_connector_state_created
    ON pty_input_events(connector_id, state_json, created_at, sequence);

CREATE INDEX IF NOT EXISTS idx_pty_input_events_session_sequence
    ON pty_input_events(pty_session_id, sequence);

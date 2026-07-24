CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    client_kind TEXT NOT NULL,
    client_instance_id TEXT NOT NULL,
    project_key TEXT,
    conversation_key TEXT,
    state_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX idx_agent_sessions_client_instance
    ON agent_sessions(client_kind, client_instance_id, last_seen_at);

ALTER TABLE agent_workspaces
ADD COLUMN agent_session_id TEXT REFERENCES agent_sessions(id);

CREATE INDEX idx_agent_workspaces_agent_session_host
    ON agent_workspaces(agent_session_id, host_id, last_activity_at);

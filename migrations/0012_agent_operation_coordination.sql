ALTER TABLE operation_runs
ADD COLUMN agent_session_id TEXT REFERENCES agent_sessions(id);

UPDATE operation_runs
SET agent_session_id = (
    SELECT agent_workspaces.agent_session_id
    FROM agent_workspaces
    WHERE agent_workspaces.workspace_id = operation_runs.workspace_id
);

ALTER TABLE operation_runs
ADD COLUMN idempotency_key TEXT;

ALTER TABLE operation_runs
ADD COLUMN requires_write_lease INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX idx_operation_runs_agent_idempotency
    ON operation_runs(agent_session_id, idempotency_key)
    WHERE agent_session_id IS NOT NULL AND idempotency_key IS NOT NULL;

CREATE INDEX idx_operation_runs_host_write_queue
    ON operation_runs(host_id, requires_write_lease, state_json, started_at);

ALTER TABLE pty_input_events
ADD COLUMN host_id TEXT REFERENCES hosts(id);

UPDATE pty_input_events
SET host_id = (
    SELECT agent_workspaces.host_id
    FROM agent_workspaces
    WHERE agent_workspaces.workspace_id = pty_input_events.workspace_id
);

ALTER TABLE pty_input_events
ADD COLUMN agent_session_id TEXT REFERENCES agent_sessions(id);

UPDATE pty_input_events
SET agent_session_id = (
    SELECT agent_workspaces.agent_session_id
    FROM agent_workspaces
    WHERE agent_workspaces.workspace_id = pty_input_events.workspace_id
);

ALTER TABLE pty_input_events
ADD COLUMN idempotency_key TEXT;

ALTER TABLE pty_input_events
ADD COLUMN input_fingerprint TEXT;

CREATE UNIQUE INDEX idx_pty_input_events_agent_idempotency
    ON pty_input_events(agent_session_id, idempotency_key)
    WHERE agent_session_id IS NOT NULL AND idempotency_key IS NOT NULL;

CREATE TABLE host_write_leases (
    host_id TEXT PRIMARY KEY NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    holder_agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    holder_workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX idx_host_write_leases_expiry
    ON host_write_leases(expires_at);

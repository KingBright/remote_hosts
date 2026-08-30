ALTER TABLE state_events
    ADD COLUMN agent_session_id TEXT REFERENCES agent_sessions(id);

ALTER TABLE state_events
    ADD COLUMN host_id TEXT REFERENCES hosts(id);

ALTER TABLE state_events
    ADD COLUMN workspace_id TEXT REFERENCES agent_workspaces(workspace_id);

ALTER TABLE state_events
    ADD COLUMN lifecycle_kind TEXT;

ALTER TABLE state_events
    ADD COLUMN lifecycle_state TEXT;

CREATE INDEX IF NOT EXISTS idx_state_events_agent_session_sequence
    ON state_events(agent_session_id, sequence);

CREATE INDEX IF NOT EXISTS idx_state_events_host_sequence
    ON state_events(host_id, sequence);

CREATE INDEX IF NOT EXISTS idx_state_events_workspace_sequence
    ON state_events(workspace_id, sequence);

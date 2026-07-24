ALTER TABLE operation_runs
    ADD COLUMN workspace_id TEXT REFERENCES agent_workspaces(workspace_id);

ALTER TABLE operation_runs
    ADD COLUMN command_profile_json TEXT;

CREATE INDEX IF NOT EXISTS idx_operation_runs_workspace_started
    ON operation_runs(workspace_id, started_at);

CREATE TABLE IF NOT EXISTS operation_output_chunks (
    id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operation_runs(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    stream_json TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    redacted_text TEXT NOT NULL,
    byte_len INTEGER NOT NULL,
    truncated INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(operation_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_operation_output_chunks_workspace_created
    ON operation_output_chunks(workspace_id, created_at, sequence);

CREATE INDEX IF NOT EXISTS idx_operation_output_chunks_operation_sequence
    ON operation_output_chunks(operation_id, sequence);

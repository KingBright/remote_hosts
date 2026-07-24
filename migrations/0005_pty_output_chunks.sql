CREATE TABLE IF NOT EXISTS pty_output_chunks (
    id TEXT PRIMARY KEY NOT NULL,
    pty_session_id TEXT NOT NULL REFERENCES pty_sessions(pty_session_id),
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    stream_json TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    redacted_text TEXT NOT NULL,
    byte_len INTEGER NOT NULL,
    truncated INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pty_output_chunks_session_sequence
    ON pty_output_chunks(pty_session_id, sequence);

CREATE INDEX IF NOT EXISTS idx_pty_output_chunks_workspace_created
    ON pty_output_chunks(workspace_id, created_at, sequence);

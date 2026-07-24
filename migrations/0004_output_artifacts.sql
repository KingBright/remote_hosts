CREATE TABLE IF NOT EXISTS operation_output_artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operation_runs(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    stream_json TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    byte_len INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    redacted_preview TEXT NOT NULL,
    truncated INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_output_artifacts_workspace_created
    ON operation_output_artifacts(workspace_id, created_at);

CREATE INDEX IF NOT EXISTS idx_operation_output_artifacts_operation
    ON operation_output_artifacts(operation_id);

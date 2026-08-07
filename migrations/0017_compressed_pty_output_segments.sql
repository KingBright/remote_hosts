CREATE TABLE IF NOT EXISTS pty_output_segments (
    segment_id INTEGER PRIMARY KEY,
    pty_session_id TEXT NOT NULL REFERENCES pty_sessions(pty_session_id),
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    encoding TEXT NOT NULL,
    original_text_byte_len INTEGER NOT NULL,
    uncompressed_byte_len INTEGER NOT NULL,
    compressed_byte_len INTEGER NOT NULL,
    payload BLOB NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (first_sequence <= last_sequence),
    CHECK (chunk_count > 0),
    CHECK (original_text_byte_len > 0),
    CHECK (uncompressed_byte_len > 0),
    CHECK (compressed_byte_len > 0),
    CHECK (length(payload) = compressed_byte_len),
    UNIQUE (pty_session_id, first_sequence)
);

CREATE INDEX IF NOT EXISTS idx_pty_output_segments_session_range
    ON pty_output_segments(pty_session_id, last_sequence, first_sequence);

CREATE TABLE IF NOT EXISTS operation_output_segments (
    segment_id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES operation_runs(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    encoding TEXT NOT NULL,
    original_text_byte_len INTEGER NOT NULL,
    uncompressed_byte_len INTEGER NOT NULL,
    compressed_byte_len INTEGER NOT NULL,
    payload BLOB NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (first_sequence <= last_sequence),
    CHECK (chunk_count > 0),
    CHECK (original_text_byte_len > 0),
    CHECK (uncompressed_byte_len > 0),
    CHECK (compressed_byte_len > 0),
    CHECK (length(payload) = compressed_byte_len),
    UNIQUE (operation_id, first_sequence)
);

CREATE INDEX IF NOT EXISTS idx_operation_output_segments_operation_range
    ON operation_output_segments(operation_id, last_sequence, first_sequence);

CREATE INDEX IF NOT EXISTS idx_operation_output_segments_workspace_created
    ON operation_output_segments(workspace_id, created_at, first_sequence);

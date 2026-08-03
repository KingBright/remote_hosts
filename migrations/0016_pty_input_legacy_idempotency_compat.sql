-- Keep older MCP clients usable while they are still attached to the same
-- local database during a rolling Remote Hosts service upgrade.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pty_input_events_pty_idempotency_compat
    ON pty_input_events(pty_session_id, idempotency_key);

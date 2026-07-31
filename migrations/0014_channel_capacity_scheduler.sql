CREATE TABLE IF NOT EXISTS system_settings (
    setting_key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO system_settings(setting_key, value)
VALUES (
    'legacy_channel_default_v1',
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM access_paths
            WHERE max_concurrent_channels = 1
        )
        THEN 'pending'
        ELSE 'done'
    END
);

CREATE INDEX IF NOT EXISTS idx_operation_runs_access_path_capacity
    ON operation_runs(access_path_id, state_json, lease_expires_at, id);

CREATE INDEX IF NOT EXISTS idx_agent_workspaces_access_path_state
    ON agent_workspaces(access_path_id, state_json, workspace_id);

CREATE INDEX IF NOT EXISTS idx_pty_sessions_channel_capacity
    ON pty_sessions(backend_state_json, input_allowed, state_json, workspace_id);

ALTER TABLE agent_workspaces
ADD COLUMN coordination_scope TEXT NOT NULL DEFAULT 'host';

ALTER TABLE operation_runs
ADD COLUMN coordination_scope TEXT NOT NULL DEFAULT 'host';

UPDATE operation_runs
SET coordination_scope = COALESCE(
    (
        SELECT agent_workspaces.coordination_scope
        FROM agent_workspaces
        WHERE agent_workspaces.workspace_id = operation_runs.workspace_id
    ),
    'host'
);

ALTER TABLE host_write_leases RENAME TO host_write_leases_legacy;

CREATE TABLE host_write_leases (
    host_id TEXT NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    coordination_scope TEXT NOT NULL,
    holder_agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    holder_workspace_id TEXT NOT NULL REFERENCES agent_workspaces(workspace_id),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (host_id, coordination_scope)
);

INSERT INTO host_write_leases (
    host_id, coordination_scope, holder_agent_session_id, holder_workspace_id,
    acquired_at, heartbeat_at, expires_at
)
SELECT
    host_id, 'host', holder_agent_session_id, holder_workspace_id,
    acquired_at, heartbeat_at, expires_at
FROM host_write_leases_legacy;

DROP TABLE host_write_leases_legacy;

CREATE INDEX idx_host_write_leases_expiry
    ON host_write_leases(expires_at);

CREATE INDEX idx_host_write_leases_scope
    ON host_write_leases(host_id, coordination_scope, expires_at);

DROP INDEX IF EXISTS idx_operation_runs_host_write_queue;

CREATE INDEX idx_operation_runs_host_write_queue
    ON operation_runs(
        host_id, coordination_scope, requires_write_lease, state_json, started_at
    );

ALTER TABLE operation_runs
ADD COLUMN coordination_scopes_json TEXT NOT NULL DEFAULT '[]';

UPDATE operation_runs
SET coordination_scopes_json = json_array(coordination_scope)
WHERE coordination_scopes_json = '[]';

ALTER TABLE pty_sessions
ADD COLUMN coordination_scopes_json TEXT NOT NULL DEFAULT '[]';

UPDATE pty_sessions
SET coordination_scopes_json = COALESCE(
    (
        SELECT json_array(agent_workspaces.coordination_scope)
        FROM agent_workspaces
        WHERE agent_workspaces.workspace_id = pty_sessions.workspace_id
    ),
    json_array('host')
)
WHERE coordination_scopes_json = '[]';

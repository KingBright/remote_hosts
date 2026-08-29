DELETE FROM host_write_leases
WHERE holder_workspace_id IN (
    SELECT closed_workspace.workspace_id
    FROM agent_workspaces closed_workspace
    WHERE closed_workspace.state_json = '"closed"'
      AND EXISTS (
          SELECT 1
          FROM operation_runs active_operation
          WHERE active_operation.workspace_id = closed_workspace.workspace_id
            AND active_operation.state_json IN ('"queued"', '"running"')
      )
);

UPDATE operation_runs
SET state_json = '"cancelled"',
    finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    exit_code = NULL,
    redacted_output_summary = 'cancelled because Workspace was closed',
    last_error = 'workspace_closed',
    claim_token = NULL,
    claimed_at = NULL,
    lease_expires_at = NULL
WHERE state_json IN ('"queued"', '"running"')
  AND workspace_id IN (
      SELECT workspace_id
      FROM agent_workspaces
      WHERE state_json = '"closed"'
  );

CREATE TRIGGER IF NOT EXISTS trg_workspace_close_cancels_operations_and_leases
AFTER UPDATE OF state_json ON agent_workspaces
WHEN NEW.state_json = '"closed"'
BEGIN
    DELETE FROM host_write_leases
    WHERE holder_workspace_id = NEW.workspace_id
      AND EXISTS (
          SELECT 1
          FROM operation_runs active_operation
          WHERE active_operation.workspace_id = NEW.workspace_id
            AND active_operation.state_json IN ('"queued"', '"running"')
      );

    UPDATE operation_runs
    SET state_json = '"cancelled"',
        finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
        exit_code = NULL,
        redacted_output_summary = 'cancelled because Workspace was closed',
        last_error = 'workspace_closed',
        claim_token = NULL,
        claimed_at = NULL,
        lease_expires_at = NULL
    WHERE workspace_id = NEW.workspace_id
      AND state_json IN ('"queued"', '"running"');
END;

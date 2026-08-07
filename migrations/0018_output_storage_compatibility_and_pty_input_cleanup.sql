INSERT OR IGNORE INTO system_settings(setting_key, value)
VALUES ('compressed_output_writes_v1', 'disabled');

UPDATE pty_input_events
SET state_json = '"failed"',
    input_text = NULL,
    failed_at = COALESCE(failed_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    claim_token = NULL,
    lease_expires_at = NULL,
    last_error = 'pty_input_delivery_unavailable'
WHERE state_json IN ('"queued"', '"claimed"')
  AND EXISTS (
      SELECT 1
      FROM pty_sessions ps
      JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
      WHERE ps.pty_session_id = pty_input_events.pty_session_id
        AND (
            ps.input_allowed = 0
            OR ps.backend_state_json IN ('"failed"', '"closed"')
            OR ps.state_json NOT IN ('"idle"', '"working"')
            OR aw.state_json NOT IN ('"idle"', '"working"')
        )
  );

CREATE TRIGGER IF NOT EXISTS trg_pty_terminal_fails_pending_input
AFTER UPDATE OF input_allowed, backend_state_json, state_json ON pty_sessions
WHEN NEW.input_allowed = 0
  OR NEW.backend_state_json IN ('"failed"', '"closed"')
  OR NEW.state_json NOT IN ('"idle"', '"working"')
BEGIN
    UPDATE pty_input_events
    SET state_json = '"failed"',
        input_text = NULL,
        failed_at = COALESCE(failed_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        claim_token = NULL,
        lease_expires_at = NULL,
        last_error = 'pty_input_delivery_unavailable'
    WHERE pty_session_id = NEW.pty_session_id
      AND state_json IN ('"queued"', '"claimed"');
END;

CREATE TRIGGER IF NOT EXISTS trg_workspace_terminal_fails_pending_pty_input
AFTER UPDATE OF state_json ON agent_workspaces
WHEN NEW.state_json NOT IN ('"idle"', '"working"')
BEGIN
    UPDATE pty_input_events
    SET state_json = '"failed"',
        input_text = NULL,
        failed_at = COALESCE(failed_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        claim_token = NULL,
        lease_expires_at = NULL,
        last_error = 'pty_input_delivery_unavailable'
    WHERE workspace_id = NEW.workspace_id
      AND state_json IN ('"queued"', '"claimed"');
END;

CREATE TRIGGER IF NOT EXISTS trg_undeliverable_pty_input_insert
AFTER INSERT ON pty_input_events
WHEN NEW.state_json IN ('"queued"', '"claimed"')
  AND EXISTS (
      SELECT 1
      FROM pty_sessions ps
      JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
      WHERE ps.pty_session_id = NEW.pty_session_id
        AND (
            ps.input_allowed = 0
            OR ps.backend_state_json IN ('"failed"', '"closed"')
            OR ps.state_json NOT IN ('"idle"', '"working"')
            OR aw.state_json NOT IN ('"idle"', '"working"')
        )
  )
BEGIN
    UPDATE pty_input_events
    SET state_json = '"failed"',
        input_text = NULL,
        failed_at = COALESCE(failed_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        claim_token = NULL,
        lease_expires_at = NULL,
        last_error = 'pty_input_delivery_unavailable'
    WHERE id = NEW.id;
END;

ALTER TABLE pty_sessions
ADD COLUMN interaction_json TEXT;

DROP TRIGGER IF EXISTS trg_workspace_terminal_fails_pending_pty_input;

CREATE TRIGGER IF NOT EXISTS trg_workspace_terminal_fails_pending_pty_input
AFTER UPDATE OF state_json ON agent_workspaces
WHEN NEW.state_json NOT IN ('"idle"', '"working"', '"blocked"')
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

DROP TRIGGER IF EXISTS trg_undeliverable_pty_input_insert;

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
            OR aw.state_json NOT IN ('"idle"', '"working"', '"blocked"')
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

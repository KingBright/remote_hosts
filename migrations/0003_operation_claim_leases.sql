ALTER TABLE operation_runs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE operation_runs
    ADD COLUMN claim_token TEXT;

ALTER TABLE operation_runs
    ADD COLUMN claimed_at TEXT;

ALTER TABLE operation_runs
    ADD COLUMN lease_expires_at TEXT;

ALTER TABLE operation_runs
    ADD COLUMN last_error TEXT;

CREATE INDEX IF NOT EXISTS idx_operation_runs_connector_claim
    ON operation_runs(connector_id, state_json, lease_expires_at, started_at);

CREATE TABLE authorized_key_bootstrap (
    access_path_id TEXT PRIMARY KEY NOT NULL REFERENCES access_paths(id) ON DELETE CASCADE,
    state_json TEXT NOT NULL,
    reason_json TEXT,
    public_key_fingerprint TEXT,
    failure_count INTEGER NOT NULL,
    attempted_at TEXT NOT NULL,
    next_retry_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_authorized_key_bootstrap_retry
    ON authorized_key_bootstrap(state_json, next_retry_at);

CREATE TABLE state_events_v2 (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    old_state_json TEXT NOT NULL,
    new_state_json TEXT NOT NULL,
    reason_code_json TEXT NOT NULL,
    observed_at TEXT NOT NULL
);

INSERT INTO state_events_v2 (
    id,
    entity_type,
    entity_id,
    old_state_json,
    new_state_json,
    reason_code_json,
    observed_at
)
SELECT
    id,
    entity_type,
    entity_id,
    old_state_json,
    new_state_json,
    reason_code_json,
    observed_at
FROM state_events
ORDER BY observed_at ASC, id ASC;

DROP TABLE state_events;
ALTER TABLE state_events_v2 RENAME TO state_events;

CREATE INDEX idx_state_events_entity_observed
    ON state_events(entity_type, entity_id, observed_at);

CREATE INDEX idx_state_events_entity_sequence
    ON state_events(entity_type, entity_id, sequence);

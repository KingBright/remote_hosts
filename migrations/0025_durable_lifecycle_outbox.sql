CREATE TABLE lifecycle_outbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    agent_session_id TEXT REFERENCES agent_sessions(id),
    host_id TEXT REFERENCES hosts(id),
    workspace_id TEXT REFERENCES agent_workspaces(workspace_id),
    lifecycle_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    published_at TEXT,
    publish_attempt_count INTEGER NOT NULL DEFAULT 0,
    last_publish_error TEXT
);

CREATE INDEX idx_lifecycle_outbox_agent_sequence
    ON lifecycle_outbox(agent_session_id, sequence);

CREATE INDEX idx_lifecycle_outbox_host_sequence
    ON lifecycle_outbox(host_id, sequence);

CREATE INDEX idx_lifecycle_outbox_pending_sequence
    ON lifecycle_outbox(published_at, sequence);

CREATE INDEX idx_lifecycle_outbox_entity_pending
    ON lifecycle_outbox(lifecycle_kind, entity_id, published_at, sequence);

CREATE TABLE agent_work_context_cursors (
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    host_filter TEXT NOT NULL,
    acknowledged_sequence INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    CHECK (acknowledged_sequence >= 0),
    PRIMARY KEY (agent_session_id, host_filter)
);

ALTER TABLE state_events
    ADD COLUMN lifecycle_outbox_sequence INTEGER REFERENCES lifecycle_outbox(sequence);

CREATE UNIQUE INDEX idx_state_events_lifecycle_outbox_sequence
    ON state_events(lifecycle_outbox_sequence)
    WHERE lifecycle_outbox_sequence IS NOT NULL;

INSERT INTO lifecycle_outbox (
    event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
    lifecycle_state, observed_at, published_at, publish_attempt_count
)
SELECT
    id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
    lifecycle_state, observed_at, observed_at, 1
FROM state_events
WHERE lifecycle_kind IS NOT NULL
ORDER BY sequence ASC;

CREATE TRIGGER trg_connection_lifecycle_outbox_insert
AFTER INSERT ON connection_sessions
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, host_id, lifecycle_kind, entity_id, lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))),
        (SELECT host_id FROM access_paths WHERE id = NEW.access_path_id),
        'connection', NEW.session_id, json_extract(NEW.state_json, '$'), NEW.last_used_at
    );
END;

CREATE TRIGGER trg_connection_lifecycle_outbox_update
AFTER UPDATE OF state_json ON connection_sessions
WHEN OLD.state_json IS NOT NEW.state_json
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, host_id, lifecycle_kind, entity_id, lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))),
        (SELECT host_id FROM access_paths WHERE id = NEW.access_path_id),
        'connection', NEW.session_id, json_extract(NEW.state_json, '$'), NEW.last_used_at
    );
END;

CREATE TRIGGER trg_workspace_lifecycle_outbox_insert
AFTER INSERT ON agent_workspaces
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        'workspace', NEW.workspace_id, json_extract(NEW.state_json, '$'), NEW.last_activity_at
    );
END;

CREATE TRIGGER trg_workspace_lifecycle_outbox_update
AFTER UPDATE OF state_json ON agent_workspaces
WHEN OLD.state_json IS NOT NEW.state_json
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        'workspace', NEW.workspace_id, json_extract(NEW.state_json, '$'), NEW.last_activity_at
    );
END;

CREATE TRIGGER trg_operation_lifecycle_outbox_insert
AFTER INSERT ON operation_runs
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        CASE json_extract(NEW.operation_type_json, '$')
            WHEN 'sftp' THEN 'transfer' ELSE 'operation'
        END,
        NEW.id, json_extract(NEW.state_json, '$'),
        COALESCE(NEW.finished_at, NEW.claimed_at, NEW.started_at)
    );
END;

CREATE TRIGGER trg_operation_lifecycle_outbox_update
AFTER UPDATE OF state_json ON operation_runs
WHEN OLD.state_json IS NOT NEW.state_json
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        CASE json_extract(NEW.operation_type_json, '$')
            WHEN 'sftp' THEN 'transfer' ELSE 'operation'
        END,
        NEW.id, json_extract(NEW.state_json, '$'),
        COALESCE(NEW.finished_at, NEW.claimed_at, NEW.started_at)
    );
END;

CREATE TRIGGER trg_pty_lifecycle_outbox_insert
AFTER INSERT ON pty_sessions
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    )
    SELECT
        lower(hex(randomblob(16))), aw.agent_session_id, aw.host_id, aw.workspace_id,
        'pty', NEW.pty_session_id,
        CASE
            WHEN NEW.interaction_json IS NOT NULL
                 AND json_extract(NEW.backend_state_json, '$') = 'active'
                 AND NEW.input_allowed = 1
            THEN 'needs_input'
            ELSE COALESCE(json_extract(NEW.backend_state_json, '$'), 'unknown')
        END,
        NEW.last_activity_at
    FROM agent_workspaces aw
    WHERE aw.workspace_id = NEW.workspace_id;
END;

CREATE TRIGGER trg_pty_lifecycle_outbox_update
AFTER UPDATE OF state_json, backend_state_json, interaction_json, input_allowed ON pty_sessions
WHEN OLD.state_json IS NOT NEW.state_json
  OR OLD.backend_state_json IS NOT NEW.backend_state_json
  OR OLD.interaction_json IS NOT NEW.interaction_json
  OR OLD.input_allowed IS NOT NEW.input_allowed
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    )
    SELECT
        lower(hex(randomblob(16))), aw.agent_session_id, aw.host_id, aw.workspace_id,
        'pty', NEW.pty_session_id,
        CASE
            WHEN NEW.interaction_json IS NOT NULL
                 AND json_extract(NEW.backend_state_json, '$') = 'active'
                 AND NEW.input_allowed = 1
            THEN 'needs_input'
            ELSE COALESCE(json_extract(NEW.backend_state_json, '$'), 'unknown')
        END,
        NEW.last_activity_at
    FROM agent_workspaces aw
    WHERE aw.workspace_id = NEW.workspace_id;
END;

CREATE TRIGGER trg_pty_input_lifecycle_outbox_insert
AFTER INSERT ON pty_input_events
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        'input', NEW.id, json_extract(NEW.state_json, '$'),
        COALESCE(NEW.delivered_at, NEW.failed_at, NEW.claimed_at, NEW.created_at)
    );
END;

CREATE TRIGGER trg_pty_input_lifecycle_outbox_update
AFTER UPDATE OF state_json ON pty_input_events
WHEN OLD.state_json IS NOT NEW.state_json
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    ) VALUES (
        lower(hex(randomblob(16))), NEW.agent_session_id, NEW.host_id, NEW.workspace_id,
        'input', NEW.id, json_extract(NEW.state_json, '$'),
        COALESCE(NEW.delivered_at, NEW.failed_at, NEW.claimed_at, NEW.created_at)
    );
END;

CREATE TRIGGER trg_transfer_chunk_lifecycle_outbox_insert
AFTER INSERT ON operation_output_chunks
WHEN EXISTS (
    SELECT 1 FROM operation_runs op
    WHERE op.id = NEW.operation_id AND json_extract(op.operation_type_json, '$') = 'sftp'
)
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    )
    SELECT
        lower(hex(randomblob(16))), op.agent_session_id, op.host_id, op.workspace_id,
        'transfer', op.id, 'progress', NEW.created_at
    FROM operation_runs op
    WHERE op.id = NEW.operation_id;
END;

CREATE TRIGGER trg_transfer_segment_lifecycle_outbox_insert
AFTER INSERT ON operation_output_segments
WHEN EXISTS (
    SELECT 1 FROM operation_runs op
    WHERE op.id = NEW.operation_id AND json_extract(op.operation_type_json, '$') = 'sftp'
)
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    )
    SELECT
        lower(hex(randomblob(16))), op.agent_session_id, op.host_id, op.workspace_id,
        'transfer', op.id, 'progress', NEW.created_at
    FROM operation_runs op
    WHERE op.id = NEW.operation_id;
END;

CREATE TRIGGER trg_transfer_segment_lifecycle_outbox_update
AFTER UPDATE OF last_sequence, chunk_count ON operation_output_segments
WHEN OLD.last_sequence IS NOT NEW.last_sequence OR OLD.chunk_count IS NOT NEW.chunk_count
BEGIN
    INSERT INTO lifecycle_outbox (
        event_id, agent_session_id, host_id, workspace_id, lifecycle_kind, entity_id,
        lifecycle_state, observed_at
    )
    SELECT
        lower(hex(randomblob(16))), op.agent_session_id, op.host_id, op.workspace_id,
        'transfer', op.id, 'progress', NEW.created_at
    FROM operation_runs op
    WHERE op.id = NEW.operation_id AND json_extract(op.operation_type_json, '$') = 'sftp';
END;

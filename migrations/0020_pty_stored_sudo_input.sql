ALTER TABLE pty_input_events
ADD COLUMN payload_kind_json TEXT NOT NULL DEFAULT '"text"';

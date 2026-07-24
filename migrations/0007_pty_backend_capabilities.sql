ALTER TABLE pty_sessions
ADD COLUMN backend_state_json TEXT;

ALTER TABLE pty_sessions
ADD COLUMN backend_capabilities_json TEXT;

UPDATE pty_sessions
SET backend_state_json = '"unknown"'
WHERE backend_state_json IS NULL;

UPDATE pty_sessions
SET backend_capabilities_json = '{"kind":"unknown","terminal_semantics":"unknown","allocates_tty":false,"reuses_ssh_transport":false,"supports_window_resize":false,"supports_signal":false,"supports_streaming_input":false,"supports_streaming_output":false}'
WHERE backend_capabilities_json IS NULL;

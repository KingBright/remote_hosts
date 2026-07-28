//! Persistent PTY session lifecycle supervision.

use remote_hosts_domain::{
    AgentWorkspace, ConnectionSession, EntityState, PtyBackendCapabilities, PtyBackendState,
    PtyInputEvent, PtyInputEventId, PtyInputEventState, PtySession, PtySessionId, SessionId,
    WorkspaceState, now_utc,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ProtectionDecision, ServerProtectionPolicy};

/// Request to open a persistent PTY record for a workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtySessionOpenCommand {
    /// Connection session backing the PTY channel.
    pub session_id: SessionId,
    /// Initial current working directory.
    pub cwd: Option<String>,
}

/// Request to update a PTY heartbeat.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtySessionHeartbeatCommand {
    /// Visible PTY state.
    pub state: WorkspaceState,
    /// Foreground process summary.
    pub foreground_process: Option<String>,
    /// Current working directory.
    pub cwd: Option<String>,
    /// Recent output artifact reference.
    pub recent_output_ref: Option<String>,
    /// Last foreground process exit code.
    pub last_exit_code: Option<i32>,
    /// Whether input is currently allowed.
    pub input_allowed: bool,
}

/// Request to enqueue input for a persistent PTY session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtySessionInputCommand {
    /// Raw input text to send through the connector-owned PTY backend.
    pub input: String,
    /// Optional requester label for audit.
    pub requested_by: Option<String>,
    /// Optional retry key scoped to the workspace's agent session.
    pub idempotency_key: Option<String>,
}

/// Plan returned after PTY input passes policy and can be queued.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtySessionInputPlan {
    /// Public input event metadata.
    pub event: PtyInputEvent,
    /// Raw input payload for database queue storage. Do not expose this through API/MCP.
    pub input_text: String,
}

/// PTY lifecycle errors.
#[derive(Debug, Error)]
pub enum PtySessionSupervisorError {
    /// Policy rejected PTY creation.
    #[error("pty session denied by policy: {0:?}")]
    PolicyDenied(ProtectionDecision),
    /// Workspace cannot accept a PTY in its current state.
    #[error("workspace state does not allow pty lifecycle operation: {0:?}")]
    WorkspaceUnavailable(WorkspaceState),
    /// Backing connection session does not belong to the workspace.
    #[error("connection session does not match workspace access path or connector")]
    ConnectionMismatch,
    /// Backing connection is not connected.
    #[error("connection session is not connected")]
    ConnectionUnavailable,
    /// PTY session does not belong to the workspace.
    #[error("pty session does not belong to workspace")]
    SessionWorkspaceMismatch,
    /// PTY session cannot accept input in its current state.
    #[error("pty session state does not allow input: {0:?}")]
    InputUnavailable(WorkspaceState),
    /// PTY session explicitly disallows input.
    #[error("pty session input is not currently allowed")]
    InputNotAllowed,
    /// PTY input payload is invalid.
    #[error("pty input must be non-empty, at most {0} bytes, and must not contain NUL bytes")]
    InvalidInput(usize),
    /// Requested-by label is invalid.
    #[error("pty input requester label must be at most 128 visible characters")]
    InvalidRequestedBy,
    /// Working directory is invalid.
    #[error("pty cwd must not contain control characters")]
    InvalidCwd,
    /// Foreground process summary is invalid.
    #[error("pty foreground process summary must be at most 200 visible characters")]
    InvalidForegroundProcess,
    /// Recent output reference is invalid.
    #[error("pty output reference must be at most 512 visible characters")]
    InvalidOutputRef,
}

/// Supervises persistent PTY session records under a server protection policy.
#[derive(Clone, Debug)]
pub struct PtySessionSupervisor {
    policy: ServerProtectionPolicy,
}

impl PtySessionSupervisor {
    /// Creates a PTY supervisor from a policy.
    pub fn new(policy: ServerProtectionPolicy) -> Self {
        Self { policy }
    }

    /// Opens a new PTY session record.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace, connection, or policy does not allow opening.
    pub fn open_session(
        &self,
        workspace: &AgentWorkspace,
        connection: &ConnectionSession,
        active_ptys: u32,
        command: PtySessionOpenCommand,
    ) -> Result<PtySession, PtySessionSupervisorError> {
        ensure_workspace_accepts_pty(workspace)?;
        validate_cwd(command.cwd.as_deref())?;
        if connection.session_id != command.session_id
            || connection.access_path_id != workspace.access_path_id
            || connection.connector_id != workspace.connector_id
        {
            return Err(PtySessionSupervisorError::ConnectionMismatch);
        }
        if !matches!(
            connection.state,
            EntityState::Resolving | EntityState::Connected | EntityState::Healthy
        ) {
            return Err(PtySessionSupervisorError::ConnectionUnavailable);
        }

        let decision = self.policy.decide(0, 0, 0, active_ptys, false);
        if !decision.allowed {
            return Err(PtySessionSupervisorError::PolicyDenied(decision));
        }

        let now = now_utc();
        Ok(PtySession {
            pty_session_id: PtySessionId::new(),
            workspace_id: workspace.id,
            session_id: connection.session_id,
            state: WorkspaceState::Idle,
            foreground_process: None,
            cwd: command.cwd.or_else(|| workspace.cwd.clone()),
            recent_output_ref: None,
            last_exit_code: None,
            input_allowed: true,
            backend_state: PtyBackendState::Pending,
            backend_capabilities: PtyBackendCapabilities::unknown(),
            transport_evidence: None,
            created_at: now,
            last_activity_at: now,
        })
    }

    /// Validates and plans a connector-owned PTY input queue event.
    ///
    /// # Errors
    ///
    /// Returns an error when the PTY, workspace, or input policy does not allow input.
    pub fn queue_input(
        &self,
        session: &PtySession,
        workspace: &AgentWorkspace,
        next_sequence: u64,
        command: PtySessionInputCommand,
    ) -> Result<PtySessionInputPlan, PtySessionSupervisorError> {
        if session.workspace_id != workspace.id {
            return Err(PtySessionSupervisorError::SessionWorkspaceMismatch);
        }
        if matches!(
            workspace.state,
            WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
        ) {
            return Err(PtySessionSupervisorError::WorkspaceUnavailable(
                workspace.state.clone(),
            ));
        }
        if matches!(
            session.state,
            WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
        ) {
            return Err(PtySessionSupervisorError::InputUnavailable(
                session.state.clone(),
            ));
        }
        if !session.input_allowed {
            return Err(PtySessionSupervisorError::InputNotAllowed);
        }
        validate_input(&command.input, self.policy.max_pty_input_bytes)?;
        validate_visible_text(command.requested_by.as_deref(), 128)
            .map_err(|()| PtySessionSupervisorError::InvalidRequestedBy)?;

        let now = now_utc();
        let byte_len = u64::try_from(command.input.len()).unwrap_or(u64::MAX);
        Ok(PtySessionInputPlan {
            event: PtyInputEvent {
                id: PtyInputEventId::new(),
                pty_session_id: session.pty_session_id,
                workspace_id: session.workspace_id,
                connector_id: workspace.connector_id,
                host_id: workspace.host_id,
                agent_session_id: workspace.agent_session_id,
                idempotency_key: command.idempotency_key,
                input_fingerprint: Some(format!("{:x}", Sha256::digest(command.input.as_bytes()))),
                state: PtyInputEventState::Queued,
                sequence: next_sequence,
                redacted_input_summary: format!("{byte_len} bytes queued for pty input"),
                byte_len,
                requested_by: command.requested_by,
                created_at: now,
                claimed_at: None,
                lease_expires_at: None,
                delivered_at: None,
                failed_at: None,
                attempt_count: 0,
                last_error: None,
            },
            input_text: command.input,
        })
    }

    /// Applies a PTY heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error when the heartbeat carries invalid visible metadata.
    pub fn heartbeat(
        &self,
        mut session: PtySession,
        command: PtySessionHeartbeatCommand,
    ) -> Result<PtySession, PtySessionSupervisorError> {
        validate_cwd(command.cwd.as_deref())?;
        validate_visible_text(command.foreground_process.as_deref(), 200)
            .map_err(|()| PtySessionSupervisorError::InvalidForegroundProcess)?;
        validate_visible_text(command.recent_output_ref.as_deref(), 512)
            .map_err(|()| PtySessionSupervisorError::InvalidOutputRef)?;

        session.state = command.state;
        session.foreground_process = command.foreground_process;
        session.cwd = command.cwd.or(session.cwd);
        session.recent_output_ref = command.recent_output_ref;
        session.last_exit_code = command.last_exit_code;
        session.input_allowed = command.input_allowed
            && !matches!(
                session.state,
                WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
            );
        session.last_activity_at = now_utc();
        Ok(session)
    }

    /// Closes a PTY session record and disables future input.
    pub fn close(&self, mut session: PtySession, last_exit_code: Option<i32>) -> PtySession {
        session.state = WorkspaceState::Closed;
        session.input_allowed = false;
        session.foreground_process = None;
        session.backend_state = PtyBackendState::Closed;
        session.last_exit_code = last_exit_code.or(session.last_exit_code);
        session.last_activity_at = now_utc();
        session
    }

    /// Closes a PTY session if its idle TTL has elapsed.
    pub fn reap_expired(
        &self,
        session: PtySession,
        now: time::OffsetDateTime,
        idle_ttl_seconds: u64,
    ) -> (PtySession, bool) {
        if matches!(session.state, WorkspaceState::Closed)
            || now
                < session.last_activity_at
                    + time::Duration::seconds(i64::try_from(idle_ttl_seconds).unwrap_or(i64::MAX))
        {
            return (session, false);
        }

        let mut closed = session;
        closed.state = WorkspaceState::Closed;
        closed.input_allowed = false;
        closed.foreground_process = None;
        closed.backend_state = PtyBackendState::Closed;
        closed.last_activity_at = now;
        (closed, true)
    }
}

impl Default for PtySessionSupervisor {
    fn default() -> Self {
        Self::new(ServerProtectionPolicy::default())
    }
}

fn ensure_workspace_accepts_pty(
    workspace: &AgentWorkspace,
) -> Result<(), PtySessionSupervisorError> {
    if matches!(
        workspace.state,
        WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
    ) {
        return Err(PtySessionSupervisorError::WorkspaceUnavailable(
            workspace.state.clone(),
        ));
    }
    Ok(())
}

fn validate_cwd(cwd: Option<&str>) -> Result<(), PtySessionSupervisorError> {
    if let Some(cwd) = cwd
        && cwd.chars().any(char::is_control)
    {
        return Err(PtySessionSupervisorError::InvalidCwd);
    }
    Ok(())
}

fn validate_visible_text(value: Option<&str>, max_chars: usize) -> Result<(), ()> {
    if let Some(value) = value
        && (value.chars().any(char::is_control) || value.chars().count() > max_chars)
    {
        return Err(());
    }
    Ok(())
}

fn validate_input(input: &str, max_input_bytes: usize) -> Result<(), PtySessionSupervisorError> {
    if input.is_empty() || input.len() > max_input_bytes || input.as_bytes().contains(&0) {
        return Err(PtySessionSupervisorError::InvalidInput(max_input_bytes));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{
        AccessPathId, AgentWorkspace, ConnectionSession, ConnectorId, EntityState, HostId,
        SessionId, WorkspaceId, WorkspaceState, now_utc,
    };

    use crate::{
        PtySessionHeartbeatCommand, PtySessionInputCommand, PtySessionOpenCommand,
        PtySessionSupervisor,
    };

    fn workspace(state: WorkspaceState) -> AgentWorkspace {
        let now = now_utc();
        AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: None,
            host_id: HostId::new(),
            access_path_id: AccessPathId::new(),
            connector_id: ConnectorId::new(),
            label: "agent-main".to_owned(),
            cwd: Some("/tmp".to_owned()),
            state,
            policy_profile: "default".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        }
    }

    fn connection(workspace: &AgentWorkspace, state: EntityState) -> ConnectionSession {
        let now = now_utc();
        ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: workspace.access_path_id,
            connector_id: workspace.connector_id,
            state,
            created_at: now,
            last_used_at: now,
            open_channels: 1,
            reused_count: 0,
            failure_count: 0,
            last_error: None,
        }
    }

    #[test]
    fn opens_heartbeat_and_closes_pty_session() -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = PtySessionSupervisor::default();
        let workspace = workspace(WorkspaceState::Idle);
        let connection = connection(&workspace, EntityState::Connected);
        let session = supervisor.open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: connection.session_id,
                cwd: None,
            },
        )?;
        assert_eq!(session.workspace_id, workspace.id);
        assert_eq!(session.cwd.as_deref(), Some("/tmp"));
        assert!(session.input_allowed);

        let session = supervisor.heartbeat(
            session,
            PtySessionHeartbeatCommand {
                state: WorkspaceState::Working,
                foreground_process: Some("python train.py".to_owned()),
                cwd: Some("/srv/app".to_owned()),
                recent_output_ref: Some("artifact:latest".to_owned()),
                last_exit_code: None,
                input_allowed: true,
            },
        )?;
        assert_eq!(session.state, WorkspaceState::Working);
        assert_eq!(
            session.foreground_process.as_deref(),
            Some("python train.py")
        );

        let session = supervisor.close(session, Some(0));
        assert_eq!(session.state, WorkspaceState::Closed);
        assert!(!session.input_allowed);
        assert_eq!(session.last_exit_code, Some(0));
        Ok(())
    }

    #[test]
    fn queues_pty_input_without_exposing_raw_payload_in_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = PtySessionSupervisor::default();
        let workspace = workspace(WorkspaceState::Idle);
        let connection = connection(&workspace, EntityState::Connected);
        let session = supervisor.open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: connection.session_id,
                cwd: Some("/tmp".to_owned()),
            },
        )?;

        let plan = supervisor.queue_input(
            &session,
            &workspace,
            7,
            PtySessionInputCommand {
                input: "echo hello\n".to_owned(),
                requested_by: Some("agent".to_owned()),
                idempotency_key: None,
            },
        )?;

        assert_eq!(plan.input_text, "echo hello\n");
        assert_eq!(plan.event.sequence, 7);
        assert_eq!(plan.event.byte_len, 11);
        assert_eq!(
            plan.event.redacted_input_summary,
            "11 bytes queued for pty input"
        );
        assert!(!plan.event.redacted_input_summary.contains("echo hello"));
        Ok(())
    }

    #[test]
    fn denies_open_when_persistent_pty_limit_is_reached() -> Result<(), Box<dyn std::error::Error>>
    {
        let supervisor = PtySessionSupervisor::default();
        let workspace = workspace(WorkspaceState::Idle);
        let connection = connection(&workspace, EntityState::Connected);
        let error = supervisor
            .open_session(
                &workspace,
                &connection,
                8,
                PtySessionOpenCommand {
                    session_id: connection.session_id,
                    cwd: None,
                },
            )
            .err()
            .ok_or("pty limit should reject new session")?;
        assert!(error.to_string().contains("policy"));
        Ok(())
    }

    #[test]
    fn reaps_expired_active_pty_session() -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = PtySessionSupervisor::default();
        let workspace = workspace(WorkspaceState::Idle);
        let connection = connection(&workspace, EntityState::Connected);
        let mut session = supervisor.open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: connection.session_id,
                cwd: None,
            },
        )?;
        session.last_activity_at = now_utc() - time::Duration::seconds(120);
        let (session, reaped) = supervisor.reap_expired(session, now_utc(), 60);
        assert!(reaped);
        assert_eq!(session.state, WorkspaceState::Closed);
        assert!(!session.input_allowed);
        Ok(())
    }
}

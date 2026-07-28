//! Agent workspace supervision policy.

use remote_hosts_domain::{
    AccessPathId, AgentSessionId, AgentWorkspace, ConnectorId, HostId, WorkspaceId, WorkspaceState,
    now_utc,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ProtectionDecision, ServerProtectionPolicy};

/// Request to create a durable agent workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceCreateCommand {
    /// Target host.
    pub host_id: HostId,
    /// Resolved access path.
    pub access_path_id: AccessPathId,
    /// Agent-client session that owns the workspace.
    pub agent_session_id: Option<AgentSessionId>,
    /// Connector that owns the connection.
    pub connector_id: ConnectorId,
    /// Human/agent label.
    pub label: String,
    /// Optional working directory.
    pub cwd: Option<String>,
    /// Policy profile name.
    pub policy_profile: String,
    /// Workspace TTL in seconds.
    pub ttl_seconds: u64,
}

/// Workspace supervisor errors.
#[derive(Debug, Error)]
pub enum WorkspaceSupervisorError {
    /// Policy rejected workspace creation.
    #[error("workspace denied by policy: {0:?}")]
    PolicyDenied(ProtectionDecision),
    /// Label is empty or too long.
    #[error("workspace label must be 1..=80 visible characters")]
    InvalidLabel,
    /// Working directory is invalid.
    #[error("workspace cwd must not contain control characters")]
    InvalidCwd,
    /// TTL is outside supported range.
    #[error("workspace ttl must be between 60 and 86400 seconds")]
    InvalidTtl,
}

/// Creates and validates agent workspaces under a server protection policy.
#[derive(Clone, Debug)]
pub struct WorkspaceSupervisor {
    policy: ServerProtectionPolicy,
}

impl WorkspaceSupervisor {
    /// Creates a supervisor from a policy.
    pub fn new(policy: ServerProtectionPolicy) -> Self {
        Self { policy }
    }

    /// Creates a new idle workspace if current host pressure allows it.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is malformed or policy denies a new workspace.
    pub fn create_workspace(
        &self,
        command: WorkspaceCreateCommand,
        active_workspace_count: u32,
    ) -> Result<AgentWorkspace, WorkspaceSupervisorError> {
        validate_command(&command)?;

        let decision = self
            .policy
            .decide_workspace_creation(active_workspace_count);
        if !decision.allowed {
            return Err(WorkspaceSupervisorError::PolicyDenied(decision));
        }

        let now = now_utc();
        Ok(AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: command.agent_session_id,
            host_id: command.host_id,
            access_path_id: command.access_path_id,
            connector_id: command.connector_id,
            label: command.label,
            cwd: command.cwd,
            state: WorkspaceState::Idle,
            policy_profile: command.policy_profile,
            created_at: now,
            last_activity_at: now,
            ttl_seconds: command.ttl_seconds,
        })
    }
}

impl Default for WorkspaceSupervisor {
    fn default() -> Self {
        Self::new(ServerProtectionPolicy::default())
    }
}

fn validate_command(command: &WorkspaceCreateCommand) -> Result<(), WorkspaceSupervisorError> {
    let label_len = command.label.trim().chars().count();
    if !(1..=80).contains(&label_len) {
        return Err(WorkspaceSupervisorError::InvalidLabel);
    }

    if let Some(cwd) = &command.cwd
        && cwd.chars().any(char::is_control)
    {
        return Err(WorkspaceSupervisorError::InvalidCwd);
    }

    if !(60..=86_400).contains(&command.ttl_seconds) {
        return Err(WorkspaceSupervisorError::InvalidTtl);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{AccessPathId, ConnectorId, EntityState, HostId, WorkspaceState};

    use super::{WorkspaceCreateCommand, WorkspaceSupervisor, WorkspaceSupervisorError};

    fn command() -> WorkspaceCreateCommand {
        WorkspaceCreateCommand {
            host_id: HostId::new(),
            access_path_id: AccessPathId::new(),
            agent_session_id: None,
            connector_id: ConnectorId::new(),
            label: "agent-main".to_owned(),
            cwd: Some("/tmp".to_owned()),
            policy_profile: "default".to_owned(),
            ttl_seconds: 3600,
        }
    }

    #[test]
    fn creates_idle_workspace() -> Result<(), WorkspaceSupervisorError> {
        let supervisor = WorkspaceSupervisor::default();
        let workspace = supervisor.create_workspace(command(), 0)?;

        assert_eq!(workspace.label, "agent-main");
        assert_eq!(workspace.state, WorkspaceState::Idle);
        assert_eq!(workspace.ttl_seconds, 3600);
        Ok(())
    }

    #[test]
    fn workspace_limit_is_independent_from_persistent_pty_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = WorkspaceSupervisor::default();
        supervisor.create_workspace(command(), 1)?;
        let error = supervisor
            .create_workspace(command(), 32)
            .err()
            .ok_or("default policy should cap active logical workspaces")?;

        match error {
            WorkspaceSupervisorError::PolicyDenied(decision) => {
                assert!(!decision.allowed);
                assert_eq!(decision.state, EntityState::RateLimited);
                assert_eq!(decision.human_message, "active workspace limit reached");
            }
            other => return Err(format!("unexpected error: {other:?}").into()),
        }
        Ok(())
    }
}

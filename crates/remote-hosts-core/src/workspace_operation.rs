//! Workspace operation planning and output bookkeeping.

use remote_hosts_domain::{
    AgentWorkspace, OperationId, OperationOutputChunk, OperationOutputChunkId, OperationRun,
    OperationState, OperationType, OutputStream, WorkspaceState, now_utc,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CommandClass, CommandProfile, CommandValidationError, FileTransferSpec,
    FileTransferValidationError, ProtectionDecision, SecretRedactor, ServerProtectionPolicy,
};

use crate::workspace::validate_coordination_scope;

/// Caller-declared coordination behavior for an arbitrary command.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationCoordinationMode {
    /// Infer coordination from the command profile for backward compatibility.
    #[default]
    Auto,
    /// Caller attests that the command only observes state and needs no write lease.
    ReadOnly,
    /// Caller declares possible mutation and requires a scoped write lease.
    Mutating,
}

impl OperationCoordinationMode {
    /// Resolves whether a command must acquire a write lease.
    #[must_use]
    pub fn requires_write_lease(self, class: &CommandClass) -> bool {
        match self {
            Self::Auto => !matches!(class, CommandClass::ReadOnly),
            Self::ReadOnly => false,
            Self::Mutating => true,
        }
    }
}

/// Request to queue a command profile inside an existing workspace.
#[derive(Clone, Debug)]
pub struct WorkspaceRunCommand {
    /// Target workspace.
    pub workspace: AgentWorkspace,
    /// Resolved command profile.
    pub command_profile: CommandProfile,
    /// Human or agent intent.
    pub intent: Option<String>,
    /// Optional retry key scoped to the workspace's agent session.
    pub idempotency_key: Option<String>,
    /// Explicit read/write coordination behavior. Defaults to profile-based inference.
    pub coordination_mode: OperationCoordinationMode,
    /// Optional operation-level scope, restricted to the Workspace scope or its descendants.
    pub coordination_scope: Option<String>,
    /// Current queued operation count for the host.
    pub queued_operations: u32,
    /// Current active exec channel count for the host.
    pub active_exec_channels: u32,
    /// Current active probe count for the host.
    pub active_probe_jobs: u32,
    /// Whether overload cooldown is currently active.
    pub overload_cooldown_active: bool,
}

/// Request to queue a managed file transfer inside an existing workspace.
#[derive(Clone, Debug)]
pub struct WorkspaceFileTransfer {
    /// Target workspace.
    pub workspace: AgentWorkspace,
    /// Validated SFTP transfer payload.
    pub spec: FileTransferSpec,
    /// Human or agent intent.
    pub intent: Option<String>,
    /// Optional retry key scoped to the workspace's agent session.
    pub idempotency_key: Option<String>,
    /// Current queued operation count for the host.
    pub queued_operations: u32,
    /// Current active channel count for the host.
    pub active_exec_channels: u32,
    /// Whether overload cooldown is currently active.
    pub overload_cooldown_active: bool,
}

/// Planned operation and workspace transition.
#[derive(Clone, Debug)]
pub struct WorkspaceRunPlan {
    /// Queued operation.
    pub operation: OperationRun,
    /// Workspace state after queueing.
    pub workspace_state: WorkspaceState,
    /// Policy decision that allowed the operation.
    pub decision: ProtectionDecision,
    /// Initial system chunk for agent-visible output.
    pub initial_output_chunk: OperationOutputChunk,
}

/// Workspace operation planning errors.
#[derive(Debug, Error)]
pub enum WorkspaceOperationError {
    /// Command validation failed.
    #[error(transparent)]
    CommandValidation(#[from] CommandValidationError),
    /// Command serialization failed.
    #[error("command profile serialization failed: {0}")]
    CommandSerialization(#[from] serde_json::Error),
    /// File-transfer validation failed.
    #[error(transparent)]
    FileTransferValidation(#[from] FileTransferValidationError),
    /// Policy denied the operation.
    #[error("workspace operation denied by policy: {0:?}")]
    PolicyDenied(ProtectionDecision),
    /// Workspace cannot accept new operations.
    #[error("workspace state `{0:?}` cannot accept new operations")]
    WorkspaceUnavailable(WorkspaceState),
    /// Intent is empty or too large.
    #[error("operation intent must be 1..=240 visible characters when provided")]
    InvalidIntent,
    /// Operation-level coordination scope is malformed or escapes the Workspace boundary.
    #[error("operation coordination scope must be valid and remain within the Workspace scope")]
    InvalidCoordinationScope,
}

/// Creates operation records under workspace and server protection policy.
#[derive(Debug)]
pub struct WorkspaceOperationSupervisor {
    policy: ServerProtectionPolicy,
    redactor: SecretRedactor,
}

impl WorkspaceOperationSupervisor {
    /// Creates a supervisor.
    pub fn new(policy: ServerProtectionPolicy) -> Self {
        Self {
            policy,
            redactor: SecretRedactor::default(),
        }
    }

    /// Queues an operation for connector-side execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the workspace is closed, the command is invalid, policy rejects the
    /// current host pressure, or the command profile cannot be serialized for audit storage.
    pub fn queue_operation(
        &self,
        command: &WorkspaceRunCommand,
    ) -> Result<WorkspaceRunPlan, WorkspaceOperationError> {
        ensure_workspace_available(&command.workspace)?;

        command.command_profile.validate()?;
        let intent = normalized_intent(command.intent.as_deref(), &command.command_profile)?;
        let decision = self.policy.decide(
            command.queued_operations,
            command.active_exec_channels,
            command.active_probe_jobs,
            0,
            command.overload_cooldown_active,
        );
        if !decision.allowed {
            return Err(WorkspaceOperationError::PolicyDenied(decision));
        }

        let now = now_utc();
        let operation_id = OperationId::new();
        let requires_write_lease = command
            .coordination_mode
            .requires_write_lease(&command.command_profile.class);
        let coordination_scope = resolve_operation_coordination_scope(
            &command.workspace,
            command.coordination_scope.as_deref(),
        )?;
        let command_summary = if matches!(command.command_profile.class, CommandClass::Sensitive) {
            let script = command
                .command_profile
                .args
                .last()
                .map_or("", String::as_str);
            format!(
                "{} via pooled workspace:\n{}",
                command.command_profile.name,
                self.redactor.command_preview(script)
            )
        } else {
            self.redactor.redact(&format!(
                "{} {}",
                command.command_profile.program,
                command.command_profile.args.join(" ")
            ))
        };
        let operation = OperationRun {
            id: operation_id,
            host_id: command.workspace.host_id,
            access_path_id: command.workspace.access_path_id,
            connector_id: command.workspace.connector_id,
            session_id: None,
            workspace_id: Some(command.workspace.id),
            agent_session_id: command.workspace.agent_session_id,
            idempotency_key: command.idempotency_key.clone(),
            requires_write_lease,
            coordination_scope,
            operation_type: operation_type(&command.command_profile.class, requires_write_lease),
            intent,
            state: OperationState::Queued,
            started_at: now,
            finished_at: None,
            exit_code: None,
            timeout_seconds: command.command_profile.timeout_seconds,
            redacted_command_summary: command_summary,
            command_profile_json: Some(serde_json::to_value(&command.command_profile)?),
            transport_evidence: None,
            redacted_output_summary: Some(
                "queued for connector-side execution; poll workspace state or output".to_owned(),
            ),
            log_ref: Some(format!("operation-output:{operation_id}")),
            attempt_count: 0,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        };
        let queued_message = "operation queued for connector-side execution; reuse this workspace and poll state/output instead of opening another SSH session".to_owned();
        let initial_output_chunk = OperationOutputChunk {
            id: OperationOutputChunkId::new(),
            operation_id,
            workspace_id: command.workspace.id,
            stream: OutputStream::System,
            sequence: 0,
            byte_len: u64::try_from(queued_message.len()).unwrap_or(u64::MAX),
            redacted_text: queued_message,
            truncated: false,
            created_at: now,
        };

        Ok(WorkspaceRunPlan {
            operation,
            workspace_state: WorkspaceState::Working,
            decision,
            initial_output_chunk,
        })
    }

    /// Queues a validated SFTP transfer for connector-side execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the workspace is unavailable, the transfer is malformed, current host
    /// pressure is too high, or the transfer payload cannot be serialized.
    pub fn queue_file_transfer(
        &self,
        transfer: &WorkspaceFileTransfer,
    ) -> Result<WorkspaceRunPlan, WorkspaceOperationError> {
        ensure_workspace_available(&transfer.workspace)?;
        transfer.spec.validate()?;
        let intent = normalized_file_transfer_intent(transfer.intent.as_deref(), &transfer.spec)?;
        let decision = self.policy.decide(
            transfer.queued_operations,
            transfer.active_exec_channels,
            0,
            0,
            transfer.overload_cooldown_active,
        );
        if !decision.allowed {
            return Err(WorkspaceOperationError::PolicyDenied(decision));
        }

        let now = now_utc();
        let operation_id = OperationId::new();
        let direction = format!("{:?}", transfer.spec.direction).to_lowercase();
        let file_name = transfer
            .spec
            .remote_path
            .rsplit('/')
            .next()
            .unwrap_or("<invalid>");
        let command_summary = format!(
            "sftp {direction} file={file_name} max_bytes={} overwrite={:?} mode={:?} via pooled workspace",
            transfer.spec.max_size_bytes, transfer.spec.overwrite, transfer.spec.mode
        );
        let operation = OperationRun {
            id: operation_id,
            host_id: transfer.workspace.host_id,
            access_path_id: transfer.workspace.access_path_id,
            connector_id: transfer.workspace.connector_id,
            session_id: None,
            workspace_id: Some(transfer.workspace.id),
            agent_session_id: transfer.workspace.agent_session_id,
            idempotency_key: transfer.idempotency_key.clone(),
            requires_write_lease: matches!(transfer.spec.direction, crate::SftpDirection::Upload),
            coordination_scope: transfer.workspace.coordination_scope.clone(),
            operation_type: OperationType::Sftp,
            intent,
            state: OperationState::Queued,
            started_at: now,
            finished_at: None,
            exit_code: None,
            timeout_seconds: transfer.spec.timeout_seconds,
            redacted_command_summary: self.redactor.redact(&command_summary),
            command_profile_json: Some(serde_json::to_value(&transfer.spec)?),
            transport_evidence: None,
            redacted_output_summary: Some(
                "file transfer queued; poll workspace state or result".to_owned(),
            ),
            log_ref: Some(format!("operation-output:{operation_id}")),
            attempt_count: 0,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        };
        let queued_message = "file transfer queued for connector-side execution over the pooled SSH session; poll workspace state/result instead of opening another SSH connection".to_owned();
        let initial_output_chunk = OperationOutputChunk {
            id: OperationOutputChunkId::new(),
            operation_id,
            workspace_id: transfer.workspace.id,
            stream: OutputStream::System,
            sequence: 0,
            byte_len: u64::try_from(queued_message.len()).unwrap_or(u64::MAX),
            redacted_text: queued_message,
            truncated: false,
            created_at: now,
        };

        Ok(WorkspaceRunPlan {
            operation,
            workspace_state: WorkspaceState::Working,
            decision,
            initial_output_chunk,
        })
    }
}

impl Default for WorkspaceOperationSupervisor {
    fn default() -> Self {
        Self::new(ServerProtectionPolicy::default())
    }
}

fn normalized_intent(
    intent: Option<&str>,
    profile: &CommandProfile,
) -> Result<String, WorkspaceOperationError> {
    let value = intent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || format!("run command profile `{}`", profile.name),
            ToOwned::to_owned,
        );
    let len = value.chars().count();
    if !(1..=240).contains(&len) {
        return Err(WorkspaceOperationError::InvalidIntent);
    }
    Ok(value)
}

fn normalized_file_transfer_intent(
    intent: Option<&str>,
    spec: &FileTransferSpec,
) -> Result<String, WorkspaceOperationError> {
    let direction = format!("{:?}", spec.direction).to_lowercase();
    let value = intent
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || format!("{direction} file through managed SFTP"),
            ToOwned::to_owned,
        );
    let len = value.chars().count();
    if !(1..=240).contains(&len) {
        return Err(WorkspaceOperationError::InvalidIntent);
    }
    Ok(value)
}

fn ensure_workspace_available(workspace: &AgentWorkspace) -> Result<(), WorkspaceOperationError> {
    if matches!(
        workspace.state,
        WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
    ) {
        return Err(WorkspaceOperationError::WorkspaceUnavailable(
            workspace.state.clone(),
        ));
    }
    Ok(())
}

/// Resolves an operation-level scope without allowing it to escape the Workspace boundary.
///
/// # Errors
///
/// Returns [`WorkspaceOperationError::InvalidCoordinationScope`] when the requested scope is
/// malformed or is outside the Workspace's coordination boundary.
pub fn resolve_operation_coordination_scope(
    workspace: &AgentWorkspace,
    requested: Option<&str>,
) -> Result<String, WorkspaceOperationError> {
    let scope = requested.unwrap_or(&workspace.coordination_scope);
    validate_coordination_scope(scope)
        .map_err(|_| WorkspaceOperationError::InvalidCoordinationScope)?;
    let workspace_scope = workspace.coordination_scope.as_str();
    let within_workspace = workspace_scope == "host"
        || scope == workspace_scope
        || scope
            .strip_prefix(workspace_scope)
            .is_some_and(|suffix| suffix.starts_with('/'));
    if !within_workspace {
        return Err(WorkspaceOperationError::InvalidCoordinationScope);
    }
    Ok(scope.to_owned())
}

fn operation_type(class: &CommandClass, requires_write_lease: bool) -> OperationType {
    if requires_write_lease {
        match class {
            CommandClass::ReadOnly | CommandClass::Build | CommandClass::Sensitive => {
                OperationType::Runbook
            }
        }
    } else {
        OperationType::ReadonlyExec
    }
}

#[cfg(test)]
mod tests {
    use remote_hosts_domain::{
        AccessPathId, AgentWorkspace, ConnectorId, HostId, WorkspaceId, WorkspaceState, now_utc,
    };

    use crate::{
        CommandProfileCatalog, DEFAULT_SFTP_MAX_SIZE_BYTES, DEFAULT_SFTP_TIMEOUT_SECONDS,
        FileTransferSpec, ServerProtectionPolicy, SftpDirection, SftpOverwritePolicy,
    };

    use super::{
        OperationCoordinationMode, WorkspaceFileTransfer, WorkspaceOperationError,
        WorkspaceOperationSupervisor, WorkspaceRunCommand,
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
            coordination_scope: "host".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        }
    }

    fn file_transfer(state: WorkspaceState) -> WorkspaceFileTransfer {
        WorkspaceFileTransfer {
            workspace: workspace(state),
            spec: FileTransferSpec {
                direction: SftpDirection::Upload,
                local_path: "/tmp/manifest.yaml".to_owned(),
                remote_path: "/var/tmp/manifest.yaml".to_owned(),
                overwrite: SftpOverwritePolicy::Deny,
                mode: Some(0o600),
                max_size_bytes: DEFAULT_SFTP_MAX_SIZE_BYTES,
                expected_sha256: None,
                timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
            },
            intent: Some("upload deployment manifest".to_owned()),
            idempotency_key: None,
            queued_operations: 0,
            active_exec_channels: 0,
            overload_cooldown_active: false,
        }
    }

    #[test]
    fn queues_sftp_transfer_without_file_content_in_audit_summary()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = WorkspaceOperationSupervisor::default()
            .queue_file_transfer(&file_transfer(WorkspaceState::Idle))?;

        assert_eq!(
            plan.operation.operation_type,
            remote_hosts_domain::OperationType::Sftp
        );
        assert_eq!(plan.workspace_state, WorkspaceState::Working);
        assert!(
            plan.operation
                .redacted_command_summary
                .contains("file=manifest.yaml")
        );
        assert_eq!(
            plan.operation
                .command_profile_json
                .as_ref()
                .and_then(|value| value.get("direction")),
            Some(&serde_json::json!("upload"))
        );
        Ok(())
    }

    #[test]
    fn rejects_sftp_transfer_for_unavailable_workspace() -> Result<(), Box<dyn std::error::Error>> {
        let error = WorkspaceOperationSupervisor::default()
            .queue_file_transfer(&file_transfer(WorkspaceState::Throttled))
            .err()
            .ok_or("throttled workspace must reject transfer")?;
        assert!(matches!(
            error,
            WorkspaceOperationError::WorkspaceUnavailable(WorkspaceState::Throttled)
        ));
        Ok(())
    }

    #[test]
    fn queues_operation_without_executing_it() -> Result<(), Box<dyn std::error::Error>> {
        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin("host.uptime", Vec::new(), &policy)?;
        let plan =
            WorkspaceOperationSupervisor::new(policy).queue_operation(&WorkspaceRunCommand {
                workspace: workspace(WorkspaceState::Idle),
                command_profile: profile,
                intent: None,
                idempotency_key: None,
                coordination_mode: OperationCoordinationMode::Auto,
                coordination_scope: None,
                queued_operations: 0,
                active_exec_channels: 0,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })?;

        assert_eq!(
            plan.operation.state,
            remote_hosts_domain::OperationState::Queued
        );
        assert_eq!(plan.workspace_state, WorkspaceState::Working);
        assert_eq!(
            plan.initial_output_chunk.stream,
            remote_hosts_domain::OutputStream::System
        );
        Ok(())
    }

    #[test]
    fn shell_operation_keeps_a_bounded_redacted_command_preview()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin(
            "shell.posix",
            vec!["set -e\nPASSWORD=hunter2\nkubectl get pods".to_owned()],
            &policy,
        )?;
        let plan =
            WorkspaceOperationSupervisor::new(policy).queue_operation(&WorkspaceRunCommand {
                workspace: workspace(WorkspaceState::Idle),
                command_profile: profile,
                intent: None,
                idempotency_key: None,
                coordination_mode: OperationCoordinationMode::Auto,
                coordination_scope: None,
                queued_operations: 0,
                active_exec_channels: 0,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })?;

        assert!(
            plan.operation
                .redacted_command_summary
                .contains("kubectl get pods")
        );
        assert!(
            plan.operation
                .redacted_command_summary
                .contains("PASSWORD=<redacted>")
        );
        assert!(!plan.operation.redacted_command_summary.contains("hunter2"));
        Ok(())
    }

    #[test]
    fn denies_when_queue_is_full() -> Result<(), Box<dyn std::error::Error>> {
        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin("host.uptime", Vec::new(), &policy)?;
        let error = WorkspaceOperationSupervisor::new(policy.clone())
            .queue_operation(&WorkspaceRunCommand {
                workspace: workspace(WorkspaceState::Idle),
                command_profile: profile,
                intent: None,
                idempotency_key: None,
                coordination_mode: OperationCoordinationMode::Auto,
                coordination_scope: None,
                queued_operations: policy.max_operation_queue_depth_per_host,
                active_exec_channels: 0,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })
            .err()
            .ok_or("queue should be full")?;

        assert!(matches!(error, WorkspaceOperationError::PolicyDenied(_)));
        Ok(())
    }

    #[test]
    fn explicit_coordination_mode_controls_shell_write_leases()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = ServerProtectionPolicy::default();
        for (mode, expected) in [
            (OperationCoordinationMode::Auto, true),
            (OperationCoordinationMode::ReadOnly, false),
            (OperationCoordinationMode::Mutating, true),
        ] {
            let profile = CommandProfileCatalog::resolve_builtin(
                "shell.posix",
                vec!["kubectl get pods".to_owned()],
                &policy,
            )?;
            let plan = WorkspaceOperationSupervisor::new(policy.clone()).queue_operation(
                &WorkspaceRunCommand {
                    workspace: workspace(WorkspaceState::Idle),
                    command_profile: profile,
                    intent: Some("inspect cluster state".to_owned()),
                    idempotency_key: None,
                    coordination_mode: mode,
                    coordination_scope: None,
                    queued_operations: 0,
                    active_exec_channels: 0,
                    active_probe_jobs: 0,
                    overload_cooldown_active: false,
                },
            )?;
            assert_eq!(plan.operation.requires_write_lease, expected);
        }
        Ok(())
    }

    #[test]
    fn operation_scope_can_narrow_but_not_escape_workspace_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin(
            "shell.posix",
            vec!["touch /tmp/result".to_owned()],
            &policy,
        )?;
        let mut scoped_workspace = workspace(WorkspaceState::Idle);
        scoped_workspace.coordination_scope = "service/api".to_owned();
        let mut command = WorkspaceRunCommand {
            workspace: scoped_workspace,
            command_profile: profile,
            intent: Some("update one api replica".to_owned()),
            idempotency_key: None,
            coordination_mode: OperationCoordinationMode::Mutating,
            coordination_scope: Some("service/api/replica/one".to_owned()),
            queued_operations: 0,
            active_exec_channels: 0,
            active_probe_jobs: 0,
            overload_cooldown_active: false,
        };
        let plan = WorkspaceOperationSupervisor::new(policy.clone()).queue_operation(&command)?;
        assert_eq!(plan.operation.coordination_scope, "service/api/replica/one");

        command.coordination_scope = Some("service/database".to_owned());
        let error = WorkspaceOperationSupervisor::new(policy)
            .queue_operation(&command)
            .err()
            .ok_or("sibling scope must not escape the Workspace boundary")?;
        assert!(matches!(
            error,
            WorkspaceOperationError::InvalidCoordinationScope
        ));
        Ok(())
    }
}

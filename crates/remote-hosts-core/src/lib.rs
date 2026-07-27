//! Core policies and service contracts.

pub mod command;
pub mod connector_state;
pub mod policy;
pub mod pty_session;
pub mod redaction;
pub mod resolver;
pub mod state;
pub mod transport;
pub mod workspace;
pub mod workspace_operation;

pub use command::{
    CommandClass, CommandProfile, CommandProfileCatalog, CommandProfileInfo,
    CommandProfileResolutionError, CommandValidationError,
};
pub use connector_state::{ConnectorHeartbeatOutcome, ConnectorStateTracker};
pub use policy::{ProtectionDecision, ServerProtectionPolicy};
pub use pty_session::{
    PtySessionHeartbeatCommand, PtySessionInputCommand, PtySessionInputPlan, PtySessionOpenCommand,
    PtySessionSupervisor, PtySessionSupervisorError,
};
pub use redaction::SecretRedactor;
pub use resolver::{AccessCandidate, AccessResolution, AccessResolutionError, AccessResolver};
pub use state::{HostStateAggregate, HostStateAggregator, HostStateInput};
pub use transport::{
    CheckRequest, CheckResult, DEFAULT_SFTP_MAX_SIZE_BYTES, DEFAULT_SFTP_TIMEOUT_SECONDS,
    ExecRequest, ExecResult, FileTransferSpec, FileTransferValidationError, ForwardHandle,
    ForwardRequest, MAX_SFTP_MAX_SIZE_BYTES, MAX_SFTP_TIMEOUT_SECONDS, RemoteTransport,
    SftpDirection, SftpOverwritePolicy, SftpProgress, SftpRequest, SftpResult,
};
pub use workspace::{WorkspaceCreateCommand, WorkspaceSupervisor, WorkspaceSupervisorError};
pub use workspace_operation::{
    WorkspaceFileTransfer, WorkspaceOperationError, WorkspaceOperationSupervisor,
    WorkspaceRunCommand, WorkspaceRunPlan,
};

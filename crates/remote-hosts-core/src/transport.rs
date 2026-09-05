//! Remote transport trait and request/response types.

use std::path::{Component, Path};

use async_trait::async_trait;
use remote_hosts_domain::{AccessPathId, HostId, OperationId, SessionId, SshTransportTelemetry};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::CommandProfile;

/// Default maximum size for one SFTP transfer.
pub const DEFAULT_SFTP_MAX_SIZE_BYTES: u64 = 512 * 1024 * 1024;
/// Hard maximum size accepted by the built-in SFTP operation contract.
pub const MAX_SFTP_MAX_SIZE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Default end-to-end timeout for one SFTP transfer.
pub const DEFAULT_SFTP_TIMEOUT_SECONDS: u64 = 600;
/// Hard maximum timeout for one SFTP transfer.
pub const MAX_SFTP_TIMEOUT_SECONDS: u64 = 7_200;

/// Transport-level error.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Request was denied by policy.
    #[error("request denied by policy: {0}")]
    PolicyDenied(String),
    /// The connector's local SSH handshake budget is temporarily exhausted.
    #[error("local SSH handshake budget exhausted; retry_after_seconds={retry_after_seconds}")]
    LocalHandshakeBudgetExhausted {
        /// Exact delay reported by the local token bucket.
        retry_after_seconds: u64,
    },
    /// Transport backend failed.
    #[error("transport backend failed: {0}")]
    Backend(String),
    /// A file transfer failed without proving that the underlying SSH connection is unhealthy.
    #[error("file transfer failed: {0}")]
    FileTransfer(String),
    /// Operation timed out.
    #[error("operation timed out")]
    Timeout,
}

/// Lightweight connectivity check request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckRequest {
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
}

/// Lightweight connectivity check result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckResult {
    /// Whether the check succeeded.
    pub ok: bool,
    /// Optional latency in milliseconds.
    pub latency_ms: Option<u64>,
    /// Human message.
    pub message: String,
}

/// Exec request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecRequest {
    /// Operation id.
    pub operation_id: OperationId,
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Command profile.
    pub profile: CommandProfile,
}

/// Exec result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecResult {
    /// Exit code.
    pub exit_code: Option<i32>,
    /// Redacted stdout.
    pub stdout: String,
    /// Redacted stderr.
    pub stderr: String,
    /// Whether output was truncated.
    pub truncated: bool,
}

/// Direction of one managed SFTP transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SftpDirection {
    /// Copy a connector-local file to the remote host.
    Upload,
    /// Copy a remote file to the connector-local filesystem.
    Download,
}

/// Destination overwrite policy for one managed SFTP transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SftpOverwritePolicy {
    /// Fail if the final destination already exists.
    Deny,
    /// Atomically replace an existing regular file when the backend supports it.
    Replace,
}

/// Validated file-transfer payload persisted with an SFTP operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileTransferSpec {
    /// Transfer direction.
    pub direction: SftpDirection,
    /// Absolute path on the connector machine.
    pub local_path: String,
    /// Absolute SFTP path on the target machine.
    pub remote_path: String,
    /// Destination overwrite policy.
    pub overwrite: SftpOverwritePolicy,
    /// Optional destination permission mode, such as decimal `384` for octal `0600`.
    pub mode: Option<u32>,
    /// Maximum bytes permitted for this transfer.
    pub max_size_bytes: u64,
    /// Optional expected SHA-256 digest.
    pub expected_sha256: Option<String>,
    /// End-to-end timeout in seconds.
    pub timeout_seconds: u64,
}

impl FileTransferSpec {
    /// Validates paths, limits, digest, mode, and timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when any transfer field is outside the bounded contract.
    pub fn validate(&self) -> Result<(), FileTransferValidationError> {
        validate_local_path(&self.local_path)?;
        validate_remote_path(&self.remote_path)?;
        if !(1..=MAX_SFTP_MAX_SIZE_BYTES).contains(&self.max_size_bytes) {
            return Err(FileTransferValidationError::InvalidMaxSize);
        }
        if !(1..=MAX_SFTP_TIMEOUT_SECONDS).contains(&self.timeout_seconds) {
            return Err(FileTransferValidationError::InvalidTimeout);
        }
        if self.mode.is_some_and(|mode| mode > 0o7777) {
            return Err(FileTransferValidationError::InvalidMode);
        }
        if self.expected_sha256.as_deref().is_some_and(|digest| {
            digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            return Err(FileTransferValidationError::InvalidSha256);
        }
        Ok(())
    }
}

/// File-transfer contract validation errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FileTransferValidationError {
    /// Connector-local path is not a clean absolute file path.
    #[error("local_path must be an absolute file path without control, `.` or `..` components")]
    InvalidLocalPath,
    /// Remote path is not a clean absolute SFTP file path.
    #[error(
        "remote_path must be an absolute POSIX or drive-letter SFTP file path without control, empty, `.` or `..` components"
    )]
    InvalidRemotePath,
    /// Transfer size is outside the supported range.
    #[error("max_size_bytes must be between 1 and {MAX_SFTP_MAX_SIZE_BYTES}")]
    InvalidMaxSize,
    /// Transfer timeout is outside the supported range.
    #[error("timeout_seconds must be between 1 and {MAX_SFTP_TIMEOUT_SECONDS}")]
    InvalidTimeout,
    /// Permission mode contains unsupported bits.
    #[error("mode must contain only Unix permission and special bits (0o0000..=0o7777)")]
    InvalidMode,
    /// Expected digest is malformed.
    #[error("expected_sha256 must contain exactly 64 hexadecimal characters")]
    InvalidSha256,
}

/// SFTP transfer request.
#[derive(Clone, Debug)]
pub struct SftpRequest {
    /// Operation id.
    pub operation_id: OperationId,
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Validated transfer payload.
    pub spec: FileTransferSpec,
    /// Optional in-process progress stream consumed by the connector worker.
    pub progress_tx: Option<mpsc::UnboundedSender<SftpProgress>>,
}

/// Progress emitted while a managed file operation is running.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SftpProgress {
    /// Current transfer stage.
    pub stage: String,
    /// Bytes already verified at the destination or temporary file.
    pub bytes_transferred: u64,
    /// Total source size when known.
    pub total_bytes: Option<u64>,
    /// Bytes retained from a prior interrupted attempt.
    pub resumed_bytes: u64,
    /// Number of safe stage retries after transient transport failure.
    pub retry_count: u32,
}

/// Verified SFTP transfer result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SftpResult {
    /// Transfer direction.
    pub direction: SftpDirection,
    /// Bytes transferred.
    pub bytes_transferred: u64,
    /// SHA-256 digest verified at both ends.
    pub sha256: String,
    /// Connector-local path.
    pub local_path: String,
    /// Remote SFTP path.
    pub remote_path: String,
    /// Overwrite policy used for final placement.
    pub overwrite: SftpOverwritePolicy,
    /// Actual file-byte transport when selected by a specialized backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_method: Option<String>,
    /// Follow-up work after verified placement; never implies the file mutation should be replayed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Port-forward request placeholder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardRequest {
    /// Operation id.
    pub operation_id: OperationId,
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
}

/// Port-forward handle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardHandle {
    /// Session id.
    pub session_id: SessionId,
    /// Local port.
    pub local_port: u16,
}

/// Remote transport abstraction.
#[async_trait]
pub trait RemoteTransport: Send + Sync {
    /// Returns connector-local transport telemetry when the backend supports it.
    fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
        None
    }

    /// Checks remote reachability.
    async fn check(&self, request: CheckRequest) -> Result<CheckResult, TransportError>;

    /// Executes a structured command.
    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, TransportError>;

    /// Runs an SFTP transfer.
    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError>;

    /// Opens a port forward.
    async fn open_forward(&self, request: ForwardRequest) -> Result<ForwardHandle, TransportError>;
}

fn validate_local_path(value: &str) -> Result<(), FileTransferValidationError> {
    if value.is_empty()
        || value.len() > 4_096
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
        || value.ends_with(std::path::MAIN_SEPARATOR)
        || value
            .split(std::path::MAIN_SEPARATOR)
            .skip(1)
            .any(|component| component.is_empty() || component == "." || component == "..")
        || Path::new(value).components().any(|component| {
            matches!(
                component,
                Component::CurDir | Component::ParentDir | Component::Prefix(_)
            )
        })
    {
        return Err(FileTransferValidationError::InvalidLocalPath);
    }
    Ok(())
}

fn validate_remote_path(value: &str) -> Result<(), FileTransferValidationError> {
    let bytes = value.as_bytes();
    let posix_absolute = value.starts_with('/');
    let windows_absolute =
        bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && bytes[2] == b'/';
    let component_start = usize::from(posix_absolute);
    if value.is_empty()
        || value.len() > 4_096
        || value.chars().any(char::is_control)
        || value.contains('\\')
        || value.ends_with('/')
        || (!posix_absolute && !windows_absolute)
        || value[component_start..]
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(FileTransferValidationError::InvalidRemotePath);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SFTP_MAX_SIZE_BYTES, DEFAULT_SFTP_TIMEOUT_SECONDS, FileTransferSpec,
        FileTransferValidationError, SftpDirection, SftpOverwritePolicy,
    };

    fn valid_spec() -> FileTransferSpec {
        FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: "/tmp/manifest.yaml".to_owned(),
            remote_path: "/var/tmp/manifest.yaml".to_owned(),
            overwrite: SftpOverwritePolicy::Deny,
            mode: Some(0o600),
            max_size_bytes: DEFAULT_SFTP_MAX_SIZE_BYTES,
            expected_sha256: Some("a".repeat(64)),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        }
    }

    #[test]
    fn file_transfer_spec_accepts_posix_and_windows_sftp_paths()
    -> Result<(), FileTransferValidationError> {
        valid_spec().validate()?;
        let mut windows = valid_spec();
        windows.remote_path = "C:/Users/liang/deploy/app.zip".to_owned();
        windows.validate()?;
        Ok(())
    }

    #[test]
    fn file_transfer_spec_rejects_traversal_and_relative_paths() {
        for local_path in ["manifest.yaml", "/tmp/../secret", "/tmp/./manifest"] {
            let mut spec = valid_spec();
            spec.local_path = local_path.to_owned();
            assert_eq!(
                spec.validate(),
                Err(FileTransferValidationError::InvalidLocalPath)
            );
        }
        for remote_path in [
            "tmp/manifest.yaml",
            "/tmp/../secret",
            "/tmp/./manifest",
            "/tmp//manifest",
            r"C:\temp\manifest.yaml",
        ] {
            let mut spec = valid_spec();
            spec.remote_path = remote_path.to_owned();
            assert_eq!(
                spec.validate(),
                Err(FileTransferValidationError::InvalidRemotePath)
            );
        }
    }

    #[test]
    fn file_transfer_spec_rejects_unbounded_or_malformed_fields() {
        let mut spec = valid_spec();
        spec.max_size_bytes = 0;
        assert_eq!(
            spec.validate(),
            Err(FileTransferValidationError::InvalidMaxSize)
        );

        let mut spec = valid_spec();
        spec.mode = Some(0o10_000);
        assert_eq!(
            spec.validate(),
            Err(FileTransferValidationError::InvalidMode)
        );

        let mut spec = valid_spec();
        spec.expected_sha256 = Some("not-a-digest".to_owned());
        assert_eq!(
            spec.validate(),
            Err(FileTransferValidationError::InvalidSha256)
        );
    }
}

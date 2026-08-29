//! Connector-side execution guards and transport backends.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::Instant as StdInstant,
};

#[cfg(unix)]
use std::{num::NonZeroUsize, process::Stdio as ProcessStdio};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
#[cfg(unix)]
use openssh::{ControlPersist, KnownHosts, Session, SessionBuilder, Stdio as OpenSshStdio};
#[cfg(unix)]
use openssh_sftp_client::{
    Error as OpenSshSftpError, Sftp as OpenSshSftp, SftpOptions,
    error::SftpErrorKind as OpenSshSftpErrorKind,
    file::TokioCompatFile as OpenSshSftpFile,
    fs::Fs as OpenSshSftpFs,
    metadata::{MetaData as OpenSshSftpMetadata, Permissions as OpenSshSftpPermissions},
};
use remote_hosts_core::{
    CheckRequest, CheckResult, CommandProfile, CommandValidationError, ConnectorStateTracker,
    ExecRequest, ExecResult, FileTransferSpec, ForwardHandle, ForwardRequest,
    PtySessionOpenCommand, PtySessionSupervisor, PtySessionSupervisorError, RemoteTransport,
    SecretRedactor, ServerProtectionPolicy, SftpDirection, SftpOverwritePolicy, SftpProgress,
    SftpRequest, SftpResult, detect_pty_interaction, transport::TransportError,
};
use remote_hosts_db::{
    AuthorizedKeyBootstrapRepository, ClaimedOperationFinish, DbError, Repositories,
};
use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AccessPathId, AgentSessionId, AgentWorkspace,
    AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason, AuthorizedKeyBootstrapState,
    ConnectionSession, ConnectorId, EntityState, HostId, HostKind, HostWriteLease, OperationId,
    OperationOutputArtifact, OperationOutputArtifactId, OperationOutputChunk,
    OperationOutputChunkId, OperationRun, OperationState, OutputStream, PtyBackendCapabilities,
    PtyBackendState, PtyInputEvent, PtyInputEventId, PtyInputEventState, PtyInputPayloadKind,
    PtyOutputChunk, PtyOutputChunkId, PtySession, PtySessionId, RouteType, SessionId,
    SshChannelKind, SshChannelTransportEvidence, SshFileTransferMode, SshTransportBackend,
    SshTransportCapabilities, SshTransportRuntime, SshTransportRuntimeId, SshTransportRuntimeState,
    SshTransportTelemetry, StateReasonCode, WorkspaceId, WorkspaceState, now_utc,
};
use remote_hosts_vault::{CredentialSecret, CredentialVault, EncryptedCredentialBlob};
use russh::{
    ChannelMsg, client,
    keys::{
        PrivateKeyWithHashAlg,
        agent::{AgentIdentity, client::AgentClient},
        check_known_hosts, check_known_hosts_path, decode_secret_key,
        known_hosts::{learn_known_hosts, learn_known_hosts_path},
        ssh_key::{HashAlg, PrivateKey, PublicKey},
    },
};
use russh_sftp::{
    client::{SftpSession as RusshSftp, error::Error as RusshSftpError},
    protocol::{
        FileAttributes as RusshSftpMetadata, OpenFlags as RusshSftpOpenFlags,
        StatusCode as RusshSftpStatusCode,
    },
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use tokio::process::{Child as LocalChild, Command as LocalCommand};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
    time::{Duration, Instant},
};
use uuid::Uuid;
use zeroize::Zeroizing;

const DEFAULT_ARTIFACT_THRESHOLD_BYTES: usize = 64 * 1024;
const DEFAULT_ARTIFACT_PREVIEW_BYTES: usize = 4 * 1024;
const DEFAULT_ARTIFACT_ROOT: &str = "remote-hosts-artifacts";
const MAX_SSH_AGENT_IDENTITIES_PER_HANDSHAKE: usize = 2;
const AUTHORIZED_KEY_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);
const AUTHORIZED_KEY_BOOTSTRAP_MAX_FAILURES: u32 = 3;
const EXEC_INLINE_UPLOAD_MAX_BYTES: u64 = 8 * 1024;
const EXEC_UPLOAD_CHUNK_BYTES: usize = 24 * 1024;
const PTY_UPLOAD_CHUNK_BYTES: usize = 16 * 1024;
const PTY_DOWNLOAD_CHUNK_BYTES: usize = 48 * 1024;
const EXEC_UPLOAD_CHUNKS_PER_SESSION: u64 = 256;
const EXEC_TRANSFER_MAX_STAGE_ATTEMPTS: u32 = 3;
const EXEC_TRANSFER_OUTPUT_LIMIT_BYTES: usize = 64 * 1024;
const PTY_TRANSFER_CAPTURE_LIMIT_BYTES: usize = 128 * 1024;
const EXEC_TRANSFER_STAGE_TIMEOUT: Duration = Duration::from_secs(45);
const PTY_TRANSFER_STAGE_TIMEOUT: Duration = Duration::from_secs(600);
const PTY_OUTPUT_BATCH_TARGET_BYTES: usize = 64 * 1024;
const PTY_OUTPUT_BATCH_MAX_DELAY: Duration = Duration::from_millis(50);
const PTY_INTERACTION_TAIL_BYTES: usize = 4 * 1024;
const WRITE_LEASE_HANDOFF_GRACE_SECONDS: i64 = 15;
const PTY_WRITE_LEASE_SECONDS: i64 = 300;
const DEFAULT_PTY_IDLE_TTL_SECONDS: u64 = 3_600;
const DEFAULT_PTY_BUSY_TTL_SECONDS: u64 = 86_400;

type SharedHandshakeBudget = Arc<StdMutex<SlidingWindowBudget>>;

struct HandshakeLimiter {
    path: StdMutex<SlidingWindowBudget>,
    global: SharedHandshakeBudget,
}

struct SlidingWindowBudget {
    max_events: usize,
    window: Duration,
    events: VecDeque<StdInstant>,
}

struct TransportTelemetryTracker {
    runtime_id: SshTransportRuntimeId,
    backend: SshTransportBackend,
    capabilities: SshTransportCapabilities,
    state: StdMutex<TransportTelemetryState>,
}

struct TransportTelemetryState {
    state: SshTransportRuntimeState,
    generation: u64,
    connection_attempt_count: u64,
    successful_handshake_count: u64,
    reuse_count: u64,
    last_handshake_at: Option<time::OffsetDateTime>,
    last_validated_at: Option<time::OffsetDateTime>,
}

impl TransportTelemetryTracker {
    fn new(backend: SshTransportBackend, capabilities: SshTransportCapabilities) -> Self {
        Self {
            runtime_id: SshTransportRuntimeId::new(),
            backend,
            capabilities,
            state: StdMutex::new(TransportTelemetryState {
                state: SshTransportRuntimeState::Cold,
                generation: 0,
                connection_attempt_count: 0,
                successful_handshake_count: 0,
                reuse_count: 0,
                last_handshake_at: None,
                last_validated_at: None,
            }),
        }
    }

    fn connection_attempted(&self) {
        let mut state = self.lock_state();
        state.state = SshTransportRuntimeState::Connecting;
        state.connection_attempt_count = state.connection_attempt_count.saturating_add(1);
    }

    fn handshake_succeeded(&self, observed_at: time::OffsetDateTime) {
        let mut state = self.lock_state();
        state.state = SshTransportRuntimeState::Ready;
        state.generation = state.generation.saturating_add(1);
        state.successful_handshake_count = state.successful_handshake_count.saturating_add(1);
        state.last_handshake_at = Some(observed_at);
        state.last_validated_at = Some(observed_at);
    }

    fn session_reused(&self, observed_at: time::OffsetDateTime) {
        let mut state = self.lock_state();
        state.state = SshTransportRuntimeState::Ready;
        state.reuse_count = state.reuse_count.saturating_add(1);
        state.last_validated_at = Some(observed_at);
    }

    fn disconnected(&self) {
        self.lock_state().state = SshTransportRuntimeState::Disconnected;
    }

    fn idled(&self) {
        self.lock_state().state = SshTransportRuntimeState::Idle;
    }

    fn snapshot(&self) -> SshTransportTelemetry {
        let state = self.lock_state();
        SshTransportTelemetry {
            runtime_id: self.runtime_id,
            backend: self.backend.clone(),
            state: state.state.clone(),
            generation: state.generation,
            connection_attempt_count: state.connection_attempt_count,
            successful_handshake_count: state.successful_handshake_count,
            reuse_count: state.reuse_count,
            last_handshake_at: state.last_handshake_at,
            last_validated_at: state.last_validated_at,
            capabilities: self.capabilities.clone(),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TransportTelemetryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl HandshakeLimiter {
    #[cfg(test)]
    fn new(max_per_minute: u16, max_per_ten_minutes: u32) -> Self {
        Self::with_shared_global(max_per_minute, Self::shared_global(max_per_ten_minutes))
    }

    fn with_shared_global(max_per_minute: u16, global: SharedHandshakeBudget) -> Self {
        Self {
            path: StdMutex::new(SlidingWindowBudget::new(
                u32::from(max_per_minute),
                Duration::from_secs(60),
            )),
            global,
        }
    }

    fn shared_global(max_per_ten_minutes: u32) -> SharedHandshakeBudget {
        Self::shared_global_for_window(max_per_ten_minutes, Duration::from_secs(600))
    }

    fn shared_global_for_window(max_events: u32, window: Duration) -> SharedHandshakeBudget {
        Arc::new(StdMutex::new(SlidingWindowBudget::new(max_events, window)))
    }

    fn try_acquire(&self) -> Result<(), TransportError> {
        self.try_acquire_at(StdInstant::now())
    }

    fn try_acquire_at(&self, now: StdInstant) -> Result<(), TransportError> {
        // Lock ordering is always global then path so the two budgets are checked and consumed
        // atomically across every access path.
        let mut global = self
            .global
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut path = self
            .path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let global_wait = global.retry_after(now);
        let path_wait = path.retry_after(now);
        let wait = [global_wait, path_wait].into_iter().flatten().max();
        if let Some(wait) = wait {
            tracing::warn!(
                global_retry_after_seconds = global_wait.map(duration_seconds_ceil),
                path_retry_after_seconds = path_wait.map(duration_seconds_ceil),
                effective_retry_after_seconds = duration_seconds_ceil(wait),
                "local SSH handshake budget denied without consuming either budget"
            );
            return Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: duration_seconds_ceil(wait),
            });
        }
        global.consume(now);
        path.consume(now);
        Ok(())
    }
}

impl SlidingWindowBudget {
    fn new(max_events: u32, window: Duration) -> Self {
        Self {
            max_events: usize::try_from(max_events.max(1)).unwrap_or(usize::MAX),
            window,
            events: VecDeque::new(),
        }
    }

    fn retry_after(&mut self, now: StdInstant) -> Option<Duration> {
        while self
            .events
            .front()
            .is_some_and(|event| now.saturating_duration_since(*event) >= self.window)
        {
            self.events.pop_front();
        }
        if self.events.len() < self.max_events {
            return None;
        }
        self.events.front().map(|event| {
            self.window
                .saturating_sub(now.saturating_duration_since(*event))
        })
    }

    fn consume(&mut self, now: StdInstant) {
        self.events.push_back(now);
    }
}

fn duration_seconds_ceil(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
        .max(1)
}

/// Host key policy for OpenSSH sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostKeyPolicy {
    /// Require known host match.
    Strict,
    /// Add new hosts, reject changed keys.
    Add,
    /// Accept all host keys.
    Accept,
}

#[cfg(unix)]
impl From<HostKeyPolicy> for KnownHosts {
    fn from(value: HostKeyPolicy) -> Self {
        match value {
            HostKeyPolicy::Strict => Self::Strict,
            HostKeyPolicy::Add => Self::Add,
            HostKeyPolicy::Accept => Self::Accept,
        }
    }
}

/// OpenSSH `ControlMaster` transport configuration.
#[cfg(unix)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenSshTransportConfig {
    /// Destination accepted by OpenSSH, for example `ssh://user@example.com:22`.
    pub destination: String,
    /// Host key policy.
    pub host_key_policy: HostKeyPolicy,
    /// Connect timeout in seconds.
    pub connect_timeout_seconds: u64,
    /// Idle lifetime for the OpenSSH control master.
    pub idle_ttl_seconds: u64,
    /// OpenSSH server-alive interval.
    pub keepalive_seconds: u64,
    /// Maximum channels sharing this access-path transport.
    pub max_concurrent_channels: u16,
    /// Access-path handshake budget per minute.
    pub max_new_connections_per_minute: u16,
    /// Global handshake budget per ten minutes.
    pub max_new_ssh_handshakes_per_10_min: u32,
}

/// OpenSSH `ControlMaster` transport backed by the `openssh` crate's native mux.
#[cfg(unix)]
pub struct OpenSshTransport {
    config: OpenSshTransportConfig,
    session: Mutex<Option<Arc<Session>>>,
    handshake_limiter: HandshakeLimiter,
    channel_semaphore: Arc<Semaphore>,
    telemetry: TransportTelemetryTracker,
}

#[cfg(unix)]
impl OpenSshTransport {
    /// Creates a transport.
    pub fn new(config: OpenSshTransportConfig) -> Self {
        let global = HandshakeLimiter::shared_global(config.max_new_ssh_handshakes_per_10_min);
        Self::with_shared_handshake_budget(config, global)
    }

    fn with_shared_handshake_budget(
        config: OpenSshTransportConfig,
        global: SharedHandshakeBudget,
    ) -> Self {
        let handshake_limiter =
            HandshakeLimiter::with_shared_global(config.max_new_connections_per_minute, global);
        let channel_limit = usize::from(config.max_concurrent_channels.max(1));
        Self {
            config,
            session: Mutex::new(None),
            handshake_limiter,
            channel_semaphore: Arc::new(Semaphore::new(channel_limit)),
            telemetry: TransportTelemetryTracker::new(
                SshTransportBackend::OpenSshControlMaster,
                SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
            ),
        }
    }

    /// Builds an OpenSSH destination URI.
    pub fn destination(username: &str, address: &str, port: u16) -> String {
        format!("ssh://{username}@{address}:{port}")
    }

    async fn session(&self) -> Result<Arc<Session>, TransportError> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            if session.check().await.is_ok() {
                self.telemetry.session_reused(now_utc());
                return Ok(Arc::clone(session));
            }
            *guard = None;
            self.telemetry.disconnected();
        }

        self.handshake_limiter.try_acquire()?;
        self.telemetry.connection_attempted();
        let mut builder = SessionBuilder::default();
        builder
            .known_hosts_check(KnownHosts::from(self.config.host_key_policy))
            .connect_timeout(Duration::from_secs(self.config.connect_timeout_seconds));
        if self.config.keepalive_seconds > 0 {
            builder.server_alive_interval(Duration::from_secs(self.config.keepalive_seconds));
        }
        let idle_ttl = usize::try_from(self.config.idle_ttl_seconds)
            .unwrap_or(usize::MAX)
            .max(1);
        builder.control_persist(ControlPersist::IdleFor(
            NonZeroUsize::new(idle_ttl).unwrap_or(NonZeroUsize::MIN),
        ));
        let connect = builder.connect_mux(&self.config.destination);
        let session = tokio::time::timeout(
            Duration::from_secs(self.config.connect_timeout_seconds),
            connect,
        )
        .await;
        let session = match session {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => {
                self.telemetry.disconnected();
                return Err(TransportError::Backend(error.to_string()));
            }
            Err(_) => {
                self.telemetry.disconnected();
                return Err(TransportError::Timeout);
            }
        };
        let session = Arc::new(session);
        *guard = Some(Arc::clone(&session));
        self.telemetry.handshake_succeeded(now_utc());
        Ok(session)
    }

    async fn acquire_channel(&self) -> Result<OwnedSemaphorePermit, TransportError> {
        self.channel_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::Backend("SSH channel pool is closed".to_owned()))
    }

    fn try_acquire_pty_channel(&self) -> Result<OwnedSemaphorePermit, ConnectorPtyError> {
        match self.channel_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                Err(ConnectorPtyError::ChannelCapacityUnavailable)
            }
            Err(tokio::sync::TryAcquireError::Closed) => Err(ConnectorPtyError::Transport(
                TransportError::Backend("SSH channel pool is closed".to_owned()),
            )),
        }
    }
}

#[cfg(unix)]
#[async_trait]
impl RemoteTransport for OpenSshTransport {
    fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
        Some(self.telemetry.snapshot())
    }

    async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
        let session = self.session().await?;
        session
            .check()
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?;
        Ok(CheckResult {
            ok: true,
            latency_ms: None,
            message: "openssh control master is healthy".to_owned(),
        })
    }

    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, TransportError> {
        let _channel_permit = self.acquire_channel().await?;
        let session = self.session().await?;
        let mut command = session.command(&request.profile.program);
        command.args(&request.profile.args);
        let output = tokio::time::timeout(
            Duration::from_secs(request.profile.timeout_seconds),
            command.output(),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|error| TransportError::Backend(error.to_string()))?;

        Ok(ExecResult {
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            truncated: false,
        })
    }

    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
        request
            .spec
            .validate()
            .map_err(|error| TransportError::FileTransfer(error.to_string()))?;
        let timeout = Duration::from_secs(request.spec.timeout_seconds);
        let _channel_permit = self.acquire_channel().await?;
        let session = self.session().await?;
        tokio::time::timeout(timeout, execute_openssh_sftp(session, request))
            .await
            .map_err(|_| TransportError::Timeout)?
    }

    async fn open_forward(
        &self,
        _request: ForwardRequest,
    ) -> Result<ForwardHandle, TransportError> {
        Err(TransportError::Backend(
            "OpenSSH port forwarding is not implemented yet".to_owned(),
        ))
    }
}

#[cfg(unix)]
async fn execute_openssh_sftp(
    session: Arc<Session>,
    request: SftpRequest,
) -> Result<SftpResult, TransportError> {
    let sftp = OpenSshSftp::from_clonable_session(session, SftpOptions::default())
        .await
        .map_err(|error| {
            TransportError::FileTransfer(format!("SFTP subsystem unavailable: {error}"))
        })?;
    let result = match request.spec.direction {
        SftpDirection::Upload => openssh_upload(&sftp, &request).await,
        SftpDirection::Download => openssh_download(&sftp, &request).await,
    };
    let close_result = sftp
        .close()
        .await
        .map_err(|error| TransportError::FileTransfer(format!("close SFTP subsystem: {error}")));
    match (result, close_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

#[cfg(unix)]
async fn openssh_upload(
    sftp: &OpenSshSftp,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError> {
    let spec = &request.spec;
    let (local_size, local_sha256) =
        hash_local_source(Path::new(&spec.local_path), spec.max_size_bytes).await?;
    ensure_expected_sha256(spec, &local_sha256)?;

    let mut fs = sftp.fs();
    ensure_openssh_remote_parent(&mut fs, &spec.remote_path).await?;
    ensure_openssh_remote_destination(&mut fs, &spec.remote_path, spec.overwrite).await?;
    let temporary_path = remote_temporary_path(&spec.remote_path, request.operation_id)?;
    cleanup_openssh_temporary_file(&mut fs, &temporary_path).await?;

    let transfer = async {
        let mut local = tokio::fs::File::open(&spec.local_path)
            .await
            .map_err(file_transfer_io)?;
        let mut options = sftp.options();
        options.write(true).create_new(true);
        let remote = options
            .open(&temporary_path)
            .await
            .map_err(openssh_file_transfer_error)?;
        let mut remote = Box::pin(OpenSshSftpFile::from(remote));
        let (bytes_transferred, transfer_sha256) = copy_bounded_and_hash(
            Pin::new(&mut local),
            remote.as_mut(),
            spec.max_size_bytes,
            request.progress_tx.as_ref(),
            "uploading",
            Some(local_size),
        )
        .await?;
        remote.as_mut().shutdown().await.map_err(file_transfer_io)?;
        drop(remote);
        if bytes_transferred != local_size || transfer_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(
                "local file changed while it was being uploaded".to_owned(),
            ));
        }
        if let Some(mode) = spec.mode {
            let mode = u16::try_from(mode).map_err(|error| {
                TransportError::FileTransfer(format!("invalid remote permission mode: {error}"))
            })?;
            fs.set_permissions(&temporary_path, OpenSshSftpPermissions::from(mode))
                .await
                .map_err(openssh_file_transfer_error)?;
        }
        let (remote_size, remote_sha256) =
            hash_openssh_remote_file(sftp, &temporary_path, spec.max_size_bytes).await?;
        if remote_size != local_size || remote_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(format!(
                "remote SHA-256 verification failed: local={local_sha256}, remote={remote_sha256}"
            )));
        }
        fs.rename(&temporary_path, &spec.remote_path)
            .await
            .map_err(|error| {
                TransportError::FileTransfer(format!(
                    "atomic remote placement failed; destination may not support the requested overwrite policy: {error}"
                ))
            })?;
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred,
            sha256: local_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }
    .await;

    if transfer.is_err() {
        let _ = fs.remove_file(&temporary_path).await;
    }
    transfer
}

#[cfg(unix)]
async fn openssh_download(
    sftp: &OpenSshSftp,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError> {
    let spec = &request.spec;
    let source = openssh_lstat(&mut sftp.fs(), &spec.remote_path)
        .await?
        .ok_or_else(|| TransportError::FileTransfer("remote source does not exist".to_owned()))?;
    ensure_openssh_regular_file(&source, "remote source")?;
    let source_size = source
        .len()
        .ok_or_else(|| TransportError::FileTransfer("remote source size is unknown".to_owned()))?;
    ensure_size_within_limit(source_size, spec.max_size_bytes)?;

    let destination = Path::new(&spec.local_path);
    ensure_local_destination(destination, spec.overwrite).await?;
    let temporary_path = local_temporary_path(destination, request.operation_id)?;
    cleanup_local_temporary_file(&temporary_path).await?;

    let transfer = async {
        let remote = sftp
            .open(&spec.remote_path)
            .await
            .map_err(openssh_file_transfer_error)?;
        let mut remote = Box::pin(OpenSshSftpFile::from(remote));
        let mut local = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .await
            .map_err(file_transfer_io)?;
        let (bytes_transferred, remote_sha256) = copy_bounded_and_hash(
            remote.as_mut(),
            Pin::new(&mut local),
            spec.max_size_bytes,
            request.progress_tx.as_ref(),
            "downloading",
            Some(source_size),
        )
        .await?;
        local.shutdown().await.map_err(file_transfer_io)?;
        drop(remote);
        if bytes_transferred != source_size {
            return Err(TransportError::FileTransfer(format!(
                "remote source changed while it was being downloaded: expected_bytes={source_size}, transferred_bytes={bytes_transferred}"
            )));
        }
        ensure_expected_sha256(spec, &remote_sha256)?;
        let (local_size, local_sha256) =
            hash_local_source(&temporary_path, spec.max_size_bytes).await?;
        if local_size != source_size || local_sha256 != remote_sha256 {
            return Err(TransportError::FileTransfer(format!(
                "local SHA-256 verification failed: remote={remote_sha256}, local={local_sha256}"
            )));
        }
        if let Some(mode) = spec.mode {
            set_local_mode(&temporary_path, mode).await?;
        }
        place_local_file(&temporary_path, destination, spec.overwrite).await?;
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred,
            sha256: remote_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }
    .await;

    if transfer.is_err() {
        let _ = tokio::fs::remove_file(&temporary_path).await;
    }
    transfer
}

#[cfg(unix)]
async fn openssh_lstat(
    fs: &mut OpenSshSftpFs,
    path: &str,
) -> Result<Option<OpenSshSftpMetadata>, TransportError> {
    match fs.symlink_metadata(path).await {
        Ok(metadata) => Ok(Some(metadata)),
        Err(OpenSshSftpError::SftpError(OpenSshSftpErrorKind::NoSuchFile, _)) => Ok(None),
        Err(error) => Err(openssh_file_transfer_error(error)),
    }
}

#[cfg(unix)]
async fn ensure_openssh_remote_parent(
    fs: &mut OpenSshSftpFs,
    path: &str,
) -> Result<(), TransportError> {
    let parent = remote_parent(path)?;
    let metadata = openssh_lstat(fs, parent).await?.ok_or_else(|| {
        TransportError::FileTransfer("remote parent directory is missing".to_owned())
    })?;
    if metadata
        .file_type()
        .is_none_or(|file_type| !file_type.is_dir())
    {
        return Err(TransportError::FileTransfer(
            "remote parent path is not a directory".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
async fn ensure_openssh_remote_destination(
    fs: &mut OpenSshSftpFs,
    path: &str,
    overwrite: SftpOverwritePolicy,
) -> Result<(), TransportError> {
    let Some(metadata) = openssh_lstat(fs, path).await? else {
        return Ok(());
    };
    ensure_openssh_regular_file(&metadata, "remote destination")?;
    if overwrite == SftpOverwritePolicy::Deny {
        return Err(TransportError::FileTransfer(
            "remote destination already exists and overwrite=deny".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_openssh_regular_file(
    metadata: &OpenSshSftpMetadata,
    label: &str,
) -> Result<(), TransportError> {
    if metadata
        .file_type()
        .is_none_or(|file_type| !file_type.is_file())
    {
        return Err(TransportError::FileTransfer(format!(
            "{label} is not a regular file"
        )));
    }
    Ok(())
}

#[cfg(unix)]
async fn cleanup_openssh_temporary_file(
    fs: &mut OpenSshSftpFs,
    path: &str,
) -> Result<(), TransportError> {
    let Some(metadata) = openssh_lstat(fs, path).await? else {
        return Ok(());
    };
    ensure_openssh_regular_file(&metadata, "stale remote temporary path")?;
    fs.remove_file(path)
        .await
        .map_err(openssh_file_transfer_error)
}

#[cfg(unix)]
async fn hash_openssh_remote_file(
    sftp: &OpenSshSftp,
    path: &str,
    max_size_bytes: u64,
) -> Result<(u64, String), TransportError> {
    let remote = sftp.open(path).await.map_err(openssh_file_transfer_error)?;
    let mut remote = Box::pin(OpenSshSftpFile::from(remote));
    hash_reader(remote.as_mut(), max_size_bytes).await
}

#[cfg(unix)]
fn openssh_file_transfer_error(error: OpenSshSftpError) -> TransportError {
    let message = error.to_string();
    drop(error);
    TransportError::FileTransfer(message)
}

async fn hash_local_source(
    path: &Path,
    max_size_bytes: u64,
) -> Result<(u64, String), TransportError> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(file_transfer_io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(TransportError::FileTransfer(
            "local source must be a regular file and cannot be a symlink".to_owned(),
        ));
    }
    ensure_size_within_limit(metadata.len(), max_size_bytes)?;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(file_transfer_io)?;
    let result = hash_reader(Pin::new(&mut file), max_size_bytes).await?;
    if result.0 != metadata.len() {
        return Err(TransportError::FileTransfer(
            "local source changed while it was being hashed".to_owned(),
        ));
    }
    Ok(result)
}

async fn hash_reader<R>(
    mut reader: Pin<&mut R>,
    max_size_bytes: u64,
) -> Result<(u64, String), TransportError>
where
    R: AsyncRead + ?Sized,
{
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader
            .as_mut()
            .read(&mut buffer)
            .await
            .map_err(file_transfer_io)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(file_transfer_conversion)?)
            .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
        ensure_size_within_limit(total, max_size_bytes)?;
        hasher.update(&buffer[..read]);
    }
    Ok((total, format!("{:x}", hasher.finalize())))
}

async fn copy_bounded_and_hash<R, W>(
    mut reader: Pin<&mut R>,
    mut writer: Pin<&mut W>,
    max_size_bytes: u64,
    progress_tx: Option<&mpsc::UnboundedSender<SftpProgress>>,
    stage: &str,
    total_bytes: Option<u64>,
) -> Result<(u64, String), TransportError>
where
    R: AsyncRead + ?Sized,
    W: AsyncWrite + ?Sized,
{
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut next_progress_bytes = 8 * 1024 * 1024_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader
            .as_mut()
            .read(&mut buffer)
            .await
            .map_err(file_transfer_io)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(file_transfer_conversion)?)
            .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
        ensure_size_within_limit(total, max_size_bytes)?;
        writer
            .as_mut()
            .write_all(&buffer[..read])
            .await
            .map_err(file_transfer_io)?;
        hasher.update(&buffer[..read]);
        if total >= next_progress_bytes {
            emit_sftp_progress_to(progress_tx, stage, total, total_bytes, 0, 0);
            while next_progress_bytes <= total {
                next_progress_bytes = next_progress_bytes.saturating_add(8 * 1024 * 1024);
            }
        }
    }
    writer.as_mut().flush().await.map_err(file_transfer_io)?;
    emit_sftp_progress_to(progress_tx, stage, total, total_bytes, 0, 0);
    Ok((total, format!("{:x}", hasher.finalize())))
}

fn ensure_size_within_limit(size: u64, max_size_bytes: u64) -> Result<(), TransportError> {
    if size > max_size_bytes {
        return Err(TransportError::FileTransfer(format!(
            "file exceeds max_size_bytes: size={size}, max_size_bytes={max_size_bytes}"
        )));
    }
    Ok(())
}

fn ensure_expected_sha256(
    spec: &FileTransferSpec,
    actual_sha256: &str,
) -> Result<(), TransportError> {
    if spec
        .expected_sha256
        .as_deref()
        .is_some_and(|expected| !expected.eq_ignore_ascii_case(actual_sha256))
    {
        return Err(TransportError::FileTransfer(format!(
            "SHA-256 mismatch: expected={}, actual={actual_sha256}",
            spec.expected_sha256.as_deref().unwrap_or_default()
        )));
    }
    Ok(())
}

async fn ensure_local_destination(
    path: &Path,
    overwrite: SftpOverwritePolicy,
) -> Result<(), TransportError> {
    let parent = path.parent().ok_or_else(|| {
        TransportError::FileTransfer("local destination parent is missing".to_owned())
    })?;
    let parent_metadata = tokio::fs::metadata(parent)
        .await
        .map_err(file_transfer_io)?;
    if !parent_metadata.is_dir() {
        return Err(TransportError::FileTransfer(
            "local destination parent is not a directory".to_owned(),
        ));
    }
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(TransportError::FileTransfer(
                    "local destination exists but is not a regular file".to_owned(),
                ));
            }
            if overwrite == SftpOverwritePolicy::Deny {
                return Err(TransportError::FileTransfer(
                    "local destination already exists and overwrite=deny".to_owned(),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(file_transfer_io(error)),
    }
    Ok(())
}

async fn cleanup_local_temporary_file(path: &Path) -> Result<(), TransportError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(TransportError::FileTransfer(
                    "stale local temporary path is not a regular file".to_owned(),
                ));
            }
            tokio::fs::remove_file(path)
                .await
                .map_err(file_transfer_io)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(file_transfer_io(error)),
    }
    Ok(())
}

async fn place_local_file(
    temporary_path: &Path,
    destination: &Path,
    overwrite: SftpOverwritePolicy,
) -> Result<(), TransportError> {
    match overwrite {
        SftpOverwritePolicy::Deny => {
            tokio::fs::hard_link(temporary_path, destination)
                .await
                .map_err(|error| {
                    TransportError::FileTransfer(format!(
                        "atomic no-overwrite placement failed: {error}"
                    ))
                })?;
            tokio::fs::remove_file(temporary_path)
                .await
                .map_err(file_transfer_io)
        }
        SftpOverwritePolicy::Replace => tokio::fs::rename(temporary_path, destination)
            .await
            .map_err(|error| {
                TransportError::FileTransfer(format!("atomic local replacement failed: {error}"))
            }),
    }
}

#[cfg(unix)]
async fn set_local_mode(path: &Path, mode: u32) -> Result<(), TransportError> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .await
        .map_err(file_transfer_io)
}

#[cfg(not(unix))]
fn set_local_mode(_path: &Path, _mode: u32) -> std::future::Ready<Result<(), TransportError>> {
    std::future::ready(Err(TransportError::FileTransfer(
        "local permission modes are unsupported on this connector platform".to_owned(),
    )))
}

fn local_temporary_path(
    destination: &Path,
    operation_id: OperationId,
) -> Result<PathBuf, TransportError> {
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| TransportError::FileTransfer("invalid local file name".to_owned()))?;
    Ok(destination.with_file_name(format!(".{file_name}.remote-hosts-{operation_id}.part")))
}

fn remote_temporary_path(
    destination: &str,
    operation_id: OperationId,
) -> Result<String, TransportError> {
    let separator = destination.rfind('/').ok_or_else(|| {
        TransportError::FileTransfer("remote destination has no parent".to_owned())
    })?;
    let file_name = &destination[separator + 1..];
    Ok(format!(
        "{}.{file_name}.remote-hosts-{operation_id}.part",
        &destination[..=separator]
    ))
}

fn resumable_remote_temporary_path(destination: &str, sha256: &str) -> String {
    let (parent, file_name) = destination.rsplit_once('/').unwrap_or(("", destination));
    let digest_prefix = sha256.get(..16).unwrap_or(sha256);
    format!("{parent}/.{file_name}.remote-hosts-{digest_prefix}.part")
}

fn streaming_remote_temporary_path(destination: &str, sha256: &str) -> String {
    let (parent, file_name) = destination.rsplit_once('/').unwrap_or(("", destination));
    let digest_prefix = sha256.get(..16).unwrap_or(sha256);
    format!("{parent}/.{file_name}.remote-hosts-stream-{digest_prefix}.part")
}

fn remote_parent(path: &str) -> Result<&str, TransportError> {
    let separator = path.rfind('/').ok_or_else(|| {
        TransportError::FileTransfer("remote path has no parent directory".to_owned())
    })?;
    Ok(&path[..=separator])
}

fn file_transfer_io(error: std::io::Error) -> TransportError {
    let message = error.to_string();
    drop(error);
    TransportError::FileTransfer(message)
}

fn file_transfer_io_context(context: &str, error: std::io::Error) -> TransportError {
    let message = error.to_string();
    drop(error);
    TransportError::FileTransfer(format!("{context}: {message}"))
}

fn file_transfer_context(context: &str, error: TransportError) -> TransportError {
    match error {
        TransportError::FileTransfer(message) => {
            TransportError::FileTransfer(format!("{context}: {message}"))
        }
        other => other,
    }
}

fn file_transfer_conversion(error: std::num::TryFromIntError) -> TransportError {
    TransportError::FileTransfer(error.to_string())
}

/// Request to spawn a managed persistent shell for a PTY session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyBackendSpawnRequest {
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Existing connection session id.
    pub session_id: SessionId,
    /// Initial working directory.
    pub cwd: Option<String>,
}

/// Output emitted by a managed PTY backend.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyBackendOutput {
    /// Output stream.
    pub stream: OutputStream,
    /// Visible output text.
    pub text: String,
    /// Whether the backend already truncated the output.
    pub truncated: bool,
}

/// Running managed PTY process handles.
pub struct ManagedPtyProcess {
    input_tx: mpsc::Sender<String>,
    output_rx: mpsc::Receiver<PtyBackendOutput>,
    close_tx: oneshot::Sender<()>,
    transport_telemetry: Option<SshTransportTelemetry>,
    transport_evidence: Option<SshChannelTransportEvidence>,
    channel_permit: Option<OwnedSemaphorePermit>,
}

impl ManagedPtyProcess {
    /// Creates process handles from channels.
    pub fn new(
        input_tx: mpsc::Sender<String>,
        output_rx: mpsc::Receiver<PtyBackendOutput>,
        close_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            input_tx,
            output_rx,
            close_tx,
            transport_telemetry: None,
            transport_evidence: None,
            channel_permit: None,
        }
    }

    /// Attaches SSH transport telemetry and evidence observed while opening the PTY channel.
    #[must_use]
    pub fn with_transport_observation(
        mut self,
        before: Option<&SshTransportTelemetry>,
        after: Option<SshTransportTelemetry>,
    ) -> Self {
        if let Some(after) = after {
            self.transport_evidence = Some(SshChannelTransportEvidence::between(
                SshChannelKind::Pty,
                before,
                &after,
                now_utc(),
            ));
            self.transport_telemetry = Some(after);
        }
        self
    }

    fn with_channel_permit(mut self, permit: OwnedSemaphorePermit) -> Self {
        self.channel_permit = Some(permit);
        self
    }
}

/// Backend capable of spawning a persistent shell for a PTY session.
#[async_trait]
pub trait ManagedPtyBackend: Send + Sync {
    /// Returns the backend capabilities that should be visible to agents.
    fn capabilities(&self) -> PtyBackendCapabilities;

    /// Spawns a managed PTY-like persistent process.
    async fn spawn(
        &self,
        request: PtyBackendSpawnRequest,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError>;
}

/// OpenSSH-backed persistent shell backend.
#[cfg(unix)]
pub struct OpenSshShellBackend {
    transport: Arc<OpenSshTransport>,
}

#[cfg(unix)]
impl OpenSshShellBackend {
    /// Creates a backend from an OpenSSH transport.
    pub fn new(transport: Arc<OpenSshTransport>) -> Self {
        Self { transport }
    }
}

#[cfg(unix)]
#[async_trait]
impl ManagedPtyBackend for OpenSshShellBackend {
    fn capabilities(&self) -> PtyBackendCapabilities {
        PtyBackendCapabilities::openssh_pipe_shell()
    }

    async fn spawn(
        &self,
        request: PtyBackendSpawnRequest,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError> {
        let channel_permit = self.transport.try_acquire_pty_channel()?;
        let before = self.transport.transport_telemetry();
        let session = self.transport.session().await?;
        let mut command = session.arc_command("sh");
        command
            .arg("-lc")
            .arg(shell_start_script(request.cwd.as_deref()))
            .stdin(OpenSshStdio::piped())
            .stdout(OpenSshStdio::piped())
            .stderr(OpenSshStdio::piped());
        let mut child = command
            .spawn()
            .await
            .map_err(|error| ConnectorPtyError::Backend(error.to_string()))?;
        let stdin = child.stdin().take();
        let stdout = child.stdout().take();
        let stderr = child.stderr().take();
        let (input_tx, mut input_rx) = mpsc::channel::<String>(64);
        let (output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(128);
        let (close_tx, close_rx) = oneshot::channel::<()>();
        let writer_input_tx = input_tx.clone();

        if let Some(mut stdin) = stdin {
            tokio::spawn(async move {
                while let Some(input) = input_rx.recv().await {
                    if stdin.write_all(input.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
        if let Some(stdout) = stdout {
            spawn_reader_task(stdout, OutputStream::Stdout, output_tx.clone());
        }
        if let Some(stderr) = stderr {
            spawn_reader_task(stderr, OutputStream::Stderr, output_tx.clone());
        }
        tokio::spawn(async move {
            let wait = child.wait();
            tokio::pin!(wait);
            tokio::select! {
                _ = close_rx => {
                    drop(writer_input_tx);
                    let _ = wait.await;
                }
                result = &mut wait => {
                    let text = match result {
                        Ok(status) => format!("remote shell exited: status={status}"),
                        Err(error) => format!("remote shell wait failed: {error}"),
                    };
                    let _ = output_tx
                        .send(PtyBackendOutput {
                            stream: OutputStream::System,
                            text,
                            truncated: false,
                        })
                        .await;
                }
            }
        });

        Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx)
            .with_channel_permit(channel_permit)
            .with_transport_observation(before.as_ref(), self.transport.transport_telemetry()))
    }
}

/// OpenSSH `ControlMaster` backend mode for persistent shell sessions.
#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenSshManagedPtyBackendMode {
    /// Compatibility mode through the `openssh` native mux API and plain pipes.
    #[default]
    PipeShell,
    /// True terminal mode through a local `ssh -tt` child reusing the `ControlMaster` socket.
    ControlMasterTty,
}

/// OpenSSH-backed true terminal backend using an existing `ControlMaster` socket.
#[cfg(unix)]
pub struct OpenSshControlMasterTtyBackend {
    transport: Arc<OpenSshTransport>,
    ssh_binary: PathBuf,
    term: String,
    columns: u32,
    rows: u32,
}

#[cfg(unix)]
impl OpenSshControlMasterTtyBackend {
    /// Creates a backend that invokes the local `ssh` binary.
    pub fn new(transport: Arc<OpenSshTransport>) -> Self {
        Self {
            transport,
            ssh_binary: PathBuf::from("ssh"),
            term: std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_owned()),
            columns: 120,
            rows: 40,
        }
    }

    /// Overrides local `ssh` process settings.
    #[must_use]
    pub fn with_options(
        mut self,
        ssh_binary: impl Into<PathBuf>,
        term: impl Into<String>,
        columns: u32,
        rows: u32,
    ) -> Self {
        self.ssh_binary = ssh_binary.into();
        self.term = term.into();
        self.columns = columns.max(1);
        self.rows = rows.max(1);
        self
    }
}

#[cfg(unix)]
#[async_trait]
impl ManagedPtyBackend for OpenSshControlMasterTtyBackend {
    fn capabilities(&self) -> PtyBackendCapabilities {
        PtyBackendCapabilities::openssh_control_master_tty()
    }

    async fn spawn(
        &self,
        request: PtyBackendSpawnRequest,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError> {
        let channel_permit = self.transport.try_acquire_pty_channel()?;
        let before = self.transport.transport_telemetry();
        let session = self.transport.session().await?;
        let control_socket = session.control_socket().to_path_buf();
        let mut command = LocalCommand::new(&self.ssh_binary);
        command
            .arg("-S")
            .arg(control_socket)
            .arg("-tt")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("LogLevel=ERROR")
            .arg(&self.transport.config.destination)
            .env("TERM", &self.term)
            .env("COLUMNS", self.columns.to_string())
            .env("LINES", self.rows.to_string())
            .stdin(ProcessStdio::piped())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());

        let mut child = command
            .spawn()
            .map_err(|error| ConnectorPtyError::Backend(error.to_string()))?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (input_tx, mut input_rx) = mpsc::channel::<String>(64);
        let (output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(128);
        let (close_tx, close_rx) = oneshot::channel::<()>();
        let writer_input_tx = input_tx.clone();

        if let Some(mut stdin) = stdin {
            let initial_input = shell_change_dir_input(request.cwd.as_deref());
            tokio::spawn(async move {
                if let Some(initial_input) = initial_input
                    && stdin.write_all(initial_input.as_bytes()).await.is_err()
                {
                    return;
                }
                while let Some(input) = input_rx.recv().await {
                    if stdin.write_all(input.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
        if let Some(stdout) = stdout {
            spawn_reader_task(stdout, OutputStream::Stdout, output_tx.clone());
        }
        if let Some(stderr) = stderr {
            spawn_reader_task(stderr, OutputStream::Stderr, output_tx.clone());
        }
        spawn_local_child_wait_task(child, close_rx, writer_input_tx, output_tx);

        Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx)
            .with_channel_permit(channel_permit)
            .with_transport_observation(before.as_ref(), self.transport.transport_telemetry()))
    }
}

/// OpenSSH-backed PTY backend factory keyed by access path.
#[cfg(unix)]
pub struct OpenSshPtyBackendFactory {
    repositories: Repositories,
    pool: Arc<OpenSshTransportPool>,
    mode: OpenSshManagedPtyBackendMode,
}

#[cfg(unix)]
impl OpenSshPtyBackendFactory {
    /// Creates an OpenSSH PTY backend factory.
    pub fn new(
        repositories: Repositories,
        host_key_policy: HostKeyPolicy,
        connect_timeout_seconds: u64,
    ) -> Self {
        let pool = Arc::new(OpenSshTransportPool::new(
            repositories.clone(),
            host_key_policy,
            connect_timeout_seconds,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        ));
        Self::with_pool(repositories, pool)
    }

    /// Creates an OpenSSH PTY backend factory from a shared raw transport pool.
    pub fn with_pool(repositories: Repositories, pool: Arc<OpenSshTransportPool>) -> Self {
        Self {
            repositories,
            pool,
            mode: OpenSshManagedPtyBackendMode::default(),
        }
    }

    /// Sets the OpenSSH persistent shell backend mode.
    #[must_use]
    pub fn with_mode(mut self, mode: OpenSshManagedPtyBackendMode) -> Self {
        self.mode = mode;
        self
    }

    async fn transport_for_session(
        &self,
        session_id: SessionId,
    ) -> Result<Arc<OpenSshTransport>, ConnectorPtyError> {
        let connection = self
            .repositories
            .connection_sessions
            .get(session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("connection session not found: {session_id}"))
            })?;
        self.pool
            .transport_for_access_path(connection.access_path_id)
            .await
            .map_err(ConnectorPtyError::Backend)
    }
}

#[cfg(unix)]
#[async_trait]
impl ManagedPtyBackend for OpenSshPtyBackendFactory {
    fn capabilities(&self) -> PtyBackendCapabilities {
        match self.mode {
            OpenSshManagedPtyBackendMode::PipeShell => PtyBackendCapabilities::openssh_pipe_shell(),
            OpenSshManagedPtyBackendMode::ControlMasterTty => {
                PtyBackendCapabilities::openssh_control_master_tty()
            }
        }
    }

    async fn spawn(
        &self,
        request: PtyBackendSpawnRequest,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError> {
        let transport = self.transport_for_session(request.session_id).await?;
        match self.mode {
            OpenSshManagedPtyBackendMode::PipeShell => {
                OpenSshShellBackend::new(transport).spawn(request).await
            }
            OpenSshManagedPtyBackendMode::ControlMasterTty => {
                OpenSshControlMasterTtyBackend::new(transport)
                    .spawn(request)
                    .await
            }
        }
    }
}

#[cfg(unix)]
fn spawn_reader_task<R>(
    mut reader: R,
    stream: OutputStream,
    output_tx: mpsc::Sender<PtyBackendOutput>,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = vec![0_u8; 8192];
        loop {
            let read = match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) => {
                    let _ = output_tx
                        .send(PtyBackendOutput {
                            stream: OutputStream::System,
                            text: format!("pty stream read failed: {error}"),
                            truncated: false,
                        })
                        .await;
                    break;
                }
            };
            let text = String::from_utf8_lossy(&buffer[..read]).to_string();
            if output_tx
                .send(PtyBackendOutput {
                    stream: stream.clone(),
                    text,
                    truncated: false,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
}

#[cfg(unix)]
fn spawn_local_child_wait_task(
    mut child: LocalChild,
    close_rx: oneshot::Receiver<()>,
    writer_input_tx: mpsc::Sender<String>,
    output_tx: mpsc::Sender<PtyBackendOutput>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = close_rx => {
                drop(writer_input_tx);
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
            result = child.wait() => {
                let text = match result {
                    Ok(status) => format!("remote tty shell exited: status={status}"),
                    Err(error) => format!("remote tty shell wait failed: {error}"),
                };
                let _ = output_tx
                    .send(PtyBackendOutput {
                        stream: OutputStream::System,
                        text,
                        truncated: false,
                    })
                    .await;
            }
        }
    });
}

/// Transport wrapper that applies command validation, concurrency limits, output limits, and
/// redaction before results become visible to agents.
pub struct GuardedTransport<T> {
    inner: T,
    policy: ServerProtectionPolicy,
    exec_semaphore: Arc<Semaphore>,
    redactor: SecretRedactor,
}

impl<T> GuardedTransport<T> {
    /// Creates a guarded transport.
    pub fn new(inner: T, policy: ServerProtectionPolicy) -> Self {
        let exec_limit = usize::try_from(policy.max_parallel_exec_channels_per_host).unwrap_or(1);
        Self {
            inner,
            policy,
            exec_semaphore: Arc::new(Semaphore::new(exec_limit.max(1))),
            redactor: SecretRedactor::default(),
        }
    }
}

#[async_trait]
impl<T> RemoteTransport for GuardedTransport<T>
where
    T: RemoteTransport,
{
    fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
        self.inner.transport_telemetry()
    }

    async fn check(&self, request: CheckRequest) -> Result<CheckResult, TransportError> {
        self.inner.check(request).await
    }

    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, TransportError> {
        request
            .profile
            .validate()
            .map_err(|error| TransportError::PolicyDenied(error.to_string()))?;

        let _permit = self
            .exec_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                TransportError::PolicyDenied(format!(
                    "parallel exec channel limit reached: {}",
                    self.policy.max_parallel_exec_channels_per_host
                ))
            })?;

        let output_limit = request.profile.output_limit_bytes;
        let mut result = self.inner.exec(request).await?;
        let (stdout, stdout_truncated) =
            redact_and_truncate(&self.redactor, &result.stdout, output_limit);
        let (stderr, stderr_truncated) =
            redact_and_truncate(&self.redactor, &result.stderr, output_limit);
        result.stdout = stdout;
        result.stderr = stderr;
        result.truncated = result.truncated || stdout_truncated || stderr_truncated;
        Ok(result)
    }

    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
        request
            .spec
            .validate()
            .map_err(|error| TransportError::FileTransfer(error.to_string()))?;
        let _permit = self
            .exec_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                TransportError::PolicyDenied(format!(
                    "parallel SSH channel limit reached: {}",
                    self.policy.max_parallel_exec_channels_per_host
                ))
            })?;
        self.inner.sftp(request).await
    }

    async fn open_forward(&self, request: ForwardRequest) -> Result<ForwardHandle, TransportError> {
        self.inner.open_forward(request).await
    }
}

fn redact_and_truncate(
    redactor: &SecretRedactor,
    value: &str,
    output_limit: usize,
) -> (String, bool) {
    let redacted = redactor.redact(value);
    if redacted.len() <= output_limit {
        return (redacted, false);
    }

    let mut truncated = String::from_utf8_lossy(&redacted.as_bytes()[..output_limit]).to_string();
    truncated.push_str("\n<truncated>");
    (truncated, true)
}

/// Provides the pooled transport for a claimed operation.
#[async_trait]
pub trait RemoteTransportProvider: Send + Sync {
    /// Returns a transport for the operation's access path.
    async fn transport_for(
        &self,
        operation: &OperationRun,
    ) -> Result<Arc<dyn RemoteTransport>, String>;
}

/// Static provider useful for one-access-path connectors and tests.
#[derive(Clone)]
pub struct StaticTransportProvider {
    transport: Arc<dyn RemoteTransport>,
}

impl StaticTransportProvider {
    /// Creates a static transport provider.
    pub fn new<T>(transport: T) -> Self
    where
        T: RemoteTransport + 'static,
    {
        Self {
            transport: Arc::new(transport),
        }
    }
}

#[async_trait]
impl RemoteTransportProvider for StaticTransportProvider {
    async fn transport_for(
        &self,
        _operation: &OperationRun,
    ) -> Result<Arc<dyn RemoteTransport>, String> {
        Ok(Arc::clone(&self.transport))
    }
}

#[derive(Clone)]
struct SharedRemoteTransport<T> {
    inner: Arc<T>,
}

impl<T> SharedRemoteTransport<T> {
    fn new(inner: Arc<T>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T> RemoteTransport for SharedRemoteTransport<T>
where
    T: RemoteTransport + 'static,
{
    fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
        self.inner.transport_telemetry()
    }

    async fn check(&self, request: CheckRequest) -> Result<CheckResult, TransportError> {
        self.inner.check(request).await
    }

    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, TransportError> {
        self.inner.exec(request).await
    }

    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
        self.inner.sftp(request).await
    }

    async fn open_forward(&self, request: ForwardRequest) -> Result<ForwardHandle, TransportError> {
        self.inner.open_forward(request).await
    }
}

fn access_path_requires_multi_hop(access_path: &AccessPath) -> bool {
    access_path.requires_multi_hop_transport()
}

async fn reject_unsupported_multi_hop_route<T>(
    repositories: &Repositories,
    cache: &Mutex<BTreeMap<AccessPathId, Arc<T>>>,
    access_path: &AccessPath,
    backend: &str,
) -> Result<(), String> {
    if !access_path_requires_multi_hop(access_path) {
        return Ok(());
    }

    cache.lock().await.remove(&access_path.id);
    let now = now_utc();
    let state = AuthorizedKeyBootstrap {
        access_path_id: access_path.id,
        state: AuthorizedKeyBootstrapState::Skipped,
        reason: Some(AuthorizedKeyBootstrapReason::MultiHopUnsupported),
        public_key_fingerprint: None,
        failure_count: 0,
        attempted_at: now,
        next_retry_at: None,
        updated_at: now,
    };
    if let Err(error) = repositories.authorized_key_bootstrap.upsert(&state).await {
        tracing::warn!(
            access_path_id = %access_path.id,
            %error,
            "failed to persist unsupported multi-hop route state"
        );
    }
    Err(format!(
        "multi-hop SSH route is not supported by the active {backend} transport; connection rejected before SSH handshake"
    ))
}

/// Shared OpenSSH transport pool with one raw ControlMaster-backed transport per access path.
#[cfg(unix)]
pub struct OpenSshTransportPool {
    repositories: Repositories,
    host_key_policy: HostKeyPolicy,
    connect_timeout_seconds: u64,
    max_new_ssh_handshakes_per_10_min: u32,
    global_handshake_limiter: SharedHandshakeBudget,
    cache: Mutex<BTreeMap<AccessPathId, Arc<OpenSshTransport>>>,
}

#[cfg(unix)]
impl OpenSshTransportPool {
    /// Creates a raw OpenSSH transport pool.
    pub fn new(
        repositories: Repositories,
        host_key_policy: HostKeyPolicy,
        connect_timeout_seconds: u64,
        max_new_ssh_handshakes_per_10_min: u32,
    ) -> Self {
        Self {
            repositories,
            host_key_policy,
            connect_timeout_seconds,
            max_new_ssh_handshakes_per_10_min,
            global_handshake_limiter: HandshakeLimiter::shared_global(
                max_new_ssh_handshakes_per_10_min,
            ),
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns the shared raw OpenSSH transport for an access path.
    ///
    /// # Errors
    ///
    /// Returns an error when the access path cannot be loaded.
    pub async fn transport_for_access_path(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<Arc<OpenSshTransport>, String> {
        let access_path = self
            .repositories
            .access_paths
            .get(access_path_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("access path not found: {access_path_id}"))?;
        reject_unsupported_multi_hop_route(
            &self.repositories,
            &self.cache,
            &access_path,
            "OpenSSH",
        )
        .await?;
        let config = OpenSshTransportConfig {
            destination: OpenSshTransport::destination(
                &access_path.username,
                &access_path.address,
                access_path.port,
            ),
            host_key_policy: self.host_key_policy,
            connect_timeout_seconds: self.connect_timeout_seconds,
            idle_ttl_seconds: access_path.idle_ttl_seconds,
            keepalive_seconds: access_path.keepalive_seconds,
            max_concurrent_channels: access_path.max_concurrent_channels,
            max_new_connections_per_minute: access_path.max_new_connections_per_minute,
            max_new_ssh_handshakes_per_10_min: self.max_new_ssh_handshakes_per_10_min,
        };
        {
            let mut cache = self.cache.lock().await;
            if let Some(transport) = cache.get(&access_path_id) {
                if transport.config == config {
                    return Ok(Arc::clone(transport));
                }
                cache.remove(&access_path_id);
            }
        }
        let transport = Arc::new(OpenSshTransport::with_shared_handshake_budget(
            config,
            Arc::clone(&self.global_handshake_limiter),
        ));

        let mut cache = self.cache.lock().await;
        let cached = cache
            .entry(access_path_id)
            .or_insert_with(|| Arc::clone(&transport));
        Ok(Arc::clone(cached))
    }
}

/// OpenSSH transport provider with one cached guarded transport per access path.
#[cfg(unix)]
pub struct OpenSshTransportProvider {
    pool: Arc<OpenSshTransportPool>,
    policy: ServerProtectionPolicy,
    cache: Mutex<BTreeMap<AccessPathId, OpenSshGuardedTransportCacheEntry>>,
}

#[cfg(unix)]
struct OpenSshGuardedTransportCacheEntry {
    pooled: Arc<OpenSshTransport>,
    transport: Arc<dyn RemoteTransport>,
}

#[cfg(unix)]
impl OpenSshTransportProvider {
    /// Creates an OpenSSH provider.
    pub fn new(
        repositories: Repositories,
        host_key_policy: HostKeyPolicy,
        connect_timeout_seconds: u64,
        policy: ServerProtectionPolicy,
    ) -> Self {
        let pool = Arc::new(OpenSshTransportPool::new(
            repositories,
            host_key_policy,
            connect_timeout_seconds,
            policy.max_new_ssh_handshakes_per_10_min,
        ));
        Self::with_pool(pool, policy)
    }

    /// Creates a provider from a shared raw OpenSSH transport pool.
    pub fn with_pool(pool: Arc<OpenSshTransportPool>, policy: ServerProtectionPolicy) -> Self {
        Self {
            pool,
            policy,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    async fn cached_transport(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<Arc<dyn RemoteTransport>, String> {
        let pooled = self.pool.transport_for_access_path(access_path_id).await?;
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&access_path_id)
                && Arc::ptr_eq(&entry.pooled, &pooled)
            {
                return Ok(Arc::clone(&entry.transport));
            }
        }
        let transport = GuardedTransport::new(
            SharedRemoteTransport::new(Arc::clone(&pooled)),
            self.policy.clone(),
        );
        let transport: Arc<dyn RemoteTransport> = Arc::new(transport);
        let mut cache = self.cache.lock().await;
        cache.insert(
            access_path_id,
            OpenSshGuardedTransportCacheEntry {
                pooled,
                transport: Arc::clone(&transport),
            },
        );
        Ok(transport)
    }
}

#[cfg(unix)]
#[async_trait]
impl RemoteTransportProvider for OpenSshTransportProvider {
    async fn transport_for(
        &self,
        operation: &OperationRun,
    ) -> Result<Arc<dyn RemoteTransport>, String> {
        self.cached_transport(operation.access_path_id).await
    }
}

/// Credential material resolved inside the connector process for native SSH.
///
/// This type intentionally does not implement `Debug` or `Serialize`.
pub struct SshCredentialSecret {
    password: Option<SecretString>,
    private_key_pem: Option<SecretString>,
    private_key_passphrase: Option<SecretString>,
    use_ssh_agent: bool,
}

impl SshCredentialSecret {
    /// Creates a secret from an internal vault secret.
    #[must_use]
    pub fn from_vault_secret(mut secret: CredentialSecret) -> Self {
        Self {
            password: secret.password.take().map(SecretString::from),
            private_key_pem: secret.private_key_pem.take().map(SecretString::from),
            private_key_passphrase: secret.private_key_passphrase.take().map(SecretString::from),
            use_ssh_agent: secret.use_ssh_agent,
        }
    }

    fn has_ssh_material(&self) -> bool {
        self.password.is_some() || self.private_key_pem.is_some() || self.use_ssh_agent
    }
}

/// Errors while resolving connector-local SSH credential material.
#[derive(Debug, thiserror::Error)]
pub enum SshCredentialError {
    /// Credential row is missing.
    #[error("credential not found")]
    NotFound,
    /// Credential row cannot be decoded.
    #[error("credential blob is invalid")]
    InvalidBlob,
    /// Vault is locked or the provided master password cannot decrypt the credential.
    #[error("credential vault is locked or cannot decrypt this credential")]
    VaultLocked,
    /// Credential does not contain SSH authentication material.
    #[error("credential does not contain ssh password or private key material")]
    MissingSshMaterial,
    /// Credential does not contain an SSH password.
    #[error("credential does not contain an ssh password")]
    MissingSshPassword,
    /// Credential does not contain a dedicated sudo password.
    #[error("credential does not contain a sudo password")]
    MissingSudoPassword,
    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] DbError),
}

/// Resolves SSH credentials for native connector transports.
#[async_trait]
pub trait SshCredentialProvider: Send + Sync {
    /// Returns SSH authentication material for an access path.
    async fn credential_for(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<SshCredentialSecret, SshCredentialError>;

    /// Returns a sudo password for one access path when the connector needs to answer a live
    /// sudo prompt. Implementations must never expose it outside the connector process.
    async fn sudo_password_for(
        &self,
        _access_path_id: AccessPathId,
    ) -> Result<SecretString, SshCredentialError> {
        Err(SshCredentialError::MissingSudoPassword)
    }

    /// Returns an SSH password for one access path when the connector needs to answer a live
    /// nested SSH password prompt. Implementations must never expose it outside the connector
    /// process.
    async fn ssh_password_for(
        &self,
        _access_path_id: AccessPathId,
    ) -> Result<SecretString, SshCredentialError> {
        Err(SshCredentialError::MissingSshPassword)
    }
}

/// Vault-backed credential provider for native SSH transports.
pub struct VaultSshCredentialProvider {
    repositories: Repositories,
    master_password: SecretString,
}

impl VaultSshCredentialProvider {
    /// Creates a vault-backed credential provider.
    pub fn new(repositories: Repositories, master_password: SecretString) -> Self {
        Self {
            repositories,
            master_password,
        }
    }
}

fn is_openssh_agent_reference(value: &serde_json::Value) -> bool {
    value.get("type").and_then(serde_json::Value::as_str) == Some("external_reference")
        && value
            .get("external_ref")
            .and_then(serde_json::Value::as_str)
            == Some("openssh-agent")
}

#[async_trait]
impl SshCredentialProvider for VaultSshCredentialProvider {
    async fn credential_for(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<SshCredentialSecret, SshCredentialError> {
        let access_path = self
            .repositories
            .access_paths
            .get(access_path_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        let stored = self
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        if is_openssh_agent_reference(&stored.encrypted_blob_json) {
            return Ok(SshCredentialSecret {
                password: None,
                private_key_pem: None,
                private_key_passphrase: None,
                use_ssh_agent: true,
            });
        }
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)
            .map_err(|_| SshCredentialError::InvalidBlob)?;
        let secret = CredentialVault::decrypt(&self.master_password, &blob)
            .map_err(|_| SshCredentialError::VaultLocked)?;
        let secret = SshCredentialSecret::from_vault_secret(secret);
        if !secret.has_ssh_material() {
            return Err(SshCredentialError::MissingSshMaterial);
        }
        Ok(secret)
    }

    async fn sudo_password_for(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<SecretString, SshCredentialError> {
        let access_path = self
            .repositories
            .access_paths
            .get(access_path_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        let stored = self
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        if is_openssh_agent_reference(&stored.encrypted_blob_json) {
            return Err(SshCredentialError::MissingSudoPassword);
        }
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)
            .map_err(|_| SshCredentialError::InvalidBlob)?;
        let mut secret = CredentialVault::decrypt(&self.master_password, &blob)
            .map_err(|_| SshCredentialError::VaultLocked)?;
        secret
            .sudo_password
            .take()
            .filter(|password| !password.is_empty())
            .map(SecretString::from)
            .ok_or(SshCredentialError::MissingSudoPassword)
    }

    async fn ssh_password_for(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<SecretString, SshCredentialError> {
        let access_path = self
            .repositories
            .access_paths
            .get(access_path_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        let stored = self
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await?
            .ok_or(SshCredentialError::NotFound)?;
        if is_openssh_agent_reference(&stored.encrypted_blob_json) {
            return Err(SshCredentialError::MissingSshPassword);
        }
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)
            .map_err(|_| SshCredentialError::InvalidBlob)?;
        let mut secret = CredentialVault::decrypt(&self.master_password, &blob)
            .map_err(|_| SshCredentialError::VaultLocked)?;
        secret
            .password
            .take()
            .filter(|password| !password.is_empty())
            .map(SecretString::from)
            .ok_or(SshCredentialError::MissingSshPassword)
    }
}

/// Native `russh` transport configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RusshTransportConfig {
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Remote address.
    pub address: String,
    /// Remote SSH port.
    pub port: u16,
    /// Remote username.
    pub username: String,
    /// Whether the target uses Windows OpenSSH shell conventions.
    pub windows: bool,
    /// Use a managed exec data stream when this gateway route cannot carry SFTP writes.
    pub use_exec_file_transfer: bool,
    /// Host key policy.
    pub host_key_policy: HostKeyPolicy,
    /// Optional `known_hosts` path.
    pub known_hosts_path: Option<PathBuf>,
    /// Connect timeout in seconds.
    pub connect_timeout_seconds: u64,
    /// Inactivity timeout in seconds.
    pub inactivity_timeout_seconds: u64,
    /// Idle lifetime after the last business use when no SSH channel remains open.
    pub idle_ttl_seconds: u64,
    /// SSH keepalive interval in seconds.
    pub keepalive_seconds: u64,
    /// Maximum channels sharing this access-path transport.
    pub max_concurrent_channels: u16,
    /// Access-path handshake budget per minute.
    pub max_new_connections_per_minute: u16,
    /// Global handshake budget per ten minutes.
    pub max_new_ssh_handshakes_per_10_min: u32,
}

#[derive(Default)]
struct RusshPtyChannelLifecycle {
    active_channels: usize,
}

impl RusshPtyChannelLifecycle {
    fn reserve(&mut self) {
        assert_ne!(
            self.active_channels,
            usize::MAX,
            "active russh PTY channel count overflowed"
        );
        self.active_channels += 1;
    }

    fn release(&mut self) -> bool {
        assert!(
            self.active_channels > 0,
            "russh PTY channel released without a matching reservation"
        );
        self.active_channels -= 1;
        self.active_channels == 0
    }

    #[cfg(test)]
    fn active_channels(&self) -> usize {
        self.active_channels
    }
}

/// Native async SSH transport backed by `russh`.
pub struct RusshTransport<C> {
    config: RusshTransportConfig,
    credentials: Arc<C>,
    authorized_key_bootstrap: AuthorizedKeyBootstrapRepository,
    session: Mutex<Option<Arc<client::Handle<RusshClientHandler>>>>,
    pty_channel_lifecycle: Mutex<RusshPtyChannelLifecycle>,
    handshake_limiter: HandshakeLimiter,
    channel_semaphore: Arc<Semaphore>,
    telemetry: TransportTelemetryTracker,
}

impl<C> RusshTransport<C> {
    /// Creates a native SSH transport.
    pub fn new(
        config: RusshTransportConfig,
        credentials: Arc<C>,
        authorized_key_bootstrap: AuthorizedKeyBootstrapRepository,
    ) -> Self {
        let global = HandshakeLimiter::shared_global(config.max_new_ssh_handshakes_per_10_min);
        Self::with_shared_handshake_budget(config, credentials, authorized_key_bootstrap, global)
    }

    fn with_shared_handshake_budget(
        config: RusshTransportConfig,
        credentials: Arc<C>,
        authorized_key_bootstrap: AuthorizedKeyBootstrapRepository,
        global: SharedHandshakeBudget,
    ) -> Self {
        let handshake_limiter =
            HandshakeLimiter::with_shared_global(config.max_new_connections_per_minute, global);
        let channel_limit = usize::from(config.max_concurrent_channels.max(1));
        let file_transfer_mode = if config.use_exec_file_transfer {
            SshFileTransferMode::ExecFramed
        } else {
            SshFileTransferMode::Sftp
        };
        Self {
            config,
            credentials,
            authorized_key_bootstrap,
            session: Mutex::new(None),
            pty_channel_lifecycle: Mutex::new(RusshPtyChannelLifecycle::default()),
            handshake_limiter,
            channel_semaphore: Arc::new(Semaphore::new(channel_limit)),
            telemetry: TransportTelemetryTracker::new(
                SshTransportBackend::Russh,
                SshTransportCapabilities::pooled(file_transfer_mode),
            ),
        }
    }
}

impl<C> RusshTransport<C>
where
    C: SshCredentialProvider,
{
    async fn session(&self) -> Result<Arc<client::Handle<RusshClientHandler>>, TransportError> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            let ping_timeout = Duration::from_secs(self.config.connect_timeout_seconds);
            if matches!(
                tokio::time::timeout(ping_timeout, session.send_ping()).await,
                Ok(Ok(()))
            ) {
                self.telemetry.session_reused(now_utc());
                return Ok(Arc::clone(session));
            }
            *guard = None;
            self.telemetry.disconnected();
        }

        self.handshake_limiter.try_acquire()?;
        self.telemetry.connection_attempted();
        let connect_timeout = Duration::from_secs(self.config.connect_timeout_seconds);
        let config = Arc::new(client::Config {
            inactivity_timeout: russh_inactivity_timeout(
                self.config.inactivity_timeout_seconds,
                self.config.keepalive_seconds,
            ),
            keepalive_interval: (self.config.keepalive_seconds > 0)
                .then(|| Duration::from_secs(self.config.keepalive_seconds)),
            keepalive_max: 4,
            nodelay: true,
            ..Default::default()
        });
        let handler = RusshClientHandler {
            host: self.config.address.clone(),
            port: self.config.port,
            policy: self.config.host_key_policy,
            known_hosts_path: self.config.known_hosts_path.clone(),
        };
        let connect = client::connect(
            config,
            (self.config.address.as_str(), self.config.port),
            handler,
        );
        let mut session = match tokio::time::timeout(connect_timeout, connect).await {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => {
                self.telemetry.disconnected();
                return Err(TransportError::Backend(error.to_string()));
            }
            Err(_) => {
                self.telemetry.disconnected();
                return Err(TransportError::Timeout);
            }
        };
        let credential = match self
            .credentials
            .credential_for(self.config.access_path_id)
            .await
        {
            Ok(credential) => credential,
            Err(error) => {
                self.telemetry.disconnected();
                return Err(TransportError::Backend(error.to_string()));
            }
        };
        let authentication = match tokio::time::timeout(
            connect_timeout,
            authenticate_russh_session(&mut session, &self.config.username, &credential),
        )
        .await
        {
            Ok(Ok(authentication)) => authentication,
            Ok(Err(error)) => {
                self.telemetry.disconnected();
                return Err(error);
            }
            Err(_) => {
                self.telemetry.disconnected();
                return Err(TransportError::Timeout);
            }
        };
        let session = Arc::new(session);
        *guard = Some(Arc::clone(&session));
        self.telemetry.handshake_succeeded(now_utc());
        drop(guard);
        if authentication.used_password {
            self.schedule_authorized_key_bootstrap(
                Arc::clone(&session),
                authentication.bootstrap_public_key,
            )
            .await;
        }
        Ok(session)
    }

    async fn acquire_channel(&self) -> Result<OwnedSemaphorePermit, TransportError> {
        self.channel_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::Backend("SSH channel pool is closed".to_owned()))
    }

    fn try_acquire_pty_channel(&self) -> Result<OwnedSemaphorePermit, ConnectorPtyError> {
        match self.channel_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                Err(ConnectorPtyError::ChannelCapacityUnavailable)
            }
            Err(tokio::sync::TryAcquireError::Closed) => Err(ConnectorPtyError::Transport(
                TransportError::Backend("SSH channel pool is closed".to_owned()),
            )),
        }
    }

    async fn reserve_pty_channel(
        &self,
    ) -> Result<Arc<client::Handle<RusshClientHandler>>, TransportError> {
        let mut lifecycle = self.pty_channel_lifecycle.lock().await;
        let session = self.session().await?;
        lifecycle.reserve();
        Ok(session)
    }

    async fn release_pty_channel(
        &self,
        expected_session: &Arc<client::Handle<RusshClientHandler>>,
    ) {
        let mut lifecycle = self.pty_channel_lifecycle.lock().await;
        if lifecycle.release() && self.config.use_exec_file_transfer {
            self.invalidate_session(expected_session).await;
        }
    }

    async fn invalidate_session(&self, expected: &Arc<client::Handle<RusshClientHandler>>) {
        let mut guard = self.session.lock().await;
        if guard
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            *guard = None;
            self.telemetry.disconnected();
        }
    }

    async fn invalidate_current_session(&self) {
        let mut guard = self.session.lock().await;
        if guard.take().is_some() {
            self.telemetry.disconnected();
        }
    }

    async fn reap_idle_session(&self, observed_at: time::OffsetDateTime) -> bool {
        let mut guard = self.session.lock().await;
        let telemetry = self.telemetry.snapshot();
        let Some(last_activity_at) = telemetry.last_validated_at.or(telemetry.last_handshake_at)
        else {
            return false;
        };
        let configured_channels = usize::from(self.config.max_concurrent_channels.max(1));
        if !should_reap_idle_transport(
            observed_at,
            last_activity_at,
            self.config.idle_ttl_seconds,
            self.channel_semaphore.available_permits(),
            configured_channels,
        ) {
            return false;
        }
        if guard.take().is_none() {
            return false;
        }
        self.telemetry.idled();
        true
    }

    async fn schedule_authorized_key_bootstrap(
        &self,
        session: Arc<client::Handle<RusshClientHandler>>,
        public_key: Option<PublicKey>,
    ) {
        let Some(public_key) = public_key else {
            self.record_missing_bootstrap_key().await;
            return;
        };
        let Some((fingerprint, previous_failure_count)) =
            self.begin_authorized_key_bootstrap(&public_key).await
        else {
            return;
        };

        let access_path_id = self.config.access_path_id;
        let windows = self.config.windows;
        let repository = self.authorized_key_bootstrap.clone();
        tokio::spawn(async move {
            let result = install_authorized_key(session, &public_key, windows).await;
            let completed_at = now_utc();
            let state = match result {
                Ok(()) => AuthorizedKeyBootstrap {
                    access_path_id,
                    state: AuthorizedKeyBootstrapState::Installed,
                    reason: None,
                    public_key_fingerprint: Some(fingerprint),
                    failure_count: previous_failure_count,
                    attempted_at: completed_at,
                    next_retry_at: None,
                    updated_at: completed_at,
                },
                Err(error) => authorized_key_bootstrap_failure_state(
                    access_path_id,
                    &fingerprint,
                    previous_failure_count,
                    error,
                    completed_at,
                ),
            };
            persist_authorized_key_bootstrap_result(&repository, &state).await;
        });
    }

    async fn record_missing_bootstrap_key(&self) {
        let now = now_utc();
        let state = AuthorizedKeyBootstrap {
            access_path_id: self.config.access_path_id,
            state: AuthorizedKeyBootstrapState::Skipped,
            reason: Some(AuthorizedKeyBootstrapReason::NoLocalPublicKey),
            public_key_fingerprint: None,
            failure_count: 0,
            attempted_at: now,
            next_retry_at: None,
            updated_at: now,
        };
        if let Err(error) = self.authorized_key_bootstrap.upsert(&state).await {
            tracing::warn!(
                access_path_id = %self.config.access_path_id,
                %error,
                "failed to persist missing-key bootstrap state"
            );
        }
    }

    async fn begin_authorized_key_bootstrap(
        &self,
        public_key: &PublicKey,
    ) -> Option<(String, u32)> {
        let now = now_utc();
        let fingerprint = public_key.fingerprint(HashAlg::Sha256).to_string();
        let existing = match self
            .authorized_key_bootstrap
            .get(self.config.access_path_id)
            .await
        {
            Ok(existing) => existing,
            Err(error) => {
                tracing::warn!(
                    access_path_id = %self.config.access_path_id,
                    %error,
                    "skipping public-key bootstrap because its retry guard could not be loaded"
                );
                return None;
            }
        };
        if !authorized_key_bootstrap_is_eligible(existing.as_ref(), &fingerprint, now) {
            tracing::debug!(
                access_path_id = %self.config.access_path_id,
                "public-key bootstrap suppressed by persisted state"
            );
            return None;
        }
        let previous_failure_count = existing
            .as_ref()
            .filter(|state| state.public_key_fingerprint.as_deref() == Some(fingerprint.as_str()))
            .map_or(0, |state| state.failure_count);
        let attempting = AuthorizedKeyBootstrap {
            access_path_id: self.config.access_path_id,
            state: AuthorizedKeyBootstrapState::Attempting,
            reason: None,
            public_key_fingerprint: Some(fingerprint.clone()),
            failure_count: previous_failure_count,
            attempted_at: now,
            next_retry_at: Some(now + time::Duration::minutes(2)),
            updated_at: now,
        };
        if let Err(error) = self.authorized_key_bootstrap.upsert(&attempting).await {
            tracing::warn!(
                access_path_id = %self.config.access_path_id,
                %error,
                "skipping public-key bootstrap because its retry guard could not be persisted"
            );
            return None;
        }
        Some((fingerprint, previous_failure_count))
    }
}

#[async_trait]
impl<C> RemoteTransport for RusshTransport<C>
where
    C: SshCredentialProvider + 'static,
{
    fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
        Some(self.telemetry.snapshot())
    }

    async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
        let session = self.session().await?;
        session
            .send_ping()
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?;
        Ok(CheckResult {
            ok: true,
            latency_ms: None,
            message: "russh native session is healthy".to_owned(),
        })
    }

    async fn exec(&self, request: ExecRequest) -> Result<ExecResult, TransportError> {
        request
            .profile
            .validate()
            .map_err(|error| TransportError::PolicyDenied(error.to_string()))?;
        let _channel_permit = self.acquire_channel().await?;
        let session = self.session().await?;
        let completion_marker = (!self.config.windows)
            .then(|| format!("REMOTE_HOSTS_EXEC_DONE_{}", request.operation_id));
        let command = match completion_marker.as_deref() {
            Some(marker) => framed_posix_ssh_exec_command(&request.profile, marker),
            None => ssh_exec_command(&request.profile, self.config.windows),
        };
        let execution = tokio::time::timeout(
            Duration::from_secs(request.profile.timeout_seconds),
            execute_russh_command(
                Arc::clone(&session),
                command,
                request.profile.output_limit_bytes,
            ),
        )
        .await;
        let mut result = match execution {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                self.invalidate_session(&session).await;
                return Err(error);
            }
            Err(_) => {
                self.invalidate_session(&session).await;
                return Err(TransportError::Timeout);
            }
        };
        if let Some(marker) = completion_marker.as_deref()
            && !recover_framed_exec_status(&mut result, marker)
        {
            self.invalidate_session(&session).await;
            return Err(TransportError::Backend(
                "remote POSIX exec did not return its completion frame; the pooled SSH session was invalidated"
                    .to_owned(),
            ));
        }
        Ok(result)
    }

    async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
        request
            .spec
            .validate()
            .map_err(|error| TransportError::FileTransfer(error.to_string()))?;
        let timeout = Duration::from_secs(request.spec.timeout_seconds);
        let _channel_permit = self.acquire_channel().await?;
        if self.config.use_exec_file_transfer {
            if let Ok(result) =
                tokio::time::timeout(timeout, execute_russh_exec_file_transfer(self, request)).await
            {
                result
            } else {
                let mut guard = self.session.lock().await;
                if guard.take().is_some() {
                    self.telemetry.disconnected();
                }
                Err(TransportError::Timeout)
            }
        } else {
            let session = self.session().await?;
            tokio::time::timeout(timeout, execute_russh_sftp(session, request))
                .await
                .map_err(|_| TransportError::Timeout)?
        }
    }

    async fn open_forward(
        &self,
        _request: ForwardRequest,
    ) -> Result<ForwardHandle, TransportError> {
        Err(TransportError::Backend(
            "russh port forwarding is not implemented yet".to_owned(),
        ))
    }
}

async fn execute_russh_exec_file_transfer<C>(
    transport: &RusshTransport<C>,
    request: SftpRequest,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    match request.spec.direction {
        SftpDirection::Upload => execute_russh_exec_upload(transport, &request).await,
        SftpDirection::Download => execute_russh_exec_download(transport, &request).await,
    }
}

async fn execute_russh_exec_upload<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    let (local_size, local_sha256) =
        hash_local_source(Path::new(&spec.local_path), spec.max_size_bytes).await?;
    ensure_expected_sha256(spec, &local_sha256)?;

    if local_size <= EXEC_INLINE_UPLOAD_MAX_BYTES {
        return execute_russh_inline_exec_upload(transport, request, local_size, &local_sha256)
            .await;
    }

    match execute_russh_stream_exec_upload(transport, request, local_size, &local_sha256).await {
        Ok(result) => Ok(result),
        Err(stream_error) if retryable_exec_transfer_error(&stream_error) => {
            tracing::warn!(
                error = %stream_error,
                "single-channel streaming upload failed; falling back to resumable exec chunks"
            );
            execute_russh_resumable_exec_upload(transport, request)
                .await
                .map_err(|fallback_error| {
                    TransportError::FileTransfer(format!(
                        "streaming upload failed ({stream_error}); resumable fallback failed ({fallback_error})"
                    ))
                })
        }
        Err(error) => Err(error),
    }
}

async fn execute_russh_inline_exec_upload<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
    local_size: u64,
    local_sha256: &str,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    let payload = tokio::fs::read(&spec.local_path)
        .await
        .map_err(file_transfer_io)?;
    if u64::try_from(payload.len()).map_err(file_transfer_conversion)? != local_size
        || format!("{:x}", Sha256::digest(&payload)) != local_sha256
    {
        return Err(TransportError::FileTransfer(
            "local file changed while it was being prepared for upload".to_owned(),
        ));
    }

    let temporary_path = streaming_remote_temporary_path(&spec.remote_path, local_sha256);
    let command =
        russh_exec_inline_upload_command(spec, &temporary_path, local_size, local_sha256, &payload);
    let mut retry_count = 0_u32;
    let (remote_size, remote_sha256) = execute_russh_upload_placement_with_retry(
        transport,
        command,
        "single-channel pooled exec upload",
        &mut retry_count,
        &ExecUploadDestinationVerification {
            remote_path: &spec.remote_path,
            temporary_path: &temporary_path,
            expected_size: local_size,
            expected_sha256: local_sha256,
        },
    )
    .await?;
    if remote_size != local_size || remote_sha256 != local_sha256 {
        return Err(TransportError::FileTransfer(format!(
            "pooled exec upload verification failed: local_bytes={local_size}, remote_bytes={remote_size}, local_sha256={local_sha256}, remote_sha256={remote_sha256}"
        )));
    }
    emit_sftp_progress(
        request,
        "completed",
        remote_size,
        Some(local_size),
        0,
        retry_count,
    );
    Ok(exec_upload_result(spec, remote_size, remote_sha256))
}

async fn execute_russh_stream_exec_upload<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
    local_size: u64,
    local_sha256: &str,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    let temporary_path = streaming_remote_temporary_path(&spec.remote_path, local_sha256);
    let script = russh_exec_stream_upload_command(spec, &temporary_path, local_size, local_sha256);
    let frame_marker = russh_transfer_frame_marker(&script);
    let command = framed_posix_script_ssh_exec_command(&script, &frame_marker);
    let mut retry_count = 0_u32;
    let mut attempt = 1_u32;
    loop {
        let result = match transport.session().await {
            Ok(session) => {
                execute_russh_stream_upload_attempt(
                    session,
                    &RusshStreamUploadAttempt {
                        request,
                        command: &command,
                        frame_marker: &frame_marker,
                        temporary_path: &temporary_path,
                        local_size,
                        local_sha256,
                        retry_count,
                    },
                )
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                emit_sftp_progress(
                    request,
                    "completed",
                    local_size,
                    Some(local_size),
                    0,
                    retry_count,
                );
                return Ok(exec_upload_result(
                    spec,
                    local_size,
                    local_sha256.to_owned(),
                ));
            }
            Err(TransportError::Timeout) => {
                transport.invalidate_current_session().await;
                return Err(TransportError::Timeout);
            }
            Err(error) if retryable_exec_transfer_error(&error) => {
                if exec_transfer_error_invalidates_session(&error) {
                    transport.invalidate_current_session().await;
                }
                let consumes_attempt = exec_transfer_retry_consumes_attempt(&error);
                if consumes_attempt && attempt >= EXEC_TRANSFER_MAX_STAGE_ATTEMPTS {
                    return Err(error);
                }
                let delay = exec_transfer_retry_delay(&error, attempt);
                tracing::warn!(
                    stage = "single-channel streaming pooled exec upload",
                    attempt,
                    max_attempts = EXEC_TRANSFER_MAX_STAGE_ATTEMPTS,
                    consumes_attempt,
                    retry_after_seconds = delay.as_secs(),
                    error = %error,
                    "streaming pooled exec upload will retry"
                );
                retry_count = retry_count.saturating_add(1);
                tokio::time::sleep(delay).await;
                if consumes_attempt {
                    attempt = attempt.saturating_add(1);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

struct RusshStreamUploadAttempt<'a> {
    request: &'a SftpRequest,
    command: &'a str,
    frame_marker: &'a str,
    temporary_path: &'a str,
    local_size: u64,
    local_sha256: &'a str,
    retry_count: u32,
}

async fn exec_transfer_stage_with_timeout<F, T>(
    timeout: Duration,
    future: F,
) -> Result<T, TransportError>
where
    F: Future<Output = Result<T, TransportError>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| TransportError::Timeout)?
}

async fn execute_russh_stream_upload_attempt(
    session: Arc<client::Handle<RusshClientHandler>>,
    context: &RusshStreamUploadAttempt<'_>,
) -> Result<(), TransportError> {
    let channel = exec_transfer_stage_with_timeout(EXEC_TRANSFER_STAGE_TIMEOUT, async {
        session.channel_open_session().await.map_err(|error| {
            TransportError::FileTransfer(format!("open pooled exec upload channel: {error}"))
        })
    })
    .await?;
    exec_transfer_stage_with_timeout(EXEC_TRANSFER_STAGE_TIMEOUT, async {
        channel
            .exec(true, russh_transfer_exec_command(context.command))
            .await
            .map_err(|error| {
                TransportError::FileTransfer(format!("start pooled exec upload: {error}"))
            })
    })
    .await?;
    let (read_half, write_half) = channel.split();
    let mut receive_task = tokio::spawn(async move {
        let mut read_half = read_half;
        receive_russh_exec_result_from_read_half(&mut read_half, EXEC_TRANSFER_OUTPUT_LIMIT_BYTES)
            .await
    });

    let upload = stream_russh_upload_body(&write_half, context).await;
    if upload.is_err() {
        let _ = write_half.close().await;
        receive_task.abort();
    }
    let (bytes_transferred, streamed_sha256) = upload?;
    let outcome = exec_transfer_stage_with_timeout(EXEC_TRANSFER_STAGE_TIMEOUT, async {
        (&mut receive_task).await.map_err(|error| {
            TransportError::FileTransfer(format!("join pooled exec upload receiver: {error}"))
        })?
    })
    .await;
    if outcome.is_err() {
        receive_task.abort();
        let _ = receive_task.await;
    }
    let mut outcome = outcome?;
    let _ = write_half.close().await;
    if !recover_framed_exec_status(&mut outcome, context.frame_marker) {
        return Err(TransportError::FileTransfer(
            "single-channel streaming pooled exec upload did not return the required completion frame"
                .to_owned(),
        ));
    }

    if bytes_transferred != context.local_size || streamed_sha256 != context.local_sha256 {
        return Err(TransportError::FileTransfer(
            "local file changed while it was being uploaded".to_owned(),
        ));
    }
    let (remote_size, remote_sha256) = verify_russh_upload_outcome_or_destination(
        session,
        &outcome,
        "single-channel streaming pooled exec upload",
        &ExecUploadDestinationVerification {
            remote_path: &context.request.spec.remote_path,
            temporary_path: context.temporary_path,
            expected_size: context.local_size,
            expected_sha256: context.local_sha256,
        },
    )
    .await?;
    if remote_size != context.local_size || remote_sha256 != context.local_sha256 {
        return Err(TransportError::FileTransfer(format!(
            "streaming pooled exec upload verification failed: local_bytes={}, remote_bytes={remote_size}, local_sha256={}, remote_sha256={remote_sha256}",
            context.local_size, context.local_sha256
        )));
    }
    Ok(())
}

async fn stream_russh_upload_body(
    write_half: &russh::ChannelWriteHalf<client::Msg>,
    context: &RusshStreamUploadAttempt<'_>,
) -> Result<(u64, String), TransportError> {
    let spec = &context.request.spec;
    let mut local = tokio::fs::File::open(&spec.local_path)
        .await
        .map_err(file_transfer_io)?;
    let mut hasher = Sha256::new();
    let mut bytes_transferred = 0_u64;
    let mut next_progress_bytes = 1024 * 1024_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = local
            .read(&mut buffer)
            .await
            .map_err(|error| file_transfer_io_context("read local upload source", error))?;
        if read == 0 {
            break;
        }
        bytes_transferred = bytes_transferred
            .checked_add(u64::try_from(read).map_err(file_transfer_conversion)?)
            .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
        ensure_size_within_limit(bytes_transferred, spec.max_size_bytes)?;
        hasher.update(&buffer[..read]);
        exec_transfer_stage_with_timeout(EXEC_TRANSFER_STAGE_TIMEOUT, async {
            write_half
                .data_bytes(buffer[..read].to_vec())
                .await
                .map_err(|error| {
                    TransportError::FileTransfer(format!("stream pooled exec upload: {error}"))
                })
        })
        .await?;
        if bytes_transferred >= next_progress_bytes {
            emit_sftp_progress(
                context.request,
                "uploading",
                bytes_transferred,
                Some(context.local_size),
                0,
                context.retry_count,
            );
            while next_progress_bytes <= bytes_transferred {
                next_progress_bytes = next_progress_bytes.saturating_add(1024 * 1024);
            }
        }
    }
    exec_transfer_stage_with_timeout(EXEC_TRANSFER_STAGE_TIMEOUT, async {
        write_half.eof().await.map_err(|error| {
            TransportError::FileTransfer(format!("finish pooled exec upload input: {error}"))
        })
    })
    .await?;
    Ok((bytes_transferred, format!("{:x}", hasher.finalize())))
}

async fn receive_russh_exec_result_from_read_half(
    channel: &mut russh::ChannelReadHalf,
    output_limit_bytes: usize,
) -> Result<ExecResult, TransportError> {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = None;
    let mut truncated = false;
    while let Some(message) = channel.wait().await {
        match message {
            ChannelMsg::Data { data } => {
                append_limited_utf8(&mut stdout, &data, output_limit_bytes, &mut truncated);
            }
            ChannelMsg::ExtendedData { data, ext: 1 } => {
                append_limited_utf8(&mut stderr, &data, output_limit_bytes, &mut truncated);
            }
            ChannelMsg::ExitStatus { exit_status } => {
                exit_code = i32::try_from(exit_status).ok();
            }
            ChannelMsg::ExitSignal { error_message, .. } => {
                if !error_message.is_empty() {
                    append_limited_utf8(
                        &mut stderr,
                        error_message.as_bytes(),
                        output_limit_bytes,
                        &mut truncated,
                    );
                }
            }
            ChannelMsg::Close => break,
            _ => {}
        }
    }
    Ok(ExecResult {
        exit_code,
        stdout,
        stderr,
        truncated,
    })
}

async fn execute_russh_resumable_exec_upload<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    match prepare_russh_exec_upload(transport, request).await? {
        PreparedExecUpload::Complete {
            local_size,
            local_sha256,
            retry_count,
        } => {
            emit_sftp_progress(
                request,
                "completed",
                local_size,
                Some(local_size),
                local_size,
                retry_count,
            );
            Ok(exec_upload_result(spec, local_size, local_sha256))
        }
        PreparedExecUpload::Pending(pending) => {
            let PendingExecUpload {
                mut local,
                hasher,
                local_size,
                local_sha256,
                temporary_path,
                resume_bytes,
                mut retry_count,
            } = *pending;
            let context = ExecUploadStreamContext {
                temporary_path: &temporary_path,
                remote_path: &spec.remote_path,
                max_size_bytes: spec.max_size_bytes,
                total_bytes: local_size,
                initial_resume_bytes: resume_bytes,
                progress_tx: request.progress_tx.as_ref(),
            };
            let (bytes_transferred, streamed_sha256) = stream_base64_upload_via_exec(
                transport,
                &mut local,
                resume_bytes,
                hasher,
                &mut retry_count,
                &context,
            )
            .await?;
            if bytes_transferred != local_size || streamed_sha256 != local_sha256 {
                return Err(TransportError::FileTransfer(
                    "local file changed while it was being uploaded".to_owned(),
                ));
            }

            let finalize = russh_exec_upload_finalize_command(
                spec,
                &temporary_path,
                local_size,
                &local_sha256,
            );
            let (remote_size, remote_sha256) = execute_russh_upload_placement_with_retry(
                transport,
                finalize,
                "finalize pooled exec upload",
                &mut retry_count,
                &ExecUploadDestinationVerification {
                    remote_path: &spec.remote_path,
                    temporary_path: &temporary_path,
                    expected_size: local_size,
                    expected_sha256: &local_sha256,
                },
            )
            .await?;
            if remote_size != local_size || remote_sha256 != local_sha256 {
                return Err(TransportError::FileTransfer(format!(
                    "pooled exec upload verification failed: local_bytes={local_size}, remote_bytes={remote_size}, local_sha256={local_sha256}, remote_sha256={remote_sha256}"
                )));
            }
            emit_sftp_progress(
                request,
                "completed",
                remote_size,
                Some(local_size),
                resume_bytes,
                retry_count,
            );
            Ok(exec_upload_result(spec, remote_size, remote_sha256))
        }
    }
}

fn exec_upload_result(
    spec: &FileTransferSpec,
    bytes_transferred: u64,
    sha256: String,
) -> SftpResult {
    SftpResult {
        direction: spec.direction,
        bytes_transferred,
        sha256,
        local_path: spec.local_path.clone(),
        remote_path: spec.remote_path.clone(),
        overwrite: spec.overwrite,
    }
}

enum PreparedExecUpload {
    Complete {
        local_size: u64,
        local_sha256: String,
        retry_count: u32,
    },
    Pending(Box<PendingExecUpload>),
}

struct PendingExecUpload {
    local: tokio::fs::File,
    hasher: Sha256,
    local_size: u64,
    local_sha256: String,
    temporary_path: String,
    resume_bytes: u64,
    retry_count: u32,
}

async fn prepare_russh_exec_upload<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
) -> Result<PreparedExecUpload, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    let (local_size, local_sha256) =
        hash_local_source(Path::new(&spec.local_path), spec.max_size_bytes).await?;
    ensure_expected_sha256(spec, &local_sha256)?;
    let temporary_path = resumable_remote_temporary_path(&spec.remote_path, &local_sha256);
    let initialize =
        russh_exec_upload_initialize_command(spec, &temporary_path, local_size, &local_sha256);
    let mut retry_count = 0_u32;
    let remote_status = execute_russh_transfer_stage_with_retry(
        transport,
        initialize.clone(),
        "initialize pooled exec upload",
        &mut retry_count,
        |outcome| {
            require_exec_transfer_success(
                outcome,
                "initialize pooled exec upload",
                &spec.remote_path,
                &temporary_path,
            )?;
            parse_exec_upload_status(&outcome.stdout)
        },
    )
    .await?;
    let ExecUploadRemoteStatus::Ready {
        bytes: mut resume_bytes,
        mut prefix_sha256,
    } = remote_status
    else {
        let ExecUploadRemoteStatus::Complete { size, sha256 } = remote_status else {
            unreachable!();
        };
        if size != local_size || sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(
                "completed pooled exec upload marker does not match local source".to_owned(),
            ));
        }
        return Ok(PreparedExecUpload::Complete {
            local_size,
            local_sha256,
            retry_count,
        });
    };

    let (mut local, mut hasher, local_prefix_sha256) =
        open_local_upload_at_offset(&spec.local_path, resume_bytes, spec.max_size_bytes).await?;
    emit_sftp_progress(
        request,
        "resume_verified",
        resume_bytes,
        Some(local_size),
        resume_bytes,
        retry_count,
    );
    if prefix_sha256 != local_prefix_sha256 {
        let (reset_local, reset_hasher, reset_prefix_sha256) = reset_mismatched_russh_exec_upload(
            transport,
            spec,
            &temporary_path,
            &initialize,
            &mut retry_count,
        )
        .await?;
        resume_bytes = 0;
        prefix_sha256 = reset_prefix_sha256;
        local = reset_local;
        hasher = reset_hasher;
        emit_sftp_progress(request, "resume_reset", 0, Some(local_size), 0, retry_count);
    }
    tracing::info!(
        remote_path = %spec.remote_path,
        local_size,
        resume_bytes,
        remote_prefix_sha256 = %prefix_sha256,
        "starting resumable pooled exec upload"
    );

    Ok(PreparedExecUpload::Pending(Box::new(PendingExecUpload {
        local,
        hasher,
        local_size,
        local_sha256,
        temporary_path,
        resume_bytes,
        retry_count,
    })))
}

async fn reset_mismatched_russh_exec_upload<C>(
    transport: &RusshTransport<C>,
    spec: &FileTransferSpec,
    temporary_path: &str,
    initialize: &str,
    retry_count: &mut u32,
) -> Result<(tokio::fs::File, Sha256, String), TransportError>
where
    C: SshCredentialProvider,
{
    let reset = russh_exec_upload_cleanup_command(temporary_path);
    execute_russh_transfer_stage_with_retry(
        transport,
        reset,
        "reset mismatched pooled upload temporary file",
        retry_count,
        |outcome| {
            require_exec_transfer_marker(
                outcome,
                "REMOTE_HOSTS_RESET_OK",
                "reset mismatched pooled upload temporary file",
                &spec.remote_path,
                temporary_path,
            )
        },
    )
    .await?;
    let reinitialized = execute_russh_transfer_stage_with_retry(
        transport,
        initialize.to_owned(),
        "reinitialize pooled exec upload",
        retry_count,
        |outcome| {
            require_exec_transfer_success(
                outcome,
                "reinitialize pooled exec upload",
                &spec.remote_path,
                temporary_path,
            )?;
            parse_exec_upload_status(&outcome.stdout)
        },
    )
    .await?;
    let ExecUploadRemoteStatus::Ready {
        bytes,
        prefix_sha256,
    } = reinitialized
    else {
        return Err(TransportError::FileTransfer(
            "pooled exec upload reset unexpectedly reported a completed destination".to_owned(),
        ));
    };
    if bytes != 0 {
        return Err(TransportError::FileTransfer(
            "pooled exec upload reset did not return an empty temporary file".to_owned(),
        ));
    }
    let (local, hasher, _) =
        open_local_upload_at_offset(&spec.local_path, 0, spec.max_size_bytes).await?;
    Ok((local, hasher, prefix_sha256))
}

async fn open_local_upload_at_offset(
    local_path: &str,
    offset: u64,
    max_size_bytes: u64,
) -> Result<(tokio::fs::File, Sha256, String), TransportError> {
    ensure_size_within_limit(offset, max_size_bytes)?;
    let mut local = tokio::fs::File::open(local_path)
        .await
        .map_err(file_transfer_io)?;
    let mut hasher = Sha256::new();
    let mut remaining = offset;
    let mut buffer = vec![0_u8; 256 * 1024];
    while remaining > 0 {
        let requested = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(file_transfer_conversion)?;
        let read = local
            .read(&mut buffer[..requested])
            .await
            .map_err(|error| file_transfer_io_context("read local upload prefix", error))?;
        if read == 0 {
            return Err(TransportError::FileTransfer(format!(
                "remote upload offset {offset} exceeds the local source size"
            )));
        }
        hasher.update(&buffer[..read]);
        remaining =
            remaining.saturating_sub(u64::try_from(read).map_err(file_transfer_conversion)?);
    }
    let prefix_sha256 = format!("{:x}", hasher.clone().finalize());
    Ok((local, hasher, prefix_sha256))
}

struct ExecUploadStreamContext<'a> {
    temporary_path: &'a str,
    remote_path: &'a str,
    max_size_bytes: u64,
    total_bytes: u64,
    initial_resume_bytes: u64,
    progress_tx: Option<&'a mpsc::UnboundedSender<SftpProgress>>,
}

async fn stream_base64_upload_via_exec<C>(
    transport: &RusshTransport<C>,
    local: &mut tokio::fs::File,
    resume_bytes: u64,
    mut hasher: Sha256,
    retry_count: &mut u32,
    context: &ExecUploadStreamContext<'_>,
) -> Result<(u64, String), TransportError>
where
    C: SshCredentialProvider,
{
    let mut buffer = vec![0_u8; EXEC_UPLOAD_CHUNK_BYTES];
    let mut bytes_transferred = resume_bytes;
    let mut chunk_index = resume_bytes / EXEC_UPLOAD_CHUNK_BYTES as u64;
    loop {
        let mut filled = 0;
        while filled < buffer.len() {
            let read = local
                .read(&mut buffer[filled..])
                .await
                .map_err(|error| file_transfer_io_context("read local upload source", error))?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            break;
        }
        let next_bytes_transferred = bytes_transferred
            .checked_add(u64::try_from(filled).map_err(file_transfer_conversion)?)
            .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
        ensure_size_within_limit(next_bytes_transferred, context.max_size_bytes)?;
        let command = russh_exec_upload_chunk_command(
            context.temporary_path,
            chunk_index,
            bytes_transferred,
            &buffer[..filled],
        );
        let payload_sha256 = format!("{:x}", Sha256::digest(&buffer[..filled]));
        let verify_command = russh_exec_upload_chunk_verify_command(
            context.temporary_path,
            chunk_index,
            next_bytes_transferred,
            filled,
            &payload_sha256,
        );
        execute_russh_upload_chunk_with_retry(
            transport,
            command,
            verify_command,
            retry_count,
            &ExecUploadChunkVerification {
                chunk_index,
                expected_size: next_bytes_transferred,
                expected_sha256: &payload_sha256,
                remote_path: context.remote_path,
                temporary_path: context.temporary_path,
            },
        )
        .await?;
        hasher.update(&buffer[..filled]);
        bytes_transferred = next_bytes_transferred;
        chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
            TransportError::FileTransfer("upload chunk index overflow".to_owned())
        })?;
        if should_rotate_exec_upload_session(chunk_index, bytes_transferred, context.total_bytes) {
            transport.invalidate_current_session().await;
        }
        if bytes_transferred % (8 * 1024 * 1024) < EXEC_UPLOAD_CHUNK_BYTES as u64 {
            emit_sftp_progress_to(
                context.progress_tx,
                "uploading",
                bytes_transferred,
                Some(context.total_bytes),
                context.initial_resume_bytes,
                *retry_count,
            );
            tracing::info!(
                remote_path = context.remote_path,
                bytes_transferred,
                "resumable pooled exec upload progress"
            );
        }
        if filled < buffer.len() {
            break;
        }
    }
    Ok((bytes_transferred, format!("{:x}", hasher.finalize())))
}

async fn execute_russh_exec_download<C>(
    transport: &RusshTransport<C>,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError>
where
    C: SshCredentialProvider,
{
    let spec = &request.spec;
    let destination = Path::new(&spec.local_path);
    ensure_local_destination(destination, spec.overwrite).await?;
    let temporary_path = local_temporary_path(destination, request.operation_id)?;
    let mut attempt = 1_u32;
    loop {
        cleanup_local_temporary_file(&temporary_path).await?;
        let session = transport.session().await?;
        let transfer = execute_russh_exec_download_attempt(
            Arc::clone(&session),
            request,
            destination,
            &temporary_path,
        )
        .await;
        match transfer {
            Ok(result) => return Ok(result),
            Err(error)
                if retryable_exec_transfer_error(&error)
                    && attempt < EXEC_TRANSFER_MAX_STAGE_ATTEMPTS =>
            {
                transport.invalidate_session(&session).await;
                let delay = exec_transfer_retry_delay(&error, attempt);
                tracing::warn!(
                    attempt,
                    max_attempts = EXEC_TRANSFER_MAX_STAGE_ATTEMPTS,
                    retry_after_seconds = delay.as_secs(),
                    error = %error,
                    "pooled exec download will retry with a fresh SSH session"
                );
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => {
                let _ = tokio::fs::remove_file(&temporary_path).await;
                return Err(error);
            }
        }
    }
}

async fn execute_russh_exec_download_attempt(
    session: Arc<client::Handle<RusshClientHandler>>,
    request: &SftpRequest,
    destination: &Path,
    temporary_path: &Path,
) -> Result<SftpResult, TransportError> {
    let spec = &request.spec;
    let command = russh_exec_download_request_command(spec);
    let transfer = async {
        let mut channel = session
            .channel_open_session()
            .await
            .map_err(|error| {
                TransportError::FileTransfer(format!(
                    "open pooled exec download channel: {error}"
                ))
            })?;
        channel.exec(true, command).await.map_err(|error| {
            TransportError::FileTransfer(format!("start pooled exec download: {error}"))
        })?;
        let mut local = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .await
            .map_err(file_transfer_io)?;
        let outcome =
            receive_russh_exec_download(&mut channel, &mut local, spec.max_size_bytes).await?;
        let _ = channel.close().await;
        local
            .shutdown()
            .await
            .map_err(|error| file_transfer_io_context("close local download file", error))?;
        if outcome.truncated {
            return Err(TransportError::FileTransfer(
                "pooled exec download diagnostics exceeded the bounded limit".to_owned(),
            ));
        }
        if outcome.exit_code != Some(0) {
            return Err(TransportError::FileTransfer(format!(
                "pooled exec download failed with exit_code={:?}: {}",
                outcome.exit_code,
                sanitized_transfer_diagnostics(&outcome.stderr, &spec.remote_path, "")
            )));
        }
        let (remote_size, remote_sha256) =
            parse_transfer_marker(&outcome.stderr, "REMOTE_HOSTS_TRANSFER_META")?;
        if remote_size != outcome.bytes_transferred || remote_sha256 != outcome.sha256 {
            return Err(TransportError::FileTransfer(format!(
                "pooled exec download verification failed: remote_bytes={remote_size}, local_bytes={}, remote_sha256={remote_sha256}, local_sha256={}",
                outcome.bytes_transferred, outcome.sha256
            )));
        }
        ensure_expected_sha256(spec, &outcome.sha256)?;
        if let Some(mode) = spec.mode {
            set_local_mode(temporary_path, mode).await?;
        }
        place_local_file(temporary_path, destination, spec.overwrite).await?;
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred: outcome.bytes_transferred,
            sha256: outcome.sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }
    .await;

    if transfer.is_err() {
        let _ = tokio::fs::remove_file(&temporary_path).await;
    }
    transfer
}

struct RusshExecDownloadOutcome {
    bytes_transferred: u64,
    sha256: String,
    stderr: String,
    exit_code: Option<i32>,
    truncated: bool,
}

async fn receive_russh_exec_download(
    channel: &mut russh::Channel<client::Msg>,
    local: &mut tokio::fs::File,
    max_size_bytes: u64,
) -> Result<RusshExecDownloadOutcome, TransportError> {
    let mut hasher = Sha256::new();
    let mut bytes_transferred = 0_u64;
    let mut stderr = String::new();
    let mut exit_code = None;
    let mut truncated = false;
    while let Some(message) = channel.wait().await {
        match message {
            ChannelMsg::Data { data } => {
                bytes_transferred = bytes_transferred
                    .checked_add(u64::try_from(data.len()).map_err(file_transfer_conversion)?)
                    .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
                ensure_size_within_limit(bytes_transferred, max_size_bytes)?;
                local.write_all(&data).await.map_err(|error| {
                    file_transfer_io_context("write local download file", error)
                })?;
                hasher.update(&data);
            }
            ChannelMsg::ExtendedData { data, ext: 1 } => {
                append_limited_utf8(&mut stderr, &data, 64 * 1024, &mut truncated);
            }
            ChannelMsg::ExitStatus { exit_status } => {
                exit_code = i32::try_from(exit_status).ok();
            }
            ChannelMsg::ExitSignal { error_message, .. } => {
                append_limited_utf8(
                    &mut stderr,
                    error_message.as_bytes(),
                    64 * 1024,
                    &mut truncated,
                );
            }
            ChannelMsg::Close => break,
            _ => {}
        }
    }
    Ok(RusshExecDownloadOutcome {
        bytes_transferred,
        sha256: format!("{:x}", hasher.finalize()),
        stderr,
        exit_code,
        truncated,
    })
}

async fn execute_russh_transfer_command(
    session: Arc<client::Handle<RusshClientHandler>>,
    script: String,
    stage: &str,
) -> Result<ExecResult, TransportError> {
    let marker = russh_transfer_frame_marker(&script);
    let command = framed_posix_script_ssh_exec_command(&script, &marker);
    let mut outcome = tokio::time::timeout(
        EXEC_TRANSFER_STAGE_TIMEOUT,
        execute_russh_command(session, command, EXEC_TRANSFER_OUTPUT_LIMIT_BYTES),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(|error| file_transfer_context(stage, error))?;
    if !recover_framed_exec_status(&mut outcome, &marker) {
        return Err(TransportError::FileTransfer(format!(
            "{stage} did not return the required completion frame"
        )));
    }
    Ok(outcome)
}

fn russh_transfer_exec_command(script: &str) -> String {
    [shell_quote("sh"), shell_quote("-lc"), shell_quote(script)].join(" ")
}

fn russh_transfer_frame_marker(script: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(script.as_bytes()));
    format!("REMOTE_HOSTS_TRANSFER_FRAME_DONE_{}", &digest[..16])
}

async fn execute_russh_transfer_stage_with_retry<C, T, F>(
    transport: &RusshTransport<C>,
    command: String,
    stage: &str,
    retry_count: &mut u32,
    mut validate: F,
) -> Result<T, TransportError>
where
    C: SshCredentialProvider,
    F: FnMut(&ExecResult) -> Result<T, TransportError>,
{
    let mut attempt = 1_u32;
    loop {
        let result = match transport.session().await {
            Ok(session) => execute_russh_transfer_command(session, command.clone(), stage)
                .await
                .and_then(|outcome| validate(&outcome)),
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => return Ok(value),
            Err(error) if retryable_exec_transfer_error(&error) => {
                if exec_transfer_error_invalidates_session(&error) {
                    transport.invalidate_current_session().await;
                }
                let consumes_attempt = exec_transfer_retry_consumes_attempt(&error);
                if consumes_attempt && attempt >= EXEC_TRANSFER_MAX_STAGE_ATTEMPTS {
                    return Err(error);
                }
                let delay = exec_transfer_retry_delay(&error, attempt);
                tracing::warn!(
                    stage,
                    attempt,
                    max_attempts = EXEC_TRANSFER_MAX_STAGE_ATTEMPTS,
                    consumes_attempt,
                    retry_after_seconds = delay.as_secs(),
                    error = %error,
                    "resumable pooled exec transfer stage will retry"
                );
                *retry_count = retry_count.saturating_add(1);
                tokio::time::sleep(delay).await;
                if consumes_attempt {
                    attempt = attempt.saturating_add(1);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

struct ExecUploadDestinationVerification<'a> {
    remote_path: &'a str,
    temporary_path: &'a str,
    expected_size: u64,
    expected_sha256: &'a str,
}

async fn execute_russh_upload_placement_with_retry<C>(
    transport: &RusshTransport<C>,
    command: String,
    stage: &str,
    retry_count: &mut u32,
    verification: &ExecUploadDestinationVerification<'_>,
) -> Result<(u64, String), TransportError>
where
    C: SshCredentialProvider,
{
    let mut attempt = 1_u32;
    loop {
        let result = async {
            let session = transport.session().await?;
            let outcome =
                execute_russh_transfer_command(Arc::clone(&session), command.clone(), stage)
                    .await?;
            verify_russh_upload_outcome_or_destination(session, &outcome, stage, verification).await
        }
        .await;

        match result {
            Ok(value) => return Ok(value),
            Err(error) if retryable_exec_transfer_error(&error) => {
                if exec_transfer_error_invalidates_session(&error) {
                    transport.invalidate_current_session().await;
                }
                let consumes_attempt = exec_transfer_retry_consumes_attempt(&error);
                if consumes_attempt && attempt >= EXEC_TRANSFER_MAX_STAGE_ATTEMPTS {
                    return Err(error);
                }
                let delay = exec_transfer_retry_delay(&error, attempt);
                tracing::warn!(
                    stage,
                    attempt,
                    max_attempts = EXEC_TRANSFER_MAX_STAGE_ATTEMPTS,
                    consumes_attempt,
                    retry_after_seconds = delay.as_secs(),
                    error = %error,
                    "pooled exec upload placement will retry"
                );
                *retry_count = retry_count.saturating_add(1);
                tokio::time::sleep(delay).await;
                if consumes_attempt {
                    attempt = attempt.saturating_add(1);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

async fn verify_russh_upload_outcome_or_destination(
    session: Arc<client::Handle<RusshClientHandler>>,
    outcome: &ExecResult,
    stage: &str,
    verification: &ExecUploadDestinationVerification<'_>,
) -> Result<(u64, String), TransportError> {
    require_exec_transfer_success(
        outcome,
        stage,
        verification.remote_path,
        verification.temporary_path,
    )?;
    if outcome
        .stdout
        .lines()
        .any(|line| line.starts_with("REMOTE_HOSTS_TRANSFER_OK"))
    {
        return parse_transfer_marker(&outcome.stdout, "REMOTE_HOSTS_TRANSFER_OK");
    }

    verify_russh_upload_destination(
        session,
        verification.remote_path,
        verification.temporary_path,
        verification.expected_size,
        verification.expected_sha256,
    )
    .await?;
    Ok((
        verification.expected_size,
        verification.expected_sha256.to_owned(),
    ))
}

async fn verify_russh_upload_destination(
    session: Arc<client::Handle<RusshClientHandler>>,
    remote_path: &str,
    temporary_path: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), TransportError> {
    let marker = format!(
        "REMOTE_HOSTS_UPLOAD_VERIFY_DONE_{}_{}",
        &expected_sha256[..16],
        expected_size
    );
    let verification =
        russh_exec_upload_destination_verify_command(remote_path, expected_size, expected_sha256);
    let command = framed_posix_script_ssh_exec_command(&verification, &marker);
    let mut outcome = execute_russh_command(session, command, EXEC_TRANSFER_OUTPUT_LIMIT_BYTES)
        .await
        .map_err(|error| file_transfer_context("verify placed pooled upload", error))?;
    let marker_seen = recover_framed_exec_status(&mut outcome, &marker);
    if !marker_seen {
        return Err(TransportError::FileTransfer(format!(
            "verify placed pooled upload did not return its completion frame; exit_code={:?}: {}",
            outcome.exit_code,
            sanitized_transfer_diagnostics(
                &format!("{}{}", outcome.stdout, outcome.stderr),
                remote_path,
                temporary_path
            )
        )));
    }
    require_exec_transfer_success(
        &outcome,
        "verify placed pooled upload",
        remote_path,
        temporary_path,
    )
}

struct ExecUploadChunkVerification<'a> {
    chunk_index: u64,
    expected_size: u64,
    expected_sha256: &'a str,
    remote_path: &'a str,
    temporary_path: &'a str,
}

async fn execute_russh_upload_chunk_with_retry<C>(
    transport: &RusshTransport<C>,
    append_command: String,
    verify_command: String,
    retry_count: &mut u32,
    verification: &ExecUploadChunkVerification<'_>,
) -> Result<(), TransportError>
where
    C: SshCredentialProvider,
{
    let stage = "append and verify pooled upload chunk";
    let mut attempt = 1_u32;
    loop {
        let result = async {
            let session = transport.session().await?;
            let append_outcome = execute_russh_transfer_command(
                session,
                append_command.clone(),
                "append pooled upload chunk",
            )
            .await?;
            // Some bastion exec gateways preserve the command and exit status
            // but suppress stdout when the command carries a large base64 body.
            require_exec_transfer_success(
                &append_outcome,
                "append pooled upload chunk",
                verification.remote_path,
                verification.temporary_path,
            )?;

            let session = transport.session().await?;
            let verify_outcome = execute_russh_transfer_command(
                session,
                verify_command.clone(),
                "verify pooled upload chunk",
            )
            .await?;
            require_exec_upload_chunk_success(
                &verify_outcome,
                verification.chunk_index,
                verification.expected_size,
                verification.expected_sha256,
                verification.remote_path,
                verification.temporary_path,
            )
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(error) if retryable_exec_transfer_error(&error) => {
                if exec_transfer_error_invalidates_session(&error) {
                    transport.invalidate_current_session().await;
                }
                let consumes_attempt = exec_transfer_retry_consumes_attempt(&error);
                if consumes_attempt && attempt >= EXEC_TRANSFER_MAX_STAGE_ATTEMPTS {
                    return Err(error);
                }
                let delay = exec_transfer_retry_delay(&error, attempt);
                tracing::warn!(
                    stage,
                    attempt,
                    max_attempts = EXEC_TRANSFER_MAX_STAGE_ATTEMPTS,
                    consumes_attempt,
                    retry_after_seconds = delay.as_secs(),
                    error = %error,
                    "resumable pooled exec upload chunk will retry"
                );
                *retry_count = retry_count.saturating_add(1);
                tokio::time::sleep(delay).await;
                if consumes_attempt {
                    attempt = attempt.saturating_add(1);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn exec_transfer_retry_consumes_attempt(error: &TransportError) -> bool {
    !matches!(error, TransportError::LocalHandshakeBudgetExhausted { .. })
}

fn retryable_exec_transfer_error(error: &TransportError) -> bool {
    match error {
        TransportError::LocalHandshakeBudgetExhausted { .. }
        | TransportError::Backend(_)
        | TransportError::Timeout => true,
        TransportError::FileTransfer(message) => {
            message.contains("did not return marker")
                || message.contains("did not return the required")
        }
        TransportError::PolicyDenied(_) => false,
    }
}

fn exec_transfer_error_invalidates_session(error: &TransportError) -> bool {
    match error {
        TransportError::Backend(_) | TransportError::Timeout => true,
        TransportError::FileTransfer(message) => {
            message.contains("did not return the required completion frame")
        }
        TransportError::LocalHandshakeBudgetExhausted { .. } | TransportError::PolicyDenied(_) => {
            false
        }
    }
}

fn should_rotate_exec_upload_session(
    completed_chunks: u64,
    bytes_transferred: u64,
    total_bytes: u64,
) -> bool {
    bytes_transferred < total_bytes
        && completed_chunks > 0
        && completed_chunks.is_multiple_of(EXEC_UPLOAD_CHUNKS_PER_SESSION)
}

fn exec_transfer_retry_delay(error: &TransportError, attempt: u32) -> Duration {
    match error {
        TransportError::LocalHandshakeBudgetExhausted {
            retry_after_seconds,
        } => Duration::from_secs((*retry_after_seconds).max(1)),
        _ => Duration::from_secs(1_u64 << attempt.saturating_sub(1).min(3)),
    }
}

fn format_sftp_progress(progress: &SftpProgress, elapsed_seconds: u64) -> String {
    let total_bytes = progress
        .total_bytes
        .map_or_else(|| "unknown".to_owned(), |value| value.to_string());
    format!(
        "file transfer progress: stage={}, bytes_transferred={}, total_bytes={}, resumed_bytes={}, retry_count={}, elapsed_seconds={elapsed_seconds}",
        progress.stage,
        progress.bytes_transferred,
        total_bytes,
        progress.resumed_bytes,
        progress.retry_count,
    )
}

fn format_sftp_heartbeat(
    direction: SftpDirection,
    progress: Option<&SftpProgress>,
    elapsed_seconds: u64,
    stalled_seconds: u64,
) -> String {
    progress.map_or_else(
        || format!(
            "file transfer heartbeat: direction={direction:?}, stage=awaiting_progress, elapsed_seconds={elapsed_seconds}, no_progress_seconds={stalled_seconds}"
        ),
        |progress| {
            let total_bytes = progress
                .total_bytes
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string());
            format!(
                "file transfer heartbeat: direction={direction:?}, stage={}, bytes_transferred={}, total_bytes={}, resumed_bytes={}, retry_count={}, elapsed_seconds={elapsed_seconds}, no_progress_seconds={stalled_seconds}",
                progress.stage,
                progress.bytes_transferred,
                total_bytes,
                progress.resumed_bytes,
                progress.retry_count,
            )
        },
    )
}

fn emit_sftp_progress(
    request: &SftpRequest,
    stage: &str,
    bytes_transferred: u64,
    total_bytes: Option<u64>,
    resumed_bytes: u64,
    retry_count: u32,
) {
    emit_sftp_progress_to(
        request.progress_tx.as_ref(),
        stage,
        bytes_transferred,
        total_bytes,
        resumed_bytes,
        retry_count,
    );
}

fn emit_sftp_progress_to(
    progress_tx: Option<&mpsc::UnboundedSender<SftpProgress>>,
    stage: &str,
    bytes_transferred: u64,
    total_bytes: Option<u64>,
    resumed_bytes: u64,
    retry_count: u32,
) {
    if let Some(progress_tx) = progress_tx {
        let _ = progress_tx.send(SftpProgress {
            stage: stage.to_owned(),
            bytes_transferred,
            total_bytes,
            resumed_bytes,
            retry_count,
        });
    }
}

fn require_exec_transfer_marker(
    outcome: &ExecResult,
    marker: &str,
    stage: &str,
    remote_path: &str,
    temporary_path: &str,
) -> Result<(), TransportError> {
    require_exec_transfer_success(outcome, stage, remote_path, temporary_path)?;
    if outcome.stdout.lines().any(|line| line == marker) {
        return Ok(());
    }
    Err(TransportError::FileTransfer(format!(
        "{stage} did not return marker {marker}; exit_code={:?}: {}",
        outcome.exit_code,
        sanitized_transfer_diagnostics(
            &format!("{}{}", outcome.stdout, outcome.stderr),
            remote_path,
            temporary_path
        )
    )))
}

fn require_exec_upload_chunk_success(
    outcome: &ExecResult,
    chunk_index: u64,
    expected_size: u64,
    expected_sha256: &str,
    remote_path: &str,
    temporary_path: &str,
) -> Result<(), TransportError> {
    let stage = "verify pooled upload chunk";
    require_exec_transfer_success(outcome, stage, remote_path, temporary_path)?;
    let marker = format!("REMOTE_HOSTS_CHUNK_OK {chunk_index} {expected_size} ");
    if let Some(line) = outcome
        .stdout
        .lines()
        .find(|line| line.starts_with(&marker))
    {
        let digest = &line[marker.len()..];
        if digest.eq_ignore_ascii_case(expected_sha256) {
            return Ok(());
        }
    }
    Err(TransportError::FileTransfer(format!(
        "{stage} did not return marker {marker}; exit_code={:?}: {}",
        outcome.exit_code,
        sanitized_transfer_diagnostics(
            &format!("{}{}", outcome.stdout, outcome.stderr),
            remote_path,
            temporary_path
        )
    )))
}

fn require_exec_transfer_success(
    outcome: &ExecResult,
    stage: &str,
    remote_path: &str,
    temporary_path: &str,
) -> Result<(), TransportError> {
    if outcome.truncated {
        return Err(TransportError::FileTransfer(format!(
            "{stage} diagnostics exceeded the bounded limit"
        )));
    }
    if outcome.exit_code.is_none_or(|exit_code| exit_code == 0) {
        return Ok(());
    }
    Err(TransportError::FileTransfer(format!(
        "{stage} failed with exit_code={:?}: {}",
        outcome.exit_code,
        sanitized_transfer_diagnostics(
            &format!("{}{}", outcome.stdout, outcome.stderr),
            remote_path,
            temporary_path
        )
    )))
}

fn russh_exec_upload_initialize_command(
    spec: &FileTransferSpec,
    temporary_path: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> String {
    let destination = shell_quote(&spec.remote_path);
    let temporary = shell_quote(temporary_path);
    let mismatched_destination = match spec.overwrite {
        SftpOverwritePolicy::Deny => "exit 73",
        SftpOverwritePolicy::Replace => ":",
    };
    format!(
        "set -eu\ndest={destination}\ntmp={temporary}\nexpected_digest={expected_sha256}\n[ \"${{#expected_digest}}\" -eq 64 ] || exit 76\nparent=${{dest%/*}}\n[ -n \"$parent\" ] || parent=/\n[ -d \"$parent\" ] && [ ! -L \"$parent\" ] || exit 72\nhash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; elif command -v shasum >/dev/null 2>&1; then shasum -a 256 \"$1\" | awk '{{print $1}}'; else exit 75; fi; }}\nif [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; dest_bytes=$(wc -c < \"$dest\" | tr -d '[:space:]'); dest_digest=$(hash_file \"$dest\"); if [ \"$dest_bytes\" = \"{expected_size}\" ] && [ \"$dest_digest\" = \"$expected_digest\" ]; then if [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; rm -f \"$tmp\"; fi; printf 'REMOTE_HOSTS_UPLOAD_COMPLETE %s %s\\n' \"$dest_bytes\" \"$dest_digest\"; exit 0; fi; {mismatched_destination}; fi\nif [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; else umask 077; : > \"$tmp\"; chmod 600 \"$tmp\"; fi\nbytes=$(wc -c < \"$tmp\" | tr -d '[:space:]')\n[ \"$bytes\" -le \"{expected_size}\" ] || exit 77\ndigest=$(hash_file \"$tmp\")\nprintf 'REMOTE_HOSTS_UPLOAD_READY %s %s\\n' \"$bytes\" \"$digest\"\n"
    )
}

fn russh_exec_upload_chunk_command(
    temporary_path: &str,
    chunk_index: u64,
    expected_offset: u64,
    payload_bytes: &[u8],
) -> String {
    let temporary = shell_quote(temporary_path);
    let encoded = BASE64_STANDARD.encode(payload_bytes);
    let payload = shell_quote(&encoded);
    let payload_size = payload_bytes.len();
    let next_offset = expected_offset.saturating_add(payload_size as u64);
    let payload_sha256 = format!("{:x}", Sha256::digest(payload_bytes));
    format!(
        "set -eu\ntmp={temporary}\nn=$(wc -c <\"$tmp\" | tr -d '[:space:]')\nif [ \"$n\" = \"{expected_offset}\" ]; then printf '%s' {payload} | (base64 -d 2>/dev/null || base64 -D) >>\"$tmp\"; elif [ \"$n\" != \"{next_offset}\" ]; then exit 78; fi\n[ \"$(wc -c <\"$tmp\" | tr -d '[:space:]')\" = \"{next_offset}\" ] || exit 78\nprintf 'REMOTE_HOSTS_CHUNK_OK {chunk_index} {next_offset} {payload_sha256}\\n'\n"
    )
}

fn russh_exec_upload_chunk_verify_command(
    temporary_path: &str,
    chunk_index: u64,
    expected_size: u64,
    payload_size: usize,
    payload_sha256: &str,
) -> String {
    let temporary = shell_quote(temporary_path);
    format!(
        "set -eu\ntmp={temporary}\n[ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74\nn=$(wc -c <\"$tmp\" | tr -d '[:space:]')\n[ \"$n\" = \"{expected_size}\" ] || exit 78\nif command -v sha256sum >/dev/null 2>&1; then digest=$(tail -c {payload_size} \"$tmp\" | sha256sum | awk '{{print $1}}'); elif command -v shasum >/dev/null 2>&1; then digest=$(tail -c {payload_size} \"$tmp\" | shasum -a 256 | awk '{{print $1}}'); else exit 75; fi\n[ \"$digest\" = \"{payload_sha256}\" ] || exit 79\nprintf 'REMOTE_HOSTS_CHUNK_OK {chunk_index} {expected_size} %s\\n' \"$digest\"\n"
    )
}

fn russh_exec_upload_finalize_command(
    spec: &FileTransferSpec,
    temporary_path: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> String {
    let destination = shell_quote(&spec.remote_path);
    let temporary = shell_quote(temporary_path);
    let mode = spec
        .mode
        .map_or_else(String::new, |mode| format!("chmod {mode:o} \"$tmp\"\n"));
    let placement = match spec.overwrite {
        SftpOverwritePolicy::Deny => format!(
            "if [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; dest_bytes=$(wc -c < \"$dest\" | tr -d '[:space:]'); if command -v sha256sum >/dev/null 2>&1; then dest_digest=$(sha256sum \"$dest\" | awk '{{print $1}}'); else dest_digest=$(shasum -a 256 \"$dest\" | awk '{{print $1}}'); fi; [ \"$dest_bytes\" = \"{expected_size}\" ] && [ \"$dest_digest\" = \"{expected_sha256}\" ] || exit 73; rm -f \"$tmp\"; else ln \"$tmp\" \"$dest\"; rm -f \"$tmp\"; fi"
        ),
        SftpOverwritePolicy::Replace => {
            "if [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; fi\nmv -f \"$tmp\" \"$dest\"".to_owned()
        }
    };
    format!(
        "set -eu\ndest={destination}\ntmp={temporary}\nhash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; else shasum -a 256 \"$1\" | awk '{{print $1}}'; fi; }}\nif [ ! -f \"$tmp\" ] || [ -L \"$tmp\" ]; then if [ -f \"$dest\" ] && [ ! -L \"$dest\" ]; then bytes=$(wc -c < \"$dest\" | tr -d '[:space:]'); digest=$(hash_file \"$dest\"); [ \"$bytes\" = \"{expected_size}\" ] && [ \"$digest\" = \"{expected_sha256}\" ] || exit 76; printf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$bytes\" \"$digest\"; exit 0; fi; exit 74; fi\nbytes=$(wc -c < \"$tmp\" | tr -d '[:space:]')\ndigest=$(hash_file \"$tmp\")\n[ \"$bytes\" = \"{expected_size}\" ] && [ \"$digest\" = \"{expected_sha256}\" ] || exit 76\n{mode}{placement}\nprintf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$bytes\" \"$digest\"\n"
    )
}

fn russh_exec_upload_destination_verify_command(
    remote_path: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> String {
    let destination = shell_quote(remote_path);
    format!(
        "set -u\ndest={destination}\n[ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 71\nbytes=$(wc -c < \"$dest\" | tr -d '[:space:]') || exit 75\n[ \"$bytes\" = \"{expected_size}\" ] || exit 76\nif command -v sha256sum >/dev/null 2>&1; then digest=$(sha256sum \"$dest\" | awk '{{print $1}}') || exit 75; elif command -v shasum >/dev/null 2>&1; then digest=$(shasum -a 256 \"$dest\" | awk '{{print $1}}') || exit 75; else exit 75; fi\n[ \"$digest\" = \"{expected_sha256}\" ] || exit 76\n"
    )
}

fn russh_exec_inline_upload_command(
    spec: &FileTransferSpec,
    temporary_path: &str,
    expected_size: u64,
    expected_sha256: &str,
    payload_bytes: &[u8],
) -> String {
    let destination = shell_quote(&spec.remote_path);
    let temporary = shell_quote(temporary_path);
    let payload = shell_quote(&BASE64_STANDARD.encode(payload_bytes));
    let mode = spec
        .mode
        .map_or_else(String::new, |mode| format!("chmod {mode:o} \"$tmp\"\n"));
    let placement = match spec.overwrite {
        SftpOverwritePolicy::Deny => "ln \"$tmp\" \"$dest\"\nrm -f \"$tmp\"".to_owned(),
        SftpOverwritePolicy::Replace => {
            "if [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; fi\nmv -f \"$tmp\" \"$dest\"".to_owned()
        }
    };
    let mismatched_destination = match spec.overwrite {
        SftpOverwritePolicy::Deny => "exit 73",
        SftpOverwritePolicy::Replace => ":",
    };
    format!(
        "set -eu\ndest={destination}\ntmp={temporary}\nexpected_digest={expected_sha256}\nparent=${{dest%/*}}\n[ -n \"$parent\" ] || parent=/\n[ -d \"$parent\" ] && [ ! -L \"$parent\" ] || exit 72\nhash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; elif command -v shasum >/dev/null 2>&1; then shasum -a 256 \"$1\" | awk '{{print $1}}'; else exit 75; fi; }}\nif [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; dest_bytes=$(wc -c < \"$dest\" | tr -d '[:space:]'); dest_digest=$(hash_file \"$dest\"); if [ \"$dest_bytes\" = \"{expected_size}\" ] && [ \"$dest_digest\" = \"$expected_digest\" ]; then if [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; rm -f \"$tmp\"; fi; printf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$dest_bytes\" \"$dest_digest\"; exit 0; fi; {mismatched_destination}; fi\nif [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; fi\numask 077\nprintf '%s' {payload} | (base64 -d 2>/dev/null || base64 -D) > \"$tmp\"\nchmod 600 \"$tmp\"\nbytes=$(wc -c < \"$tmp\" | tr -d '[:space:]')\ndigest=$(hash_file \"$tmp\")\n[ \"$bytes\" = \"{expected_size}\" ] && [ \"$digest\" = \"$expected_digest\" ] || exit 76\n{mode}{placement}\nprintf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$bytes\" \"$digest\"\n"
    )
}

fn russh_exec_stream_upload_command(
    spec: &FileTransferSpec,
    temporary_path: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> String {
    let destination = shell_quote(&spec.remote_path);
    let temporary = shell_quote(temporary_path);
    let mode = spec
        .mode
        .map_or_else(String::new, |mode| format!("chmod {mode:o} \"$tmp\"\n"));
    let placement = match spec.overwrite {
        SftpOverwritePolicy::Deny => "ln \"$tmp\" \"$dest\"\nrm -f \"$tmp\"".to_owned(),
        SftpOverwritePolicy::Replace => {
            "if [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; fi\nmv -f \"$tmp\" \"$dest\"".to_owned()
        }
    };
    let mismatched_destination = match spec.overwrite {
        SftpOverwritePolicy::Deny => "exit 73",
        SftpOverwritePolicy::Replace => ":",
    };
    format!(
        "set -eu\ndest={destination}\ntmp={temporary}\nexpected_digest={expected_sha256}\nparent=${{dest%/*}}\n[ -n \"$parent\" ] || parent=/\n[ -d \"$parent\" ] && [ ! -L \"$parent\" ] || exit 72\nhash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; elif command -v shasum >/dev/null 2>&1; then shasum -a 256 \"$1\" | awk '{{print $1}}'; else exit 75; fi; }}\nif [ -e \"$dest\" ] || [ -L \"$dest\" ]; then [ -f \"$dest\" ] && [ ! -L \"$dest\" ] || exit 73; dest_bytes=$(wc -c < \"$dest\" | tr -d '[:space:]'); dest_digest=$(hash_file \"$dest\"); if [ \"$dest_bytes\" = \"{expected_size}\" ] && [ \"$dest_digest\" = \"$expected_digest\" ]; then cat >/dev/null; if [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; rm -f \"$tmp\"; fi; printf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$dest_bytes\" \"$dest_digest\"; exit 0; fi; {mismatched_destination}; fi\nif [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; fi\numask 077\ncat > \"$tmp\"\nchmod 600 \"$tmp\"\nbytes=$(wc -c < \"$tmp\" | tr -d '[:space:]')\ndigest=$(hash_file \"$tmp\")\n[ \"$bytes\" = \"{expected_size}\" ] && [ \"$digest\" = \"$expected_digest\" ] || exit 76\n{mode}{placement}\nprintf 'REMOTE_HOSTS_TRANSFER_OK %s %s\\n' \"$bytes\" \"$digest\"\n"
    )
}

fn russh_exec_upload_cleanup_command(temporary_path: &str) -> String {
    let temporary = shell_quote(temporary_path);
    format!(
        "set -eu\ntmp={temporary}\nif [ -e \"$tmp\" ] || [ -L \"$tmp\" ]; then [ -f \"$tmp\" ] && [ ! -L \"$tmp\" ] || exit 74; rm -f \"$tmp\"; fi\nprintf 'REMOTE_HOSTS_RESET_OK\\n'\n"
    )
}

fn russh_exec_download_command(spec: &FileTransferSpec) -> String {
    let source = shell_quote(&spec.remote_path);
    let expected_digest = spec
        .expected_sha256
        .as_deref()
        .map_or_else(String::new, |digest| {
            format!("[ \"$digest\" = \"{digest}\" ] || exit 76\n")
        });
    format!(
        "set -eu\nsrc={source}\n[ -f \"$src\" ] && [ ! -L \"$src\" ] || exit 71\nbytes=$(wc -c < \"$src\" | tr -d '[:space:]')\n[ \"$bytes\" -le \"{}\" ] || exit 77\nif command -v sha256sum >/dev/null 2>&1; then digest=$(sha256sum \"$src\"); digest=${{digest%% *}}; elif command -v shasum >/dev/null 2>&1; then digest=$(shasum -a 256 \"$src\"); digest=${{digest%% *}}; else exit 75; fi\n{expected_digest}cat \"$src\"\nprintf 'REMOTE_HOSTS_TRANSFER_META %s %s\\n' \"$bytes\" \"$digest\" >&2\n",
        spec.max_size_bytes
    )
}

fn russh_exec_download_request_command(spec: &FileTransferSpec) -> String {
    russh_transfer_exec_command(&russh_exec_download_command(spec))
}

fn parse_transfer_marker(output: &str, marker: &str) -> Result<(u64, String), TransportError> {
    let line = output
        .lines()
        .find(|line| line.starts_with(marker))
        .ok_or_else(|| {
            TransportError::FileTransfer(format!(
                "remote transfer did not return the required {marker} marker"
            ))
        })?;
    let mut fields = line.split_ascii_whitespace();
    if fields.next() != Some(marker) {
        return Err(TransportError::FileTransfer(
            "remote transfer marker is malformed".to_owned(),
        ));
    }
    let size = fields
        .next()
        .ok_or_else(|| TransportError::FileTransfer("remote size is missing".to_owned()))?
        .parse::<u64>()
        .map_err(|error| TransportError::FileTransfer(format!("parse remote size: {error}")))?;
    let sha256 = fields
        .next()
        .ok_or_else(|| TransportError::FileTransfer("remote SHA-256 is missing".to_owned()))?
        .to_ascii_lowercase();
    if fields.next().is_some()
        || sha256.len() != 64
        || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(TransportError::FileTransfer(
            "remote transfer marker is malformed".to_owned(),
        ));
    }
    Ok((size, sha256))
}

enum ExecUploadRemoteStatus {
    Ready { bytes: u64, prefix_sha256: String },
    Complete { size: u64, sha256: String },
}

fn parse_exec_upload_status(output: &str) -> Result<ExecUploadRemoteStatus, TransportError> {
    if output
        .lines()
        .any(|line| line.starts_with("REMOTE_HOSTS_UPLOAD_COMPLETE"))
    {
        let (size, sha256) = parse_transfer_marker(output, "REMOTE_HOSTS_UPLOAD_COMPLETE")?;
        return Ok(ExecUploadRemoteStatus::Complete { size, sha256 });
    }
    let (bytes, prefix_sha256) = parse_transfer_marker(output, "REMOTE_HOSTS_UPLOAD_READY")?;
    Ok(ExecUploadRemoteStatus::Ready {
        bytes,
        prefix_sha256,
    })
}

fn sanitized_transfer_diagnostics(
    diagnostics: &str,
    remote_path: &str,
    temporary_path: &str,
) -> String {
    let redacted = diagnostics.replace(remote_path, "<remote-path>");
    if temporary_path.is_empty() {
        redacted
    } else {
        redacted.replace(temporary_path, "<remote-temporary-path>")
    }
}

async fn execute_russh_sftp(
    session: Arc<client::Handle<RusshClientHandler>>,
    request: SftpRequest,
) -> Result<SftpResult, TransportError> {
    let channel = session.channel_open_session().await.map_err(|error| {
        TransportError::FileTransfer(format!("open SFTP channel on pooled SSH session: {error}"))
    })?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|error| {
            TransportError::FileTransfer(format!("request SFTP subsystem: {error}"))
        })?;
    let sftp = RusshSftp::new(channel.into_stream())
        .await
        .map_err(|error| {
            TransportError::FileTransfer(format!("initialize SFTP subsystem: {error}"))
        })?;
    sftp.set_timeout(request.spec.timeout_seconds);
    let result = match request.spec.direction {
        SftpDirection::Upload => russh_upload(&sftp, &request).await,
        SftpDirection::Download => russh_download(&sftp, &request).await,
    };
    let close_result = sftp
        .close()
        .await
        .map_err(|error| TransportError::FileTransfer(format!("close SFTP subsystem: {error}")));
    match (result, close_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

async fn russh_upload(
    sftp: &RusshSftp,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError> {
    let spec = &request.spec;
    let (local_size, local_sha256) =
        hash_local_source(Path::new(&spec.local_path), spec.max_size_bytes).await?;
    ensure_expected_sha256(spec, &local_sha256)?;

    ensure_russh_remote_parent(sftp, &spec.remote_path).await?;
    ensure_russh_remote_destination(sftp, &spec.remote_path, spec.overwrite).await?;
    let temporary_path = remote_temporary_path(&spec.remote_path, request.operation_id)?;
    cleanup_russh_temporary_file(sftp, &temporary_path).await?;

    let transfer = async {
        let mut local = tokio::fs::File::open(&spec.local_path)
            .await
            .map_err(file_transfer_io)?;
        let attributes = RusshSftpMetadata {
            permissions: spec.mode,
            ..RusshSftpMetadata::empty()
        };
        let remote = sftp
            .open_with_flags_and_attributes(
                temporary_path.clone(),
                RusshSftpOpenFlags::CREATE
                    | RusshSftpOpenFlags::EXCLUDE
                    | RusshSftpOpenFlags::WRITE,
                attributes,
            )
            .await
            .map_err(|error| {
                russh_file_transfer_error("create remote temporary file", error)
            })?;
        let mut remote = Box::pin(remote);
        let (bytes_transferred, transfer_sha256) = copy_bounded_and_hash(
            Pin::new(&mut local),
            remote.as_mut(),
            spec.max_size_bytes,
            request.progress_tx.as_ref(),
            "uploading",
            Some(local_size),
        )
        .await
        .map_err(|error| {
                    file_transfer_context("stream upload to remote temporary file", error)
                })?;
        remote
            .as_mut()
            .shutdown()
            .await
            .map_err(|error| file_transfer_io_context("close remote temporary file", error))?;
        if bytes_transferred != local_size || transfer_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(
                "local file changed while it was being uploaded".to_owned(),
            ));
        }
        if let Some(mode) = spec.mode {
            sftp.set_metadata(
                temporary_path.clone(),
                RusshSftpMetadata {
                    permissions: Some(mode),
                    ..RusshSftpMetadata::empty()
                },
            )
            .await
            .map_err(|error| {
                russh_file_transfer_error("set remote temporary file mode", error)
            })?;
        }
        let (remote_size, remote_sha256) =
            hash_russh_remote_file(sftp, &temporary_path, spec.max_size_bytes).await?;
        if remote_size != local_size || remote_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(format!(
                "remote SHA-256 verification failed: local={local_sha256}, remote={remote_sha256}"
            )));
        }
        sftp.rename(temporary_path.clone(), spec.remote_path.clone())
            .await
            .map_err(|error| {
                TransportError::FileTransfer(format!(
                    "atomic remote placement failed; destination may not support the requested overwrite policy: {error}"
                ))
            })?;
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred,
            sha256: local_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }
    .await;

    if transfer.is_err() {
        let _ = sftp.remove_file(temporary_path).await;
    }
    transfer
}

async fn russh_download(
    sftp: &RusshSftp,
    request: &SftpRequest,
) -> Result<SftpResult, TransportError> {
    let spec = &request.spec;
    let source = russh_lstat(sftp, &spec.remote_path, "inspect remote source")
        .await?
        .ok_or_else(|| TransportError::FileTransfer("remote source does not exist".to_owned()))?;
    ensure_russh_regular_file(&source, "remote source")?;
    let source_size = source
        .size
        .ok_or_else(|| TransportError::FileTransfer("remote source size is unknown".to_owned()))?;
    ensure_size_within_limit(source_size, spec.max_size_bytes)?;

    let destination = Path::new(&spec.local_path);
    ensure_local_destination(destination, spec.overwrite).await?;
    let temporary_path = local_temporary_path(destination, request.operation_id)?;
    cleanup_local_temporary_file(&temporary_path).await?;

    let transfer = async {
        let remote = sftp
            .open(spec.remote_path.clone())
            .await
            .map_err(|error| russh_file_transfer_error("open remote source", error))?;
        let mut remote = Box::pin(remote);
        let mut local = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .await
            .map_err(file_transfer_io)?;
        let (bytes_transferred, remote_sha256) = copy_bounded_and_hash(
            remote.as_mut(),
            Pin::new(&mut local),
            spec.max_size_bytes,
            request.progress_tx.as_ref(),
            "downloading",
            Some(source_size),
        )
        .await
        .map_err(|error| {
                    file_transfer_context("stream remote source to local file", error)
                })?;
        local.shutdown().await.map_err(file_transfer_io)?;
        remote
            .as_mut()
            .shutdown()
            .await
            .map_err(|error| file_transfer_io_context("close remote source", error))?;
        if bytes_transferred != source_size {
            return Err(TransportError::FileTransfer(format!(
                "remote source changed while it was being downloaded: expected_bytes={source_size}, transferred_bytes={bytes_transferred}"
            )));
        }
        ensure_expected_sha256(spec, &remote_sha256)?;
        let (local_size, local_sha256) =
            hash_local_source(&temporary_path, spec.max_size_bytes).await?;
        if local_size != source_size || local_sha256 != remote_sha256 {
            return Err(TransportError::FileTransfer(format!(
                "local SHA-256 verification failed: remote={remote_sha256}, local={local_sha256}"
            )));
        }
        if let Some(mode) = spec.mode {
            set_local_mode(&temporary_path, mode).await?;
        }
        place_local_file(&temporary_path, destination, spec.overwrite).await?;
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred,
            sha256: remote_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }
    .await;

    if transfer.is_err() {
        let _ = tokio::fs::remove_file(&temporary_path).await;
    }
    transfer
}

async fn russh_lstat(
    sftp: &RusshSftp,
    path: &str,
    context: &str,
) -> Result<Option<RusshSftpMetadata>, TransportError> {
    match sftp.symlink_metadata(path.to_owned()).await {
        Ok(metadata) => Ok(Some(metadata)),
        Err(RusshSftpError::Status(status))
            if status.status_code == RusshSftpStatusCode::NoSuchFile =>
        {
            Ok(None)
        }
        Err(error) => Err(russh_file_transfer_error(context, error)),
    }
}

async fn ensure_russh_remote_parent(sftp: &RusshSftp, path: &str) -> Result<(), TransportError> {
    let parent = remote_parent(path)?;
    let metadata = russh_lstat(sftp, parent, "inspect remote parent directory")
        .await?
        .ok_or_else(|| {
            TransportError::FileTransfer("remote parent directory is missing".to_owned())
        })?;
    if !metadata.is_dir() {
        return Err(TransportError::FileTransfer(
            "remote parent path is not a directory".to_owned(),
        ));
    }
    Ok(())
}

async fn ensure_russh_remote_destination(
    sftp: &RusshSftp,
    path: &str,
    overwrite: SftpOverwritePolicy,
) -> Result<(), TransportError> {
    let Some(metadata) = russh_lstat(sftp, path, "inspect remote destination").await? else {
        return Ok(());
    };
    ensure_russh_regular_file(&metadata, "remote destination")?;
    if overwrite == SftpOverwritePolicy::Deny {
        return Err(TransportError::FileTransfer(
            "remote destination already exists and overwrite=deny".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_russh_regular_file(
    metadata: &RusshSftpMetadata,
    label: &str,
) -> Result<(), TransportError> {
    if !metadata.is_regular() {
        return Err(TransportError::FileTransfer(format!(
            "{label} is not a regular file"
        )));
    }
    Ok(())
}

async fn cleanup_russh_temporary_file(sftp: &RusshSftp, path: &str) -> Result<(), TransportError> {
    let Some(metadata) = russh_lstat(sftp, path, "inspect stale remote temporary file").await?
    else {
        return Ok(());
    };
    ensure_russh_regular_file(&metadata, "stale remote temporary path")?;
    sftp.remove_file(path.to_owned())
        .await
        .map_err(|error| russh_file_transfer_error("remove stale remote temporary file", error))
}

async fn hash_russh_remote_file(
    sftp: &RusshSftp,
    path: &str,
    max_size_bytes: u64,
) -> Result<(u64, String), TransportError> {
    let remote = sftp.open(path.to_owned()).await.map_err(|error| {
        russh_file_transfer_error("open remote temporary file for verification", error)
    })?;
    let mut remote = Box::pin(remote);
    let result = hash_reader(remote.as_mut(), max_size_bytes)
        .await
        .map_err(|error| file_transfer_context("hash remote temporary file", error));
    let close_result = remote
        .as_mut()
        .shutdown()
        .await
        .map_err(|error| file_transfer_io_context("close remote verification stream", error));
    match (result, close_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

fn russh_inactivity_timeout(
    inactivity_timeout_seconds: u64,
    keepalive_seconds: u64,
) -> Option<Duration> {
    if inactivity_timeout_seconds == 0 {
        return None;
    }
    let keepalive_grace_seconds = keepalive_seconds.saturating_mul(5);
    Some(Duration::from_secs(
        inactivity_timeout_seconds.max(keepalive_grace_seconds),
    ))
}

fn should_reap_idle_transport(
    observed_at: time::OffsetDateTime,
    last_activity_at: time::OffsetDateTime,
    idle_ttl_seconds: u64,
    available_channels: usize,
    configured_channels: usize,
) -> bool {
    if idle_ttl_seconds == 0 || available_channels < configured_channels.max(1) {
        return false;
    }
    let Ok(idle_ttl_seconds) = i64::try_from(idle_ttl_seconds) else {
        return false;
    };
    observed_at >= last_activity_at + time::Duration::seconds(idle_ttl_seconds)
}

fn russh_file_transfer_error(context: &str, error: RusshSftpError) -> TransportError {
    let message = error.to_string();
    drop(error);
    TransportError::FileTransfer(format!("{context}: {message}"))
}

/// Shared native `russh` transport pool with one SSH session cache per access path.
pub struct RusshTransportPool<C> {
    repositories: Repositories,
    credentials: Arc<C>,
    host_key_policy: HostKeyPolicy,
    known_hosts_path: Option<PathBuf>,
    connect_timeout_seconds: u64,
    inactivity_timeout_seconds: u64,
    max_new_ssh_handshakes_per_10_min: u32,
    global_handshake_limiter: SharedHandshakeBudget,
    cache: Mutex<BTreeMap<AccessPathId, Arc<RusshTransport<C>>>>,
}

/// Connector-owned transport cache maintenance.
#[async_trait]
pub trait IdleTransportReaper: Send + Sync {
    /// Releases cached authenticated transports that have no channels and exceeded their TTL.
    async fn reap_idle_transports(&self) -> Result<u64, String>;
}

impl<C> RusshTransportPool<C> {
    /// Creates a native `russh` transport pool.
    pub fn new(
        repositories: Repositories,
        credentials: Arc<C>,
        host_key_policy: HostKeyPolicy,
        known_hosts_path: Option<PathBuf>,
        connect_timeout_seconds: u64,
        inactivity_timeout_seconds: u64,
        max_new_ssh_handshakes_per_10_min: u32,
    ) -> Self {
        Self {
            repositories,
            credentials,
            host_key_policy,
            known_hosts_path,
            connect_timeout_seconds,
            inactivity_timeout_seconds,
            max_new_ssh_handshakes_per_10_min,
            global_handshake_limiter: HandshakeLimiter::shared_global(
                max_new_ssh_handshakes_per_10_min,
            ),
            cache: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<C> RusshTransportPool<C>
where
    C: SshCredentialProvider + 'static,
{
    /// Returns a cached native SSH transport for an access path.
    ///
    /// # Errors
    ///
    /// Returns an error when the access path cannot be loaded.
    pub async fn transport_for_access_path(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<Arc<RusshTransport<C>>, String> {
        let access_path = self
            .repositories
            .access_paths
            .get(access_path_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("access path not found: {access_path_id}"))?;
        reject_unsupported_multi_hop_route(
            &self.repositories,
            &self.cache,
            &access_path,
            "native russh",
        )
        .await?;
        let host = self
            .repositories
            .hosts
            .get(access_path.host_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("host not found: {}", access_path.host_id))?;
        let windows = matches!(host.kind, HostKind::Windows);
        let use_exec_file_transfer = !windows
            && access_path.route_type == RouteType::Bastion
            && access_path.proxy_chain.is_empty();
        let config = RusshTransportConfig {
            access_path_id,
            address: access_path.address,
            port: access_path.port,
            username: access_path.username,
            windows,
            use_exec_file_transfer,
            host_key_policy: self.host_key_policy,
            known_hosts_path: self.known_hosts_path.clone(),
            connect_timeout_seconds: self.connect_timeout_seconds,
            inactivity_timeout_seconds: self.inactivity_timeout_seconds,
            idle_ttl_seconds: access_path.idle_ttl_seconds,
            keepalive_seconds: access_path.keepalive_seconds,
            max_concurrent_channels: access_path.max_concurrent_channels,
            max_new_connections_per_minute: access_path.max_new_connections_per_minute,
            max_new_ssh_handshakes_per_10_min: self.max_new_ssh_handshakes_per_10_min,
        };
        {
            let mut cache = self.cache.lock().await;
            if let Some(transport) = cache.get(&access_path_id) {
                if transport.config == config {
                    return Ok(Arc::clone(transport));
                }
                cache.remove(&access_path_id);
            }
        }
        let transport = Arc::new(RusshTransport::with_shared_handshake_budget(
            config,
            Arc::clone(&self.credentials),
            self.repositories.authorized_key_bootstrap.clone(),
            Arc::clone(&self.global_handshake_limiter),
        ));

        let mut cache = self.cache.lock().await;
        let cached = cache
            .entry(access_path_id)
            .or_insert_with(|| Arc::clone(&transport));
        Ok(Arc::clone(cached))
    }

    async fn reap_idle_transports_inner(&self) -> Result<u64, String> {
        let observed_at = now_utc();
        let transports: Vec<_> = self.cache.lock().await.values().cloned().collect();
        let mut reaped = 0_u64;
        for transport in transports {
            if !transport.reap_idle_session(observed_at).await {
                continue;
            }
            let access_path = self
                .repositories
                .access_paths
                .get(transport.config.access_path_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "access path disappeared while reaping idle transport: {}",
                        transport.config.access_path_id
                    )
                })?;
            if let Some(connector_id) = access_path.connector_id {
                self.repositories
                    .ssh_transport_runtimes
                    .upsert(&SshTransportRuntime {
                        access_path_id: access_path.id,
                        connector_id,
                        telemetry: transport.telemetry.snapshot(),
                        updated_at: observed_at,
                    })
                    .await
                    .map_err(|error| error.to_string())?;
            }
            reaped = reaped.saturating_add(1);
        }
        Ok(reaped)
    }
}

#[async_trait]
impl<C> IdleTransportReaper for RusshTransportPool<C>
where
    C: SshCredentialProvider + 'static,
{
    async fn reap_idle_transports(&self) -> Result<u64, String> {
        self.reap_idle_transports_inner().await
    }
}

/// Native `russh` transport provider with one guarded transport per access path.
pub struct RusshTransportProvider<C> {
    pool: Arc<RusshTransportPool<C>>,
    policy: ServerProtectionPolicy,
    cache: Mutex<BTreeMap<AccessPathId, RusshGuardedTransportCacheEntry<C>>>,
}

struct RusshGuardedTransportCacheEntry<C> {
    pooled: Arc<RusshTransport<C>>,
    transport: Arc<dyn RemoteTransport>,
}

impl<C> RusshTransportProvider<C> {
    /// Creates a native `russh` provider.
    pub fn new(
        repositories: Repositories,
        credentials: Arc<C>,
        host_key_policy: HostKeyPolicy,
        known_hosts_path: Option<PathBuf>,
        connect_timeout_seconds: u64,
        inactivity_timeout_seconds: u64,
        policy: ServerProtectionPolicy,
    ) -> Self {
        Self::with_pool(
            Arc::new(RusshTransportPool::new(
                repositories,
                credentials,
                host_key_policy,
                known_hosts_path,
                connect_timeout_seconds,
                inactivity_timeout_seconds,
                policy.max_new_ssh_handshakes_per_10_min,
            )),
            policy,
        )
    }

    /// Creates a provider from an existing native `russh` pool.
    pub fn with_pool(pool: Arc<RusshTransportPool<C>>, policy: ServerProtectionPolicy) -> Self {
        Self {
            pool,
            policy,
            cache: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<C> RusshTransportProvider<C>
where
    C: SshCredentialProvider + 'static,
{
    async fn cached_transport(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<Arc<dyn RemoteTransport>, String> {
        let pooled = self.pool.transport_for_access_path(access_path_id).await?;
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&access_path_id)
                && Arc::ptr_eq(&entry.pooled, &pooled)
            {
                return Ok(Arc::clone(&entry.transport));
            }
        }
        let transport = GuardedTransport::new(
            SharedRemoteTransport::new(Arc::clone(&pooled)),
            self.policy.clone(),
        );
        let transport: Arc<dyn RemoteTransport> = Arc::new(transport);

        let mut cache = self.cache.lock().await;
        cache.insert(
            access_path_id,
            RusshGuardedTransportCacheEntry {
                pooled,
                transport: Arc::clone(&transport),
            },
        );
        Ok(transport)
    }
}

#[async_trait]
impl<C> RemoteTransportProvider for RusshTransportProvider<C>
where
    C: SshCredentialProvider + 'static,
{
    async fn transport_for(
        &self,
        operation: &OperationRun,
    ) -> Result<Arc<dyn RemoteTransport>, String> {
        self.cached_transport(operation.access_path_id).await
    }
}

/// Native `russh` PTY backend factory keyed by access path.
pub struct RusshPtyBackendFactory<C> {
    repositories: Repositories,
    pool: Arc<RusshTransportPool<C>>,
    term: String,
    columns: u32,
    rows: u32,
}

impl<C> RusshPtyBackendFactory<C> {
    /// Creates a native `russh` PTY backend factory.
    pub fn new(
        repositories: Repositories,
        credentials: Arc<C>,
        host_key_policy: HostKeyPolicy,
        known_hosts_path: Option<PathBuf>,
        connect_timeout_seconds: u64,
        inactivity_timeout_seconds: u64,
    ) -> Self {
        Self::with_pool(
            repositories.clone(),
            Arc::new(RusshTransportPool::new(
                repositories,
                credentials,
                host_key_policy,
                known_hosts_path,
                connect_timeout_seconds,
                inactivity_timeout_seconds,
                ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
            )),
        )
    }

    /// Creates a native `russh` PTY backend factory from an existing pool.
    pub fn with_pool(repositories: Repositories, pool: Arc<RusshTransportPool<C>>) -> Self {
        Self {
            repositories,
            pool,
            term: std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_owned()),
            columns: 120,
            rows: 40,
        }
    }

    /// Overrides terminal characteristics sent in `request-pty`.
    #[must_use]
    pub fn with_terminal(mut self, term: impl Into<String>, columns: u32, rows: u32) -> Self {
        self.term = term.into();
        self.columns = columns.max(1);
        self.rows = rows.max(1);
        self
    }

    async fn transport_for_session(
        &self,
        session_id: SessionId,
    ) -> Result<Arc<RusshTransport<C>>, ConnectorPtyError>
    where
        C: SshCredentialProvider + 'static,
    {
        let connection = self
            .repositories
            .connection_sessions
            .get(session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("connection session not found: {session_id}"))
            })?;
        self.pool
            .transport_for_access_path(connection.access_path_id)
            .await
            .map_err(ConnectorPtyError::Backend)
    }
}

#[async_trait]
impl<C> ManagedPtyBackend for RusshPtyBackendFactory<C>
where
    C: SshCredentialProvider + 'static,
{
    fn capabilities(&self) -> PtyBackendCapabilities {
        PtyBackendCapabilities::russh_native_pty()
    }

    async fn spawn(
        &self,
        request: PtyBackendSpawnRequest,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError> {
        let transport = self.transport_for_session(request.session_id).await?;
        let channel_permit = transport.try_acquire_pty_channel()?;
        let before = transport.transport_telemetry();
        let session = transport.reserve_pty_channel().await?;
        let channel_result = async {
            let channel = session
                .channel_open_session()
                .await
                .map_err(|error| ConnectorPtyError::Backend(error.to_string()))?;
            channel
                .request_pty(false, &self.term, self.columns, self.rows, 0, 0, &[])
                .await
                .map_err(|error| ConnectorPtyError::Backend(error.to_string()))?;
            channel
                .request_shell(true)
                .await
                .map_err(|error| ConnectorPtyError::Backend(error.to_string()))?;
            Ok(channel)
        }
        .await;
        let channel = match channel_result {
            Ok(channel) => channel,
            Err(error) => {
                transport.release_pty_channel(&session).await;
                return Err(error);
            }
        };

        let (input_tx, input_rx) = mpsc::channel::<String>(64);
        let (output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(128);
        let (close_tx, close_rx) = oneshot::channel::<()>();
        let initial_input = shell_change_dir_input(request.cwd.as_deref());
        tokio::spawn(run_russh_pty_channel(
            Arc::clone(&transport),
            session,
            channel,
            input_rx,
            output_tx,
            close_rx,
            initial_input,
        ));

        Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx)
            .with_channel_permit(channel_permit)
            .with_transport_observation(before.as_ref(), transport.transport_telemetry()))
    }
}

#[derive(Clone, Debug)]
struct RusshClientHandler {
    host: String,
    port: u16,
    policy: HostKeyPolicy,
    known_hosts_path: Option<PathBuf>,
}

impl client::Handler for RusshClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        match self.policy {
            HostKeyPolicy::Accept => Ok(true),
            HostKeyPolicy::Strict => Ok(self.check_known_host(server_public_key)),
            HostKeyPolicy::Add => {
                if self.check_known_host(server_public_key) {
                    return Ok(true);
                }
                Ok(self.learn_known_host(server_public_key))
            }
        }
    }
}

impl RusshClientHandler {
    fn check_known_host(&self, server_public_key: &russh::keys::ssh_key::PublicKey) -> bool {
        self.known_hosts_path.as_ref().map_or_else(
            || check_known_hosts(&self.host, self.port, server_public_key).unwrap_or(false),
            |path| {
                check_known_hosts_path(&self.host, self.port, server_public_key, path)
                    .unwrap_or(false)
            },
        )
    }

    fn learn_known_host(&self, server_public_key: &russh::keys::ssh_key::PublicKey) -> bool {
        self.known_hosts_path.as_ref().map_or_else(
            || learn_known_hosts(&self.host, self.port, server_public_key).is_ok(),
            |path| learn_known_hosts_path(&self.host, self.port, server_public_key, path).is_ok(),
        )
    }
}

struct RusshAuthentication {
    used_password: bool,
    bootstrap_public_key: Option<PublicKey>,
}

struct AgentAuthentication {
    authenticated: bool,
    bootstrap_public_key: Option<PublicKey>,
    attempted_identities: usize,
}

type DynamicSshAgent =
    AgentClient<Box<dyn russh::keys::agent::client::AgentStream + Send + Unpin + 'static>>;

#[cfg(unix)]
async fn connect_local_ssh_agent() -> Result<DynamicSshAgent, russh::keys::Error> {
    AgentClient::connect_env().await.map(AgentClient::dynamic)
}

#[cfg(windows)]
async fn connect_local_ssh_agent() -> Result<DynamicSshAgent, russh::keys::Error> {
    if let Ok(path) = std::env::var("SSH_AUTH_SOCK") {
        AgentClient::connect_named_pipe(path)
            .await
            .map(AgentClient::dynamic)
    } else {
        AgentClient::connect_pageant()
            .await
            .map(AgentClient::dynamic)
    }
}

async fn authenticate_with_ssh_agent(
    session: &mut client::Handle<RusshClientHandler>,
    username: &str,
) -> Result<AgentAuthentication, String> {
    let mut agent = connect_local_ssh_agent()
        .await
        .map_err(|error| format!("connect to local SSH agent: {error}"))?;
    let identities = agent
        .request_identities()
        .await
        .map_err(|error| format!("list local SSH agent identities: {error}"))?;
    let bootstrap_public_key = identities.iter().find_map(|identity| match identity {
        AgentIdentity::PublicKey { key, .. } => Some(key.clone()),
        AgentIdentity::Certificate { .. } => None,
    });
    let hash_alg = session
        .best_supported_rsa_hash()
        .await
        .map_err(|error| format!("negotiate SSH agent RSA hash: {error}"))?
        .flatten();

    for identity in identities
        .iter()
        .take(MAX_SSH_AGENT_IDENTITIES_PER_HANDSHAKE)
    {
        let result = match identity {
            AgentIdentity::PublicKey { key, .. } => {
                session
                    .authenticate_publickey_with(username, key.clone(), hash_alg, &mut agent)
                    .await
            }
            AgentIdentity::Certificate { certificate, .. } => {
                session
                    .authenticate_certificate_with(
                        username,
                        certificate.clone(),
                        hash_alg,
                        &mut agent,
                    )
                    .await
            }
        };
        match result {
            Ok(authentication) if authentication.success() => {
                return Ok(AgentAuthentication {
                    authenticated: true,
                    bootstrap_public_key,
                    attempted_identities: identities
                        .len()
                        .min(MAX_SSH_AGENT_IDENTITIES_PER_HANDSHAKE),
                });
            }
            Ok(_) => {}
            Err(error) => tracing::debug!(%error, "SSH-agent identity authentication failed"),
        }
    }

    Ok(AgentAuthentication {
        authenticated: false,
        bootstrap_public_key,
        attempted_identities: identities.len().min(MAX_SSH_AGENT_IDENTITIES_PER_HANDSHAKE),
    })
}

fn default_private_key_paths() -> Vec<PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    let Some(home) = home else {
        return Vec::new();
    };
    let ssh_dir = PathBuf::from(home).join(".ssh");
    ["id_ed25519", "id_ecdsa", "id_rsa"]
        .into_iter()
        .map(|name| ssh_dir.join(name))
        .collect()
}

async fn load_default_private_keys() -> Vec<PrivateKey> {
    let mut keys = Vec::new();
    for path in default_private_key_paths() {
        let Ok(material) = tokio::fs::read_to_string(&path).await else {
            continue;
        };
        let material = Zeroizing::new(material);
        match decode_secret_key(material.as_str(), None) {
            Ok(key) => keys.push(key),
            Err(error) => {
                tracing::debug!(path = %path.display(), %error, "default SSH key is unavailable without a passphrase");
            }
        }
        if keys.len() >= MAX_SSH_AGENT_IDENTITIES_PER_HANDSHAKE {
            break;
        }
    }
    keys
}

async fn authenticate_with_default_private_keys(
    session: &mut client::Handle<RusshClientHandler>,
    username: &str,
) -> Result<AgentAuthentication, String> {
    let keys = load_default_private_keys().await;
    let bootstrap_public_key = keys.first().map(|key| key.public_key().clone());
    let attempted_identities = keys.len();
    let hash_alg = session
        .best_supported_rsa_hash()
        .await
        .map_err(|error| format!("negotiate default-key RSA hash: {error}"))?
        .flatten();
    for key in keys {
        match session
            .authenticate_publickey(
                username,
                PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
            )
            .await
        {
            Ok(authentication) if authentication.success() => {
                return Ok(AgentAuthentication {
                    authenticated: true,
                    bootstrap_public_key,
                    attempted_identities,
                });
            }
            Ok(_) => {}
            Err(error) => tracing::debug!(%error, "default private-key authentication failed"),
        }
    }
    Ok(AgentAuthentication {
        authenticated: false,
        bootstrap_public_key,
        attempted_identities,
    })
}

async fn authenticate_russh_session(
    session: &mut client::Handle<RusshClientHandler>,
    username: &str,
    credential: &SshCredentialSecret,
) -> Result<RusshAuthentication, TransportError> {
    if let Some(private_key_pem) = &credential.private_key_pem {
        let private_key = decode_secret_key(
            private_key_pem.expose_secret(),
            credential
                .private_key_passphrase
                .as_ref()
                .map(SecretString::expose_secret),
        );
        match private_key {
            Ok(private_key) => {
                let hash_alg = session
                    .best_supported_rsa_hash()
                    .await
                    .ok()
                    .flatten()
                    .flatten();
                match session
                    .authenticate_publickey(
                        username,
                        PrivateKeyWithHashAlg::new(Arc::new(private_key), hash_alg),
                    )
                    .await
                {
                    Ok(authentication) if authentication.success() => {
                        return Ok(RusshAuthentication {
                            used_password: false,
                            bootstrap_public_key: None,
                        });
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::debug!(%error, "stored private-key authentication failed");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "stored private key could not be decoded"),
        }
    }

    let mut bootstrap_public_key = None;
    if credential.use_ssh_agent {
        let mut agent_attempted_identities = 0;
        match authenticate_with_ssh_agent(session, username).await {
            Ok(agent_authentication) if agent_authentication.authenticated => {
                return Ok(RusshAuthentication {
                    used_password: false,
                    bootstrap_public_key: None,
                });
            }
            Ok(agent_authentication) => {
                agent_attempted_identities = agent_authentication.attempted_identities;
                bootstrap_public_key = agent_authentication.bootstrap_public_key;
            }
            Err(error) => tracing::debug!(%error, "local SSH-agent authentication unavailable"),
        }
        if agent_attempted_identities == 0 {
            match authenticate_with_default_private_keys(session, username).await {
                Ok(default_authentication) if default_authentication.authenticated => {
                    return Ok(RusshAuthentication {
                        used_password: false,
                        bootstrap_public_key: None,
                    });
                }
                Ok(default_authentication) => {
                    bootstrap_public_key = default_authentication.bootstrap_public_key;
                }
                Err(error) => {
                    tracing::debug!(%error, "default local SSH-key authentication unavailable");
                }
            }
        }
    }

    if let Some(password) = &credential.password {
        let auth = session
            .authenticate_password(username, password.expose_secret().to_owned())
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?;
        if auth.success() {
            return Ok(RusshAuthentication {
                used_password: true,
                bootstrap_public_key,
            });
        }
    }

    Err(TransportError::Backend(
        "ssh authentication failed".to_owned(),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum AuthorizedKeyInstallError {
    #[error("authorized-keys update timed out")]
    Timeout,
    #[error("remote account cannot update authorized keys")]
    WriteDenied,
    #[error("remote authorized-keys location is read-only")]
    ReadOnlyFilesystem,
    #[error("remote shell does not support the bootstrap command")]
    UnsupportedTargetShell,
    #[error("remote authorized-keys command failed")]
    RemoteCommandFailed,
}

impl AuthorizedKeyInstallError {
    fn reason(self) -> AuthorizedKeyBootstrapReason {
        match self {
            Self::Timeout => AuthorizedKeyBootstrapReason::Timeout,
            Self::WriteDenied => AuthorizedKeyBootstrapReason::WriteDenied,
            Self::ReadOnlyFilesystem => AuthorizedKeyBootstrapReason::ReadOnlyFilesystem,
            Self::UnsupportedTargetShell => AuthorizedKeyBootstrapReason::UnsupportedTargetShell,
            Self::RemoteCommandFailed => AuthorizedKeyBootstrapReason::RemoteCommandFailed,
        }
    }

    fn is_permanent(self) -> bool {
        matches!(
            self,
            Self::WriteDenied | Self::ReadOnlyFilesystem | Self::UnsupportedTargetShell
        )
    }
}

fn authorized_key_bootstrap_is_eligible(
    existing: Option<&AuthorizedKeyBootstrap>,
    public_key_fingerprint: &str,
    now: time::OffsetDateTime,
) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    if existing.public_key_fingerprint.as_deref() != Some(public_key_fingerprint) {
        return true;
    }
    match existing.state {
        AuthorizedKeyBootstrapState::Installed | AuthorizedKeyBootstrapState::Skipped => false,
        AuthorizedKeyBootstrapState::Attempting | AuthorizedKeyBootstrapState::Deferred => existing
            .next_retry_at
            .is_some_and(|retry_at| retry_at <= now),
    }
}

fn authorized_key_bootstrap_failure_state(
    access_path_id: AccessPathId,
    public_key_fingerprint: &str,
    previous_failure_count: u32,
    error: AuthorizedKeyInstallError,
    now: time::OffsetDateTime,
) -> AuthorizedKeyBootstrap {
    let failure_count = previous_failure_count.saturating_add(1);
    let exhausted = failure_count >= AUTHORIZED_KEY_BOOTSTRAP_MAX_FAILURES;
    let state = if error.is_permanent() || exhausted {
        AuthorizedKeyBootstrapState::Skipped
    } else {
        AuthorizedKeyBootstrapState::Deferred
    };
    let reason = if exhausted && !error.is_permanent() {
        AuthorizedKeyBootstrapReason::AttemptsExhausted
    } else {
        error.reason()
    };
    let next_retry_at = (state == AuthorizedKeyBootstrapState::Deferred).then(|| {
        let multiplier = 1_i64 << failure_count.saturating_sub(1).min(2);
        now + time::Duration::minutes(15 * multiplier)
    });
    AuthorizedKeyBootstrap {
        access_path_id,
        state,
        reason: Some(reason),
        public_key_fingerprint: Some(public_key_fingerprint.to_owned()),
        failure_count,
        attempted_at: now,
        next_retry_at,
        updated_at: now,
    }
}

async fn persist_authorized_key_bootstrap_result(
    repository: &AuthorizedKeyBootstrapRepository,
    state: &AuthorizedKeyBootstrap,
) {
    let access_path_id = state.access_path_id;
    if let Err(error) = repository.upsert(state).await {
        tracing::warn!(
            %access_path_id,
            %error,
            "failed to persist final public-key bootstrap state"
        );
    } else if state.state == AuthorizedKeyBootstrapState::Installed {
        tracing::info!(%access_path_id, "installed managed SSH public key");
    } else {
        tracing::warn!(
            %access_path_id,
            state = ?state.state,
            reason = ?state.reason,
            "managed SSH public-key bootstrap did not complete"
        );
    }
}

async fn install_authorized_key(
    session: Arc<client::Handle<RusshClientHandler>>,
    public_key: &PublicKey,
    windows: bool,
) -> Result<(), AuthorizedKeyInstallError> {
    let managed_key = PublicKey::new(public_key.key_data().clone(), "remote-hosts-managed");
    let key = managed_key
        .to_openssh()
        .map_err(|_| AuthorizedKeyInstallError::RemoteCommandFailed)?;
    let command = authorized_keys_install_command(&key, windows);
    execute_authorized_key_install_with_timeout(
        AUTHORIZED_KEY_BOOTSTRAP_TIMEOUT,
        execute_russh_command(session, command, 16 * 1024),
    )
    .await
}

async fn execute_authorized_key_install_with_timeout<F>(
    timeout: Duration,
    execute: F,
) -> Result<(), AuthorizedKeyInstallError>
where
    F: Future<Output = Result<ExecResult, TransportError>>,
{
    let result = tokio::time::timeout(timeout, execute)
        .await
        .map_err(|_| AuthorizedKeyInstallError::Timeout)?
        .map_err(|error| match error {
            TransportError::Timeout => AuthorizedKeyInstallError::Timeout,
            TransportError::PolicyDenied(_)
            | TransportError::LocalHandshakeBudgetExhausted { .. }
            | TransportError::Backend(_)
            | TransportError::FileTransfer(_) => AuthorizedKeyInstallError::RemoteCommandFailed,
        })?;
    if result.exit_code == Some(0) {
        return Ok(());
    }
    Err(classify_authorized_key_install_failure(&result.stderr))
}

fn classify_authorized_key_install_failure(stderr: &str) -> AuthorizedKeyInstallError {
    let stderr = stderr.to_ascii_lowercase();
    if [
        "read-only file system",
        "read only file system",
        "media is write protected",
    ]
    .iter()
    .any(|needle| stderr.contains(needle))
    {
        AuthorizedKeyInstallError::ReadOnlyFilesystem
    } else if [
        "permission denied",
        "access is denied",
        "unauthorizedaccessexception",
        "requested operation requires elevation",
    ]
    .iter()
    .any(|needle| stderr.contains(needle))
    {
        AuthorizedKeyInstallError::WriteDenied
    } else if [
        "command not found",
        "not recognized as the name of a cmdlet",
        "is not recognized as an internal or external command",
        "powershell.exe: not found",
    ]
    .iter()
    .any(|needle| stderr.contains(needle))
    {
        AuthorizedKeyInstallError::UnsupportedTargetShell
    } else {
        AuthorizedKeyInstallError::RemoteCommandFailed
    }
}

fn authorized_keys_install_command(key: &str, windows: bool) -> String {
    if windows {
        let key = key.replace('\'', "''");
        format!(
            "powershell.exe -NoProfile -NonInteractive -Command \"$ErrorActionPreference='Stop'; $d=Join-Path $HOME '.ssh'; New-Item -ItemType Directory -Force -Path $d | Out-Null; $u=Join-Path $d 'authorized_keys'; $isAdmin=([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator); $f=if ($isAdmin) {{ Join-Path $env:ProgramData 'ssh\\administrators_authorized_keys' }} else {{ $u }}; if (-not (Test-Path -LiteralPath $f)) {{ New-Item -ItemType File -Force -Path $f | Out-Null }}; $k='{key}'; $lines=@(Get-Content -LiteralPath $f -ErrorAction SilentlyContinue); if ($lines -notcontains $k) {{ Add-Content -LiteralPath $f -Value $k -Encoding ascii }}; if ($isAdmin) {{ & icacls.exe $f /inheritance:r /grant:r '*S-1-5-32-544:F' /grant:r '*S-1-5-18:F' | Out-Null; if ($LASTEXITCODE -ne 0) {{ throw 'failed to secure administrators_authorized_keys' }} }}\""
        )
    } else {
        format!(
            "umask 077; mkdir -p \"$HOME/.ssh\" && chmod 700 \"$HOME/.ssh\" && touch \"$HOME/.ssh/authorized_keys\" && chmod 600 \"$HOME/.ssh/authorized_keys\" && key={} && (grep -qxF -- \"$key\" \"$HOME/.ssh/authorized_keys\" || printf '%s\\n' \"$key\" >> \"$HOME/.ssh/authorized_keys\")",
            shell_quote(key)
        )
    }
}

async fn execute_russh_command(
    session: Arc<client::Handle<RusshClientHandler>>,
    command: String,
    output_limit_bytes: usize,
) -> Result<ExecResult, TransportError> {
    let mut channel = session
        .channel_open_session()
        .await
        .map_err(|error| TransportError::Backend(error.to_string()))?;
    channel
        .exec(true, command)
        .await
        .map_err(|error| TransportError::Backend(error.to_string()))?;
    let result = receive_russh_exec_result(&mut channel, output_limit_bytes).await;
    let _ = channel.close().await;
    result
}

async fn receive_russh_exec_result(
    channel: &mut russh::Channel<client::Msg>,
    output_limit_bytes: usize,
) -> Result<ExecResult, TransportError> {
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = None;
    let mut truncated = false;
    while let Some(message) = channel.wait().await {
        match message {
            ChannelMsg::Data { data } => {
                append_limited_utf8(&mut stdout, &data, output_limit_bytes, &mut truncated);
            }
            ChannelMsg::ExtendedData { data, ext: 1 } => {
                append_limited_utf8(&mut stderr, &data, output_limit_bytes, &mut truncated);
            }
            ChannelMsg::ExitStatus { exit_status } => {
                exit_code = i32::try_from(exit_status).ok();
            }
            ChannelMsg::ExitSignal { error_message, .. } => {
                if !error_message.is_empty() {
                    append_limited_utf8(
                        &mut stderr,
                        error_message.as_bytes(),
                        output_limit_bytes,
                        &mut truncated,
                    );
                }
            }
            ChannelMsg::Close => break,
            _ => {}
        }
    }
    Ok(ExecResult {
        exit_code,
        stdout,
        stderr,
        truncated,
    })
}

async fn run_russh_pty_channel<C>(
    transport: Arc<RusshTransport<C>>,
    session: Arc<client::Handle<RusshClientHandler>>,
    channel: russh::Channel<client::Msg>,
    input_rx: mpsc::Receiver<String>,
    output_tx: mpsc::Sender<PtyBackendOutput>,
    close_rx: oneshot::Receiver<()>,
    initial_input: Option<String>,
) where
    C: SshCredentialProvider + 'static,
{
    drive_russh_pty_channel(
        &transport,
        &session,
        channel,
        input_rx,
        output_tx,
        close_rx,
        initial_input,
    )
    .await;
    transport.release_pty_channel(&session).await;
}

async fn drive_russh_pty_channel<C>(
    transport: &RusshTransport<C>,
    session: &Arc<client::Handle<RusshClientHandler>>,
    mut channel: russh::Channel<client::Msg>,
    mut input_rx: mpsc::Receiver<String>,
    output_tx: mpsc::Sender<PtyBackendOutput>,
    mut close_rx: oneshot::Receiver<()>,
    initial_input: Option<String>,
) where
    C: SshCredentialProvider + 'static,
{
    if let Some(initial_input) = initial_input
        && let Err(error) = channel.data_bytes(initial_input.into_bytes()).await
    {
        transport.invalidate_session(session).await;
        let _ = send_pty_backend_output(
            &output_tx,
            OutputStream::System,
            format!("russh pty initial input failed: {error}"),
        )
        .await;
        let _ = channel.close().await;
        return;
    }

    let mut input_closed = false;
    loop {
        tokio::select! {
            _ = &mut close_rx => {
                let _ = channel.close().await;
                break;
            }
            input = input_rx.recv(), if !input_closed => {
                if let Some(input) = input {
                    if let Err(error) = channel.data_bytes(input.into_bytes()).await {
                        transport.invalidate_session(session).await;
                        let sent = send_pty_backend_output(
                            &output_tx,
                            OutputStream::System,
                            format!("russh pty input failed: {error}"),
                        )
                        .await;
                        if !sent {
                            break;
                        }
                        let _ = channel.close().await;
                        break;
                    }
                } else {
                    input_closed = true;
                    let _ = channel.eof().await;
                }
            }
            message = channel.wait() => {
                let Some(message) = message else {
                    break;
                };
                if russh_pty_message_invalidates_session(&message) {
                    transport.invalidate_session(session).await;
                }
                if !handle_russh_pty_message(&output_tx, message).await {
                    break;
                }
            }
        }
    }
}

fn russh_pty_message_invalidates_session(message: &ChannelMsg) -> bool {
    match message {
        ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } => {
            russh_pty_output_invalidates_session(data)
        }
        _ => false,
    }
}

fn russh_pty_output_invalidates_session(data: &[u8]) -> bool {
    String::from_utf8_lossy(data)
        .to_ascii_lowercase()
        .contains("please re-login")
}

async fn handle_russh_pty_message(
    output_tx: &mpsc::Sender<PtyBackendOutput>,
    message: ChannelMsg,
) -> bool {
    match message {
        ChannelMsg::Data { data } => {
            send_pty_backend_output(
                output_tx,
                OutputStream::Stdout,
                String::from_utf8_lossy(&data).to_string(),
            )
            .await
        }
        ChannelMsg::ExtendedData { data, ext: 1 } => {
            send_pty_backend_output(
                output_tx,
                OutputStream::Stderr,
                String::from_utf8_lossy(&data).to_string(),
            )
            .await
        }
        ChannelMsg::ExtendedData { data, ext } => {
            send_pty_backend_output(
                output_tx,
                OutputStream::System,
                format!(
                    "russh pty extended data ext={ext}: {}",
                    String::from_utf8_lossy(&data)
                ),
            )
            .await
        }
        ChannelMsg::ExitStatus { exit_status } => {
            send_pty_backend_output(
                output_tx,
                OutputStream::System,
                format!("russh pty shell exited: status={exit_status}"),
            )
            .await
        }
        ChannelMsg::ExitSignal {
            signal_name,
            core_dumped,
            error_message,
            ..
        } => {
            send_pty_backend_output(
                output_tx,
                OutputStream::System,
                russh_exit_signal_message(&signal_name, core_dumped, &error_message),
            )
            .await
        }
        ChannelMsg::Failure => {
            send_pty_backend_output(
                output_tx,
                OutputStream::System,
                "russh pty request failed".to_owned(),
            )
            .await
        }
        ChannelMsg::Close => false,
        _ => true,
    }
}

fn russh_exit_signal_message(
    signal_name: &russh::Sig,
    core_dumped: bool,
    error_message: &str,
) -> String {
    if error_message.is_empty() {
        format!("russh pty shell exited by signal: {signal_name:?}, core_dumped={core_dumped}")
    } else {
        format!(
            "russh pty shell exited by signal: {signal_name:?}, core_dumped={core_dumped}, message={error_message}"
        )
    }
}

async fn send_pty_backend_output(
    output_tx: &mpsc::Sender<PtyBackendOutput>,
    stream: OutputStream,
    text: String,
) -> bool {
    output_tx
        .send(PtyBackendOutput {
            stream,
            text,
            truncated: false,
        })
        .await
        .is_ok()
}

fn append_limited_utf8(output: &mut String, bytes: &[u8], limit: usize, truncated: &mut bool) {
    if output.len() >= limit {
        *truncated = true;
        return;
    }
    let remaining = limit - output.len();
    let take = remaining.min(bytes.len());
    output.push_str(&String::from_utf8_lossy(&bytes[..take]));
    if take < bytes.len() {
        *truncated = true;
    }
}

fn ssh_exec_command(profile: &CommandProfile, windows: bool) -> String {
    let quote = if windows {
        windows_command_quote
    } else {
        shell_quote
    };
    std::iter::once(quote(&profile.program))
        .chain(profile.args.iter().map(|arg| quote(arg)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn framed_posix_ssh_exec_command(profile: &CommandProfile, marker: &str) -> String {
    let command = ssh_exec_command(profile, false);
    framed_posix_command(&command, marker)
}

fn framed_posix_script_ssh_exec_command(script: &str, marker: &str) -> String {
    framed_posix_command(&russh_transfer_exec_command(script), marker)
}

fn framed_posix_command(command: &str, marker: &str) -> String {
    let script =
        format!("{command}\nstatus=$?\nprintf '\\n{marker} %s\\n' \"$status\"\nexit \"$status\"");
    russh_transfer_exec_command(&script)
}

fn recover_framed_exec_status(result: &mut ExecResult, marker: &str) -> bool {
    let prefix = format!("{marker} ");
    let Some(start) = result.stdout.rfind(&prefix) else {
        return false;
    };
    if start > 0 && result.stdout.as_bytes()[start - 1] != b'\n' {
        return false;
    }
    let status_start = start + prefix.len();
    let status_end = result.stdout[status_start..]
        .find('\n')
        .map_or(result.stdout.len(), |offset| status_start + offset);
    let Ok(exit_code) = result.stdout[status_start..status_end]
        .trim()
        .parse::<i32>()
    else {
        return false;
    };
    if !result.stdout[status_end..].trim().is_empty() {
        return false;
    }

    let remove_start = start
        .checked_sub(1)
        .filter(|index| result.stdout.as_bytes()[*index] == b'\n')
        .unwrap_or(start);
    let remove_end = status_end
        + usize::from(
            result
                .stdout
                .as_bytes()
                .get(status_end)
                .is_some_and(|byte| *byte == b'\n'),
        );
    result.stdout.replace_range(remove_start..remove_end, "");
    result.exit_code = Some(exit_code);
    true
}

fn windows_command_quote(value: &str) -> String {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-._/,=:\\".contains(&byte))
    {
        value.to_owned()
    } else {
        format!("\"{}\"", value.replace('"', "\\\""))
    }
}

/// Connector worker configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOperationWorkerConfig {
    /// Connector that owns the worker.
    pub connector_id: ConnectorId,
    /// Lease duration for one claimed operation.
    pub lease_seconds: u64,
    /// Maximum attempts before the worker stops claiming an operation.
    pub max_attempts: u32,
    /// Output size threshold above which content is written to a file artifact.
    pub artifact_threshold_bytes: usize,
    /// Preview bytes stored in the agent-visible summary chunk and artifact metadata.
    pub artifact_preview_bytes: usize,
}

impl ConnectorOperationWorkerConfig {
    /// Builds a production default config for a connector.
    #[must_use]
    pub fn production_default(connector_id: ConnectorId) -> Self {
        Self {
            connector_id,
            lease_seconds: 300,
            max_attempts: 3,
            artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
            artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
        }
    }
}

/// Result of one worker iteration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOperationOutcome {
    /// Operation id.
    pub operation_id: OperationId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Final operation state.
    pub state: OperationState,
    /// Final workspace state.
    pub workspace_state: WorkspaceState,
    /// Exit code, when available.
    pub exit_code: Option<i32>,
}

/// Errors returned by connector operation workers.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorWorkerError {
    /// Database error.
    #[error(transparent)]
    Database(#[from] DbError),
    /// Filesystem error.
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    /// Operation has no workspace id.
    #[error("claimed operation has no workspace id")]
    MissingWorkspace,
    /// Operation has no serialized command profile.
    #[error("claimed operation has no command profile")]
    MissingCommandProfile,
    /// Operation has no claim token.
    #[error("claimed operation has no claim token")]
    MissingClaimToken,
    /// Operation lease was lost before completion.
    #[error("operation lease was lost before completion")]
    LeaseLost,
    /// Host write lease was lost while a mutating operation was running.
    #[error("scoped host write lease was lost before mutating operation completion")]
    WriteLeaseLost,
    /// Artifact path is invalid or outside the artifact root.
    #[error("invalid artifact path: {0}")]
    InvalidArtifactPath(String),
    /// Command profile JSON is invalid.
    #[error("invalid command profile json: {0}")]
    CommandProfileJson(#[from] serde_json::Error),
    /// Command profile failed validation.
    #[error("invalid command profile: {0}")]
    CommandValidation(#[from] CommandValidationError),
    /// File-transfer payload failed validation.
    #[error("invalid file transfer payload: {0}")]
    InvalidFileTransfer(String),
    /// Transport provider failed.
    #[error("transport provider failed: {0}")]
    TransportProvider(String),
    /// Transport failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Integer conversion failed.
    #[error("integer conversion error: {0}")]
    Int(#[from] std::num::TryFromIntError),
}

/// Connector PTY manager configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorPtyManagerConfig {
    /// Connector that owns PTY sessions.
    pub connector_id: ConnectorId,
    /// Maximum input bytes per write.
    pub max_input_bytes: usize,
    /// Maximum output bytes per stored chunk.
    pub output_limit_bytes: usize,
    /// Claim lease duration for queued PTY input events.
    pub input_lease_seconds: u64,
    /// Maximum attempts for queued PTY input delivery.
    pub input_max_attempts: u32,
}

impl ConnectorPtyManagerConfig {
    /// Builds a production default config.
    pub fn production_default(connector_id: ConnectorId) -> Self {
        Self {
            connector_id,
            max_input_bytes: 16 * 1024,
            output_limit_bytes: 64 * 1024,
            input_lease_seconds: 30,
            input_max_attempts: 3,
        }
    }
}

/// Result of opening a managed PTY.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorPtyOpenOutcome {
    /// PTY session record.
    pub pty_session: PtySession,
}

/// Result of writing PTY input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorPtyInputOutcome {
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Number of bytes accepted for delivery.
    pub byte_len: usize,
}

/// Result of delivering one queued PTY input event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorPtyInputDeliveryOutcome {
    /// Input event id.
    pub input_event_id: PtyInputEventId,
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Final queue state for the event.
    pub state: PtyInputEventState,
    /// Number of input bytes processed.
    pub byte_len: usize,
    /// Redacted delivery error, when delivery failed.
    pub error: Option<String>,
}

/// Claims queued PTY input and delivers it through connector-owned backends.
#[async_trait]
pub trait QueuedPtyInputPump: Send + Sync {
    /// Reconciles persisted PTY state with the newly started connector runtime.
    async fn reconcile_startup(&self) -> Result<u64, ConnectorPtyError> {
        Ok(0)
    }

    /// Closes connector-local PTY handles whose persisted session is no longer active.
    async fn reconcile_runtime_state(&self) -> Result<u64, ConnectorPtyError> {
        Ok(0)
    }

    /// Closes PTYs that exceeded their idle policy and releases their local SSH channels.
    async fn reap_idle(
        &self,
        _idle_ttl_seconds: u64,
        _busy_ttl_seconds: u64,
    ) -> Result<u64, ConnectorPtyError> {
        Ok(0)
    }

    /// Activates one pending PTY before its first input is queued.
    async fn activate_next(&self) -> Result<Option<PtySessionId>, ConnectorPtyError> {
        Ok(None)
    }

    /// Delivers one queued PTY input event when available.
    async fn deliver_next(
        &self,
    ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, ConnectorPtyError>;
}

/// Connector-local file transfer path that can reuse a workspace's selected interactive PTY.
#[async_trait]
pub trait InteractiveFileTransferBackend: Send + Sync {
    /// Handles a transfer when the workspace route requires an interactive target selection.
    ///
    /// Returning `None` delegates to the access path's normal SFTP or exec transport.
    async fn transfer_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        request: SftpRequest,
    ) -> Result<Option<SftpResult>, TransportError>;
}

enum PtyPumpOutcome {
    Activated,
    Reconciled,
    Input(ConnectorPtyInputDeliveryOutcome),
}

async fn poll_pty_pump(
    pump: &dyn QueuedPtyInputPump,
) -> Result<Option<PtyPumpOutcome>, ConnectorPtyError> {
    if pump.reconcile_runtime_state().await? > 0 {
        return Ok(Some(PtyPumpOutcome::Reconciled));
    }
    if pump.activate_next().await?.is_some() {
        return Ok(Some(PtyPumpOutcome::Activated));
    }
    Ok(pump.deliver_next().await?.map(PtyPumpOutcome::Input))
}

/// Connector PTY manager errors.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorPtyError {
    /// Database error.
    #[error(transparent)]
    Database(#[from] DbError),
    /// Transport error.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Supervisor rejected the PTY lifecycle operation.
    #[error(transparent)]
    Supervisor(#[from] PtySessionSupervisorError),
    /// Backend failed.
    #[error("pty backend failed: {0}")]
    Backend(String),
    /// The access path has no immediately available SSH channel.
    #[error("SSH channel capacity is currently unavailable")]
    ChannelCapacityUnavailable,
    /// PTY session does not belong to this connector.
    #[error("pty session does not belong to this connector")]
    ConnectorMismatch,
    /// PTY session has no active backend process.
    #[error("pty session is not active in this connector process")]
    NotActive,
    /// A persisted active PTY no longer has its original connector-local runtime.
    #[error("pty runtime continuity was lost; open a new PTY session instead of retrying input")]
    RuntimeContinuityLost,
    /// PTY session is not accepting input.
    #[error("pty session input is not currently allowed")]
    InputNotAllowed,
    /// PTY input is invalid.
    #[error("pty input must be non-empty, at most {0} bytes, and must not contain NUL")]
    InvalidInput(usize),
    /// PTY input could not be delivered.
    #[error("pty input channel is closed")]
    InputClosed,
    /// A vault-backed sudo response was requested when no live sudo prompt remains.
    #[error("stored sudo password injection requires a live sudo password prompt")]
    StoredSudoPromptUnavailable,
    /// The connector cannot resolve a dedicated sudo credential for this access path.
    #[error("stored sudo password injection is unavailable for this access path")]
    StoredSudoPasswordUnavailable,
    /// A vault-backed SSH response was requested when no live generic password prompt remains.
    #[error("stored SSH password injection requires a live password prompt")]
    StoredSshPromptUnavailable,
    /// The connector cannot resolve an SSH password for the requested target access path.
    #[error("stored SSH password injection is unavailable for the requested target access path")]
    StoredSshPasswordUnavailable,
    /// The live prompt is not bound to the immediately preceding connector-verified SSH command.
    #[error(
        "stored SSH password injection requires the immediately preceding verified nested SSH command: {0}"
    )]
    StoredSshCommandUnverified(String),
    /// A vault-backed target sudo response was requested when no live sudo prompt remains.
    #[error("target stored sudo password injection requires a live sudo password prompt")]
    StoredTargetSudoPromptUnavailable,
    /// The connector cannot resolve a dedicated sudo password for the target access path.
    #[error(
        "target stored sudo password injection is unavailable for the requested target access path"
    )]
    StoredTargetSudoPasswordUnavailable,
    /// The live sudo prompt is not bound to an immediately preceding allowlisted recovery command.
    #[error(
        "target stored sudo password injection requires the immediately preceding verified nested sudo command: {0}"
    )]
    StoredTargetSudoCommandUnverified(String),
    /// Integer conversion failed.
    #[error("integer conversion error: {0}")]
    Int(#[from] std::num::TryFromIntError),
}

fn verified_nested_ssh_command(
    target: &remote_hosts_domain::AccessPath,
) -> Result<String, ConnectorPtyError> {
    let username_is_safe = !target.username.is_empty()
        && target.username.len() <= 64
        && target
            .username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
    let address_is_safe = !target.address.is_empty()
        && target.address.len() <= 255
        && target
            .address
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".:-_[]".contains(&byte));
    if !username_is_safe || !address_is_safe || target.port == 0 {
        return Err(ConnectorPtyError::StoredSshCommandUnverified(
            "target SSH route contains an unsafe username, address, or port".to_owned(),
        ));
    }
    let address = if target.address.contains(':')
        && !(target.address.starts_with('[') && target.address.ends_with(']'))
    {
        format!("[{}]", target.address)
    } else {
        target.address.clone()
    };
    Ok(format!(
        "/usr/bin/ssh -o StrictHostKeyChecking=yes -o NumberOfPasswordPrompts=1 -p {} {}@{}\n",
        target.port, target.username, address
    ))
}

const VERIFIED_NESTED_SUDO_COMMANDS: [&str; 3] = [
    "/usr/bin/sudo -S -p '[sudo] password for %u: ' -- /usr/bin/systemctl start algo-agent.service\n",
    "/usr/bin/sudo -S -p '[sudo] password for %u: ' -- /usr/bin/systemctl restart algo-agent.service\n",
    "/usr/bin/sudo -S -p '[sudo] password for %u: ' -- /usr/bin/systemctl enable --now algo-agent.service\n",
];

struct ActivePtyHandle {
    input_tx: mpsc::Sender<String>,
    close_tx: Option<oneshot::Sender<()>>,
    _channel_permit: Option<OwnedSemaphorePermit>,
    transfer_lock: Arc<Mutex<()>>,
    transfer_capture: Arc<StdMutex<Option<mpsc::Sender<PtyBackendOutput>>>>,
}

struct PtyTransferCaptureGuard {
    capture: Arc<StdMutex<Option<mpsc::Sender<PtyBackendOutput>>>>,
}

impl Drop for PtyTransferCaptureGuard {
    fn drop(&mut self) {
        match self.capture.lock() {
            Ok(mut capture) => *capture = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
    }
}

/// Connector-local manager for persistent PTY backend processes.
pub struct ConnectorPtyManager<B> {
    repositories: Repositories,
    backend: B,
    config: ConnectorPtyManagerConfig,
    credential_provider: Option<Arc<dyn SshCredentialProvider>>,
    active: Arc<Mutex<BTreeMap<PtySessionId, ActivePtyHandle>>>,
    capacity_wait_notified: Arc<Mutex<BTreeSet<PtySessionId>>>,
}

impl<B> ConnectorPtyManager<B>
where
    B: ManagedPtyBackend + 'static,
{
    /// Creates a PTY manager.
    pub fn new(repositories: Repositories, backend: B, config: ConnectorPtyManagerConfig) -> Self {
        Self {
            repositories,
            backend,
            config,
            credential_provider: None,
            active: Arc::new(Mutex::new(BTreeMap::new())),
            capacity_wait_notified: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Enables connector-local injection of stored sudo and SSH passwords for live PTY prompts.
    #[must_use]
    pub fn with_credential_provider(mut self, provider: Arc<dyn SshCredentialProvider>) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    /// Reconciles stale active PTY records left by an earlier connector process.
    ///
    /// A connector-local PTY handle cannot be reconstructed after process exit. Marking these
    /// records failed prevents a new shell from being mistaken for the original runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted PTY or workspace state cannot be updated.
    pub async fn reconcile_startup(&self) -> Result<u64, ConnectorPtyError> {
        let observed_at = now_utc();
        let sessions = self
            .repositories
            .pty_sessions
            .mark_active_backends_lost_for_connector(self.config.connector_id, observed_at)
            .await?;
        let mut reconciled = 0_u64;
        for session in sessions {
            self.mark_connection_channel_closed(session.session_id)
                .await?;
            let Some(workspace) = self
                .repositories
                .workspaces
                .get(session.workspace_id)
                .await?
            else {
                reconciled = reconciled.saturating_add(1);
                continue;
            };
            if workspace_keeps_pty_open(&workspace.state) {
                self.repositories
                    .workspaces
                    .update_state(session.workspace_id, WorkspaceState::Blocked, observed_at)
                    .await?;
                if let Some(agent_session_id) = workspace.agent_session_id {
                    shorten_host_write_leases(
                        &self.repositories,
                        workspace.host_id,
                        &pty_coordination_scopes(&session, &workspace),
                        agent_session_id,
                        observed_at,
                    )
                    .await?;
                }
                self.append_pty_system_output(
                    &session,
                    "connector runtime restarted; previous PTY backend continuity was lost; open a new PTY session",
                )
                .await?;
            } else {
                let closed = PtySessionSupervisor::default().close(session, None);
                self.repositories.pty_sessions.upsert(&closed).await?;
                self.append_pty_system_output(
                    &closed,
                    "PTY closed during connector restart because its Workspace had already reached a terminal state",
                )
                .await?;
            }
            reconciled = reconciled.saturating_add(1);
        }
        reconciled = reconciled.saturating_add(self.reconcile_terminal_pending_ptys().await?);
        Ok(reconciled)
    }

    async fn reconcile_terminal_pending_ptys(&self) -> Result<u64, ConnectorPtyError> {
        let closed = self
            .repositories
            .pty_sessions
            .close_pending_for_terminal_workspaces(self.config.connector_id, now_utc())
            .await?;
        for session in &closed {
            self.capacity_wait_notified
                .lock()
                .await
                .remove(&session.pty_session_id);
            self.append_pty_system_output(
                session,
                "PTY activation cancelled because its Workspace reached a terminal state before the remote shell started",
            )
            .await?;
        }
        Ok(u64::try_from(closed.len())?)
    }

    async fn note_pending_channel_capacity_wait(
        &self,
        session: &PtySession,
    ) -> Result<(), ConnectorPtyError> {
        let should_notify = self
            .capacity_wait_notified
            .lock()
            .await
            .insert(session.pty_session_id);
        self.repositories.pty_sessions.upsert(session).await?;
        if should_notify {
            self.append_pty_system_output(
                session,
                "PTY is queued locally because this access path has no free SSH channel; the remote menu has not started and no input can be sent. Keep this PTY, wait for channel capacity, and inspect the runtime snapshot instead of reconnecting.",
            )
            .await?;
        }
        Ok(())
    }

    async fn note_channel_capacity_waits(&self) -> Result<u64, ConnectorPtyError> {
        let sessions = self
            .repositories
            .pty_sessions
            .list_pending_waiting_for_channel(self.config.connector_id, 32)
            .await?;
        let count = u64::try_from(sessions.len())?;
        for session in sessions {
            self.note_pending_channel_capacity_wait(&session).await?;
        }
        Ok(count)
    }

    /// Opens a persistent PTY backend process for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when repository state, policy, or backend spawning fails.
    pub async fn open(
        &self,
        workspace_id: WorkspaceId,
        session_id: SessionId,
        cwd: Option<String>,
    ) -> Result<ConnectorPtyOpenOutcome, ConnectorPtyError> {
        let workspace = self
            .repositories
            .workspaces
            .get(workspace_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("workspace not found: {workspace_id}"))
            })?;
        if workspace.connector_id != self.config.connector_id {
            return Err(ConnectorPtyError::ConnectorMismatch);
        }
        let connection = self
            .repositories
            .connection_sessions
            .get(session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("connection session not found: {session_id}"))
            })?;
        let active_ptys = self
            .repositories
            .pty_sessions
            .count_active_for_host(workspace.host_id)
            .await?;
        let mut pty_session = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            active_ptys,
            PtySessionOpenCommand {
                session_id,
                cwd,
                coordination_scopes: None,
            },
        )?;
        let process = match self.spawn_backend_process(&pty_session).await {
            Ok(process) => process,
            Err(ConnectorPtyError::ChannelCapacityUnavailable) => {
                self.note_pending_channel_capacity_wait(&pty_session)
                    .await?;
                return Ok(ConnectorPtyOpenOutcome { pty_session });
            }
            Err(error) => {
                pty_session.backend_state = PtyBackendState::Failed;
                pty_session.last_activity_at = now_utc();
                self.repositories.pty_sessions.upsert(&pty_session).await?;
                self.record_activation_connection_failure(&connection, &error)
                    .await?;
                return Err(error);
            }
        };
        self.apply_backend_active(&mut pty_session, &process);
        self.capacity_wait_notified
            .lock()
            .await
            .remove(&pty_session.pty_session_id);
        self.persist_pty_transport_runtime(&connection, process.transport_telemetry.as_ref())
            .await;
        self.mark_connection_channel_open(connection).await?;
        self.repositories.pty_sessions.upsert(&pty_session).await?;
        self.register_active_process(&pty_session, process).await;
        Ok(ConnectorPtyOpenOutcome { pty_session })
    }

    /// Activates the oldest pending PTY owned by this connector.
    ///
    /// This lets agents observe the remote banner or interactive menu before sending input.
    ///
    /// # Errors
    ///
    /// Returns an error when pending state cannot be read or the backend cannot be activated.
    pub async fn activate_next_pending(&self) -> Result<Option<PtySessionId>, ConnectorPtyError> {
        let Some(session) = self
            .repositories
            .pty_sessions
            .next_pending_for_connector(self.config.connector_id)
            .await?
        else {
            self.note_channel_capacity_waits().await?;
            return Ok(None);
        };
        match self.activate_existing(session.pty_session_id).await {
            Ok(()) => Ok(Some(session.pty_session_id)),
            Err(ConnectorPtyError::ChannelCapacityUnavailable) => {
                tracing::debug!(
                    pty_session_id = %session.pty_session_id,
                    "pending PTY is waiting for access-path channel capacity"
                );
                Ok(None)
            }
            Err(error) => {
                let terminalized = self
                    .repositories
                    .pty_sessions
                    .get(session.pty_session_id)
                    .await?
                    .is_some_and(|pty| {
                        pty.backend_state == PtyBackendState::Failed && !pty.input_allowed
                    });
                if terminalized {
                    tracing::warn!(
                        pty_session_id = %session.pty_session_id,
                        %error,
                        "pending PTY activation failed and was terminalized"
                    );
                    Ok(Some(session.pty_session_id))
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Writes input to an active PTY backend process.
    ///
    /// # Errors
    ///
    /// Returns an error when input is invalid or the PTY is not active.
    pub async fn write_input(
        &self,
        pty_session_id: PtySessionId,
        input: String,
    ) -> Result<ConnectorPtyInputOutcome, ConnectorPtyError> {
        validate_pty_input(&input, self.config.max_input_bytes)?;
        let byte_len = input.len();
        let (input_tx, transfer_lock) = {
            let active = self.active.lock().await;
            active
                .get(&pty_session_id)
                .map(|handle| (handle.input_tx.clone(), Arc::clone(&handle.transfer_lock)))
                .ok_or(ConnectorPtyError::NotActive)?
        };
        let input_guard = transfer_lock.lock().await;
        input_tx
            .send(input)
            .await
            .map_err(|_| ConnectorPtyError::InputClosed)?;
        drop(input_guard);
        self.clear_interaction_after_input(pty_session_id).await;
        Ok(ConnectorPtyInputOutcome {
            pty_session_id,
            byte_len,
        })
    }

    async fn clear_interaction_after_input(&self, pty_session_id: PtySessionId) {
        let observed_at = now_utc();
        let session = match self.repositories.pty_sessions.get(pty_session_id).await {
            Ok(Some(session)) => session,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%pty_session_id, %error, "failed to load PTY after input delivery");
                return;
            }
        };
        if session.interaction.is_none() {
            return;
        }

        let workspace_id = session.workspace_id;
        let mut updated_session = session;
        updated_session.interaction = None;
        updated_session.last_activity_at = observed_at;
        if let Err(error) = self
            .repositories
            .pty_sessions
            .upsert(&updated_session)
            .await
        {
            tracing::warn!(%pty_session_id, %error, "failed to clear PTY interaction after accepted input");
            return;
        }

        match self.repositories.workspaces.get(workspace_id).await {
            Ok(Some(workspace)) if workspace.state == WorkspaceState::Blocked => {
                if let Err(error) = self
                    .repositories
                    .workspaces
                    .update_state(workspace_id, WorkspaceState::Working, observed_at)
                    .await
                {
                    tracing::warn!(%pty_session_id, %workspace_id, %error, "failed to resume workspace after accepted PTY input");
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%pty_session_id, %workspace_id, %error, "failed to load workspace after accepted PTY input");
            }
        }
    }

    /// Claims and delivers one queued PTY input event owned by this connector.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue cannot be claimed or updated.
    #[allow(clippy::too_many_lines)]
    pub async fn deliver_next_queued_input(
        &self,
        lease_seconds: u64,
        max_attempts: u32,
    ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, ConnectorPtyError> {
        let claimed_at = now_utc();
        let claim_token = Uuid::new_v4().to_string();
        let lease_expires_at = claimed_at + time::Duration::seconds(i64::try_from(lease_seconds)?);
        let Some(claimed) = self
            .repositories
            .pty_input_events
            .claim_next_for_connector(
                self.config.connector_id,
                &claim_token,
                claimed_at,
                lease_expires_at,
                max_attempts,
            )
            .await?
        else {
            return Ok(None);
        };

        let input_event_id = claimed.event.id;
        let pty_session_id = claimed.event.pty_session_id;
        let byte_len = usize::try_from(claimed.event.byte_len).unwrap_or(usize::MAX);
        if !self
            .acquire_pty_input_write_lease(&claimed.event, lease_seconds)
            .await?
        {
            if !self
                .repositories
                .pty_input_events
                .defer_claimed_for_write_lease(input_event_id, &claimed.claim_token)
                .await?
            {
                return Err(ConnectorPtyError::Backend(format!(
                    "lost PTY input claim for event {input_event_id}"
                )));
            }
            return Ok(Some(ConnectorPtyInputDeliveryOutcome {
                input_event_id,
                pty_session_id,
                state: PtyInputEventState::Queued,
                byte_len: 0,
                error: None,
            }));
        }
        let delivery = match claimed.event.payload_kind {
            PtyInputPayloadKind::Text => {
                self.deliver_input_text(pty_session_id, claimed.input_text)
                    .await
            }
            PtyInputPayloadKind::StoredSudoPassword => {
                self.deliver_stored_sudo_password(pty_session_id).await
            }
            PtyInputPayloadKind::StoredSshPassword => {
                self.deliver_stored_ssh_password(&claimed.event, &claimed.input_text)
                    .await
            }
            PtyInputPayloadKind::StoredTargetSudoPassword => {
                self.deliver_stored_target_sudo_password(&claimed.event, &claimed.input_text)
                    .await
            }
        };
        match delivery {
            Ok(_) => {
                let finished = self
                    .repositories
                    .pty_input_events
                    .finish_claimed(input_event_id, &claimed.claim_token, now_utc())
                    .await?;
                if !finished {
                    return Err(ConnectorPtyError::Backend(format!(
                        "lost PTY input claim for event {input_event_id}"
                    )));
                }
                Ok(Some(ConnectorPtyInputDeliveryOutcome {
                    input_event_id,
                    pty_session_id,
                    state: PtyInputEventState::Delivered,
                    byte_len,
                    error: None,
                }))
            }
            Err(error) => {
                let redacted_error = SecretRedactor::default().redact(&error.to_string());
                self.repositories
                    .pty_input_events
                    .fail_claimed(
                        input_event_id,
                        &claimed.claim_token,
                        now_utc(),
                        &redacted_error,
                    )
                    .await?;
                self.shorten_pty_input_write_lease(&claimed.event).await?;
                Ok(Some(ConnectorPtyInputDeliveryOutcome {
                    input_event_id,
                    pty_session_id,
                    state: PtyInputEventState::Failed,
                    byte_len,
                    error: Some(redacted_error),
                }))
            }
        }
    }

    async fn acquire_pty_input_write_lease(
        &self,
        event: &remote_hosts_domain::PtyInputEvent,
        lease_seconds: u64,
    ) -> Result<bool, ConnectorPtyError> {
        let Some(agent_session_id) = event.agent_session_id else {
            return Ok(true);
        };
        let observed_at = now_utc();
        let lease_seconds = i64::try_from(lease_seconds)?.max(PTY_WRITE_LEASE_SECONDS);
        let workspace = self
            .repositories
            .workspaces
            .get(event.workspace_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "workspace not found for PTY input: {}",
                    event.workspace_id
                ))
            })?;
        let pty = self
            .repositories
            .pty_sessions
            .get(event.pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "PTY session not found for input: {}",
                    event.pty_session_id
                ))
            })?;
        let coordination_scopes = pty_coordination_scopes(&pty, &workspace);
        let leases = coordination_scopes
            .iter()
            .map(|coordination_scope| HostWriteLease {
                host_id: event.host_id,
                coordination_scope: coordination_scope.clone(),
                holder_agent_session_id: agent_session_id,
                holder_workspace_id: event.workspace_id,
                acquired_at: observed_at,
                heartbeat_at: observed_at,
                expires_at: observed_at + time::Duration::seconds(lease_seconds),
            })
            .collect::<Vec<_>>();
        Ok(self
            .repositories
            .host_write_leases
            .try_acquire_many(&leases, observed_at)
            .await?
            .is_some())
    }

    async fn shorten_pty_input_write_lease(
        &self,
        event: &remote_hosts_domain::PtyInputEvent,
    ) -> Result<(), ConnectorPtyError> {
        let Some(agent_session_id) = event.agent_session_id else {
            return Ok(());
        };
        let workspace = self
            .repositories
            .workspaces
            .get(event.workspace_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "workspace not found for PTY input: {}",
                    event.workspace_id
                ))
            })?;
        let pty = self
            .repositories
            .pty_sessions
            .get(event.pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "PTY session not found for input: {}",
                    event.pty_session_id
                ))
            })?;
        shorten_host_write_leases(
            &self.repositories,
            event.host_id,
            &pty_coordination_scopes(&pty, &workspace),
            agent_session_id,
            now_utc(),
        )
        .await?;
        Ok(())
    }

    /// Closes an active PTY backend process and session record.
    ///
    /// # Errors
    ///
    /// Returns an error when the PTY does not exist or cannot be updated.
    pub async fn close(
        &self,
        pty_session_id: PtySessionId,
        last_exit_code: Option<i32>,
    ) -> Result<PtySession, ConnectorPtyError> {
        self.capacity_wait_notified
            .lock()
            .await
            .remove(&pty_session_id);
        if let Some(mut handle) = self.active.lock().await.remove(&pty_session_id)
            && let Some(close_tx) = handle.close_tx.take()
        {
            let _ = close_tx.send(());
        }
        let session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("pty session not found: {pty_session_id}"))
            })?;
        let connection_session_id = session.session_id;
        let channel_was_active = session.backend_state == PtyBackendState::Active;
        let workspace = self
            .repositories
            .workspaces
            .get(session.workspace_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("workspace not found: {}", session.workspace_id))
            })?;
        if workspace.connector_id != self.config.connector_id {
            return Err(ConnectorPtyError::ConnectorMismatch);
        }
        let closed = PtySessionSupervisor::default().close(session, last_exit_code);
        self.repositories.pty_sessions.upsert(&closed).await?;
        if channel_was_active {
            self.mark_connection_channel_closed(connection_session_id)
                .await?;
        }
        if let Some(agent_session_id) = workspace.agent_session_id {
            shorten_host_write_leases(
                &self.repositories,
                workspace.host_id,
                &pty_coordination_scopes(&closed, &workspace),
                agent_session_id,
                closed.last_activity_at,
            )
            .await?;
        }
        Ok(closed)
    }

    async fn reconcile_runtime_state(&self) -> Result<u64, ConnectorPtyError> {
        let pty_session_ids: Vec<_> = self.active.lock().await.keys().copied().collect();
        let mut reconciled = self.reconcile_terminal_pending_ptys().await?;
        for pty_session_id in pty_session_ids {
            let Some(session) = self.repositories.pty_sessions.get(pty_session_id).await? else {
                if let Some(mut handle) = self.active.lock().await.remove(&pty_session_id)
                    && let Some(close_tx) = handle.close_tx.take()
                {
                    let _ = close_tx.send(());
                }
                reconciled = reconciled.saturating_add(1);
                continue;
            };
            let workspace = self
                .repositories
                .workspaces
                .get(session.workspace_id)
                .await?;
            let workspace_keeps_pty = workspace
                .as_ref()
                .is_some_and(|workspace| workspace_keeps_pty_open(&workspace.state));
            let persisted_active = session.backend_state == PtyBackendState::Active
                && session.input_allowed
                && matches!(
                    session.state,
                    WorkspaceState::Idle | WorkspaceState::Working | WorkspaceState::Blocked
                )
                && workspace_keeps_pty;
            if persisted_active {
                continue;
            }
            let Some(mut handle) = self.active.lock().await.remove(&pty_session_id) else {
                continue;
            };
            if let Some(close_tx) = handle.close_tx.take() {
                let _ = close_tx.send(());
            }
            self.capacity_wait_notified
                .lock()
                .await
                .remove(&pty_session_id);
            if workspace
                .as_ref()
                .is_some_and(|workspace| !workspace_keeps_pty_open(&workspace.state))
            {
                let closed = PtySessionSupervisor::default().close(session.clone(), None);
                self.repositories.pty_sessions.upsert(&closed).await?;
                self.append_pty_system_output(
                    &closed,
                    "PTY closed because its Workspace reached a terminal state; its SSH channel was released",
                )
                .await?;
            }
            self.mark_connection_channel_closed(session.session_id)
                .await?;
            if let Some(workspace) = workspace
                && let Some(agent_session_id) = workspace.agent_session_id
            {
                shorten_host_write_leases(
                    &self.repositories,
                    workspace.host_id,
                    &pty_coordination_scopes(&session, &workspace),
                    agent_session_id,
                    now_utc(),
                )
                .await?;
            }
            reconciled = reconciled.saturating_add(1);
        }
        Ok(reconciled)
    }

    async fn reap_idle(
        &self,
        idle_ttl_seconds: u64,
        busy_ttl_seconds: u64,
    ) -> Result<u64, ConnectorPtyError> {
        let closed = self
            .repositories
            .pty_sessions
            .close_idle_for_connector(
                self.config.connector_id,
                now_utc(),
                idle_ttl_seconds,
                normalized_busy_ttl(idle_ttl_seconds, busy_ttl_seconds),
                100,
            )
            .await?;
        for session in &closed {
            self.capacity_wait_notified
                .lock()
                .await
                .remove(&session.pty_session_id);
            let handle = self.active.lock().await.remove(&session.pty_session_id);
            if let Some(mut handle) = handle {
                if let Some(close_tx) = handle.close_tx.take() {
                    let _ = close_tx.send(());
                }
                self.mark_connection_channel_closed(session.session_id)
                    .await?;
            }
            if let Some(workspace) = self
                .repositories
                .workspaces
                .get(session.workspace_id)
                .await?
                && let Some(agent_session_id) = workspace.agent_session_id
            {
                shorten_host_write_leases(
                    &self.repositories,
                    workspace.host_id,
                    &pty_coordination_scopes(session, &workspace),
                    agent_session_id,
                    session.last_activity_at,
                )
                .await?;
            }
            self.append_pty_system_output(
                session,
                "PTY closed automatically after its business-activity idle TTL elapsed; the SSH channel was released",
            )
            .await?;
        }
        Ok(u64::try_from(closed.len())?)
    }

    async fn deliver_input_text(
        &self,
        pty_session_id: PtySessionId,
        input: String,
    ) -> Result<ConnectorPtyInputOutcome, ConnectorPtyError> {
        match self.write_input(pty_session_id, input.clone()).await {
            Err(ConnectorPtyError::NotActive) => {
                self.activate_existing(pty_session_id).await?;
                self.write_input(pty_session_id, input).await
            }
            result => result,
        }
    }

    async fn deliver_stored_sudo_password(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<ConnectorPtyInputOutcome, ConnectorPtyError> {
        let provider = self
            .credential_provider
            .as_ref()
            .ok_or(ConnectorPtyError::StoredSudoPasswordUnavailable)?;
        let session = self.ensure_live_sudo_prompt(pty_session_id).await?;
        let connection = self
            .repositories
            .connection_sessions
            .get(session.session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "connection session not found: {}",
                    session.session_id
                ))
            })?;
        let password = provider
            .sudo_password_for(connection.access_path_id)
            .await
            .map_err(|_| ConnectorPtyError::StoredSudoPasswordUnavailable)?;

        // A prompt can disappear while the connector reads the vault. Recheck before writing so
        // this path never answers a later unrelated interactive request.
        self.ensure_live_sudo_prompt(pty_session_id).await?;
        let mut input = Zeroizing::new(password.expose_secret().to_owned());
        input.push('\n');
        self.write_input(pty_session_id, input.to_string()).await
    }

    async fn deliver_stored_ssh_password(
        &self,
        event: &PtyInputEvent,
        target_access_path_id: &str,
    ) -> Result<ConnectorPtyInputOutcome, ConnectorPtyError> {
        let provider = self
            .credential_provider
            .as_ref()
            .ok_or(ConnectorPtyError::StoredSshPasswordUnavailable)?;
        let target_access_path_id = target_access_path_id
            .parse::<AccessPathId>()
            .map_err(|_| ConnectorPtyError::StoredSshPasswordUnavailable)?;
        let session = self
            .ensure_live_password_prompt(event.pty_session_id)
            .await?;
        let target = self
            .ensure_enabled_ssh_target(target_access_path_id)
            .await?;
        self.ensure_verified_nested_ssh_command(event, &session, &target)
            .await?;
        let password = provider
            .ssh_password_for(target_access_path_id)
            .await
            .map_err(|_| ConnectorPtyError::StoredSshPasswordUnavailable)?;

        // The prompt or target route can change while the vault is being read. Recheck both
        // immediately before writing so the credential cannot answer a later unrelated prompt.
        let session = self
            .ensure_live_password_prompt(event.pty_session_id)
            .await?;
        let target = self
            .ensure_enabled_ssh_target(target_access_path_id)
            .await?;
        self.ensure_verified_nested_ssh_command(event, &session, &target)
            .await?;
        let mut input = Zeroizing::new(password.expose_secret().to_owned());
        input.push('\n');
        self.write_input(event.pty_session_id, input.to_string())
            .await
    }

    async fn deliver_stored_target_sudo_password(
        &self,
        event: &PtyInputEvent,
        target_access_path_id: &str,
    ) -> Result<ConnectorPtyInputOutcome, ConnectorPtyError> {
        let provider = self
            .credential_provider
            .as_ref()
            .ok_or(ConnectorPtyError::StoredTargetSudoPasswordUnavailable)?;
        let target_access_path_id = target_access_path_id
            .parse::<AccessPathId>()
            .map_err(|_| ConnectorPtyError::StoredTargetSudoPasswordUnavailable)?;
        let session = self
            .ensure_live_target_sudo_prompt(event.pty_session_id)
            .await?;
        self.ensure_enabled_ssh_target(target_access_path_id)
            .await?;
        self.ensure_verified_nested_sudo_command(event, &session)
            .await?;
        let password = provider
            .sudo_password_for(target_access_path_id)
            .await
            .map_err(|_| ConnectorPtyError::StoredTargetSudoPasswordUnavailable)?;

        let session = self
            .ensure_live_target_sudo_prompt(event.pty_session_id)
            .await?;
        self.ensure_enabled_ssh_target(target_access_path_id)
            .await?;
        self.ensure_verified_nested_sudo_command(event, &session)
            .await?;
        let mut input = Zeroizing::new(password.expose_secret().to_owned());
        input.push('\n');
        self.write_input(event.pty_session_id, input.to_string())
            .await
    }

    async fn ensure_enabled_ssh_target(
        &self,
        target_access_path_id: AccessPathId,
    ) -> Result<remote_hosts_domain::AccessPath, ConnectorPtyError> {
        self.repositories
            .access_paths
            .get(target_access_path_id)
            .await?
            .filter(|path| path.enabled && path.protocol == remote_hosts_domain::Protocol::Ssh)
            .ok_or(ConnectorPtyError::StoredSshPasswordUnavailable)
    }

    async fn ensure_verified_nested_ssh_command(
        &self,
        event: &PtyInputEvent,
        session: &PtySession,
        target: &remote_hosts_domain::AccessPath,
    ) -> Result<(), ConnectorPtyError> {
        let preceding = self
            .repositories
            .pty_input_events
            .get_preceding_delivered_input(event.pty_session_id, event.sequence)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::StoredSshCommandUnverified(
                    "no immediately preceding delivered text input exists".to_owned(),
                )
            })?;
        let delivered_at = preceding.delivered_at.ok_or_else(|| {
            ConnectorPtyError::StoredSshCommandUnverified(
                "the preceding input has no delivery timestamp".to_owned(),
            )
        })?;
        let prompt_delay = session
            .interaction
            .as_ref()
            .map(|interaction| interaction.observed_at - delivered_at)
            .ok_or_else(|| {
                ConnectorPtyError::StoredSshCommandUnverified(
                    "the live password interaction has no observation timestamp".to_owned(),
                )
            })?;
        let payload_kind_matches = preceding.payload_kind == PtyInputPayloadKind::Text;
        let agent_session_matches = preceding.agent_session_id == event.agent_session_id;
        let expected_command = verified_nested_ssh_command(target)?;
        let expected_fingerprint = format!("{:x}", Sha256::digest(expected_command.as_bytes()));
        let command_matches =
            preceding.input_fingerprint.as_deref() == Some(expected_fingerprint.as_str());
        let prompt_delay_matches =
            !prompt_delay.is_negative() && prompt_delay <= time::Duration::minutes(2);
        if !(payload_kind_matches
            && agent_session_matches
            && command_matches
            && prompt_delay_matches)
        {
            tracing::warn!(
                pty_session_id = %event.pty_session_id,
                event_sequence = event.sequence,
                payload_kind_matches,
                agent_session_matches,
                command_matches,
                prompt_delay_matches,
                prompt_delay_ms = prompt_delay.whole_milliseconds(),
                "rejected stored SSH password injection because its nested command binding did not verify"
            );
            return Err(ConnectorPtyError::StoredSshCommandUnverified(format!(
                "binding mismatch (payload_kind_matches={payload_kind_matches}, agent_session_matches={agent_session_matches}, command_matches={command_matches}, prompt_delay_matches={prompt_delay_matches}, input_sha256={}, expected_sha256={:x})",
                preceding.input_fingerprint.as_deref().unwrap_or("missing"),
                Sha256::digest(expected_command.as_bytes())
            )));
        }
        Ok(())
    }

    async fn ensure_verified_nested_sudo_command(
        &self,
        event: &PtyInputEvent,
        session: &PtySession,
    ) -> Result<(), ConnectorPtyError> {
        let preceding = self
            .repositories
            .pty_input_events
            .get_preceding_delivered_input(event.pty_session_id, event.sequence)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::StoredTargetSudoCommandUnverified(
                    "no immediately preceding delivered text input exists".to_owned(),
                )
            })?;
        let delivered_at = preceding.delivered_at.ok_or_else(|| {
            ConnectorPtyError::StoredTargetSudoCommandUnverified(
                "the preceding input has no delivery timestamp".to_owned(),
            )
        })?;
        let prompt_delay = session
            .interaction
            .as_ref()
            .map(|interaction| interaction.observed_at - delivered_at)
            .ok_or_else(|| {
                ConnectorPtyError::StoredTargetSudoCommandUnverified(
                    "the live sudo interaction has no observation timestamp".to_owned(),
                )
            })?;
        let payload_kind_matches = preceding.payload_kind == PtyInputPayloadKind::Text;
        let agent_session_matches = preceding.agent_session_id == event.agent_session_id;
        let command_matches = preceding
            .input_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                VERIFIED_NESTED_SUDO_COMMANDS.iter().any(|command| {
                    fingerprint == format!("{:x}", Sha256::digest(command.as_bytes()))
                })
            });
        let prompt_delay_matches =
            !prompt_delay.is_negative() && prompt_delay <= time::Duration::minutes(2);
        if !(payload_kind_matches
            && agent_session_matches
            && command_matches
            && prompt_delay_matches)
        {
            tracing::warn!(
                pty_session_id = %event.pty_session_id,
                event_sequence = event.sequence,
                payload_kind_matches,
                agent_session_matches,
                command_matches,
                prompt_delay_matches,
                prompt_delay_ms = prompt_delay.whole_milliseconds(),
                "rejected target stored sudo password injection because its nested command binding did not verify"
            );
            return Err(ConnectorPtyError::StoredTargetSudoCommandUnverified(
                format!(
                    "binding mismatch (payload_kind_matches={payload_kind_matches}, agent_session_matches={agent_session_matches}, command_matches={command_matches}, prompt_delay_matches={prompt_delay_matches}, input_sha256={})",
                    preceding.input_fingerprint.as_deref().unwrap_or("missing")
                ),
            ));
        }
        Ok(())
    }

    async fn ensure_live_password_prompt(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<PtySession, ConnectorPtyError> {
        let session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("pty session not found: {pty_session_id}"))
            })?;
        if session.backend_state != PtyBackendState::Active
            || !session.input_allowed
            || !matches!(
                session
                    .interaction
                    .as_ref()
                    .map(|interaction| &interaction.kind),
                Some(remote_hosts_domain::PtyInteractionKind::Password)
            )
        {
            return Err(ConnectorPtyError::StoredSshPromptUnavailable);
        }
        Ok(session)
    }

    async fn ensure_live_target_sudo_prompt(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<PtySession, ConnectorPtyError> {
        let session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("pty session not found: {pty_session_id}"))
            })?;
        if session.backend_state != PtyBackendState::Active
            || !session.input_allowed
            || !matches!(
                session
                    .interaction
                    .as_ref()
                    .map(|interaction| &interaction.kind),
                Some(remote_hosts_domain::PtyInteractionKind::SudoPassword)
            )
        {
            return Err(ConnectorPtyError::StoredTargetSudoPromptUnavailable);
        }
        Ok(session)
    }

    async fn ensure_live_sudo_prompt(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<PtySession, ConnectorPtyError> {
        let session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("pty session not found: {pty_session_id}"))
            })?;
        if session.backend_state != PtyBackendState::Active
            || !session.input_allowed
            || !matches!(
                session
                    .interaction
                    .as_ref()
                    .map(|interaction| &interaction.kind),
                Some(remote_hosts_domain::PtyInteractionKind::SudoPassword)
            )
        {
            return Err(ConnectorPtyError::StoredSudoPromptUnavailable);
        }
        Ok(session)
    }

    #[allow(clippy::too_many_lines)]
    async fn activate_existing(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<(), ConnectorPtyError> {
        if self.active.lock().await.contains_key(&pty_session_id) {
            return Ok(());
        }
        let pty_session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("pty session not found: {pty_session_id}"))
            })?;
        if pty_session.backend_state == PtyBackendState::Active {
            self.mark_runtime_lost(pty_session).await?;
            return Err(ConnectorPtyError::RuntimeContinuityLost);
        }
        if pty_session.backend_state != PtyBackendState::Pending {
            return Err(ConnectorPtyError::RuntimeContinuityLost);
        }
        if !pty_session.input_allowed
            || !matches!(
                pty_session.state,
                WorkspaceState::Idle | WorkspaceState::Working
            )
        {
            return Err(ConnectorPtyError::InputNotAllowed);
        }
        let Some(workspace) = self
            .repositories
            .workspaces
            .get(pty_session.workspace_id)
            .await?
        else {
            let error = ConnectorPtyError::Backend(format!(
                "workspace not found: {}",
                pty_session.workspace_id
            ));
            self.mark_activation_failed(pty_session, &error.to_string(), false)
                .await?;
            return Err(error);
        };
        if workspace.connector_id != self.config.connector_id {
            return Err(ConnectorPtyError::ConnectorMismatch);
        }
        if !matches!(
            workspace.state,
            WorkspaceState::Idle | WorkspaceState::Working
        ) {
            let error = ConnectorPtyError::Backend(format!(
                "pty backing workspace is not activatable: {:?}",
                workspace.state
            ));
            self.mark_activation_failed(pty_session, &error.to_string(), false)
                .await?;
            return Err(error);
        }
        let Some(connection) = self
            .repositories
            .connection_sessions
            .get(pty_session.session_id)
            .await?
        else {
            let error = ConnectorPtyError::Backend(format!(
                "connection session not found: {}",
                pty_session.session_id
            ));
            self.mark_activation_failed(pty_session, &error.to_string(), true)
                .await?;
            return Err(error);
        };
        if !connection_is_usable_for_workspace(&connection, &workspace) {
            let error = ConnectorPtyError::Backend(
                "pty backing connection is not usable for this workspace".to_owned(),
            );
            self.mark_activation_failed(pty_session, &error.to_string(), true)
                .await?;
            return Err(error);
        }

        let mut active_session = pty_session;
        let process = match self.spawn_backend_process(&active_session).await {
            Ok(process) => process,
            Err(ConnectorPtyError::ChannelCapacityUnavailable) => {
                self.note_pending_channel_capacity_wait(&active_session)
                    .await?;
                return Err(ConnectorPtyError::ChannelCapacityUnavailable);
            }
            Err(error) => {
                self.mark_activation_failed(active_session, &error.to_string(), true)
                    .await?;
                self.record_activation_connection_failure(&connection, &error)
                    .await?;
                return Err(error);
            }
        };
        self.apply_backend_active(&mut active_session, &process);
        self.capacity_wait_notified
            .lock()
            .await
            .remove(&active_session.pty_session_id);
        self.persist_pty_transport_runtime(&connection, process.transport_telemetry.as_ref())
            .await;
        self.mark_connection_channel_open(connection).await?;
        self.repositories
            .pty_sessions
            .upsert(&active_session)
            .await?;
        self.register_active_process(&active_session, process).await;
        Ok(())
    }

    fn apply_backend_active(&self, pty_session: &mut PtySession, process: &ManagedPtyProcess) {
        pty_session.backend_state = PtyBackendState::Active;
        pty_session.backend_capabilities = self.backend.capabilities();
        pty_session
            .transport_evidence
            .clone_from(&process.transport_evidence);
        pty_session.last_activity_at = now_utc();
    }

    async fn persist_pty_transport_runtime(
        &self,
        connection: &ConnectionSession,
        telemetry: Option<&SshTransportTelemetry>,
    ) {
        let Some(telemetry) = telemetry else {
            return;
        };
        let runtime = SshTransportRuntime {
            access_path_id: connection.access_path_id,
            connector_id: connection.connector_id,
            telemetry: telemetry.clone(),
            updated_at: now_utc(),
        };
        if let Err(error) = self
            .repositories
            .ssh_transport_runtimes
            .upsert(&runtime)
            .await
        {
            tracing::warn!(
                access_path_id = %connection.access_path_id,
                session_id = %connection.session_id,
                %error,
                "failed to persist PTY SSH transport runtime telemetry"
            );
        }
    }

    async fn spawn_backend_process(
        &self,
        pty_session: &PtySession,
    ) -> Result<ManagedPtyProcess, ConnectorPtyError> {
        let workspace = self
            .repositories
            .workspaces
            .get(pty_session.workspace_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "workspace not found: {}",
                    pty_session.workspace_id
                ))
            })?;
        let access_path = self
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "access path not found: {}",
                    workspace.access_path_id
                ))
            })?;
        let host = self
            .repositories
            .hosts
            .get(workspace.host_id)
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!("host not found: {}", workspace.host_id))
            })?;
        self.backend
            .spawn(PtyBackendSpawnRequest {
                pty_session_id: pty_session.pty_session_id,
                workspace_id: pty_session.workspace_id,
                session_id: pty_session.session_id,
                cwd: initial_pty_cwd(
                    pty_session.cwd.clone(),
                    &host.kind,
                    access_path.requires_tty,
                ),
            })
            .await
    }

    async fn register_active_process(&self, pty_session: &PtySession, process: ManagedPtyProcess) {
        let ManagedPtyProcess {
            input_tx,
            output_rx,
            close_tx,
            transport_telemetry: _,
            transport_evidence: _,
            channel_permit,
        } = process;
        let transfer_capture = Arc::new(StdMutex::new(None));
        self.active.lock().await.insert(
            pty_session.pty_session_id,
            ActivePtyHandle {
                input_tx,
                close_tx: Some(close_tx),
                _channel_permit: channel_permit,
                transfer_lock: Arc::new(Mutex::new(())),
                transfer_capture: Arc::clone(&transfer_capture),
            },
        );
        self.spawn_output_writer(pty_session, output_rx, transfer_capture);
    }

    #[allow(clippy::too_many_lines)]
    fn spawn_output_writer(
        &self,
        pty_session: &PtySession,
        mut output_rx: mpsc::Receiver<PtyBackendOutput>,
        transfer_capture: Arc<StdMutex<Option<mpsc::Sender<PtyBackendOutput>>>>,
    ) {
        let repositories = self.repositories.clone();
        let output_limit_bytes = self.config.output_limit_bytes;
        let pty_session_id = pty_session.pty_session_id;
        let workspace_id = pty_session.workspace_id;
        let active = Arc::clone(&self.active);
        tokio::spawn(async move {
            let redactor = SecretRedactor::default();
            let lease_owner = pty_lease_owner(&repositories, pty_session_id, workspace_id).await;
            let mut sequence = repositories
                .pty_output_chunks
                .next_sequence(pty_session_id)
                .await
                .unwrap_or(0);
            let mut pending = Vec::new();
            let mut pending_bytes = 0_usize;
            let mut recent_redacted_tail = String::new();
            let mut flush_interval = tokio::time::interval_at(
                Instant::now() + PTY_OUTPUT_BATCH_MAX_DELAY,
                PTY_OUTPUT_BATCH_MAX_DELAY,
            );
            flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let output = tokio::select! {
                    output = output_rx.recv() => output,
                    _ = flush_interval.tick(), if !pending.is_empty() => {
                        if flush_pty_output_batch(&repositories, &mut pending).await.is_err() {
                            break;
                        }
                        pending_bytes = 0;
                        continue;
                    }
                };
                let Some(output) = output else {
                    break;
                };
                let observed_at = now_utc();
                record_pty_output_activity(
                    &repositories,
                    pty_session_id,
                    workspace_id,
                    lease_owner.clone(),
                    observed_at,
                )
                .await;
                let capture_tx = match transfer_capture.lock() {
                    Ok(capture) => capture.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };
                if let Some(capture_tx) = capture_tx {
                    if flush_pty_output_batch(&repositories, &mut pending)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    pending_bytes = 0;
                    let _ = capture_tx.send(output).await;
                    continue;
                }
                let (redacted_text, truncated) =
                    redact_and_truncate(&redactor, &output.text, output_limit_bytes);
                if redacted_text.is_empty() {
                    continue;
                }
                append_pty_interaction_tail(&mut recent_redacted_tail, &redacted_text);
                if let Some(interaction) =
                    detect_pty_interaction(&recent_redacted_tail, observed_at)
                {
                    mark_pty_interaction_blocked(
                        &repositories,
                        pty_session_id,
                        workspace_id,
                        interaction,
                    )
                    .await;
                }
                pending_bytes = pending_bytes.saturating_add(redacted_text.len());
                pending.push(PtyOutputChunk {
                    id: PtyOutputChunkId::new(),
                    pty_session_id,
                    workspace_id,
                    stream: output.stream,
                    sequence,
                    byte_len: u64::try_from(redacted_text.len()).unwrap_or(u64::MAX),
                    redacted_text,
                    truncated: output.truncated || truncated,
                    created_at: observed_at,
                });
                sequence = sequence.saturating_add(1);
                if pending_bytes >= PTY_OUTPUT_BATCH_TARGET_BYTES {
                    if flush_pty_output_batch(&repositories, &mut pending)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    pending_bytes = 0;
                }
            }
            if flush_pty_output_batch(&repositories, &mut pending)
                .await
                .is_err()
            {
                tracing::warn!(
                    %pty_session_id,
                    "failed to persist the final compressed PTY output segment"
                );
            }
            let owns_channel_cleanup = active.lock().await.remove(&pty_session_id).is_some();
            if !owns_channel_cleanup {
                return;
            }
            let Ok(Some(mut session)) = repositories.pty_sessions.get(pty_session_id).await else {
                return;
            };
            if session.backend_state == PtyBackendState::Active {
                session.state = WorkspaceState::Done;
                session.input_allowed = false;
                session.foreground_process = None;
                session.backend_state = PtyBackendState::Closed;
                session.interaction = None;
                session.last_activity_at = now_utc();
                if repositories.pty_sessions.upsert(&session).await.is_err() {
                    return;
                }
                if let Ok(Some(workspace)) = repositories.workspaces.get(workspace_id).await
                    && !matches!(
                        workspace.state,
                        WorkspaceState::Closed | WorkspaceState::Failed | WorkspaceState::Throttled
                    )
                {
                    let _ = repositories
                        .workspaces
                        .update_state(workspace_id, WorkspaceState::Done, session.last_activity_at)
                        .await;
                }
            }
            let _ = repositories
                .connection_sessions
                .close_channel(session.session_id, session.last_activity_at)
                .await;
            if let Some((host_id, coordination_scopes, agent_session_id)) = lease_owner
                && let Err(error) = shorten_host_write_leases(
                    &repositories,
                    host_id,
                    &coordination_scopes,
                    agent_session_id,
                    session.last_activity_at,
                )
                .await
            {
                tracing::warn!(
                    %pty_session_id,
                    %workspace_id,
                    %error,
                    "failed to shorten scoped host write lease after PTY backend exit"
                );
            }
        });
    }

    async fn mark_runtime_lost(&self, mut session: PtySession) -> Result<(), ConnectorPtyError> {
        let observed_at = now_utc();
        session.state = WorkspaceState::Blocked;
        session.input_allowed = false;
        session.foreground_process = None;
        session.backend_state = PtyBackendState::Failed;
        session.interaction = None;
        session.last_activity_at = observed_at;
        self.repositories.pty_sessions.upsert(&session).await?;
        self.mark_connection_channel_closed(session.session_id)
            .await?;
        self.repositories
            .workspaces
            .update_state(session.workspace_id, WorkspaceState::Blocked, observed_at)
            .await?;
        if let Some(workspace) = self
            .repositories
            .workspaces
            .get(session.workspace_id)
            .await?
            && let Some(agent_session_id) = workspace.agent_session_id
        {
            shorten_host_write_leases(
                &self.repositories,
                workspace.host_id,
                &pty_coordination_scopes(&session, &workspace),
                agent_session_id,
                observed_at,
            )
            .await?;
        }
        self.append_pty_system_output(
            &session,
            "PTY backend handle is missing; runtime continuity was lost; open a new PTY session",
        )
        .await
    }

    async fn mark_activation_failed(
        &self,
        mut session: PtySession,
        reason: &str,
        block_workspace: bool,
    ) -> Result<(), ConnectorPtyError> {
        let observed_at = now_utc();
        session.state = WorkspaceState::Blocked;
        session.input_allowed = false;
        session.foreground_process = None;
        session.backend_state = PtyBackendState::Failed;
        session.interaction = None;
        session.last_activity_at = observed_at;
        self.repositories.pty_sessions.upsert(&session).await?;
        if block_workspace
            && let Some(workspace) = self
                .repositories
                .workspaces
                .get(session.workspace_id)
                .await?
            && matches!(
                workspace.state,
                WorkspaceState::Idle | WorkspaceState::Working
            )
        {
            self.repositories
                .workspaces
                .update_state(session.workspace_id, WorkspaceState::Blocked, observed_at)
                .await?;
        }
        let reason = SecretRedactor::default().redact(reason);
        self.append_pty_system_output(
            &session,
            &format!(
                "PTY activation stopped: {reason}; automatic retry disabled; inspect the runtime snapshot and open a new PTY after recovering the workspace connection"
            ),
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn record_activation_connection_failure(
        &self,
        connection: &ConnectionSession,
        error: &ConnectorPtyError,
    ) -> Result<(), ConnectorPtyError> {
        if matches!(
            error,
            ConnectorPtyError::StoredSudoPromptUnavailable
                | ConnectorPtyError::StoredSudoPasswordUnavailable
                | ConnectorPtyError::StoredSshPromptUnavailable
                | ConnectorPtyError::StoredSshPasswordUnavailable
                | ConnectorPtyError::StoredSshCommandUnverified(_)
                | ConnectorPtyError::StoredTargetSudoPromptUnavailable
                | ConnectorPtyError::StoredTargetSudoPasswordUnavailable
                | ConnectorPtyError::StoredTargetSudoCommandUnverified(_)
        ) {
            return Ok(());
        }
        let observed_at = now_utc();
        let message = SecretRedactor::default().redact(&error.to_string());
        let local_handshake_budget = matches!(
            error,
            ConnectorPtyError::Transport(TransportError::LocalHandshakeBudgetExhausted { .. })
        );
        let (mut state, mut reason_code, mut retry_after_seconds) = match error {
            ConnectorPtyError::Transport(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds,
            }) => (
                EntityState::Throttled,
                StateReasonCode::LocalHandshakeBudgetExhausted,
                Some(*retry_after_seconds),
            ),
            ConnectorPtyError::Transport(TransportError::PolicyDenied(_)) => (
                EntityState::RateLimited,
                StateReasonCode::TargetSshdRateLimited,
                Some(60),
            ),
            ConnectorPtyError::Transport(
                TransportError::Backend(_) | TransportError::FileTransfer(_),
            )
            | ConnectorPtyError::Backend(_) => classify_connection_failure(&message),
            ConnectorPtyError::Transport(TransportError::Timeout)
            | ConnectorPtyError::Database(_)
            | ConnectorPtyError::Supervisor(_)
            | ConnectorPtyError::ChannelCapacityUnavailable
            | ConnectorPtyError::ConnectorMismatch
            | ConnectorPtyError::NotActive
            | ConnectorPtyError::RuntimeContinuityLost
            | ConnectorPtyError::InputNotAllowed
            | ConnectorPtyError::InvalidInput(_)
            | ConnectorPtyError::InputClosed
            | ConnectorPtyError::StoredSudoPromptUnavailable
            | ConnectorPtyError::StoredSudoPasswordUnavailable
            | ConnectorPtyError::StoredSshPromptUnavailable
            | ConnectorPtyError::StoredSshPasswordUnavailable
            | ConnectorPtyError::StoredSshCommandUnverified(_)
            | ConnectorPtyError::StoredTargetSudoPromptUnavailable
            | ConnectorPtyError::StoredTargetSudoPasswordUnavailable
            | ConnectorPtyError::StoredTargetSudoCommandUnverified(_)
            | ConnectorPtyError::Int(_) => (
                EntityState::Degraded,
                StateReasonCode::SshHandshakeFailed,
                Some(30),
            ),
        };
        let circuit_breaker_eligible = !local_handshake_budget
            && !matches!(state, EntityState::AuthFailed | EntityState::HostKeyChanged);
        let connection = self
            .repositories
            .connection_sessions
            .record_failure(
                connection.session_id,
                observed_at,
                state.clone(),
                &message,
                !local_handshake_budget,
                false,
                circuit_breaker_eligible,
                3,
            )
            .await?
            .ok_or_else(|| {
                ConnectorPtyError::Backend(format!(
                    "connection session not found: {}",
                    connection.session_id
                ))
            })?;
        state = connection.state.clone();
        if state == EntityState::CircuitOpen {
            reason_code = StateReasonCode::CircuitOpen;
            retry_after_seconds = Some(300);
        }
        self.repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: connection.access_path_id,
                state,
                last_checked_at: Some(observed_at),
                latency_ms: None,
                failure_count: connection.failure_count,
                last_error_code: Some(reason_code),
                next_retry_at: retry_after_seconds.map(|seconds| {
                    observed_at
                        + time::Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
                }),
            })
            .await?;
        Ok(())
    }

    async fn mark_connection_channel_open(
        &self,
        mut connection: ConnectionSession,
    ) -> Result<(), ConnectorPtyError> {
        let access_path_id = connection.access_path_id;
        let observed_at = now_utc();
        let reused = matches!(
            connection.state,
            EntityState::Connected | EntityState::Healthy
        );
        connection.state = EntityState::Connected;
        connection.last_used_at = observed_at;
        self.repositories
            .connection_sessions
            .open_channel(&connection, reused, true)
            .await?;
        self.repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id,
                state: EntityState::Connected,
                last_checked_at: Some(observed_at),
                latency_ms: None,
                failure_count: 0,
                last_error_code: None,
                next_retry_at: None,
            })
            .await?;
        Ok(())
    }

    async fn mark_connection_channel_closed(
        &self,
        session_id: SessionId,
    ) -> Result<(), ConnectorPtyError> {
        self.repositories
            .connection_sessions
            .close_channel(session_id, now_utc())
            .await?;
        Ok(())
    }

    async fn append_pty_system_output(
        &self,
        session: &PtySession,
        message: &str,
    ) -> Result<(), ConnectorPtyError> {
        let sequence = self
            .repositories
            .pty_output_chunks
            .next_sequence(session.pty_session_id)
            .await?;
        self.repositories
            .pty_output_chunks
            .insert(&PtyOutputChunk {
                id: PtyOutputChunkId::new(),
                pty_session_id: session.pty_session_id,
                workspace_id: session.workspace_id,
                stream: OutputStream::System,
                sequence,
                redacted_text: message.to_owned(),
                byte_len: u64::try_from(message.len())?,
                truncated: false,
                created_at: now_utc(),
            })
            .await?;
        Ok(())
    }
}

fn workspace_keeps_pty_open(state: &WorkspaceState) -> bool {
    matches!(
        state,
        WorkspaceState::Idle | WorkspaceState::Working | WorkspaceState::Blocked
    )
}

fn append_pty_interaction_tail(tail: &mut String, text: &str) {
    tail.push_str(text);
    if tail.len() <= PTY_INTERACTION_TAIL_BYTES {
        return;
    }

    let bytes_to_drop = tail.len().saturating_sub(PTY_INTERACTION_TAIL_BYTES);
    let start = tail
        .char_indices()
        .find_map(|(index, _)| (index >= bytes_to_drop).then_some(index))
        .unwrap_or(tail.len());
    tail.drain(..start);
}

async fn mark_pty_interaction_blocked(
    repositories: &Repositories,
    pty_session_id: PtySessionId,
    workspace_id: WorkspaceId,
    interaction: remote_hosts_domain::PtyInteraction,
) {
    let mut session = match repositories.pty_sessions.get(pty_session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%pty_session_id, %error, "failed to load PTY for interaction detection");
            return;
        }
    };
    if session
        .interaction
        .as_ref()
        .is_some_and(|existing| existing.kind == interaction.kind)
    {
        return;
    }

    let observed_at = interaction.observed_at;
    match repositories.workspaces.get(workspace_id).await {
        Ok(Some(workspace))
            if matches!(
                workspace.state,
                WorkspaceState::Idle | WorkspaceState::Working
            ) =>
        {
            if let Err(error) = repositories
                .workspaces
                .update_state(workspace_id, WorkspaceState::Blocked, observed_at)
                .await
            {
                tracing::warn!(%pty_session_id, %workspace_id, %error, "failed to mark workspace blocked for PTY interaction");
                return;
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%pty_session_id, %workspace_id, %error, "failed to load workspace for PTY interaction");
            return;
        }
    }

    session.interaction = Some(interaction);
    session.last_activity_at = observed_at;
    if let Err(error) = repositories.pty_sessions.upsert(&session).await {
        tracing::warn!(%pty_session_id, %error, "failed to persist detected PTY interaction");
    }
}

async fn flush_pty_output_batch(
    repositories: &Repositories,
    pending: &mut Vec<PtyOutputChunk>,
) -> Result<(), DbError> {
    if pending.is_empty() {
        return Ok(());
    }
    repositories.pty_output_chunks.insert_batch(pending).await?;
    pending.clear();
    Ok(())
}

#[async_trait]
impl<B> QueuedPtyInputPump for ConnectorPtyManager<B>
where
    B: ManagedPtyBackend + 'static,
{
    async fn reconcile_startup(&self) -> Result<u64, ConnectorPtyError> {
        ConnectorPtyManager::reconcile_startup(self).await
    }

    async fn reconcile_runtime_state(&self) -> Result<u64, ConnectorPtyError> {
        ConnectorPtyManager::reconcile_runtime_state(self).await
    }

    async fn reap_idle(
        &self,
        idle_ttl_seconds: u64,
        busy_ttl_seconds: u64,
    ) -> Result<u64, ConnectorPtyError> {
        ConnectorPtyManager::reap_idle(self, idle_ttl_seconds, busy_ttl_seconds).await
    }

    async fn activate_next(&self) -> Result<Option<PtySessionId>, ConnectorPtyError> {
        self.activate_next_pending().await
    }

    async fn deliver_next(
        &self,
    ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, ConnectorPtyError> {
        self.deliver_next_queued_input(
            self.config.input_lease_seconds,
            self.config.input_max_attempts,
        )
        .await
    }
}

#[derive(Clone)]
struct InteractivePtyTransferHandle {
    input_tx: mpsc::Sender<String>,
    transfer_lock: Arc<Mutex<()>>,
    transfer_capture: Arc<StdMutex<Option<mpsc::Sender<PtyBackendOutput>>>>,
}

enum InteractivePtyUploadPreparation {
    Complete(SftpResult),
    Pending {
        local: tokio::fs::File,
        hasher: Sha256,
        resume_bytes: u64,
    },
}

struct InteractivePtyDownloadChunk {
    index: u64,
    offset: u64,
    payload: Vec<u8>,
    sha256: String,
}

impl<B> ConnectorPtyManager<B>
where
    B: ManagedPtyBackend + 'static,
{
    async fn interactive_transfer_handle(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<InteractivePtyTransferHandle, TransportError> {
        let sessions = self
            .repositories
            .pty_sessions
            .list_for_workspace(workspace_id)
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?;
        let active = self.active.lock().await;
        sessions
            .into_iter()
            .filter(|session| {
                session.backend_state == PtyBackendState::Active && session.input_allowed
            })
            .find_map(|session| {
                active
                    .get(&session.pty_session_id)
                    .map(|handle| InteractivePtyTransferHandle {
                        input_tx: handle.input_tx.clone(),
                        transfer_lock: Arc::clone(&handle.transfer_lock),
                        transfer_capture: Arc::clone(&handle.transfer_capture),
                    })
            })
            .ok_or_else(|| {
                TransportError::FileTransfer(
                    "interactive bastion transfer requires an active PTY in the same workspace"
                        .to_owned(),
                )
            })
    }

    async fn upload_through_interactive_pty(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
    ) -> Result<SftpResult, TransportError> {
        let spec = &request.spec;
        let (local_size, local_sha256) =
            hash_local_source(Path::new(&spec.local_path), spec.max_size_bytes).await?;
        ensure_expected_sha256(spec, &local_sha256)?;
        let temporary_path = resumable_remote_temporary_path(&spec.remote_path, &local_sha256);
        let _transfer_guard = handle.transfer_lock.lock().await;
        self.enter_pty_transfer_mode(handle, request.operation_id)
            .await?;
        let transfer = tokio::time::timeout(
            Duration::from_secs(spec.timeout_seconds),
            self.run_interactive_pty_upload(
                handle,
                request,
                local_size,
                &local_sha256,
                &temporary_path,
            ),
        )
        .await
        .map_err(|_| TransportError::Timeout)
        .and_then(std::convert::identity);
        let restore = self
            .leave_pty_transfer_mode(handle, request.operation_id)
            .await;
        match (transfer, restore) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn run_interactive_pty_upload(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        local_size: u64,
        local_sha256: &str,
        temporary_path: &str,
    ) -> Result<SftpResult, TransportError> {
        let preparation = self
            .prepare_interactive_pty_upload(
                handle,
                request,
                local_size,
                local_sha256,
                temporary_path,
            )
            .await?;
        let InteractivePtyUploadPreparation::Pending {
            local,
            hasher,
            resume_bytes,
        } = preparation
        else {
            let InteractivePtyUploadPreparation::Complete(result) = preparation else {
                unreachable!();
            };
            return Ok(result);
        };
        let (bytes_transferred, streamed_sha256) = self
            .stream_interactive_pty_upload(
                handle,
                request,
                temporary_path,
                local_size,
                local,
                hasher,
                resume_bytes,
            )
            .await?;
        if bytes_transferred != local_size || streamed_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(
                "interactive PTY upload local stream verification failed".to_owned(),
            ));
        }
        self.finalize_interactive_pty_upload(
            handle,
            request,
            temporary_path,
            local_size,
            local_sha256,
            resume_bytes,
        )
        .await
    }

    async fn prepare_interactive_pty_upload(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        local_size: u64,
        local_sha256: &str,
        temporary_path: &str,
    ) -> Result<InteractivePtyUploadPreparation, TransportError> {
        let spec = &request.spec;
        let initialize =
            russh_exec_upload_initialize_command(spec, temporary_path, local_size, local_sha256);
        let initialized = self
            .execute_pty_transfer_stage(handle, request.operation_id, "initialize", &initialize)
            .await?;
        require_exec_transfer_success(
            &initialized,
            "initialize interactive PTY upload",
            &spec.remote_path,
            temporary_path,
        )?;
        let status = parse_exec_upload_status(&initialized.stdout)?;
        let ExecUploadRemoteStatus::Ready {
            bytes: mut resume_bytes,
            prefix_sha256,
        } = status
        else {
            let ExecUploadRemoteStatus::Complete { size, sha256 } = status else {
                unreachable!();
            };
            if size != local_size || sha256 != local_sha256 {
                return Err(TransportError::FileTransfer(
                    "completed interactive PTY upload marker does not match local source"
                        .to_owned(),
                ));
            }
            return Ok(InteractivePtyUploadPreparation::Complete(SftpResult {
                direction: spec.direction,
                bytes_transferred: size,
                sha256,
                local_path: spec.local_path.clone(),
                remote_path: spec.remote_path.clone(),
                overwrite: spec.overwrite,
            }));
        };

        let (mut local, mut hasher, local_prefix_sha256) =
            open_local_upload_at_offset(&spec.local_path, resume_bytes, spec.max_size_bytes)
                .await?;
        if prefix_sha256 != local_prefix_sha256 {
            let reset = russh_exec_upload_cleanup_command(temporary_path);
            let reset_outcome = self
                .execute_pty_transfer_stage(handle, request.operation_id, "reset", &reset)
                .await?;
            require_exec_transfer_marker(
                &reset_outcome,
                "REMOTE_HOSTS_RESET_OK",
                "reset interactive PTY upload",
                &spec.remote_path,
                temporary_path,
            )?;
            let reinitialized = self
                .execute_pty_transfer_stage(
                    handle,
                    request.operation_id,
                    "reinitialize",
                    &initialize,
                )
                .await?;
            require_exec_transfer_success(
                &reinitialized,
                "reinitialize interactive PTY upload",
                &spec.remote_path,
                temporary_path,
            )?;
            let ExecUploadRemoteStatus::Ready {
                bytes,
                prefix_sha256: _,
            } = parse_exec_upload_status(&reinitialized.stdout)?
            else {
                return Err(TransportError::FileTransfer(
                    "interactive PTY upload reset unexpectedly completed".to_owned(),
                ));
            };
            if bytes != 0 {
                return Err(TransportError::FileTransfer(
                    "interactive PTY upload reset did not produce an empty temporary file"
                        .to_owned(),
                ));
            }
            (local, hasher, _) =
                open_local_upload_at_offset(&spec.local_path, 0, spec.max_size_bytes).await?;
            resume_bytes = 0;
        }

        emit_sftp_progress(
            request,
            "resume_verified",
            resume_bytes,
            Some(local_size),
            resume_bytes,
            0,
        );
        Ok(InteractivePtyUploadPreparation::Pending {
            local,
            hasher,
            resume_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_interactive_pty_upload(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        temporary_path: &str,
        local_size: u64,
        mut local: tokio::fs::File,
        mut hasher: Sha256,
        resume_bytes: u64,
    ) -> Result<(u64, String), TransportError> {
        let spec = &request.spec;
        let initial_resume_bytes = resume_bytes;
        let mut bytes_transferred = resume_bytes;
        let mut chunk_index = resume_bytes / PTY_UPLOAD_CHUNK_BYTES as u64;
        let mut buffer = vec![0_u8; PTY_UPLOAD_CHUNK_BYTES];
        loop {
            let mut filled = 0;
            while filled < buffer.len() {
                let read = local.read(&mut buffer[filled..]).await.map_err(|error| {
                    file_transfer_io_context("read interactive PTY upload source", error)
                })?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                break;
            }
            let next_offset = bytes_transferred
                .checked_add(u64::try_from(filled).map_err(file_transfer_conversion)?)
                .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
            ensure_size_within_limit(next_offset, spec.max_size_bytes)?;
            let payload = &buffer[..filled];
            let payload_sha256 = format!("{:x}", Sha256::digest(payload));
            let append = russh_exec_upload_chunk_command(
                temporary_path,
                chunk_index,
                bytes_transferred,
                payload,
            );
            let verify = russh_exec_upload_chunk_verify_command(
                temporary_path,
                chunk_index,
                next_offset,
                filled,
                &payload_sha256,
            );
            let stage = format!("{append}\n{verify}");
            let outcome = self
                .execute_pty_transfer_stage(
                    handle,
                    request.operation_id,
                    &format!("chunk-{chunk_index}"),
                    &stage,
                )
                .await?;
            require_exec_upload_chunk_success(
                &outcome,
                chunk_index,
                next_offset,
                &payload_sha256,
                &spec.remote_path,
                temporary_path,
            )?;
            hasher.update(payload);
            bytes_transferred = next_offset;
            chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
                TransportError::FileTransfer("upload chunk index overflow".to_owned())
            })?;
            emit_sftp_progress(
                request,
                "uploading",
                bytes_transferred,
                Some(local_size),
                initial_resume_bytes,
                0,
            );
        }
        Ok((bytes_transferred, format!("{:x}", hasher.finalize())))
    }

    async fn finalize_interactive_pty_upload(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        temporary_path: &str,
        local_size: u64,
        local_sha256: &str,
        initial_resume_bytes: u64,
    ) -> Result<SftpResult, TransportError> {
        let spec = &request.spec;
        let finalize =
            russh_exec_upload_finalize_command(spec, temporary_path, local_size, local_sha256);
        let finalized = self
            .execute_pty_transfer_stage(handle, request.operation_id, "finalize", &finalize)
            .await?;
        require_exec_transfer_success(
            &finalized,
            "finalize interactive PTY upload",
            &spec.remote_path,
            temporary_path,
        )?;
        let (remote_size, remote_sha256) =
            parse_transfer_marker(&finalized.stdout, "REMOTE_HOSTS_TRANSFER_OK")?;
        if remote_size != local_size || remote_sha256 != local_sha256 {
            return Err(TransportError::FileTransfer(
                "interactive PTY upload destination verification failed".to_owned(),
            ));
        }
        emit_sftp_progress(
            request,
            "completed",
            remote_size,
            Some(local_size),
            initial_resume_bytes,
            0,
        );
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred: remote_size,
            sha256: remote_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }

    async fn download_through_interactive_pty(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
    ) -> Result<SftpResult, TransportError> {
        let spec = &request.spec;
        let destination = Path::new(&spec.local_path);
        ensure_local_destination(destination, spec.overwrite).await?;
        let temporary_path = local_temporary_path(destination, request.operation_id)?;
        cleanup_local_temporary_file(&temporary_path).await?;

        let _transfer_guard = handle.transfer_lock.lock().await;
        self.enter_pty_transfer_mode(handle, request.operation_id)
            .await?;
        let transfer = tokio::time::timeout(
            Duration::from_secs(spec.timeout_seconds),
            self.run_interactive_pty_download(handle, request, &temporary_path),
        )
        .await
        .map_err(|_| TransportError::Timeout)
        .and_then(std::convert::identity);
        let restore = self
            .leave_pty_transfer_mode(handle, request.operation_id)
            .await;
        let result = match (transfer, restore) {
            (Ok(result), Ok(())) => place_local_file(&temporary_path, destination, spec.overwrite)
                .await
                .map(|()| result),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        };
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary_path).await;
        }
        result
    }

    async fn run_interactive_pty_download(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        temporary_path: &Path,
    ) -> Result<SftpResult, TransportError> {
        let spec = &request.spec;
        let metadata_script = interactive_pty_download_metadata_command(spec);
        let initial = self
            .execute_pty_transfer_stage(
                handle,
                request.operation_id,
                "download-metadata-initial",
                &metadata_script,
            )
            .await?;
        require_exec_transfer_success(
            &initial,
            "inspect interactive PTY download source",
            &spec.remote_path,
            "",
        )?;
        let (remote_size, remote_sha256) =
            parse_transfer_marker(&initial.stdout, "REMOTE_HOSTS_DOWNLOAD_META")?;
        ensure_size_within_limit(remote_size, spec.max_size_bytes)?;
        ensure_expected_sha256(spec, &remote_sha256)?;

        let local_sha256 = self
            .stream_interactive_pty_download_chunks(handle, request, temporary_path, remote_size)
            .await?;
        if local_sha256 != remote_sha256 {
            return Err(TransportError::FileTransfer(
                "interactive PTY download whole-file verification failed".to_owned(),
            ));
        }
        let final_metadata = self
            .execute_pty_transfer_stage(
                handle,
                request.operation_id,
                "download-metadata-final",
                &metadata_script,
            )
            .await?;
        require_exec_transfer_success(
            &final_metadata,
            "reinspect interactive PTY download source",
            &spec.remote_path,
            "",
        )?;
        let final_remote =
            parse_transfer_marker(&final_metadata.stdout, "REMOTE_HOSTS_DOWNLOAD_META")?;
        if final_remote != (remote_size, remote_sha256.clone()) {
            return Err(TransportError::FileTransfer(
                "remote source changed during interactive PTY download".to_owned(),
            ));
        }
        if let Some(mode) = spec.mode {
            set_local_mode(temporary_path, mode).await?;
        }
        emit_sftp_progress(request, "completed", remote_size, Some(remote_size), 0, 0);
        Ok(SftpResult {
            direction: spec.direction,
            bytes_transferred: remote_size,
            sha256: remote_sha256,
            local_path: spec.local_path.clone(),
            remote_path: spec.remote_path.clone(),
            overwrite: spec.overwrite,
        })
    }

    async fn stream_interactive_pty_download_chunks(
        &self,
        handle: &InteractivePtyTransferHandle,
        request: &SftpRequest,
        temporary_path: &Path,
        remote_size: u64,
    ) -> Result<String, TransportError> {
        let spec = &request.spec;
        let mut local = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_path)
            .await
            .map_err(file_transfer_io)?;
        let mut hasher = Sha256::new();
        let mut bytes_transferred = 0_u64;
        let mut chunk_index = 0_u64;
        while bytes_transferred < remote_size {
            let remaining = remote_size - bytes_transferred;
            let expected_chunk_size = remaining.min(PTY_DOWNLOAD_CHUNK_BYTES as u64);
            let chunk_script = interactive_pty_download_chunk_command(
                spec,
                remote_size,
                chunk_index,
                bytes_transferred,
                expected_chunk_size,
            );
            let stage = format!("download-chunk-{chunk_index}");
            let outcome = self
                .execute_pty_transfer_stage(handle, request.operation_id, &stage, &chunk_script)
                .await?;
            require_exec_transfer_success(&outcome, &stage, &spec.remote_path, "")?;
            let chunk = parse_interactive_pty_download_chunk(&outcome.stdout)?;
            if chunk.index != chunk_index
                || chunk.offset != bytes_transferred
                || u64::try_from(chunk.payload.len()).map_err(file_transfer_conversion)?
                    != expected_chunk_size
            {
                return Err(TransportError::FileTransfer(
                    "interactive PTY download chunk metadata does not match the request".to_owned(),
                ));
            }
            let actual_chunk_sha256 = format!("{:x}", Sha256::digest(&chunk.payload));
            if actual_chunk_sha256 != chunk.sha256 {
                return Err(TransportError::FileTransfer(
                    "interactive PTY download chunk SHA-256 verification failed".to_owned(),
                ));
            }
            local.write_all(&chunk.payload).await.map_err(|error| {
                file_transfer_io_context("write interactive PTY download", error)
            })?;
            hasher.update(&chunk.payload);
            bytes_transferred = bytes_transferred
                .checked_add(expected_chunk_size)
                .ok_or_else(|| TransportError::FileTransfer("file size overflow".to_owned()))?;
            chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
                TransportError::FileTransfer("download chunk index overflow".to_owned())
            })?;
            emit_sftp_progress(
                request,
                "downloading",
                bytes_transferred,
                Some(remote_size),
                0,
                0,
            );
        }
        local
            .shutdown()
            .await
            .map_err(|error| file_transfer_io_context("close local download file", error))?;
        Ok(format!("{:x}", hasher.finalize()))
    }

    async fn enter_pty_transfer_mode(
        &self,
        handle: &InteractivePtyTransferHandle,
        operation_id: OperationId,
    ) -> Result<(), TransportError> {
        let (command, marker) = pty_transfer_enter_command(operation_id);
        self.send_pty_command_and_wait(handle, command, &marker, PTY_TRANSFER_STAGE_TIMEOUT)
            .await
            .map(|_| ())
    }

    async fn leave_pty_transfer_mode(
        &self,
        handle: &InteractivePtyTransferHandle,
        operation_id: OperationId,
    ) -> Result<(), TransportError> {
        let (command, marker) = pty_transfer_leave_command(operation_id);
        self.send_pty_command_and_wait(handle, command, &marker, PTY_TRANSFER_STAGE_TIMEOUT)
            .await
            .map(|_| ())
    }

    async fn execute_pty_transfer_stage(
        &self,
        handle: &InteractivePtyTransferHandle,
        operation_id: OperationId,
        stage: &str,
        script: &str,
    ) -> Result<ExecResult, TransportError> {
        let (command, marker) = pty_transfer_stage_command(operation_id, stage, script);
        let output = self
            .send_pty_command_and_wait(handle, command, &marker, PTY_TRANSFER_STAGE_TIMEOUT)
            .await?;
        let marker_prefix = format!("{marker} ");
        let exit_code = output.lines().find_map(|line| {
            line.trim_matches('\r')
                .strip_prefix(&marker_prefix)
                .and_then(|value| value.trim().parse::<i32>().ok())
        });
        let Some(exit_code) = exit_code else {
            return Err(TransportError::FileTransfer(format!(
                "interactive PTY transfer stage `{stage}` returned a malformed completion marker"
            )));
        };
        Ok(ExecResult {
            exit_code: Some(exit_code),
            stdout: output,
            stderr: String::new(),
            truncated: false,
        })
    }

    async fn send_pty_command_and_wait(
        &self,
        handle: &InteractivePtyTransferHandle,
        command: String,
        marker: &str,
        timeout: Duration,
    ) -> Result<String, TransportError> {
        let (capture_tx, mut capture_rx) = mpsc::channel(8);
        match handle.transfer_capture.lock() {
            Ok(mut capture) => {
                if capture.is_some() {
                    return Err(TransportError::FileTransfer(
                        "interactive PTY already has an active transfer capture".to_owned(),
                    ));
                }
                *capture = Some(capture_tx);
            }
            Err(poisoned) => {
                let mut capture = poisoned.into_inner();
                if capture.is_some() {
                    return Err(TransportError::FileTransfer(
                        "interactive PTY already has an active transfer capture".to_owned(),
                    ));
                }
                *capture = Some(capture_tx);
            }
        }
        let _capture_guard = PtyTransferCaptureGuard {
            capture: Arc::clone(&handle.transfer_capture),
        };
        handle.input_tx.send(command).await.map_err(|_| {
            TransportError::FileTransfer(
                "interactive PTY input channel closed during file transfer".to_owned(),
            )
        })?;
        let deadline = Instant::now() + timeout;
        let mut output = String::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TransportError::FileTransfer(format!(
                    "interactive PTY transfer did not return marker {marker}"
                )));
            }
            let captured = tokio::time::timeout(remaining, capture_rx.recv())
                .await
                .map_err(|_| {
                    TransportError::FileTransfer(format!(
                        "interactive PTY transfer did not return marker {marker}"
                    ))
                })?
                .ok_or_else(|| {
                    TransportError::FileTransfer(
                        "interactive PTY output channel closed during file transfer".to_owned(),
                    )
                })?;
            if captured.truncated {
                return Err(TransportError::FileTransfer(
                    "interactive PTY backend truncated transfer output".to_owned(),
                ));
            }
            output.push_str(&captured.text);
            if output.len() > PTY_TRANSFER_CAPTURE_LIMIT_BYTES {
                return Err(TransportError::FileTransfer(format!(
                    "interactive PTY transfer output exceeded {PTY_TRANSFER_CAPTURE_LIMIT_BYTES} bytes before marker {marker}"
                )));
            }
            if output.contains(marker) {
                return Ok(output);
            }
        }
    }
}

fn pty_transfer_stage_command(
    operation_id: OperationId,
    stage: &str,
    script: &str,
) -> (String, String) {
    let stage_digest = format!("{:x}", Sha256::digest(stage.as_bytes()));
    let marker = format!(
        "REMOTE_HOSTS_PTY_TRANSFER_FRAME_{operation_id}_{}",
        &stage_digest[..12]
    );
    let encoded_script = shell_quote(&BASE64_STANDARD.encode(script.as_bytes()));
    let command = format!(
        "printf '\\n'; printf '%s' {encoded_script} | (base64 -d 2>/dev/null || base64 -D) | sh; rc=$?; printf '\\n{marker} %s\\n' \"$rc\"\n"
    );
    (command, marker)
}

fn pty_transfer_enter_command(operation_id: OperationId) -> (String, String) {
    let marker = format!("REMOTE_HOSTS_PTY_TRANSFER_READY_{operation_id}");
    let operation_id_text = operation_id.to_string();
    let command = format!(
        "stty -echo -icanon min 1 time 0 && printf '\\nREMOTE_HOSTS_PTY_TRANSFER_READY_%s\\n' '{operation_id_text}'\n"
    );
    (command, marker)
}

fn pty_transfer_leave_command(operation_id: OperationId) -> (String, String) {
    let marker = format!("REMOTE_HOSTS_PTY_TRANSFER_RESTORED_{operation_id}");
    let operation_id_text = operation_id.to_string();
    let command = format!(
        "stty echo icanon && printf '\\nREMOTE_HOSTS_PTY_TRANSFER_RESTORED_%s\\n' '{operation_id_text}'\n"
    );
    (command, marker)
}

fn interactive_pty_download_metadata_command(spec: &FileTransferSpec) -> String {
    let source = shell_quote(&spec.remote_path);
    let expected_digest = spec
        .expected_sha256
        .as_deref()
        .map_or_else(String::new, |digest| {
            format!("[ \"$digest\" = \"{digest}\" ] || exit 76\n")
        });
    format!(
        "set -eu\nsrc={source}\n[ -f \"$src\" ] && [ ! -L \"$src\" ] || exit 71\nbytes=$(wc -c < \"$src\" | tr -d '[:space:]')\n[ \"$bytes\" -le \"{}\" ] || exit 77\nif command -v sha256sum >/dev/null 2>&1; then digest=$(sha256sum \"$src\" | awk '{{print $1}}'); elif command -v shasum >/dev/null 2>&1; then digest=$(shasum -a 256 \"$src\" | awk '{{print $1}}'); else exit 75; fi\n{expected_digest}printf 'REMOTE_HOSTS_DOWNLOAD_META %s %s\\n' \"$bytes\" \"$digest\"\n",
        spec.max_size_bytes
    )
}

fn interactive_pty_download_chunk_command(
    spec: &FileTransferSpec,
    remote_size: u64,
    chunk_index: u64,
    offset: u64,
    expected_chunk_size: u64,
) -> String {
    let source = shell_quote(&spec.remote_path);
    format!(
        "set -eu\nsrc={source}\n[ -f \"$src\" ] && [ ! -L \"$src\" ] || exit 71\n[ \"$(wc -c < \"$src\" | tr -d '[:space:]')\" = \"{remote_size}\" ] || exit 78\nread_chunk() {{ dd if=\"$src\" bs={PTY_DOWNLOAD_CHUNK_BYTES} skip={chunk_index} count=1 2>/dev/null; }}\nif command -v sha256sum >/dev/null 2>&1; then chunk_digest=$(read_chunk | sha256sum | awk '{{print $1}}'); elif command -v shasum >/dev/null 2>&1; then chunk_digest=$(read_chunk | shasum -a 256 | awk '{{print $1}}'); else exit 75; fi\nprintf 'REMOTE_HOSTS_DOWNLOAD_CHUNK_BEGIN {chunk_index} {offset} {expected_chunk_size} %s\\n' \"$chunk_digest\"\nread_chunk | base64 | tr -d '\\r\\n'\nprintf '\\nREMOTE_HOSTS_DOWNLOAD_CHUNK_END {chunk_index}\\n'\n"
    )
}

fn parse_interactive_pty_download_chunk(
    output: &str,
) -> Result<InteractivePtyDownloadChunk, TransportError> {
    let lines = output
        .lines()
        .map(|line| line.trim_matches('\r'))
        .collect::<Vec<_>>();
    let (begin_position, begin) = lines
        .iter()
        .enumerate()
        .find(|(_, line)| line.starts_with("REMOTE_HOSTS_DOWNLOAD_CHUNK_BEGIN "))
        .ok_or_else(|| {
            TransportError::FileTransfer(
                "interactive PTY download chunk start marker is missing".to_owned(),
            )
        })?;
    let mut fields = begin.split_ascii_whitespace();
    if fields.next() != Some("REMOTE_HOSTS_DOWNLOAD_CHUNK_BEGIN") {
        return Err(TransportError::FileTransfer(
            "interactive PTY download chunk start marker is malformed".to_owned(),
        ));
    }
    let index = parse_download_chunk_number(fields.next(), "index")?;
    let offset = parse_download_chunk_number(fields.next(), "offset")?;
    let expected_size = parse_download_chunk_number(fields.next(), "size")?;
    let sha256 = fields
        .next()
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            TransportError::FileTransfer(
                "interactive PTY download chunk SHA-256 is malformed".to_owned(),
            )
        })?
        .to_ascii_lowercase();
    if fields.next().is_some() {
        return Err(TransportError::FileTransfer(
            "interactive PTY download chunk start marker has extra fields".to_owned(),
        ));
    }
    let payload_text = lines.get(begin_position + 1).ok_or_else(|| {
        TransportError::FileTransfer("interactive PTY download chunk payload is missing".to_owned())
    })?;
    let end = lines.get(begin_position + 2).ok_or_else(|| {
        TransportError::FileTransfer(
            "interactive PTY download chunk end marker is missing".to_owned(),
        )
    })?;
    if *end != format!("REMOTE_HOSTS_DOWNLOAD_CHUNK_END {index}") {
        return Err(TransportError::FileTransfer(
            "interactive PTY download chunk end marker is malformed".to_owned(),
        ));
    }
    let payload = BASE64_STANDARD.decode(payload_text).map_err(|_| {
        TransportError::FileTransfer(
            "interactive PTY download chunk Base64 payload is malformed".to_owned(),
        )
    })?;
    if u64::try_from(payload.len()).map_err(file_transfer_conversion)? != expected_size {
        return Err(TransportError::FileTransfer(
            "interactive PTY download chunk payload length is invalid".to_owned(),
        ));
    }
    Ok(InteractivePtyDownloadChunk {
        index,
        offset,
        payload,
        sha256,
    })
}

fn parse_download_chunk_number(value: Option<&str>, label: &str) -> Result<u64, TransportError> {
    value
        .ok_or_else(|| {
            TransportError::FileTransfer(format!(
                "interactive PTY download chunk {label} is missing"
            ))
        })?
        .parse::<u64>()
        .map_err(|error| {
            TransportError::FileTransfer(format!(
                "parse interactive PTY download chunk {label}: {error}"
            ))
        })
}

#[async_trait]
impl<B> InteractiveFileTransferBackend for ConnectorPtyManager<B>
where
    B: ManagedPtyBackend + 'static,
{
    async fn transfer_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        request: SftpRequest,
    ) -> Result<Option<SftpResult>, TransportError> {
        let workspace = self
            .repositories
            .workspaces
            .get(workspace_id)
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?
            .ok_or_else(|| {
                TransportError::Backend(format!("workspace not found: {workspace_id}"))
            })?;
        if workspace.host_id != request.host_id
            || workspace.access_path_id != request.access_path_id
        {
            return Err(TransportError::PolicyDenied(
                "interactive file transfer request does not match its workspace route".to_owned(),
            ));
        }
        let access_path = self
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await
            .map_err(|error| TransportError::Backend(error.to_string()))?
            .ok_or_else(|| {
                TransportError::Backend(format!(
                    "access path not found: {}",
                    workspace.access_path_id
                ))
            })?;
        if access_path.route_type != RouteType::Bastion || !access_path.proxy_chain.is_empty() {
            return Ok(None);
        }
        let handle = self.interactive_transfer_handle(workspace_id).await?;
        let result = match request.spec.direction {
            SftpDirection::Upload => {
                self.upload_through_interactive_pty(&handle, &request)
                    .await?
            }
            SftpDirection::Download => {
                self.download_through_interactive_pty(&handle, &request)
                    .await?
            }
        };
        Ok(Some(result))
    }
}

/// Worker that claims queued operations and executes them through a reusable transport.
pub struct ConnectorOperationWorker<P> {
    repositories: Repositories,
    provider: P,
    config: ConnectorOperationWorkerConfig,
    redactor: SecretRedactor,
    artifact_store: Arc<dyn OutputArtifactStore>,
    interactive_file_transfer: StdMutex<Option<Arc<dyn InteractiveFileTransferBackend>>>,
}

enum ClaimedOperationExecution {
    Exec(CommandProfile),
    Sftp(FileTransferSpec),
}

impl<P> ConnectorOperationWorker<P>
where
    P: RemoteTransportProvider,
{
    /// Creates a worker.
    pub fn new(
        repositories: Repositories,
        provider: P,
        config: ConnectorOperationWorkerConfig,
    ) -> Self {
        Self::with_artifact_store(
            repositories,
            provider,
            config,
            Arc::new(FileOutputArtifactStore::new(DEFAULT_ARTIFACT_ROOT)),
        )
    }

    /// Creates a worker with an explicit output artifact store.
    pub fn with_artifact_store(
        repositories: Repositories,
        provider: P,
        config: ConnectorOperationWorkerConfig,
        artifact_store: Arc<dyn OutputArtifactStore>,
    ) -> Self {
        Self {
            repositories,
            provider,
            config,
            redactor: SecretRedactor::default(),
            artifact_store,
            interactive_file_transfer: StdMutex::new(None),
        }
    }

    /// Attaches a connector-local transfer backend for interactive bastion workspaces.
    pub fn set_interactive_file_transfer(&self, backend: Arc<dyn InteractiveFileTransferBackend>) {
        *self
            .interactive_file_transfer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(backend);
    }

    /// Claims and executes at most one operation.
    ///
    /// # Errors
    ///
    /// Returns an error for database failures or worker infrastructure failures. Operation-level
    /// transport and validation failures are persisted to the operation before returning an
    /// outcome.
    pub async fn run_once(
        &self,
    ) -> Result<Option<ConnectorOperationOutcome>, ConnectorWorkerError> {
        let claimed_at = now_utc();
        if let Some(outcome) = self.exhaust_next_operation(claimed_at).await? {
            return Ok(Some(outcome));
        }

        let lease_seconds = i64::try_from(self.config.lease_seconds)?;
        let lease_expires_at = claimed_at + time::Duration::seconds(lease_seconds);
        let claim_token = Uuid::new_v4().to_string();
        let Some(operation) = self
            .repositories
            .operations
            .claim_next_for_connector(
                self.config.connector_id,
                &claim_token,
                claimed_at,
                lease_expires_at,
                self.config.max_attempts,
            )
            .await?
        else {
            return Ok(None);
        };

        self.append_system_chunk(&operation, "operation claimed by connector worker")
            .await?;
        self.execute_claimed(operation).await.map(Some)
    }

    async fn exhaust_next_operation(
        &self,
        observed_at: time::OffsetDateTime,
    ) -> Result<Option<ConnectorOperationOutcome>, ConnectorWorkerError> {
        let summary = exhaustion_summary(self.config.max_attempts);
        let Some(operation) = self
            .repositories
            .operations
            .exhaust_next_for_connector(
                self.config.connector_id,
                observed_at,
                self.config.max_attempts,
                &summary,
                &summary,
            )
            .await?
        else {
            return Ok(None);
        };
        let workspace_id = operation
            .workspace_id
            .ok_or(ConnectorWorkerError::MissingWorkspace)?;
        let message = format!(
            "{} attempt_count={}; recovery_hint=inspect connector health, use an alternate access path, or queue a fresh operation after recovery",
            summary, operation.attempt_count
        );
        self.append_system_chunk(&operation, &message).await?;
        self.repositories
            .workspaces
            .update_state(workspace_id, WorkspaceState::Blocked, observed_at)
            .await?;
        Ok(Some(ConnectorOperationOutcome {
            operation_id: operation.id,
            workspace_id,
            state: OperationState::Exhausted,
            workspace_state: WorkspaceState::Blocked,
            exit_code: None,
        }))
    }

    async fn execute_claimed(
        &self,
        mut operation: OperationRun,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        operation
            .workspace_id
            .ok_or(ConnectorWorkerError::MissingWorkspace)?;
        let execution = match Self::claimed_execution(&operation) {
            Ok(execution) => execution,
            Err(error) => {
                let message = self.redactor.redact(&error.to_string());
                self.append_system_chunk(&operation, &format!("operation rejected: {message}"))
                    .await?;
                return self
                    .finish_operation(
                        &operation,
                        OperationState::Rejected,
                        WorkspaceState::Failed,
                        None,
                        "operation rejected before execution",
                        Some(&message),
                    )
                    .await;
            }
        };
        if let Some(outcome) = self.defer_for_foreign_write_lease(&operation).await? {
            return Ok(outcome);
        }
        if let Some((message, workspace_state)) = self.active_connection_block(&operation).await? {
            self.append_system_chunk(&operation, &message).await?;
            return self
                .finish_operation(
                    &operation,
                    OperationState::Rejected,
                    workspace_state,
                    None,
                    "operation blocked by connection protection state",
                    Some(&message),
                )
                .await;
        }
        self.begin_connection_use(&mut operation).await?;
        let transport = match self.provider.transport_for(&operation).await {
            Ok(transport) => transport,
            Err(error) => {
                let message = self.redactor.redact(&error);
                self.record_connection_failure(&operation, &message, None)
                    .await?;
                self.append_system_chunk(&operation, &format!("transport unavailable: {message}"))
                    .await?;
                return self
                    .finish_operation(
                        &operation,
                        OperationState::Failed,
                        WorkspaceState::Failed,
                        None,
                        "transport unavailable",
                        Some(&message),
                    )
                    .await;
            }
        };
        let before_telemetry = self
            .transport_observation_baseline(&operation, transport.transport_telemetry())
            .await;

        match execution {
            ClaimedOperationExecution::Exec(profile) => {
                self.execute_exec_operation(
                    &mut operation,
                    transport,
                    profile,
                    before_telemetry.as_ref(),
                )
                .await
            }
            ClaimedOperationExecution::Sftp(spec) => {
                self.execute_sftp_operation(
                    &mut operation,
                    transport,
                    spec,
                    before_telemetry.as_ref(),
                )
                .await
            }
        }
    }

    async fn defer_for_foreign_write_lease(
        &self,
        operation: &OperationRun,
    ) -> Result<Option<ConnectorOperationOutcome>, ConnectorWorkerError> {
        if !operation.requires_write_lease {
            return Ok(None);
        }
        let (Some(agent_session_id), Some(workspace_id), Some(claim_token)) = (
            operation.agent_session_id,
            operation.workspace_id,
            operation.claim_token.as_deref(),
        ) else {
            return Ok(None);
        };
        let observed_at = now_utc();
        let lease_seconds = i64::try_from(self.config.lease_seconds)?;
        let coordination_scopes = operation_coordination_scopes(operation);
        let leases = coordination_scopes
            .iter()
            .map(|coordination_scope| HostWriteLease {
                host_id: operation.host_id,
                coordination_scope: coordination_scope.clone(),
                holder_agent_session_id: agent_session_id,
                holder_workspace_id: workspace_id,
                acquired_at: observed_at,
                heartbeat_at: observed_at,
                expires_at: observed_at + time::Duration::seconds(lease_seconds),
            })
            .collect::<Vec<_>>();
        let acquired = self
            .repositories
            .host_write_leases
            .try_acquire_many(&leases, observed_at)
            .await?;
        if acquired.is_some() {
            return Ok(None);
        }
        let summary =
            "operation is waiting for another agent session's overlapping write scope to expire";
        self.append_system_chunk(operation, summary).await?;
        if !self
            .repositories
            .operations
            .defer_claimed_for_write_lease(operation.id, claim_token, summary)
            .await?
        {
            return Err(ConnectorWorkerError::LeaseLost);
        }
        Ok(Some(ConnectorOperationOutcome {
            operation_id: operation.id,
            workspace_id,
            state: OperationState::Queued,
            workspace_state: WorkspaceState::Working,
            exit_code: None,
        }))
    }

    async fn execute_exec_operation(
        &self,
        operation: &mut OperationRun,
        transport: Arc<dyn RemoteTransport>,
        profile: CommandProfile,
        before_telemetry: Option<&SshTransportTelemetry>,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        let request = ExecRequest {
            operation_id: operation.id,
            host_id: operation.host_id,
            access_path_id: operation.access_path_id,
            profile: profile.clone(),
        };
        let result = self
            .exec_with_lease_renewal(operation, Arc::clone(&transport), request)
            .await;
        self.record_transport_observation(
            operation,
            SshChannelKind::Exec,
            before_telemetry,
            transport.transport_telemetry(),
        )
        .await;
        match result {
            Ok(result) => self.persist_exec_result(operation, &profile, result).await,
            Err(error) => self.persist_transport_error(operation, error).await,
        }
    }

    async fn execute_sftp_operation(
        &self,
        operation: &mut OperationRun,
        transport: Arc<dyn RemoteTransport>,
        spec: FileTransferSpec,
        before_telemetry: Option<&SshTransportTelemetry>,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        let request = SftpRequest {
            operation_id: operation.id,
            host_id: operation.host_id,
            access_path_id: operation.access_path_id,
            spec,
            progress_tx: None,
        };
        let interactive_backend = self
            .interactive_file_transfer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let result = self
            .sftp_with_lease_renewal(
                operation,
                Arc::clone(&transport),
                request,
                interactive_backend,
            )
            .await;
        self.record_transport_observation(
            operation,
            SshChannelKind::FileTransfer,
            before_telemetry,
            transport.transport_telemetry(),
        )
        .await;
        match result {
            Ok(result) => self.persist_sftp_result(operation, result).await,
            Err(error) => self.persist_transport_error(operation, error).await,
        }
    }

    async fn record_transport_observation(
        &self,
        operation: &mut OperationRun,
        channel_kind: SshChannelKind,
        before: Option<&SshTransportTelemetry>,
        after: Option<SshTransportTelemetry>,
    ) {
        let Some(after) = after else {
            return;
        };
        let observed_at = now_utc();
        let evidence =
            SshChannelTransportEvidence::between(channel_kind, before, &after, observed_at);
        let runtime = SshTransportRuntime {
            access_path_id: operation.access_path_id,
            connector_id: operation.connector_id,
            telemetry: after,
            updated_at: observed_at,
        };
        if let Err(error) = self
            .repositories
            .ssh_transport_runtimes
            .upsert(&runtime)
            .await
        {
            tracing::warn!(
                operation_id = %operation.id,
                access_path_id = %operation.access_path_id,
                %error,
                "failed to persist SSH transport runtime telemetry"
            );
        }
        let Some(claim_token) = operation.claim_token.as_deref() else {
            return;
        };
        match self
            .repositories
            .operations
            .attach_transport_evidence(operation.id, claim_token, &evidence)
            .await
        {
            Ok(true) => operation.transport_evidence = Some(evidence),
            Ok(false) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    "SSH transport evidence was not attached because the operation lease changed"
                );
            }
            Err(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    %error,
                    "failed to persist SSH transport evidence"
                );
            }
        }
    }

    async fn transport_observation_baseline(
        &self,
        operation: &OperationRun,
        current: Option<SshTransportTelemetry>,
    ) -> Option<SshTransportTelemetry> {
        let current = current?;
        if current.generation > 0 {
            return Some(current);
        }
        match self
            .repositories
            .ssh_transport_runtimes
            .get(operation.access_path_id, operation.connector_id)
            .await
        {
            Ok(Some(previous)) if previous.telemetry.runtime_id != current.runtime_id => {
                Some(previous.telemetry)
            }
            Ok(_) => Some(current),
            Err(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    access_path_id = %operation.access_path_id,
                    %error,
                    "failed to load previous SSH transport runtime telemetry"
                );
                Some(current)
            }
        }
    }

    async fn exec_with_lease_renewal(
        &self,
        operation: &OperationRun,
        transport: Arc<dyn RemoteTransport>,
        request: ExecRequest,
    ) -> Result<ExecResult, TransportError> {
        let claim_token = operation.claim_token.clone().ok_or_else(|| {
            TransportError::Backend(ConnectorWorkerError::MissingClaimToken.to_string())
        })?;
        let renew_delay = self.lease_renew_delay();
        let exec = transport.exec(request);
        tokio::pin!(exec);

        loop {
            tokio::select! {
                result = &mut exec => return result,
                () = tokio::time::sleep(renew_delay) => {
                    let lease_seconds = i64::try_from(self.config.lease_seconds)
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                    let renewed = self.repositories.operations
                        .renew_claim(
                            operation.id,
                            &claim_token,
                            now_utc() + time::Duration::seconds(lease_seconds),
                        )
                        .await
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                    if !renewed {
                        return Err(TransportError::Backend(
                            ConnectorWorkerError::LeaseLost.to_string(),
                        ));
                    }
                    self.renew_write_lease(operation)
                        .await
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn sftp_with_lease_renewal(
        &self,
        operation: &OperationRun,
        transport: Arc<dyn RemoteTransport>,
        mut request: SftpRequest,
        interactive_backend: Option<Arc<dyn InteractiveFileTransferBackend>>,
    ) -> Result<SftpResult, TransportError> {
        let claim_token = operation.claim_token.clone().ok_or_else(|| {
            TransportError::Backend(ConnectorWorkerError::MissingClaimToken.to_string())
        })?;
        let renew_delay = self.lease_renew_delay();
        let started_at = Instant::now();
        let transfer_timeout = Duration::from_secs(request.spec.timeout_seconds);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        request.progress_tx = Some(progress_tx);
        let direction = request.spec.direction;
        let max_size_bytes = request.spec.max_size_bytes;
        self.append_system_chunk(
            operation,
            &format!(
                "file transfer started: direction={direction:?}, max_size_bytes={max_size_bytes}"
            ),
        )
        .await
        .map_err(|error| TransportError::Backend(error.to_string()))?;
        let workspace_id = operation
            .workspace_id
            .ok_or_else(|| TransportError::Backend("file transfer has no workspace".to_owned()))?;
        let fallback_request = request.clone();
        let transfer = async move {
            if let Some(backend) = interactive_backend
                && let Some(result) = backend
                    .transfer_for_workspace(workspace_id, request)
                    .await?
            {
                return Ok(result);
            }
            transport.sftp(fallback_request).await
        };
        tokio::pin!(transfer);
        let mut lease_interval = tokio::time::interval(renew_delay);
        lease_interval.tick().await;
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));
        heartbeat_interval.tick().await;
        let deadline = tokio::time::sleep_until(started_at + transfer_timeout);
        tokio::pin!(deadline);
        let mut progress_open = true;
        let mut latest_progress: Option<SftpProgress> = None;
        let mut last_progress_at = started_at;

        loop {
            tokio::select! {
                biased;
                result = &mut transfer => return result,
                () = &mut deadline => {
                    let summary = format!(
                        "file transfer deadline reached: {}",
                        format_sftp_heartbeat(
                            direction,
                            latest_progress.as_ref(),
                            started_at.elapsed().as_secs(),
                            last_progress_at.elapsed().as_secs(),
                        )
                    );
                    self.append_transfer_status_best_effort(operation, &summary)
                        .await;
                    return Err(TransportError::Timeout);
                }
                progress = progress_rx.recv(), if progress_open => {
                    match progress {
                        Some(progress) => {
                            if latest_progress.as_ref() != Some(&progress) {
                                let summary = format_sftp_progress(
                                    &progress,
                                    started_at.elapsed().as_secs(),
                                );
                                self.append_transfer_status_best_effort(operation, &summary)
                                    .await;
                                last_progress_at = Instant::now();
                                latest_progress = Some(progress);
                            }
                        }
                        None => progress_open = false,
                    }
                }
                _ = heartbeat_interval.tick() => {
                    let summary = format_sftp_heartbeat(
                        direction,
                        latest_progress.as_ref(),
                        started_at.elapsed().as_secs(),
                        last_progress_at.elapsed().as_secs(),
                    );
                    self.append_transfer_status_best_effort(operation, &summary)
                        .await;
                }
                _ = lease_interval.tick() => {
                    let lease_seconds = i64::try_from(self.config.lease_seconds)
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                    let renewed = self.repositories.operations
                        .renew_claim(
                            operation.id,
                            &claim_token,
                            now_utc() + time::Duration::seconds(lease_seconds),
                        )
                        .await
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                    if !renewed {
                        return Err(TransportError::Backend(
                            ConnectorWorkerError::LeaseLost.to_string(),
                        ));
                    }
                    self.renew_write_lease(operation)
                        .await
                        .map_err(|error| TransportError::Backend(error.to_string()))?;
                }
            }
        }
    }

    async fn renew_write_lease(
        &self,
        operation: &OperationRun,
    ) -> Result<(), ConnectorWorkerError> {
        if !operation.requires_write_lease {
            return Ok(());
        }
        let (Some(agent_session_id), Some(workspace_id)) =
            (operation.agent_session_id, operation.workspace_id)
        else {
            return Ok(());
        };
        let heartbeat_at = now_utc();
        let lease_seconds = i64::try_from(self.config.lease_seconds)?;
        let coordination_scopes = operation_coordination_scopes(operation);
        if !self
            .repositories
            .host_write_leases
            .renew_many(
                operation.host_id,
                &coordination_scopes,
                agent_session_id,
                workspace_id,
                heartbeat_at,
                heartbeat_at + time::Duration::seconds(lease_seconds),
            )
            .await?
        {
            return Err(ConnectorWorkerError::WriteLeaseLost);
        }
        Ok(())
    }

    fn lease_renew_delay(&self) -> Duration {
        let lease_millis = self.config.lease_seconds.saturating_mul(1000);
        Duration::from_millis((lease_millis / 3).clamp(250, 30_000))
    }

    fn command_profile(operation: &OperationRun) -> Result<CommandProfile, ConnectorWorkerError> {
        let value = operation
            .command_profile_json
            .clone()
            .ok_or(ConnectorWorkerError::MissingCommandProfile)?;
        let profile: CommandProfile = serde_json::from_value(value)?;
        profile.validate()?;
        Ok(profile)
    }

    fn claimed_execution(
        operation: &OperationRun,
    ) -> Result<ClaimedOperationExecution, ConnectorWorkerError> {
        if operation.operation_type == remote_hosts_domain::OperationType::Sftp {
            let value = operation
                .command_profile_json
                .clone()
                .ok_or(ConnectorWorkerError::MissingCommandProfile)?;
            let spec: FileTransferSpec = serde_json::from_value(value)?;
            spec.validate()
                .map_err(|error| ConnectorWorkerError::InvalidFileTransfer(error.to_string()))?;
            return Ok(ClaimedOperationExecution::Sftp(spec));
        }
        Self::command_profile(operation).map(ClaimedOperationExecution::Exec)
    }

    async fn persist_exec_result(
        &self,
        operation: &OperationRun,
        profile: &CommandProfile,
        result: ExecResult,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        self.record_connection_success(operation).await?;
        let (stdout, stdout_truncated) =
            redact_and_truncate(&self.redactor, &result.stdout, profile.output_limit_bytes);
        let (stderr, stderr_truncated) =
            redact_and_truncate(&self.redactor, &result.stderr, profile.output_limit_bytes);
        let output_truncated = result.truncated || stdout_truncated || stderr_truncated;
        if !stdout.is_empty() {
            self.append_stream_output(operation, OutputStream::Stdout, &stdout, output_truncated)
                .await?;
        }
        if !stderr.is_empty() {
            self.append_stream_output(operation, OutputStream::Stderr, &stderr, output_truncated)
                .await?;
        }

        let state = if result.exit_code == Some(0) {
            OperationState::Succeeded
        } else {
            OperationState::Failed
        };
        // A remote command exit status is an operation result, not evidence that
        // the pooled transport or another active PTY became unusable.
        let workspace_state = WorkspaceState::Done;
        let summary = format!(
            "operation finished: state={state:?}, exit_code={:?}, stdout_bytes={}, stderr_bytes={}, truncated={output_truncated}",
            result.exit_code,
            stdout.len(),
            stderr.len(),
        );
        self.append_system_chunk(operation, &summary).await?;
        self.finish_operation(
            operation,
            state,
            workspace_state,
            result.exit_code,
            &summary,
            None,
        )
        .await
    }

    async fn persist_sftp_result(
        &self,
        operation: &OperationRun,
        result: SftpResult,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        self.record_connection_success(operation).await?;
        let direction = format!("{:?}", result.direction).to_lowercase();
        let file_name = result.remote_path.rsplit('/').next().unwrap_or("<invalid>");
        let summary = format!(
            "file transfer finished: state=succeeded, direction={direction}, file={file_name}, bytes={}, sha256={}, overwrite={:?}, pooled_session=true",
            result.bytes_transferred, result.sha256, result.overwrite
        );
        self.append_system_chunk(operation, &summary).await?;
        self.finish_operation(
            operation,
            OperationState::Succeeded,
            WorkspaceState::Done,
            Some(0),
            &summary,
            None,
        )
        .await
    }

    async fn persist_transport_error(
        &self,
        operation: &OperationRun,
        error: TransportError,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        let redacted = self.redactor.redact(&error.to_string());
        if matches!(error, TransportError::FileTransfer(_)) {
            self.record_connection_success(operation).await?;
        } else {
            self.record_connection_failure(operation, &redacted, Some(&error))
                .await?;
        }
        let (state, workspace_state) = match error {
            TransportError::PolicyDenied(_)
            | TransportError::LocalHandshakeBudgetExhausted { .. } => {
                (OperationState::Rejected, WorkspaceState::Throttled)
            }
            TransportError::Timeout => (OperationState::TimedOut, WorkspaceState::Failed),
            TransportError::Backend(_) => (OperationState::Failed, WorkspaceState::Failed),
            // FileTransfer explicitly means the operation failed without proving
            // that the underlying SSH connection is unhealthy.
            TransportError::FileTransfer(_) => (OperationState::Failed, WorkspaceState::Done),
        };
        self.append_system_chunk(operation, &format!("operation failed: {redacted}"))
            .await?;
        self.finish_operation(
            operation,
            state,
            workspace_state,
            None,
            "operation failed during connector execution",
            Some(&redacted),
        )
        .await
    }

    async fn begin_connection_use(
        &self,
        operation: &mut OperationRun,
    ) -> Result<(), ConnectorWorkerError> {
        let now = now_utc();
        let existing_session = match operation.session_id {
            Some(session_id) => {
                self.repositories
                    .connection_sessions
                    .get(session_id)
                    .await?
            }
            None => {
                self.repositories
                    .connection_sessions
                    .find_reusable(operation.access_path_id, operation.connector_id)
                    .await?
            }
        };
        let (mut session, reused) = existing_session.map_or_else(
            || {
                (
                    ConnectionSession {
                        session_id: SessionId::new(),
                        access_path_id: operation.access_path_id,
                        connector_id: operation.connector_id,
                        state: EntityState::Resolving,
                        created_at: now,
                        last_used_at: now,
                        open_channels: 0,
                        reused_count: 0,
                        failure_count: 0,
                        last_error: None,
                    },
                    false,
                )
            },
            |session| {
                let reused = matches!(
                    session.state,
                    EntityState::Connected | EntityState::Healthy | EntityState::Resolving
                );
                (session, reused)
            },
        );
        session.state = if matches!(session.state, EntityState::Connected | EntityState::Healthy) {
            EntityState::Connected
        } else {
            EntityState::Resolving
        };
        session.last_used_at = now;
        let session = self
            .repositories
            .connection_sessions
            .open_channel(&session, reused, false)
            .await?;
        let claim_token = operation
            .claim_token
            .as_deref()
            .ok_or(ConnectorWorkerError::MissingClaimToken)?;
        let attached = self
            .repositories
            .operations
            .attach_session(operation.id, claim_token, session.session_id)
            .await;
        match attached {
            Ok(true) => {}
            Ok(false) => {
                self.repositories
                    .connection_sessions
                    .close_channel(session.session_id, now_utc())
                    .await?;
                return Err(ConnectorWorkerError::LeaseLost);
            }
            Err(error) => {
                let _ = self
                    .repositories
                    .connection_sessions
                    .close_channel(session.session_id, now_utc())
                    .await;
                return Err(error.into());
            }
        }
        operation.session_id = Some(session.session_id);
        Ok(())
    }

    async fn active_connection_block(
        &self,
        operation: &OperationRun,
    ) -> Result<Option<(String, WorkspaceState)>, ConnectorWorkerError> {
        let Some(health) = self
            .repositories
            .access_path_health
            .get(operation.access_path_id)
            .await?
        else {
            return Ok(None);
        };
        if matches!(
            health.state,
            EntityState::AuthFailed | EntityState::HostKeyChanged
        ) {
            return Ok(Some((
                format!(
                    "connection blocked by {:?}; change credentials or verify the host key before retrying",
                    health.state
                ),
                WorkspaceState::Blocked,
            )));
        }
        if matches!(
            health.state,
            EntityState::RateLimited
                | EntityState::Throttled
                | EntityState::TargetOverloaded
                | EntityState::CircuitOpen
        ) && health
            .next_retry_at
            .is_none_or(|retry_at| retry_at > now_utc())
        {
            return Ok(Some((
                format!(
                    "connection cooldown is active: state={:?}, next_retry_at={:?}; reuse cached state and wait",
                    health.state, health.next_retry_at
                ),
                WorkspaceState::Throttled,
            )));
        }
        Ok(None)
    }

    async fn record_connection_success(
        &self,
        operation: &OperationRun,
    ) -> Result<(), ConnectorWorkerError> {
        let Some(session_id) = operation.session_id else {
            return Ok(());
        };
        let now = now_utc();
        if let Some(session) = self
            .repositories
            .connection_sessions
            .close_channel_success(session_id, now)
            .await?
        {
            debug_assert_eq!(session.state, EntityState::Connected);
        }
        self.repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: operation.access_path_id,
                state: EntityState::Connected,
                last_checked_at: Some(now),
                latency_ms: None,
                failure_count: 0,
                last_error_code: None,
                next_retry_at: None,
            })
            .await?;
        Ok(())
    }

    async fn record_connection_failure(
        &self,
        operation: &OperationRun,
        message: &str,
        error: Option<&TransportError>,
    ) -> Result<(), ConnectorWorkerError> {
        let Some(session_id) = operation.session_id else {
            return Ok(());
        };
        let now = now_utc();
        let local_handshake_budget = matches!(
            error,
            Some(TransportError::LocalHandshakeBudgetExhausted { .. })
        );
        let (mut state, mut reason_code, mut retry_after_seconds) = match error {
            Some(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds,
            }) => (
                EntityState::Throttled,
                StateReasonCode::LocalHandshakeBudgetExhausted,
                Some(*retry_after_seconds),
            ),
            Some(TransportError::PolicyDenied(_)) => (
                EntityState::RateLimited,
                StateReasonCode::TargetSshdRateLimited,
                Some(60),
            ),
            Some(TransportError::Timeout) => (
                EntityState::Degraded,
                StateReasonCode::SshHandshakeFailed,
                Some(30),
            ),
            Some(TransportError::FileTransfer(_)) => {
                return self.record_connection_success(operation).await;
            }
            Some(TransportError::Backend(_)) | None => classify_connection_failure(message),
        };
        let circuit_breaker_eligible = !local_handshake_budget
            && !matches!(state, EntityState::AuthFailed | EntityState::HostKeyChanged);
        let Some(session) = self
            .repositories
            .connection_sessions
            .record_failure(
                session_id,
                now,
                state.clone(),
                message,
                !local_handshake_budget,
                true,
                circuit_breaker_eligible,
                3,
            )
            .await?
        else {
            return Ok(());
        };
        state = session.state.clone();
        let failure_count = session.failure_count;
        if state == EntityState::CircuitOpen {
            reason_code = StateReasonCode::CircuitOpen;
            retry_after_seconds = Some(300);
        }
        let next_retry_at = retry_after_seconds.map(|seconds| {
            now + time::Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
        });
        self.repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: operation.access_path_id,
                state,
                last_checked_at: Some(now),
                latency_ms: None,
                failure_count,
                last_error_code: Some(reason_code),
                next_retry_at,
            })
            .await?;
        Ok(())
    }

    async fn finish_operation(
        &self,
        operation: &OperationRun,
        state: OperationState,
        workspace_state: WorkspaceState,
        exit_code: Option<i32>,
        summary: &str,
        last_error: Option<&str>,
    ) -> Result<ConnectorOperationOutcome, ConnectorWorkerError> {
        let workspace_id = operation
            .workspace_id
            .ok_or(ConnectorWorkerError::MissingWorkspace)?;
        let claim_token = operation
            .claim_token
            .as_deref()
            .ok_or(ConnectorWorkerError::MissingClaimToken)?;
        let finished_at = now_utc();
        let finished = self
            .repositories
            .operations
            .finish_claimed(ClaimedOperationFinish {
                id: operation.id,
                claim_token,
                state: state.clone(),
                finished_at,
                exit_code,
                redacted_output_summary: Some(summary),
                last_error,
            })
            .await?;
        if !finished {
            return Err(ConnectorWorkerError::LeaseLost);
        }
        self.repositories
            .workspaces
            .update_state(workspace_id, workspace_state.clone(), finished_at)
            .await?;
        self.shorten_write_lease_after_completion(operation, finished_at)
            .await?;
        Ok(ConnectorOperationOutcome {
            operation_id: operation.id,
            workspace_id,
            state,
            workspace_state,
            exit_code,
        })
    }

    async fn shorten_write_lease_after_completion(
        &self,
        operation: &OperationRun,
        finished_at: time::OffsetDateTime,
    ) -> Result<(), ConnectorWorkerError> {
        if !operation.requires_write_lease {
            return Ok(());
        }
        let Some(agent_session_id) = operation.agent_session_id else {
            return Ok(());
        };
        shorten_host_write_leases(
            &self.repositories,
            operation.host_id,
            &operation_coordination_scopes(operation),
            agent_session_id,
            finished_at,
        )
        .await?;
        Ok(())
    }

    async fn append_system_chunk(
        &self,
        operation: &OperationRun,
        message: &str,
    ) -> Result<(), ConnectorWorkerError> {
        self.append_output_chunk(operation, OutputStream::System, message, false)
            .await
    }

    async fn append_transfer_status_best_effort(&self, operation: &OperationRun, message: &str) {
        if let Err(error) = self.append_system_chunk(operation, message).await {
            tracing::warn!(
                operation_id = %operation.id,
                %error,
                "failed to persist file transfer progress without interrupting the data channel"
            );
        }
    }

    async fn append_stream_output(
        &self,
        operation: &OperationRun,
        stream: OutputStream,
        text: &str,
        truncated: bool,
    ) -> Result<(), ConnectorWorkerError> {
        if text.len() <= self.config.artifact_threshold_bytes {
            return self
                .append_output_chunk(operation, stream, text, truncated)
                .await;
        }

        let preview = preview_text(text, self.config.artifact_preview_bytes);
        let artifact = self
            .artifact_store
            .write_artifact(OutputArtifactWriteRequest {
                operation,
                stream: stream.clone(),
                redacted_text: text,
                redacted_preview: preview,
                truncated,
            })
            .await?;
        self.repositories
            .operation_output_artifacts
            .insert(&artifact)
            .await?;
        let summary = format!(
            "large {stream:?} output stored as artifact_id={} bytes={} sha256={} truncated={}\n{}",
            artifact.id,
            artifact.byte_len,
            artifact.sha256,
            artifact.truncated,
            artifact.redacted_preview,
        );
        self.append_output_chunk(operation, stream, &summary, true)
            .await
    }

    async fn append_output_chunk(
        &self,
        operation: &OperationRun,
        stream: OutputStream,
        text: &str,
        truncated: bool,
    ) -> Result<(), ConnectorWorkerError> {
        let workspace_id = operation
            .workspace_id
            .ok_or(ConnectorWorkerError::MissingWorkspace)?;
        let redacted_text = self.redactor.redact(text);
        let sequence = self
            .repositories
            .operation_output_chunks
            .next_sequence(operation.id)
            .await?;
        let chunk = OperationOutputChunk {
            id: OperationOutputChunkId::new(),
            operation_id: operation.id,
            workspace_id,
            stream,
            sequence,
            byte_len: u64::try_from(redacted_text.len())?,
            redacted_text,
            truncated,
            created_at: now_utc(),
        };
        self.repositories
            .operation_output_chunks
            .insert(&chunk)
            .await?;
        Ok(())
    }
}

/// Long-running connector daemon configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorDaemonConfig {
    /// Connector id.
    pub connector_id: ConnectorId,
    /// Connector version written into heartbeat records.
    pub version: String,
    /// Optional current network label.
    pub current_network: Option<String>,
    /// Maximum queued operations executed concurrently by this connector.
    #[serde(default = "default_max_concurrent_operations")]
    pub max_concurrent_operations: usize,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Minimum idle poll delay in milliseconds.
    pub idle_min_delay_ms: u64,
    /// Maximum idle poll delay in milliseconds.
    pub idle_max_delay_ms: u64,
    /// Backoff after infrastructure errors in milliseconds.
    pub error_backoff_ms: u64,
}

impl ConnectorDaemonConfig {
    /// Returns a normalized config with nonzero delays and a valid idle range.
    #[must_use]
    pub fn normalized(mut self) -> Self {
        if self.version.trim().is_empty() {
            "unknown".clone_into(&mut self.version);
        }
        self.max_concurrent_operations = self.max_concurrent_operations.max(1);
        self.heartbeat_interval_ms = self.heartbeat_interval_ms.max(1);
        self.idle_min_delay_ms = self.idle_min_delay_ms.max(1);
        self.idle_max_delay_ms = self.idle_max_delay_ms.max(self.idle_min_delay_ms);
        self.error_backoff_ms = self.error_backoff_ms.max(1);
        self
    }
}

const fn default_max_concurrent_operations() -> usize {
    16
}

/// Daemon stop reason.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorDaemonStopReason {
    /// A shutdown signal was received.
    ShutdownSignal,
}

/// Connector daemon execution summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectorDaemonReport {
    /// Number of access paths upgraded from the historical one-channel default.
    pub upgraded_legacy_access_paths: u64,
    /// Number of connector-local SSH sessions invalidated at connector startup.
    pub reconciled_connection_sessions: u64,
    /// Number of connector-local transport runtimes marked lost at startup.
    pub reconciled_transport_runtimes: u64,
    /// Number of stale active PTY records reconciled at connector startup.
    pub reconciled_pty_sessions: u64,
    /// Number of Agent Sessions whose expired lease was persisted.
    pub reconciled_expired_agent_sessions: u64,
    /// Number of expired logical Workspaces closed without interrupting active work.
    pub reconciled_expired_workspaces: u64,
    /// Number of idle PTY sessions closed automatically by the connector.
    pub reaped_idle_pty_sessions: u64,
    /// Number of zero-channel SSH transports released after their idle TTL.
    pub reaped_idle_transports: u64,
    /// Number of completed worker iterations with claimed operations.
    pub completed_operations: u64,
    /// Number of queued PTY input events delivered by an attached pump.
    pub delivered_pty_inputs: u64,
    /// Number of queued PTY input events marked failed by an attached pump.
    pub failed_pty_inputs: u64,
    /// Number of idle polls where no operation was available.
    pub idle_polls: u64,
    /// Number of infrastructure errors encountered by the loop.
    pub infrastructure_errors: u64,
    /// Stop reason.
    pub stop_reason: ConnectorDaemonStopReason,
}

fn initial_connector_daemon_report() -> ConnectorDaemonReport {
    ConnectorDaemonReport {
        upgraded_legacy_access_paths: 0,
        reconciled_connection_sessions: 0,
        reconciled_transport_runtimes: 0,
        reconciled_pty_sessions: 0,
        reconciled_expired_agent_sessions: 0,
        reconciled_expired_workspaces: 0,
        reaped_idle_pty_sessions: 0,
        reaped_idle_transports: 0,
        completed_operations: 0,
        delivered_pty_inputs: 0,
        failed_pty_inputs: 0,
        idle_polls: 0,
        infrastructure_errors: 0,
        stop_reason: ConnectorDaemonStopReason::ShutdownSignal,
    }
}

/// Connector daemon errors.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorDaemonError {
    /// Database error.
    #[error(transparent)]
    Database(#[from] DbError),
    /// Connector is missing from registry.
    #[error("connector not found: {0}")]
    ConnectorNotFound(ConnectorId),
}

/// Long-running connector daemon with heartbeat, backoff, and graceful shutdown.
pub struct ConnectorDaemon<P> {
    repositories: Repositories,
    worker: Arc<ConnectorOperationWorker<P>>,
    config: ConnectorDaemonConfig,
    pty_input_pump: Option<Arc<dyn QueuedPtyInputPump>>,
    idle_transport_reaper: Option<Arc<dyn IdleTransportReaper>>,
    pty_idle_ttl_seconds: u64,
    pty_busy_ttl_seconds: u64,
}

impl<P> ConnectorDaemon<P>
where
    P: RemoteTransportProvider + 'static,
{
    /// Creates a daemon from repositories, provider, worker config, and daemon config.
    pub fn new(
        repositories: Repositories,
        provider: P,
        worker_config: ConnectorOperationWorkerConfig,
        config: ConnectorDaemonConfig,
    ) -> Self {
        let worker = Arc::new(ConnectorOperationWorker::new(
            repositories.clone(),
            provider,
            worker_config,
        ));
        Self {
            repositories,
            worker,
            config: config.normalized(),
            pty_input_pump: None,
            idle_transport_reaper: None,
            pty_idle_ttl_seconds: DEFAULT_PTY_IDLE_TTL_SECONDS,
            pty_busy_ttl_seconds: DEFAULT_PTY_BUSY_TTL_SECONDS,
        }
    }

    /// Creates a daemon with an explicit output artifact store.
    pub fn with_artifact_store(
        repositories: Repositories,
        provider: P,
        worker_config: ConnectorOperationWorkerConfig,
        config: ConnectorDaemonConfig,
        artifact_store: Arc<dyn OutputArtifactStore>,
    ) -> Self {
        let worker = Arc::new(ConnectorOperationWorker::with_artifact_store(
            repositories.clone(),
            provider,
            worker_config,
            artifact_store,
        ));
        Self {
            repositories,
            worker,
            config: config.normalized(),
            pty_input_pump: None,
            idle_transport_reaper: None,
            pty_idle_ttl_seconds: DEFAULT_PTY_IDLE_TTL_SECONDS,
            pty_busy_ttl_seconds: DEFAULT_PTY_BUSY_TTL_SECONDS,
        }
    }

    /// Attaches a connector-owned PTY input pump to the daemon loop.
    #[must_use]
    pub fn with_pty_input_pump(mut self, pump: Arc<dyn QueuedPtyInputPump>) -> Self {
        self.pty_input_pump = Some(pump);
        self
    }

    /// Attaches connector-local SSH transport cache maintenance.
    #[must_use]
    pub fn with_idle_transport_reaper(mut self, reaper: Arc<dyn IdleTransportReaper>) -> Self {
        self.idle_transport_reaper = Some(reaper);
        self
    }

    /// Overrides PTY idle policy. Zero disables automatic closure for that class.
    #[must_use]
    pub fn with_pty_idle_policy(mut self, idle_ttl_seconds: u64, busy_ttl_seconds: u64) -> Self {
        self.pty_idle_ttl_seconds = idle_ttl_seconds;
        self.pty_busy_ttl_seconds = normalized_busy_ttl(idle_ttl_seconds, busy_ttl_seconds);
        self
    }

    /// Attaches the PTY-aware file transfer path used by interactive bastion routes.
    #[must_use]
    pub fn with_interactive_file_transfer(
        self,
        backend: Arc<dyn InteractiveFileTransferBackend>,
    ) -> Self {
        self.worker.set_interactive_file_transfer(backend);
        self
    }

    /// Runs until the shutdown receiver becomes true or its sender is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error when connector heartbeat persistence fails.
    pub async fn run_until_stopped(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ConnectorDaemonReport, ConnectorDaemonError> {
        let mut report = initial_connector_daemon_report();
        let mut idle_delay = Duration::from_millis(self.config.idle_min_delay_ms);
        let max_idle_delay = Duration::from_millis(self.config.idle_max_delay_ms);
        let heartbeat_interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        let mut next_heartbeat = Instant::now();
        let mut next_claim = Instant::now();
        let mut next_pty_poll = Instant::now();
        let mut accepting_operations = !*shutdown.borrow();
        let mut operations = JoinSet::new();
        let mut pty_polls = JoinSet::new();

        self.reconcile_runtime_startup(&mut report).await?;

        loop {
            if !accepting_operations && operations.is_empty() && pty_polls.is_empty() {
                break;
            }

            let heartbeat_due = tokio::time::sleep_until(next_heartbeat);
            let claim_due = tokio::time::sleep_until(next_claim);
            let pty_poll_due = tokio::time::sleep_until(next_pty_poll);
            tokio::pin!(heartbeat_due);
            tokio::pin!(claim_due);
            tokio::pin!(pty_poll_due);

            tokio::select! {
                changed = shutdown.changed(), if accepting_operations => {
                    accepting_operations = changed.is_ok() && !*shutdown.borrow();
                }
                () = &mut heartbeat_due => {
                    let state = self.reconcile_lifecycle(&mut report).await;
                    self.record_connector_state(state).await?;
                    next_heartbeat = Instant::now() + heartbeat_interval;
                }
                () = &mut claim_due,
                    if accepting_operations
                        && operations.len() < self.config.max_concurrent_operations =>
                {
                    let available = self
                        .config
                        .max_concurrent_operations
                        .saturating_sub(operations.len());
                    for _ in 0..available {
                        let worker = Arc::clone(&self.worker);
                        operations.spawn(async move { worker.run_once().await });
                    }
                    next_claim = Instant::now() + idle_delay;
                }
                joined = operations.join_next(), if !operations.is_empty() => {
                    match joined {
                        Some(Ok(Ok(Some(_outcome)))) => {
                            report.completed_operations += 1;
                            idle_delay = Duration::from_millis(self.config.idle_min_delay_ms);
                            next_claim = Instant::now();
                        }
                        Some(Ok(Ok(None))) => {
                            report.idle_polls += 1;
                            if operations.is_empty() {
                                idle_delay = doubled_duration(idle_delay, max_idle_delay);
                                next_claim = Instant::now() + idle_delay;
                            }
                        }
                        Some(Ok(Err(error))) => {
                            report.infrastructure_errors += 1;
                            tracing::warn!(%error, "connector daemon infrastructure error");
                            self.record_connector_state(EntityState::Degraded).await?;
                            next_claim = Instant::now()
                                + Duration::from_millis(self.config.error_backoff_ms);
                        }
                        Some(Err(error)) => {
                            report.infrastructure_errors += 1;
                            tracing::error!(%error, "connector operation task failed");
                            self.record_connector_state(EntityState::Degraded).await?;
                            next_claim = Instant::now()
                                + Duration::from_millis(self.config.error_backoff_ms);
                        }
                        None => {}
                    }
                }
                () = &mut pty_poll_due,
                    if accepting_operations
                        && self.pty_input_pump.is_some()
                        && pty_polls.is_empty() =>
                {
                    if let Some(pump) = self.pty_input_pump.clone() {
                        pty_polls.spawn(async move { poll_pty_pump(pump.as_ref()).await });
                    }
                }
                joined = pty_polls.join_next(), if !pty_polls.is_empty() => {
                    if let Some(delay) = self
                        .handle_background_pty_join(joined, &mut report)
                        .await?
                    {
                        next_pty_poll = Instant::now() + delay;
                    }
                }
            }
        }

        self.record_connector_state(EntityState::ConnectorOffline)
            .await?;
        Ok(report)
    }

    async fn handle_background_pty_join(
        &self,
        joined: Option<
            Result<Result<Option<PtyPumpOutcome>, ConnectorPtyError>, tokio::task::JoinError>,
        >,
        report: &mut ConnectorDaemonReport,
    ) -> Result<Option<Duration>, ConnectorDaemonError> {
        match joined {
            Some(Ok(result)) => Ok(Some(
                self.handle_background_pty_result(result, report).await?,
            )),
            Some(Err(error)) => {
                report.infrastructure_errors += 1;
                tracing::error!(%error, "connector PTY pump task failed");
                self.record_connector_state(EntityState::Degraded).await?;
                Ok(Some(Duration::from_millis(self.config.error_backoff_ms)))
            }
            None => Ok(None),
        }
    }

    async fn handle_background_pty_result(
        &self,
        result: Result<Option<PtyPumpOutcome>, ConnectorPtyError>,
        report: &mut ConnectorDaemonReport,
    ) -> Result<Duration, ConnectorDaemonError> {
        match result {
            Ok(Some(PtyPumpOutcome::Input(outcome))) => {
                match outcome.state {
                    PtyInputEventState::Delivered => report.delivered_pty_inputs += 1,
                    PtyInputEventState::Failed => report.failed_pty_inputs += 1,
                    PtyInputEventState::Queued | PtyInputEventState::Claimed => {}
                }
                Ok(Duration::from_millis(self.config.idle_min_delay_ms))
            }
            Ok(Some(PtyPumpOutcome::Activated | PtyPumpOutcome::Reconciled) | None) => {
                Ok(Duration::from_millis(self.config.idle_min_delay_ms))
            }
            Err(error) => {
                report.infrastructure_errors += 1;
                tracing::warn!(
                    %error,
                    "connector daemon PTY pump error while operation is running"
                );
                self.record_connector_state(EntityState::Degraded).await?;
                Ok(Duration::from_millis(self.config.error_backoff_ms))
            }
        }
    }

    async fn reconcile_runtime_startup(
        &self,
        report: &mut ConnectorDaemonReport,
    ) -> Result<(), ConnectorDaemonError> {
        let observed_at = now_utc();
        report.upgraded_legacy_access_paths = self
            .repositories
            .access_paths
            .upgrade_legacy_channel_default()
            .await?;
        if report.upgraded_legacy_access_paths > 0 {
            tracing::info!(
                upgraded_access_paths = report.upgraded_legacy_access_paths,
                "upgraded historical SSH channel limits before creating connector transports"
            );
        }
        report.reconciled_connection_sessions = self
            .repositories
            .connection_sessions
            .mark_runtime_lost_for_connector(self.config.connector_id, observed_at)
            .await?;
        report.reconciled_transport_runtimes = self
            .repositories
            .ssh_transport_runtimes
            .mark_runtime_lost_for_connector(self.config.connector_id, observed_at)
            .await?;
        let mut state = EntityState::Healthy;
        if let Some(pump) = &self.pty_input_pump {
            match pump.reconcile_startup().await {
                Ok(count) => report.reconciled_pty_sessions = count,
                Err(error) => {
                    report.infrastructure_errors += 1;
                    tracing::warn!(%error, "connector daemon PTY startup reconciliation failed");
                    state = EntityState::Degraded;
                }
            }
        }
        if self.reconcile_lifecycle(report).await == EntityState::Degraded {
            state = EntityState::Degraded;
        }
        self.record_connector_state(state).await?;
        Ok(())
    }

    async fn reconcile_expired_workspaces(
        &self,
        report: &mut ConnectorDaemonReport,
    ) -> EntityState {
        let mut state = EntityState::Healthy;
        match self
            .repositories
            .agent_sessions
            .reconcile_expired(now_utc(), 1_000)
            .await
        {
            Ok(count) => {
                report.reconciled_expired_agent_sessions = report
                    .reconciled_expired_agent_sessions
                    .saturating_add(count);
                if count > 0 {
                    tracing::info!(
                        reconciled_expired_agent_sessions = count,
                        "persisted expired Agent Session leases"
                    );
                }
            }
            Err(error) => {
                report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                tracing::warn!(
                    %error,
                    "connector daemon Agent Session reconciliation failed"
                );
                state = EntityState::Degraded;
            }
        }
        match self
            .repositories
            .workspaces
            .reconcile_expired(now_utc(), 1_000)
            .await
        {
            Ok(count) => {
                report.reconciled_expired_workspaces =
                    report.reconciled_expired_workspaces.saturating_add(count);
                if count > 0 {
                    tracing::info!(
                        reconciled_expired_workspaces = count,
                        "closed expired logical Workspaces with no active operation or PTY"
                    );
                }
                state
            }
            Err(error) => {
                report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                tracing::warn!(
                    %error,
                    "connector daemon Workspace lifecycle reconciliation failed"
                );
                EntityState::Degraded
            }
        }
    }

    async fn reconcile_lifecycle(&self, report: &mut ConnectorDaemonReport) -> EntityState {
        let mut state = EntityState::Healthy;
        if let Some(pump) = &self.pty_input_pump {
            match pump
                .reap_idle(self.pty_idle_ttl_seconds, self.pty_busy_ttl_seconds)
                .await
            {
                Ok(count) => {
                    report.reaped_idle_pty_sessions =
                        report.reaped_idle_pty_sessions.saturating_add(count);
                    if count > 0 {
                        tracing::info!(
                            reaped_idle_pty_sessions = count,
                            "closed idle PTYs and released their SSH channels"
                        );
                    }
                }
                Err(error) => {
                    report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                    tracing::warn!(%error, "connector daemon PTY idle reconciliation failed");
                    state = EntityState::Degraded;
                }
            }
        }
        if let Some(reaper) = &self.idle_transport_reaper {
            match reaper.reap_idle_transports().await {
                Ok(count) => {
                    report.reaped_idle_transports =
                        report.reaped_idle_transports.saturating_add(count);
                    if count > 0 {
                        tracing::info!(
                            reaped_idle_transports = count,
                            "released zero-channel SSH transports after their idle TTL"
                        );
                    }
                }
                Err(error) => {
                    report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                    tracing::warn!(%error, "connector daemon SSH idle reconciliation failed");
                    state = EntityState::Degraded;
                }
            }
        }
        if self.reconcile_expired_workspaces(report).await == EntityState::Degraded {
            state = EntityState::Degraded;
        }
        state
    }

    async fn record_connector_state(&self, state: EntityState) -> Result<(), ConnectorDaemonError> {
        let observed_at = now_utc();
        let (old_state, connector) = self
            .repositories
            .connectors
            .update_heartbeat(
                self.config.connector_id,
                state,
                Some(&self.config.version),
                self.config.current_network.as_deref(),
                observed_at,
            )
            .await?
            .ok_or(ConnectorDaemonError::ConnectorNotFound(
                self.config.connector_id,
            ))?;
        let outcome = ConnectorStateTracker::record_heartbeat(
            self.config.connector_id,
            old_state,
            connector.state,
            observed_at,
        );
        if let Some(event) = outcome.event {
            self.repositories.state_events.insert(&event).await?;
        }
        Ok(())
    }
}

fn doubled_duration(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

const fn normalized_busy_ttl(idle_ttl_seconds: u64, busy_ttl_seconds: u64) -> u64 {
    if busy_ttl_seconds == 0 {
        0
    } else if busy_ttl_seconds < idle_ttl_seconds {
        idle_ttl_seconds
    } else {
        busy_ttl_seconds
    }
}

/// Request to create a file-backed output artifact.
#[derive(Clone, Debug)]
pub struct OutputArtifactWriteRequest<'a> {
    /// Operation.
    pub operation: &'a OperationRun,
    /// Output stream.
    pub stream: OutputStream,
    /// Redacted output text.
    pub redacted_text: &'a str,
    /// Redacted preview text.
    pub redacted_preview: String,
    /// Whether the original output was truncated before storage.
    pub truncated: bool,
}

/// Output artifact store.
#[async_trait]
pub trait OutputArtifactStore: Send + Sync {
    /// Writes an output artifact and returns its metadata.
    async fn write_artifact(
        &self,
        request: OutputArtifactWriteRequest<'_>,
    ) -> Result<OperationOutputArtifact, ConnectorWorkerError>;

    /// Reads a bounded prefix from an artifact.
    async fn read_artifact_prefix(
        &self,
        artifact: &OperationOutputArtifact,
        max_bytes: usize,
    ) -> Result<String, ConnectorWorkerError>;
}

/// File-backed artifact store rooted in a configured directory.
#[derive(Clone, Debug)]
pub struct FileOutputArtifactStore {
    root: PathBuf,
}

impl FileOutputArtifactStore {
    /// Creates a file artifact store.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn relative_path(
        artifact_id: OperationOutputArtifactId,
        operation: &OperationRun,
        stream: &OutputStream,
    ) -> String {
        format!(
            "{}/{}/{}-{}.log",
            operation.host_id,
            operation.id,
            artifact_id,
            stream_name(stream)
        )
    }

    fn absolute_path(&self, relative_path: &str) -> Result<PathBuf, ConnectorWorkerError> {
        if relative_path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(ConnectorWorkerError::InvalidArtifactPath(
                relative_path.to_owned(),
            ));
        }
        Ok(self.root.join(relative_path))
    }
}

#[async_trait]
impl OutputArtifactStore for FileOutputArtifactStore {
    async fn write_artifact(
        &self,
        request: OutputArtifactWriteRequest<'_>,
    ) -> Result<OperationOutputArtifact, ConnectorWorkerError> {
        let workspace_id = request
            .operation
            .workspace_id
            .ok_or(ConnectorWorkerError::MissingWorkspace)?;
        let artifact_id = OperationOutputArtifactId::new();
        let relative_path = Self::relative_path(artifact_id, request.operation, &request.stream);
        let path = self.absolute_path(&relative_path)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let bytes = request.redacted_text.as_bytes();
        tokio::fs::write(&path, bytes).await?;
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        Ok(OperationOutputArtifact {
            id: artifact_id,
            operation_id: request.operation.id,
            workspace_id,
            stream: request.stream,
            relative_path,
            byte_len: u64::try_from(bytes.len())?,
            sha256,
            redacted_preview: request.redacted_preview,
            truncated: request.truncated,
            created_at: now_utc(),
        })
    }

    async fn read_artifact_prefix(
        &self,
        artifact: &OperationOutputArtifact,
        max_bytes: usize,
    ) -> Result<String, ConnectorWorkerError> {
        let root = canonical_root(&self.root).await?;
        let path = self.absolute_path(&artifact.relative_path)?;
        let canonical_path = tokio::fs::canonicalize(&path).await?;
        if !canonical_path.starts_with(&root) {
            return Err(ConnectorWorkerError::InvalidArtifactPath(
                artifact.relative_path.clone(),
            ));
        }
        let bytes = tokio::fs::read(&canonical_path).await?;
        let limit = max_bytes.min(bytes.len());
        Ok(String::from_utf8_lossy(&bytes[..limit]).to_string())
    }
}

async fn canonical_root(root: &Path) -> Result<PathBuf, ConnectorWorkerError> {
    tokio::fs::create_dir_all(root).await?;
    tokio::fs::canonicalize(root)
        .await
        .map_err(ConnectorWorkerError::from)
}

fn stream_name(stream: &OutputStream) -> &'static str {
    match stream {
        OutputStream::Stdout => "stdout",
        OutputStream::Stderr => "stderr",
        OutputStream::System => "system",
    }
}

fn preview_text(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut preview = text[..end].to_owned();
    preview.push_str("\n<artifact-preview-truncated>");
    preview
}

fn validate_pty_input(input: &str, max_input_bytes: usize) -> Result<(), ConnectorPtyError> {
    if input.is_empty() || input.len() > max_input_bytes || input.as_bytes().contains(&0) {
        return Err(ConnectorPtyError::InvalidInput(max_input_bytes));
    }
    Ok(())
}

async fn shorten_host_write_leases(
    repositories: &Repositories,
    host_id: HostId,
    coordination_scopes: &[String],
    agent_session_id: AgentSessionId,
    observed_at: time::OffsetDateTime,
) -> Result<(), DbError> {
    let mut releasable = Vec::new();
    for coordination_scope in coordination_scopes {
        if !repositories
            .host_write_leases
            .has_pending_write_work(
                host_id,
                agent_session_id,
                std::slice::from_ref(coordination_scope),
            )
            .await?
        {
            releasable.push(coordination_scope.clone());
        }
    }
    if releasable.is_empty() {
        return Ok(());
    }
    repositories
        .host_write_leases
        .shorten_many(
            host_id,
            &releasable,
            agent_session_id,
            observed_at,
            observed_at + time::Duration::seconds(WRITE_LEASE_HANDOFF_GRACE_SECONDS),
        )
        .await?;
    Ok(())
}

async fn record_pty_output_activity(
    repositories: &Repositories,
    pty_session_id: PtySessionId,
    workspace_id: WorkspaceId,
    lease_owner: Option<(HostId, Vec<String>, AgentSessionId)>,
    observed_at: time::OffsetDateTime,
) {
    match repositories
        .pty_sessions
        .touch_activity_if_active(pty_session_id, observed_at)
        .await
    {
        Ok(true) => {
            if let Some((host_id, coordination_scopes, agent_session_id)) = lease_owner
                && let Err(error) = repositories
                    .host_write_leases
                    .renew_many(
                        host_id,
                        &coordination_scopes,
                        agent_session_id,
                        workspace_id,
                        observed_at,
                        observed_at + time::Duration::seconds(PTY_WRITE_LEASE_SECONDS),
                    )
                    .await
            {
                tracing::warn!(
                    %pty_session_id,
                    %workspace_id,
                    %error,
                    "failed to renew scoped host write lease from PTY output activity"
                );
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(
                %pty_session_id,
                %workspace_id,
                %error,
                "failed to persist PTY output activity"
            );
        }
    }
}

async fn pty_lease_owner(
    repositories: &Repositories,
    pty_session_id: PtySessionId,
    workspace_id: WorkspaceId,
) -> Option<(HostId, Vec<String>, AgentSessionId)> {
    let workspace = repositories
        .workspaces
        .get(workspace_id)
        .await
        .ok()
        .flatten()?;
    let pty = repositories
        .pty_sessions
        .get(pty_session_id)
        .await
        .ok()
        .flatten()?;
    workspace.agent_session_id.map(|agent_session_id| {
        let coordination_scopes = if pty.coordination_scopes.is_empty() {
            vec![workspace.coordination_scope]
        } else {
            pty.coordination_scopes
        };
        (workspace.host_id, coordination_scopes, agent_session_id)
    })
}

fn operation_coordination_scopes(operation: &OperationRun) -> Vec<String> {
    if operation.coordination_scopes.is_empty() {
        vec![operation.coordination_scope.clone()]
    } else {
        operation.coordination_scopes.clone()
    }
}

fn pty_coordination_scopes(pty: &PtySession, workspace: &AgentWorkspace) -> Vec<String> {
    if pty.coordination_scopes.is_empty() {
        vec![workspace.coordination_scope.clone()]
    } else {
        pty.coordination_scopes.clone()
    }
}

#[cfg(unix)]
fn shell_start_script(cwd: Option<&str>) -> String {
    cwd.map_or_else(
        || "exec ${SHELL:-sh} -i".to_owned(),
        |cwd| format!("cd {} && exec ${{SHELL:-sh}} -i", shell_quote(cwd)),
    )
}

fn shell_change_dir_input(cwd: Option<&str>) -> Option<String> {
    cwd.map(|cwd| format!("cd -- {}\n", shell_quote(cwd)))
}

fn initial_pty_cwd(
    cwd: Option<String>,
    _host_kind: &HostKind,
    access_path_requires_tty: bool,
) -> Option<String> {
    if access_path_requires_tty {
        return None;
    }
    cwd
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn exhaustion_summary(max_attempts: u32) -> String {
    format!(
        "automatic connector retry budget exhausted after max_attempts={max_attempts}; operation will not be claimed again automatically"
    )
}

fn connection_is_usable_for_workspace(
    connection: &ConnectionSession,
    workspace: &AgentWorkspace,
) -> bool {
    connection.access_path_id == workspace.access_path_id
        && connection.connector_id == workspace.connector_id
        && matches!(
            connection.state,
            EntityState::Resolving | EntityState::Connected | EntityState::Healthy
        )
}

fn classify_connection_failure(message: &str) -> (EntityState, StateReasonCode, Option<u64>) {
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("pooled ssh session was invalidated") {
        return (
            EntityState::Degraded,
            StateReasonCode::PooledTransportInvalidated,
            Some(10),
        );
    }
    if normalized.contains("host key") || normalized.contains("known_hosts") {
        return (
            EntityState::HostKeyChanged,
            StateReasonCode::SshHandshakeFailed,
            None,
        );
    }
    if normalized.contains("permission denied")
        || normalized.contains("authentication")
        || normalized.contains("credential")
    {
        return (
            EntityState::AuthFailed,
            StateReasonCode::SshAuthFailed,
            None,
        );
    }
    if normalized.contains("rate limit")
        || normalized.contains("too many connections")
        || normalized.contains("maxstartups")
    {
        return (
            EntityState::RateLimited,
            StateReasonCode::TargetSshdRateLimited,
            Some(300),
        );
    }
    (
        EntityState::SshHandshakeFailed,
        StateReasonCode::SshHandshakeFailed,
        Some(30),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        io::Write,
        process::Stdio,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use base64::Engine as _;
    use remote_hosts_core::{
        CheckRequest, CheckResult, CommandClass, CommandProfile, CommandProfileCatalog,
        DEFAULT_SFTP_MAX_SIZE_BYTES, DEFAULT_SFTP_TIMEOUT_SECONDS, ExecRequest, ExecResult,
        FileTransferSpec, ForwardHandle, ForwardRequest, OperationCoordinationMode,
        PtySessionOpenCommand, PtySessionSupervisor, RemoteTransport, SftpDirection,
        SftpOverwritePolicy, SftpProgress, SftpRequest, SftpResult, WorkspaceFileTransfer,
        WorkspaceOperationSupervisor, WorkspaceRunCommand, transport::TransportError,
    };
    use remote_hosts_db::{Repositories, connect_sqlite, migrate};
    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId,
        AgentSessionState, AgentWorkspace, AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason,
        AuthorizedKeyBootstrapState, ConnectionMode, ConnectionSession, Connector, ConnectorId,
        CredentialId, CredentialKind, CredentialMetadata, EntityState, Environment, EnvironmentId,
        EnvironmentKind, Host, HostId, HostKind, HostWriteLease, OperationId, OperationRun,
        OperationState, OutputStream, Protocol, PtyBackendCapabilities, PtyBackendState,
        PtyInputEvent, PtyInputEventId, PtyInputEventState, PtyInputPayloadKind, PtyInteraction,
        PtyInteractionKind, PtySessionId, RiskLevel, RouteType, SessionId, SshChannelKind,
        SshConnectionUse, SshFileTransferMode, SshTransportBackend, SshTransportCapabilities,
        SshTransportRuntime, SshTransportRuntimeId, SshTransportRuntimeState,
        SshTransportTelemetry, StateReasonCode, StoredCredential, TrustLevel, WorkspaceId,
        WorkspaceState, now_utc,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::sync::{mpsc, oneshot, watch};

    use super::{
        ConnectorDaemon, ConnectorDaemonConfig, ConnectorOperationWorker,
        ConnectorOperationWorkerConfig, ConnectorPtyInputDeliveryOutcome, ConnectorPtyManager,
        ConnectorPtyManagerConfig, DEFAULT_ARTIFACT_PREVIEW_BYTES,
        DEFAULT_ARTIFACT_THRESHOLD_BYTES, FileOutputArtifactStore, GuardedTransport, HostKeyPolicy,
        ManagedPtyBackend, ManagedPtyProcess, OutputArtifactStore, PtyBackendOutput,
        PtyBackendSpawnRequest, QueuedPtyInputPump, RusshPtyBackendFactory, RusshTransportPool,
        SshCredentialProvider, StaticTransportProvider, TransportTelemetryTracker,
        VERIFIED_NESTED_SUDO_COMMANDS, VaultSshCredentialProvider,
        authorized_key_bootstrap_failure_state, authorized_key_bootstrap_is_eligible,
        execute_authorized_key_install_with_timeout, initial_pty_cwd, russh_inactivity_timeout,
    };
    #[cfg(unix)]
    use super::{
        OpenSshTransport, OpenSshTransportPool, OpenSshTransportProvider, RemoteTransportProvider,
    };
    use remote_hosts_core::ServerProtectionPolicy;
    use remote_hosts_vault::{CredentialSecret, CredentialVault};
    use secrecy::{ExposeSecret, SecretString};

    fn run_test_shell(command: &str) -> std::io::Result<std::process::Output> {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
    }

    #[test]
    fn pty_cwd_is_deferred_for_interactive_jump_host_menu() {
        assert_eq!(
            initial_pty_cwd(Some("/".to_owned()), &HostKind::JumpHost, true,),
            None,
        );
        assert_eq!(
            initial_pty_cwd(Some("/root".to_owned()), &HostKind::Linux, true,),
            None,
        );
        assert_eq!(
            initial_pty_cwd(Some("/srv/app".to_owned()), &HostKind::Linux, false,),
            Some("/srv/app".to_owned()),
        );
    }

    #[test]
    fn pooled_transport_invalidation_is_not_classified_as_tcp_or_handshake_failure() {
        let (state, reason_code, retry_after_seconds) = super::classify_connection_failure(
            "remote POSIX exec did not return its completion frame; the pooled SSH session was invalidated",
        );

        assert_eq!(state, EntityState::Degraded);
        assert_eq!(reason_code, StateReasonCode::PooledTransportInvalidated);
        assert_eq!(retry_after_seconds, Some(10));
    }

    fn exec_transfer_test_spec() -> FileTransferSpec {
        FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: "/tmp/local-payload.bin".to_owned(),
            remote_path: "/tmp/release's payload.bin".to_owned(),
            overwrite: SftpOverwritePolicy::Deny,
            mode: Some(0o600),
            max_size_bytes: 1024,
            expected_sha256: Some("a".repeat(64)),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        }
    }

    #[test]
    fn russh_inactivity_timeout_allows_keepalive_grace() {
        assert_eq!(
            russh_inactivity_timeout(30, 30),
            Some(Duration::from_secs(150))
        );
        assert_eq!(
            russh_inactivity_timeout(600, 30),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            russh_inactivity_timeout(30, 0),
            Some(Duration::from_secs(30))
        );
        assert_eq!(russh_inactivity_timeout(0, 30), None);
    }

    #[tokio::test]
    async fn exec_transfer_stage_timeout_allows_resumable_fallback() {
        let result = super::exec_transfer_stage_with_timeout(
            Duration::from_millis(5),
            std::future::pending::<Result<(), TransportError>>(),
        )
        .await;

        assert!(matches!(result, Err(TransportError::Timeout)));
    }

    #[test]
    fn exec_transfer_retries_only_transient_or_unproven_outcomes() {
        assert!(super::retryable_exec_transfer_error(
            &TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: 1,
            }
        ));
        assert!(super::retryable_exec_transfer_error(
            &TransportError::Backend("connection closed".to_owned())
        ));
        assert!(super::retryable_exec_transfer_error(
            &TransportError::Timeout
        ));
        assert!(super::retryable_exec_transfer_error(
            &TransportError::FileTransfer(
                "verify pooled upload chunk did not return marker".to_owned()
            )
        ));
        assert!(!super::retryable_exec_transfer_error(
            &TransportError::FileTransfer("remote SHA-256 mismatch".to_owned())
        ));
        assert!(!super::retryable_exec_transfer_error(
            &TransportError::PolicyDenied("write denied".to_owned())
        ));
    }

    #[test]
    fn exec_transfer_command_failures_invalidate_the_pooled_session() {
        assert!(super::exec_transfer_error_invalidates_session(
            &TransportError::Backend("channel send failed".to_owned())
        ));
        assert!(super::exec_transfer_error_invalidates_session(
            &TransportError::Timeout
        ));
        assert!(super::exec_transfer_error_invalidates_session(
            &TransportError::FileTransfer(
                "append pooled upload chunk did not return the required completion frame"
                    .to_owned()
            )
        ));
        assert!(!super::exec_transfer_error_invalidates_session(
            &TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: 1,
            }
        ));
        assert!(!super::exec_transfer_error_invalidates_session(
            &TransportError::FileTransfer("remote SHA-256 mismatch".to_owned())
        ));
    }

    #[test]
    fn exec_upload_rotates_long_lived_gateway_sessions_before_channel_exhaustion() {
        assert!(!super::should_rotate_exec_upload_session(
            255,
            255 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
            512 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
        ));
        assert!(super::should_rotate_exec_upload_session(
            256,
            256 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
            512 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
        ));
        assert!(!super::should_rotate_exec_upload_session(
            512,
            512 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
            512 * super::EXEC_UPLOAD_CHUNK_BYTES as u64,
        ));
    }

    #[test]
    fn interactive_pty_upload_uses_latency_bounded_sha_verified_frames() {
        assert_eq!(super::PTY_UPLOAD_CHUNK_BYTES, 16 * 1024);
        const {
            assert!(super::PTY_UPLOAD_CHUNK_BYTES <= super::EXEC_UPLOAD_CHUNK_BYTES);
        }
        assert_eq!(
            super::PTY_TRANSFER_STAGE_TIMEOUT,
            std::time::Duration::from_secs(600)
        );
        assert!(super::PTY_TRANSFER_STAGE_TIMEOUT > super::EXEC_TRANSFER_STAGE_TIMEOUT);

        let artifact_bytes: usize = 11 * 1024 * 1024;
        let exec_frames = artifact_bytes.div_ceil(super::EXEC_UPLOAD_CHUNK_BYTES);
        let pty_frames = artifact_bytes.div_ceil(super::PTY_UPLOAD_CHUNK_BYTES);
        assert!(pty_frames > exec_frames);
    }

    #[test]
    fn bastion_relogin_banner_invalidates_the_pooled_pty_session() {
        assert!(super::russh_pty_output_invalidates_session(
            b"Please re-login.\r\n"
        ));
        assert!(!super::russh_pty_output_invalidates_session(
            b"root@target:~# "
        ));
    }

    #[test]
    fn concurrent_pty_channels_share_one_lifecycle_without_early_invalidation() {
        let mut lifecycle = super::RusshPtyChannelLifecycle::default();

        lifecycle.reserve();
        lifecycle.reserve();

        assert_eq!(lifecycle.active_channels(), 2);
        assert!(!lifecycle.release());
        assert_eq!(lifecycle.active_channels(), 1);
        assert!(lifecycle.release());
        assert_eq!(lifecycle.active_channels(), 0);
    }

    #[test]
    #[should_panic(expected = "released without a matching reservation")]
    fn pty_channel_lifecycle_rejects_unbalanced_release() {
        let mut lifecycle = super::RusshPtyChannelLifecycle::default();

        lifecycle.release();
    }

    #[test]
    fn markerless_gateway_chunk_verification_is_not_success() {
        let result = super::require_exec_upload_chunk_success(
            &ExecResult {
                exit_code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
                truncated: false,
            },
            7,
            7,
            &format!("{:x}", Sha256::digest(b"payload")),
            "/tmp/release.bin",
            "/tmp/.release.bin.part",
        );

        assert!(result.is_err());
    }

    #[test]
    fn interactive_pty_ready_marker_cannot_be_satisfied_by_terminal_echo() {
        let operation_id = OperationId::new();
        let (command, marker) = super::pty_transfer_enter_command(operation_id);

        assert!(!command.contains(&marker));
        assert!(command.contains("stty -echo -icanon"));
        assert!(command.contains("&& printf"));
    }

    #[test]
    fn interactive_pty_restore_marker_requires_success_and_cannot_be_echoed() {
        let operation_id = OperationId::new();
        let (command, marker) = super::pty_transfer_leave_command(operation_id);

        assert!(!command.contains(&marker));
        assert!(command.contains("stty echo icanon"));
        assert!(command.contains("&& printf"));
    }

    #[test]
    fn interactive_pty_transfer_stage_uses_one_terminal_input_line()
    -> Result<(), Box<dyn std::error::Error>> {
        let operation_id = OperationId::new();
        let script = "set -eu\nvalue='payload'\nprintf '%s\\n' \"$value\"";
        let (command, marker) = super::pty_transfer_stage_command(operation_id, "chunk-7", script);

        assert!(command.ends_with('\n'));
        assert_eq!(
            command[..command.len() - 1].matches('\n').count(),
            0,
            "bastion PTYs throttle multiline terminal input"
        );
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .output()?;
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("payload"));
        assert!(stdout.contains(&format!("{marker} 0")));
        Ok(())
    }

    fn run_test_shell_with_input(
        command: &str,
        input: &[u8],
    ) -> std::io::Result<std::process::Output> {
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("child stdin is unavailable"))?
            .write_all(input)?;
        child.wait_with_output()
    }

    struct FakeTransport;

    struct TelemetryFakeTransport {
        tracker: Arc<TransportTelemetryTracker>,
    }

    impl TelemetryFakeTransport {
        fn new() -> Self {
            Self {
                tracker: Arc::new(TransportTelemetryTracker::new(
                    SshTransportBackend::Russh,
                    SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
                )),
            }
        }

        fn mark_channel_use(&self) {
            if self.tracker.snapshot().state == SshTransportRuntimeState::Ready {
                self.tracker.session_reused(now_utc());
            } else {
                self.tracker.connection_attempted();
                self.tracker.handshake_succeeded(now_utc());
            }
        }
    }

    #[test]
    fn transport_telemetry_tracks_handshake_reuse_and_disconnect() {
        let tracker = TransportTelemetryTracker::new(
            SshTransportBackend::Russh,
            SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
        );
        assert_eq!(tracker.snapshot().state, SshTransportRuntimeState::Cold);

        tracker.connection_attempted();
        assert_eq!(
            tracker.snapshot().state,
            SshTransportRuntimeState::Connecting
        );
        tracker.handshake_succeeded(now_utc());
        tracker.session_reused(now_utc());
        let ready = tracker.snapshot();
        assert_eq!(ready.state, SshTransportRuntimeState::Ready);
        assert_eq!(ready.generation, 1);
        assert_eq!(ready.connection_attempt_count, 1);
        assert_eq!(ready.successful_handshake_count, 1);
        assert_eq!(ready.reuse_count, 1);

        tracker.disconnected();
        assert_eq!(
            tracker.snapshot().state,
            SshTransportRuntimeState::Disconnected
        );
    }

    struct OneShotPtyInputPump {
        delivered: AtomicBool,
    }

    struct OneShotIdlePtyReaper {
        reaped: AtomicBool,
    }

    impl OneShotIdlePtyReaper {
        fn new() -> Self {
            Self {
                reaped: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl QueuedPtyInputPump for OneShotIdlePtyReaper {
        async fn reap_idle(
            &self,
            _idle_ttl_seconds: u64,
            _busy_ttl_seconds: u64,
        ) -> Result<u64, super::ConnectorPtyError> {
            Ok(u64::from(!self.reaped.swap(true, Ordering::SeqCst)))
        }

        async fn deliver_next(
            &self,
        ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, super::ConnectorPtyError> {
            Ok(None)
        }
    }

    impl OneShotPtyInputPump {
        fn new() -> Self {
            Self {
                delivered: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl QueuedPtyInputPump for OneShotPtyInputPump {
        async fn deliver_next(
            &self,
        ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, super::ConnectorPtyError> {
            if self.delivered.swap(true, Ordering::SeqCst) {
                return Ok(None);
            }
            Ok(Some(ConnectorPtyInputDeliveryOutcome {
                input_event_id: PtyInputEventId::new(),
                pty_session_id: PtySessionId::new(),
                state: PtyInputEventState::Delivered,
                byte_len: 1,
                error: None,
            }))
        }
    }

    struct DeferredOneShotPtyInputPump {
        ready_at: tokio::time::Instant,
        delivered: AtomicBool,
    }

    struct SlowOneShotPtyActivationPump {
        started: AtomicBool,
        completed: AtomicBool,
    }

    impl SlowOneShotPtyActivationPump {
        fn new() -> Self {
            Self {
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
            }
        }
    }

    impl DeferredOneShotPtyInputPump {
        fn new(delay: Duration) -> Self {
            Self {
                ready_at: tokio::time::Instant::now() + delay,
                delivered: AtomicBool::new(false),
            }
        }

        fn was_delivered(&self) -> bool {
            self.delivered.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl QueuedPtyInputPump for DeferredOneShotPtyInputPump {
        async fn deliver_next(
            &self,
        ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, super::ConnectorPtyError> {
            if tokio::time::Instant::now() < self.ready_at
                || self.delivered.swap(true, Ordering::SeqCst)
            {
                return Ok(None);
            }
            Ok(Some(ConnectorPtyInputDeliveryOutcome {
                input_event_id: PtyInputEventId::new(),
                pty_session_id: PtySessionId::new(),
                state: PtyInputEventState::Delivered,
                byte_len: 1,
                error: None,
            }))
        }
    }

    #[async_trait]
    impl QueuedPtyInputPump for SlowOneShotPtyActivationPump {
        async fn activate_next(&self) -> Result<Option<PtySessionId>, super::ConnectorPtyError> {
            if self.started.swap(true, Ordering::SeqCst) {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.completed.store(true, Ordering::SeqCst);
            Ok(Some(PtySessionId::new()))
        }

        async fn deliver_next(
            &self,
        ) -> Result<Option<ConnectorPtyInputDeliveryOutcome>, super::ConnectorPtyError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl RemoteTransport for FakeTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "password=hunter2\nhello".to_owned(),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            if let Some(progress_tx) = &request.progress_tx {
                let _ = progress_tx.send(SftpProgress {
                    stage: "uploading".to_owned(),
                    bytes_transferred: 128,
                    total_bytes: Some(256),
                    resumed_bytes: 64,
                    retry_count: 1,
                });
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(SftpResult {
                direction: request.spec.direction,
                bytes_transferred: 0,
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_owned(),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            })
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    #[async_trait]
    impl RemoteTransport for TelemetryFakeTransport {
        fn transport_telemetry(&self) -> Option<SshTransportTelemetry> {
            Some(self.tracker.snapshot())
        }

        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            self.mark_channel_use();
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            self.mark_channel_use();
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "telemetry ok".to_owned(),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            self.mark_channel_use();
            Ok(SftpResult {
                direction: request.spec.direction,
                bytes_transferred: 0,
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_owned(),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            })
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    struct SlowTransport(Duration);

    struct StalledProgressTransport;

    #[async_trait]
    impl RemoteTransport for SlowTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            tokio::time::sleep(self.0).await;
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "slow ok".to_owned(),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            Ok(SftpResult {
                direction: request.spec.direction,
                bytes_transferred: 0,
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_owned(),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            })
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    #[async_trait]
    impl RemoteTransport for StalledProgressTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            loop {
                if let Some(progress_tx) = &request.progress_tx {
                    let _ = progress_tx.send(SftpProgress {
                        stage: "initializing".to_owned(),
                        bytes_transferred: 0,
                        total_bytes: Some(1024),
                        resumed_bytes: 0,
                        retry_count: 0,
                    });
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    struct ConcurrencyTrackingTransport {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay: Duration,
    }

    struct InteractiveRoutingTransport {
        fallback_called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl RemoteTransport for InteractiveRoutingTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "ok".to_owned(),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, _request: SftpRequest) -> Result<SftpResult, TransportError> {
            self.fallback_called.store(true, Ordering::SeqCst);
            Err(TransportError::FileTransfer(
                "ordinary transport must not handle interactive transfer".to_owned(),
            ))
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    struct RecordingInteractiveFileTransfer {
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl super::InteractiveFileTransferBackend for RecordingInteractiveFileTransfer {
        async fn transfer_for_workspace(
            &self,
            _workspace_id: WorkspaceId,
            request: SftpRequest,
        ) -> Result<Option<SftpResult>, TransportError> {
            self.called.store(true, Ordering::SeqCst);
            let bytes_transferred = tokio::fs::metadata(&request.spec.local_path)
                .await
                .map_err(|error| TransportError::FileTransfer(error.to_string()))?
                .len();
            Ok(Some(SftpResult {
                direction: request.spec.direction,
                bytes_transferred,
                sha256: request
                    .spec
                    .expected_sha256
                    .clone()
                    .unwrap_or_else(|| "0".repeat(64)),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            }))
        }
    }

    #[async_trait]
    impl RemoteTransport for ConcurrencyTrackingTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "concurrent ok".to_owned(),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            Ok(SftpResult {
                direction: request.spec.direction,
                bytes_transferred: 0,
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_owned(),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            })
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    struct LargeOutputTransport;

    #[async_trait]
    impl RemoteTransport for LargeOutputTransport {
        async fn check(&self, _request: CheckRequest) -> Result<CheckResult, TransportError> {
            Ok(CheckResult {
                ok: true,
                latency_ms: Some(1),
                message: "ok".to_owned(),
            })
        }

        async fn exec(&self, _request: ExecRequest) -> Result<ExecResult, TransportError> {
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: format!("{}\npassword=hunter2\n{}", "A".repeat(128), "B".repeat(128)),
                stderr: String::new(),
                truncated: false,
            })
        }

        async fn sftp(&self, request: SftpRequest) -> Result<SftpResult, TransportError> {
            Ok(SftpResult {
                direction: request.spec.direction,
                bytes_transferred: 0,
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_owned(),
                local_path: request.spec.local_path,
                remote_path: request.spec.remote_path,
                overwrite: request.spec.overwrite,
            })
        }

        async fn open_forward(
            &self,
            _request: ForwardRequest,
        ) -> Result<ForwardHandle, TransportError> {
            Err(TransportError::Backend("not implemented".to_owned()))
        }
    }

    struct UnusedSshCredentialProvider;

    #[async_trait]
    impl SshCredentialProvider for UnusedSshCredentialProvider {
        async fn credential_for(
            &self,
            _access_path_id: AccessPathId,
        ) -> Result<super::SshCredentialSecret, super::SshCredentialError> {
            Err(super::SshCredentialError::NotFound)
        }
    }

    #[derive(Clone)]
    struct CapturingPtyBackend {
        inputs: Arc<tokio::sync::Mutex<Vec<String>>>,
        output_tx: mpsc::Sender<PtyBackendOutput>,
        output_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<PtyBackendOutput>>>>,
    }

    #[derive(Clone)]
    struct LocalScriptPtyBackend {
        inputs: Arc<tokio::sync::Mutex<Vec<String>>>,
        output_tx: mpsc::Sender<PtyBackendOutput>,
        output_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<PtyBackendOutput>>>>,
    }

    struct EndingPtyBackend;

    struct HandshakeBudgetPtyBackend {
        retry_after_seconds: u64,
    }

    struct OneChannelPtyBackend {
        spawn_count: AtomicUsize,
        inputs: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    impl OneChannelPtyBackend {
        fn new() -> Self {
            Self {
                spawn_count: AtomicUsize::new(0),
                inputs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }
    }

    struct TelemetryPtyBackend {
        tracker: Arc<TransportTelemetryTracker>,
    }

    impl TelemetryPtyBackend {
        fn new() -> Self {
            Self {
                tracker: Arc::new(TransportTelemetryTracker::new(
                    SshTransportBackend::Russh,
                    SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
                )),
            }
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for TelemetryPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::russh_native_pty()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            let before = self.tracker.snapshot();
            if before.state == SshTransportRuntimeState::Ready {
                self.tracker.session_reused(now_utc());
            } else {
                self.tracker.connection_attempted();
                self.tracker.handshake_succeeded(now_utc());
            }
            let (input_tx, _input_rx) = mpsc::channel::<String>(1);
            let (_output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(1);
            let (close_tx, _close_rx) = oneshot::channel();
            Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx)
                .with_transport_observation(Some(&before), Some(self.tracker.snapshot())))
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for EndingPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::openssh_pipe_shell()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            let (input_tx, _input_rx) = mpsc::channel::<String>(1);
            let (_output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(1);
            let (close_tx, _close_rx) = oneshot::channel();
            Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx))
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for HandshakeBudgetPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::russh_native_pty()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: self.retry_after_seconds,
            }
            .into())
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for OneChannelPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::russh_native_pty()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            if self.spawn_count.fetch_add(1, Ordering::SeqCst) > 0 {
                return Err(super::ConnectorPtyError::ChannelCapacityUnavailable);
            }
            let (input_tx, mut input_rx) = mpsc::channel::<String>(16);
            let inputs = Arc::clone(&self.inputs);
            tokio::spawn(async move {
                while let Some(input) = input_rx.recv().await {
                    inputs.lock().await.push(input);
                }
            });
            let (output_tx, output_rx) = mpsc::channel::<PtyBackendOutput>(1);
            let (close_tx, close_rx) = oneshot::channel();
            tokio::spawn(async move {
                let _ = close_rx.await;
                drop(output_tx);
            });
            Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx))
        }
    }

    impl CapturingPtyBackend {
        fn new() -> Self {
            let (output_tx, output_rx) = mpsc::channel(16);
            Self {
                inputs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                output_tx,
                output_rx: Arc::new(tokio::sync::Mutex::new(Some(output_rx))),
            }
        }
    }

    impl LocalScriptPtyBackend {
        fn new() -> Self {
            let (output_tx, output_rx) = mpsc::channel(16);
            Self {
                inputs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                output_tx,
                output_rx: Arc::new(tokio::sync::Mutex::new(Some(output_rx))),
            }
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for CapturingPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::openssh_pipe_shell()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            let (input_tx, mut input_rx) = mpsc::channel::<String>(16);
            let inputs = Arc::clone(&self.inputs);
            tokio::spawn(async move {
                while let Some(input) = input_rx.recv().await {
                    inputs.lock().await.push(input);
                }
            });
            let output_rx =
                self.output_rx.lock().await.take().ok_or_else(|| {
                    super::ConnectorPtyError::Backend("already spawned".to_owned())
                })?;
            let (close_tx, _close_rx) = oneshot::channel();
            Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx))
        }
    }

    #[async_trait]
    impl ManagedPtyBackend for LocalScriptPtyBackend {
        fn capabilities(&self) -> PtyBackendCapabilities {
            PtyBackendCapabilities::openssh_pipe_shell()
        }

        async fn spawn(
            &self,
            _request: PtyBackendSpawnRequest,
        ) -> Result<ManagedPtyProcess, super::ConnectorPtyError> {
            let (input_tx, mut input_rx) = mpsc::channel::<String>(16);
            let inputs = Arc::clone(&self.inputs);
            let output_tx = self.output_tx.clone();
            tokio::spawn(async move {
                while let Some(input) = input_rx.recv().await {
                    inputs.lock().await.push(input.clone());
                    let executable = input
                        .replace("stty -echo -icanon min 1 time 0 && ", "")
                        .replace("stty echo icanon && ", "");
                    let output = tokio::process::Command::new("sh")
                        .arg("-c")
                        .arg(executable)
                        .output()
                        .await;
                    let text = match output {
                        Ok(output) => format!(
                            "{}{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr)
                        ),
                        Err(error) => format!("local scripted PTY failed: {error}\n"),
                    };
                    if output_tx
                        .send(PtyBackendOutput {
                            stream: OutputStream::Stdout,
                            text,
                            truncated: false,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let output_rx =
                self.output_rx.lock().await.take().ok_or_else(|| {
                    super::ConnectorPtyError::Backend("already spawned".to_owned())
                })?;
            let (close_tx, _close_rx) = oneshot::channel();
            Ok(ManagedPtyProcess::new(input_tx, output_rx, close_tx))
        }
    }

    fn request(program: &str) -> ExecRequest {
        ExecRequest {
            operation_id: OperationId::new(),
            host_id: HostId::new(),
            access_path_id: AccessPathId::new(),
            profile: CommandProfile {
                name: "check".to_owned(),
                program: program.to_owned(),
                args: Vec::new(),
                class: CommandClass::ReadOnly,
                timeout_seconds: 30,
                output_limit_bytes: 1024,
                requires_tty: false,
            },
        }
    }

    #[test]
    fn russh_exec_command_quotes_structured_arguments() {
        let profile = CommandProfile {
            name: "quote-test".to_owned(),
            program: "printf".to_owned(),
            args: vec!["hello world".to_owned(), "it's ok".to_owned()],
            class: CommandClass::ReadOnly,
            timeout_seconds: 30,
            output_limit_bytes: 1024,
            requires_tty: false,
        };

        assert_eq!(
            super::ssh_exec_command(&profile, false),
            "'printf' 'hello world' 'it'\\''s ok'"
        );
        assert_eq!(
            super::ssh_exec_command(&profile, true),
            "printf \"hello world\" \"it's ok\""
        );

        let whoami = CommandProfile {
            name: "windows-whoami".to_owned(),
            program: "whoami".to_owned(),
            args: Vec::new(),
            class: CommandClass::ReadOnly,
            timeout_seconds: 30,
            output_limit_bytes: 1024,
            requires_tty: false,
        };
        assert_eq!(super::ssh_exec_command(&whoami, true), "whoami");
    }

    #[test]
    fn framed_posix_exec_recovers_status_when_gateway_omits_exit_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let marker = "REMOTE_HOSTS_EXEC_DONE_test";
        let profile = CommandProfile {
            name: "status-test".to_owned(),
            program: "sh".to_owned(),
            args: vec!["-lc".to_owned(), "printf 'before'; exit 17".to_owned()],
            class: CommandClass::ReadOnly,
            timeout_seconds: 30,
            output_limit_bytes: 1024,
            requires_tty: false,
        };
        let output = run_test_shell(&super::framed_posix_ssh_exec_command(&profile, marker))?;
        assert_eq!(output.status.code(), Some(17));

        let mut result = remote_hosts_core::ExecResult {
            exit_code: None,
            stdout: String::from_utf8(output.stdout)?,
            stderr: String::from_utf8(output.stderr)?,
            truncated: false,
        };
        assert!(super::recover_framed_exec_status(&mut result, marker));

        assert_eq!(result.exit_code, Some(17));
        assert_eq!(result.stdout, "before");
        assert!(!result.stdout.contains(marker));
        Ok(())
    }

    #[test]
    fn framed_posix_exec_status_overrides_incorrect_gateway_status() {
        let marker = "REMOTE_HOSTS_EXEC_DONE_override";
        let mut result = remote_hosts_core::ExecResult {
            exit_code: Some(0),
            stdout: format!("output\n\n{marker} 23\n"),
            stderr: String::new(),
            truncated: false,
        };

        assert!(super::recover_framed_exec_status(&mut result, marker));

        assert_eq!(result.exit_code, Some(23));
        assert_eq!(result.stdout, "output\n");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn vault_ssh_credential_provider_decrypts_connector_only_secret()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repositories = Repositories::new(pool);
        let now = now_utc();
        let host = Host {
            id: HostId::new(),
            name: "vault-host".to_owned(),
            display_name: "Vault Host".to_owned(),
            kind: HostKind::Linux,
            owner: None,
            tags: Vec::new(),
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repositories.hosts.insert(&host).await?;
        let environment = Environment {
            id: EnvironmentId::new(),
            name: "vault-env".to_owned(),
            kind: EnvironmentKind::CompanyLan,
            description: None,
            trust_level: TrustLevel::Trusted,
            notes: None,
        };
        repositories.environments.insert(&environment).await?;
        let connector = Connector {
            id: ConnectorId::new(),
            name: "vault-connector".to_owned(),
            environment_id: environment.id,
            host_id: None,
            version: "0.1.0".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("test".to_owned()),
        };
        repositories.connectors.upsert(&connector).await?;
        let master = SecretString::from("unit-test-master".to_owned());
        let blob = CredentialVault::encrypt(
            &master,
            &CredentialSecret {
                password: Some("unit-test-credential".to_owned()),
                private_key_pem: None,
                private_key_passphrase: None,
                sudo_password: None,
                token: None,
                secret_text: None,
                use_ssh_agent: false,
            },
        )?;
        let credential_id = CredentialId::new();
        repositories
            .credentials
            .insert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential_id,
                    name: "vault-credential".to_owned(),
                    kind: CredentialKind::SshPassword,
                    username_hint: Some("ops".to_owned()),
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: serde_json::to_value(blob)?,
            })
            .await?;
        let access_path = AccessPath {
            id: AccessPathId::new(),
            host_id: host.id,
            environment_id: environment.id,
            connector_id: Some(connector.id),
            protocol: Protocol::Ssh,
            address: "10.0.0.42".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repositories.access_paths.insert(&access_path).await?;

        let provider = super::VaultSshCredentialProvider::new(repositories.clone(), master);
        let secret = provider.credential_for(access_path.id).await?;
        assert_eq!(
            secret.password.as_ref().map(SecretString::expose_secret),
            Some("unit-test-credential")
        );
        assert!(secret.private_key_pem.is_none());

        let wrong_provider = super::VaultSshCredentialProvider::new(
            repositories.clone(),
            SecretString::from("wrong-master".to_owned()),
        );
        let Err(error) = wrong_provider.credential_for(access_path.id).await else {
            return Err("wrong master password should not decrypt".into());
        };
        assert!(matches!(error, super::SshCredentialError::VaultLocked));

        repositories
            .credentials
            .upsert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential_id,
                    name: "vault-credential".to_owned(),
                    kind: CredentialKind::SshPrivateKey,
                    username_hint: Some("ops".to_owned()),
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: serde_json::json!({
                    "type": "external_reference",
                    "external_ref": "openssh-agent"
                }),
            })
            .await?;
        let provider = super::VaultSshCredentialProvider::new(
            repositories,
            SecretString::from("master-is-unused-for-agent-reference".to_owned()),
        );
        let secret = provider.credential_for(access_path.id).await?;
        assert!(secret.use_ssh_agent);
        assert!(secret.password.is_none());
        Ok(())
    }

    #[test]
    fn authorized_key_bootstrap_commands_are_idempotent_for_posix_and_windows() {
        let key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest remote-hosts-managed";

        let posix = super::authorized_keys_install_command(key, false);
        assert!(posix.contains("grep -qxF"));
        assert!(posix.contains("chmod 700"));
        assert!(posix.contains("chmod 600"));
        assert_eq!(posix.matches(key).count(), 1);

        let windows = super::authorized_keys_install_command(key, true);
        assert!(windows.contains("powershell.exe"));
        assert!(windows.contains("-notcontains"));
        assert!(windows.contains("Add-Content"));
        assert!(windows.contains("administrators_authorized_keys"));
        assert!(windows.contains("S-1-5-32-544"));
        assert_eq!(windows.matches(key).count(), 1);
    }

    #[test]
    fn authorized_key_bootstrap_suppresses_retries_until_key_or_cooldown_changes() {
        let now = now_utc();
        let access_path_id = AccessPathId::new();
        for state in [
            AuthorizedKeyBootstrapState::Installed,
            AuthorizedKeyBootstrapState::Skipped,
        ] {
            let existing = AuthorizedKeyBootstrap {
                access_path_id,
                state,
                reason: None,
                public_key_fingerprint: Some("SHA256:same".to_owned()),
                failure_count: 1,
                attempted_at: now,
                next_retry_at: None,
                updated_at: now,
            };
            assert!(!authorized_key_bootstrap_is_eligible(
                Some(&existing),
                "SHA256:same",
                now
            ));
            assert!(authorized_key_bootstrap_is_eligible(
                Some(&existing),
                "SHA256:changed",
                now
            ));
        }

        let deferred = AuthorizedKeyBootstrap {
            access_path_id,
            state: AuthorizedKeyBootstrapState::Deferred,
            reason: Some(AuthorizedKeyBootstrapReason::Timeout),
            public_key_fingerprint: Some("SHA256:same".to_owned()),
            failure_count: 1,
            attempted_at: now,
            next_retry_at: Some(now + time::Duration::minutes(5)),
            updated_at: now,
        };
        assert!(!authorized_key_bootstrap_is_eligible(
            Some(&deferred),
            "SHA256:same",
            now
        ));
        assert!(authorized_key_bootstrap_is_eligible(
            Some(&deferred),
            "SHA256:same",
            now + time::Duration::minutes(5)
        ));
    }

    #[test]
    fn authorized_key_bootstrap_caps_transient_failures_and_skips_permanent_failures() {
        let now = now_utc();
        let access_path_id = AccessPathId::new();
        let deferred = authorized_key_bootstrap_failure_state(
            access_path_id,
            "SHA256:test",
            0,
            super::AuthorizedKeyInstallError::Timeout,
            now,
        );
        assert_eq!(deferred.state, AuthorizedKeyBootstrapState::Deferred);
        assert_eq!(deferred.failure_count, 1);
        assert_eq!(deferred.reason, Some(AuthorizedKeyBootstrapReason::Timeout));
        assert!(deferred.next_retry_at.is_some_and(|retry| retry > now));

        let exhausted = authorized_key_bootstrap_failure_state(
            access_path_id,
            "SHA256:test",
            2,
            super::AuthorizedKeyInstallError::RemoteCommandFailed,
            now,
        );
        assert_eq!(exhausted.state, AuthorizedKeyBootstrapState::Skipped);
        assert_eq!(
            exhausted.reason,
            Some(AuthorizedKeyBootstrapReason::AttemptsExhausted)
        );
        assert_eq!(exhausted.failure_count, 3);
        assert_eq!(exhausted.next_retry_at, None);

        let denied = authorized_key_bootstrap_failure_state(
            access_path_id,
            "SHA256:test",
            0,
            super::AuthorizedKeyInstallError::WriteDenied,
            now,
        );
        assert_eq!(denied.state, AuthorizedKeyBootstrapState::Skipped);
        assert_eq!(
            denied.reason,
            Some(AuthorizedKeyBootstrapReason::WriteDenied)
        );
    }

    #[tokio::test]
    async fn authorized_key_bootstrap_has_an_independent_hard_timeout() {
        let result = execute_authorized_key_install_with_timeout(
            Duration::from_millis(10),
            std::future::pending::<Result<ExecResult, TransportError>>(),
        )
        .await;
        assert!(matches!(
            result,
            Err(super::AuthorizedKeyInstallError::Timeout)
        ));
    }

    #[test]
    fn authorized_key_bootstrap_classifies_permanent_remote_failures() {
        assert_eq!(
            super::classify_authorized_key_install_failure("mkdir: Permission denied"),
            super::AuthorizedKeyInstallError::WriteDenied
        );
        assert_eq!(
            super::classify_authorized_key_install_failure("Read-only file system"),
            super::AuthorizedKeyInstallError::ReadOnlyFilesystem
        );
        assert_eq!(
            super::classify_authorized_key_install_failure(
                "powershell.exe is not recognized as an internal or external command"
            ),
            super::AuthorizedKeyInstallError::UnsupportedTargetShell
        );
    }

    #[cfg(unix)]
    #[test]
    fn openssh_destination_uses_uri_with_port() {
        let destination = OpenSshTransport::destination("ops", "10.0.0.10", 2222);
        assert_eq!(destination, "ssh://ops@10.0.0.10:2222");
    }

    #[test]
    fn handshake_limiter_rejects_burst_above_combined_budget() {
        let limiter = super::HandshakeLimiter::new(60, 2);

        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_ok());
        assert!(matches!(
            limiter.try_acquire(),
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds
            }) if retry_after_seconds > 0
        ));
    }

    #[test]
    fn handshake_limiters_share_the_connector_global_budget_across_access_paths() {
        let global = super::HandshakeLimiter::shared_global(1);
        let first = super::HandshakeLimiter::with_shared_global(60, Arc::clone(&global));
        let second = super::HandshakeLimiter::with_shared_global(60, global);

        assert!(first.try_acquire().is_ok());
        assert!(matches!(
            second.try_acquire(),
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds
            }) if retry_after_seconds > 0
        ));
    }

    #[test]
    fn rejected_global_handshake_budget_does_not_consume_the_path_budget() {
        let global = super::HandshakeLimiter::shared_global_for_window(1, Duration::from_secs(1));
        let first = super::HandshakeLimiter::with_shared_global(60, Arc::clone(&global));
        let second = super::HandshakeLimiter::with_shared_global(1, global);
        let now = std::time::Instant::now();

        assert!(first.try_acquire_at(now).is_ok());
        assert!(matches!(
            second.try_acquire_at(now),
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: 1
            })
        ));
        assert!(second.try_acquire_at(now + Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn handshake_retry_delay_rounds_up_to_avoid_an_immediate_second_throttle() {
        let limiter = super::HandshakeLimiter::new(1, 100);
        let now = std::time::Instant::now();

        assert!(limiter.try_acquire_at(now).is_ok());
        assert!(matches!(
            limiter.try_acquire_at(now + Duration::from_millis(4_100)),
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: 56
            })
        ));
    }

    #[test]
    fn per_path_handshake_cooldown_is_not_expanded_to_the_global_window() {
        let limiter = super::HandshakeLimiter::new(1, 100);

        assert!(limiter.try_acquire().is_ok());
        assert!(matches!(
            limiter.try_acquire(),
            Err(TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds
            }) if (1..=60).contains(&retry_after_seconds)
        ));
    }

    #[tokio::test]
    async fn nonzero_exec_result_keeps_the_workspace_reusable()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
        );
        let now = now_utc();
        let operation = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "nonzero-exec-test",
                now,
                now + time::Duration::minutes(1),
                3,
            )
            .await?
            .ok_or("operation should exist")?;
        let profile = CommandProfileCatalog::resolve_builtin(
            "host.uptime",
            Vec::new(),
            &ServerProtectionPolicy::default(),
        )?;

        let outcome = worker
            .persist_exec_result(
                &operation,
                &profile,
                ExecResult {
                    exit_code: Some(72),
                    stdout: String::new(),
                    stderr: "remote command failed".to_owned(),
                    truncated: false,
                },
            )
            .await?;

        assert_eq!(outcome.state, OperationState::Failed);
        assert_eq!(outcome.workspace_state, WorkspaceState::Done);
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Done);
        Ok(())
    }

    #[tokio::test]
    async fn file_transfer_error_keeps_the_workspace_and_active_pty_reusable()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
        );
        let now = now_utc();
        let operation = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "file-transfer-error-test",
                now,
                now + time::Duration::minutes(1),
                3,
            )
            .await?
            .ok_or("operation should exist")?;

        let outcome = worker
            .persist_transport_error(
                &operation,
                TransportError::FileTransfer("remote parent directory is missing".to_owned()),
            )
            .await?;

        assert_eq!(outcome.state, OperationState::Failed);
        assert_eq!(outcome.workspace_state, WorkspaceState::Done);
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Done);
        Ok(())
    }

    #[tokio::test]
    async fn local_handshake_budget_preserves_retry_without_target_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        let mut operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        operation.session_id = Some(fixture.session_id);
        let before = now_utc();
        let error = TransportError::LocalHandshakeBudgetExhausted {
            retry_after_seconds: 164,
        };

        worker
            .record_connection_failure(&operation, &error.to_string(), Some(&error))
            .await?;

        let session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        assert_eq!(session.state, EntityState::Throttled);
        assert_eq!(session.failure_count, 0);
        let health = fixture
            .repositories
            .access_path_health
            .get(operation.access_path_id)
            .await?
            .ok_or("access path health should exist")?;
        assert_eq!(health.state, EntityState::Throttled);
        assert_eq!(
            health.last_error_code,
            Some(StateReasonCode::LocalHandshakeBudgetExhausted)
        );
        let retry_after = health
            .next_retry_at
            .ok_or("local cooldown should have a retry time")?
            - before;
        assert!((164..=165).contains(&retry_after.whole_seconds()));
        assert_eq!(health.failure_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn guarded_transport_redacts_output() -> Result<(), TransportError> {
        let guarded = GuardedTransport::new(FakeTransport, ServerProtectionPolicy::default());
        let result = guarded.exec(request("uname")).await?;
        assert!(!result.stdout.contains("hunter2"));
        assert!(result.stdout.contains("<redacted>"));
        Ok(())
    }

    #[tokio::test]
    async fn guarded_transport_rejects_dangerous_program() {
        let guarded = GuardedTransport::new(FakeTransport, ServerProtectionPolicy::default());
        let result = guarded.exec(request("rm")).await;
        assert!(matches!(result, Err(TransportError::PolicyDenied(_))));
    }

    #[tokio::test]
    async fn file_transfer_local_guards_reject_symlinks_size_and_hash_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source.bin");
        tokio::fs::write(&source, b"deployment-payload").await?;

        let size_error = super::hash_local_source(&source, 4)
            .await
            .err()
            .ok_or("oversized source should be rejected")?;
        assert!(size_error.to_string().contains("max_size_bytes"));

        let actual = super::hash_local_source(&source, 1024).await?.1;
        let spec = FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: source.to_string_lossy().into_owned(),
            remote_path: "/tmp/source.bin".to_owned(),
            overwrite: SftpOverwritePolicy::Deny,
            mode: None,
            max_size_bytes: 1024,
            expected_sha256: Some("0".repeat(64)),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        };
        let hash_error = super::ensure_expected_sha256(&spec, &actual)
            .err()
            .ok_or("mismatched digest should be rejected")?;
        assert!(hash_error.to_string().contains("SHA-256 mismatch"));

        #[cfg(unix)]
        {
            let link = directory.path().join("source-link");
            tokio::fs::symlink(&source, &link).await?;
            let link_error = super::hash_local_source(&link, 1024)
                .await
                .err()
                .ok_or("symlink source should be rejected")?;
            assert!(link_error.to_string().contains("cannot be a symlink"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_no_overwrite_placement_preserves_existing_destination()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let temporary = directory.path().join(".download.part");
        let destination = directory.path().join("download.bin");
        tokio::fs::write(&temporary, b"new").await?;
        tokio::fs::write(&destination, b"existing").await?;

        let error = super::place_local_file(&temporary, &destination, SftpOverwritePolicy::Deny)
            .await
            .err()
            .ok_or("deny policy should preserve existing destination")?;

        assert!(error.to_string().contains("no-overwrite"));
        assert_eq!(tokio::fs::read(&destination).await?, b"existing");
        assert_eq!(tokio::fs::read(&temporary).await?, b"new");
        Ok(())
    }

    #[test]
    fn exec_channel_upload_commands_are_resumable_bounded_and_shell_quoted()
    -> Result<(), Box<dyn std::error::Error>> {
        let spec = exec_transfer_test_spec();
        let initialize = super::russh_exec_upload_initialize_command(
            &spec,
            "/tmp/.release-part",
            17,
            &"a".repeat(64),
        );
        assert!(initialize.contains("REMOTE_HOSTS_UPLOAD_READY"));
        assert!(initialize.contains("REMOTE_HOSTS_UPLOAD_COMPLETE"));
        assert!(initialize.contains("'\\''"));
        assert!(!initialize.contains("local-payload.bin"));
        assert!(initialize.contains("dest_digest"));

        let chunk = super::russh_exec_upload_chunk_command("/tmp/.release-part", 7, 0, b"payload");
        assert!(chunk.contains("REMOTE_HOSTS_CHUNK_OK 7"));
        assert!(chunk.contains("cGF5bG9hZA=="));
        assert!(chunk.contains("elif [ \"$n\" != \"7\" ]"));
        assert!(chunk.contains("wc -c"));
        let maximum_chunk = super::russh_exec_upload_chunk_command(
            "/root/datatool-dev-deploy-20260724/.release.tgz.remote-hosts-ae94167e0388e3c4.part",
            8,
            0,
            &vec![0x5a; super::EXEC_UPLOAD_CHUNK_BYTES],
        );
        assert!(maximum_chunk.len() < 40 * 1024);
        let wrapped_chunk = super::russh_transfer_exec_command(&maximum_chunk);
        assert!(wrapped_chunk.starts_with("'sh' '-lc' '"));
        assert!(wrapped_chunk.contains("'\\''"));
        assert!(wrapped_chunk.len() < 40 * 1024);
        let payload_sha256 = format!("{:x}", Sha256::digest(b"payload"));
        let markerless = super::require_exec_upload_chunk_success(
            &ExecResult {
                exit_code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
                truncated: false,
            },
            7,
            7,
            &payload_sha256,
            &spec.remote_path,
            "/tmp/.release-part",
        );
        assert!(markerless.is_err());
        let missing_marker_and_status = super::require_exec_upload_chunk_success(
            &ExecResult {
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                truncated: false,
            },
            7,
            7,
            &payload_sha256,
            &spec.remote_path,
            "/tmp/.release-part",
        );
        assert!(missing_marker_and_status.is_err());
        super::require_exec_upload_chunk_success(
            &ExecResult {
                exit_code: None,
                stdout: format!(
                    "REMOTE_HOSTS_CHUNK_OK 7 7 {:x}\n",
                    Sha256::digest(b"payload")
                ),
                stderr: String::new(),
                truncated: false,
            },
            7,
            7,
            &payload_sha256,
            &spec.remote_path,
            "/tmp/.release-part",
        )?;
        Ok(())
    }

    #[test]
    fn exec_channel_finalize_and_download_commands_are_verified_and_shell_quoted()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut spec = exec_transfer_test_spec();
        let finalize = super::russh_exec_upload_finalize_command(
            &spec,
            "/tmp/.release-part",
            17,
            &"a".repeat(64),
        );
        assert!(finalize.contains("REMOTE_HOSTS_TRANSFER_OK"));
        assert!(finalize.contains("chmod 600 \"$tmp\""));
        assert!(finalize.contains("'\\''"));
        assert!(!finalize.contains("local-payload.bin"));
        assert!(finalize.contains("[ -f \"$dest\" ]"));

        spec.direction = SftpDirection::Download;
        let download = super::russh_exec_download_command(&spec);
        assert!(download.contains("REMOTE_HOSTS_TRANSFER_META"));
        assert!(download.contains("[ \"$bytes\" -le \"1024\" ]"));
        assert!(download.contains("'\\''"));
        let payload_position = download.find("cat \"$src\"").unwrap_or(usize::MAX);
        let metadata_position = download.find("REMOTE_HOSTS_TRANSFER_META").unwrap_or(0);
        assert!(payload_position < metadata_position);
        let wrapped_download = super::russh_exec_download_request_command(&spec);
        assert!(wrapped_download.starts_with("'sh' '-lc' '"));
        assert!(wrapped_download.contains("REMOTE_HOSTS_TRANSFER_META"));
        assert!(wrapped_download.contains("'\\''"));

        let marker = format!("banner\nREMOTE_HOSTS_TRANSFER_OK 17 {}\n", "a".repeat(64));
        assert_eq!(
            super::parse_transfer_marker(&marker, "REMOTE_HOSTS_TRANSFER_OK")?,
            (17, "a".repeat(64))
        );
        Ok(())
    }

    #[test]
    fn exec_channel_upload_commands_resume_without_duplicate_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("release.bin");
        let temporary = directory.path().join(".release.bin.remote-hosts-test.part");
        let first_chunk = b"deployment-";
        let second_chunk = b"payload";
        let payload = [first_chunk.as_slice(), second_chunk.as_slice()].concat();
        let payload_sha256 = format!("{:x}", Sha256::digest(&payload));
        let empty_sha256 = format!("{:x}", Sha256::digest([]));
        let spec = FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: "/tmp/local-release.bin".to_owned(),
            remote_path: destination.to_string_lossy().into_owned(),
            overwrite: SftpOverwritePolicy::Replace,
            mode: Some(0o600),
            max_size_bytes: 1024,
            expected_sha256: Some(payload_sha256.clone()),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        };
        let temporary = temporary.to_string_lossy().into_owned();

        let initialize = super::russh_exec_upload_initialize_command(
            &spec,
            &temporary,
            u64::try_from(payload.len())?,
            &payload_sha256,
        );
        let initialized = run_test_shell(&initialize)?;
        assert!(initialized.status.success());
        assert!(
            String::from_utf8(initialized.stdout)?
                .contains(&format!("REMOTE_HOSTS_UPLOAD_READY 0 {empty_sha256}"))
        );

        let first_chunk_command =
            super::russh_exec_upload_chunk_command(&temporary, 0, 0, first_chunk);
        let first_append = run_test_shell(&first_chunk_command)?;
        assert!(first_append.status.success());
        assert_eq!(std::fs::read(&temporary)?, first_chunk);

        // A new stateless exec channel observes and verifies the retained prefix.
        let resumed = run_test_shell(&initialize)?;
        assert!(resumed.status.success());
        assert!(String::from_utf8(resumed.stdout)?.contains(&format!(
            "REMOTE_HOSTS_UPLOAD_READY {} {:x}",
            first_chunk.len(),
            Sha256::digest(first_chunk)
        )));

        let second_chunk_command = super::russh_exec_upload_chunk_command(
            &temporary,
            1,
            u64::try_from(first_chunk.len())?,
            second_chunk,
        );
        for _ in 0..2 {
            let appended = run_test_shell(&second_chunk_command)?;
            assert!(
                appended.status.success(),
                "{}",
                String::from_utf8_lossy(&appended.stderr)
            );
            assert!(String::from_utf8(appended.stdout)?.contains(&format!(
                "REMOTE_HOSTS_CHUNK_OK 1 {} {:x}",
                payload.len(),
                Sha256::digest(second_chunk)
            )));
        }
        assert_eq!(std::fs::read(&temporary)?, payload);

        let finalize = super::russh_exec_upload_finalize_command(
            &spec,
            &temporary,
            u64::try_from(payload.len())?,
            &payload_sha256,
        );
        for _ in 0..2 {
            let finalized = run_test_shell(&finalize)?;
            assert!(
                finalized.status.success(),
                "{}",
                String::from_utf8_lossy(&finalized.stderr)
            );
            assert!(String::from_utf8(finalized.stdout)?.contains("REMOTE_HOSTS_TRANSFER_OK"));
        }
        assert_eq!(std::fs::read(destination)?, payload);

        let mut deny_spec = spec.clone();
        deny_spec.overwrite = SftpOverwritePolicy::Deny;
        let recover_after_placement = super::russh_exec_upload_initialize_command(
            &deny_spec,
            &temporary,
            u64::try_from(payload.len())?,
            &payload_sha256,
        );
        let recovered = run_test_shell(&recover_after_placement)?;
        assert!(
            recovered.status.success(),
            "{}",
            String::from_utf8_lossy(&recovered.stderr)
        );
        assert!(String::from_utf8(recovered.stdout)?.contains(&format!(
            "REMOTE_HOSTS_UPLOAD_COMPLETE {} {payload_sha256}",
            payload.len()
        )));
        Ok(())
    }

    #[test]
    fn exec_channel_inline_upload_is_atomic_idempotent_and_verifies_final_artifact()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("release.bin");
        let temporary = directory
            .path()
            .join(".release.bin.remote-hosts-stream.part");
        let payload = b"binary\0deployment\npayload";
        let payload_sha256 = format!("{:x}", Sha256::digest(payload));
        let spec = FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: "/tmp/local-release.bin".to_owned(),
            remote_path: destination.to_string_lossy().into_owned(),
            overwrite: SftpOverwritePolicy::Replace,
            mode: Some(0o600),
            max_size_bytes: 1024,
            expected_sha256: Some(payload_sha256.clone()),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        };
        let command = super::russh_exec_inline_upload_command(
            &spec,
            &temporary.to_string_lossy(),
            u64::try_from(payload.len())?,
            &payload_sha256,
            payload,
        );
        assert!(command.contains("YmluYXJ5AGRlcGxveW1lbnQKcGF5bG9hZA=="));
        assert!(command.contains("REMOTE_HOSTS_TRANSFER_OK"));
        assert!(!command.contains("binary"));
        assert!(super::russh_transfer_exec_command(&command).len() < 2048);

        for _ in 0..2 {
            let output = run_test_shell(&command)?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8(output.stdout)?.contains(&format!(
                "REMOTE_HOSTS_TRANSFER_OK {} {payload_sha256}",
                payload.len()
            )));
        }
        assert_eq!(std::fs::read(destination)?, payload);

        let verification = super::russh_exec_upload_destination_verify_command(
            &spec.remote_path,
            u64::try_from(payload.len())?,
            &payload_sha256,
        );
        let marker = "REMOTE_HOSTS_UPLOAD_VERIFY_DONE_test";
        let verified = run_test_shell(&super::framed_posix_script_ssh_exec_command(
            &verification,
            marker,
        ))?;
        let mut verified = ExecResult {
            exit_code: Some(0),
            stdout: String::from_utf8(verified.stdout)?,
            stderr: String::from_utf8(verified.stderr)?,
            truncated: false,
        };
        assert!(super::recover_framed_exec_status(&mut verified, marker));
        assert_eq!(verified.exit_code, Some(0));
        assert!(verified.stdout.is_empty());

        let mismatched = super::russh_exec_upload_destination_verify_command(
            &spec.remote_path,
            u64::try_from(payload.len())?.saturating_add(1),
            &payload_sha256,
        );
        let mismatched = run_test_shell(&super::framed_posix_script_ssh_exec_command(
            &mismatched,
            marker,
        ))?;
        let mut mismatched = ExecResult {
            exit_code: Some(0),
            stdout: String::from_utf8(mismatched.stdout)?,
            stderr: String::from_utf8(mismatched.stderr)?,
            truncated: false,
        };
        assert!(super::recover_framed_exec_status(&mut mismatched, marker));
        assert_eq!(mismatched.exit_code, Some(76));
        Ok(())
    }

    #[test]
    fn exec_channel_stream_upload_keeps_payload_out_of_command_and_verifies_stdin()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("stream-release.bin");
        let temporary = directory
            .path()
            .join(".stream-release.bin.remote-hosts-test.part");
        let payload = vec![0x5a; 600 * 1024];
        let payload_sha256 = format!("{:x}", Sha256::digest(&payload));
        let spec = FileTransferSpec {
            direction: SftpDirection::Upload,
            local_path: "/tmp/local-stream-release.bin".to_owned(),
            remote_path: destination.to_string_lossy().into_owned(),
            overwrite: SftpOverwritePolicy::Replace,
            mode: Some(0o600),
            max_size_bytes: 1024 * 1024,
            expected_sha256: Some(payload_sha256.clone()),
            timeout_seconds: DEFAULT_SFTP_TIMEOUT_SECONDS,
        };
        let command = super::russh_exec_stream_upload_command(
            &spec,
            &temporary.to_string_lossy(),
            u64::try_from(payload.len())?,
            &payload_sha256,
        );

        assert!(command.contains("cat > \"$tmp\""));
        assert!(command.contains("REMOTE_HOSTS_TRANSFER_OK"));
        assert!(!command.contains("WlpaWlpaWlpaWlpa"));
        assert!(super::russh_transfer_exec_command(&command).len() < 4096);

        let output = run_test_shell_with_input(&command, &payload)?;
        assert!(output.status.success());
        assert!(String::from_utf8(output.stdout)?.contains(&format!(
            "REMOTE_HOSTS_TRANSFER_OK {} {payload_sha256}",
            payload.len()
        )));
        assert_eq!(std::fs::read(destination)?, payload);
        Ok(())
    }

    #[test]
    fn resumable_upload_temporary_path_is_stable_for_the_same_artifact() {
        let destination = "/root/release_artifacts/app.tar.gz";
        let digest = "a".repeat(64);
        let first = super::resumable_remote_temporary_path(destination, &digest);
        let second = super::resumable_remote_temporary_path(destination, &digest);
        let other = super::resumable_remote_temporary_path(destination, &"b".repeat(64));

        assert_eq!(first, second);
        assert_ne!(first, other);
        assert!(first.starts_with("/root/release_artifacts/.app.tar.gz.remote-hosts-"));
        assert!(
            std::path::Path::new(&first)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("part"))
        );
    }

    #[test]
    fn streaming_upload_uses_a_distinct_temporary_path_from_resumable_upload() {
        let destination = "/root/release_artifacts/app.tar.gz";
        let digest = "a".repeat(64);
        let resumable = super::resumable_remote_temporary_path(destination, &digest);
        let streaming = super::streaming_remote_temporary_path(destination, &digest);

        assert_ne!(streaming, resumable);
        assert!(streaming.starts_with("/root/release_artifacts/.app.tar.gz.remote-hosts-stream-"));
        assert!(
            std::path::Path::new(&streaming)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("part"))
        );
    }

    #[test]
    fn local_handshake_cooldown_does_not_consume_a_transfer_stage_attempt() {
        assert!(!super::exec_transfer_retry_consumes_attempt(
            &TransportError::LocalHandshakeBudgetExhausted {
                retry_after_seconds: 17,
            }
        ));
        assert!(super::exec_transfer_retry_consumes_attempt(
            &TransportError::Timeout
        ));
        assert!(super::exec_transfer_retry_consumes_attempt(
            &TransportError::Backend("connection closed".to_owned())
        ));
    }

    #[tokio::test]
    async fn connector_worker_claims_executes_and_redacts_workspace_operation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let provider = StaticTransportProvider::new(FakeTransport);
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );

        let outcome = worker
            .run_once()
            .await?
            .ok_or("worker should claim one operation")?;
        assert_eq!(outcome.operation_id, fixture.operation_id);
        assert_eq!(outcome.state, OperationState::Succeeded);
        assert_eq!(outcome.workspace_state, WorkspaceState::Done);

        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should still exist")?;
        assert_eq!(operation.state, OperationState::Succeeded);
        assert_eq!(operation.exit_code, Some(0));
        assert_eq!(operation.attempt_count, 1);
        assert_eq!(operation.session_id, Some(fixture.session_id));
        assert!(operation.claim_token.is_none());
        assert!(operation.lease_expires_at.is_none());

        let session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        assert_eq!(session.state, EntityState::Connected);
        assert_eq!(session.reused_count, 1);
        assert_eq!(session.failure_count, 0);
        assert!(session.last_error.is_none());

        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Done);

        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(fixture.operation_id), None, 20)
            .await?;
        assert!(
            chunks
                .iter()
                .any(|chunk| chunk.stream == OutputStream::Stdout)
        );
        let joined = chunks
            .iter()
            .map(|chunk| chunk.redacted_text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!joined.contains("hunter2"));
        assert!(joined.contains("<redacted>"));
        assert!(worker.run_once().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_worker_waits_for_foreign_write_lease_then_hands_off_after_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        worker
            .run_once()
            .await?
            .ok_or("fixture read operation should run first")?;

        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let foreign_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "foreign-worker-test".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("foreign-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: now_utc(),
            last_seen_at: now_utc(),
            expires_at: now_utc() + time::Duration::hours(24),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&foreign_session)
            .await?;
        let foreign_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(foreign_session.id),
            label: "foreign-agent".to_owned(),
            ..workspace.clone()
        };
        fixture
            .repositories
            .workspaces
            .insert(&foreign_workspace)
            .await?;
        let lease_at = now_utc();
        fixture
            .repositories
            .host_write_leases
            .try_acquire(
                &HostWriteLease {
                    host_id: fixture.host_id,
                    coordination_scope: "host".to_owned(),
                    holder_agent_session_id: foreign_session.id,
                    holder_workspace_id: foreign_workspace.id,
                    acquired_at: lease_at,
                    heartbeat_at: lease_at,
                    expires_at: lease_at + time::Duration::minutes(5),
                },
                lease_at,
            )
            .await?
            .ok_or("foreign lease should be acquired")?;

        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin(
            "shell.posix",
            vec!["touch /tmp/remote-hosts-write-lease-test".to_owned()],
            &policy,
        )?;
        let plan =
            WorkspaceOperationSupervisor::new(policy).queue_operation(&WorkspaceRunCommand {
                workspace,
                command_profile: profile,
                intent: Some("verify host write lease".to_owned()),
                idempotency_key: Some("write-lease-test".to_owned()),
                coordination_mode: OperationCoordinationMode::Auto,
                coordination_scope: None,
                coordination_scopes: None,
                queued_operations: 0,
                active_exec_channels: 0,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })?;
        fixture
            .repositories
            .operations
            .insert(&plan.operation)
            .await?;
        fixture
            .repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, plan.workspace_state, now_utc())
            .await?;

        assert!(worker.run_once().await?.is_none());
        assert_eq!(
            fixture
                .repositories
                .operations
                .get(plan.operation.id)
                .await?
                .ok_or("queued mutation should exist")?
                .state,
            OperationState::Queued
        );

        let handoff_at = now_utc();
        fixture
            .repositories
            .host_write_leases
            .shorten(
                fixture.host_id,
                "host",
                foreign_session.id,
                handoff_at,
                handoff_at,
            )
            .await?;
        let outcome = worker
            .run_once()
            .await?
            .ok_or("mutation should run after lease handoff")?;
        assert_eq!(outcome.operation_id, plan.operation.id);
        assert_eq!(outcome.state, OperationState::Succeeded);
        let observed_at = now_utc();
        let lease = fixture
            .repositories
            .host_write_leases
            .list_active(fixture.host_id, observed_at)
            .await?
            .into_iter()
            .next()
            .ok_or("completed mutation should retain a short handoff grace")?;
        assert_eq!(lease.holder_agent_session_id, fixture.agent_session_id);
        assert!(lease.expires_at <= observed_at + time::Duration::seconds(16));
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_persists_real_transport_reuse_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let queued = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("queued operation should exist")?;
        let previous_runtime_id = seed_ssh_transport_runtime(
            &fixture.repositories,
            &queued,
            SshTransportRuntimeState::RuntimeLost,
            2,
        )
        .await?;
        let provider = StaticTransportProvider::new(TelemetryFakeTransport::new());
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );

        worker
            .run_once()
            .await?
            .ok_or("first operation should run")?;
        let first = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("first operation should exist")?;
        let first_evidence = first
            .transport_evidence
            .as_ref()
            .ok_or("first operation should expose transport evidence")?;
        assert_eq!(
            first_evidence.connection_use,
            SshConnectionUse::FirstHandshake
        );
        assert!(first_evidence.runtime_replaced);
        assert_ne!(first_evidence.runtime_id, previous_runtime_id);
        assert_eq!(first_evidence.generation, 1);

        let mut second = first.clone();
        second.id = OperationId::new();
        second.state = OperationState::Queued;
        second.started_at = now_utc();
        second.finished_at = None;
        second.exit_code = None;
        second.transport_evidence = None;
        second.redacted_output_summary = Some("queued for reuse verification".to_owned());
        second.attempt_count = 0;
        second.claim_token = None;
        second.claimed_at = None;
        second.lease_expires_at = None;
        second.last_error = None;
        fixture.repositories.operations.insert(&second).await?;

        worker
            .run_once()
            .await?
            .ok_or("second operation should run")?;
        let second = fixture
            .repositories
            .operations
            .get(second.id)
            .await?
            .ok_or("second operation should exist")?;
        let second_evidence = second
            .transport_evidence
            .as_ref()
            .ok_or("second operation should expose transport evidence")?;
        assert_eq!(second_evidence.connection_use, SshConnectionUse::Reused);
        assert_eq!(second_evidence.runtime_id, first_evidence.runtime_id);
        assert_eq!(second_evidence.generation, 1);

        let runtime = fixture
            .repositories
            .ssh_transport_runtimes
            .get(second.access_path_id, second.connector_id)
            .await?
            .ok_or("latest transport runtime should exist")?;
        assert_eq!(runtime.telemetry.reuse_count, 1);
        assert_eq!(runtime.telemetry.state, SshTransportRuntimeState::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_executes_sftp_on_the_existing_logical_session()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        fixture
            .repositories
            .operations
            .update_state(
                fixture.operation_id,
                OperationState::Succeeded,
                Some(now_utc()),
                Some(0),
                Some("superseded by SFTP test"),
            )
            .await?;
        let workspace = fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Idle, now_utc())
            .await?
            .ok_or("workspace should exist")?;
        let plan = WorkspaceOperationSupervisor::default().queue_file_transfer(
            &WorkspaceFileTransfer {
                workspace,
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
            },
        )?;
        fixture
            .repositories
            .operations
            .insert(&plan.operation)
            .await?;
        fixture
            .repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, plan.workspace_state, now_utc())
            .await?;

        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        let outcome = worker
            .run_once()
            .await?
            .ok_or("worker should claim the SFTP operation")?;

        assert_eq!(outcome.operation_id, plan.operation.id);
        assert_eq!(outcome.state, OperationState::Succeeded);
        let operation = fixture
            .repositories
            .operations
            .get(plan.operation.id)
            .await?
            .ok_or("SFTP operation should exist")?;
        assert_eq!(operation.session_id, Some(fixture.session_id));
        assert_eq!(operation.exit_code, Some(0));
        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(plan.operation.id), None, 20)
            .await?;
        assert!(chunks.iter().any(|chunk| {
            chunk.redacted_text.contains("pooled_session=true")
                && chunk.redacted_text.contains("file=manifest.yaml")
        }));
        assert!(chunks.iter().any(|chunk| {
            chunk.redacted_text.contains("file transfer started")
                && chunk.redacted_text.contains("direction=Upload")
        }));
        assert!(chunks.iter().any(|chunk| {
            chunk.redacted_text.contains("stage=uploading")
                && chunk.redacted_text.contains("bytes_transferred=128")
                && chunk.redacted_text.contains("total_bytes=256")
                && chunk.redacted_text.contains("resumed_bytes=64")
                && chunk.redacted_text.contains("retry_count=1")
        }));
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn connector_worker_times_out_stalled_sftp_despite_progress_heartbeats()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        fixture
            .repositories
            .operations
            .update_state(
                fixture.operation_id,
                OperationState::Succeeded,
                Some(now_utc()),
                Some(0),
                Some("superseded by stalled SFTP test"),
            )
            .await?;
        let workspace = fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Idle, now_utc())
            .await?
            .ok_or("workspace should exist")?;
        let plan = WorkspaceOperationSupervisor::default().queue_file_transfer(
            &WorkspaceFileTransfer {
                workspace,
                spec: FileTransferSpec {
                    direction: SftpDirection::Upload,
                    local_path: "/tmp/stalled-upload.bin".to_owned(),
                    remote_path: "/var/tmp/stalled-upload.bin".to_owned(),
                    overwrite: SftpOverwritePolicy::Deny,
                    mode: Some(0o600),
                    max_size_bytes: 1024,
                    expected_sha256: None,
                    timeout_seconds: 1,
                },
                intent: Some("prove stalled transfer deadline".to_owned()),
                idempotency_key: None,
                queued_operations: 0,
                active_exec_channels: 0,
                overload_cooldown_active: false,
            },
        )?;
        fixture
            .repositories
            .operations
            .insert(&plan.operation)
            .await?;
        fixture
            .repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, plan.workspace_state, now_utc())
            .await?;

        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(StalledProgressTransport),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        let outcome = tokio::time::timeout(Duration::from_millis(1_500), worker.run_once())
            .await
            .map_err(|_| "worker ignored the file transfer hard deadline")??
            .ok_or("worker should claim the stalled SFTP operation")?;

        assert_eq!(outcome.operation_id, plan.operation.id);
        assert_eq!(outcome.state, OperationState::TimedOut);
        let operation = fixture
            .repositories
            .operations
            .get(plan.operation.id)
            .await?
            .ok_or("stalled SFTP operation should exist")?;
        assert!(operation.finished_at.is_some());
        assert!(
            operation
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("timed out"))
        );
        let observed_at = now_utc();
        let lease = fixture
            .repositories
            .host_write_leases
            .list_active(fixture.host_id, observed_at)
            .await?
            .into_iter()
            .next()
            .ok_or("timed-out transfer should retain only a short handoff grace")?;
        assert_eq!(lease.holder_agent_session_id, fixture.agent_session_id);
        assert!(lease.expires_at <= observed_at + time::Duration::seconds(16));
        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(plan.operation.id), None, 100)
            .await?;
        assert_eq!(
            chunks
                .iter()
                .filter(|chunk| chunk.redacted_text.contains("file transfer progress:"))
                .count(),
            1,
            "identical zero-byte progress events must be deduplicated"
        );
        assert!(chunks.iter().any(|chunk| {
            chunk
                .redacted_text
                .contains("file transfer deadline reached")
                && chunk.redacted_text.contains("file transfer heartbeat")
                && chunk.redacted_text.contains("no_progress_seconds=")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_does_not_bypass_active_connection_circuit()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: fixture
                    .repositories
                    .operations
                    .get(fixture.operation_id)
                    .await?
                    .ok_or("operation should exist")?
                    .access_path_id,
                state: EntityState::CircuitOpen,
                last_checked_at: Some(now_utc()),
                latency_ms: None,
                failure_count: 3,
                last_error_code: Some(remote_hosts_domain::StateReasonCode::CircuitOpen),
                next_retry_at: Some(now_utc() + time::Duration::minutes(5)),
            })
            .await?;
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(FakeTransport),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );

        let outcome = worker
            .run_once()
            .await?
            .ok_or("worker should reject one operation")?;
        assert_eq!(outcome.state, OperationState::Rejected);
        assert_eq!(outcome.workspace_state, WorkspaceState::Throttled);
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        assert!(operation.session_id.is_none());
        assert!(
            operation
                .last_error
                .as_deref()
                .is_some_and(|message| message.contains("cooldown"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_renews_lease_during_slow_exec()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let provider = StaticTransportProvider::new(SlowTransport(Duration::from_millis(1500)));
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 1,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        let repositories = fixture.repositories.clone();
        let connector_id = fixture.connector_id;
        let operation_id = fixture.operation_id;
        let handle = tokio::spawn(async move { worker.run_once().await });

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let stolen = repositories
            .operations
            .claim_next_for_connector(
                connector_id,
                "steal",
                now_utc(),
                now_utc() + time::Duration::seconds(5),
                3,
            )
            .await?;
        assert!(stolen.is_none());

        let outcome = handle.await??.ok_or("slow operation should finish")?;
        assert_eq!(outcome.operation_id, operation_id);
        assert_eq!(outcome.state, OperationState::Succeeded);
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_marks_expired_max_attempt_operation_exhausted()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let first_claim = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "claim-1",
                now_utc(),
                now_utc() - time::Duration::seconds(1),
                2,
            )
            .await?
            .ok_or("first claim should succeed")?;
        assert_eq!(first_claim.attempt_count, 1);
        let second_claim = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "claim-2",
                now_utc(),
                now_utc() - time::Duration::seconds(1),
                2,
            )
            .await?
            .ok_or("second claim should succeed after expired lease")?;
        assert_eq!(second_claim.attempt_count, 2);

        let provider = StaticTransportProvider::new(FakeTransport);
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 2,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
        );
        let outcome = worker
            .run_once()
            .await?
            .ok_or("worker should exhaust the stale operation")?;
        assert_eq!(outcome.operation_id, fixture.operation_id);
        assert_eq!(outcome.state, OperationState::Exhausted);
        assert_eq!(outcome.workspace_state, WorkspaceState::Blocked);

        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should still exist")?;
        assert_eq!(operation.state, OperationState::Exhausted);
        assert_eq!(operation.attempt_count, 2);
        assert!(operation.claim_token.is_none());
        assert!(operation.lease_expires_at.is_none());
        assert!(operation.finished_at.is_some());
        assert!(
            operation
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("retry budget exhausted")
        );

        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Blocked);

        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(fixture.operation_id), None, 20)
            .await?;
        let joined = chunks
            .iter()
            .map(|chunk| chunk.redacted_text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("automatic connector retry budget exhausted"));
        assert!(joined.contains("recovery_hint="));
        assert!(worker.run_once().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn connector_worker_stores_large_output_as_file_artifact()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let tempdir = tempfile::tempdir()?;
        let provider = StaticTransportProvider::new(LargeOutputTransport);
        let worker = ConnectorOperationWorker::with_artifact_store(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: 64,
                artifact_preview_bytes: 48,
            },
            Arc::new(FileOutputArtifactStore::new(tempdir.path())),
        );

        let outcome = worker
            .run_once()
            .await?
            .ok_or("worker should claim one operation")?;
        assert_eq!(outcome.state, OperationState::Succeeded);

        let artifacts = fixture
            .repositories
            .operation_output_artifacts
            .list_for_workspace(fixture.workspace_id, Some(fixture.operation_id), 10)
            .await?;
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].stream, OutputStream::Stdout);
        assert!(artifacts[0].byte_len > 64);
        assert!(!artifacts[0].redacted_preview.contains("hunter2"));

        let store = FileOutputArtifactStore::new(tempdir.path());
        let content = store.read_artifact_prefix(&artifacts[0], 4096).await?;
        assert!(content.contains("<redacted>"));
        assert!(!content.contains("hunter2"));

        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(fixture.operation_id), None, 20)
            .await?;
        let joined = chunks
            .iter()
            .map(|chunk| chunk.redacted_text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("artifact_id="));
        assert!(!joined.contains("hunter2"));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_persists_transport_runtime_and_channel_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            TelemetryPtyBackend::new(),
            ConnectorPtyManagerConfig {
                connector_id: fixture.connector_id,
                max_input_bytes: 1024,
                output_limit_bytes: 1024,
                input_lease_seconds: 30,
                input_max_attempts: 3,
            },
        );

        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let evidence = opened
            .pty_session
            .transport_evidence
            .as_ref()
            .ok_or("PTY should expose transport evidence")?;
        assert_eq!(evidence.channel_kind, SshChannelKind::Pty);
        assert_eq!(evidence.connection_use, SshConnectionUse::FirstHandshake);
        assert_eq!(evidence.generation, 1);

        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        let runtime = fixture
            .repositories
            .ssh_transport_runtimes
            .get(connection.access_path_id, connection.connector_id)
            .await?
            .ok_or("PTY transport runtime should be persisted")?;
        assert_eq!(runtime.telemetry.runtime_id, evidence.runtime_id);
        assert_eq!(runtime.telemetry.state, SshTransportRuntimeState::Ready);
        manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_reaps_idle_backend_and_preserves_declared_foreground_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            OneChannelPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let channels_before_open = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?
            .open_channels;
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let mut pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("PTY should exist")?;
        pty.foreground_process = Some("long-running-maintenance".to_owned());
        pty.last_activity_at = now_utc() - time::Duration::seconds(120);
        fixture.repositories.pty_sessions.upsert(&pty).await?;

        assert_eq!(manager.reap_idle(60, 3_600).await?, 0);
        assert_eq!(manager.active.lock().await.len(), 1);

        pty.foreground_process = None;
        fixture.repositories.pty_sessions.upsert(&pty).await?;
        let queued_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("idle-reap-race-protection".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: None,
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "queued input".to_owned(),
            byte_len: 5,
            requested_by: Some("test".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&queued_input, "date\n")
            .await?;
        assert_eq!(manager.reap_idle(60, 3_600).await?, 0);
        manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("queued input should be delivered")?;
        pty.last_activity_at = now_utc() - time::Duration::seconds(120);
        fixture.repositories.pty_sessions.upsert(&pty).await?;
        assert_eq!(manager.reap_idle(60, 3_600).await?, 1);
        assert!(manager.active.lock().await.is_empty());
        let closed = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("closed PTY should remain inspectable")?;
        assert_eq!(closed.backend_state, PtyBackendState::Closed);
        assert!(!closed.input_allowed);
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        assert_eq!(connection.open_channels, channels_before_open);
        Ok(())
    }

    #[tokio::test]
    async fn channel_capacity_polling_does_not_keep_pending_pty_alive()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            OneChannelPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let active = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let pending = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        assert_eq!(pending.pty_session.backend_state, PtyBackendState::Pending);

        let stale_activity = now_utc() - time::Duration::seconds(120);
        let mut pending_record = fixture
            .repositories
            .pty_sessions
            .get(pending.pty_session.pty_session_id)
            .await?
            .ok_or("pending PTY should exist")?;
        pending_record.last_activity_at = stale_activity;
        fixture
            .repositories
            .pty_sessions
            .upsert(&pending_record)
            .await?;

        manager.note_channel_capacity_waits().await?;
        let after_poll = fixture
            .repositories
            .pty_sessions
            .get(pending_record.pty_session_id)
            .await?
            .ok_or("pending PTY should remain inspectable")?;
        assert_eq!(after_poll.last_activity_at, stale_activity);
        assert_eq!(manager.reap_idle(60, 3_600).await?, 1);
        assert_eq!(manager.active.lock().await.len(), 1);

        manager
            .close(active.pty_session.pty_session_id, Some(0))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn interactive_prompt_blocks_workspace_without_releasing_pty_channel()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let output_tx = backend.output_tx.clone();
        let inputs = Arc::clone(&backend.inputs);
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;

        output_tx
            .send(PtyBackendOutput {
                stream: OutputStream::Stdout,
                text: "[sudo] password for ops: ".to_owned(),
                truncated: false,
            })
            .await?;
        wait_for_pty_interaction(
            &fixture.repositories,
            opened.pty_session.pty_session_id,
            PtyInteractionKind::SudoPassword,
        )
        .await?;

        let pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("PTY should exist")?;
        assert_eq!(pty.backend_state, PtyBackendState::Active);
        assert!(pty.input_allowed);
        assert_eq!(
            pty.interaction
                .as_ref()
                .map(|interaction| &interaction.kind),
            Some(&PtyInteractionKind::SudoPassword)
        );
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Blocked);
        let access_path = fixture
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let usage = fixture
            .repositories
            .access_paths
            .channel_usage(access_path.id, now_utc())
            .await?;
        assert_eq!(usage.active_ptys, 1);

        manager
            .write_input(
                opened.pty_session.pty_session_id,
                "supplied-input\n".to_owned(),
            )
            .await?;
        assert_eq!(inputs.lock().await.as_slice(), ["supplied-input\n"]);
        let resumed_pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("PTY should exist")?;
        assert!(resumed_pty.interaction.is_none());
        let resumed_workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(resumed_workspace.state, WorkspaceState::Working);
        manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_pty_manager_streams_redacted_output_and_accepts_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        fixture
            .repositories
            .activate_compressed_output_writes()
            .await?;
        let backend = CapturingPtyBackend::new();
        let output_tx = backend.output_tx.clone();
        let inputs = Arc::clone(&backend.inputs);
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig {
                connector_id: fixture.connector_id,
                max_input_bytes: 1024,
                output_limit_bytes: 1024,
                input_lease_seconds: 30,
                input_max_attempts: 3,
            },
        );

        let opened = manager
            .open(
                fixture.workspace_id,
                fixture.session_id,
                Some("/tmp".to_owned()),
            )
            .await?;
        assert_eq!(opened.pty_session.workspace_id, fixture.workspace_id);
        assert!(opened.pty_session.input_allowed);
        assert_eq!(opened.pty_session.backend_state, PtyBackendState::Active);
        assert_eq!(
            opened.pty_session.backend_capabilities,
            PtyBackendCapabilities::openssh_pipe_shell()
        );

        let input = manager
            .write_input(opened.pty_session.pty_session_id, "echo hello\n".to_owned())
            .await?;
        assert_eq!(input.byte_len, "echo hello\n".len());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(inputs.lock().await.as_slice(), ["echo hello\n"]);

        output_tx
            .send(PtyBackendOutput {
                stream: OutputStream::Stdout,
                text: "password=hunter2\nhello\n".to_owned(),
                truncated: false,
            })
            .await?;
        wait_for_pty_output(
            &fixture.repositories,
            opened.pty_session.pty_session_id,
            "<redacted>",
        )
        .await?;

        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(opened.pty_session.pty_session_id, None, 10)
            .await?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].stream, OutputStream::Stdout);
        assert!(!chunks[0].redacted_text.contains("hunter2"));
        assert!(chunks[0].redacted_text.contains("<redacted>"));

        for index in 0..10 {
            output_tx
                .send(PtyBackendOutput {
                    stream: OutputStream::Stdout,
                    text: format!("batched-output-{index}\n"),
                    truncated: false,
                })
                .await?;
        }
        wait_for_pty_output(
            &fixture.repositories,
            opened.pty_session.pty_session_id,
            "batched-output-9",
        )
        .await?;
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(opened.pty_session.pty_session_id, None, 20)
            .await?;
        assert_eq!(chunks.len(), 11);
        let storage = fixture
            .repositories
            .pty_output_chunks
            .storage_stats()
            .await?;
        assert_eq!(storage.compressed_chunks, 11);
        assert!(
            storage.compressed_segments <= 3,
            "rapid output should be persisted in a bounded number of compressed segments"
        );

        let closed = manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        assert_eq!(closed.state, WorkspaceState::Closed);
        assert!(!closed.input_allowed);
        assert_eq!(closed.last_exit_code, Some(0));
        assert!(
            manager
                .write_input(
                    opened.pty_session.pty_session_id,
                    "after close\n".to_owned()
                )
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn interactive_pty_transfer_fails_closed_without_completion_marker()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let output_tx = backend.output_tx.clone();
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let handle = manager
            .interactive_transfer_handle(fixture.workspace_id)
            .await?;
        let error = manager
            .send_pty_command_and_wait(
                &handle,
                "printf payload-without-marker\n".to_owned(),
                "REMOTE_HOSTS_REQUIRED_MARKER",
                Duration::from_millis(30),
            )
            .await
            .err()
            .ok_or("markerless PTY stage must fail")?;

        assert!(error.to_string().contains("did not return marker"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            ["printf payload-without-marker\n"]
        );
        let recovery_marker = "REMOTE_HOSTS_RECOVERY_MARKER";
        let recovery_output_tx = output_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let _ = recovery_output_tx
                .send(PtyBackendOutput {
                    stream: OutputStream::Stdout,
                    text: format!("{recovery_marker}\n"),
                    truncated: false,
                })
                .await;
        });
        let recovered = manager
            .send_pty_command_and_wait(
                &handle,
                "printf recovery-marker\n".to_owned(),
                recovery_marker,
                Duration::from_millis(100),
            )
            .await?;
        assert!(recovered.contains(recovery_marker));
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(opened.pty_session.pty_session_id, None, 10)
            .await?;
        assert!(chunks.is_empty());
        manager
            .close(opened.pty_session.pty_session_id, Some(1))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn interactive_bastion_download_reuses_pty_and_keeps_file_bytes_out_of_audit()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.route_type = RouteType::Bastion;
        access_path.requires_tty = true;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;

        let directory = tempfile::tempdir()?;
        let remote_source = directory.path().join("remote-source.bin");
        let local_destination = directory.path().join("downloaded.bin");
        let payload = (0..(super::PTY_DOWNLOAD_CHUNK_BYTES * 2 + 17))
            .map(|index| u8::try_from(index % 251))
            .collect::<Result<Vec<_>, _>>()?;
        tokio::fs::write(&remote_source, &payload).await?;
        let payload_sha256 = format!("{:x}", Sha256::digest(&payload));

        let backend = LocalScriptPtyBackend::new();
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let result = super::InteractiveFileTransferBackend::transfer_for_workspace(
            &manager,
            fixture.workspace_id,
            SftpRequest {
                operation_id: OperationId::new(),
                host_id: fixture.host_id,
                access_path_id: access_path.id,
                spec: FileTransferSpec {
                    direction: SftpDirection::Download,
                    local_path: local_destination.to_string_lossy().into_owned(),
                    remote_path: remote_source.to_string_lossy().into_owned(),
                    overwrite: SftpOverwritePolicy::Deny,
                    mode: Some(0o600),
                    max_size_bytes: u64::try_from(payload.len())?,
                    expected_sha256: Some(payload_sha256.clone()),
                    timeout_seconds: 30,
                },
                progress_tx: None,
            },
        )
        .await?
        .ok_or("interactive bastion should handle the download")?;

        assert_eq!(result.direction, SftpDirection::Download);
        assert_eq!(result.bytes_transferred, u64::try_from(payload.len())?);
        assert_eq!(result.sha256, payload_sha256);
        assert_eq!(tokio::fs::read(&local_destination).await?, payload);
        let transfer_chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(opened.pty_session.pty_session_id, None, 20)
            .await?;
        assert!(
            transfer_chunks.is_empty(),
            "download frames and file bodies must stay out of persisted PTY output"
        );

        manager
            .write_input(
                opened.pty_session.pty_session_id,
                "printf 'ordinary-output-after-transfer\\n'\n".to_owned(),
            )
            .await?;
        wait_for_pty_output(
            &fixture.repositories,
            opened.pty_session.pty_session_id,
            "ordinary-output-after-transfer",
        )
        .await?;
        manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        Ok(())
    }

    #[test]
    fn interactive_pty_download_parser_rejects_malformed_or_mismatched_frames()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"download-payload";
        let sha256 = format!("{:x}", Sha256::digest(payload));
        let valid = format!(
            "banner\nREMOTE_HOSTS_DOWNLOAD_CHUNK_BEGIN 2 64 {} {sha256}\n{}\nREMOTE_HOSTS_DOWNLOAD_CHUNK_END 2\n",
            payload.len(),
            super::BASE64_STANDARD.encode(payload)
        );
        let parsed = super::parse_interactive_pty_download_chunk(&valid)?;
        assert_eq!(parsed.index, 2);
        assert_eq!(parsed.offset, 64);
        assert_eq!(parsed.payload, payload);

        let malformed = valid.replace("REMOTE_HOSTS_DOWNLOAD_CHUNK_END 2", "BROKEN_END 2");
        assert!(super::parse_interactive_pty_download_chunk(&malformed).is_err());
        let wrong_size = valid.replacen(
            &format!("BEGIN 2 64 {}", payload.len()),
            "BEGIN 2 64 999",
            1,
        );
        assert!(super::parse_interactive_pty_download_chunk(&wrong_size).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn regular_pty_input_waits_for_an_active_interactive_transfer()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let manager = Arc::new(ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        ));
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let handle = manager
            .interactive_transfer_handle(fixture.workspace_id)
            .await?;
        let transfer_guard = handle.transfer_lock.lock().await;
        let manager_for_input = Arc::clone(&manager);
        let input_task = tokio::spawn(async move {
            manager_for_input
                .write_input(
                    opened.pty_session.pty_session_id,
                    "ordinary command\n".to_owned(),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            inputs.lock().await.is_empty(),
            "ordinary PTY input must not interleave with transfer frames"
        );
        drop(transfer_guard);
        input_task.await??;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(inputs.lock().await.as_slice(), ["ordinary command\n"]);
        manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn connector_releases_runtime_handle_after_external_pty_close()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            CapturingPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let pty_session_id = opened.pty_session.pty_session_id;
        let closed = PtySessionSupervisor::default().close(opened.pty_session, Some(0));
        fixture.repositories.pty_sessions.upsert(&closed).await?;

        assert_eq!(manager.reconcile_runtime_state().await?, 1);
        assert!(!manager.active.lock().await.contains_key(&pty_session_id));
        assert!(
            manager
                .write_input(pty_session_id, "after close\n".to_owned())
                .await
                .is_err()
        );
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        assert_eq!(
            connection.open_channels, 1,
            "external PTY close must release the connector channel count"
        );
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_activates_pending_session_before_first_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            CapturingPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );

        let activated = manager.activate_next_pending().await?;

        assert_eq!(activated, Some(pending.pty_session_id));
        let active = fixture
            .repositories
            .pty_sessions
            .get(pending.pty_session_id)
            .await?
            .ok_or("PTY session should exist")?;
        assert_eq!(active.backend_state, PtyBackendState::Active);
        assert_eq!(
            active.backend_capabilities,
            PtyBackendCapabilities::openssh_pipe_shell()
        );
        assert!(manager.activate_next_pending().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn operation_claim_skips_a_saturated_access_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let first_path = fixture
            .repositories
            .access_paths
            .get(
                fixture
                    .repositories
                    .workspaces
                    .get(fixture.workspace_id)
                    .await?
                    .ok_or("workspace should exist")?
                    .access_path_id,
            )
            .await?
            .ok_or("first access path should exist")?;
        let first_workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("first workspace should exist")?;
        let first_connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("first connection should exist")?;
        let mut active_pty = PtySessionSupervisor::default().open_session(
            &first_workspace,
            &first_connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: None,
                coordination_scopes: None,
            },
        )?;
        active_pty.backend_state = PtyBackendState::Active;
        fixture
            .repositories
            .pty_sessions
            .upsert(&active_pty)
            .await?;

        let mut second_path = first_path;
        second_path.id = AccessPathId::new();
        second_path.address = "10.0.0.41".to_owned();
        second_path.priority = 2;
        fixture
            .repositories
            .access_paths
            .insert(&second_path)
            .await?;
        let mut second_workspace = first_workspace;
        second_workspace.id = WorkspaceId::new();
        second_workspace.access_path_id = second_path.id;
        second_workspace.label = "available-path".to_owned();
        fixture
            .repositories
            .workspaces
            .insert(&second_workspace)
            .await?;
        let first_operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("first operation should exist")?;
        let mut second_operation = first_operation;
        second_operation.id = OperationId::new();
        second_operation.access_path_id = second_path.id;
        second_operation.workspace_id = Some(second_workspace.id);
        second_operation.started_at += time::Duration::seconds(1);
        fixture
            .repositories
            .operations
            .insert(&second_operation)
            .await?;

        let claimed = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "capacity-aware-claim",
                now_utc(),
                now_utc() + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or("an operation should be claimable")?;

        assert_eq!(
            claimed.id, second_operation.id,
            "a full path must not occupy a connector worker while another path has capacity"
        );
        assert_eq!(
            fixture
                .repositories
                .operations
                .get(fixture.operation_id)
                .await?
                .ok_or("first operation should remain queued")?
                .state,
            OperationState::Queued
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_pty_selection_skips_a_saturated_access_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let first_workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("first workspace should exist")?;
        let first_path = fixture
            .repositories
            .access_paths
            .get(first_workspace.access_path_id)
            .await?
            .ok_or("first access path should exist")?;
        let first_connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("first connection should exist")?;
        let mut active_pty = PtySessionSupervisor::default().open_session(
            &first_workspace,
            &first_connection,
            0,
            PtySessionOpenCommand {
                session_id: first_connection.session_id,
                cwd: None,
                coordination_scopes: None,
            },
        )?;
        active_pty.backend_state = PtyBackendState::Active;
        fixture
            .repositories
            .pty_sessions
            .upsert(&active_pty)
            .await?;
        let mut blocked_pending = active_pty.clone();
        blocked_pending.pty_session_id = PtySessionId::new();
        blocked_pending.backend_state = PtyBackendState::Pending;
        blocked_pending.created_at -= time::Duration::seconds(1);
        fixture
            .repositories
            .pty_sessions
            .upsert(&blocked_pending)
            .await?;

        let mut second_path = first_path;
        second_path.id = AccessPathId::new();
        second_path.address = "10.0.0.42".to_owned();
        second_path.priority = 2;
        fixture
            .repositories
            .access_paths
            .insert(&second_path)
            .await?;
        let mut second_workspace = first_workspace;
        second_workspace.id = WorkspaceId::new();
        second_workspace.access_path_id = second_path.id;
        second_workspace.label = "available-pty-path".to_owned();
        fixture
            .repositories
            .workspaces
            .insert(&second_workspace)
            .await?;
        let mut second_connection = first_connection;
        second_connection.session_id = SessionId::new();
        second_connection.access_path_id = second_path.id;
        fixture
            .repositories
            .connection_sessions
            .upsert(&second_connection)
            .await?;
        let available_pending = PtySessionSupervisor::default().open_session(
            &second_workspace,
            &second_connection,
            0,
            PtySessionOpenCommand {
                session_id: second_connection.session_id,
                cwd: None,
                coordination_scopes: None,
            },
        )?;
        fixture
            .repositories
            .pty_sessions
            .upsert(&available_pending)
            .await?;

        let selected = fixture
            .repositories
            .pty_sessions
            .next_pending_for_connector(fixture.connector_id)
            .await?
            .ok_or("a pending PTY should be activatable")?;

        assert_eq!(
            selected.pty_session_id, available_pending.pty_session_id,
            "a pending PTY on a full path must not block activation on another path"
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn pty_capacity_wait_preserves_pending_state_and_delivers_active_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(
                fixture
                    .repositories
                    .workspaces
                    .get(fixture.workspace_id)
                    .await?
                    .ok_or("workspace should exist")?
                    .access_path_id,
            )
            .await?
            .ok_or("access path should exist")?;
        access_path.max_concurrent_channels = 2;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let backend = OneChannelPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let active = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?
            .pty_session;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let mut pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: None,
                coordination_scopes: None,
            },
        )?;
        pending.created_at -= time::Duration::seconds(1);
        fixture.repositories.pty_sessions.upsert(&pending).await?;

        let pending_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pending.pty_session_id,
            workspace_id: workspace.id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("pending-input".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: None,
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "pending input".to_owned(),
            byte_len: 8,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc() - time::Duration::seconds(1),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&pending_input, "pending\n")
            .await?;
        let active_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: active.pty_session_id,
            idempotency_key: Some("active-input".to_owned()),
            created_at: now_utc(),
            redacted_input_summary: "active input".to_owned(),
            byte_len: 11,
            ..pending_input.clone()
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&active_input, "echo alive\n")
            .await?;

        let outcome = super::poll_pty_pump(&manager)
            .await?
            .ok_or("active PTY input should be delivered")?;

        assert!(matches!(
            outcome,
            super::PtyPumpOutcome::Input(ConnectorPtyInputDeliveryOutcome {
                input_event_id,
                state: PtyInputEventState::Delivered,
                ..
            }) if input_event_id == active_input.id
        ));
        assert_eq!(
            fixture
                .repositories
                .pty_sessions
                .get(pending.pty_session_id)
                .await?
                .ok_or("pending PTY should exist")?
                .backend_state,
            PtyBackendState::Pending
        );
        assert_eq!(
            fixture
                .repositories
                .connection_sessions
                .get(fixture.session_id)
                .await?
                .ok_or("connection should exist")?
                .state,
            EntityState::Connected
        );
        assert_eq!(
            fixture
                .repositories
                .workspaces
                .get(fixture.workspace_id)
                .await?
                .ok_or("workspace should exist")?
                .state,
            WorkspaceState::Working
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(inputs.lock().await.as_slice(), ["echo alive\n"]);
        assert_eq!(
            fixture
                .repositories
                .pty_input_events
                .get(pending_input.id)
                .await?
                .ok_or("pending input should remain queued")?
                .state,
            PtyInputEventState::Queued
        );
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_explains_channel_capacity_wait_without_remote_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            OneChannelPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );

        manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let pending = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?
            .pty_session;

        assert_eq!(pending.backend_state, PtyBackendState::Pending);
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(pending.pty_session_id, None, 10)
            .await?;
        assert!(chunks.iter().any(|chunk| {
            chunk.stream == OutputStream::System
                && chunk.redacted_text.contains("no free SSH channel")
                && chunk.redacted_text.contains("remote menu has not started")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_explains_database_visible_channel_saturation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.max_concurrent_channels = 1;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;

        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            OneChannelPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: None,
                coordination_scopes: None,
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;

        assert!(manager.activate_next_pending().await?.is_none());
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(pending.pty_session_id, None, 10)
            .await?;
        assert!(chunks.iter().any(|chunk| {
            chunk.stream == OutputStream::System
                && chunk.redacted_text.contains("no free SSH channel")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_releases_active_channel_when_workspace_becomes_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            OneChannelPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Done, now_utc())
            .await?;

        assert_eq!(manager.reconcile_runtime_state().await?, 1);
        let closed = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("PTY session should exist")?;
        assert_eq!(closed.state, WorkspaceState::Closed);
        assert_eq!(closed.backend_state, PtyBackendState::Closed);
        assert!(!closed.input_allowed);
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(closed.pty_session_id, None, 10)
            .await?;
        assert!(chunks.iter().any(|chunk| {
            chunk.stream == OutputStream::System
                && chunk.redacted_text.contains("SSH channel was released")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn expired_running_operation_can_reclaim_its_own_channel_reservation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let now = now_utc();
        let first = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "first-expiring-claim",
                now,
                now + time::Duration::seconds(1),
                3,
            )
            .await?
            .ok_or("first claim should succeed")?;
        assert_eq!(first.id, fixture.operation_id);

        let reclaimed = fixture
            .repositories
            .operations
            .claim_next_for_connector(
                fixture.connector_id,
                "reclaimed-capacity",
                now + time::Duration::seconds(2),
                now + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or("expired operation should be reclaimable")?;

        assert_eq!(reclaimed.id, fixture.operation_id);
        assert_eq!(reclaimed.attempt_count, 2);
        assert_eq!(reclaimed.claim_token.as_deref(), Some("reclaimed-capacity"));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_activation_clears_expired_handshake_throttle()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: connection.access_path_id,
                state: EntityState::Throttled,
                last_checked_at: Some(now_utc() - time::Duration::minutes(5)),
                latency_ms: None,
                failure_count: 0,
                last_error_code: Some(StateReasonCode::LocalHandshakeBudgetExhausted),
                next_retry_at: Some(now_utc() - time::Duration::minutes(1)),
            })
            .await?;
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            CapturingPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );

        assert_eq!(
            manager.activate_next_pending().await?,
            Some(pending.pty_session_id)
        );
        let health = fixture
            .repositories
            .access_path_health
            .get(connection.access_path_id)
            .await?
            .ok_or("access path health should exist")?;
        assert_eq!(health.state, EntityState::Connected);
        assert_eq!(health.failure_count, 0);
        assert_eq!(health.last_error_code, None);
        assert_eq!(health.next_retry_at, None);
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_activation_failure_updates_its_logical_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let mut connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        connection.state = EntityState::Resolving;
        fixture
            .repositories
            .connection_sessions
            .upsert(&connection)
            .await?;
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            HandshakeBudgetPtyBackend {
                retry_after_seconds: 17,
            },
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let before = now_utc();

        assert_eq!(
            manager.activate_next_pending().await?,
            Some(pending.pty_session_id)
        );
        let failed_connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        assert_eq!(failed_connection.state, EntityState::Throttled);
        assert_eq!(failed_connection.failure_count, 0);
        assert!(
            failed_connection
                .last_error
                .as_deref()
                .is_some_and(|message| message.contains("retry_after_seconds=17"))
        );
        let health = fixture
            .repositories
            .access_path_health
            .get(connection.access_path_id)
            .await?
            .ok_or("access path health should exist")?;
        assert_eq!(health.state, EntityState::Throttled);
        assert_eq!(
            health.last_error_code,
            Some(StateReasonCode::LocalHandshakeBudgetExhausted)
        );
        assert_eq!(health.failure_count, 0);
        let retry_after = health
            .next_retry_at
            .ok_or("local cooldown should have a retry time")?
            - before;
        assert!((17..=18).contains(&retry_after.whole_seconds()));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_closes_pending_sessions_for_terminal_workspaces()
    -> Result<(), Box<dyn std::error::Error>> {
        for terminal_state in [
            WorkspaceState::Done,
            WorkspaceState::Failed,
            WorkspaceState::Closed,
        ] {
            let fixture = WorkerFixture::new().await?;
            let workspace = fixture
                .repositories
                .workspaces
                .get(fixture.workspace_id)
                .await?
                .ok_or("workspace should exist")?;
            let connection = fixture
                .repositories
                .connection_sessions
                .get(fixture.session_id)
                .await?
                .ok_or("connection should exist")?;
            let pending = PtySessionSupervisor::default().open_session(
                &workspace,
                &connection,
                0,
                PtySessionOpenCommand {
                    session_id: fixture.session_id,
                    cwd: Some("/tmp".to_owned()),
                    coordination_scopes: None,
                },
            )?;
            fixture.repositories.pty_sessions.upsert(&pending).await?;
            fixture
                .repositories
                .workspaces
                .update_state(fixture.workspace_id, terminal_state, now_utc())
                .await?;
            let manager = ConnectorPtyManager::new(
                fixture.repositories.clone(),
                CapturingPtyBackend::new(),
                ConnectorPtyManagerConfig::production_default(fixture.connector_id),
            );

            assert_eq!(manager.reconcile_runtime_state().await?, 1);
            assert!(manager.activate_next_pending().await?.is_none());
            let closed = fixture
                .repositories
                .pty_sessions
                .get(pending.pty_session_id)
                .await?
                .ok_or("PTY session should exist")?;
            assert_eq!(closed.state, WorkspaceState::Closed);
            assert_eq!(closed.backend_state, PtyBackendState::Closed);
            assert!(!closed.input_allowed);
            let chunks = fixture
                .repositories
                .pty_output_chunks
                .list_for_session(pending.pty_session_id, None, 10)
                .await?;
            assert!(chunks.iter().any(|chunk| {
                chunk.stream == OutputStream::System
                    && chunk
                        .redacted_text
                        .contains("Workspace reached a terminal state")
            }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_terminalizes_unusable_pending_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let mut connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;
        connection.state = EntityState::SshHandshakeFailed;
        fixture
            .repositories
            .connection_sessions
            .upsert(&connection)
            .await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            CapturingPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );

        assert_eq!(
            manager.activate_next_pending().await?,
            Some(pending.pty_session_id)
        );
        assert!(manager.activate_next_pending().await?.is_none());
        let failed = fixture
            .repositories
            .pty_sessions
            .get(pending.pty_session_id)
            .await?
            .ok_or("PTY session should exist")?;
        assert_eq!(failed.state, WorkspaceState::Blocked);
        assert_eq!(failed.backend_state, PtyBackendState::Failed);
        assert!(!failed.input_allowed);
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Blocked);
        let chunks = fixture
            .repositories
            .pty_output_chunks
            .list_for_session(pending.pty_session_id, None, 10)
            .await?;
        assert!(chunks.iter().any(|chunk| {
            chunk.stream == OutputStream::System
                && chunk.redacted_text.contains("automatic retry disabled")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_reconciles_lost_active_runtime_on_startup()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let mut stale = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        stale.state = WorkspaceState::Working;
        stale.backend_state = PtyBackendState::Active;
        stale.backend_capabilities = PtyBackendCapabilities::openssh_pipe_shell();
        fixture.repositories.pty_sessions.upsert(&stale).await?;
        let queued_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: stale.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: workspace.host_id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: Some("startup-reconciliation-input".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some("startup-reconciliation-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "queued input awaiting stale PTY".to_owned(),
            byte_len: 12,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&queued_input, "never send\n")
            .await?;

        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            CapturingPtyBackend::new(),
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let reconciled = manager.reconcile_startup().await?;

        assert_eq!(reconciled, 1);
        let lost = fixture
            .repositories
            .pty_sessions
            .get(stale.pty_session_id)
            .await?
            .ok_or("pty session should exist")?;
        assert_eq!(lost.state, WorkspaceState::Blocked);
        assert_eq!(lost.backend_state, PtyBackendState::Failed);
        assert!(!lost.input_allowed);
        let terminal_input = fixture
            .repositories
            .pty_input_events
            .get(queued_input.id)
            .await?
            .ok_or("queued input should still have public metadata")?;
        assert_eq!(terminal_input.state, PtyInputEventState::Failed);
        assert!(terminal_input.failed_at.is_some());
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        assert_eq!(workspace.state, WorkspaceState::Blocked);
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_marks_backend_exit_in_persistent_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            EndingPtyBackend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let session = fixture
                .repositories
                .pty_sessions
                .get(opened.pty_session.pty_session_id)
                .await?
                .ok_or("pty session should exist")?;
            if session.backend_state != PtyBackendState::Active {
                assert_eq!(session.backend_state, PtyBackendState::Closed);
                assert_eq!(session.state, WorkspaceState::Done);
                assert!(!session.input_allowed);
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("backend exit did not update persistent PTY state".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_claims_and_delivers_queued_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig {
                connector_id: fixture.connector_id,
                max_input_bytes: 1024,
                output_limit_bytes: 1024,
                input_lease_seconds: 30,
                input_max_attempts: 3,
            },
        );
        let opened = manager
            .open(
                fixture.workspace_id,
                fixture.session_id,
                Some("/tmp".to_owned()),
            )
            .await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: opened.pty_session.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: workspace.host_id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: None,
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: None,
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "13 bytes queued for pty input".to_owned(),
            byte_len: 13,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, "queued input\n")
            .await?;

        let outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("queued input should be delivered")?;
        assert_eq!(outcome.input_event_id, event.id);
        assert_eq!(outcome.state, PtyInputEventState::Delivered);
        assert_eq!(outcome.error, None);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(inputs.lock().await.as_slice(), ["queued input\n"]);

        let delivered = fixture
            .repositories
            .pty_input_events
            .get(event.id)
            .await?
            .ok_or("event should exist")?;
        assert_eq!(delivered.state, PtyInputEventState::Delivered);
        assert!(delivered.delivered_at.is_some());
        assert!(manager.deliver_next_queued_input(30, 3).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_injects_stored_sudo_password_without_persisting_the_secret()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let access_path = fixture
            .repositories
            .access_paths
            .get(connection.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let existing_credential = fixture
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await?
            .ok_or("credential should exist")?;
        let master = SecretString::from("connector-sudo-test-master".to_owned());
        let sudo_password = "connector-only-sudo-password";
        let blob = CredentialVault::encrypt(
            &master,
            &CredentialSecret {
                password: Some("ssh-password-not-used-for-sudo".to_owned()),
                private_key_pem: None,
                private_key_passphrase: None,
                sudo_password: Some(sudo_password.to_owned()),
                token: None,
                secret_text: None,
                use_ssh_agent: false,
            },
        )?;
        fixture
            .repositories
            .credentials
            .upsert(&StoredCredential {
                metadata: existing_credential.metadata,
                encrypted_blob_json: serde_json::to_value(blob)?,
            })
            .await?;

        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let sudo_provider: Arc<dyn SshCredentialProvider> = Arc::new(
            VaultSshCredentialProvider::new(fixture.repositories.clone(), master),
        );
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        )
        .with_credential_provider(sudo_provider);
        let opened = manager
            .open(
                fixture.workspace_id,
                fixture.session_id,
                Some("/tmp".to_owned()),
            )
            .await?;
        let now = now_utc();
        let mut pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("opened pty should exist")?;
        pty.state = WorkspaceState::Working;
        pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::SudoPassword,
            confidence: 100,
            observed_at: now,
        });
        fixture.repositories.pty_sessions.upsert(&pty).await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Blocked, now)
            .await?;
        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-sudo-prompt-1".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredSudoPassword,
            input_fingerprint: Some("stored-sudo-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "stored sudo password queued for pty input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now,
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, "")
            .await?;

        let outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("stored sudo event should be delivered")?;
        assert_eq!(outcome.state, PtyInputEventState::Delivered);
        assert_eq!(outcome.byte_len, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [format!("{sudo_password}\n")]
        );

        let delivered = fixture
            .repositories
            .pty_input_events
            .get(event.id)
            .await?
            .ok_or("stored sudo event should remain visible")?;
        let visible = serde_json::to_string(&delivered)?;
        assert_eq!(
            delivered.payload_kind,
            PtyInputPayloadKind::StoredSudoPassword
        );
        assert_eq!(
            delivered.redacted_input_summary,
            event.redacted_input_summary
        );
        assert!(!visible.contains(sudo_password));
        assert!(!visible.contains("ssh-password-not-used-for-sudo"));

        let stale_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-sudo-prompt-stale".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredSudoPassword,
            input_fingerprint: Some("stored-sudo-stale-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 1,
            redacted_input_summary: "stored sudo password queued for pty input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&stale_event, "")
            .await?;
        let stale_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("stale stored sudo event should be terminalized")?;
        assert_eq!(stale_outcome.state, PtyInputEventState::Failed);
        assert_eq!(stale_outcome.byte_len, 0);
        assert!(
            stale_outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("live sudo password prompt"))
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [format!("{sudo_password}\n")]
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_injects_target_host_ssh_password_only_for_a_live_password_prompt()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let access_path = fixture
            .repositories
            .access_paths
            .get(connection.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let existing_credential = fixture
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await?
            .ok_or("credential should exist")?;
        let master = SecretString::from("connector-nested-ssh-test-master".to_owned());
        let ssh_password = "connector-only-nested-ssh-password";
        let target_sudo_password = "connector-only-target-sudo-password";
        let blob = CredentialVault::encrypt(
            &master,
            &CredentialSecret {
                password: Some(ssh_password.to_owned()),
                private_key_pem: None,
                private_key_passphrase: None,
                sudo_password: Some(target_sudo_password.to_owned()),
                token: None,
                secret_text: None,
                use_ssh_agent: false,
            },
        )?;
        fixture
            .repositories
            .credentials
            .upsert(&StoredCredential {
                metadata: existing_credential.metadata,
                encrypted_blob_json: serde_json::to_value(blob)?,
            })
            .await?;

        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let credential_provider: Arc<dyn SshCredentialProvider> = Arc::new(
            VaultSshCredentialProvider::new(fixture.repositories.clone(), master),
        );
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        )
        .with_credential_provider(credential_provider);
        let opened = manager
            .open(
                fixture.workspace_id,
                fixture.session_id,
                Some("/tmp".to_owned()),
            )
            .await?;
        let now = now_utc();
        let mut pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("opened pty should exist")?;
        pty.state = WorkspaceState::Working;
        pty.interaction = None;
        fixture.repositories.pty_sessions.upsert(&pty).await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Blocked, now)
            .await?;
        let verified_ssh_command = format!(
            "/usr/bin/ssh -o StrictHostKeyChecking=yes -o NumberOfPasswordPrompts=1 -p {} {}@{}\n",
            access_path.port, access_path.username, access_path.address
        );
        let command_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("verified-nested-ssh-command".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some(format!(
                "{:x}",
                Sha256::digest(verified_ssh_command.as_bytes())
            )),
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "verified nested SSH command".to_owned(),
            byte_len: u64::try_from(verified_ssh_command.len())?,
            requested_by: Some("agent".to_owned()),
            created_at: now,
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&command_event, &verified_ssh_command)
            .await?;
        let command_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("verified nested SSH command should be delivered")?;
        assert_eq!(command_outcome.state, PtyInputEventState::Delivered);

        let mut prompted_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        prompted_pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::Password,
            confidence: 92,
            observed_at: now_utc(),
        });
        fixture
            .repositories
            .pty_sessions
            .upsert(&prompted_pty)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Blocked, now_utc())
            .await?;
        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-nested-ssh-prompt-1".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredSshPassword,
            input_fingerprint: Some("stored-nested-ssh-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 1,
            redacted_input_summary: "stored SSH password queued for PTY input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now,
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, &access_path.id.to_string())
            .await?;

        let outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("stored SSH event should be delivered")?;
        assert_eq!(outcome.state, PtyInputEventState::Delivered);
        assert_eq!(outcome.byte_len, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [verified_ssh_command.clone(), format!("{ssh_password}\n")]
        );

        let delivered = fixture
            .repositories
            .pty_input_events
            .get(event.id)
            .await?
            .ok_or("stored SSH event should remain visible")?;
        let visible = serde_json::to_string(&delivered)?;
        assert_eq!(
            delivered.payload_kind,
            PtyInputPayloadKind::StoredSshPassword
        );
        assert!(!visible.contains(ssh_password));
        assert!(!visible.contains(&access_path.id.to_string()));

        let mut unbound_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        unbound_pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::Password,
            confidence: 92,
            observed_at: now_utc(),
        });
        fixture
            .repositories
            .pty_sessions
            .upsert(&unbound_pty)
            .await?;

        let unbound_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-nested-ssh-unbound".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredSshPassword,
            input_fingerprint: Some("stored-nested-ssh-unbound-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 2,
            redacted_input_summary: "stored SSH password queued for PTY input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&unbound_event, &access_path.id.to_string())
            .await?;
        let unbound_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("unbound stored SSH event should be terminalized")?;
        assert_eq!(unbound_outcome.state, PtyInputEventState::Failed);
        assert!(
            unbound_outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("verified nested SSH command"))
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [verified_ssh_command.clone(), format!("{ssh_password}\n")]
        );

        let mut stale_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        stale_pty.interaction = None;
        fixture.repositories.pty_sessions.upsert(&stale_pty).await?;
        let stale_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-nested-ssh-stale".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredSshPassword,
            input_fingerprint: Some("stored-nested-ssh-stale-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 3,
            redacted_input_summary: "stored SSH password queued for PTY input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&stale_event, &access_path.id.to_string())
            .await?;
        let stale_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("stale stored SSH event should be terminalized")?;
        assert_eq!(stale_outcome.state, PtyInputEventState::Failed);
        assert!(
            stale_outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("live password prompt"))
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [verified_ssh_command.clone(), format!("{ssh_password}\n")]
        );

        let mut sudo_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        sudo_pty.interaction = None;
        fixture.repositories.pty_sessions.upsert(&sudo_pty).await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Working, now_utc())
            .await?;
        let verified_sudo_command = VERIFIED_NESTED_SUDO_COMMANDS[0].to_owned();
        let sudo_command_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("verified-nested-sudo-command".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some(format!(
                "{:x}",
                Sha256::digest(verified_sudo_command.as_bytes())
            )),
            state: PtyInputEventState::Queued,
            sequence: 4,
            redacted_input_summary: "verified nested sudo command".to_owned(),
            byte_len: u64::try_from(verified_sudo_command.len())?,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&sudo_command_event, &verified_sudo_command)
            .await?;
        let sudo_command_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("verified nested sudo command should be delivered")?;
        assert_eq!(sudo_command_outcome.state, PtyInputEventState::Delivered);

        let mut prompted_sudo_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        prompted_sudo_pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::SudoPassword,
            confidence: 100,
            observed_at: now_utc(),
        });
        fixture
            .repositories
            .pty_sessions
            .upsert(&prompted_sudo_pty)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Blocked, now_utc())
            .await?;
        let target_sudo_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-target-sudo-prompt-1".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredTargetSudoPassword,
            input_fingerprint: Some("stored-target-sudo-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 5,
            redacted_input_summary: "target stored sudo password queued for PTY input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&target_sudo_event, &access_path.id.to_string())
            .await?;
        let target_sudo_outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("target stored sudo event should be delivered")?;
        assert_eq!(target_sudo_outcome.state, PtyInputEventState::Delivered);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [
                verified_ssh_command.clone(),
                format!("{ssh_password}\n"),
                verified_sudo_command.clone(),
                format!("{target_sudo_password}\n")
            ]
        );
        let visible = serde_json::to_string(
            &fixture
                .repositories
                .pty_input_events
                .get(target_sudo_event.id)
                .await?
                .ok_or("target sudo event should remain visible")?,
        )?;
        assert!(!visible.contains(target_sudo_password));
        assert!(!visible.contains(&access_path.id.to_string()));

        let mut denied_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        denied_pty.interaction = None;
        fixture
            .repositories
            .pty_sessions
            .upsert(&denied_pty)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Working, now_utc())
            .await?;
        let disallowed_sudo_command =
            "/usr/bin/sudo -S -p '[sudo] password for %u: ' -- /usr/bin/id\n".to_owned();
        let disallowed_command_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("disallowed-nested-sudo-command".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some(format!(
                "{:x}",
                Sha256::digest(disallowed_sudo_command.as_bytes())
            )),
            state: PtyInputEventState::Queued,
            sequence: 6,
            redacted_input_summary: "disallowed nested sudo command".to_owned(),
            byte_len: u64::try_from(disallowed_sudo_command.len())?,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&disallowed_command_event, &disallowed_sudo_command)
            .await?;
        manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("disallowed command text should still be delivered as ordinary PTY input")?;
        let mut fake_prompt_pty = fixture
            .repositories
            .pty_sessions
            .get(pty.pty_session_id)
            .await?
            .ok_or("PTY should still exist")?;
        fake_prompt_pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::SudoPassword,
            confidence: 100,
            observed_at: now_utc(),
        });
        fixture
            .repositories
            .pty_sessions
            .upsert(&fake_prompt_pty)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, WorkspaceState::Blocked, now_utc())
            .await?;
        let denied_target_sudo_event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: fixture.host_id,
            agent_session_id: Some(fixture.agent_session_id),
            idempotency_key: Some("stored-target-sudo-denied".to_owned()),
            payload_kind: PtyInputPayloadKind::StoredTargetSudoPassword,
            input_fingerprint: Some("stored-target-sudo-denied-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 7,
            redacted_input_summary: "target stored sudo password queued for PTY input".to_owned(),
            byte_len: 0,
            requested_by: None,
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&denied_target_sudo_event, &access_path.id.to_string())
            .await?;
        let denied = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("disallowed target sudo request should be terminalized")?;
        assert_eq!(denied.state, PtyInputEventState::Failed);
        assert!(
            denied
                .error
                .as_deref()
                .is_some_and(|error| error.contains("verified nested sudo command"))
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            inputs.lock().await.as_slice(),
            [
                verified_ssh_command,
                format!("{ssh_password}\n"),
                verified_sudo_command,
                format!("{target_sudo_password}\n"),
                disallowed_sudo_command,
            ]
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_pty_write_lease_blocks_foreign_session_until_close_or_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let output_tx = backend.output_tx.clone();
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let opened = manager
            .open(fixture.workspace_id, fixture.session_id, None)
            .await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: opened.pty_session.pty_session_id,
            workspace_id: workspace.id,
            connector_id: workspace.connector_id,
            host_id: workspace.host_id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: Some("interactive-deploy-step".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some("test-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "18 bytes queued for pty input".to_owned(),
            byte_len: 18,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, "run deployment\n")
            .await?;
        manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("PTY input should be delivered")?;

        let observed_after_input = now_utc();
        let initial_lease = fixture
            .repositories
            .host_write_leases
            .list_active(workspace.host_id, observed_after_input)
            .await?
            .into_iter()
            .next()
            .ok_or("PTY input should retain a write lease")?;
        assert_eq!(
            initial_lease.holder_agent_session_id,
            fixture.agent_session_id
        );
        assert!(initial_lease.expires_at >= observed_after_input + time::Duration::seconds(250));

        let foreign_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "foreign-pty-session".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("foreign-pty-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: observed_after_input,
            last_seen_at: observed_after_input,
            expires_at: observed_after_input + time::Duration::hours(24),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&foreign_session)
            .await?;
        let foreign_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(foreign_session.id),
            label: "foreign-pty-agent".to_owned(),
            ..workspace.clone()
        };
        fixture
            .repositories
            .workspaces
            .insert(&foreign_workspace)
            .await?;
        assert!(
            fixture
                .repositories
                .host_write_leases
                .try_acquire(
                    &HostWriteLease {
                        host_id: workspace.host_id,
                        coordination_scope: workspace.coordination_scope.clone(),
                        holder_agent_session_id: foreign_session.id,
                        holder_workspace_id: foreign_workspace.id,
                        acquired_at: observed_after_input,
                        heartbeat_at: observed_after_input,
                        expires_at: observed_after_input + time::Duration::minutes(5),
                    },
                    observed_after_input,
                )
                .await?
                .is_none()
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        output_tx
            .send(PtyBackendOutput {
                stream: OutputStream::Stdout,
                text: "deployment still running\n".to_owned(),
                truncated: false,
            })
            .await?;
        wait_for_pty_output(
            &fixture.repositories,
            opened.pty_session.pty_session_id,
            "deployment still running",
        )
        .await?;
        let renewed_lease = fixture
            .repositories
            .host_write_leases
            .list_active(workspace.host_id, now_utc())
            .await?
            .into_iter()
            .next()
            .ok_or("PTY output should keep the write lease active")?;
        assert!(renewed_lease.heartbeat_at > initial_lease.heartbeat_at);
        assert!(renewed_lease.expires_at > initial_lease.expires_at);
        let active_pty = fixture
            .repositories
            .pty_sessions
            .get(opened.pty_session.pty_session_id)
            .await?
            .ok_or("active PTY should still exist")?;
        assert!(active_pty.last_activity_at >= renewed_lease.heartbeat_at);

        manager
            .close(opened.pty_session.pty_session_id, Some(0))
            .await?;
        let observed_after_close = now_utc();
        let closing_lease = fixture
            .repositories
            .host_write_leases
            .list_active(workspace.host_id, observed_after_close)
            .await?
            .into_iter()
            .next()
            .ok_or("closed PTY should retain only a short handoff grace")?;
        assert!(
            closing_lease.expires_at
                <= observed_after_close
                    + time::Duration::seconds(super::WRITE_LEASE_HANDOFF_GRACE_SECONDS + 1)
        );

        let takeover_at = closing_lease.expires_at + time::Duration::milliseconds(1);
        let takeover = fixture
            .repositories
            .host_write_leases
            .try_acquire(
                &HostWriteLease {
                    host_id: workspace.host_id,
                    coordination_scope: workspace.coordination_scope.clone(),
                    holder_agent_session_id: foreign_session.id,
                    holder_workspace_id: foreign_workspace.id,
                    acquired_at: takeover_at,
                    heartbeat_at: takeover_at,
                    expires_at: takeover_at + time::Duration::minutes(5),
                },
                takeover_at,
            )
            .await?
            .ok_or("foreign session should acquire the lease after PTY grace expires")?;
        assert_eq!(takeover.holder_agent_session_id, foreign_session.id);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_pty_renews_only_its_exact_multi_resource_scopes()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let output_tx = backend.output_tx.clone();
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig::production_default(fixture.connector_id),
        );
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let coordination_scopes = vec![
            "prod/datatool-dev/storage/minio/rejected-data".to_owned(),
            "prod/datatool-dev/database/mysql/rejected-data".to_owned(),
        ];
        let pending = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: None,
                coordination_scopes: Some(coordination_scopes.clone()),
            },
        )?;
        fixture.repositories.pty_sessions.upsert(&pending).await?;
        manager.activate_existing(pending.pty_session_id).await?;

        let before_input = now_utc();
        let unrelated_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "preexisting-unrelated-session".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("preexisting-unrelated-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: before_input,
            last_seen_at: before_input,
            expires_at: before_input + time::Duration::hours(24),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&unrelated_session)
            .await?;
        let unrelated_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(unrelated_session.id),
            label: "preexisting-unrelated-agent".to_owned(),
            ..workspace.clone()
        };
        fixture
            .repositories
            .workspaces
            .insert(&unrelated_workspace)
            .await?;
        assert!(
            fixture
                .repositories
                .host_write_leases
                .try_acquire(
                    &HostWriteLease {
                        host_id: workspace.host_id,
                        coordination_scope: "prod/datatool-dev/deployment/lichtblick".to_owned(),
                        holder_agent_session_id: unrelated_session.id,
                        holder_workspace_id: unrelated_workspace.id,
                        acquired_at: before_input,
                        heartbeat_at: before_input,
                        expires_at: before_input + time::Duration::minutes(5),
                    },
                    before_input,
                )
                .await?
                .is_some()
        );

        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pending.pty_session_id,
            workspace_id: workspace.id,
            connector_id: workspace.connector_id,
            host_id: workspace.host_id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: Some("multi-resource-pty-input".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some("multi-resource-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "22 bytes queued for pty input".to_owned(),
            byte_len: 22,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, "run cleanup command\n")
            .await?;
        manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("PTY input should be delivered")?;
        let observed_at = now_utc();
        let initial = fixture
            .repositories
            .host_write_leases
            .list_active(workspace.host_id, observed_at)
            .await?;
        assert_eq!(
            initial
                .iter()
                .filter(|lease| lease.holder_agent_session_id == fixture.agent_session_id)
                .map(|lease| lease.coordination_scope.clone())
                .collect::<BTreeSet<_>>(),
            coordination_scopes.iter().cloned().collect()
        );

        let foreign_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "foreign-disjoint-pty-session".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("foreign-disjoint-pty-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: observed_at,
            last_seen_at: observed_at,
            expires_at: observed_at + time::Duration::hours(24),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&foreign_session)
            .await?;
        let foreign_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(foreign_session.id),
            label: "foreign-disjoint-pty-agent".to_owned(),
            ..workspace.clone()
        };
        fixture
            .repositories
            .workspaces
            .insert(&foreign_workspace)
            .await?;
        let foreign_lease = |scope: &str| HostWriteLease {
            host_id: workspace.host_id,
            coordination_scope: scope.to_owned(),
            holder_agent_session_id: foreign_session.id,
            holder_workspace_id: foreign_workspace.id,
            acquired_at: observed_at,
            heartbeat_at: observed_at,
            expires_at: observed_at + time::Duration::minutes(5),
        };
        assert!(
            fixture
                .repositories
                .host_write_leases
                .try_acquire(
                    &foreign_lease("prod/datatool-dev/pipeline-recovery/clean"),
                    observed_at,
                )
                .await?
                .is_some()
        );
        assert!(
            fixture
                .repositories
                .host_write_leases
                .try_acquire(
                    &foreign_lease("prod/datatool-dev/storage/minio/rejected-data/object-42"),
                    observed_at,
                )
                .await?
                .is_none()
        );

        tokio::time::sleep(Duration::from_millis(5)).await;
        output_tx
            .send(PtyBackendOutput {
                stream: OutputStream::Stdout,
                text: "multi-resource cleanup still running\n".to_owned(),
                truncated: false,
            })
            .await?;
        wait_for_pty_output(
            &fixture.repositories,
            pending.pty_session_id,
            "multi-resource cleanup still running",
        )
        .await?;
        let renewed = fixture
            .repositories
            .host_write_leases
            .list_active(workspace.host_id, now_utc())
            .await?;
        for scope in &coordination_scopes {
            let before = initial
                .iter()
                .find(|lease| &lease.coordination_scope == scope)
                .ok_or("initial exact PTY lease should exist")?;
            let after = renewed
                .iter()
                .find(|lease| &lease.coordination_scope == scope)
                .ok_or("renewed exact PTY lease should exist")?;
            assert!(after.heartbeat_at > before.heartbeat_at);
        }
        Ok(())
    }

    #[tokio::test]
    async fn connector_pty_manager_activates_existing_session_for_queued_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let backend = CapturingPtyBackend::new();
        let inputs = Arc::clone(&backend.inputs);
        let manager = ConnectorPtyManager::new(
            fixture.repositories.clone(),
            backend,
            ConnectorPtyManagerConfig {
                connector_id: fixture.connector_id,
                max_input_bytes: 1024,
                output_limit_bytes: 1024,
                input_lease_seconds: 30,
                input_max_attempts: 3,
            },
        );
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let connection = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection should exist")?;
        let pty_session = PtySessionSupervisor::default().open_session(
            &workspace,
            &connection,
            0,
            PtySessionOpenCommand {
                session_id: fixture.session_id,
                cwd: Some("/tmp".to_owned()),
                coordination_scopes: None,
            },
        )?;
        fixture
            .repositories
            .pty_sessions
            .upsert(&pty_session)
            .await?;
        let event = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty_session.pty_session_id,
            workspace_id: fixture.workspace_id,
            connector_id: fixture.connector_id,
            host_id: workspace.host_id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: None,
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: None,
            state: PtyInputEventState::Queued,
            sequence: 0,
            redacted_input_summary: "13 bytes queued for pty input".to_owned(),
            byte_len: 13,
            requested_by: Some("agent".to_owned()),
            created_at: now_utc(),
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        fixture
            .repositories
            .pty_input_events
            .insert(&event, "attach input\n")
            .await?;

        assert_eq!(
            manager.activate_next_pending().await?,
            Some(pty_session.pty_session_id)
        );
        let outcome = manager
            .deliver_next_queued_input(30, 3)
            .await?
            .ok_or("queued input should activate and deliver")?;
        assert_eq!(outcome.state, PtyInputEventState::Delivered);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(inputs.lock().await.as_slice(), ["attach input\n"]);
        let active_session = fixture
            .repositories
            .pty_sessions
            .get(pty_session.pty_session_id)
            .await?
            .ok_or("pty session should exist")?;
        assert_eq!(active_session.backend_state, PtyBackendState::Active);
        assert_eq!(
            active_session.backend_capabilities,
            PtyBackendCapabilities::openssh_pipe_shell()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn openssh_provider_reuses_cached_transport_per_access_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let provider = OpenSshTransportProvider::new(
            fixture.repositories.clone(),
            HostKeyPolicy::Add,
            5,
            ServerProtectionPolicy::default(),
        );
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;

        let first = provider.transport_for(&operation).await?;
        let second = provider.transport_for(&operation).await?;
        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn openssh_operation_and_pty_backends_share_one_raw_transport()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let pool = Arc::new(OpenSshTransportPool::new(
            fixture.repositories.clone(),
            HostKeyPolicy::Add,
            5,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        ));
        let provider = OpenSshTransportProvider::with_pool(
            Arc::clone(&pool),
            ServerProtectionPolicy::default(),
        );
        let backend = super::OpenSshPtyBackendFactory::with_pool(
            fixture.repositories.clone(),
            Arc::clone(&pool),
        );
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;

        let _guarded = provider.transport_for(&operation).await?;
        let operation_transport = pool
            .transport_for_access_path(operation.access_path_id)
            .await?;
        let pty_transport = backend.transport_for_session(fixture.session_id).await?;

        assert!(Arc::ptr_eq(&operation_transport, &pty_transport));
        Ok(())
    }

    #[tokio::test]
    async fn russh_transport_pool_reuses_raw_transport_per_access_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let pool = RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );

        let first = pool
            .transport_for_access_path(operation.access_path_id)
            .await?;
        let second = pool
            .transport_for_access_path(operation.access_path_id)
            .await?;

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!first.config.use_exec_file_transfer);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ssh_transport_pools_replace_cached_transports_when_route_metadata_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let mut path = fixture
            .repositories
            .access_paths
            .get(operation.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let russh_pool = RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );
        let openssh_pool = OpenSshTransportPool::new(
            fixture.repositories.clone(),
            HostKeyPolicy::Add,
            5,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );
        let first_russh = russh_pool.transport_for_access_path(path.id).await?;
        let first_openssh = openssh_pool.transport_for_access_path(path.id).await?;

        path.address = "10.0.0.99".to_owned();
        path.port = 2222;
        path.route_type = RouteType::Bastion;
        path.proxy_chain.clear();
        fixture.repositories.access_paths.upsert(&path).await?;

        let second_russh = russh_pool.transport_for_access_path(path.id).await?;
        let second_openssh = openssh_pool.transport_for_access_path(path.id).await?;
        assert!(!Arc::ptr_eq(&first_russh, &second_russh));
        assert!(!Arc::ptr_eq(&first_openssh, &second_openssh));
        assert_eq!(second_russh.config.address, "10.0.0.99");
        assert_eq!(second_russh.config.port, 2222);
        assert!(second_russh.config.use_exec_file_transfer);
        assert_eq!(
            second_openssh.config.destination,
            "ssh://ops@10.0.0.99:2222"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn guarded_transport_providers_follow_replaced_raw_transports()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let mut path = fixture
            .repositories
            .access_paths
            .get(operation.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let russh_pool = Arc::new(RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        ));
        let russh_provider =
            super::RusshTransportProvider::with_pool(russh_pool, ServerProtectionPolicy::default());
        let openssh_pool = Arc::new(OpenSshTransportPool::new(
            fixture.repositories.clone(),
            HostKeyPolicy::Add,
            5,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        ));
        let openssh_provider =
            OpenSshTransportProvider::with_pool(openssh_pool, ServerProtectionPolicy::default());
        let first_russh = russh_provider.cached_transport(path.id).await?;
        let first_openssh = openssh_provider.cached_transport(path.id).await?;

        path.address = "10.0.0.100".to_owned();
        fixture.repositories.access_paths.upsert(&path).await?;

        let second_russh = russh_provider.cached_transport(path.id).await?;
        let second_openssh = openssh_provider.cached_transport(path.id).await?;
        assert!(!Arc::ptr_eq(&first_russh, &second_russh));
        assert!(!Arc::ptr_eq(&first_openssh, &second_openssh));
        Ok(())
    }

    #[tokio::test]
    async fn russh_empty_bastion_route_uses_pooled_exec_file_streams()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(operation.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.route_type = RouteType::Bastion;
        access_path.proxy_chain.clear();
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let pool = RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );

        let transport = pool
            .transport_for_access_path(operation.access_path_id)
            .await?;

        assert!(transport.config.use_exec_file_transfer);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ssh_transport_pools_fail_fast_for_multi_hop_routes_without_caching()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let mut path = fixture
            .repositories
            .access_paths
            .get(operation.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        let russh_pool = RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );
        let openssh_pool = OpenSshTransportPool::new(
            fixture.repositories.clone(),
            HostKeyPolicy::Add,
            5,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        );
        russh_pool.transport_for_access_path(path.id).await?;
        openssh_pool.transport_for_access_path(path.id).await?;
        assert_eq!(russh_pool.cache.lock().await.len(), 1);
        assert_eq!(openssh_pool.cache.lock().await.len(), 1);

        path.route_type = RouteType::Bastion;
        path.proxy_chain = Vec::new();
        fixture.repositories.access_paths.upsert(&path).await?;
        russh_pool.transport_for_access_path(path.id).await?;
        openssh_pool.transport_for_access_path(path.id).await?;

        path.route_type = RouteType::ProxyJump;
        path.proxy_chain = vec!["jump-user@jump.example:22".to_owned()];
        fixture.repositories.access_paths.upsert(&path).await?;

        let Err(russh_error) = russh_pool.transport_for_access_path(path.id).await else {
            return Err("native transport silently accepted a multi-hop route".into());
        };
        assert!(russh_error.contains("multi-hop"));
        assert!(russh_pool.cache.lock().await.is_empty());
        let bootstrap = fixture
            .repositories
            .authorized_key_bootstrap
            .get(path.id)
            .await?
            .ok_or("multi-hop decision should be visible")?;
        assert_eq!(bootstrap.state, AuthorizedKeyBootstrapState::Skipped);
        assert_eq!(
            bootstrap.reason,
            Some(AuthorizedKeyBootstrapReason::MultiHopUnsupported)
        );

        let Err(openssh_error) = openssh_pool.transport_for_access_path(path.id).await else {
            return Err("OpenSSH transport silently ignored a proxy chain".into());
        };
        assert!(openssh_error.contains("multi-hop"));
        assert!(openssh_pool.cache.lock().await.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn russh_pty_backend_factory_reports_native_capabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let pool = Arc::new(RusshTransportPool::new(
            fixture.repositories.clone(),
            Arc::new(UnusedSshCredentialProvider),
            HostKeyPolicy::Add,
            None,
            5,
            30,
            ServerProtectionPolicy::default().max_new_ssh_handshakes_per_10_min,
        ));
        let backend = RusshPtyBackendFactory::with_pool(fixture.repositories.clone(), pool);

        assert_eq!(
            backend.capabilities(),
            PtyBackendCapabilities::russh_native_pty()
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_daemon_processes_operations_heartbeats_and_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let operation = fixture
            .repositories
            .operations
            .get(fixture.operation_id)
            .await?
            .ok_or("operation should exist")?;
        let operation_workspace_id = operation
            .workspace_id
            .ok_or("operation should belong to a Workspace")?;
        let mut expired_workspace = fixture
            .repositories
            .workspaces
            .get(operation_workspace_id)
            .await?
            .ok_or("operation Workspace should exist")?;
        expired_workspace.id = WorkspaceId::new();
        expired_workspace.label = "expired-history".to_owned();
        expired_workspace.agent_session_id = None;
        expired_workspace.state = WorkspaceState::Idle;
        expired_workspace.last_activity_at = now_utc() - time::Duration::hours(2);
        expired_workspace.ttl_seconds = 60;
        fixture
            .repositories
            .workspaces
            .insert(&expired_workspace)
            .await?;
        seed_ssh_transport_runtime(
            &fixture.repositories,
            &operation,
            SshTransportRuntimeState::Ready,
            3,
        )
        .await?;
        let provider = StaticTransportProvider::new(FakeTransport);
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            provider,
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "test-daemon".to_owned(),
                current_network: Some("test-net".to_owned()),
                max_concurrent_operations: 4,
                heartbeat_interval_ms: 5,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        )
        .with_pty_input_pump(Arc::new(OneShotPtyInputPump::new()));
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Succeeded,
        )
        .await?;
        stop_tx.send(true)?;
        let report = handle.await??;

        assert_eq!(report.reconciled_connection_sessions, 1);
        assert_eq!(report.reconciled_transport_runtimes, 1);
        assert_eq!(report.reconciled_expired_workspaces, 1);
        assert_eq!(report.completed_operations, 1);
        assert_eq!(report.delivered_pty_inputs, 1);
        assert_eq!(report.failed_pty_inputs, 0);
        assert_eq!(
            fixture
                .repositories
                .workspaces
                .get(expired_workspace.id)
                .await?
                .ok_or("expired Workspace history should remain inspectable")?
                .state,
            WorkspaceState::Closed
        );
        let stale_session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("pre-restart connection session should still exist")?;
        assert_eq!(stale_session.state, EntityState::Unknown);
        assert_eq!(stale_session.open_channels, 0);
        assert!(
            stale_session
                .last_error
                .as_deref()
                .is_some_and(|message| message.contains("connector runtime restarted"))
        );
        let stale_runtime = fixture
            .repositories
            .ssh_transport_runtimes
            .get(operation.access_path_id, operation.connector_id)
            .await?
            .ok_or("transport runtime should still be inspectable")?;
        assert_eq!(
            stale_runtime.telemetry.state,
            SshTransportRuntimeState::RuntimeLost
        );
        let connector = fixture
            .repositories
            .connectors
            .get(fixture.connector_id)
            .await?
            .ok_or("connector should exist")?;
        assert_eq!(connector.state, EntityState::ConnectorOffline);
        assert_eq!(connector.version, "test-daemon");
        assert_eq!(connector.current_network.as_deref(), Some("test-net"));

        let events = fixture
            .repositories
            .state_events
            .list_for_entity("connector", &fixture.connector_id.to_string(), 10)
            .await?;
        assert!(
            events
                .iter()
                .any(|event| event.new_state == EntityState::ConnectorOffline)
        );
        Ok(())
    }

    #[tokio::test]
    async fn worker_routes_file_bytes_to_connector_local_interactive_backend()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let fallback_called = Arc::new(AtomicBool::new(false));
        let worker = ConnectorOperationWorker::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(InteractiveRoutingTransport {
                fallback_called: Arc::clone(&fallback_called),
            }),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
        );
        worker
            .run_once()
            .await?
            .ok_or("fixture operation should finish first")?;

        let directory = tempfile::tempdir()?;
        let source = directory.path().join("interactive-upload.bin");
        let payload = b"connector-local-file-bytes-must-not-enter-audit";
        tokio::fs::write(&source, payload).await?;
        let digest = format!("{:x}", Sha256::digest(payload));
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let plan = WorkspaceOperationSupervisor::default().queue_file_transfer(
            &WorkspaceFileTransfer {
                workspace,
                spec: FileTransferSpec {
                    direction: SftpDirection::Upload,
                    local_path: source.to_string_lossy().into_owned(),
                    remote_path: "/tmp/interactive-upload.bin".to_owned(),
                    overwrite: SftpOverwritePolicy::Replace,
                    mode: Some(0o600),
                    max_size_bytes: 1024,
                    expected_sha256: Some(digest),
                    timeout_seconds: 30,
                },
                intent: Some("verify connector-local interactive routing".to_owned()),
                idempotency_key: None,
                queued_operations: 0,
                active_exec_channels: 0,
                overload_cooldown_active: false,
            },
        )?;
        fixture
            .repositories
            .operations
            .insert(&plan.operation)
            .await?;
        fixture
            .repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(fixture.workspace_id, plan.workspace_state, now_utc())
            .await?;

        let interactive_called = Arc::new(AtomicBool::new(false));
        worker.set_interactive_file_transfer(Arc::new(RecordingInteractiveFileTransfer {
            called: Arc::clone(&interactive_called),
        }));
        let outcome = worker
            .run_once()
            .await?
            .ok_or("interactive transfer should be claimed")?;
        assert_eq!(outcome.state, OperationState::Succeeded);
        assert!(interactive_called.load(Ordering::SeqCst));
        assert!(!fallback_called.load(Ordering::SeqCst));

        let chunks = fixture
            .repositories
            .operation_output_chunks
            .list_for_workspace(fixture.workspace_id, Some(plan.operation.id), None, 100)
            .await?;
        assert!(
            chunks
                .iter()
                .all(|chunk| !chunk.redacted_text.contains("connector-local-file-bytes"))
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_daemon_overlaps_readonly_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let workspace = fixture
            .repositories
            .workspaces
            .get(fixture.workspace_id)
            .await?
            .ok_or("workspace should exist")?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.max_concurrent_channels = 2;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let policy = ServerProtectionPolicy::default();
        let profile = CommandProfileCatalog::resolve_builtin("host.identity", Vec::new(), &policy)?;
        let second =
            WorkspaceOperationSupervisor::new(policy).queue_operation(&WorkspaceRunCommand {
                workspace,
                command_profile: profile,
                intent: Some("parallel readonly smoke".to_owned()),
                idempotency_key: None,
                coordination_mode: OperationCoordinationMode::Auto,
                coordination_scope: None,
                coordination_scopes: None,
                queued_operations: 1,
                active_exec_channels: 0,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })?;
        fixture
            .repositories
            .operations
            .insert(&second.operation)
            .await?;
        fixture
            .repositories
            .operation_output_chunks
            .insert(&second.initial_output_chunk)
            .await?;

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let transport = ConcurrencyTrackingTransport {
            active,
            max_active: Arc::clone(&max_active),
            delay: Duration::from_millis(250),
        };
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(transport),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "parallel-test".to_owned(),
                current_network: None,
                max_concurrent_operations: 2,
                heartbeat_interval_ms: 10,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        for _ in 0..50 {
            if max_active.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        let concurrent_channels: u32 = fixture
            .repositories
            .connection_sessions
            .list_for_host(fixture.host_id)
            .await?
            .into_iter()
            .map(|session| session.open_channels)
            .sum();
        assert_eq!(concurrent_channels, 2);

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Succeeded,
        )
        .await?;
        wait_for_operation_state(
            &fixture.repositories,
            second.operation.id,
            OperationState::Succeeded,
        )
        .await?;
        stop_tx.send(true)?;
        handle.await??;

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        let completed_channels: u32 = fixture
            .repositories
            .connection_sessions
            .list_for_host(fixture.host_id)
            .await?
            .into_iter()
            .map(|session| session.open_channels)
            .sum();
        assert_eq!(completed_channels, 0);
        Ok(())
    }

    #[tokio::test]
    async fn connector_daemon_keeps_heartbeating_while_operation_is_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(SlowTransport(Duration::from_millis(150))),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "test-daemon".to_owned(),
                current_network: Some("test-net".to_owned()),
                max_concurrent_operations: 4,
                heartbeat_interval_ms: 10,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Running,
        )
        .await?;
        let first_seen_at = fixture
            .repositories
            .connectors
            .get(fixture.connector_id)
            .await?
            .and_then(|connector| connector.last_seen_at)
            .ok_or("connector heartbeat should exist")?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        let second_seen_at = fixture
            .repositories
            .connectors
            .get(fixture.connector_id)
            .await?
            .and_then(|connector| connector.last_seen_at)
            .ok_or("connector heartbeat should still exist")?;

        assert!(
            second_seen_at > first_seen_at,
            "connector heartbeat must advance while a remote operation is still running"
        );

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Succeeded,
        )
        .await?;
        stop_tx.send(true)?;
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn connector_daemon_keeps_pumping_pty_input_while_operation_is_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let pump = Arc::new(DeferredOneShotPtyInputPump::new(Duration::from_millis(30)));
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(SlowTransport(Duration::from_millis(500))),
            ConnectorOperationWorkerConfig {
                connector_id: fixture.connector_id,
                lease_seconds: 300,
                max_attempts: 3,
                artifact_threshold_bytes: DEFAULT_ARTIFACT_THRESHOLD_BYTES,
                artifact_preview_bytes: DEFAULT_ARTIFACT_PREVIEW_BYTES,
            },
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "test-daemon".to_owned(),
                current_network: Some("test-net".to_owned()),
                max_concurrent_operations: 4,
                heartbeat_interval_ms: 10,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        )
        .with_pty_input_pump(pump.clone());
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Running,
        )
        .await?;
        for _ in 0..40 {
            if pump.was_delivered() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            pump.was_delivered(),
            "PTY input must be delivered before the long remote operation completes"
        );

        wait_for_operation_state(
            &fixture.repositories,
            fixture.operation_id,
            OperationState::Succeeded,
        )
        .await?;
        stop_tx.send(true)?;
        let report = handle.await??;
        assert_eq!(report.delivered_pty_inputs, 1);
        Ok(())
    }

    #[tokio::test]
    async fn connector_daemon_does_not_cancel_slow_pty_activation_for_heartbeats()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let pump = Arc::new(SlowOneShotPtyActivationPump::new());
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(SlowTransport(Duration::from_millis(100))),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "pty-cancellation-test".to_owned(),
                current_network: None,
                max_concurrent_operations: 4,
                heartbeat_interval_ms: 5,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        )
        .with_pty_input_pump(pump.clone());
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        for _ in 0..40 {
            if pump.completed.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stop_tx.send(true)?;
        handle.await??;

        assert!(
            pump.completed.load(Ordering::SeqCst),
            "heartbeat and operation polling must not cancel an in-flight PTY activation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn connector_daemon_runs_idle_pty_reaping_without_an_external_api_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = WorkerFixture::new().await?;
        let reaper = Arc::new(OneShotIdlePtyReaper::new());
        let daemon = ConnectorDaemon::new(
            fixture.repositories.clone(),
            StaticTransportProvider::new(SlowTransport(Duration::from_millis(5))),
            ConnectorOperationWorkerConfig::production_default(fixture.connector_id),
            ConnectorDaemonConfig {
                connector_id: fixture.connector_id,
                version: "idle-reaper-test".to_owned(),
                current_network: None,
                max_concurrent_operations: 1,
                heartbeat_interval_ms: 5,
                idle_min_delay_ms: 5,
                idle_max_delay_ms: 10,
                error_backoff_ms: 5,
            },
        )
        .with_pty_input_pump(reaper);
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { daemon.run_until_stopped(stop_rx).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_tx.send(true)?;
        let report = handle.await??;
        assert_eq!(report.reaped_idle_pty_sessions, 1);
        Ok(())
    }

    #[test]
    fn russh_idle_reap_requires_zero_channels_and_elapsed_ttl() {
        let now = now_utc();
        let last_activity_at = now - time::Duration::seconds(601);

        assert!(super::should_reap_idle_transport(
            now,
            last_activity_at,
            600,
            8,
            8,
        ));
        assert!(!super::should_reap_idle_transport(
            now,
            last_activity_at,
            600,
            7,
            8,
        ));
        assert!(!super::should_reap_idle_transport(
            now,
            now - time::Duration::seconds(599),
            600,
            8,
            8,
        ));
        assert!(!super::should_reap_idle_transport(
            now,
            last_activity_at,
            0,
            8,
            8,
        ));
    }

    async fn seed_ssh_transport_runtime(
        repositories: &Repositories,
        operation: &OperationRun,
        state: SshTransportRuntimeState,
        reuse_count: u64,
    ) -> Result<SshTransportRuntimeId, Box<dyn std::error::Error>> {
        let runtime_id = SshTransportRuntimeId::new();
        let now = now_utc();
        repositories
            .ssh_transport_runtimes
            .upsert(&SshTransportRuntime {
                access_path_id: operation.access_path_id,
                connector_id: operation.connector_id,
                telemetry: SshTransportTelemetry {
                    runtime_id,
                    backend: SshTransportBackend::Russh,
                    state,
                    generation: 1,
                    connection_attempt_count: 1,
                    successful_handshake_count: 1,
                    reuse_count,
                    last_handshake_at: Some(now),
                    last_validated_at: Some(now),
                    capabilities: SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
                },
                updated_at: now,
            })
            .await?;
        Ok(runtime_id)
    }

    async fn wait_for_operation_state(
        repositories: &Repositories,
        operation_id: OperationId,
        expected: OperationState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..100 {
            let operation = repositories
                .operations
                .get(operation_id)
                .await?
                .ok_or("operation should exist")?;
            if operation.state == expected {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err("operation did not reach expected state".into())
    }

    async fn wait_for_pty_output(
        repositories: &Repositories,
        pty_session_id: PtySessionId,
        expected_text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..100 {
            let chunks = repositories
                .pty_output_chunks
                .list_for_session(pty_session_id, None, 20)
                .await?;
            if chunks
                .iter()
                .any(|chunk| chunk.redacted_text.contains(expected_text))
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err("pty output did not reach expected text".into())
    }

    async fn wait_for_pty_interaction(
        repositories: &Repositories,
        pty_session_id: PtySessionId,
        expected_kind: PtyInteractionKind,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..100 {
            let pty = repositories
                .pty_sessions
                .get(pty_session_id)
                .await?
                .ok_or("PTY should exist")?;
            if pty
                .interaction
                .as_ref()
                .is_some_and(|interaction| interaction.kind == expected_kind)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err("PTY did not reach expected interaction state".into())
    }

    struct WorkerFixture {
        repositories: Repositories,
        host_id: HostId,
        agent_session_id: AgentSessionId,
        connector_id: ConnectorId,
        workspace_id: WorkspaceId,
        session_id: SessionId,
        operation_id: OperationId,
    }

    impl WorkerFixture {
        #[allow(clippy::too_many_lines)]
        async fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let pool = connect_sqlite("sqlite::memory:").await?;
            migrate(&pool).await?;
            let repositories = Repositories::new(pool);
            let now = now_utc();
            let host = Host {
                id: HostId::new(),
                name: "worker-host".to_owned(),
                display_name: "Worker Host".to_owned(),
                kind: HostKind::Linux,
                owner: None,
                tags: Vec::new(),
                description: None,
                risk_level: RiskLevel::Development,
                created_at: now,
                updated_at: now,
            };
            repositories.hosts.insert(&host).await?;
            let environment = Environment {
                id: EnvironmentId::new(),
                name: format!("worker-env-{}", host.id),
                kind: EnvironmentKind::CompanyLan,
                description: None,
                trust_level: TrustLevel::Trusted,
                notes: None,
            };
            repositories.environments.insert(&environment).await?;
            let connector = Connector {
                id: ConnectorId::new(),
                name: format!("worker-connector-{}", host.id),
                environment_id: environment.id,
                host_id: None,
                version: "0.1.0".to_owned(),
                state: EntityState::Healthy,
                last_seen_at: Some(now),
                current_network: Some("test".to_owned()),
            };
            repositories.connectors.upsert(&connector).await?;
            let credential_id = CredentialId::new();
            repositories
                .credentials
                .insert(&StoredCredential {
                    metadata: CredentialMetadata {
                        id: credential_id,
                        name: format!("worker-credential-{}", host.id),
                        kind: CredentialKind::SshPrivateKey,
                        username_hint: Some("ops".to_owned()),
                        created_at: now,
                        updated_at: now,
                        last_used_at: None,
                    },
                    encrypted_blob_json: json!({"version": 1}),
                })
                .await?;
            let access_path = AccessPath {
                id: AccessPathId::new(),
                host_id: host.id,
                environment_id: environment.id,
                connector_id: Some(connector.id),
                protocol: Protocol::Ssh,
                address: "10.0.0.40".to_owned(),
                port: 22,
                username: "ops".to_owned(),
                credential_id,
                route_type: RouteType::Lan,
                proxy_chain: Vec::new(),
                priority: 1,
                enabled: true,
                connection_mode: ConnectionMode::Pooled,
                idle_ttl_seconds: 600,
                keepalive_seconds: 30,
                max_concurrent_channels: 1,
                max_new_connections_per_minute: 1,
                requires_tty: false,
                notes: None,
            };
            repositories.access_paths.insert(&access_path).await?;
            let session = ConnectionSession {
                session_id: SessionId::new(),
                access_path_id: access_path.id,
                connector_id: connector.id,
                state: EntityState::Connected,
                created_at: now,
                last_used_at: now,
                open_channels: 1,
                reused_count: 0,
                failure_count: 0,
                last_error: None,
            };
            repositories.connection_sessions.upsert(&session).await?;
            repositories
                .access_path_health
                .upsert(&AccessPathHealth {
                    access_path_id: access_path.id,
                    state: EntityState::Healthy,
                    last_checked_at: Some(now),
                    latency_ms: Some(3),
                    failure_count: 0,
                    last_error_code: None,
                    next_retry_at: None,
                })
                .await?;

            let agent_session = AgentSession {
                id: AgentSessionId::new(),
                client_kind: "codex".to_owned(),
                client_instance_id: "connector-worker-test".to_owned(),
                project_key: Some("remote-hosts".to_owned()),
                conversation_key: Some("worker-fixture".to_owned()),
                state: AgentSessionState::Active,
                created_at: now,
                last_seen_at: now,
                expires_at: now + time::Duration::hours(24),
            };
            repositories.agent_sessions.upsert(&agent_session).await?;
            let workspace = AgentWorkspace {
                id: WorkspaceId::new(),
                agent_session_id: Some(agent_session.id),
                host_id: host.id,
                access_path_id: access_path.id,
                connector_id: connector.id,
                label: "agent-main".to_owned(),
                cwd: Some("/tmp".to_owned()),
                state: WorkspaceState::Idle,
                policy_profile: "default".to_owned(),
                coordination_scope: "host".to_owned(),
                created_at: now,
                last_activity_at: now,
                ttl_seconds: 3600,
            };
            repositories.workspaces.insert(&workspace).await?;

            let policy = ServerProtectionPolicy::default();
            let profile =
                CommandProfileCatalog::resolve_builtin("host.uptime", Vec::new(), &policy)?;
            let plan = WorkspaceOperationSupervisor::new(policy).queue_operation(
                &WorkspaceRunCommand {
                    workspace: workspace.clone(),
                    command_profile: profile,
                    intent: Some("worker smoke".to_owned()),
                    idempotency_key: None,
                    coordination_mode: OperationCoordinationMode::Auto,
                    coordination_scope: None,
                    coordination_scopes: None,
                    queued_operations: 0,
                    active_exec_channels: 0,
                    active_probe_jobs: 0,
                    overload_cooldown_active: false,
                },
            )?;
            repositories.operations.insert(&plan.operation).await?;
            repositories
                .operation_output_chunks
                .insert(&plan.initial_output_chunk)
                .await?;
            repositories
                .workspaces
                .update_state(workspace.id, plan.workspace_state, now)
                .await?;

            Ok(Self {
                repositories,
                host_id: host.id,
                agent_session_id: agent_session.id,
                connector_id: connector.id,
                workspace_id: workspace.id,
                session_id: session.session_id,
                operation_id: plan.operation.id,
            })
        }
    }
}

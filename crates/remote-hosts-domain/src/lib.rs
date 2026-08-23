//! Shared domain model for the remote hosts system.

use std::{collections::BTreeSet, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[doc = concat!("Stable identifier for ", stringify!($name), ".")]
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a new time-sortable v7 identifier.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Returns the underlying UUID.
            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(input).map(Self)
            }
        }
    };
}

id_type!(HostId);
id_type!(EnvironmentId);
id_type!(ConnectorId);
id_type!(AccessPathId);
id_type!(CredentialId);
id_type!(SessionId);
id_type!(AgentSessionId);
id_type!(WorkspaceId);
id_type!(PtySessionId);
id_type!(PtyOutputChunkId);
id_type!(PtyInputEventId);
id_type!(OperationId);
id_type!(OperationOutputChunkId);
id_type!(OperationOutputArtifactId);
id_type!(SshTransportRuntimeId);
id_type!(StateEventId);
id_type!(KnowledgeItemId);
id_type!(SoftwareInstallId);
id_type!(HostFactId);
id_type!(TopologyNodeId);
id_type!(TopologyEdgeId);
id_type!(TopologySyncRunId);
id_type!(CredentialBindingId);

/// Returns the current UTC timestamp.
pub fn now_utc() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

/// Lifecycle state of one agent-client session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionState {
    /// The client session can own and operate workspaces.
    Active,
    /// The client session expired after its lease was not renewed.
    Expired,
    /// The client session was closed intentionally.
    Closed,
}

/// Durable identity and lease for one agent client or conversation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentSession {
    /// Session identity assigned by Remote Hosts.
    pub id: AgentSessionId,
    /// Client family, for example `codex`, `antigravity`, or `mcp`.
    pub client_kind: String,
    /// One MCP process or explicitly supplied client-instance key.
    pub client_instance_id: String,
    /// Optional project-level isolation key.
    pub project_key: Option<String>,
    /// Optional conversation-level isolation key.
    pub conversation_key: Option<String>,
    /// Current client-session lifecycle state.
    pub state: AgentSessionState,
    /// Session creation timestamp.
    pub created_at: OffsetDateTime,
    /// Most recent tool activity.
    pub last_seen_at: OffsetDateTime,
    /// Time after which an unrenewed session is considered expired.
    pub expires_at: OffsetDateTime,
}

/// High-level host category.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostKind {
    /// macOS machine.
    Macos,
    /// Windows machine.
    Windows,
    /// Linux machine.
    Linux,
    /// GPU server.
    GpuServer,
    /// Jump host or bastion.
    JumpHost,
    /// Customer deployment server.
    CustomerServer,
    /// Other machine category.
    Other(String),
}

/// Operational risk level of a host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    /// Personal machine.
    Personal,
    /// Development or test machine.
    Development,
    /// Production machine.
    Production,
    /// Customer-site machine.
    CustomerSite,
}

/// Network or physical environment category.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentKind {
    /// Home LAN.
    HomeLan,
    /// Company LAN.
    CompanyLan,
    /// Customer site LAN.
    CustomerSite,
    /// Public Internet route.
    PublicInternet,
    /// VPN route.
    Vpn,
    /// FRP or similar reverse proxy route.
    Frp,
}

/// Trust level for an environment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    /// Controlled by the owner.
    Owned,
    /// Shared but trusted network.
    Trusted,
    /// External or customer network.
    External,
    /// Untrusted network.
    Untrusted,
}

/// Remote access protocol.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// SSH protocol.
    Ssh,
}

/// Route style used by an access path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteType {
    /// Direct LAN route.
    Lan,
    /// Public network route.
    Public,
    /// FRP route.
    Frp,
    /// VPN route.
    Vpn,
    /// OpenSSH `ProxyJump` or equivalent.
    ProxyJump,
    /// Bastion host route.
    Bastion,
}

/// Connection lifecycle mode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionMode {
    /// Reuse transports.
    Pooled,
    /// Always use a one-shot transport.
    OneShot,
}

/// Secret category stored in the internal vault.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    /// SSH password.
    SshPassword,
    /// SSH private key.
    SshPrivateKey,
    /// SSH private key plus passphrase.
    SshPrivateKeyWithPassphrase,
    /// sudo password.
    SudoPassword,
    /// Windows account password.
    WindowsPassword,
    /// Username and password used by an application or web service.
    BasicAuth,
    /// API token or bearer token.
    ApiToken,
    /// Database username and password.
    DatabasePassword,
    /// Middleware or cluster administrative credential.
    ServiceAccount,
    /// Arbitrary secret text for an internal system.
    GenericSecret,
}

/// Infrastructure resource represented in the topology graph.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyNodeKind {
    /// A registered physical or logical host.
    Host,
    /// A group of cooperating machines or services.
    Cluster,
    /// A virtual machine.
    VirtualMachine,
    /// A container or workload instance.
    Container,
    /// A reverse proxy such as nginx, Caddy, Traefik, or `HAProxy`.
    ReverseProxy,
    /// A load balancer.
    LoadBalancer,
    /// A shared middleware service.
    Middleware,
    /// A database service.
    Database,
    /// A cache service.
    Cache,
    /// A message queue or streaming service.
    MessageQueue,
    /// A business-facing application or API.
    BusinessService,
    /// A storage service.
    Storage,
    /// A network, subnet, or overlay.
    Network,
    /// A generic reachable endpoint.
    Endpoint,
    /// A resource outside the predefined categories.
    Other,
}

/// Operational status reported for one topology node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyNodeStatus {
    /// No health observation is available.
    Unknown,
    /// The resource is healthy.
    Healthy,
    /// The resource is usable with known degradation.
    Degraded,
    /// The resource is offline or unreachable.
    Offline,
    /// The resource is intentionally under maintenance.
    Maintenance,
}

/// Directed relationship between two topology nodes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyRelation {
    /// The source logically contains the target.
    Contains,
    /// The source is a member of the target.
    MemberOf,
    /// The source runs on the target.
    RunsOn,
    /// The source reverse-proxies requests to the target.
    ProxiesTo,
    /// The source routes traffic to the target.
    RoutesTo,
    /// The source depends on the target.
    DependsOn,
    /// The source opens a network or protocol connection to the target.
    ConnectsTo,
    /// The source replicates data to the target.
    ReplicatesTo,
    /// The source exposes the target.
    Exposes,
    /// The source is managed by the target.
    ManagedBy,
    /// A relationship outside the predefined categories.
    Other,
}

/// Source for observed facts and knowledge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactSource {
    /// Added by a human.
    Manual,
    /// Gathered by a probe.
    Probe,
    /// Derived from an operation.
    Operation,
    /// Imported from external data.
    Import,
}

/// Remote operation category.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationType {
    /// Read-only probe.
    Probe,
    /// Read-only exec profile.
    ReadonlyExec,
    /// SFTP transfer.
    Sftp,
    /// Port forwarding.
    PortForward,
    /// Approved runbook.
    Runbook,
}

/// Operation lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    /// Operation is queued.
    Queued,
    /// Operation is running.
    Running,
    /// Operation completed successfully.
    Succeeded,
    /// Operation failed.
    Failed,
    /// Operation timed out.
    TimedOut,
    /// Operation was cancelled.
    Cancelled,
    /// Operation was rejected by policy.
    Rejected,
    /// Operation exhausted its automatic connector retry budget.
    Exhausted,
}

/// Persistent PTY input queue state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyInputEventState {
    /// Input is waiting for the owning connector.
    Queued,
    /// Input has been claimed by a connector pump.
    Claimed,
    /// Input has been written to the live PTY backend.
    Delivered,
    /// Input could not be delivered.
    Failed,
}

/// Source of a queued PTY input payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyInputPayloadKind {
    /// The event carries caller-provided text in its private queue payload.
    Text,
    /// The connector resolves a stored sudo password only when delivering the event.
    StoredSudoPassword,
    /// The connector resolves another registered SSH route's password only when delivering the
    /// event to a live nested SSH password prompt.
    StoredSshPassword,
    /// The connector resolves another registered SSH route's dedicated sudo password only when
    /// delivering the event to a verified nested sudo prompt.
    StoredTargetSudoPassword,
}

/// State values visible to agents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityState {
    /// State is unknown.
    Unknown,
    /// Entity is not configured.
    NotConfigured,
    /// Connector is offline.
    ConnectorOffline,
    /// Resolver is working.
    Resolving,
    /// TCP cannot be reached.
    TcpUnreachable,
    /// SSH handshake failed.
    SshHandshakeFailed,
    /// Authentication failed.
    AuthFailed,
    /// Host key changed.
    HostKeyChanged,
    /// Entity is connected.
    Connected,
    /// Entity is degraded.
    Degraded,
    /// Rate limit is active.
    RateLimited,
    /// System throttled the agent.
    Throttled,
    /// Target appears overloaded.
    TargetOverloaded,
    /// Circuit breaker is open.
    CircuitOpen,
    /// Entity is in maintenance.
    Maintenance,
    /// Entity is healthy.
    Healthy,
}

/// Agent-facing recovery hint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHint {
    /// Use another access path.
    UseAlternateAccessPath,
    /// Wait before retrying.
    WaitBeforeRetry,
    /// Ask user to unlock the vault.
    AskUserToUnlockVault,
    /// Connector is offline; try public path.
    ConnectorOfflineTryPublicPath,
    /// Auth failed; do not retry automatically.
    AuthFailedDoNotRetry,
    /// Refresh facts before execution.
    RefreshFactsBeforeExecution,
    /// Reuse an existing workspace.
    UseExistingWorkspace,
    /// Use cached state or wait.
    UseCachedStateOrWait,
    /// Reduce probe frequency.
    ReduceProbeFrequency,
}

/// Machine-readable state reason.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateReasonCode {
    /// No specific reason.
    None,
    /// Connector heartbeat is stale.
    ConnectorHeartbeatStale,
    /// TCP probe failed.
    TcpProbeFailed,
    /// SSH handshake failed.
    SshHandshakeFailed,
    /// A pooled SSH transport was discarded after a channel-level failure.
    PooledTransportInvalidated,
    /// SSH authentication failed.
    SshAuthFailed,
    /// Target SSH daemon appears rate limited.
    TargetSshdRateLimited,
    /// The connector's local SSH handshake budget is exhausted.
    LocalHandshakeBudgetExhausted,
    /// Target is overloaded.
    TargetOverloaded,
    /// Circuit breaker is open.
    CircuitOpen,
    /// Vault is locked.
    VaultLocked,
    /// Policy rejected the request.
    PolicyRejected,
}

/// Agent workspace state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceState {
    /// Workspace is idle.
    Idle,
    /// Workspace is actively working.
    Working,
    /// Workspace appears blocked.
    Blocked,
    /// Workspace completed its current task.
    Done,
    /// Workspace failed.
    Failed,
    /// Workspace is throttled.
    Throttled,
    /// Workspace has been closed and should not accept new operations.
    Closed,
}

/// Output stream captured for an operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
    /// System-generated status message.
    System,
}

/// Connector backend used for a persistent PTY session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyBackendKind {
    /// Backend has not been activated or reported yet.
    Unknown,
    /// OpenSSH `ControlMaster` channel running a long-lived shell over pipes.
    OpenSshPipeShell,
    /// OpenSSH `ControlMaster` reused by an `ssh -tt` child process.
    OpenSshControlMasterTty,
    /// Native `russh` SSH channel with `request-pty`.
    RusshNativePty,
}

/// Terminal semantics exposed by a persistent PTY session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyTerminalSemantics {
    /// Backend semantics have not been reported yet.
    Unknown,
    /// Persistent shell over stdin/stdout/stderr pipes without SSH `request-pty`.
    PipeShell,
    /// SSH protocol PTY channel semantics.
    SshPty,
}

/// Connector-local backend lifecycle state for a persistent PTY session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyBackendState {
    /// Backend state has not been reported yet.
    Unknown,
    /// The session record exists and waits for connector activation.
    Pending,
    /// The connector process has an active backend handle.
    Active,
    /// Backend activation or delivery failed.
    Failed,
    /// Backend has been closed.
    Closed,
}

/// Generic type of interactive input requested by a live PTY.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyInteractionKind {
    /// SSH or another program requests a password or passphrase.
    Password,
    /// `sudo` requests a password.
    SudoPassword,
    /// SSH requests explicit host-key confirmation.
    HostKeyConfirmation,
    /// A command requests a yes/no confirmation.
    Confirmation,
    /// A pager waits for a navigation key.
    Pager,
    /// An interactive menu requests an option selection.
    SelectionMenu,
}

/// Agent-visible observation that an active PTY needs input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyInteraction {
    /// Detected interaction category.
    pub kind: PtyInteractionKind,
    /// Detector confidence from 0 through 100.
    pub confidence: u8,
    /// When the connector observed the prompt.
    pub observed_at: OffsetDateTime,
}

/// Agent-visible PTY backend capability summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct PtyBackendCapabilities {
    /// Backend implementation kind.
    pub kind: PtyBackendKind,
    /// Terminal semantics agents can rely on.
    pub terminal_semantics: PtyTerminalSemantics,
    /// Whether the remote side allocated an SSH PTY.
    pub allocates_tty: bool,
    /// Whether this backend reuses a pooled SSH transport/master connection.
    pub reuses_ssh_transport: bool,
    /// Whether window resize requests are supported by this backend.
    pub supports_window_resize: bool,
    /// Whether signal delivery is supported by this backend.
    pub supports_signal: bool,
    /// Whether input can stream into the session after it is active.
    pub supports_streaming_input: bool,
    /// Whether output streams are persisted for polling.
    pub supports_streaming_output: bool,
}

impl PtyBackendCapabilities {
    /// Unknown backend capabilities before connector activation.
    pub fn unknown() -> Self {
        Self {
            kind: PtyBackendKind::Unknown,
            terminal_semantics: PtyTerminalSemantics::Unknown,
            allocates_tty: false,
            reuses_ssh_transport: false,
            supports_window_resize: false,
            supports_signal: false,
            supports_streaming_input: false,
            supports_streaming_output: false,
        }
    }

    /// Capabilities for the current OpenSSH pipe-shell compatibility backend.
    pub fn openssh_pipe_shell() -> Self {
        Self {
            kind: PtyBackendKind::OpenSshPipeShell,
            terminal_semantics: PtyTerminalSemantics::PipeShell,
            allocates_tty: false,
            reuses_ssh_transport: true,
            supports_window_resize: false,
            supports_signal: false,
            supports_streaming_input: true,
            supports_streaming_output: true,
        }
    }

    /// Capabilities for an OpenSSH `ControlMaster` backed true-tty shell.
    pub fn openssh_control_master_tty() -> Self {
        Self {
            kind: PtyBackendKind::OpenSshControlMasterTty,
            terminal_semantics: PtyTerminalSemantics::SshPty,
            allocates_tty: true,
            reuses_ssh_transport: true,
            supports_window_resize: false,
            supports_signal: false,
            supports_streaming_input: true,
            supports_streaming_output: true,
        }
    }

    /// Capabilities expected from the native `russh` PTY backend.
    pub fn russh_native_pty() -> Self {
        Self {
            kind: PtyBackendKind::RusshNativePty,
            terminal_semantics: PtyTerminalSemantics::SshPty,
            allocates_tty: true,
            reuses_ssh_transport: true,
            supports_window_resize: true,
            supports_signal: true,
            supports_streaming_input: true,
            supports_streaming_output: true,
        }
    }
}

impl Default for PtyBackendCapabilities {
    fn default() -> Self {
        Self::unknown()
    }
}

/// Host registry record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Host {
    /// Host id.
    pub id: HostId,
    /// Stable slug-like name.
    pub name: String,
    /// Human-facing name.
    pub display_name: String,
    /// Host kind.
    pub kind: HostKind,
    /// Optional owner.
    pub owner: Option<String>,
    /// Tags.
    pub tags: Vec<String>,
    /// Description.
    pub description: Option<String>,
    /// Operational risk level.
    pub risk_level: RiskLevel,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Updated timestamp.
    pub updated_at: OffsetDateTime,
}

/// Network environment record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Environment {
    /// Environment id.
    pub id: EnvironmentId,
    /// Environment name.
    pub name: String,
    /// Environment kind.
    pub kind: EnvironmentKind,
    /// Description.
    pub description: Option<String>,
    /// Trust level.
    pub trust_level: TrustLevel,
    /// Notes.
    pub notes: Option<String>,
}

/// Connector record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connector {
    /// Connector id.
    pub id: ConnectorId,
    /// Connector name.
    pub name: String,
    /// Environment where the connector runs.
    pub environment_id: EnvironmentId,
    /// Managed host on which the connector runs.
    pub host_id: Option<HostId>,
    /// Connector version.
    pub version: String,
    /// Current state.
    pub state: EntityState,
    /// Last heartbeat timestamp.
    pub last_seen_at: Option<OffsetDateTime>,
    /// Current network label.
    pub current_network: Option<String>,
}

/// Access path record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessPath {
    /// Access path id.
    pub id: AccessPathId,
    /// Target host id.
    pub host_id: HostId,
    /// Environment id.
    pub environment_id: EnvironmentId,
    /// Preferred connector id.
    pub connector_id: Option<ConnectorId>,
    /// Protocol.
    pub protocol: Protocol,
    /// Address or hostname.
    pub address: String,
    /// Port.
    pub port: u16,
    /// Username.
    pub username: String,
    /// Credential reference.
    pub credential_id: CredentialId,
    /// Route type.
    pub route_type: RouteType,
    /// Serialized proxy chain.
    pub proxy_chain: Vec<String>,
    /// Lower values are preferred.
    pub priority: i32,
    /// Whether this path is enabled.
    pub enabled: bool,
    /// Connection mode.
    pub connection_mode: ConnectionMode,
    /// Idle transport time-to-live in seconds.
    pub idle_ttl_seconds: u64,
    /// Keepalive interval in seconds.
    pub keepalive_seconds: u64,
    /// Maximum concurrent channels.
    pub max_concurrent_channels: u16,
    /// Maximum new SSH connections per minute.
    pub max_new_connections_per_minute: u16,
    /// Whether a TTY is required.
    pub requires_tty: bool,
    /// Notes.
    pub notes: Option<String>,
}

impl AccessPath {
    /// Returns whether this route requires a separate SSH jump transport.
    ///
    /// A `bastion` route with an empty proxy chain can still be one physical SSH connection,
    /// for example when the gateway selects the final asset from the SSH username.
    #[must_use]
    pub fn requires_multi_hop_transport(&self) -> bool {
        matches!(self.route_type, RouteType::ProxyJump) || !self.proxy_chain.is_empty()
    }
}

/// Credential metadata visible outside the vault.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CredentialMetadata {
    /// Credential id.
    pub id: CredentialId,
    /// Credential name.
    pub name: String,
    /// Credential kind.
    pub kind: CredentialKind,
    /// Username hint.
    pub username_hint: Option<String>,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Updated timestamp.
    pub updated_at: OffsetDateTime,
    /// Last used timestamp.
    pub last_used_at: Option<OffsetDateTime>,
}

/// Encrypted credential record stored by the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredCredential {
    /// Credential metadata.
    pub metadata: CredentialMetadata,
    /// Serialized encrypted vault blob.
    pub encrypted_blob_json: Value,
}

/// Time-aware host fact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostFact {
    /// Fact id.
    pub id: HostFactId,
    /// Host id.
    pub host_id: HostId,
    /// Namespace.
    pub namespace: String,
    /// Key.
    pub key: String,
    /// JSON value.
    pub value_json: Value,
    /// Source.
    pub source: FactSource,
    /// Observation time.
    pub observed_at: OffsetDateTime,
    /// Expiration time.
    pub expires_at: Option<OffsetDateTime>,
    /// Confidence from 0.0 to 1.0.
    pub confidence: f32,
}

/// Software installation record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SoftwareInstall {
    /// Software id.
    pub id: SoftwareInstallId,
    /// Host id.
    pub host_id: HostId,
    /// Software name.
    pub name: String,
    /// Version.
    pub version: Option<String>,
    /// Install path.
    pub install_path: Option<String>,
    /// Config paths.
    pub config_paths: Vec<String>,
    /// Service names.
    pub service_names: Vec<String>,
    /// Ports.
    pub ports: Vec<u16>,
    /// Installing operation id.
    pub installed_by_operation_id: Option<OperationId>,
    /// Notes.
    pub notes: Option<String>,
}

/// One resource in the infrastructure topology graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyNode {
    /// Stable node identifier.
    pub id: TopologyNodeId,
    /// Caller-controlled stable identity used to merge repeated observations.
    pub external_key: String,
    /// Optional link to a host in the primary host registry.
    pub host_id: Option<HostId>,
    /// Human-readable resource name.
    pub name: String,
    /// Resource category.
    pub kind: TopologyNodeKind,
    /// Last reported status.
    pub status: TopologyNodeStatus,
    /// Optional address, DNS name, URL, virtual IP, or subnet.
    pub address: Option<String>,
    /// Exposed or listened ports.
    pub ports: Vec<u16>,
    /// Non-secret extensible inventory attributes.
    pub metadata: Value,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Most recent content update.
    pub updated_at: OffsetDateTime,
    /// Most recent observation in any snapshot.
    pub last_observed_at: OffsetDateTime,
    /// Whether at least one current snapshot still includes the node.
    pub active: bool,
}

/// One directed relationship in the infrastructure topology graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyEdge {
    /// Stable edge identifier.
    pub id: TopologyEdgeId,
    /// Caller-controlled stable identity used to merge repeated observations.
    pub external_key: String,
    /// Relationship source.
    pub source_node_id: TopologyNodeId,
    /// Relationship target.
    pub target_node_id: TopologyNodeId,
    /// Relationship category.
    pub relation: TopologyRelation,
    /// Non-secret extensible relationship attributes.
    pub metadata: Value,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Most recent content update.
    pub updated_at: OffsetDateTime,
    /// Most recent observation in any snapshot.
    pub last_observed_at: OffsetDateTime,
    /// Whether at least one current snapshot still includes the edge.
    pub active: bool,
}

/// Durable result of one topology snapshot reconciliation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologySyncRun {
    /// Sync run identifier.
    pub id: TopologySyncRunId,
    /// Stable reconciliation scope, such as `host:<uuid>` or `cluster:factory-a`.
    pub scope_key: String,
    /// Producer of the snapshot, such as `manual`, `nginx-probe`, or `inventory-agent`.
    pub source: String,
    /// Number of active nodes supplied by the snapshot.
    pub active_node_count: u32,
    /// Number of formerly active nodes omitted by the snapshot.
    pub inactive_node_count: u32,
    /// Number of active edges supplied by the snapshot.
    pub active_edge_count: u32,
    /// Number of formerly active edges omitted by the snapshot.
    pub inactive_edge_count: u32,
    /// Completion timestamp.
    pub completed_at: OffsetDateTime,
}

/// A purpose-specific link from a topology node to encrypted credential metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CredentialBinding {
    /// Binding identifier.
    pub id: CredentialBindingId,
    /// Topology resource that can use the credential.
    pub topology_node_id: TopologyNodeId,
    /// Encrypted credential record.
    pub credential_id: CredentialId,
    /// Human-readable use, such as `admin`, `readonly`, or `database`.
    pub purpose: String,
    /// Binding creation timestamp.
    pub created_at: OffsetDateTime,
}

/// Public credential binding view that never contains decrypted secret material.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CredentialBindingView {
    /// Binding identifier.
    pub id: CredentialBindingId,
    /// Topology resource that can use the credential.
    pub topology_node_id: TopologyNodeId,
    /// Human-readable use.
    pub purpose: String,
    /// Public metadata for the encrypted credential.
    pub credential: CredentialMetadata,
    /// Binding creation timestamp.
    pub created_at: OffsetDateTime,
}

/// Operation run record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationRun {
    /// Operation id.
    pub id: OperationId,
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Connector id.
    pub connector_id: ConnectorId,
    /// Session id.
    pub session_id: Option<SessionId>,
    /// Workspace id that owns the operation, when the operation was submitted through a workspace.
    pub workspace_id: Option<WorkspaceId>,
    /// Agent-client session that submitted the operation.
    pub agent_session_id: Option<AgentSessionId>,
    /// Optional caller-supplied retry key, unique within one agent session.
    pub idempotency_key: Option<String>,
    /// Whether execution must hold the Workspace's scoped host write lease.
    pub requires_write_lease: bool,
    /// Hierarchical write-coordination scope inherited from the owning workspace.
    pub coordination_scope: String,
    /// Operation type.
    pub operation_type: OperationType,
    /// Human or agent intent.
    pub intent: String,
    /// Operation state.
    pub state: OperationState,
    /// Start time.
    pub started_at: OffsetDateTime,
    /// Finish time.
    pub finished_at: Option<OffsetDateTime>,
    /// Exit code.
    pub exit_code: Option<i32>,
    /// Timeout in seconds.
    pub timeout_seconds: u64,
    /// Redacted command summary.
    pub redacted_command_summary: String,
    /// Serialized structured command profile.
    pub command_profile_json: Option<Value>,
    /// Structured evidence of the SSH transport and channel used for execution.
    pub transport_evidence: Option<SshChannelTransportEvidence>,
    /// Redacted output summary.
    pub redacted_output_summary: Option<String>,
    /// Log artifact reference.
    pub log_ref: Option<String>,
    /// Number of times a connector has claimed this operation.
    pub attempt_count: u32,
    /// Current connector claim token.
    pub claim_token: Option<String>,
    /// Claim timestamp.
    pub claimed_at: Option<OffsetDateTime>,
    /// Claim lease expiration timestamp.
    pub lease_expires_at: Option<OffsetDateTime>,
    /// Last redacted execution error.
    pub last_error: Option<String>,
}

/// Redacted output chunk stored for an operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationOutputChunk {
    /// Chunk id.
    pub id: OperationOutputChunkId,
    /// Operation id.
    pub operation_id: OperationId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Output stream.
    pub stream: OutputStream,
    /// Monotonic sequence per operation.
    pub sequence: u64,
    /// Redacted visible text.
    pub redacted_text: String,
    /// UTF-8 byte length of the stored text.
    pub byte_len: u64,
    /// Whether the original output was truncated before storage.
    pub truncated: bool,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
}

/// File-backed redacted output artifact for large streams.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationOutputArtifact {
    /// Artifact id.
    pub id: OperationOutputArtifactId,
    /// Operation id.
    pub operation_id: OperationId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Output stream.
    pub stream: OutputStream,
    /// Relative path below the configured artifact root.
    pub relative_path: String,
    /// UTF-8 byte length of the redacted artifact.
    pub byte_len: u64,
    /// SHA-256 digest of the redacted artifact bytes.
    pub sha256: String,
    /// Redacted preview suitable for agent context.
    pub redacted_preview: String,
    /// Whether the original output exceeded the command output limit before artifact storage.
    pub truncated: bool,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
}

/// Durable knowledge item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KnowledgeItem {
    /// Knowledge id.
    pub id: KnowledgeItemId,
    /// Title.
    pub title: String,
    /// Body.
    pub body: String,
    /// Source.
    pub source: FactSource,
    /// Linked host ids.
    pub linked_host_ids: Vec<HostId>,
    /// Linked access path ids.
    pub linked_access_path_ids: Vec<AccessPathId>,
    /// Linked software ids.
    pub linked_software_ids: Vec<SoftwareInstallId>,
    /// Linked operation ids.
    pub linked_operation_ids: Vec<OperationId>,
    /// Tags.
    pub tags: Vec<String>,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Updated timestamp.
    pub updated_at: OffsetDateTime,
}

/// Access path health snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessPathHealth {
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Current state.
    pub state: EntityState,
    /// Last check timestamp.
    pub last_checked_at: Option<OffsetDateTime>,
    /// Last observed latency.
    pub latency_ms: Option<u64>,
    /// Consecutive failure count.
    pub failure_count: u32,
    /// Last error reason.
    pub last_error_code: Option<StateReasonCode>,
    /// Next retry timestamp.
    pub next_retry_at: Option<OffsetDateTime>,
}

/// Lifecycle state for automatic remote `authorized_keys` bootstrap.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizedKeyBootstrapState {
    /// A bounded install attempt is in progress or protected by a crash cooldown.
    Attempting,
    /// The selected local public key was installed successfully.
    Installed,
    /// A transient failure is cooling down before another bounded attempt.
    Deferred,
    /// Automatic installation is intentionally disabled for this path and key.
    Skipped,
}

/// Stable, agent-visible reason for an `authorized_keys` bootstrap state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizedKeyBootstrapReason {
    /// Authentication succeeded but no local public key was available to install.
    NoLocalPublicKey,
    /// The route contains one or more jump hosts unsupported by the active transport.
    MultiHopUnsupported,
    /// The remote account is not allowed to update the authorized-keys file.
    WriteDenied,
    /// The target filesystem or authorized-keys location is read-only.
    ReadOnlyFilesystem,
    /// The target shell lacks a required non-interactive bootstrap command.
    UnsupportedTargetShell,
    /// The bounded bootstrap command exceeded its own timeout.
    Timeout,
    /// The bounded remote command failed for another non-secret reason.
    RemoteCommandFailed,
    /// The bounded retry budget was exhausted.
    AttemptsExhausted,
}

/// Persisted automatic public-key bootstrap state for one access path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorizedKeyBootstrap {
    /// Access path whose final SSH account is affected.
    pub access_path_id: AccessPathId,
    /// Current bootstrap lifecycle state.
    pub state: AuthorizedKeyBootstrapState,
    /// Stable reason for non-installed states.
    pub reason: Option<AuthorizedKeyBootstrapReason>,
    /// SHA-256 fingerprint of the selected local public key, when available.
    pub public_key_fingerprint: Option<String>,
    /// Number of failed remote installation attempts for this key.
    pub failure_count: u32,
    /// Most recent attempt or decision timestamp.
    pub attempted_at: OffsetDateTime,
    /// Earliest automatic retry time for deferred/attempting states.
    pub next_retry_at: Option<OffsetDateTime>,
    /// Last persisted state update.
    pub updated_at: OffsetDateTime,
}

/// SSH connection session state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionSession {
    /// Session id.
    pub session_id: SessionId,
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Connector id.
    pub connector_id: ConnectorId,
    /// Session state.
    pub state: EntityState,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Last used timestamp.
    pub last_used_at: OffsetDateTime,
    /// Open channel count.
    pub open_channels: u32,
    /// Reuse count.
    pub reused_count: u64,
    /// Consecutive failure count.
    pub failure_count: u32,
    /// Last error.
    pub last_error: Option<String>,
}

/// Connector SSH backend that owns a reusable transport runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshTransportBackend {
    /// Backend is unavailable or was not reported by an older connector.
    Unknown,
    /// OpenSSH native multiplexing through one control master.
    OpenSshControlMaster,
    /// Native asynchronous SSH through `russh`.
    Russh,
}

/// Current connector-local lifecycle state for a reusable SSH transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshTransportRuntimeState {
    /// The transport object exists but has not opened an SSH connection.
    Cold,
    /// A new network connection and SSH handshake are in progress.
    Connecting,
    /// The cached SSH connection is available for additional channels.
    Ready,
    /// The cached SSH connection was intentionally released after its idle TTL elapsed.
    Idle,
    /// The cached SSH connection failed validation or a handshake failed.
    Disconnected,
    /// The owning connector process restarted and destroyed the in-memory runtime.
    RuntimeLost,
}

/// File-transfer channel used by one SSH transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshFileTransferMode {
    /// Native SFTP subsystem over the pooled SSH connection.
    Sftp,
    /// Framed POSIX exec-channel fallback over the pooled SSH connection.
    ExecFramed,
    /// No verified file-transfer channel is available.
    Unavailable,
}

/// Optional capability implemented by an SSH transport runtime.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshTransportFeature {
    /// Multiple channels reuse one authenticated SSH transport.
    ReusesSshTransport,
    /// Non-interactive exec channels are supported.
    Exec,
    /// The connector can attach a persistent managed PTY.
    PersistentPty,
    /// Local or remote port forwarding is implemented.
    PortForwarding,
    /// A typed multi-hop proxy chain is implemented.
    MultiHop,
}

/// Stable capabilities of one connector-local SSH transport runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SshTransportCapabilities {
    /// Supported transport features.
    pub features: BTreeSet<SshTransportFeature>,
    /// File-transfer channel selected for this route.
    pub file_transfer_mode: SshFileTransferMode,
}

impl SshTransportCapabilities {
    /// Capabilities of the current pooled exec, PTY, and file-transfer transports.
    #[must_use]
    pub fn pooled(file_transfer_mode: SshFileTransferMode) -> Self {
        Self {
            features: BTreeSet::from([
                SshTransportFeature::ReusesSshTransport,
                SshTransportFeature::Exec,
                SshTransportFeature::PersistentPty,
            ]),
            file_transfer_mode,
        }
    }

    /// Capabilities used when a connector cannot report its transport backend.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            features: BTreeSet::new(),
            file_transfer_mode: SshFileTransferMode::Unavailable,
        }
    }
}

/// In-memory transport telemetry safe to persist and expose to agents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SshTransportTelemetry {
    /// Identity of one connector-local transport object.
    pub runtime_id: SshTransportRuntimeId,
    /// SSH implementation serving the runtime.
    pub backend: SshTransportBackend,
    /// Current connector-local lifecycle state.
    pub state: SshTransportRuntimeState,
    /// Successful SSH connection generation within this runtime.
    pub generation: u64,
    /// Network connection attempts that passed local handshake budgets.
    pub connection_attempt_count: u64,
    /// Successful authenticated SSH handshakes.
    pub successful_handshake_count: u64,
    /// Successful validations and channel opens on an existing SSH connection.
    pub reuse_count: u64,
    /// Most recent successful authenticated handshake.
    pub last_handshake_at: Option<OffsetDateTime>,
    /// Most recent successful validation of a cached SSH connection.
    pub last_validated_at: Option<OffsetDateTime>,
    /// Backend and route capabilities.
    pub capabilities: SshTransportCapabilities,
}

/// Latest persisted transport telemetry for one connector and access path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SshTransportRuntime {
    /// Access path served by this runtime.
    pub access_path_id: AccessPathId,
    /// Connector process that owns the runtime.
    pub connector_id: ConnectorId,
    /// Latest connector-local telemetry.
    pub telemetry: SshTransportTelemetry,
    /// Persistence timestamp.
    pub updated_at: OffsetDateTime,
}

/// SSH channel category used by an operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshChannelKind {
    /// Non-interactive remote command channel.
    Exec,
    /// Managed file-transfer channel.
    FileTransfer,
    /// Persistent interactive terminal channel.
    Pty,
}

/// Mutually exclusive connection behavior observed while opening one SSH channel.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshConnectionUse {
    /// No connection attempt or cached-connection validation was observed.
    Unchanged,
    /// An existing authenticated SSH connection served the channel.
    Reused,
    /// This runtime completed its first authenticated SSH handshake.
    FirstHandshake,
    /// The same runtime replaced a previously authenticated connection.
    Reconnected,
    /// A real connection attempt started but did not complete an authenticated handshake.
    AttemptFailed,
}

/// Structured proof of how one exec, file, or PTY channel used the SSH transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SshChannelTransportEvidence {
    /// Connector-local transport runtime used by the operation.
    pub runtime_id: SshTransportRuntimeId,
    /// SSH backend used by the operation.
    pub backend: SshTransportBackend,
    /// SSH connection generation used by the channel.
    pub generation: u64,
    /// Channel category used by the operation.
    pub channel_kind: SshChannelKind,
    /// Mutually exclusive connection behavior observed for the channel.
    pub connection_use: SshConnectionUse,
    /// Whether a different connector-local runtime replaced the previous one.
    pub runtime_replaced: bool,
    /// Total successful handshakes in this runtime after the operation.
    pub successful_handshake_count: u64,
    /// Total cached-connection reuses in this runtime after the operation.
    pub reuse_count: u64,
    /// Capabilities observed for the selected transport.
    pub capabilities: SshTransportCapabilities,
    /// Time when the evidence was captured.
    pub observed_at: OffsetDateTime,
}

impl SshChannelTransportEvidence {
    /// Compares transport telemetry before and after an operation.
    #[must_use]
    pub fn between(
        channel_kind: SshChannelKind,
        before: Option<&SshTransportTelemetry>,
        after: &SshTransportTelemetry,
        observed_at: OffsetDateTime,
    ) -> Self {
        let same_runtime = before.is_some_and(|before| before.runtime_id == after.runtime_id);
        let runtime_replaced = before.is_some_and(|before| before.runtime_id != after.runtime_id);
        let connection_attempted = before.map_or(after.connection_attempt_count > 0, |before| {
            !same_runtime || after.connection_attempt_count > before.connection_attempt_count
        });
        let handshake_performed = before.map_or(after.successful_handshake_count > 0, |before| {
            !same_runtime || after.successful_handshake_count > before.successful_handshake_count
        });
        let transport_reused = same_runtime
            && before.is_some_and(|before| after.reuse_count > before.reuse_count)
            && !handshake_performed;
        let reconnect_performed = same_runtime
            && handshake_performed
            && before.is_some_and(|before| before.successful_handshake_count > 0);
        let connection_use = if reconnect_performed {
            SshConnectionUse::Reconnected
        } else if handshake_performed {
            SshConnectionUse::FirstHandshake
        } else if transport_reused {
            SshConnectionUse::Reused
        } else if connection_attempted {
            SshConnectionUse::AttemptFailed
        } else {
            SshConnectionUse::Unchanged
        };

        Self {
            runtime_id: after.runtime_id,
            backend: after.backend.clone(),
            generation: after.generation,
            channel_kind,
            connection_use,
            runtime_replaced,
            successful_handshake_count: after.successful_handshake_count,
            reuse_count: after.reuse_count,
            capabilities: after.capabilities.clone(),
            observed_at,
        }
    }
}

/// Persistent PTY session state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PtySession {
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Connection session id.
    pub session_id: SessionId,
    /// PTY state.
    pub state: WorkspaceState,
    /// Foreground process summary.
    pub foreground_process: Option<String>,
    /// Current working directory.
    pub cwd: Option<String>,
    /// Recent output artifact reference.
    pub recent_output_ref: Option<String>,
    /// Last process exit code.
    pub last_exit_code: Option<i32>,
    /// Whether input is allowed.
    pub input_allowed: bool,
    /// Current connector backend state for this PTY process.
    pub backend_state: PtyBackendState,
    /// Capabilities reported by the connector backend.
    pub backend_capabilities: PtyBackendCapabilities,
    /// Latest live input request, if the connector recognized one.
    pub interaction: Option<PtyInteraction>,
    /// Structured evidence of the SSH transport used to open this PTY channel.
    pub transport_evidence: Option<SshChannelTransportEvidence>,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Last activity timestamp.
    pub last_activity_at: OffsetDateTime,
}

/// Redacted output chunk stored for a persistent PTY session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PtyOutputChunk {
    /// Chunk id.
    pub id: PtyOutputChunkId,
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Output stream.
    pub stream: OutputStream,
    /// Monotonic sequence per PTY session.
    pub sequence: u64,
    /// Redacted visible text.
    pub redacted_text: String,
    /// UTF-8 byte length of the stored text.
    pub byte_len: u64,
    /// Whether the original output was truncated before storage.
    pub truncated: bool,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
}

/// Public metadata for a queued persistent PTY input event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyInputEvent {
    /// Input event id.
    pub id: PtyInputEventId,
    /// PTY session id.
    pub pty_session_id: PtySessionId,
    /// Workspace id.
    pub workspace_id: WorkspaceId,
    /// Connector id that owns delivery.
    pub connector_id: ConnectorId,
    /// Host whose interactive shell receives the input.
    pub host_id: HostId,
    /// Agent-client session that queued the input.
    pub agent_session_id: Option<AgentSessionId>,
    /// Optional caller-supplied retry key, unique within one agent session.
    pub idempotency_key: Option<String>,
    /// Source used to resolve the private input payload.
    pub payload_kind: PtyInputPayloadKind,
    /// Non-reversible input digest used only to reject mismatched idempotent retries.
    #[serde(skip)]
    pub input_fingerprint: Option<String>,
    /// Event state.
    pub state: PtyInputEventState,
    /// Monotonic sequence per PTY session.
    pub sequence: u64,
    /// Redacted input summary safe for API/MCP responses.
    pub redacted_input_summary: String,
    /// Original input byte length.
    pub byte_len: u64,
    /// Optional requester label.
    pub requested_by: Option<String>,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Claim timestamp.
    pub claimed_at: Option<OffsetDateTime>,
    /// Claim lease expiration timestamp.
    pub lease_expires_at: Option<OffsetDateTime>,
    /// Delivery timestamp.
    pub delivered_at: Option<OffsetDateTime>,
    /// Failure timestamp.
    pub failed_at: Option<OffsetDateTime>,
    /// Connector claim attempts.
    pub attempt_count: u32,
    /// Last redacted delivery error.
    pub last_error: Option<String>,
}

/// Internal claimed PTY input event including the payload for connector-owned delivery.
#[derive(Clone, Debug)]
pub struct ClaimedPtyInputEvent {
    /// Public input event metadata.
    pub event: PtyInputEvent,
    /// Raw input text to write to the PTY. This must not be returned through API/MCP.
    pub input_text: String,
    /// Claim token owned by the connector pump.
    pub claim_token: String,
}

/// State transition event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateEvent {
    /// Event id.
    pub id: StateEventId,
    /// Entity type.
    pub entity_type: String,
    /// Entity id as string.
    pub entity_id: String,
    /// Old state.
    pub old_state: EntityState,
    /// New state.
    pub new_state: EntityState,
    /// Reason code.
    pub reason_code: StateReasonCode,
    /// Observation timestamp.
    pub observed_at: OffsetDateTime,
}

/// Persisted state transition event with its global monotonic cursor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequencedStateEvent {
    /// Global event sequence used for replay and resume cursors.
    pub sequence: u64,
    /// State transition payload.
    #[serde(flatten)]
    pub event: StateEvent,
}

/// Agent-facing state snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// Entity state.
    pub state: EntityState,
    /// Observation time.
    pub observed_at: OffsetDateTime,
    /// State age in seconds.
    pub state_age_seconds: u64,
    /// Confidence from 0.0 to 1.0.
    pub confidence: f32,
    /// Reason code.
    pub reason_code: StateReasonCode,
    /// Human-facing message.
    pub human_message: String,
    /// Suggested agent action.
    pub agent_hint: Option<AgentHint>,
    /// Retry delay in seconds.
    pub retry_after_seconds: Option<u64>,
}

/// Agent workspace record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentWorkspace {
    /// Workspace id.
    pub id: WorkspaceId,
    /// Agent-client session that owns this workspace.
    pub agent_session_id: Option<AgentSessionId>,
    /// Host id.
    pub host_id: HostId,
    /// Access path id.
    pub access_path_id: AccessPathId,
    /// Connector id.
    pub connector_id: ConnectorId,
    /// Human label.
    pub label: String,
    /// Working directory.
    pub cwd: Option<String>,
    /// Workspace state.
    pub state: WorkspaceState,
    /// Policy profile.
    pub policy_profile: String,
    /// Hierarchical write-coordination scope. `host` preserves whole-host exclusion.
    pub coordination_scope: String,
    /// Created timestamp.
    pub created_at: OffsetDateTime,
    /// Last activity timestamp.
    pub last_activity_at: OffsetDateTime,
    /// Workspace TTL in seconds.
    pub ttl_seconds: u64,
}

/// Exclusive write coordination lease for one host and hierarchical resource scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostWriteLease {
    /// Host protected from cross-session write interleaving.
    pub host_id: HostId,
    /// Protected scope. `host` conflicts with every scope on the host.
    pub coordination_scope: String,
    /// Agent session currently allowed to submit mutations.
    pub holder_agent_session_id: AgentSessionId,
    /// Most recently active workspace for the holder.
    pub holder_workspace_id: WorkspaceId,
    /// Time the current lease ownership was acquired or refreshed.
    pub acquired_at: OffsetDateTime,
    /// Most recent queue or connector heartbeat.
    pub heartbeat_at: OffsetDateTime,
    /// Crash-safe lease expiration.
    pub expires_at: OffsetDateTime,
}

#[cfg(test)]
mod tests {
    use super::{
        AccessPathId, EntityState, HostId, OperationState, SshChannelKind,
        SshChannelTransportEvidence, SshConnectionUse, SshFileTransferMode, SshTransportBackend,
        SshTransportCapabilities, SshTransportRuntimeId, SshTransportRuntimeState,
        SshTransportTelemetry, now_utc,
    };

    #[test]
    fn ids_round_trip_as_strings() -> Result<(), uuid::Error> {
        let id = HostId::new();
        let parsed: HostId = id.to_string().parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }

    #[test]
    fn ids_are_distinct_types() {
        let host_id = HostId::new();
        let access_path_id = AccessPathId::new();
        assert_ne!(host_id.to_string(), access_path_id.to_string());
    }

    #[test]
    fn state_serializes_to_snake_case() -> Result<(), serde_json::Error> {
        let value = serde_json::to_value(EntityState::TargetOverloaded)?;
        assert_eq!(value, serde_json::json!("target_overloaded"));
        let value = serde_json::to_value(OperationState::Exhausted)?;
        assert_eq!(value, serde_json::json!("exhausted"));
        Ok(())
    }

    #[test]
    fn operation_transport_evidence_distinguishes_reuse_from_reconnect() {
        let runtime_id = SshTransportRuntimeId::new();
        let observed_at = now_utc();
        let capabilities = SshTransportCapabilities::pooled(SshFileTransferMode::Sftp);
        let first = SshTransportTelemetry {
            runtime_id,
            backend: SshTransportBackend::Russh,
            state: SshTransportRuntimeState::Ready,
            generation: 1,
            connection_attempt_count: 1,
            successful_handshake_count: 1,
            reuse_count: 0,
            last_handshake_at: Some(observed_at),
            last_validated_at: Some(observed_at),
            capabilities: capabilities.clone(),
        };
        let reused = SshTransportTelemetry {
            reuse_count: 1,
            ..first.clone()
        };
        let reuse_evidence = SshChannelTransportEvidence::between(
            SshChannelKind::Exec,
            Some(&first),
            &reused,
            observed_at,
        );
        assert_eq!(reuse_evidence.connection_use, SshConnectionUse::Reused);
        assert_eq!(reuse_evidence.generation, 1);

        let reconnected = SshTransportTelemetry {
            generation: 2,
            connection_attempt_count: 2,
            successful_handshake_count: 2,
            state: SshTransportRuntimeState::Ready,
            ..reused
        };
        let reconnect_evidence = SshChannelTransportEvidence::between(
            SshChannelKind::Exec,
            Some(&first),
            &reconnected,
            observed_at,
        );
        assert_eq!(
            reconnect_evidence.connection_use,
            SshConnectionUse::Reconnected
        );
        assert_eq!(reconnect_evidence.generation, 2);

        let replacement = SshTransportTelemetry {
            runtime_id: SshTransportRuntimeId::new(),
            generation: 1,
            connection_attempt_count: 1,
            successful_handshake_count: 1,
            reuse_count: 0,
            ..first.clone()
        };
        let replacement_evidence = SshChannelTransportEvidence::between(
            SshChannelKind::Exec,
            Some(&first),
            &replacement,
            observed_at,
        );
        assert!(replacement_evidence.runtime_replaced);
        assert_eq!(
            replacement_evidence.connection_use,
            SshConnectionUse::FirstHandshake
        );
    }
}

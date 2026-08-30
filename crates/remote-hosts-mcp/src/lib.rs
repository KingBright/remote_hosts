//! MCP tool contracts and server handlers for remote hosts.

use std::{
    collections::BTreeSet,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use remote_hosts_core::{
    AccessCandidate, AccessResolutionError, AccessResolver, CommandProfileCatalog,
    ConnectorStateTracker, DEFAULT_SFTP_MAX_SIZE_BYTES, DEFAULT_SFTP_TIMEOUT_SECONDS,
    FileTransferSpec, HostStateAggregator, HostStateInput, OperationCoordinationMode,
    PtySessionHeartbeatCommand, PtySessionInputCommand, PtySessionOpenCommand,
    PtySessionSupervisor, ServerProtectionPolicy, SftpDirection, SftpOverwritePolicy,
    WorkspaceCreateCommand, WorkspaceFileTransfer, WorkspaceOperationSupervisor,
    WorkspaceRunCommand, WorkspaceSupervisor, common_coordination_scope,
    resolve_operation_coordination_scopes,
};
use remote_hosts_db::{DbError, Repositories, WorkspaceCapacityStatus, retry_sqlite_contention};
use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId, AgentSessionState,
    AgentWorkspace, ConnectionMode, ConnectionSession, Connector, ConnectorId, CredentialId,
    CredentialKind, CredentialMetadata, EntityState, Environment, EnvironmentId, EnvironmentKind,
    FactSource, Host, HostFact, HostFactId, HostId, HostKind, HostWriteLease, InstancePeerId,
    InstanceSyncCollection, KnowledgeItem, KnowledgeItemId, OperationId, OperationOutputArtifactId,
    OperationRun, OperationState, Protocol, PtyBackendState, PtyInputEvent, PtySession,
    PtySessionId, RiskLevel, RouteType, SessionId, SoftwareInstallId, StateReasonCode,
    StateSnapshot, StoredCredential, TrustLevel, WorkspaceId, WorkspaceState, now_utc,
};
use remote_hosts_sync::InstanceSyncService;
use rmcp::{
    Json, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

use remote_hosts_vault::{CredentialSecret, CredentialVault, EncryptedCredentialBlob};

/// MCP server name.
pub const SERVER_NAME: &str = "remote-hosts";
const DEFAULT_ARTIFACT_ROOT: &str = "remote-hosts-artifacts";
const DEFAULT_ARTIFACT_READ_BYTES: usize = 64 * 1024;
const MAX_ARTIFACT_READ_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_CONCURRENT_CHANNELS: u16 = 8;
const WRITE_LEASE_SECONDS: i64 = 300;

/// Stable MCP tool names.
pub mod tools {
    /// List hosts.
    pub const LIST_HOSTS: &str = "remote_hosts_list_hosts";
    /// Idempotently register or update a host and optional SSH access path.
    pub const ENSURE_HOST: &str = "remote_hosts_ensure_host";
    /// Store encrypted SSH credential material for one host access path.
    pub const STORE_HOST_CREDENTIAL: &str = "remote_hosts_store_host_credential";
    /// Find possible duplicate hosts.
    pub const FIND_HOST_DUPLICATES: &str = "remote_hosts_find_host_duplicates";
    /// Insert or update a host.
    pub const UPSERT_HOST: &str = "remote_hosts_upsert_host";
    /// Get one host.
    pub const GET_HOST: &str = "remote_hosts_get_host";
    /// List environments.
    pub const LIST_ENVIRONMENTS: &str = "remote_hosts_list_environments";
    /// Insert or update an environment.
    pub const UPSERT_ENVIRONMENT: &str = "remote_hosts_upsert_environment";
    /// List credential metadata.
    pub const LIST_CREDENTIALS: &str = "remote_hosts_list_credentials";
    /// Insert or update a credential reference without secret material.
    pub const UPSERT_CREDENTIAL_REF: &str = "remote_hosts_upsert_credential_ref";
    /// Insert or update an access path.
    pub const UPSERT_ACCESS_PATH: &str = "remote_hosts_upsert_access_path";
    /// Record a host fact.
    pub const RECORD_HOST_FACT: &str = "remote_hosts_record_host_fact";
    /// Record a knowledge item.
    pub const RECORD_KNOWLEDGE: &str = "remote_hosts_record_knowledge";
    /// Search knowledge.
    pub const SEARCH_KNOWLEDGE: &str = "remote_hosts_search_knowledge";
    /// Resolve access path.
    pub const RESOLVE_ACCESS: &str = "remote_hosts_resolve_access";
    /// Get host state.
    pub const GET_HOST_STATE: &str = "remote_hosts_get_host_state";
    /// Get a consistent host runtime snapshot.
    pub const GET_HOST_RUNTIME_SNAPSHOT: &str = "remote_hosts_get_host_runtime_snapshot";
    /// Record connector heartbeat.
    pub const CONNECTOR_HEARTBEAT: &str = "remote_hosts_connector_heartbeat";
    /// List connector state events.
    pub const LIST_CONNECTOR_EVENTS: &str = "remote_hosts_list_connector_events";
    /// Wait for sequenced runtime state events.
    pub const WAIT_RUNTIME_EVENTS: &str = "remote_hosts_wait_runtime_events";
    /// Refresh state.
    pub const REFRESH_STATE: &str = "remote_hosts_refresh_state";
    /// Get server protection state.
    pub const GET_SERVER_PROTECTION_STATE: &str = "remote_hosts_get_server_protection_state";
    /// List command profiles.
    pub const LIST_COMMAND_PROFILES: &str = "remote_hosts_list_command_profiles";
    /// List workspaces for a host.
    pub const LIST_WORKSPACES: &str = "remote_hosts_list_workspaces";
    /// Create workspace.
    pub const CREATE_WORKSPACE: &str = "remote_hosts_create_workspace";
    /// Reuse or create one workspace and return execution context.
    pub const PREPARE_WORKSPACE: &str = "remote_hosts_prepare_workspace";
    /// Get one workspace.
    pub const GET_WORKSPACE: &str = "remote_hosts_get_workspace";
    /// Update workspace state.
    pub const UPDATE_WORKSPACE_STATE: &str = "remote_hosts_update_workspace_state";
    /// List PTY sessions for a workspace.
    pub const LIST_WORKSPACE_PTY_SESSIONS: &str = "remote_hosts_list_workspace_pty_sessions";
    /// Open a PTY session for a workspace.
    pub const OPEN_WORKSPACE_PTY_SESSION: &str = "remote_hosts_open_workspace_pty_session";
    /// Update a PTY session heartbeat.
    pub const HEARTBEAT_PTY_SESSION: &str = "remote_hosts_heartbeat_pty_session";
    /// Read PTY output chunks.
    pub const READ_PTY_OUTPUT: &str = "remote_hosts_read_pty_output";
    /// Queue PTY input.
    pub const QUEUE_PTY_INPUT: &str = "remote_hosts_queue_pty_input";
    /// List PTY input events.
    pub const LIST_PTY_INPUT_EVENTS: &str = "remote_hosts_list_pty_input_events";
    /// Close a PTY session.
    pub const CLOSE_PTY_SESSION: &str = "remote_hosts_close_pty_session";
    /// Reap expired PTY sessions.
    pub const REAP_EXPIRED_PTY_SESSIONS: &str = "remote_hosts_reap_expired_pty_sessions";
    /// Close a workspace.
    pub const CLOSE_WORKSPACE: &str = "remote_hosts_close_workspace";
    /// Run command in workspace.
    pub const RUN_IN_WORKSPACE: &str = "remote_hosts_run_in_workspace";
    /// Upload one connector-local file through the workspace's pooled SSH session.
    pub const UPLOAD_FILE: &str = "remote_hosts_upload_file";
    /// Download one remote file through the workspace's pooled SSH session.
    pub const DOWNLOAD_FILE: &str = "remote_hosts_download_file";
    /// Read workspace output.
    pub const READ_WORKSPACE_OUTPUT: &str = "remote_hosts_read_workspace_output";
    /// Read one combined workspace result.
    pub const GET_WORKSPACE_RESULT: &str = "remote_hosts_get_workspace_result";
    /// List workspace output artifacts.
    pub const LIST_WORKSPACE_OUTPUT_ARTIFACTS: &str =
        "remote_hosts_list_workspace_output_artifacts";
    /// Get output artifact metadata.
    pub const GET_OUTPUT_ARTIFACT: &str = "remote_hosts_get_output_artifact";
    /// Read a bounded chunk of a redacted output artifact.
    pub const READ_OUTPUT_ARTIFACT_CONTENT: &str = "remote_hosts_read_output_artifact_content";
    /// Wait for workspace state.
    pub const WAIT_WORKSPACE_STATE: &str = "remote_hosts_wait_workspace_state";
    /// Configure one approved Remote Hosts instance peer without returning its token.
    pub const CONFIGURE_INSTANCE_SYNC_PEER: &str = "remote_hosts_configure_instance_sync_peer";
    /// Push selected durable metadata directly to one configured peer.
    pub const SYNC_INSTANCE_PEER: &str = "remote_hosts_sync_instance_peer";
}

const AGENT_TOOL_NAMES: &[&str] = &[
    tools::LIST_HOSTS,
    tools::ENSURE_HOST,
    tools::STORE_HOST_CREDENTIAL,
    tools::GET_HOST_RUNTIME_SNAPSHOT,
    tools::SEARCH_KNOWLEDGE,
    tools::RECORD_KNOWLEDGE,
    tools::PREPARE_WORKSPACE,
    tools::RUN_IN_WORKSPACE,
    tools::UPLOAD_FILE,
    tools::DOWNLOAD_FILE,
    tools::WAIT_WORKSPACE_STATE,
    tools::GET_WORKSPACE_RESULT,
    tools::READ_OUTPUT_ARTIFACT_CONTENT,
    tools::OPEN_WORKSPACE_PTY_SESSION,
    tools::HEARTBEAT_PTY_SESSION,
    tools::QUEUE_PTY_INPUT,
    tools::READ_PTY_OUTPUT,
    tools::CLOSE_PTY_SESSION,
    tools::WAIT_RUNTIME_EVENTS,
    tools::CONFIGURE_INSTANCE_SYNC_PEER,
    tools::SYNC_INSTANCE_PEER,
];

const ADMIN_TOOL_NAMES: &[&str] = &[
    tools::FIND_HOST_DUPLICATES,
    tools::UPSERT_HOST,
    tools::GET_HOST,
    tools::LIST_ENVIRONMENTS,
    tools::UPSERT_ENVIRONMENT,
    tools::LIST_CREDENTIALS,
    tools::UPSERT_CREDENTIAL_REF,
    tools::UPSERT_ACCESS_PATH,
    tools::RECORD_HOST_FACT,
    tools::RESOLVE_ACCESS,
    tools::GET_HOST_STATE,
    tools::LIST_CONNECTOR_EVENTS,
    tools::GET_SERVER_PROTECTION_STATE,
    tools::LIST_COMMAND_PROFILES,
    tools::LIST_WORKSPACES,
    tools::GET_WORKSPACE,
    tools::LIST_WORKSPACE_PTY_SESSIONS,
    tools::CLOSE_WORKSPACE,
];

/// MCP tool visibility profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolProfile {
    /// Compact task-oriented surface for normal agent operation.
    Agent,
    /// Agent surface plus host registry and operational maintenance tools.
    Admin,
    /// Every registered MCP tool, intended for debugging and development.
    Full,
}

/// Optional client identity supplied by an MCP launcher.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentSessionContext {
    /// Client family, for example `codex` or `antigravity`.
    pub client_kind: Option<String>,
    /// Stable client instance or conversation-process key.
    pub client_instance_id: Option<String>,
    /// Optional project-level isolation key.
    pub project_key: Option<String>,
    /// Optional conversation-level isolation key.
    pub conversation_key: Option<String>,
}

impl AgentSessionContext {
    fn into_session(self) -> AgentSession {
        let client_kind = non_empty_or(self.client_kind, "mcp");
        let supplied_client_instance_id = trim_optional(self.client_instance_id);
        let project_key = trim_optional(self.project_key);
        let conversation_key = trim_optional(self.conversation_key);
        let stable_context = supplied_client_instance_id.is_some() || conversation_key.is_some();
        let id = if stable_context {
            let identity = format!(
                "remote-hosts-agent-session\0{client_kind}\0{}\0{}\0{}",
                supplied_client_instance_id.as_deref().unwrap_or_default(),
                project_key.as_deref().unwrap_or_default(),
                conversation_key.as_deref().unwrap_or_default()
            );
            AgentSessionId::from(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                identity.as_bytes(),
            ))
        } else {
            AgentSessionId::new()
        };
        let now = now_utc();
        AgentSession {
            id,
            client_kind,
            client_instance_id: supplied_client_instance_id.unwrap_or_else(|| id.to_string()),
            project_key,
            conversation_key,
            state: AgentSessionState::Active,
            created_at: now,
            last_seen_at: now,
            expires_at: now + time::Duration::hours(24),
        }
    }
}

fn non_empty_or(value: Option<String>, fallback: &str) -> String {
    trim_optional(value).unwrap_or_else(|| fallback.to_owned())
}

impl ToolProfile {
    /// Stable profile name used by CLI and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Admin => "admin",
            Self::Full => "full",
        }
    }

    fn includes(self, name: &str) -> bool {
        match self {
            Self::Agent => AGENT_TOOL_NAMES.contains(&name),
            Self::Admin => AGENT_TOOL_NAMES.contains(&name) || ADMIN_TOOL_NAMES.contains(&name),
            Self::Full => true,
        }
    }
}

/// Host id request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct HostIdRequest {
    /// Host id as UUID string.
    pub host_id: String,
}

/// Host duplicate search request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FindHostDuplicatesRequest {
    /// Stable host slug or proposed slug.
    pub name: Option<String>,
    /// Human-facing display name.
    pub display_name: Option<String>,
    /// Candidate SSH address or hostname.
    pub address: Option<String>,
    /// Candidate SSH port.
    pub port: Option<u16>,
    /// Candidate SSH username.
    pub username: Option<String>,
}

/// Upsert host request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertHostRequest {
    /// Stable lowercase slug. Use letters, digits, and hyphens.
    pub name: String,
    /// Human-facing display name.
    pub display_name: String,
    /// Host kind, for example `macos`, `linux`, `gpu_server`, or `customer_server`.
    pub kind: String,
    /// Risk level, for example `personal`, `development`, `production`, or `customer_site`.
    pub risk_level: String,
    /// Optional owner.
    pub owner: Option<String>,
    /// Tags.
    pub tags: Option<Vec<String>>,
    /// Optional description.
    pub description: Option<String>,
}

/// Idempotent task-level host registration request.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnsureHostRequest {
    /// Proposed stable host slug. Existing canonical slugs are preserved when another identity matches.
    pub name: String,
    /// Human-facing display name. Defaults to the proposed name.
    pub display_name: Option<String>,
    /// Host kind, for example `macos`, `windows`, `linux`, or `gpu_server`.
    pub kind: String,
    /// Risk level, for example `personal`, `development`, `production`, or `customer_site`.
    pub risk_level: String,
    /// Optional owner. Omission preserves the existing value.
    pub owner: Option<String>,
    /// Tags to merge with existing tags.
    pub tags: Option<Vec<String>>,
    /// Optional description. Omission preserves the existing value.
    pub description: Option<String>,
    /// Optional SSH access path to register in the same task.
    pub access: Option<EnsureHostAccessRequest>,
}

/// SSH access details accepted by task-level host registration.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnsureHostAccessRequest {
    /// SSH address or hostname.
    pub address: String,
    /// SSH port. Defaults to 22.
    pub port: Option<u16>,
    /// SSH username.
    pub username: String,
    /// Stable environment name, for example `home-lan` or `public-internet`.
    pub environment_name: String,
    /// Environment kind, for example `home_lan`, `company_lan`, `vpn`, or `public_internet`.
    pub environment_kind: String,
    /// Environment trust level: `owned`, `trusted`, `external`, or `untrusted`.
    pub trust_level: String,
    /// Route type: `lan`, `public`, `frp`, `vpn`, `proxy_jump`, or `bastion`.
    pub route_type: String,
    /// Optional connector id. When omitted, a single healthy connector is selected automatically.
    pub connector_id: Option<String>,
    /// Existing or proposed non-secret credential reference name. Defaults to `openssh-default`.
    pub credential_name: Option<String>,
    /// Credential kind used only when the reference must be created. Defaults to `ssh_private_key`.
    pub credential_kind: Option<String>,
    /// Optional secret material to encrypt in the local vault. Values are never returned.
    pub credential_secret: Option<CredentialSecretRequest>,
    /// Optional proxy chain.
    pub proxy_chain: Option<Vec<String>>,
    /// Lower values are preferred. Defaults to 100.
    pub priority: Option<i32>,
    /// Whether this path is enabled. Defaults to true.
    pub enabled: Option<bool>,
    /// Connection mode. Defaults to `pooled`.
    pub connection_mode: Option<String>,
    /// Idle transport TTL in seconds. Defaults to 600.
    pub idle_ttl_seconds: Option<u64>,
    /// Keepalive interval in seconds. Defaults to 30.
    pub keepalive_seconds: Option<u64>,
    /// Max concurrent SSH channels. Defaults to 8.
    pub max_concurrent_channels: Option<u16>,
    /// Max new SSH connections per minute. Defaults to 1.
    pub max_new_connections_per_minute: Option<u16>,
    /// Whether the path requires TTY semantics. Defaults to false.
    pub requires_tty: Option<bool>,
    /// Optional non-secret notes.
    pub notes: Option<String>,
}

/// Secret SSH material accepted for encrypted local storage.
///
/// This type intentionally does not implement `Debug`, `Clone`, or `Serialize`.
#[derive(Deserialize, JsonSchema, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct CredentialSecretRequest {
    /// SSH or Windows account password.
    pub password: Option<String>,
    /// PEM or OpenSSH private key.
    pub private_key_pem: Option<String>,
    /// Private key passphrase.
    pub private_key_passphrase: Option<String>,
    /// Optional sudo or administrator password.
    pub sudo_password: Option<String>,
    /// Also try identities from the connector process's SSH agent. Defaults to true.
    #[serde(default = "default_true")]
    pub use_ssh_agent: bool,
}

/// Store or update encrypted credential material for an existing host route.
///
/// This type intentionally does not implement `Debug`, `Clone`, or `Serialize`.
#[derive(Deserialize, JsonSchema, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct StoreHostCredentialRequest {
    /// Canonical host id.
    pub host_id: String,
    /// Access path id. May be omitted when the host has exactly one path.
    pub access_path_id: Option<String>,
    /// SSH or Windows account password.
    pub password: Option<String>,
    /// PEM or OpenSSH private key.
    pub private_key_pem: Option<String>,
    /// Private key passphrase.
    pub private_key_passphrase: Option<String>,
    /// Optional sudo or administrator password.
    pub sudo_password: Option<String>,
    /// Also try identities from the connector process's SSH agent. Defaults to true.
    #[serde(default = "default_true")]
    pub use_ssh_agent: bool,
}

impl CredentialSecretRequest {
    fn stored_fields(&self) -> Vec<String> {
        [
            ("password", self.password.is_some()),
            ("private_key_pem", self.private_key_pem.is_some()),
            (
                "private_key_passphrase",
                self.private_key_passphrase.is_some(),
            ),
            ("sudo_password", self.sudo_password.is_some()),
        ]
        .into_iter()
        .filter(|(_, present)| *present)
        .map(|(name, _)| name.to_owned())
        .collect()
    }

    fn merge_into(mut self, current: &mut CredentialSecret) {
        if let Some(value) = self.password.take() {
            current.password = Some(value);
        }
        if let Some(value) = self.private_key_pem.take() {
            current.private_key_pem = Some(value);
        }
        if let Some(value) = self.private_key_passphrase.take() {
            current.private_key_passphrase = Some(value);
        }
        if let Some(value) = self.sudo_password.take() {
            current.sudo_password = Some(value);
        }
        current.use_ssh_agent = self.use_ssh_agent;
    }
}

impl StoreHostCredentialRequest {
    fn take_secret(&mut self) -> CredentialSecretRequest {
        CredentialSecretRequest {
            password: self.password.take(),
            private_key_pem: self.private_key_pem.take(),
            private_key_passphrase: self.private_key_passphrase.take(),
            sudo_password: self.sudo_password.take(),
            use_ssh_agent: self.use_ssh_agent,
        }
    }
}

/// Upsert environment request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertEnvironmentRequest {
    /// Environment name, for example `home-lan` or `company-lan`.
    pub name: String,
    /// Environment kind, for example `home_lan`, `company_lan`, `vpn`, or `public_internet`.
    pub kind: String,
    /// Trust level, for example `owned`, `trusted`, `external`, or `untrusted`.
    pub trust_level: String,
    /// Optional description.
    pub description: Option<String>,
    /// Optional notes.
    pub notes: Option<String>,
}

/// Upsert credential reference request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertCredentialRefRequest {
    /// Credential metadata name. This is not a secret.
    pub name: String,
    /// Credential kind, for example `ssh_private_key` or `ssh_password`.
    pub kind: String,
    /// Username hint.
    pub username_hint: Option<String>,
    /// Non-secret external reference such as `openssh-agent` or `vault-pending`.
    pub external_ref: Option<String>,
    /// Optional non-secret notes.
    pub notes: Option<String>,
}

/// Upsert access path request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertAccessPathRequest {
    /// Optional access path id to update. If omitted, equivalent paths are reused.
    pub access_path_id: Option<String>,
    /// Host id as UUID string.
    pub host_id: String,
    /// Environment id as UUID string.
    pub environment_id: String,
    /// Optional connector id as UUID string.
    pub connector_id: Option<String>,
    /// Credential id as UUID string.
    pub credential_id: String,
    /// SSH address or hostname.
    pub address: String,
    /// SSH port.
    pub port: u16,
    /// SSH username.
    pub username: String,
    /// Route type, for example `lan`, `public`, `frp`, `vpn`, `proxy_jump`, or `bastion`.
    pub route_type: String,
    /// Optional proxy chain.
    pub proxy_chain: Option<Vec<String>>,
    /// Lower values are preferred.
    pub priority: Option<i32>,
    /// Whether the path is enabled.
    pub enabled: Option<bool>,
    /// Connection mode, defaults to `pooled`.
    pub connection_mode: Option<String>,
    /// Idle transport TTL in seconds.
    pub idle_ttl_seconds: Option<u64>,
    /// Keepalive interval in seconds.
    pub keepalive_seconds: Option<u64>,
    /// Max concurrent channels.
    pub max_concurrent_channels: Option<u16>,
    /// Max new SSH connections per minute.
    pub max_new_connections_per_minute: Option<u16>,
    /// Whether this path requires TTY semantics.
    pub requires_tty: Option<bool>,
    /// Optional notes.
    pub notes: Option<String>,
}

/// Connector id request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorIdRequest {
    /// Connector id as UUID string.
    pub connector_id: String,
}

/// Workspace id request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceIdRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
}

/// Connector heartbeat request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorHeartbeatRequest {
    /// Connector id as UUID string.
    pub connector_id: String,
    /// Observed state, for example `healthy`, `connector_offline`, or `throttled`.
    pub state: String,
    /// Optional connector version.
    pub version: Option<String>,
    /// Optional current network label.
    pub current_network: Option<String>,
}

/// Knowledge search request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchKnowledgeRequest {
    /// Full-text search query.
    pub query: String,
    /// Maximum number of results. Defaults to 20 and is capped at 100.
    pub limit: Option<u32>,
}

/// Record host fact request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordHostFactRequest {
    /// Host id as UUID string.
    pub host_id: String,
    /// Fact namespace, for example `os`, `hardware`, `network`, or `software`.
    pub namespace: String,
    /// Fact key.
    pub key: String,
    /// JSON fact value.
    pub value: Value,
    /// Fact source. Defaults to `manual`.
    pub source: Option<String>,
    /// Confidence from 0.0 to 1.0. Defaults to 1.0.
    pub confidence: Option<f32>,
}

/// Record knowledge request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordKnowledgeRequest {
    /// Knowledge title.
    pub title: String,
    /// Redacted knowledge body.
    pub body: String,
    /// Knowledge source. Defaults to `manual`.
    pub source: Option<String>,
    /// Linked host ids.
    pub linked_host_ids: Option<Vec<String>>,
    /// Linked access path ids.
    pub linked_access_path_ids: Option<Vec<String>>,
    /// Linked software ids.
    pub linked_software_ids: Option<Vec<String>>,
    /// Linked operation ids.
    pub linked_operation_ids: Option<Vec<String>>,
    /// Tags.
    pub tags: Option<Vec<String>>,
}

/// Configure a direct instance-sync peer. The token is encrypted locally and never returned.
#[derive(Deserialize, JsonSchema, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct ConfigureInstanceSyncPeerRequest {
    /// Human-facing stable peer label, for example `macstudio`.
    pub display_name: String,
    /// Direct peer API base URL, for example `https://macstudio.local:8787`.
    pub endpoint: String,
    /// Shared peer token. It is encrypted in the local vault and never returned.
    pub token: String,
    /// Durable collections to exchange. Defaults to inventory, knowledge, and authorized credentials.
    pub collections: Option<Vec<String>>,
}

/// Push local durable metadata to one configured instance peer.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SyncInstancePeerRequest {
    /// Configured peer id.
    pub peer_id: String,
}

/// Compact response after configuring one peer.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ConfigureInstanceSyncPeerResponse {
    /// Configured peer id.
    pub peer_id: String,
    /// Peer display name.
    pub display_name: String,
    /// Approved collection names.
    pub collections: Vec<String>,
    /// Next action for the caller.
    pub next_action: String,
}

/// Compact response after one direct peer synchronization.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SyncInstancePeerResponse {
    /// Peer display name.
    pub peer: String,
    /// Number of records sent.
    pub sent: u32,
    /// Number of remote records applied.
    pub applied: u32,
    /// Number of remote duplicate receipts.
    pub duplicates: u32,
    /// Number of remote records retained as visible conflicts.
    pub conflicts: u32,
    /// Number of remote records rejected by validation or peer policy.
    pub rejected: u32,
    /// Bounded actionable details.
    pub details: Vec<String>,
}

/// State event listing request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListConnectorEventsRequest {
    /// Connector id as UUID string.
    pub connector_id: String,
    /// Maximum number of events. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Starting behavior for a runtime event wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventStartMode {
    /// Ignore retained history and wait only for events created after this request starts.
    LiveOnly,
    /// Replay events strictly after the supplied cursor, then continue waiting if necessary.
    AfterCursor,
}

/// Runtime event wait request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WaitRuntimeEventsRequest {
    /// Explicitly choose live-only delivery or cursor-based replay.
    pub start_mode: RuntimeEventStartMode,
    /// Required for `after_cursor` and forbidden for `live_only`.
    pub after_cursor: Option<u64>,
    /// Optional entity type filter, such as `connector`.
    pub entity_type: Option<String>,
    /// Optional entity id filter; requires `entity_type`.
    pub entity_id: Option<String>,
    /// Long-poll timeout in milliseconds. Defaults to 5000 and is capped at 60000.
    pub timeout_ms: Option<u64>,
    /// Maximum events returned. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// State refresh depth.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RefreshLevel {
    /// Use cached state only.
    Passive,
    /// Probe TCP reachability.
    Tcp,
    /// Perform SSH handshake/auth check.
    Ssh,
    /// Run lightweight facts probe.
    Facts,
}

/// Refresh state request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RefreshStateRequest {
    /// Host id as UUID string.
    pub host_id: String,
    /// Refresh level.
    pub level: RefreshLevel,
}

/// Create workspace request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateWorkspaceRequest {
    /// Host id as UUID string.
    pub host_id: String,
    /// Optional access path id.
    pub access_path_id: Option<String>,
    /// Optional connector id override.
    pub connector_id: Option<String>,
    /// Human label.
    pub label: String,
    /// Initial working directory.
    pub cwd: Option<String>,
    /// Optional policy profile.
    pub policy_profile: Option<String>,
    /// Hierarchical write-coordination scope. Defaults to `host`.
    pub coordination_scope: Option<String>,
    /// Optional TTL in seconds.
    pub ttl_seconds: Option<u64>,
}

/// Prepare a reusable workspace with the context needed for normal execution.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrepareWorkspaceRequest {
    /// Host id as UUID string.
    pub host_id: String,
    /// Optional required access path. Existing compatible workspaces are preferred.
    pub access_path_id: Option<String>,
    /// Label used only when a workspace must be created. Defaults to `agent-main`.
    pub label: Option<String>,
    /// Initial working directory used only for workspace creation.
    pub cwd: Option<String>,
    /// Policy profile used only for workspace creation.
    pub policy_profile: Option<String>,
    /// Hierarchical write-coordination scope. Defaults to `host`.
    pub coordination_scope: Option<String>,
    /// TTL used only for workspace creation. Defaults to 3600 seconds.
    pub ttl_seconds: Option<u64>,
}

/// Update workspace state request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateWorkspaceStateRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// New state, for example `idle`, `working`, `blocked`, `done`, `failed`, or `throttled`.
    pub state: String,
}

/// Open PTY session request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct OpenPtySessionRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Optional existing connection session id. The service resolves or creates one when omitted.
    pub session_id: Option<String>,
    /// Optional initial current working directory.
    pub cwd: Option<String>,
    /// Exact resource scopes coordinated by commands sent through this PTY.
    pub coordination_scopes: Option<Vec<String>>,
}

/// PTY heartbeat request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct HeartbeatPtySessionRequest {
    /// PTY session id as UUID string.
    pub pty_session_id: String,
    /// PTY state, for example `idle`, `working`, `blocked`, `done`, `failed`, or `closed`.
    pub state: String,
    /// Foreground process summary.
    pub foreground_process: Option<String>,
    /// Current working directory.
    pub cwd: Option<String>,
    /// Recent output artifact reference.
    pub recent_output_ref: Option<String>,
    /// Last foreground process exit code.
    pub last_exit_code: Option<i32>,
    /// Whether input remains allowed.
    pub input_allowed: bool,
}

/// Read PTY output request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ReadPtyOutputRequest {
    /// PTY session id as UUID string.
    pub pty_session_id: String,
    /// Only return chunks after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of chunks. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Queue PTY input request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct QueuePtyInputRequest {
    /// PTY session id as UUID string.
    pub pty_session_id: String,
    /// Raw input to enqueue for connector-owned PTY delivery. Exactly one of `input`,
    /// `use_stored_sudo_password`, `use_stored_password_from_host_id`, or
    /// `use_stored_sudo_password_from_host_id` is required.
    pub input: Option<String>,
    /// Resolve the access path's encrypted sudo password inside the connector and send it only
    /// to a live sudo prompt. This mode rejects a caller-provided `input` value.
    #[serde(default)]
    pub use_stored_sudo_password: bool,
    /// Resolve the only enabled SSH access path for this registered host, then inject that
    /// route's encrypted SSH password into a live nested SSH password prompt. Only the target
    /// access-path id enters the private queue payload; the password is decrypted in connector
    /// memory at delivery time.
    pub use_stored_password_from_host_id: Option<String>,
    /// Resolve the only enabled SSH access path for this registered host, then inject that
    /// route's encrypted dedicated sudo password into a verified live nested sudo prompt.
    pub use_stored_sudo_password_from_host_id: Option<String>,
    /// Optional requester label.
    pub requested_by: Option<String>,
    /// Stable retry key. Reusing it in this conversation returns the original input event.
    pub idempotency_key: Option<String>,
}

/// List PTY input events request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListPtyInputEventsRequest {
    /// PTY session id as UUID string.
    pub pty_session_id: String,
    /// Only return events after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of events. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Close PTY session request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ClosePtySessionRequest {
    /// PTY session id as UUID string.
    pub pty_session_id: String,
    /// Last foreground process exit code.
    pub last_exit_code: Option<i32>,
}

/// Reap expired PTY sessions request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ReapExpiredPtySessionsRequest {
    /// Idle TTL in seconds. Defaults to 3600 and is clamped to 60..=86400.
    pub idle_ttl_seconds: Option<u64>,
    /// Maximum number of PTYs to close. Defaults to 100 and is capped at 500.
    pub limit: Option<u32>,
}

/// Caller-declared write coordination for an arbitrary shell command.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunCoordinationMode {
    /// Preserve profile-based behavior for older clients.
    #[default]
    Auto,
    /// The caller attests that the command only observes state.
    ReadOnly,
    /// The command may mutate state and must acquire a scoped write lease.
    Mutating,
}

impl From<RunCoordinationMode> for OperationCoordinationMode {
    fn from(value: RunCoordinationMode) -> Self {
        match value {
            RunCoordinationMode::Auto => Self::Auto,
            RunCoordinationMode::ReadOnly => Self::ReadOnly,
            RunCoordinationMode::Mutating => Self::Mutating,
        }
    }
}

/// Run-in-workspace request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RunInWorkspaceRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Command profile name.
    pub command_profile: String,
    /// Structured arguments.
    pub args: Vec<String>,
    /// Human or agent intent for audit and later knowledge linking.
    pub intent: Option<String>,
    /// `read_only` skips write leasing, `mutating` requires it, and `auto` preserves legacy inference.
    pub coordination_mode: Option<RunCoordinationMode>,
    /// Optional operation scope within the Workspace scope. Useful for independent mutations.
    pub coordination_scope: Option<String>,
    /// Optional exact resource scopes acquired atomically for one multi-resource operation.
    pub coordination_scopes: Option<Vec<String>>,
    /// Optional command timeout override in seconds. Shell profiles allow up to 7200.
    pub timeout_seconds: Option<u64>,
    /// Optional captured output limit override in bytes, up to 8 MiB.
    pub output_limit_bytes: Option<usize>,
    /// Atomically wait for this exact queued operation, capped at 60 seconds.
    pub wait_timeout_ms: Option<u64>,
    /// Stable retry key. Reusing it in this conversation returns the original operation.
    pub idempotency_key: Option<String>,
}

/// Upload one connector-local file through a workspace.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UploadFileRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Absolute path on the connector machine.
    pub local_path: String,
    /// Absolute POSIX or drive-letter SFTP path on the target host.
    pub remote_path: String,
    /// `deny` (default) or `replace`.
    pub overwrite: Option<String>,
    /// Optional octal destination mode such as `0600` or `0755`.
    pub mode: Option<String>,
    /// Maximum transfer bytes. Defaults to 512 MiB and is capped at 4 GiB.
    pub max_size_bytes: Option<u64>,
    /// Optional expected SHA-256 digest of the local source.
    pub expected_sha256: Option<String>,
    /// End-to-end timeout in seconds. Defaults to 600 and is capped at 7200.
    pub timeout_seconds: Option<u64>,
    /// Human or agent intent for audit and later knowledge linking.
    pub intent: Option<String>,
    /// Stable retry key. Reusing it in this conversation returns the original operation.
    pub idempotency_key: Option<String>,
}

/// Download one remote file through a workspace.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DownloadFileRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Absolute SFTP path on the target host.
    pub remote_path: String,
    /// Absolute destination path on the connector machine.
    pub local_path: String,
    /// `deny` (default) or `replace`.
    pub overwrite: Option<String>,
    /// Optional octal mode to apply to the connector-local destination.
    pub mode: Option<String>,
    /// Maximum transfer bytes. Defaults to 512 MiB and is capped at 4 GiB.
    pub max_size_bytes: Option<u64>,
    /// Optional expected SHA-256 digest of the remote source.
    pub expected_sha256: Option<String>,
    /// End-to-end timeout in seconds. Defaults to 600 and is capped at 7200.
    pub timeout_seconds: Option<u64>,
    /// Human or agent intent for audit and later knowledge linking.
    pub intent: Option<String>,
    /// Stable retry key. Reusing it in this conversation returns the original operation.
    pub idempotency_key: Option<String>,
}

/// Read workspace output request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ReadWorkspaceOutputRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Optional operation id as UUID string.
    pub operation_id: Option<String>,
    /// Only return chunks after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of chunks. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Combined workspace result request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetWorkspaceResultRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Optional operation id as UUID string.
    pub operation_id: Option<String>,
    /// Only return chunks after this sequence number. Requires `operation_id`.
    pub after_sequence: Option<u64>,
    /// Maximum chunks and artifacts per collection. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// List workspace output artifacts request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListWorkspaceOutputArtifactsRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Optional operation id as UUID string.
    pub operation_id: Option<String>,
    /// Maximum number of artifacts. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Output artifact id request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct OutputArtifactIdRequest {
    /// Output artifact id as UUID string.
    pub artifact_id: String,
}

/// Bounded output-artifact content request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadOutputArtifactContentRequest {
    /// Output artifact id as UUID string.
    pub artifact_id: String,
    /// Byte offset. Start at zero and then reuse `next_offset`.
    pub offset: Option<u64>,
    /// Maximum UTF-8 bytes. Defaults to 64 KiB and is capped at 256 KiB.
    pub max_bytes: Option<usize>,
}

/// Wait workspace state request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct WaitWorkspaceStateRequest {
    /// Workspace id as UUID string.
    pub workspace_id: String,
    /// Desired states. Defaults to idle/done/failed/throttled/blocked/closed.
    pub desired_states: Option<Vec<String>>,
    /// Timeout in milliseconds. Defaults to 5000 and is capped at 60000.
    pub timeout_ms: Option<u64>,
    /// Poll interval in milliseconds. Defaults to 250 and is clamped to 100..=5000.
    pub poll_interval_ms: Option<u64>,
}

/// Repository-backed MCP server for agent access.
#[derive(Clone)]
pub struct RemoteHostsMcpServer {
    repositories: Arc<Repositories>,
    tool_router: ToolRouter<Self>,
    tool_profile: ToolProfile,
    vault_master_password: Option<Arc<SecretString>>,
    artifact_root: Arc<PathBuf>,
    agent_session: Arc<AgentSession>,
}

impl RemoteHostsMcpServer {
    /// Creates an MCP server from repositories.
    pub fn new(repositories: Repositories) -> Self {
        Self::with_profile(repositories, ToolProfile::Full)
    }

    /// Creates an MCP server with a bounded tool visibility profile.
    pub fn with_profile(repositories: Repositories, tool_profile: ToolProfile) -> Self {
        Self::with_profile_and_vault(repositories, tool_profile, None)
    }

    /// Creates an MCP server with a bounded tool profile and optional local vault access.
    pub fn with_profile_and_vault(
        repositories: Repositories,
        tool_profile: ToolProfile,
        vault_master_password: Option<SecretString>,
    ) -> Self {
        Self::with_profile_vault_and_artifact_root(
            repositories,
            tool_profile,
            vault_master_password,
            DEFAULT_ARTIFACT_ROOT,
        )
    }

    /// Creates an MCP server with an explicit output-artifact root.
    pub fn with_profile_vault_and_artifact_root(
        repositories: Repositories,
        tool_profile: ToolProfile,
        vault_master_password: Option<SecretString>,
        artifact_root: impl Into<PathBuf>,
    ) -> Self {
        let mut tool_router = Self::tool_router();
        for tool in tool_router.list_all() {
            if !tool_profile.includes(&tool.name) {
                tool_router.remove_route(&tool.name);
            }
        }
        Self {
            repositories: Arc::new(repositories),
            tool_router,
            tool_profile,
            vault_master_password: vault_master_password.map(Arc::new),
            artifact_root: Arc::new(artifact_root.into()),
            agent_session: Arc::new(AgentSessionContext::default().into_session()),
        }
    }

    /// Overrides the generated MCP process identity with launcher-provided context.
    #[must_use]
    pub fn with_agent_session_context(mut self, context: AgentSessionContext) -> Self {
        self.agent_session = Arc::new(context.into_session());
        self
    }

    async fn ensure_agent_session(&self) -> Result<AgentSession, String> {
        let mut session = self.agent_session.as_ref().clone();
        let now = now_utc();
        session.last_seen_at = now;
        session.expires_at = now + time::Duration::hours(24);
        self.repositories
            .agent_sessions
            .upsert(&session)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(session)
    }

    fn requires_workspace_ownership(&self) -> bool {
        self.tool_profile == ToolProfile::Agent
    }

    fn compact_responses(&self) -> bool {
        self.tool_profile == ToolProfile::Agent
    }

    fn instance_sync_service(&self) -> Result<InstanceSyncService, String> {
        InstanceSyncService::with_vault_master_password(
            (*self.repositories).clone(),
            self.vault_master_password.as_deref().cloned(),
        )
        .map_err(|error| error.to_string())
    }

    async fn workspace_for_tool(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<AgentWorkspace, String> {
        let workspace = self
            .repositories
            .workspaces
            .get(workspace_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("workspace not found: {workspace_id}"))?;
        if self.requires_workspace_ownership()
            && workspace.agent_session_id != Some(self.agent_session.id)
        {
            return Err(format!(
                "workspace is owned by another agent session: {workspace_id}; prepare a workspace in this conversation"
            ));
        }
        Ok(workspace)
    }

    async fn pty_session_for_tool(
        &self,
        pty_session_id: PtySessionId,
    ) -> Result<PtySession, String> {
        let pty_session = self
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("pty session not found: {pty_session_id}"))?;
        self.workspace_for_tool(pty_session.workspace_id).await?;
        Ok(pty_session)
    }

    async fn prepare_encrypted_credential(
        &self,
        existing: Option<StoredCredential>,
        name: String,
        username: String,
        host_kind: HostKind,
        request: CredentialSecretRequest,
    ) -> Result<PreparedCredentialWrite, String> {
        let master_password = self
            .vault_master_password
            .as_deref()
            .cloned()
            .ok_or_else(|| {
                "local credential vault is unavailable; configure --vault-master-password-file"
                    .to_owned()
            })?;
        let stored_fields = request.stored_fields();
        let created = existing.is_none();
        let existing_metadata = existing
            .as_ref()
            .map(|credential| credential.metadata.clone());
        let encrypted_blob = existing.map(|credential| credential.encrypted_blob_json);

        let (encrypted_blob, kind) = tokio::task::spawn_blocking(move || {
            let mut current = match encrypted_blob {
                Some(value) if is_openssh_agent_reference(&value) => CredentialSecret {
                    password: None,
                    private_key_pem: None,
                    private_key_passphrase: None,
                    sudo_password: None,
                    token: None,
                    secret_text: None,
                    use_ssh_agent: true,
                },
                Some(value) => {
                    let blob: EncryptedCredentialBlob =
                        serde_json::from_value(value).map_err(|_| {
                            "existing credential is not a supported vault blob".to_owned()
                        })?;
                    CredentialVault::decrypt(&master_password, &blob).map_err(|_| {
                        "existing credential cannot be decrypted with the local vault key"
                            .to_owned()
                    })?
                }
                None => CredentialSecret {
                    password: None,
                    private_key_pem: None,
                    private_key_passphrase: None,
                    sudo_password: None,
                    token: None,
                    secret_text: None,
                    use_ssh_agent: false,
                },
            };
            request.merge_into(&mut current);
            if current.password.is_none()
                && current.private_key_pem.is_none()
                && !current.use_ssh_agent
            {
                return Err(
                    "credential must contain a password, private key, or SSH-agent access"
                        .to_owned(),
                );
            }
            let kind = credential_kind_for_secret(&host_kind, &current);
            let blob = CredentialVault::encrypt(&master_password, &current)
                .map_err(|_| "failed to encrypt credential in the local vault".to_owned())?;
            serde_json::to_value(blob)
                .map(|value| (value, kind))
                .map_err(|_| "failed to encode encrypted credential".to_owned())
        })
        .await
        .map_err(|_| "credential vault worker stopped unexpectedly".to_owned())??;

        let now = now_utc();
        let metadata = CredentialMetadata {
            id: existing_metadata
                .as_ref()
                .map_or_else(CredentialId::new, |metadata| metadata.id),
            name,
            kind,
            username_hint: Some(username),
            created_at: existing_metadata
                .as_ref()
                .map_or(now, |metadata| metadata.created_at),
            updated_at: now,
            last_used_at: existing_metadata.and_then(|metadata| metadata.last_used_at),
        };
        Ok(PreparedCredentialWrite {
            credential: StoredCredential {
                metadata,
                encrypted_blob_json: encrypted_blob,
            },
            status: if created { "created" } else { "updated" }.to_owned(),
            stored_fields,
        })
    }

    async fn access_candidates_for_host(
        &self,
        host_id: HostId,
    ) -> Result<Vec<AccessCandidate>, String> {
        let access_paths = self
            .repositories
            .access_paths
            .list_enabled_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mut candidates = Vec::with_capacity(access_paths.len());
        for access_path in access_paths {
            let connector = match access_path.connector_id {
                Some(connector_id) => self
                    .repositories
                    .connectors
                    .get(connector_id)
                    .await
                    .map_err(|error| tool_error(&error))?,
                None => None,
            };
            let health = self
                .repositories
                .access_path_health
                .get(access_path.id)
                .await
                .map_err(|error| tool_error(&error))?;
            candidates.push(AccessCandidate {
                access_path,
                connector,
                health,
            });
        }
        Ok(candidates)
    }

    async fn resolve_access_path(&self, host_id: HostId) -> Result<AccessPath, String> {
        let candidates = self.access_candidates_for_host(host_id).await?;
        AccessResolver::resolve(&candidates)
            .map(|resolution| resolution.selected.access_path)
            .map_err(|error| resolution_error(&error))
    }

    async fn workspace_capacity(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
    ) -> Result<WorkspaceCapacityStatus, String> {
        self.repositories
            .workspaces
            .capacity_for_host(host_id, Some(agent_session_id), now_utc())
            .await
            .map_err(|error| tool_error(&error))
    }

    async fn reconcile_workspace_capacity(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
    ) -> Result<(u64, WorkspaceCapacityStatus), String> {
        let policy = ServerProtectionPolicy::default();
        let observed_at = now_utc();
        self.repositories
            .agent_sessions
            .reconcile_expired(observed_at, 1_000)
            .await
            .map_err(|error| tool_error(&error))?;
        let expired_reaped = self
            .repositories
            .workspaces
            .reconcile_expired_for_host(
                host_id,
                observed_at,
                policy.max_active_workspaces_per_host.saturating_mul(32),
            )
            .await
            .map_err(|error| tool_error(&error))?;
        let capacity = self
            .repositories
            .workspaces
            .capacity_for_host(host_id, Some(agent_session_id), observed_at)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok((expired_reaped, capacity))
    }

    async fn connection_for_workspace(
        &self,
        workspace: &AgentWorkspace,
        requested_session_id: Option<&str>,
    ) -> Result<ConnectionSession, String> {
        if let Some(session_id) = requested_session_id {
            let session_id = parse_session_id(session_id)?;
            let session = self
                .repositories
                .connection_sessions
                .get(session_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("connection session not found: {session_id}"))?;
            if session.access_path_id != workspace.access_path_id
                || session.connector_id != workspace.connector_id
            {
                return Err("connection session does not belong to this workspace route".to_owned());
            }
            return Ok(session);
        }

        if let Some(session) = self
            .repositories
            .connection_sessions
            .find_reusable(workspace.access_path_id, workspace.connector_id)
            .await
            .map_err(|error| tool_error(&error))?
        {
            return Ok(session);
        }

        let now = now_utc();
        let session = ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: workspace.access_path_id,
            connector_id: workspace.connector_id,
            state: EntityState::Resolving,
            created_at: now,
            last_used_at: now,
            open_channels: 0,
            reused_count: 0,
            failure_count: 0,
            last_error: None,
        };
        self.repositories
            .connection_sessions
            .upsert(&session)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(session)
    }

    async fn find_idempotent_operation(
        &self,
        agent_session_id: Option<AgentSessionId>,
        idempotency_key: Option<&str>,
        workspace_id: WorkspaceId,
        expected_profile: &Value,
        expected_requires_write_lease: bool,
        expected_coordination_scopes: &[String],
    ) -> Result<Option<OperationRun>, String> {
        let (Some(agent_session_id), Some(idempotency_key)) = (agent_session_id, idempotency_key)
        else {
            return Ok(None);
        };
        let Some(existing) = self
            .repositories
            .operations
            .get_by_agent_session_and_idempotency_key(agent_session_id, idempotency_key)
            .await
            .map_err(|error| tool_error(&error))?
        else {
            return Ok(None);
        };
        if existing.workspace_id != Some(workspace_id)
            || existing.command_profile_json.as_ref() != Some(expected_profile)
            || existing.requires_write_lease != expected_requires_write_lease
            || existing.coordination_scope
                != common_coordination_scope(expected_coordination_scopes)
            || existing.coordination_scopes != expected_coordination_scopes
        {
            return Err(format!(
                "idempotency_key `{idempotency_key}` is already bound to a different request in this conversation"
            ));
        }
        Ok(Some(existing))
    }

    async fn idempotent_operation_output(
        &self,
        operation: OperationRun,
    ) -> Result<QueuedOperationOutput, String> {
        let workspace_id = operation
            .workspace_id
            .ok_or_else(|| "idempotent operation has no workspace".to_owned())?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let initial_output_chunk = self
            .repositories
            .operation_output_chunks
            .list_for_workspace(workspace_id, Some(operation.id), None, 1)
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .next()
            .map_or(Value::Null, |chunk| {
                to_json_value(&chunk).unwrap_or(Value::Null)
            });
        Ok(QueuedOperationOutput {
            next_action: operation_next_action(&operation.state).to_owned(),
            retry_after_ms: operation_retry_after_ms(&operation.state),
            operation: if self.compact_responses() {
                compact_operation_value(&operation)
            } else {
                public_operation_value(&operation)?
            },
            workspace: if self.compact_responses() {
                compact_workspace_value(&workspace)
            } else {
                to_json_value(&workspace)?
            },
            initial_output_chunk: (!self.compact_responses()).then_some(initial_output_chunk),
            protection_decision: (!self.compact_responses()).then(|| {
                json!({
                    "allowed": true,
                    "state": "healthy",
                    "reason_code": "policy_allowed",
                    "agent_hint": null,
                    "retry_after_seconds": null,
                    "human_message": "existing idempotent operation returned"
                })
            }),
            idempotency_reused: true,
            completion: None,
        })
    }

    async fn acquire_write_lease_for_operation(
        &self,
        operation: &OperationRun,
        workspace_id: WorkspaceId,
    ) -> Result<(), String> {
        if !operation.requires_write_lease {
            return Ok(());
        }
        let Some(agent_session_id) = operation.agent_session_id else {
            return Ok(());
        };
        let observed_at = now_utc();
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
                expires_at: observed_at + time::Duration::seconds(WRITE_LEASE_SECONDS),
            })
            .collect::<Vec<_>>();
        self.repositories
            .host_write_leases
            .try_acquire_many(&leases, observed_at)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn queue_workspace_operation(
        &self,
        request: RunInWorkspaceRequest,
    ) -> Result<QueuedOperationOutput, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let access_path = self
            .repositories
            .access_paths
            .get(workspace.access_path_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("access path not found: {}", workspace.access_path_id))?;
        if access_path.requires_tty {
            return Err(
                "this access path requires a persistent PTY; open the workspace PTY, read the interactive menu, select the intended asset, and send commands through that PTY instead of remote_hosts_run_in_workspace"
                    .to_owned(),
            );
        }
        let idempotency_key = normalize_idempotency_key(request.idempotency_key)?;
        let coordination_mode: OperationCoordinationMode =
            request.coordination_mode.unwrap_or_default().into();
        let requested_coordination_scope = trim_optional(request.coordination_scope);
        let requested_coordination_scopes = request.coordination_scopes.map(|scopes| {
            scopes
                .into_iter()
                .map(|scope| scope.trim().to_owned())
                .collect::<Vec<_>>()
        });
        let coordination_scopes = resolve_operation_coordination_scopes(
            &workspace,
            requested_coordination_scope.as_deref(),
            requested_coordination_scopes.as_deref(),
        )
        .map_err(|error| error.to_string())?;
        let policy = ServerProtectionPolicy::default();
        let mut profile =
            CommandProfileCatalog::resolve_builtin(&request.command_profile, request.args, &policy)
                .map_err(|error| error.to_string())?;
        if let Some(timeout_seconds) = request.timeout_seconds {
            profile.timeout_seconds = timeout_seconds;
        }
        if let Some(output_limit_bytes) = request.output_limit_bytes {
            profile.output_limit_bytes = output_limit_bytes;
        }
        profile.validate().map_err(|error| error.to_string())?;
        let expected_profile = to_json_value(&profile)?;
        let requires_write_lease = coordination_mode.requires_write_lease(&profile.class);
        if let Some(existing) = self
            .find_idempotent_operation(
                workspace.agent_session_id,
                idempotency_key.as_deref(),
                workspace_id,
                &expected_profile,
                requires_write_lease,
                &coordination_scopes,
            )
            .await?
        {
            return self.idempotent_operation_output(existing).await;
        }
        let queued_operations = self
            .repositories
            .operations
            .count_queued_for_host(workspace.host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let active_exec_channels = self
            .repositories
            .operations
            .count_running_for_host(workspace.host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let plan = WorkspaceOperationSupervisor::new(policy)
            .queue_operation(&WorkspaceRunCommand {
                workspace,
                command_profile: profile,
                intent: request.intent,
                idempotency_key,
                coordination_mode,
                coordination_scope: requested_coordination_scope,
                coordination_scopes: requested_coordination_scopes,
                queued_operations,
                active_exec_channels,
                active_probe_jobs: 0,
                overload_cooldown_active: false,
            })
            .map_err(|error| error.to_string())?;

        if let Err(error) = self.repositories.operations.insert(&plan.operation).await {
            if let Some(existing) = self
                .find_idempotent_operation(
                    plan.operation.agent_session_id,
                    plan.operation.idempotency_key.as_deref(),
                    workspace_id,
                    &expected_profile,
                    requires_write_lease,
                    &coordination_scopes,
                )
                .await?
            {
                return self.idempotent_operation_output(existing).await;
            }
            return Err(tool_error(&error));
        }
        self.repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await
            .map_err(|error| tool_error(&error))?;
        let workspace = self
            .repositories
            .workspaces
            .update_state(workspace_id, plan.workspace_state, now_utc())
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("workspace not found after queueing: {workspace_id}"))?;
        self.acquire_write_lease_for_operation(&plan.operation, workspace_id)
            .await?;

        Ok(QueuedOperationOutput {
            next_action: operation_next_action(&plan.operation.state).to_owned(),
            retry_after_ms: operation_retry_after_ms(&plan.operation.state),
            operation: if self.compact_responses() {
                compact_operation_value(&plan.operation)
            } else {
                public_operation_value(&plan.operation)?
            },
            workspace: if self.compact_responses() {
                compact_workspace_value(&workspace)
            } else {
                to_json_value(&workspace)?
            },
            initial_output_chunk: (!self.compact_responses())
                .then(|| to_json_value(&plan.initial_output_chunk))
                .transpose()?,
            protection_decision: (!self.compact_responses())
                .then(|| to_json_value(&plan.decision))
                .transpose()?,
            idempotency_reused: false,
            completion: None,
        })
    }

    async fn wait_for_operation_completion(
        &self,
        workspace_id: WorkspaceId,
        operation_id: OperationId,
        requested_timeout_ms: u64,
    ) -> Result<OperationCompletionOutput, String> {
        let timeout_ms = requested_timeout_ms.min(60_000);
        let poll_interval_ms = 100_u64;
        let started_at = Instant::now();
        loop {
            let workspace = self.workspace_for_tool(workspace_id).await?;
            let operation = self
                .repositories
                .operations
                .get(operation_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("operation not found: {operation_id}"))?;
            if operation.workspace_id != Some(workspace_id) {
                return Err(format!(
                    "operation does not belong to workspace: {operation_id}"
                ));
            }
            if is_terminal_operation_state(&operation.state) {
                return Ok(OperationCompletionOutput {
                    completed: true,
                    state: operation_state_name(&operation.state).to_owned(),
                    exit_code: operation.exit_code,
                    summary: operation_result_summary(&operation),
                    next_action: "none".to_owned(),
                    operation: (!self.compact_responses())
                        .then(|| public_operation_value(&operation))
                        .transpose()?,
                    workspace: (!self.compact_responses())
                        .then(|| to_json_value(&workspace))
                        .transpose()?,
                    elapsed_ms: elapsed_ms(started_at),
                    retry_after_ms: None,
                });
            }
            if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
                return Ok(OperationCompletionOutput {
                    completed: false,
                    state: operation_state_name(&operation.state).to_owned(),
                    exit_code: operation.exit_code,
                    summary: operation_result_summary(&operation),
                    next_action: "get_workspace_result".to_owned(),
                    operation: (!self.compact_responses())
                        .then(|| public_operation_value(&operation))
                        .transpose()?,
                    workspace: (!self.compact_responses())
                        .then(|| to_json_value(&workspace))
                        .transpose()?,
                    elapsed_ms: elapsed_ms(started_at),
                    retry_after_ms: Some(poll_interval_ms),
                });
            }
            tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
        }
    }

    async fn queue_workspace_file_transfer(
        &self,
        workspace_id: WorkspaceId,
        spec: FileTransferSpec,
        intent: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<QueuedOperationOutput, String> {
        spec.validate().map_err(|error| error.to_string())?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let idempotency_key = normalize_idempotency_key(idempotency_key)?;
        let expected_profile = to_json_value(&spec)?;
        let expected_requires_write_lease = matches!(spec.direction, SftpDirection::Upload);
        let expected_coordination_scope = workspace.coordination_scope.clone();
        let expected_coordination_scopes = vec![expected_coordination_scope.clone()];
        if let Some(existing) = self
            .find_idempotent_operation(
                workspace.agent_session_id,
                idempotency_key.as_deref(),
                workspace_id,
                &expected_profile,
                expected_requires_write_lease,
                &expected_coordination_scopes,
            )
            .await?
        {
            return self.idempotent_operation_output(existing).await;
        }
        let queued_operations = self
            .repositories
            .operations
            .count_queued_for_host(workspace.host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let active_exec_channels = self
            .repositories
            .operations
            .count_running_for_host(workspace.host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let plan = WorkspaceOperationSupervisor::default()
            .queue_file_transfer(&WorkspaceFileTransfer {
                workspace,
                spec,
                intent,
                idempotency_key,
                queued_operations,
                active_exec_channels,
                overload_cooldown_active: false,
            })
            .map_err(|error| error.to_string())?;

        if let Err(error) = self.repositories.operations.insert(&plan.operation).await {
            if let Some(existing) = self
                .find_idempotent_operation(
                    plan.operation.agent_session_id,
                    plan.operation.idempotency_key.as_deref(),
                    workspace_id,
                    &expected_profile,
                    plan.operation.requires_write_lease,
                    &plan.operation.coordination_scopes,
                )
                .await?
            {
                return self.idempotent_operation_output(existing).await;
            }
            return Err(tool_error(&error));
        }
        self.repositories
            .operation_output_chunks
            .insert(&plan.initial_output_chunk)
            .await
            .map_err(|error| tool_error(&error))?;
        let workspace = self
            .repositories
            .workspaces
            .update_state(workspace_id, plan.workspace_state, now_utc())
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("workspace not found after queueing: {workspace_id}"))?;
        self.acquire_write_lease_for_operation(&plan.operation, workspace_id)
            .await?;

        Ok(QueuedOperationOutput {
            next_action: operation_next_action(&plan.operation.state).to_owned(),
            retry_after_ms: operation_retry_after_ms(&plan.operation.state),
            operation: if self.compact_responses() {
                compact_operation_value(&plan.operation)
            } else {
                public_operation_value(&plan.operation)?
            },
            workspace: if self.compact_responses() {
                compact_workspace_value(&workspace)
            } else {
                to_json_value(&workspace)?
            },
            initial_output_chunk: (!self.compact_responses())
                .then(|| to_json_value(&plan.initial_output_chunk))
                .transpose()?,
            protection_decision: (!self.compact_responses())
                .then(|| to_json_value(&plan.decision))
                .transpose()?,
            idempotency_reused: false,
            completion: None,
        })
    }

    async fn connector_snapshots_for_paths(
        &self,
        access_paths: &[AccessPath],
    ) -> Result<Vec<ConnectorSnapshotOutput>, String> {
        let mut snapshots = Vec::new();
        let mut seen = BTreeSet::new();
        let now = now_utc();

        for connector_id in access_paths
            .iter()
            .filter_map(|access_path| access_path.connector_id)
        {
            if !seen.insert(connector_id) {
                continue;
            }
            if let Some(connector) = self
                .repositories
                .connectors
                .get(connector_id)
                .await
                .map_err(|error| tool_error(&error))?
            {
                let observed_at = connector.last_seen_at.unwrap_or(now);
                let visible_state = if connector.last_seen_at.is_some() {
                    connector.state.clone()
                } else {
                    EntityState::ConnectorOffline
                };
                let snapshot = ConnectorStateTracker::snapshot(visible_state, observed_at, now, 60);
                snapshots.push(ConnectorSnapshotOutput {
                    connector: to_json_value(&connector)?,
                    snapshot: to_json_value(&snapshot)?,
                });
            }
        }

        Ok(snapshots)
    }

    async fn duplicate_host_matches(
        &self,
        request: &FindHostDuplicatesRequest,
    ) -> Result<Vec<HostDuplicateMatch>, String> {
        if request
            .name
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
            && request
                .display_name
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            && request
                .address
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
        {
            return Err("provide at least one of name, display_name, or address".to_owned());
        }

        let normalized_name = request
            .name
            .as_deref()
            .map(normalize_candidate_name)
            .transpose()?;
        let display_name = request
            .display_name
            .as_deref()
            .map(|value| value.trim().to_lowercase());
        let address = request
            .address
            .as_deref()
            .map(|value| value.trim().to_lowercase());
        let username = request
            .username
            .as_deref()
            .map(|value| value.trim().to_lowercase());
        let hosts = self
            .repositories
            .hosts
            .list()
            .await
            .map_err(|error| tool_error(&error))?;
        let mut candidates = Vec::new();

        for host in hosts {
            let mut signals = Vec::new();
            if normalized_name.as_deref() == Some(host.name.as_str()) {
                signals.push("name".to_owned());
            }
            if display_name.as_deref() == Some(host.display_name.to_lowercase().as_str()) {
                signals.push("display_name".to_owned());
            }
            let access_paths = self
                .repositories
                .access_paths
                .list_for_host(host.id)
                .await
                .map_err(|error| tool_error(&error))?;
            let access_path_match = access_paths.iter().any(|path| {
                address.as_deref() == Some(path.address.to_lowercase().as_str())
                    && request.port.is_none_or(|port| port == path.port)
                    && username
                        .as_deref()
                        .is_none_or(|user| user == path.username.to_lowercase())
            });
            if access_path_match {
                signals.push("access_path".to_owned());
            }
            if !signals.is_empty() {
                candidates.push(HostDuplicateMatch {
                    host,
                    access_paths,
                    signals,
                });
            }
        }

        Ok(candidates)
    }

    async fn select_connector_for_registration(
        &self,
        requested: Option<ConnectorId>,
        environment_id: EnvironmentId,
    ) -> Result<(Option<ConnectorId>, Option<String>, Option<String>), String> {
        if let Some(connector_id) = requested {
            ensure_exists(
                self.repositories
                    .connectors
                    .get(connector_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .is_some(),
                || format!("connector not found: {connector_id}"),
            )?;
            return Ok((Some(connector_id), None, None));
        }

        let available = self
            .repositories
            .connectors
            .list()
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .filter(connector_is_available)
            .collect::<Vec<_>>();
        let environment_matches = available
            .iter()
            .filter(|connector| connector.environment_id == environment_id)
            .collect::<Vec<_>>();

        if let [connector] = environment_matches.as_slice() {
            return Ok((
                Some(connector.id),
                Some("connector:environment-match".to_owned()),
                None,
            ));
        }
        if let [connector] = available.as_slice() {
            return Ok((
                Some(connector.id),
                Some("connector:single-healthy".to_owned()),
                None,
            ));
        }
        if available.is_empty() {
            return Ok((
                None,
                None,
                Some("no healthy connector is available; the host is registered but cannot open a workspace yet".to_owned()),
            ));
        }
        Ok((
            None,
            None,
            Some("multiple healthy connectors are available; set access.connector_id before opening a workspace".to_owned()),
        ))
    }

    async fn equivalent_access_path(
        &self,
        key: EquivalentAccessPathKey<'_>,
    ) -> Result<Option<AccessPath>, String> {
        let address = key.address.to_lowercase();
        let username = key.username.to_lowercase();
        Ok(self
            .repositories
            .access_paths
            .list_for_host(key.host_id)
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .find(|path| {
                path.environment_id == key.environment_id
                    && path.address.to_lowercase() == address
                    && path.port == key.port
                    && path.username.to_lowercase() == username
                    && &path.route_type == key.route_type
                    && path.proxy_chain == key.proxy_chain
            }))
    }

    async fn same_endpoint_access_path(
        &self,
        key: EquivalentAccessPathKey<'_>,
    ) -> Result<Option<AccessPath>, String> {
        let address = key.address.to_lowercase();
        let username = key.username.to_lowercase();
        Ok(self
            .repositories
            .access_paths
            .list_for_host(key.host_id)
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .find(|path| {
                path.address.to_lowercase() == address
                    && path.port == key.port
                    && path.username.to_lowercase() == username
                    && path.proxy_chain == key.proxy_chain
            }))
    }
}

struct EquivalentAccessPathKey<'a> {
    host_id: HostId,
    environment_id: EnvironmentId,
    address: &'a str,
    port: u16,
    username: &'a str,
    route_type: &'a RouteType,
    proxy_chain: &'a [String],
}

struct HostDuplicateMatch {
    host: Host,
    access_paths: Vec<AccessPath>,
    signals: Vec<String>,
}

struct PreparedEnsureAccess {
    address: String,
    port: u16,
    username: String,
    environment_name: String,
    environment_kind: EnvironmentKind,
    trust_level: TrustLevel,
    route_type: RouteType,
    connector_id: Option<ConnectorId>,
    credential_name: Option<String>,
    credential_kind: Option<CredentialKind>,
    credential_secret: Option<CredentialSecretRequest>,
    proxy_chain: Vec<String>,
    priority: Option<i32>,
    enabled: Option<bool>,
    connection_mode: ConnectionMode,
    idle_ttl_seconds: Option<u64>,
    keepalive_seconds: Option<u64>,
    max_concurrent_channels: Option<u16>,
    max_new_connections_per_minute: Option<u16>,
    requires_tty: Option<bool>,
    notes: Option<String>,
}

struct PreparedCredentialWrite {
    credential: StoredCredential,
    status: String,
    stored_fields: Vec<String>,
}

/// Serves the repository-backed MCP server over stdio.
///
/// # Errors
///
/// Returns an error if MCP initialization or the server task fails.
pub async fn serve_stdio(repositories: Repositories) -> Result<(), String> {
    serve_stdio_with_profile(repositories, ToolProfile::Agent).await
}

/// Serves the repository-backed MCP server with an explicit tool profile.
///
/// # Errors
///
/// Returns an error if MCP initialization or the server task fails.
pub async fn serve_stdio_with_profile(
    repositories: Repositories,
    tool_profile: ToolProfile,
) -> Result<(), String> {
    serve_stdio_with_profile_and_vault(repositories, tool_profile, None).await
}

/// Serves the repository-backed MCP server with an explicit profile and local vault access.
///
/// # Errors
///
/// Returns an error if MCP initialization or the server task fails.
pub async fn serve_stdio_with_profile_and_vault(
    repositories: Repositories,
    tool_profile: ToolProfile,
    vault_master_password: Option<SecretString>,
) -> Result<(), String> {
    serve_stdio_with_profile_vault_and_artifact_root(
        repositories,
        tool_profile,
        vault_master_password,
        DEFAULT_ARTIFACT_ROOT,
    )
    .await
}

/// Serves stdio with an explicit profile, local vault access, and artifact root.
///
/// # Errors
///
/// Returns an error if MCP initialization or the server task fails.
pub async fn serve_stdio_with_profile_vault_and_artifact_root(
    repositories: Repositories,
    tool_profile: ToolProfile,
    vault_master_password: Option<SecretString>,
    artifact_root: impl Into<PathBuf>,
) -> Result<(), String> {
    serve_stdio_with_profile_vault_artifact_root_and_agent_context(
        repositories,
        tool_profile,
        vault_master_password,
        artifact_root,
        AgentSessionContext::default(),
    )
    .await
}

/// Serves stdio with explicit tool, vault, artifact, and agent-session context.
///
/// # Errors
///
/// Returns an error if MCP initialization or the server task fails.
pub async fn serve_stdio_with_profile_vault_artifact_root_and_agent_context(
    repositories: Repositories,
    tool_profile: ToolProfile,
    vault_master_password: Option<SecretString>,
    artifact_root: impl Into<PathBuf>,
    agent_session_context: AgentSessionContext,
) -> Result<(), String> {
    let service = RemoteHostsMcpServer::with_profile_vault_and_artifact_root(
        repositories,
        tool_profile,
        vault_master_password,
        artifact_root,
    )
    .with_agent_session_context(agent_session_context)
    .serve(stdio())
    .await
    .map_err(|error| format!("serve MCP stdio transport: {error}"))?;
    service
        .waiting()
        .await
        .map_err(|error| format!("wait MCP stdio server: {error}"))?;
    Ok(())
}

#[tool_router]
impl RemoteHostsMcpServer {
    /// List registered hosts without exposing credentials.
    #[tool(
        name = "remote_hosts_list_hosts",
        description = "List registered remote hosts without credential material.",
        annotations(title = "List Hosts", read_only_hint = true, destructive_hint = false)
    )]
    async fn list_hosts(&self) -> Result<Json<ListHostsOutput>, String> {
        let hosts = self
            .repositories
            .hosts
            .list()
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .map(|host| to_json_value(&host))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(ListHostsOutput {
            count: hosts.len(),
            hosts,
        }))
    }

    /// Idempotently register one canonical host and an optional usable SSH route.
    #[tool(
        name = "remote_hosts_ensure_host",
        description = "Register or update one canonical host and optional SSH route in one idempotent call. Matches existing hosts by slug, display name, or SSH endpoint, rejects ambiguous matches, and can encrypt explicitly supplied credential material in the local vault without returning it.",
        annotations(
            title = "Ensure Host",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    #[allow(clippy::too_many_lines)]
    async fn ensure_host(
        &self,
        Parameters(request): Parameters<EnsureHostRequest>,
    ) -> Result<Json<EnsureHostOutput>, String> {
        let proposed_name = normalize_slug(&request.name, "name")?;
        let requested_display_name = trim_optional(request.display_name);
        let kind = parse_host_kind(&request.kind)?;
        let risk_level = parse_risk_level(&request.risk_level)?;
        let requested_owner = trim_optional(request.owner);
        let requested_tags = normalize_tags(request.tags.unwrap_or_default())?;
        let requested_description = trim_optional(request.description);
        for (field, value) in [
            ("display_name", requested_display_name.as_deref()),
            ("owner", requested_owner.as_deref()),
            ("description", requested_description.as_deref()),
        ] {
            if let Some(value) = value {
                ensure_no_secret_like_text(value, field)?;
            }
        }
        for tag in &requested_tags {
            ensure_no_secret_like_text(tag, "tags")?;
        }
        let access = request.access.map(prepare_ensure_access).transpose()?;
        let duplicate_request = FindHostDuplicatesRequest {
            name: Some(proposed_name.clone()),
            display_name: requested_display_name.clone(),
            address: access.as_ref().map(|access| access.address.clone()),
            port: access.as_ref().map(|access| access.port),
            username: access.as_ref().map(|access| access.username.clone()),
        };
        let mut matches = self.duplicate_host_matches(&duplicate_request).await?;
        let interactive_bastion = access.as_ref().is_some_and(|access| {
            access.route_type == RouteType::Bastion && access.requires_tty == Some(true)
        });
        if matches.len() > 1 && interactive_bastion {
            let exact_name_matches = matches
                .iter()
                .filter(|candidate| candidate.signals.iter().any(|signal| signal == "name"))
                .count();
            if exact_name_matches == 1 {
                matches.retain(|candidate| candidate.signals.iter().any(|signal| signal == "name"));
            }
        }
        if matches.len() > 1 {
            let conflicts = matches
                .iter()
                .map(|candidate| {
                    format!(
                        "{} ({}, signals={})",
                        candidate.host.name,
                        candidate.host.id,
                        candidate.signals.join("+")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "host identity is ambiguous; no changes were written. Conflicting canonical hosts: {conflicts}"
            ));
        }

        let matched = matches.pop();
        let duplicate_signals = matched
            .as_ref()
            .map(|candidate| candidate.signals.clone())
            .unwrap_or_default();
        let existing_host = matched.as_ref().map(|candidate| &candidate.host);
        let now = now_utc();
        let mut tags = existing_host
            .map(|host| host.tags.clone())
            .unwrap_or_default();
        tags.extend(requested_tags);
        let host = Host {
            id: existing_host.map_or_else(HostId::new, |host| host.id),
            name: existing_host.map_or(proposed_name, |host| host.name.clone()),
            display_name: requested_display_name.unwrap_or_else(|| {
                existing_host.map_or_else(
                    || request.name.trim().to_owned(),
                    |host| host.display_name.clone(),
                )
            }),
            kind,
            owner: requested_owner.or_else(|| existing_host.and_then(|host| host.owner.clone())),
            tags: normalize_tags(tags)?,
            description: requested_description
                .or_else(|| existing_host.and_then(|host| host.description.clone())),
            risk_level,
            created_at: existing_host.map_or(now, |host| host.created_at),
            updated_at: now,
        };
        let host_created = existing_host.is_none();

        let mut environment = None;
        let mut environment_created = false;
        let mut credential = None;
        let mut credential_created = false;
        let mut access_path = None;
        let mut access_path_created = false;
        let mut credential_secret_changed = false;
        let mut credential_status = "not_provided".to_owned();
        let mut stored_credential_fields = Vec::new();
        let mut defaults_applied = Vec::new();
        let mut attention = Vec::new();

        if let Some(mut access) = access {
            if let Some(notes) = access.notes.as_deref() {
                ensure_no_secret_like_text(notes, "access.notes")?;
            }
            let existing_environment = self
                .repositories
                .environments
                .get_by_name(&access.environment_name)
                .await
                .map_err(|error| tool_error(&error))?;
            if let Some(existing) = existing_environment.as_ref()
                && (existing.kind != access.environment_kind
                    || existing.trust_level != access.trust_level)
            {
                defaults_applied.push("environment:preserved_existing_classification".to_owned());
                attention.push(format!(
                    "environment `{}` already exists; its canonical kind and trust level were preserved",
                    access.environment_name
                ));
            }
            let mut resolved_environment = existing_environment.unwrap_or_else(|| Environment {
                id: EnvironmentId::new(),
                name: access.environment_name.clone(),
                kind: access.environment_kind.clone(),
                description: None,
                trust_level: access.trust_level.clone(),
                notes: None,
            });
            environment_created = self
                .repositories
                .environments
                .get(resolved_environment.id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_none();

            let mut existing_path = if host_created {
                None
            } else {
                self.equivalent_access_path(EquivalentAccessPathKey {
                    host_id: host.id,
                    environment_id: resolved_environment.id,
                    address: &access.address,
                    port: access.port,
                    username: &access.username,
                    route_type: &access.route_type,
                    proxy_chain: &access.proxy_chain,
                })
                .await?
            };
            if existing_path.is_none() && !host_created {
                existing_path = self
                    .same_endpoint_access_path(EquivalentAccessPathKey {
                        host_id: host.id,
                        environment_id: resolved_environment.id,
                        address: &access.address,
                        port: access.port,
                        username: &access.username,
                        route_type: &access.route_type,
                        proxy_chain: &access.proxy_chain,
                    })
                    .await?;
                if existing_path.is_some() {
                    defaults_applied.push("access_path:reclassified_route".to_owned());
                }
            }
            if let Some(existing_path) = existing_path.as_ref()
                && existing_path.environment_id != resolved_environment.id
            {
                resolved_environment = self
                    .repositories
                    .environments
                    .get(existing_path.environment_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .ok_or_else(|| {
                        format!(
                            "environment not found for existing access path: {}",
                            existing_path.environment_id
                        )
                    })?;
                environment_created = false;
                defaults_applied.push("access_path:preserved_environment".to_owned());
                attention.push(format!(
                    "the existing access path already belongs to canonical environment `{}`; the requested environment was ignored",
                    resolved_environment.name
                ));
            }

            let (mut stored_credential, mut used_default_credential) = if let Some(name) =
                access.credential_name.as_deref()
            {
                let existing = self
                    .repositories
                    .credentials
                    .get_by_name(name)
                    .await
                    .map_err(|error| tool_error(&error))?;
                match existing {
                    Some(existing) => {
                        if access.credential_secret.is_none()
                            && access
                                .credential_kind
                                .as_ref()
                                .is_some_and(|kind| kind != &existing.metadata.kind)
                        {
                            return Err(format!(
                                "credential reference `{name}` already exists with a different kind; no changes were written"
                            ));
                        }
                        (existing, false)
                    }
                    None => (
                        new_openssh_agent_credential(
                            name.to_owned(),
                            if access.credential_secret.is_none() {
                                access.credential_kind.clone()
                            } else {
                                None
                            },
                            &access.username,
                            now,
                        )?,
                        false,
                    ),
                }
            } else if let Some(existing_path) = existing_path.as_ref() {
                let existing = self
                    .repositories
                    .credentials
                    .get(existing_path.credential_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .ok_or_else(|| {
                        format!(
                            "credential not found for existing access path: {}",
                            existing_path.credential_id
                        )
                    })?;
                (existing, false)
            } else {
                let name = "openssh-default".to_owned();
                let existing = self
                    .repositories
                    .credentials
                    .get_by_name(&name)
                    .await
                    .map_err(|error| tool_error(&error))?;
                let stored = match existing {
                    Some(existing) => existing,
                    None => new_openssh_agent_credential(
                        name,
                        access.credential_kind.clone(),
                        &access.username,
                        now,
                    )?,
                };
                (stored, true)
            };
            let selected_credential_exists = self
                .repositories
                .credentials
                .get(stored_credential.metadata.id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_some();
            if let Some(secret) = access.credential_secret.take() {
                let can_update_selected = selected_credential_exists
                    && stored_credential.metadata.name != "openssh-default";
                let target_name = if stored_credential.metadata.name == "openssh-default" {
                    host_credential_name(&host, &access.username)
                } else {
                    stored_credential.metadata.name.clone()
                };
                let target_existing = if can_update_selected {
                    Some(stored_credential)
                } else {
                    self.repositories
                        .credentials
                        .get_by_name(&target_name)
                        .await
                        .map_err(|error| tool_error(&error))?
                };
                let write = self
                    .prepare_encrypted_credential(
                        target_existing,
                        target_name,
                        access.username.clone(),
                        host.kind.clone(),
                        secret,
                    )
                    .await?;
                stored_credential = write.credential;
                credential_status = write.status;
                stored_credential_fields = write.stored_fields;
                credential_secret_changed = true;
                used_default_credential = false;
            }
            if used_default_credential {
                defaults_applied.push("credential:openssh-default".to_owned());
            }
            credential_created = self
                .repositories
                .credentials
                .get(stored_credential.metadata.id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_none();

            let (selected_connector_id, connector_default, connector_attention) = self
                .select_connector_for_registration(access.connector_id, resolved_environment.id)
                .await?;
            if let Some(default) = connector_default {
                defaults_applied.push(default);
            }
            if let Some(message) = connector_attention {
                attention.push(message);
            }
            let path = AccessPath {
                id: existing_path
                    .as_ref()
                    .map_or_else(AccessPathId::new, |path| path.id),
                host_id: host.id,
                environment_id: resolved_environment.id,
                connector_id: selected_connector_id
                    .or_else(|| existing_path.as_ref().and_then(|path| path.connector_id)),
                protocol: Protocol::Ssh,
                address: existing_path
                    .as_ref()
                    .map_or(access.address, |path| path.address.clone()),
                port: access.port,
                username: existing_path
                    .as_ref()
                    .map_or(access.username, |path| path.username.clone()),
                credential_id: stored_credential.metadata.id,
                route_type: access.route_type,
                proxy_chain: access.proxy_chain,
                priority: access
                    .priority
                    .unwrap_or_else(|| existing_path.as_ref().map_or(100, |path| path.priority)),
                enabled: access
                    .enabled
                    .unwrap_or_else(|| existing_path.as_ref().is_none_or(|path| path.enabled)),
                connection_mode: access.connection_mode,
                idle_ttl_seconds: access.idle_ttl_seconds.unwrap_or_else(|| {
                    existing_path
                        .as_ref()
                        .map_or(600, |path| path.idle_ttl_seconds)
                }),
                keepalive_seconds: access.keepalive_seconds.unwrap_or_else(|| {
                    existing_path
                        .as_ref()
                        .map_or(30, |path| path.keepalive_seconds)
                }),
                max_concurrent_channels: access.max_concurrent_channels.unwrap_or_else(|| {
                    existing_path
                        .as_ref()
                        .map_or(DEFAULT_MAX_CONCURRENT_CHANNELS, |path| {
                            path.max_concurrent_channels
                        })
                }),
                max_new_connections_per_minute: access
                    .max_new_connections_per_minute
                    .unwrap_or_else(|| {
                        existing_path
                            .as_ref()
                            .map_or(1, |path| path.max_new_connections_per_minute)
                    }),
                requires_tty: access.requires_tty.unwrap_or_else(|| {
                    existing_path.as_ref().is_some_and(|path| path.requires_tty)
                }),
                notes: access
                    .notes
                    .or_else(|| existing_path.as_ref().and_then(|path| path.notes.clone())),
            };
            access_path_created = existing_path.is_none();
            environment = Some(resolved_environment);
            credential = Some(stored_credential);
            access_path = Some(path);
        } else {
            attention.push(
                "no SSH access path was supplied; register one before opening a workspace"
                    .to_owned(),
            );
        }

        self.repositories
            .hosts
            .upsert(&host)
            .await
            .map_err(|error| tool_error(&error))?;
        if let Some(environment) = environment.as_ref().filter(|_| environment_created) {
            self.repositories
                .environments
                .insert(environment)
                .await
                .map_err(|error| tool_error(&error))?;
        }
        if let Some(credential) = credential.as_ref().filter(|_| credential_secret_changed) {
            self.repositories
                .credentials
                .upsert(credential)
                .await
                .map_err(|error| tool_error(&error))?;
        } else if let Some(credential) = credential.as_ref().filter(|_| credential_created) {
            self.repositories
                .credentials
                .insert(credential)
                .await
                .map_err(|error| tool_error(&error))?;
        }
        if let Some(access_path) = access_path.as_ref() {
            self.repositories
                .access_paths
                .upsert(access_path)
                .await
                .map_err(|error| tool_error(&error))?;
        }

        Ok(Json(EnsureHostOutput {
            host_created,
            environment_created,
            credential_created,
            credential_status,
            stored_credential_fields,
            access_path_created,
            host: to_json_value(&host)?,
            environment: environment
                .as_ref()
                .map(to_json_value)
                .transpose()?
                .unwrap_or(Value::Null),
            credential: credential
                .as_ref()
                .map(|credential| to_json_value(&credential.metadata))
                .transpose()?
                .unwrap_or(Value::Null),
            access_path: access_path
                .as_ref()
                .map(to_json_value)
                .transpose()?
                .unwrap_or(Value::Null),
            duplicate_signals,
            defaults_applied,
            attention,
        }))
    }

    /// Store encrypted credential material for an existing host access path.
    #[tool(
        name = "remote_hosts_store_host_credential",
        description = "Encrypt a user-supplied SSH password, private key, passphrase, or sudo password for an existing host route. Selects the only route automatically, preserves secret fields that are not being updated, enables SSH-agent-first authentication by default, and never returns plaintext.",
        annotations(
            title = "Store Host Credential",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn store_host_credential(
        &self,
        Parameters(mut request): Parameters<StoreHostCredentialRequest>,
    ) -> Result<Json<StoreHostCredentialOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let host = self
            .repositories
            .hosts
            .get(host_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("host not found: {host_id}"))?;
        let paths = self
            .repositories
            .access_paths
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mut access_path = if let Some(access_path_id) = request.access_path_id.as_deref() {
            let access_path_id = parse_access_path_id(access_path_id)?;
            paths
                .into_iter()
                .find(|path| path.id == access_path_id)
                .ok_or_else(|| {
                    format!("access path {access_path_id} does not belong to host {host_id}")
                })?
        } else {
            match paths.len() {
                0 => return Err(format!("host {host_id} has no SSH access path")),
                1 => {
                    let Some(path) = paths.into_iter().next() else {
                        return Err(format!("host {host_id} has no SSH access path"));
                    };
                    path
                }
                count => {
                    return Err(format!(
                        "host {host_id} has {count} access paths; provide access_path_id"
                    ));
                }
            }
        };
        let selected = self
            .repositories
            .credentials
            .get(access_path.credential_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("credential not found: {}", access_path.credential_id))?;
        let target_name = if selected.metadata.name == "openssh-default" {
            host_credential_name(&host, &access_path.username)
        } else {
            selected.metadata.name.clone()
        };
        let existing = if selected.metadata.name == "openssh-default" {
            self.repositories
                .credentials
                .get_by_name(&target_name)
                .await
                .map_err(|error| tool_error(&error))?
        } else {
            Some(selected)
        };
        let write = self
            .prepare_encrypted_credential(
                existing,
                target_name,
                access_path.username.clone(),
                host.kind,
                request.take_secret(),
            )
            .await?;
        self.repositories
            .credentials
            .upsert(&write.credential)
            .await
            .map_err(|error| tool_error(&error))?;
        access_path.credential_id = write.credential.metadata.id;
        self.repositories
            .access_paths
            .upsert(&access_path)
            .await
            .map_err(|error| tool_error(&error))?;

        let mut attention = vec![
            "SSH-agent identities will be tried before the stored password; after password authentication the connector will attempt an idempotent public-key install"
                .to_owned(),
        ];
        let stale_auth_failure_cleared = self
            .repositories
            .access_path_health
            .get(access_path.id)
            .await
            .map_err(|error| tool_error(&error))?
            .is_some_and(|health| health.state == EntityState::AuthFailed);
        if stale_auth_failure_cleared {
            self.repositories
                .access_path_health
                .upsert(&AccessPathHealth {
                    access_path_id: access_path.id,
                    state: EntityState::Unknown,
                    last_checked_at: None,
                    latency_ms: None,
                    failure_count: 0,
                    last_error_code: None,
                    next_retry_at: None,
                })
                .await
                .map_err(|error| tool_error(&error))?;
            attention.push(
                "the previous authentication failure was cleared so the updated credential can be tried once"
                    .to_owned(),
            );
        }

        Ok(Json(StoreHostCredentialOutput {
            credential_status: write.status,
            stored_fields: write.stored_fields,
            credential: to_json_value(&write.credential.metadata)?,
            access_path: to_json_value(&access_path)?,
            attention,
        }))
    }

    /// Find possible duplicate hosts before registry mutation.
    #[tool(
        name = "remote_hosts_find_host_duplicates",
        description = "Find possible duplicate host records by name, display name, and SSH access path hints.",
        annotations(
            title = "Find Host Duplicates",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn find_host_duplicates(
        &self,
        Parameters(request): Parameters<FindHostDuplicatesRequest>,
    ) -> Result<Json<DuplicateHostsOutput>, String> {
        let candidates = self
            .duplicate_host_matches(&request)
            .await?
            .into_iter()
            .map(|candidate| {
                Ok(DuplicateHostCandidateOutput {
                    host: to_json_value(&candidate.host)?,
                    access_paths: values(&candidate.access_paths)?,
                    confidence: duplicate_confidence(&candidate.signals),
                    signals: candidate.signals,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(Json(DuplicateHostsOutput {
            count: candidates.len(),
            candidates,
        }))
    }

    /// Insert or update a host without creating duplicates for the same stable name.
    #[tool(
        name = "remote_hosts_upsert_host",
        description = "Insert or update one host by stable slug after duplicate checks.",
        annotations(
            title = "Upsert Host",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn upsert_host(
        &self,
        Parameters(request): Parameters<UpsertHostRequest>,
    ) -> Result<Json<HostMutationOutput>, String> {
        let name = normalize_slug(&request.name, "name")?;
        let display_name = required_trimmed(&request.display_name, "display_name")?;
        let kind = parse_host_kind(&request.kind)?;
        let risk_level = parse_risk_level(&request.risk_level)?;
        let tags = normalize_tags(request.tags.unwrap_or_default())?;
        let now = now_utc();
        let existing = self
            .repositories
            .hosts
            .get_by_name(&name)
            .await
            .map_err(|error| tool_error(&error))?;
        let created = existing.is_none();
        let host = Host {
            id: existing.as_ref().map_or_else(HostId::new, |host| host.id),
            name,
            display_name,
            kind,
            owner: trim_optional(request.owner),
            tags,
            description: trim_optional(request.description),
            risk_level,
            created_at: existing.as_ref().map_or(now, |host| host.created_at),
            updated_at: now,
        };
        self.repositories
            .hosts
            .upsert(&host)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(HostMutationOutput {
            created,
            host: to_json_value(&host)?,
            duplicate_policy: "matched_by_stable_name".to_owned(),
        }))
    }

    /// Get one host by id.
    #[tool(
        name = "remote_hosts_get_host",
        description = "Get one registered remote host by id.",
        annotations(title = "Get Host", read_only_hint = true, destructive_hint = false)
    )]
    async fn get_host(
        &self,
        Parameters(request): Parameters<HostIdRequest>,
    ) -> Result<Json<HostOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let host = self
            .repositories
            .hosts
            .get(host_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("host not found: {host_id}"))?;
        Ok(Json(HostOutput {
            host: to_json_value(&host)?,
        }))
    }

    /// List network environments.
    #[tool(
        name = "remote_hosts_list_environments",
        description = "List network environments used by host access paths.",
        annotations(
            title = "List Environments",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_environments(&self) -> Result<Json<EnvironmentsOutput>, String> {
        let environments = self
            .repositories
            .environments
            .list()
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .map(|environment| to_json_value(&environment))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(EnvironmentsOutput {
            count: environments.len(),
            environments,
        }))
    }

    /// Insert or update a network environment.
    #[tool(
        name = "remote_hosts_upsert_environment",
        description = "Insert or update one network environment by name.",
        annotations(
            title = "Upsert Environment",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn upsert_environment(
        &self,
        Parameters(request): Parameters<UpsertEnvironmentRequest>,
    ) -> Result<Json<EnvironmentMutationOutput>, String> {
        let name = normalize_slug(&request.name, "name")?;
        let kind = parse_environment_kind(&request.kind)?;
        let trust_level = parse_trust_level(&request.trust_level)?;
        let existing = self
            .repositories
            .environments
            .get_by_name(&name)
            .await
            .map_err(|error| tool_error(&error))?;
        let created = existing.is_none();
        let environment = Environment {
            id: existing
                .as_ref()
                .map_or_else(EnvironmentId::new, |environment| environment.id),
            name,
            kind,
            description: trim_optional(request.description),
            trust_level,
            notes: trim_optional(request.notes),
        };
        self.repositories
            .environments
            .upsert(&environment)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(EnvironmentMutationOutput {
            created,
            environment: to_json_value(&environment)?,
        }))
    }

    /// List credential metadata without secret blobs.
    #[tool(
        name = "remote_hosts_list_credentials",
        description = "List credential metadata without encrypted blobs or secret material.",
        annotations(
            title = "List Credentials",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_credentials(&self) -> Result<Json<CredentialsOutput>, String> {
        let credentials = self
            .repositories
            .credentials
            .list_metadata()
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .map(|credential| to_json_value(&credential))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(CredentialsOutput {
            count: credentials.len(),
            credentials,
        }))
    }

    /// Insert or update a credential reference without secret material.
    #[tool(
        name = "remote_hosts_upsert_credential_ref",
        description = "Insert or update credential metadata and a non-secret external reference; never accepts passwords or private keys.",
        annotations(
            title = "Upsert Credential Ref",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn upsert_credential_ref(
        &self,
        Parameters(request): Parameters<UpsertCredentialRefRequest>,
    ) -> Result<Json<CredentialMutationOutput>, String> {
        let name = normalize_slug(&request.name, "name")?;
        let kind = parse_credential_kind(&request.kind)?;
        if let Some(external_ref) = request.external_ref.as_deref() {
            ensure_no_secret_like_text(external_ref, "external_ref")?;
        }
        if let Some(notes) = request.notes.as_deref() {
            ensure_no_secret_like_text(notes, "notes")?;
        }
        let now = now_utc();
        let existing = self
            .repositories
            .credentials
            .get_by_name(&name)
            .await
            .map_err(|error| tool_error(&error))?;
        let created = existing.is_none();
        let metadata = CredentialMetadata {
            id: existing
                .as_ref()
                .map_or_else(CredentialId::new, |credential| credential.metadata.id),
            name,
            kind,
            username_hint: trim_optional(request.username_hint),
            created_at: existing
                .as_ref()
                .map_or(now, |credential| credential.metadata.created_at),
            updated_at: now,
            last_used_at: existing
                .as_ref()
                .and_then(|credential| credential.metadata.last_used_at),
        };
        let credential = StoredCredential {
            metadata: metadata.clone(),
            encrypted_blob_json: json!({
                "type": "external_reference",
                "external_ref": trim_optional(request.external_ref),
                "notes": trim_optional(request.notes),
            }),
        };
        self.repositories
            .credentials
            .upsert(&credential)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(CredentialMutationOutput {
            created,
            credential: to_json_value(&metadata)?,
        }))
    }

    /// Insert or update an SSH access path for a host.
    #[tool(
        name = "remote_hosts_upsert_access_path",
        description = "Insert or update one SSH access path, reusing equivalent paths to avoid duplicates.",
        annotations(
            title = "Upsert Access Path",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn upsert_access_path(
        &self,
        Parameters(request): Parameters<UpsertAccessPathRequest>,
    ) -> Result<Json<AccessPathMutationOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let environment_id = parse_environment_id(&request.environment_id)?;
        let credential_id = parse_credential_id(&request.credential_id)?;
        let connector_id = request
            .connector_id
            .as_deref()
            .map(parse_connector_id)
            .transpose()?;
        ensure_exists(
            self.repositories
                .hosts
                .get(host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_some(),
            || format!("host not found: {host_id}"),
        )?;
        ensure_exists(
            self.repositories
                .environments
                .get(environment_id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_some(),
            || format!("environment not found: {environment_id}"),
        )?;
        ensure_exists(
            self.repositories
                .credentials
                .get(credential_id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_some(),
            || format!("credential not found: {credential_id}"),
        )?;
        if let Some(connector_id) = connector_id {
            ensure_exists(
                self.repositories
                    .connectors
                    .get(connector_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .is_some(),
                || format!("connector not found: {connector_id}"),
            )?;
        }

        let route_type = parse_route_type(&request.route_type)?;
        let proxy_chain = normalize_proxy_chain(request.proxy_chain.unwrap_or_default())?;
        let address = required_trimmed(&request.address, "address")?;
        let username = required_trimmed(&request.username, "username")?;
        if request.port == 0 {
            return Err("port must be greater than 0".to_owned());
        }
        let existing = if let Some(access_path_id) = request.access_path_id.as_deref() {
            let access_path_id = parse_access_path_id(access_path_id)?;
            let access_path = self
                .repositories
                .access_paths
                .get(access_path_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("access path not found: {access_path_id}"))?;
            if access_path.host_id != host_id {
                return Err("access_path_id belongs to a different host".to_owned());
            }
            Some(access_path)
        } else {
            self.equivalent_access_path(EquivalentAccessPathKey {
                host_id,
                environment_id,
                address: &address,
                port: request.port,
                username: &username,
                route_type: &route_type,
                proxy_chain: &proxy_chain,
            })
            .await?
        };
        let created = existing.is_none();
        let path = AccessPath {
            id: existing
                .as_ref()
                .map_or_else(AccessPathId::new, |path| path.id),
            host_id,
            environment_id,
            connector_id,
            protocol: Protocol::Ssh,
            address,
            port: request.port,
            username,
            credential_id,
            route_type,
            proxy_chain,
            priority: request
                .priority
                .unwrap_or_else(|| existing.as_ref().map_or(100, |path| path.priority)),
            enabled: request
                .enabled
                .unwrap_or_else(|| existing.as_ref().is_none_or(|path| path.enabled)),
            connection_mode: parse_connection_mode(request.connection_mode.as_deref())?,
            idle_ttl_seconds: request
                .idle_ttl_seconds
                .unwrap_or_else(|| existing.as_ref().map_or(600, |path| path.idle_ttl_seconds)),
            keepalive_seconds: request
                .keepalive_seconds
                .unwrap_or_else(|| existing.as_ref().map_or(30, |path| path.keepalive_seconds)),
            max_concurrent_channels: request.max_concurrent_channels.unwrap_or_else(|| {
                existing
                    .as_ref()
                    .map_or(DEFAULT_MAX_CONCURRENT_CHANNELS, |path| {
                        path.max_concurrent_channels
                    })
            }),
            max_new_connections_per_minute: request.max_new_connections_per_minute.unwrap_or_else(
                || {
                    existing
                        .as_ref()
                        .map_or(1, |path| path.max_new_connections_per_minute)
                },
            ),
            requires_tty: request
                .requires_tty
                .unwrap_or_else(|| existing.as_ref().is_some_and(|path| path.requires_tty)),
            notes: trim_optional(request.notes),
        };
        self.repositories
            .access_paths
            .upsert(&path)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(AccessPathMutationOutput {
            created,
            access_path: to_json_value(&path)?,
            duplicate_policy: "matched_by_host_environment_address_port_username_route".to_owned(),
        }))
    }

    /// Search durable host knowledge.
    #[tool(
        name = "remote_hosts_search_knowledge",
        description = "Search durable host knowledge and operation notes.",
        annotations(
            title = "Search Knowledge",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn search_knowledge(
        &self,
        Parameters(request): Parameters<SearchKnowledgeRequest>,
    ) -> Result<Json<SearchKnowledgeOutput>, String> {
        let query = request.query.trim();
        if query.is_empty() {
            return Err("query must not be empty".to_owned());
        }
        let limit = request.limit.unwrap_or(20).clamp(1, 100);
        let items = self
            .repositories
            .knowledge
            .search(query, limit)
            .await
            .map_err(|error| tool_error(&error))?
            .into_iter()
            .map(|item| to_json_value(&item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(SearchKnowledgeOutput {
            count: items.len(),
            items,
        }))
    }

    /// Record a host fact.
    #[tool(
        name = "remote_hosts_record_host_fact",
        description = "Record one timestamped host fact with a JSON value.",
        annotations(
            title = "Record Host Fact",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn record_host_fact(
        &self,
        Parameters(request): Parameters<RecordHostFactRequest>,
    ) -> Result<Json<HostFactOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        ensure_exists(
            self.repositories
                .hosts
                .get(host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .is_some(),
            || format!("host not found: {host_id}"),
        )?;
        let namespace = required_trimmed(&request.namespace, "namespace")?;
        let key = required_trimmed(&request.key, "key")?;
        ensure_not_secret_key(&namespace, "namespace")?;
        ensure_not_secret_key(&key, "key")?;
        ensure_no_secret_like_json(&request.value, "value")?;
        let fact = HostFact {
            id: HostFactId::new(),
            host_id,
            namespace,
            key,
            value_json: request.value,
            source: parse_fact_source(request.source.as_deref().unwrap_or("manual"))?,
            observed_at: now_utc(),
            expires_at: None,
            confidence: request.confidence.unwrap_or(1.0).clamp(0.0, 1.0),
        };
        self.repositories
            .host_facts
            .insert(&fact)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(HostFactOutput {
            fact: to_json_value(&fact)?,
        }))
    }

    /// Record a durable knowledge item.
    #[tool(
        name = "remote_hosts_record_knowledge",
        description = "Record one durable redacted knowledge item linked to hosts, access paths, software, or operations.",
        annotations(
            title = "Record Knowledge",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn record_knowledge(
        &self,
        Parameters(request): Parameters<RecordKnowledgeRequest>,
    ) -> Result<Json<KnowledgeItemOutput>, String> {
        let title = required_trimmed(&request.title, "title")?;
        let body = required_trimmed(&request.body, "body")?;
        ensure_no_secret_like_text(&title, "title")?;
        ensure_no_secret_like_text(&body, "body")?;
        let linked_host_ids = parse_host_ids(request.linked_host_ids.unwrap_or_default())?;
        let linked_access_path_ids =
            parse_access_path_ids(request.linked_access_path_ids.unwrap_or_default())?;
        let linked_software_ids =
            parse_software_install_ids(request.linked_software_ids.unwrap_or_default())?;
        let linked_operation_ids =
            parse_operation_ids(request.linked_operation_ids.unwrap_or_default())?;
        for host_id in &linked_host_ids {
            ensure_exists(
                self.repositories
                    .hosts
                    .get(*host_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .is_some(),
                || format!("host not found: {host_id}"),
            )?;
        }
        for access_path_id in &linked_access_path_ids {
            ensure_exists(
                self.repositories
                    .access_paths
                    .get(*access_path_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .is_some(),
                || format!("access path not found: {access_path_id}"),
            )?;
        }
        for operation_id in &linked_operation_ids {
            ensure_exists(
                self.repositories
                    .operations
                    .get(*operation_id)
                    .await
                    .map_err(|error| tool_error(&error))?
                    .is_some(),
                || format!("operation not found: {operation_id}"),
            )?;
        }
        let now = now_utc();
        let item = KnowledgeItem {
            id: KnowledgeItemId::new(),
            title,
            body,
            source: parse_fact_source(request.source.as_deref().unwrap_or("manual"))?,
            linked_host_ids,
            linked_access_path_ids,
            linked_software_ids,
            linked_operation_ids,
            tags: normalize_tags(request.tags.unwrap_or_default())?,
            created_at: now,
            updated_at: now,
        };
        self.repositories
            .knowledge
            .insert(&item)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(KnowledgeItemOutput {
            item: to_json_value(&item)?,
        }))
    }

    /// Configure a direct, approved instance peer. The pairing token never appears in the result.
    #[tool(
        name = "remote_hosts_configure_instance_sync_peer",
        description = "Configure one approved Remote Hosts instance peer. The token is encrypted locally and credentials selected for synchronization are re-encrypted for the peer's local vault.",
        annotations(
            title = "Configure Instance Peer",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn configure_instance_sync_peer(
        &self,
        Parameters(mut request): Parameters<ConfigureInstanceSyncPeerRequest>,
    ) -> Result<Json<ConfigureInstanceSyncPeerResponse>, String> {
        let collections = parse_instance_sync_collections(request.collections.take())?;
        let service = self.instance_sync_service()?;
        let peer = service
            .configure_peer(
                std::mem::take(&mut request.display_name),
                std::mem::take(&mut request.endpoint),
                SecretString::from(std::mem::take(&mut request.token)),
                collections,
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(Json(ConfigureInstanceSyncPeerResponse {
            peer_id: peer.id.to_string(),
            display_name: peer.display_name,
            collections: peer
                .allowed_collections
                .iter()
                .map(|collection| format!("{collection:?}").to_ascii_lowercase())
                .collect(),
            next_action: "use remote_hosts_sync_instance_peer with this peer id; selected SSH credentials are copied only as peer-sealed ciphertext and re-encrypted in the receiving local vault; workspaces, PTYs, queues, and runtime state are never synchronized".to_owned(),
        }))
    }

    /// Push a bounded inventory, knowledge, and peer-sealed credential envelope to one peer.
    #[tool(
        name = "remote_hosts_sync_instance_peer",
        description = "Push approved inventory, knowledge, and authorized SSH credentials directly to one configured Remote Hosts instance peer. Credential material stays encrypted in transit and is re-encrypted in the receiving local vault.",
        annotations(
            title = "Sync Instance Peer",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn sync_instance_peer(
        &self,
        Parameters(request): Parameters<SyncInstancePeerRequest>,
    ) -> Result<Json<SyncInstancePeerResponse>, String> {
        let peer_id = InstancePeerId::from(
            Uuid::parse_str(&request.peer_id)
                .map_err(|_| format!("invalid peer_id: {}", request.peer_id))?,
        );
        let report = self
            .instance_sync_service()?
            .push(peer_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Json(SyncInstancePeerResponse {
            peer: report.peer,
            sent: report.sent,
            applied: report.result.applied,
            duplicates: report.result.duplicates,
            conflicts: report.result.conflicts,
            rejected: report.result.rejected,
            details: report.result.details,
        }))
    }

    /// Resolve the best current access path for a host.
    #[tool(
        name = "remote_hosts_resolve_access",
        description = "Resolve the best current access path for a host.",
        annotations(
            title = "Resolve Access",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn resolve_access(
        &self,
        Parameters(request): Parameters<HostIdRequest>,
    ) -> Result<Json<ResolveAccessOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let candidates = self.access_candidates_for_host(host_id).await?;
        let resolution =
            AccessResolver::resolve(&candidates).map_err(|error| resolution_error(&error))?;
        Ok(Json(ResolveAccessOutput {
            selected_access_path: to_json_value(&resolution.selected.access_path)?,
            reason: resolution.reason,
            used_cached_state: resolution.used_cached_state,
        }))
    }

    /// Get agent-visible host state.
    #[tool(
        name = "remote_hosts_get_host_state",
        description = "Get connector, access path, session, fact, and aggregate host state.",
        annotations(
            title = "Get Host State",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn get_host_state(
        &self,
        Parameters(request): Parameters<HostIdRequest>,
    ) -> Result<Json<HostStateOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let access_paths = self
            .repositories
            .access_paths
            .list_enabled_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mut access_path_health = Vec::new();
        for access_path in &access_paths {
            if let Some(snapshot) = self
                .repositories
                .access_path_health
                .get(access_path.id)
                .await
                .map_err(|error| tool_error(&error))?
            {
                access_path_health.push(snapshot);
            }
        }
        let facts = self
            .repositories
            .host_facts
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mut sessions = self
            .repositories
            .connection_sessions
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        retain_runtime_connection_sessions(&mut sessions, &access_paths);
        let connector_snapshots = self.connector_snapshots_for_paths(&access_paths).await?;
        let connector_state = if !connector_snapshots.is_empty()
            && connector_snapshots
                .iter()
                .all(|snapshot| snapshot.snapshot_state() == Some(EntityState::ConnectorOffline))
        {
            connector_snapshots
                .first()
                .and_then(ConnectorSnapshotOutput::state_snapshot)
        } else {
            None
        };
        let aggregate = HostStateAggregator::aggregate(&HostStateInput {
            connector_state,
            access_paths: access_path_health.clone(),
            sessions: sessions.clone(),
            facts: facts.clone(),
        });

        Ok(Json(HostStateOutput {
            host_id: host_id.to_string(),
            aggregate: to_json_value(&aggregate)?,
            facts: values(&facts)?,
            access_path_health: values(&access_path_health)?,
            sessions: values(&sessions)?,
            connector_snapshots,
        }))
    }

    /// Get a snapshot-first view of all runtime state for one host.
    #[tool(
        name = "remote_hosts_get_host_runtime_snapshot",
        description = "Get one consistent host runtime snapshot with connector, access path, SSH session, workspace, PTY, and recent operation state.",
        annotations(
            title = "Get Host Runtime Snapshot",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn get_host_runtime_snapshot(
        &self,
        Parameters(request): Parameters<HostIdRequest>,
    ) -> Result<Json<HostRuntimeSnapshotOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let event_cursor = self
            .repositories
            .state_events
            .latest_sequence()
            .await
            .map_err(|error| tool_error(&error))?;
        let host = self
            .repositories
            .hosts
            .get(host_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("host not found: {host_id}"))?;
        let access_paths = self
            .repositories
            .access_paths
            .list_enabled_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mut access_path_snapshots = Vec::with_capacity(access_paths.len());
        let mut access_path_health = Vec::new();
        let mut local_handshake_budget_ready_paths = BTreeSet::new();
        let mut attention = Vec::new();
        let generated_at = now_utc();
        let workspace_capacity_status = self
            .repositories
            .workspaces
            .capacity_for_host(host_id, Some(self.agent_session.id), generated_at)
            .await
            .map_err(|error| tool_error(&error))?;
        let workspace_capacity = WorkspaceCapacityOutput::new(workspace_capacity_status, 0);
        if workspace_capacity.expired_reapable > 0 {
            attention.push(RuntimeAttentionOutput {
                code: "expired_workspace_state_reapable".to_owned(),
                entity_type: "host".to_owned(),
                entity_id: host_id.to_string(),
                message: format!(
                    "{} expired logical Workspace records are safe to reconcile; this is not SSH channel pressure",
                    workspace_capacity.expired_reapable
                ),
                recommended_action: "prepare_workspace_to_reconcile_state".to_owned(),
            });
        }
        if workspace_capacity.effective_active >= workspace_capacity.limit {
            attention.push(RuntimeAttentionOutput {
                code: "logical_workspace_capacity_saturated".to_owned(),
                entity_type: "host".to_owned(),
                entity_id: host_id.to_string(),
                message: format!(
                    "logical Workspace capacity is full: limit={}, current_agent_session_active={}, other_agent_sessions_active={}",
                    workspace_capacity.limit,
                    workspace_capacity.current_agent_session_active,
                    workspace_capacity.other_agent_sessions_active
                ),
                recommended_action: "close_owned_workspace_or_wait_for_live_work".to_owned(),
            });
        }
        let write_leases = self
            .repositories
            .host_write_leases
            .list_active(host_id, generated_at)
            .await
            .map_err(|error| tool_error(&error))?;
        let write_lease_snapshot = write_lease_snapshot_value(
            &write_leases,
            self.agent_session.id,
            !self.requires_workspace_ownership(),
            generated_at,
        );
        for access_path in &access_paths {
            let mut health = self
                .repositories
                .access_path_health
                .get(access_path.id)
                .await
                .map_err(|error| tool_error(&error))?;
            if let Some(snapshot) = &mut health {
                if snapshot.last_error_code == Some(StateReasonCode::LocalHandshakeBudgetExhausted)
                {
                    if let Some(retry_at) = snapshot
                        .next_retry_at
                        .filter(|retry_at| *retry_at > generated_at)
                    {
                        let retry_after_seconds = (retry_at - generated_at)
                            .whole_seconds()
                            .max(1)
                            .cast_unsigned();
                        attention.push(RuntimeAttentionOutput {
                            code: "local_handshake_budget_exhausted".to_owned(),
                            entity_type: "access_path".to_owned(),
                            entity_id: access_path.id.to_string(),
                            message: format!(
                                "the connector's local SSH handshake budget is exhausted; retry_after_seconds={retry_after_seconds}; this does not indicate target sshd rate limiting"
                            ),
                            recommended_action: "wait_for_local_handshake_budget".to_owned(),
                        });
                    } else {
                        snapshot.state = EntityState::Unknown;
                        snapshot.last_error_code = None;
                        snapshot.next_retry_at = None;
                        local_handshake_budget_ready_paths.insert(access_path.id);
                        attention.push(RuntimeAttentionOutput {
                            code: "local_handshake_budget_ready".to_owned(),
                            entity_type: "access_path".to_owned(),
                            entity_id: access_path.id.to_string(),
                            message: "the connector-local SSH handshake cooldown has elapsed; target reachability is stale and one normal retry is allowed".to_owned(),
                            recommended_action: "retry_connection_once".to_owned(),
                        });
                    }
                }
                access_path_health.push(snapshot.clone());
            }
            let authorized_key_bootstrap = self
                .repositories
                .authorized_key_bootstrap
                .get(access_path.id)
                .await
                .map_err(|error| tool_error(&error))?;
            let transport_runtime = if let Some(connector_id) = access_path.connector_id {
                self.repositories
                    .ssh_transport_runtimes
                    .get(access_path.id, connector_id)
                    .await
                    .map_err(|error| tool_error(&error))?
            } else {
                None
            };
            let channel_usage = self
                .repositories
                .access_paths
                .channel_usage(access_path.id, generated_at)
                .await
                .map_err(|error| tool_error(&error))?;
            let channel_capacity = RuntimeChannelCapacityOutput::new(
                access_path.max_concurrent_channels,
                channel_usage,
            );
            if channel_capacity.state != "available" {
                attention.push(RuntimeAttentionOutput {
                    code: "ssh_channel_capacity_saturated".to_owned(),
                    entity_type: "access_path".to_owned(),
                    entity_id: access_path.id.to_string(),
                    message: format!(
                        "SSH channel capacity is {}; configured_limit={}, running_operations={}, active_ptys={}, pending_ptys={}",
                        channel_capacity.state,
                        channel_capacity.configured_limit,
                        channel_capacity.running_operations,
                        channel_capacity.active_ptys,
                        channel_capacity.pending_ptys
                    ),
                    recommended_action: "wait_for_channel_or_raise_limit".to_owned(),
                });
            }
            let multi_hop = access_path.requires_multi_hop_transport();
            if multi_hop {
                attention.push(RuntimeAttentionOutput {
                    code: "ssh_route_unsupported".to_owned(),
                    entity_type: "access_path".to_owned(),
                    entity_id: access_path.id.to_string(),
                    message: "this access path requires one or more jump hosts, but the active connector rejects unconfigured multi-hop routes before SSH handshake".to_owned(),
                    recommended_action: "configure_proxy_aware_route".to_owned(),
                });
            }
            if let Some(bootstrap) = &authorized_key_bootstrap {
                match bootstrap.state {
                    remote_hosts_domain::AuthorizedKeyBootstrapState::Deferred => {
                        attention.push(RuntimeAttentionOutput {
                            code: "authorized_key_bootstrap_deferred".to_owned(),
                            entity_type: "access_path".to_owned(),
                            entity_id: access_path.id.to_string(),
                            message: "automatic public-key installation is cooling down after a bounded failure; stored password access remains available".to_owned(),
                            recommended_action: "wait_for_bootstrap_retry".to_owned(),
                        });
                    }
                    remote_hosts_domain::AuthorizedKeyBootstrapState::Skipped
                        if bootstrap.reason
                            != Some(
                                remote_hosts_domain::AuthorizedKeyBootstrapReason::MultiHopUnsupported,
                            ) =>
                    {
                        let recommended_action = match bootstrap.reason {
                            Some(
                                remote_hosts_domain::AuthorizedKeyBootstrapReason::NoLocalPublicKey,
                            ) => "add_local_ssh_key",
                            _ => "continue_with_stored_password",
                        };
                        attention.push(RuntimeAttentionOutput {
                            code: "authorized_key_bootstrap_skipped".to_owned(),
                            entity_type: "access_path".to_owned(),
                            entity_id: access_path.id.to_string(),
                            message: "automatic public-key installation is disabled for this route and key; stored password access remains available".to_owned(),
                            recommended_action: recommended_action.to_owned(),
                        });
                    }
                    remote_hosts_domain::AuthorizedKeyBootstrapState::Attempting
                        if bootstrap
                            .next_retry_at
                            .is_none_or(|retry_at| retry_at <= generated_at) =>
                    {
                        attention.push(RuntimeAttentionOutput {
                            code: "authorized_key_bootstrap_stalled".to_owned(),
                            entity_type: "access_path".to_owned(),
                            entity_id: access_path.id.to_string(),
                            message: "the previous public-key installation attempt ended without a final state; its crash cooldown has elapsed".to_owned(),
                            recommended_action: "retry_connection_once".to_owned(),
                        });
                    }
                    remote_hosts_domain::AuthorizedKeyBootstrapState::Attempting
                    | remote_hosts_domain::AuthorizedKeyBootstrapState::Installed
                    | remote_hosts_domain::AuthorizedKeyBootstrapState::Skipped => {}
                }
            }
            if transport_runtime.as_ref().is_some_and(|runtime| {
                runtime.telemetry.state
                    == remote_hosts_domain::SshTransportRuntimeState::RuntimeLost
            }) {
                attention.push(RuntimeAttentionOutput {
                    code: "transport_runtime_lost".to_owned(),
                    entity_type: "access_path".to_owned(),
                    entity_id: access_path.id.to_string(),
                    message: "the connector restarted and the previously observed in-memory SSH transport no longer exists".to_owned(),
                    recommended_action: "retry_connection_once".to_owned(),
                });
            }
            access_path_snapshots.push(RuntimeAccessPathSnapshotOutput {
                access_path: to_json_value(access_path)?,
                health: optional_value(health.as_ref())?,
                authorized_key_bootstrap: optional_value(authorized_key_bootstrap.as_ref())?,
                transport_runtime: optional_value(transport_runtime.as_ref())?,
                channel_capacity,
            });
        }
        let mut sessions = self
            .repositories
            .connection_sessions
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        retain_runtime_connection_sessions(&mut sessions, &access_paths);
        for session in &mut sessions {
            if local_handshake_budget_ready_paths.contains(&session.access_path_id)
                && session.state == EntityState::Throttled
                && session.failure_count == 0
            {
                session.state = EntityState::Unknown;
                session.last_error = None;
            }
        }
        let facts = self
            .repositories
            .host_facts
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let connector_snapshots = self.connector_snapshots_for_paths(&access_paths).await?;
        let connector_state = if !connector_snapshots.is_empty()
            && connector_snapshots
                .iter()
                .all(|snapshot| snapshot.snapshot_state() == Some(EntityState::ConnectorOffline))
        {
            connector_snapshots
                .first()
                .and_then(ConnectorSnapshotOutput::state_snapshot)
        } else {
            None
        };
        let aggregate = HostStateAggregator::aggregate(&HostStateInput {
            connector_state,
            access_paths: access_path_health.clone(),
            sessions: sessions.clone(),
            facts,
        });

        let workspaces = if self.requires_workspace_ownership() {
            self.repositories
                .workspaces
                .list_for_host_and_agent_session(host_id, self.agent_session.id)
                .await
        } else {
            self.repositories.workspaces.list_for_host(host_id).await
        }
        .map_err(|error| tool_error(&error))?;
        let mut workspace_snapshots = Vec::with_capacity(workspaces.len());
        let mut current_session_waits_for_write_lease = false;
        for workspace in workspaces {
            let pty_sessions = self
                .repositories
                .pty_sessions
                .list_for_workspace(workspace.id)
                .await
                .map_err(|error| tool_error(&error))?;
            let recent_operations = self
                .repositories
                .operations
                .list_for_workspace(workspace.id, 10)
                .await
                .map_err(|error| tool_error(&error))?;
            if recent_operations.iter().any(|operation| {
                operation.requires_write_lease
                    && operation.state == remote_hosts_domain::OperationState::Queued
                    && write_leases.iter().any(|lease| {
                        lease.holder_agent_session_id != self.agent_session.id
                            && operation_coordination_scopes(operation).iter().any(
                                |operation_scope| {
                                    coordination_scopes_overlap(
                                        &lease.coordination_scope,
                                        operation_scope,
                                    )
                                },
                            )
                    })
            }) {
                current_session_waits_for_write_lease = true;
            }
            let has_live_pty_interaction = pty_sessions.iter().any(|pty| {
                pty.backend_state == remote_hosts_domain::PtyBackendState::Active
                    && pty.input_allowed
                    && pty.interaction.is_some()
            });
            if workspace.state == WorkspaceState::Blocked && !has_live_pty_interaction {
                attention.push(RuntimeAttentionOutput {
                    code: "workspace_blocked".to_owned(),
                    entity_type: "workspace".to_owned(),
                    entity_id: workspace.id.to_string(),
                    message: "workspace is blocked; inspect recent operation or PTY output before retrying"
                        .to_owned(),
                    recommended_action: "inspect_output".to_owned(),
                });
            }
            for pty in &pty_sessions {
                if let Some(interaction) = &pty.interaction
                    && pty.backend_state == remote_hosts_domain::PtyBackendState::Active
                    && pty.input_allowed
                {
                    attention.push(RuntimeAttentionOutput {
                        code: "pty_input_required".to_owned(),
                        entity_type: "pty_session".to_owned(),
                        entity_id: pty.pty_session_id.to_string(),
                        message: format!(
                            "active PTY is waiting for {:?}; read the latest PTY output, then queue input on this same PTY",
                            interaction.kind
                        ),
                        recommended_action: "read_pty_output_then_queue_input".to_owned(),
                    });
                } else if pty.backend_state == remote_hosts_domain::PtyBackendState::Failed {
                    attention.push(RuntimeAttentionOutput {
                        code: "pty_runtime_lost".to_owned(),
                        entity_type: "pty_session".to_owned(),
                        entity_id: pty.pty_session_id.to_string(),
                        message:
                            "PTY backend is failed and cannot preserve the previous runtime context"
                                .to_owned(),
                        recommended_action: "open_new_pty".to_owned(),
                    });
                }
            }
            workspace_snapshots.push(RuntimeWorkspaceSnapshotOutput {
                workspace: to_json_value(&workspace)?,
                pty_sessions: values(&pty_sessions)?,
                recent_operations: public_operation_values(&recent_operations)?,
            });
        }
        if current_session_waits_for_write_lease {
            attention.push(RuntimeAttentionOutput {
                code: "host_write_lease_wait".to_owned(),
                entity_type: "host".to_owned(),
                entity_id: host_id.to_string(),
                message:
                    "mutating work is queued behind another conversation's overlapping write-coordination scope"
                        .to_owned(),
                recommended_action: "wait_for_overlapping_scope_or_refine_scope".to_owned(),
            });
        }
        for session in &sessions {
            if local_handshake_budget_ready_paths.contains(&session.access_path_id) {
                continue;
            }
            let local_handshake_budget = access_path_health.iter().any(|health| {
                health.access_path_id == session.access_path_id
                    && health.last_error_code
                        == Some(StateReasonCode::LocalHandshakeBudgetExhausted)
            });
            let pooled_transport_recovery = access_path_health
                .iter()
                .find(|health| health.access_path_id == session.access_path_id)
                .filter(|health| {
                    health.state == EntityState::Degraded
                        && health.last_error_code
                            == Some(StateReasonCode::PooledTransportInvalidated)
                });
            if !matches!(session.state, EntityState::Connected | EntityState::Healthy) {
                if local_handshake_budget {
                    continue;
                }
                let (code, message, recommended_action) = if let Some(health) =
                    pooled_transport_recovery
                {
                    if health
                        .next_retry_at
                        .is_some_and(|retry_at| retry_at > generated_at)
                    {
                        (
                            "pooled_transport_reconnect_cooldown",
                            "the connector discarded an unhealthy pooled SSH transport after a channel failure; wait for the short cooldown before one fresh connection attempt",
                            "wait_for_pooled_transport_reconnect",
                        )
                    } else {
                        (
                            "pooled_transport_reconnect_ready",
                            "the connector already discarded the unhealthy pooled SSH transport; prepare a fresh workspace and retry one normal connection without restarting the connector or interrupting unrelated PTYs",
                            "prepare_fresh_workspace_and_retry_once",
                        )
                    }
                } else {
                    (
                        "connection_unhealthy",
                        "SSH connection session is unhealthy; inspect access-path state before retrying",
                        "inspect_access_path",
                    )
                };
                attention.push(RuntimeAttentionOutput {
                    code: code.to_owned(),
                    entity_type: "connection_session".to_owned(),
                    entity_id: session.session_id.to_string(),
                    message: format!("{message}; state={:?}", session.state),
                    recommended_action: recommended_action.to_owned(),
                });
            }
        }

        Ok(Json(HostRuntimeSnapshotOutput {
            snapshot_version: 11,
            event_cursor,
            generated_at: generated_at.to_string(),
            agent_session: to_json_value(self.agent_session.as_ref())?,
            host: to_json_value(&host)?,
            aggregate: to_json_value(&aggregate)?,
            connector_snapshots,
            access_paths: access_path_snapshots,
            connection_sessions: values(&sessions)?,
            write_lease: write_lease_snapshot,
            workspace_capacity,
            workspaces: workspace_snapshots,
            attention,
        }))
    }

    /// Record a connector heartbeat.
    #[tool(
        name = "remote_hosts_connector_heartbeat",
        description = "Record a connector heartbeat and write a state event when the state changes.",
        annotations(
            title = "Connector Heartbeat",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn connector_heartbeat(
        &self,
        Parameters(request): Parameters<ConnectorHeartbeatRequest>,
    ) -> Result<Json<ConnectorHeartbeatOutput>, String> {
        let connector_id = parse_connector_id(&request.connector_id)?;
        let state = parse_entity_state(&request.state)?;
        let observed_at = now_utc();
        let (old_state, connector) = self
            .repositories
            .connectors
            .update_heartbeat(
                connector_id,
                state,
                request.version.as_deref(),
                request.current_network.as_deref(),
                observed_at,
            )
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("connector not found: {connector_id}"))?;
        let outcome = ConnectorStateTracker::record_heartbeat(
            connector_id,
            old_state,
            connector.state.clone(),
            observed_at,
        );
        if let Some(event) = &outcome.event {
            self.repositories
                .state_events
                .insert(event)
                .await
                .map_err(|error| tool_error(&error))?;
        }

        Ok(Json(ConnectorHeartbeatOutput {
            connector: to_json_value(&connector)?,
            snapshot: to_json_value(&outcome.snapshot)?,
            event: optional_value(outcome.event.as_ref())?,
        }))
    }

    /// List recent connector state events.
    #[tool(
        name = "remote_hosts_list_connector_events",
        description = "List recent state transition events for a connector.",
        annotations(
            title = "List Connector Events",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_connector_events(
        &self,
        Parameters(request): Parameters<ListConnectorEventsRequest>,
    ) -> Result<Json<StateEventsOutput>, String> {
        let connector_id = parse_connector_id(&request.connector_id)?;
        let limit = request.limit.unwrap_or(50).clamp(1, 200);
        let events = self
            .repositories
            .state_events
            .list_for_entity("connector", &connector_id.to_string(), limit)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(StateEventsOutput {
            count: events.len(),
            events: values(&events)?,
        }))
    }

    /// Wait for sequenced runtime events with explicit live or replay semantics.
    #[tool(
        name = "remote_hosts_wait_runtime_events",
        description = "Wait for runtime state transitions. Use live_only to ignore retained history, or after_cursor to resume from a snapshot/event cursor without missing changes.",
        annotations(
            title = "Wait Runtime Events",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn wait_runtime_events(
        &self,
        Parameters(request): Parameters<WaitRuntimeEventsRequest>,
    ) -> Result<Json<RuntimeEventsOutput>, String> {
        if request.entity_id.is_some() && request.entity_type.is_none() {
            return Err("entity_id requires entity_type".to_owned());
        }
        let entity_type = normalized_event_filter(request.entity_type, "entity_type")?;
        let entity_id = normalized_event_filter(request.entity_id, "entity_id")?;
        let start_cursor = match request.start_mode {
            RuntimeEventStartMode::LiveOnly => {
                if request.after_cursor.is_some() {
                    return Err("after_cursor is forbidden when start_mode is live_only".to_owned());
                }
                self.repositories
                    .state_events
                    .latest_sequence()
                    .await
                    .map_err(|error| tool_error(&error))?
            }
            RuntimeEventStartMode::AfterCursor => request.after_cursor.ok_or_else(|| {
                "after_cursor is required when start_mode is after_cursor".to_owned()
            })?,
        };
        let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(5_000).min(60_000));
        let deadline = Instant::now() + timeout;
        let limit = request.limit.unwrap_or(50).clamp(1, 200);

        loop {
            let events = self
                .repositories
                .state_events
                .list_after(
                    start_cursor,
                    entity_type.as_deref(),
                    entity_id.as_deref(),
                    limit,
                )
                .await
                .map_err(|error| tool_error(&error))?;
            if let Some(next_cursor) = events.last().map(|event| event.sequence) {
                return Ok(Json(RuntimeEventsOutput {
                    start_cursor,
                    next_cursor,
                    timed_out: false,
                    count: events.len(),
                    events: values(&events)?,
                }));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(Json(RuntimeEventsOutput {
                    start_cursor,
                    next_cursor: start_cursor,
                    timed_out: true,
                    count: 0,
                    events: Vec::new(),
                }));
            }
            tokio::time::sleep(Duration::from_millis(200).min(deadline - now)).await;
        }
    }

    /// Get the default server protection policy.
    #[tool(
        name = "remote_hosts_get_server_protection_state",
        description = "Get the default server protection policy used to throttle agent behavior.",
        annotations(
            title = "Get Server Protection",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    fn get_server_protection_state(&self) -> Result<Json<ServerProtectionOutput>, String> {
        Ok(Json(ServerProtectionOutput {
            policy: to_json_value(&ServerProtectionPolicy::default())?,
            registered_tool_count: self.tool_router.list_all().len(),
        }))
    }

    /// List built-in command profiles accepted by workspace execution tools.
    #[tool(
        name = "remote_hosts_list_command_profiles",
        description = "List built-in structured command profiles that agents may run in workspaces.",
        annotations(
            title = "List Command Profiles",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    fn list_command_profiles(&self) -> Result<Json<CommandProfilesOutput>, String> {
        let profiles = CommandProfileCatalog::list_builtin()
            .into_iter()
            .map(|profile| to_json_value(&profile))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(CommandProfilesOutput {
            count: profiles.len(),
            profiles,
            registered_tool_count: self.tool_router.list_all().len(),
        }))
    }

    /// List agent workspaces for a host.
    #[tool(
        name = "remote_hosts_list_workspaces",
        description = "List durable agent workspaces for a host.",
        annotations(
            title = "List Workspaces",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_workspaces(
        &self,
        Parameters(request): Parameters<HostIdRequest>,
    ) -> Result<Json<WorkspacesOutput>, String> {
        let host_id = parse_host_id(&request.host_id)?;
        let workspaces = self
            .repositories
            .workspaces
            .list_for_host(host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(WorkspacesOutput {
            count: workspaces.len(),
            workspaces: values(&workspaces)?,
        }))
    }

    /// Create a durable agent workspace for a host.
    #[tool(
        name = "remote_hosts_create_workspace",
        description = "Create a durable agent workspace after resolving an access path and applying protection policy.",
        annotations(
            title = "Create Workspace",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn create_workspace(
        &self,
        Parameters(request): Parameters<CreateWorkspaceRequest>,
    ) -> Result<Json<WorkspaceOutput>, String> {
        let agent_session = self.ensure_agent_session().await?;
        let host_id = parse_host_id(&request.host_id)?;
        let access_path = if let Some(access_path_id) = request.access_path_id.as_deref() {
            let parsed = parse_access_path_id(access_path_id)?;
            self.repositories
                .access_paths
                .list_enabled_for_host(host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .into_iter()
                .find(|path| path.id == parsed)
                .ok_or_else(|| "access_path_id is not enabled for this host".to_owned())?
        } else {
            self.resolve_access_path(host_id).await?
        };
        let connector_id = match request.connector_id.as_deref() {
            Some(id) => parse_connector_id(id)?,
            None => access_path
                .connector_id
                .ok_or_else(|| "workspace creation requires a connector_id".to_owned())?,
        };
        if self
            .repositories
            .connectors
            .get(connector_id)
            .await
            .map_err(|error| tool_error(&error))?
            .is_none()
        {
            return Err(format!("connector not found: {connector_id}"));
        }

        let (_, capacity) = self
            .reconcile_workspace_capacity(host_id, agent_session.id)
            .await?;
        let policy = ServerProtectionPolicy::default();

        let workspace = WorkspaceSupervisor::default()
            .create_workspace(
                WorkspaceCreateCommand {
                    host_id,
                    access_path_id: access_path.id,
                    agent_session_id: Some(agent_session.id),
                    connector_id,
                    label: request.label,
                    cwd: request.cwd,
                    policy_profile: request
                        .policy_profile
                        .unwrap_or_else(|| "default".to_owned()),
                    coordination_scope: trim_optional(request.coordination_scope)
                        .unwrap_or_else(|| "host".to_owned()),
                    ttl_seconds: request.ttl_seconds.unwrap_or(3600),
                },
                capacity.effective_active,
            )
            .map_err(|error| workspace_capacity_error(&error.to_string(), &capacity, &policy))?;
        let inserted = self
            .repositories
            .workspaces
            .insert_below_active_limit(&workspace, policy.max_active_workspaces_per_host)
            .await
            .map_err(|error| tool_error(&error))?;
        if !inserted {
            let current = self.workspace_capacity(host_id, agent_session.id).await?;
            return Err(workspace_capacity_error(
                "logical Workspace capacity changed concurrently",
                &current,
                &policy,
            ));
        }
        Ok(Json(WorkspaceOutput {
            workspace: to_json_value(&workspace)?,
        }))
    }

    /// Reuse or create one workspace and return the context required to run work.
    #[tool(
        name = "remote_hosts_prepare_workspace",
        description = "Reuse only an idle or working workspace for a host, otherwise create one through normal access resolution; never reuse throttled, blocked, failed, done, or closed workspaces.",
        annotations(
            title = "Prepare Workspace",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn prepare_workspace(
        &self,
        Parameters(request): Parameters<PrepareWorkspaceRequest>,
    ) -> Result<Json<PreparedWorkspaceOutput>, String> {
        let agent_session = self.ensure_agent_session().await?;
        let host_id = parse_host_id(&request.host_id)?;
        let (expired_reaped, _) = self
            .reconcile_workspace_capacity(host_id, agent_session.id)
            .await?;
        let requested_access_path_id = request
            .access_path_id
            .as_deref()
            .map(parse_access_path_id)
            .transpose()?;
        let requested_coordination_scope =
            trim_optional(request.coordination_scope.clone()).unwrap_or_else(|| "host".to_owned());
        let workspaces = if self.requires_workspace_ownership() {
            self.repositories
                .workspaces
                .list_for_host_and_agent_session(host_id, agent_session.id)
                .await
        } else {
            self.repositories.workspaces.list_for_host(host_id).await
        }
        .map_err(|error| tool_error(&error))?;
        let reusable = workspaces.into_iter().find(|workspace| {
            matches!(
                workspace.state,
                WorkspaceState::Idle | WorkspaceState::Working
            ) && requested_access_path_id
                .is_none_or(|access_path_id| workspace.access_path_id == access_path_id)
                && workspace.coordination_scope == requested_coordination_scope
        });

        let (workspace, reused) = if let Some(workspace) = reusable {
            (workspace, true)
        } else {
            let Json(created) = self
                .create_workspace(Parameters(CreateWorkspaceRequest {
                    host_id: request.host_id.clone(),
                    access_path_id: request.access_path_id,
                    connector_id: None,
                    label: trim_optional(request.label).unwrap_or_else(|| "agent-main".to_owned()),
                    cwd: trim_optional(request.cwd),
                    policy_profile: trim_optional(request.policy_profile),
                    coordination_scope: Some(requested_coordination_scope),
                    ttl_seconds: request.ttl_seconds,
                }))
                .await?;
            (
                serde_json::from_value(created.workspace)
                    .map_err(|error| format!("failed to decode created workspace: {error}"))?,
                false,
            )
        };
        let full_context = !self.compact_responses();
        let runtime_snapshot = if full_context {
            Some(
                self.get_host_runtime_snapshot(Parameters(HostIdRequest {
                    host_id: request.host_id,
                }))
                .await?
                .0,
            )
        } else {
            None
        };
        let command_profiles = full_context.then(CommandProfileCatalog::list_builtin);
        let workspace_capacity = if full_context {
            Some(WorkspaceCapacityOutput::new(
                self.workspace_capacity(host_id, agent_session.id).await?,
                expired_reaped,
            ))
        } else {
            None
        };

        Ok(Json(PreparedWorkspaceOutput {
            reused,
            next_action: if workspace.state == WorkspaceState::Working {
                "run_in_workspace_or_get_workspace_result"
            } else {
                "run_in_workspace"
            }
            .to_owned(),
            agent_session: if full_context {
                to_json_value(&agent_session)?
            } else {
                compact_agent_session_value(&agent_session)
            },
            workspace: if full_context {
                to_json_value(&workspace)?
            } else {
                compact_workspace_value(&workspace)
            },
            workspace_capacity,
            runtime_snapshot,
            command_profile_count: command_profiles.as_ref().map(Vec::len),
            command_profiles: command_profiles
                .as_ref()
                .map(|profiles| values(profiles))
                .transpose()?,
        }))
    }

    /// Get one workspace by id.
    #[tool(
        name = "remote_hosts_get_workspace",
        description = "Get one durable agent workspace by id.",
        annotations(
            title = "Get Workspace",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn get_workspace(
        &self,
        Parameters(request): Parameters<WorkspaceIdRequest>,
    ) -> Result<Json<WorkspaceOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        Ok(Json(WorkspaceOutput {
            workspace: to_json_value(&workspace)?,
        }))
    }

    /// Update workspace state.
    #[tool(
        name = "remote_hosts_update_workspace_state",
        description = "Update the visible state of an agent workspace.",
        annotations(
            title = "Update Workspace State",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn update_workspace_state(
        &self,
        Parameters(request): Parameters<UpdateWorkspaceStateRequest>,
    ) -> Result<Json<WorkspaceOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        self.workspace_for_tool(workspace_id).await?;
        let workspace_state = parse_workspace_state(&request.state)?;
        let workspace = self
            .repositories
            .workspaces
            .update_state(workspace_id, workspace_state, now_utc())
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("workspace not found: {workspace_id}"))?;
        Ok(Json(WorkspaceOutput {
            workspace: to_json_value(&workspace)?,
        }))
    }

    /// List PTY sessions for a workspace.
    #[tool(
        name = "remote_hosts_list_workspace_pty_sessions",
        description = "List PTY sessions attached to a durable agent workspace.",
        annotations(
            title = "List Workspace PTYs",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_workspace_pty_sessions(
        &self,
        Parameters(request): Parameters<WorkspaceIdRequest>,
    ) -> Result<Json<PtySessionsOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        self.workspace_for_tool(workspace_id).await?;
        let sessions = self
            .repositories
            .pty_sessions
            .list_for_workspace(workspace_id)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(PtySessionsOutput {
            count: sessions.len(),
            pty_sessions: values(&sessions)?,
        }))
    }

    /// Read redacted output chunks from a PTY session.
    #[tool(
        name = "remote_hosts_read_pty_output",
        description = "Read redacted output chunks from a persistent PTY session without exposing shell input or credentials.",
        annotations(
            title = "Read PTY Output",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn read_pty_output(
        &self,
        Parameters(request): Parameters<ReadPtyOutputRequest>,
    ) -> Result<Json<PtyOutputChunksOutput>, String> {
        let pty_session_id = parse_pty_session_id(&request.pty_session_id)?;
        let pty_session = self.pty_session_for_tool(pty_session_id).await?;
        let limit = request.limit.unwrap_or(50).clamp(1, 200);
        let chunks = self
            .repositories
            .pty_output_chunks
            .list_for_session(pty_session_id, request.after_sequence, limit)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(PtyOutputChunksOutput {
            pty_session: if self.compact_responses() {
                compact_pty_session_value(&pty_session)
            } else {
                to_json_value(&pty_session)?
            },
            count: chunks.len(),
            chunks: if self.compact_responses() {
                chunks.iter().map(compact_pty_chunk_value).collect()
            } else {
                values(&chunks)?
            },
        }))
    }

    /// Queue input for connector-owned PTY delivery.
    #[tool(
        name = "remote_hosts_queue_pty_input",
        description = "Queue text for a persistent PTY, inject the route's encrypted sudo password into a live sudo prompt, inject another registered host's encrypted SSH password into a live nested SSH prompt, or inject that host's dedicated sudo password into a connector-verified nested sudo prompt. Stored passwords never enter MCP arguments, output, or audit records.",
        annotations(
            title = "Queue PTY Input",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn queue_pty_input(
        &self,
        Parameters(request): Parameters<QueuePtyInputRequest>,
    ) -> Result<Json<PtyInputEventOutput>, String> {
        let pty_session_id = parse_pty_session_id(&request.pty_session_id)?;
        let pty_session = self.pty_session_for_tool(pty_session_id).await?;
        let workspace = self.workspace_for_tool(pty_session.workspace_id).await?;
        let idempotency_key = normalize_idempotency_key(request.idempotency_key)?;
        let next_sequence = self
            .repositories
            .pty_input_events
            .next_sequence(pty_session_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let mode_count = usize::from(request.input.is_some())
            + usize::from(request.use_stored_sudo_password)
            + usize::from(request.use_stored_password_from_host_id.is_some())
            + usize::from(request.use_stored_sudo_password_from_host_id.is_some());
        if mode_count != 1 {
            return Err(
                "exactly one of input, use_stored_sudo_password, use_stored_password_from_host_id, or use_stored_sudo_password_from_host_id is required"
                    .to_owned(),
            );
        }
        let plan = if request.use_stored_sudo_password {
            if request.requested_by.is_some() {
                return Err(
                    "requested_by must be omitted when use_stored_sudo_password is true".to_owned(),
                );
            }
            PtySessionSupervisor::default()
                .queue_stored_sudo_password(
                    &pty_session,
                    &workspace,
                    next_sequence,
                    idempotency_key,
                )
                .map_err(|error| error.to_string())?
        } else if let Some(target_host_id) =
            request.use_stored_sudo_password_from_host_id.as_deref()
        {
            if request.requested_by.is_some() {
                return Err(
                    "requested_by must be omitted when use_stored_sudo_password_from_host_id is set"
                        .to_owned(),
                );
            }
            let target_host_id = parse_host_id(target_host_id)?;
            self.repositories
                .hosts
                .get(target_host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("host not found: {target_host_id}"))?;
            let target_paths = self
                .repositories
                .access_paths
                .list_enabled_for_host(target_host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .into_iter()
                .filter(|path| path.protocol == Protocol::Ssh)
                .collect::<Vec<_>>();
            let target_access_path = match target_paths.as_slice() {
                [target_access_path] => target_access_path,
                [] => {
                    return Err(format!(
                        "host {target_host_id} has no enabled SSH access path"
                    ));
                }
                paths => {
                    return Err(format!(
                        "host {target_host_id} has {} enabled SSH access paths; nested stored-sudo injection requires exactly one",
                        paths.len()
                    ));
                }
            };
            PtySessionSupervisor::default()
                .queue_stored_target_sudo_password(
                    &pty_session,
                    &workspace,
                    next_sequence,
                    idempotency_key,
                    target_access_path.id,
                )
                .map_err(|error| error.to_string())?
        } else if let Some(target_host_id) = request.use_stored_password_from_host_id.as_deref() {
            if request.requested_by.is_some() {
                return Err(
                    "requested_by must be omitted when use_stored_password_from_host_id is set"
                        .to_owned(),
                );
            }
            let target_host_id = parse_host_id(target_host_id)?;
            self.repositories
                .hosts
                .get(target_host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("host not found: {target_host_id}"))?;
            let target_paths = self
                .repositories
                .access_paths
                .list_enabled_for_host(target_host_id)
                .await
                .map_err(|error| tool_error(&error))?
                .into_iter()
                .filter(|path| path.protocol == Protocol::Ssh)
                .collect::<Vec<_>>();
            let target_access_path = match target_paths.as_slice() {
                [target_access_path] => target_access_path,
                [] => {
                    return Err(format!(
                        "host {target_host_id} has no enabled SSH access path"
                    ));
                }
                paths => {
                    return Err(format!(
                        "host {target_host_id} has {} enabled SSH access paths; nested stored-password injection requires exactly one",
                        paths.len()
                    ));
                }
            };
            PtySessionSupervisor::default()
                .queue_stored_ssh_password(
                    &pty_session,
                    &workspace,
                    next_sequence,
                    idempotency_key,
                    target_access_path.id,
                )
                .map_err(|error| error.to_string())?
        } else {
            let input = request.input.ok_or_else(|| {
                "input is required when no stored-password mode is selected".to_owned()
            })?;
            PtySessionSupervisor::default()
                .queue_input(
                    &pty_session,
                    &workspace,
                    next_sequence,
                    PtySessionInputCommand {
                        input,
                        requested_by: request.requested_by,
                        idempotency_key,
                    },
                )
                .map_err(|error| error.to_string())?
        };
        if let (Some(agent_session_id), Some(idempotency_key)) = (
            plan.event.agent_session_id,
            plan.event.idempotency_key.as_deref(),
        ) && let Some(existing) = self
            .repositories
            .pty_input_events
            .get_by_agent_session_and_idempotency_key(agent_session_id, idempotency_key)
            .await
            .map_err(|error| tool_error(&error))?
        {
            ensure_matching_pty_idempotent_request(&existing, &plan.event, idempotency_key)?;
            return Ok(Json(PtyInputEventOutput {
                input_event: to_json_value(&existing)?,
                idempotency_reused: true,
            }));
        }
        if let Err(error) = self
            .repositories
            .pty_input_events
            .insert(&plan.event, &plan.input_text)
            .await
        {
            if let (Some(agent_session_id), Some(idempotency_key)) = (
                plan.event.agent_session_id,
                plan.event.idempotency_key.as_deref(),
            ) && let Some(existing) = self
                .repositories
                .pty_input_events
                .get_by_agent_session_and_idempotency_key(agent_session_id, idempotency_key)
                .await
                .map_err(|lookup_error| tool_error(&lookup_error))?
            {
                ensure_matching_pty_idempotent_request(&existing, &plan.event, idempotency_key)?;
                return Ok(Json(PtyInputEventOutput {
                    input_event: to_json_value(&existing)?,
                    idempotency_reused: true,
                }));
            }
            return Err(tool_error(&error));
        }
        if let Some(agent_session_id) = plan.event.agent_session_id {
            let observed_at = now_utc();
            let leases = pty_coordination_scopes(&pty_session, &workspace)
                .into_iter()
                .map(|coordination_scope| HostWriteLease {
                    host_id: plan.event.host_id,
                    coordination_scope,
                    holder_agent_session_id: agent_session_id,
                    holder_workspace_id: plan.event.workspace_id,
                    acquired_at: observed_at,
                    heartbeat_at: observed_at,
                    expires_at: observed_at + time::Duration::seconds(WRITE_LEASE_SECONDS),
                })
                .collect::<Vec<_>>();
            self.repositories
                .host_write_leases
                .try_acquire_many(&leases, observed_at)
                .await
                .map_err(|error| tool_error(&error))?;
        }
        Ok(Json(PtyInputEventOutput {
            input_event: to_json_value(&plan.event)?,
            idempotency_reused: false,
        }))
    }

    /// List queued and delivered PTY input event metadata.
    #[tool(
        name = "remote_hosts_list_pty_input_events",
        description = "List PTY input event metadata so agents can observe queued, delivered, or failed input without seeing raw payloads.",
        annotations(
            title = "List PTY Input Events",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_pty_input_events(
        &self,
        Parameters(request): Parameters<ListPtyInputEventsRequest>,
    ) -> Result<Json<PtyInputEventsOutput>, String> {
        let pty_session_id = parse_pty_session_id(&request.pty_session_id)?;
        let pty_session = self.pty_session_for_tool(pty_session_id).await?;
        let events = self
            .repositories
            .pty_input_events
            .list_for_session(
                pty_session_id,
                request.after_sequence,
                request.limit.unwrap_or(50).clamp(1, 200),
            )
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(PtyInputEventsOutput {
            pty_session: to_json_value(&pty_session)?,
            count: events.len(),
            input_events: values(&events)?,
        }))
    }

    /// Open a persistent PTY session record for a workspace.
    #[tool(
        name = "remote_hosts_open_workspace_pty_session",
        description = "Open a policy-guarded persistent PTY session; the service reuses or creates the backing connection session.",
        annotations(
            title = "Open Workspace PTY",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn open_workspace_pty_session(
        &self,
        Parameters(request): Parameters<OpenPtySessionRequest>,
    ) -> Result<Json<PtySessionOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let connection = self
            .connection_for_workspace(&workspace, request.session_id.as_deref())
            .await?;
        let session_id = connection.session_id;
        let active_ptys = self
            .repositories
            .pty_sessions
            .count_active_for_host(workspace.host_id)
            .await
            .map_err(|error| tool_error(&error))?;
        let pty_session = PtySessionSupervisor::default()
            .open_session(
                &workspace,
                &connection,
                active_ptys,
                PtySessionOpenCommand {
                    session_id,
                    cwd: request.cwd,
                    coordination_scopes: request.coordination_scopes,
                },
            )
            .map_err(|error| error.to_string())?;
        self.repositories
            .pty_sessions
            .upsert(&pty_session)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(pty_session_output(&pty_session)?))
    }

    /// Update a PTY session heartbeat.
    #[tool(
        name = "remote_hosts_heartbeat_pty_session",
        description = "Update a PTY session state, foreground process, cwd, output reference, and input allowance.",
        annotations(
            title = "Heartbeat PTY",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn heartbeat_pty_session(
        &self,
        Parameters(request): Parameters<HeartbeatPtySessionRequest>,
    ) -> Result<Json<PtySessionOutput>, String> {
        let pty_session_id = parse_pty_session_id(&request.pty_session_id)?;
        let state = parse_workspace_state(&request.state)?;
        let pty_session = self.pty_session_for_tool(pty_session_id).await?;
        let updated = PtySessionSupervisor::default()
            .heartbeat(
                pty_session,
                PtySessionHeartbeatCommand {
                    state,
                    foreground_process: request.foreground_process,
                    cwd: request.cwd,
                    recent_output_ref: request.recent_output_ref,
                    last_exit_code: request.last_exit_code,
                    input_allowed: request.input_allowed,
                },
            )
            .map_err(|error| error.to_string())?;
        retry_sqlite_contention(|| async {
            self.repositories.pty_sessions.upsert(&updated).await?;
            if updated.state != WorkspaceState::Closed {
                self.repositories
                    .workspaces
                    .update_state(
                        updated.workspace_id,
                        updated.state.clone(),
                        updated.last_activity_at,
                    )
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(|error| tool_error(&error))?;
        Ok(Json(pty_session_output(&updated)?))
    }

    /// Close a PTY session.
    #[tool(
        name = "remote_hosts_close_pty_session",
        description = "Close a PTY session and disable further input.",
        annotations(
            title = "Close PTY",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn close_pty_session(
        &self,
        Parameters(request): Parameters<ClosePtySessionRequest>,
    ) -> Result<Json<PtySessionOutput>, String> {
        let pty_session_id = parse_pty_session_id(&request.pty_session_id)?;
        let pty_session = self.pty_session_for_tool(pty_session_id).await?;
        let closed = PtySessionSupervisor::default().close(pty_session, request.last_exit_code);
        retry_sqlite_contention(|| async {
            self.repositories.pty_sessions.upsert(&closed).await?;
            if self
                .repositories
                .pty_sessions
                .count_active_for_workspace(closed.workspace_id)
                .await?
                == 0
            {
                self.repositories
                    .workspaces
                    .update_state(
                        closed.workspace_id,
                        WorkspaceState::Idle,
                        closed.last_activity_at,
                    )
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(|error| tool_error(&error))?;
        Ok(Json(pty_session_output(&closed)?))
    }

    /// Reap expired PTY sessions.
    #[tool(
        name = "remote_hosts_reap_expired_pty_sessions",
        description = "Close expired active PTY session records after an idle TTL.",
        annotations(
            title = "Reap Expired PTYs",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn reap_expired_pty_sessions(
        &self,
        Parameters(request): Parameters<ReapExpiredPtySessionsRequest>,
    ) -> Result<Json<PtySessionsOutput>, String> {
        let idle_ttl_seconds = request.idle_ttl_seconds.unwrap_or(3600).clamp(60, 86_400);
        let limit = request.limit.unwrap_or(100).clamp(1, 500);
        let sessions = self
            .repositories
            .pty_sessions
            .close_expired(now_utc(), idle_ttl_seconds, limit)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(PtySessionsOutput {
            count: sessions.len(),
            pty_sessions: values(&sessions)?,
        }))
    }

    /// Close a workspace so it will not accept more operations.
    #[tool(
        name = "remote_hosts_close_workspace",
        description = "Close a durable agent workspace and prevent new operations from being queued.",
        annotations(
            title = "Close Workspace",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn close_workspace(
        &self,
        Parameters(request): Parameters<WorkspaceIdRequest>,
    ) -> Result<Json<WorkspaceOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        self.workspace_for_tool(workspace_id).await?;
        let workspace = self
            .repositories
            .workspaces
            .update_state(workspace_id, WorkspaceState::Closed, now_utc())
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("workspace not found: {workspace_id}"))?;
        self.repositories
            .pty_sessions
            .close_for_workspace(workspace_id, workspace.last_activity_at)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(WorkspaceOutput {
            workspace: to_json_value(&workspace)?,
        }))
    }

    /// Queue a managed command profile in a workspace.
    #[tool(
        name = "remote_hosts_run_in_workspace",
        description = "Queue a narrow profile or managed POSIX/PowerShell script on pooled SSH. Declare shell coordination_mode=read_only for observation or mutating for scoped changes; auto keeps legacy conservative behavior.",
        annotations(
            title = "Run In Workspace",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn run_in_workspace(
        &self,
        Parameters(request): Parameters<RunInWorkspaceRequest>,
    ) -> Result<Json<QueuedOperationOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let wait_timeout_ms = request.wait_timeout_ms;
        let mut output = self.queue_workspace_operation(request).await?;
        if let Some(wait_timeout_ms) = wait_timeout_ms {
            let operation_id = output
                .operation
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| "queued operation response omitted its id".to_owned())
                .and_then(parse_operation_id)?;
            output.completion = Some(
                self.wait_for_operation_completion(workspace_id, operation_id, wait_timeout_ms)
                    .await?,
            );
        }
        Ok(Json(output))
    }

    /// Upload one file over a workspace's pooled SSH session.
    #[tool(
        name = "remote_hosts_upload_file",
        description = "Queue a bounded, SHA-256-verified file upload through an existing workspace and pooled SSH/SFTP session.",
        annotations(
            title = "Upload File",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn upload_file(
        &self,
        Parameters(request): Parameters<UploadFileRequest>,
    ) -> Result<Json<QueuedOperationOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let spec = file_transfer_spec(
            SftpDirection::Upload,
            &request.local_path,
            &request.remote_path,
            request.overwrite.as_deref(),
            request.mode.as_deref(),
            request.max_size_bytes,
            request.expected_sha256,
            request.timeout_seconds,
        )?;
        let output = self
            .queue_workspace_file_transfer(
                workspace_id,
                spec,
                request.intent,
                request.idempotency_key,
            )
            .await?;
        Ok(Json(output))
    }

    /// Download one file over a workspace's pooled SSH session.
    #[tool(
        name = "remote_hosts_download_file",
        description = "Queue a bounded, SHA-256-verified file download through an existing workspace and pooled SSH/SFTP session.",
        annotations(
            title = "Download File",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn download_file(
        &self,
        Parameters(request): Parameters<DownloadFileRequest>,
    ) -> Result<Json<QueuedOperationOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let spec = file_transfer_spec(
            SftpDirection::Download,
            &request.local_path,
            &request.remote_path,
            request.overwrite.as_deref(),
            request.mode.as_deref(),
            request.max_size_bytes,
            request.expected_sha256,
            request.timeout_seconds,
        )?;
        let output = self
            .queue_workspace_file_transfer(
                workspace_id,
                spec,
                request.intent,
                request.idempotency_key,
            )
            .await?;
        Ok(Json(output))
    }

    /// Read redacted output chunks from a workspace.
    #[tool(
        name = "remote_hosts_read_workspace_output",
        description = "Read redacted output chunks for a workspace or one queued operation.",
        annotations(
            title = "Read Workspace Output",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn read_workspace_output(
        &self,
        Parameters(request): Parameters<ReadWorkspaceOutputRequest>,
    ) -> Result<Json<WorkspaceOutputChunksOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let operation_id = request
            .operation_id
            .as_deref()
            .map(parse_operation_id)
            .transpose()?;
        if operation_id.is_none() && request.after_sequence.is_some() {
            return Err("after_sequence requires operation_id".to_owned());
        }
        let requested_operation = if let Some(operation_id) = operation_id {
            let operation = self
                .repositories
                .operations
                .get(operation_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("operation not found: {operation_id}"))?;
            if operation.workspace_id != Some(workspace_id) {
                return Err("operation does not belong to workspace".to_owned());
            }
            Some(operation)
        } else {
            None
        };
        let limit = request.limit.unwrap_or(50).clamp(1, 200);
        let chunks = self
            .repositories
            .operation_output_chunks
            .list_for_workspace(workspace_id, operation_id, request.after_sequence, limit)
            .await
            .map_err(|error| tool_error(&error))?;
        let operations = if let Some(operation) = requested_operation {
            vec![operation]
        } else {
            self.repositories
                .operations
                .list_for_workspace(workspace_id, if self.compact_responses() { 3 } else { 10 })
                .await
                .map_err(|error| tool_error(&error))?
        };
        Ok(Json(WorkspaceOutputChunksOutput {
            workspace: if self.compact_responses() {
                compact_workspace_status_value(&workspace)
            } else {
                to_json_value(&workspace)?
            },
            count: chunks.len(),
            chunks: if self.compact_responses() {
                chunks.iter().map(compact_operation_chunk_value).collect()
            } else {
                values(&chunks)?
            },
            recent_operations: if self.compact_responses() {
                operations.iter().map(compact_operation_value).collect()
            } else {
                public_operation_values(&operations)?
            },
        }))
    }

    /// Read output, operations, and artifact metadata in one bounded result.
    #[tool(
        name = "remote_hosts_get_workspace_result",
        description = "Get one bounded workspace result containing redacted output chunks, recent operations, and large-output artifact metadata.",
        annotations(
            title = "Get Workspace Result",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn get_workspace_result(
        &self,
        Parameters(request): Parameters<GetWorkspaceResultRequest>,
    ) -> Result<Json<WorkspaceResultOutput>, String> {
        let Json(output) = self
            .read_workspace_output(Parameters(ReadWorkspaceOutputRequest {
                workspace_id: request.workspace_id.clone(),
                operation_id: request.operation_id.clone(),
                after_sequence: request.after_sequence,
                limit: request.limit,
            }))
            .await?;
        let Json(artifact_output) = self
            .list_workspace_output_artifacts(Parameters(ListWorkspaceOutputArtifactsRequest {
                workspace_id: request.workspace_id,
                operation_id: request.operation_id,
                limit: request.limit,
            }))
            .await?;

        Ok(Json(WorkspaceResultOutput {
            workspace: output.workspace,
            chunk_count: output.count,
            chunks: output.chunks,
            recent_operations: output.recent_operations,
            artifact_count: artifact_output.count,
            artifacts: artifact_output.artifacts,
        }))
    }

    /// List file-backed output artifacts for a workspace.
    #[tool(
        name = "remote_hosts_list_workspace_output_artifacts",
        description = "List file-backed redacted output artifact metadata and previews for a workspace.",
        annotations(
            title = "List Workspace Output Artifacts",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn list_workspace_output_artifacts(
        &self,
        Parameters(request): Parameters<ListWorkspaceOutputArtifactsRequest>,
    ) -> Result<Json<WorkspaceOutputArtifactsOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let workspace = self.workspace_for_tool(workspace_id).await?;
        let operation_id = request
            .operation_id
            .as_deref()
            .map(parse_operation_id)
            .transpose()?;
        if let Some(operation_id) = operation_id {
            let operation = self
                .repositories
                .operations
                .get(operation_id)
                .await
                .map_err(|error| tool_error(&error))?
                .ok_or_else(|| format!("operation not found: {operation_id}"))?;
            if operation.workspace_id != Some(workspace_id) {
                return Err("operation does not belong to workspace".to_owned());
            }
        }
        let limit = request.limit.unwrap_or(50).clamp(1, 200);
        let artifacts = self
            .repositories
            .operation_output_artifacts
            .list_for_workspace(workspace_id, operation_id, limit)
            .await
            .map_err(|error| tool_error(&error))?;
        Ok(Json(WorkspaceOutputArtifactsOutput {
            workspace: if self.compact_responses() {
                compact_workspace_status_value(&workspace)
            } else {
                to_json_value(&workspace)?
            },
            count: artifacts.len(),
            artifacts: if self.compact_responses() {
                artifacts.iter().map(compact_artifact_value).collect()
            } else {
                values(&artifacts)?
            },
        }))
    }

    /// Get one output artifact metadata record.
    #[tool(
        name = "remote_hosts_get_output_artifact",
        description = "Get one file-backed redacted output artifact metadata record by id.",
        annotations(
            title = "Get Output Artifact",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn get_output_artifact(
        &self,
        Parameters(request): Parameters<OutputArtifactIdRequest>,
    ) -> Result<Json<OutputArtifactOutput>, String> {
        let artifact_id = parse_output_artifact_id(&request.artifact_id)?;
        let artifact = self
            .repositories
            .operation_output_artifacts
            .get(artifact_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("output artifact not found: {artifact_id}"))?;
        self.workspace_for_tool(artifact.workspace_id).await?;
        Ok(Json(OutputArtifactOutput {
            artifact: to_json_value(&artifact)?,
        }))
    }

    /// Read a bounded chunk from one redacted output artifact.
    #[tool(
        name = "remote_hosts_read_output_artifact_content",
        description = "Read one bounded UTF-8 chunk from a redacted large-output artifact; continue with next_offset.",
        annotations(
            title = "Read Output Artifact Content",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn read_output_artifact_content(
        &self,
        Parameters(request): Parameters<ReadOutputArtifactContentRequest>,
    ) -> Result<Json<OutputArtifactContentOutput>, String> {
        let artifact_id = parse_output_artifact_id(&request.artifact_id)?;
        let artifact = self
            .repositories
            .operation_output_artifacts
            .get(artifact_id)
            .await
            .map_err(|error| tool_error(&error))?
            .ok_or_else(|| format!("output artifact not found: {artifact_id}"))?;
        self.workspace_for_tool(artifact.workspace_id).await?;
        let offset = request.offset.unwrap_or(0);
        if offset > artifact.byte_len {
            return Err(format!(
                "offset exceeds artifact length: offset={offset}, byte_len={}",
                artifact.byte_len
            ));
        }
        let max_bytes = request
            .max_bytes
            .unwrap_or(DEFAULT_ARTIFACT_READ_BYTES)
            .clamp(1_024, MAX_ARTIFACT_READ_BYTES);
        let chunk = read_artifact_utf8_chunk(
            self.artifact_root.as_ref(),
            &artifact.relative_path,
            artifact.byte_len,
            offset,
            max_bytes,
        )
        .await?;
        let bytes_read = chunk.len();
        let next_offset = offset
            .checked_add(
                u64::try_from(bytes_read)
                    .map_err(|error| format!("artifact chunk length conversion: {error}"))?,
            )
            .ok_or_else(|| "artifact offset overflow".to_owned())?;
        Ok(Json(OutputArtifactContentOutput {
            artifact_id: artifact_id.to_string(),
            offset,
            next_offset,
            bytes_read,
            eof: next_offset >= artifact.byte_len,
            sha256: artifact.sha256,
            content: chunk,
        }))
    }

    /// Wait for a workspace to enter a desired visible state.
    #[tool(
        name = "remote_hosts_wait_workspace_state",
        description = "Wait briefly for a workspace to enter desired states and return the latest visible state.",
        annotations(
            title = "Wait Workspace State",
            read_only_hint = true,
            destructive_hint = false
        )
    )]
    async fn wait_workspace_state(
        &self,
        Parameters(request): Parameters<WaitWorkspaceStateRequest>,
    ) -> Result<Json<WaitWorkspaceStateOutput>, String> {
        let workspace_id = parse_workspace_id(&request.workspace_id)?;
        let desired_states = parse_desired_workspace_states(request.desired_states)?;
        let timeout_ms = request.timeout_ms.unwrap_or(5000).clamp(0, 60_000);
        let poll_interval_ms = request.poll_interval_ms.unwrap_or(250).clamp(100, 5000);
        let started_at = std::time::Instant::now();
        let deadline = started_at + Duration::from_millis(timeout_ms);

        loop {
            let workspace = self.workspace_for_tool(workspace_id).await?;
            if desired_states.contains(&workspace.state) {
                return Ok(Json(WaitWorkspaceStateOutput {
                    matched: true,
                    workspace: to_json_value(&workspace)?,
                    desired_states: workspace_states_to_values(&desired_states)?,
                    elapsed_ms: elapsed_ms(started_at),
                    retry_after_ms: None,
                }));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(Json(WaitWorkspaceStateOutput {
                    matched: false,
                    workspace: to_json_value(&workspace)?,
                    desired_states: workspace_states_to_values(&desired_states)?,
                    elapsed_ms: elapsed_ms(started_at),
                    retry_after_ms: Some(poll_interval_ms),
                }));
            }
            tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
        }
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "remote-hosts",
    version = "0.1.0",
    instructions = "Manage remote SSH hosts, access paths, connector state, and agent workspaces without exposing secrets."
)]
impl ServerHandler for RemoteHostsMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(format!(
                "Manage remote SSH hosts, access paths, connector state, and agent workspaces without exposing secrets. Active tool profile: {}.",
                self.tool_profile.as_str()
            ))
    }
}

/// List hosts tool output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ListHostsOutput {
    /// Number of returned hosts.
    pub count: usize,
    /// Host records as JSON.
    pub hosts: Vec<Value>,
}

/// Host tool output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HostOutput {
    /// Host record as JSON.
    pub host: Value,
}

/// Host registry mutation output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HostMutationOutput {
    /// Whether a new row was created.
    pub created: bool,
    /// Host record as JSON.
    pub host: Value,
    /// Duplicate avoidance policy used.
    pub duplicate_policy: String,
}

/// Result of idempotent task-level host registration.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[allow(clippy::struct_excessive_bools)]
pub struct EnsureHostOutput {
    /// Whether the canonical host was created.
    pub host_created: bool,
    /// Whether the environment was created.
    pub environment_created: bool,
    /// Whether a non-secret credential reference was created.
    pub credential_created: bool,
    /// Secret write result: `created`, `updated`, or `not_provided`.
    pub credential_status: String,
    /// Secret field names that were encrypted, never their values.
    pub stored_credential_fields: Vec<String>,
    /// Whether the SSH access path was created.
    pub access_path_created: bool,
    /// Canonical host record.
    pub host: Value,
    /// Environment record, or null when no access path was supplied.
    pub environment: Value,
    /// Credential metadata only, or null when no access path was supplied.
    pub credential: Value,
    /// SSH access path, or null when none was supplied.
    pub access_path: Value,
    /// Identity signals that matched an existing canonical host.
    pub duplicate_signals: Vec<String>,
    /// Explicit defaults selected by the service.
    pub defaults_applied: Vec<String>,
    /// Follow-up conditions that prevent immediate use.
    pub attention: Vec<String>,
}

/// Result of storing encrypted credential material for a host route.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct StoreHostCredentialOutput {
    /// Whether the encrypted credential row was created or updated.
    pub credential_status: String,
    /// Secret field names that were supplied, never their values.
    pub stored_fields: Vec<String>,
    /// Credential metadata only.
    pub credential: Value,
    /// Access path now linked to the credential.
    pub access_path: Value,
    /// Follow-up behavior the connector will perform.
    pub attention: Vec<String>,
}

/// Environment list output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct EnvironmentsOutput {
    /// Number of returned environments.
    pub count: usize,
    /// Environment records as JSON.
    pub environments: Vec<Value>,
}

/// Environment mutation output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct EnvironmentMutationOutput {
    /// Whether a new row was created.
    pub created: bool,
    /// Environment record as JSON.
    pub environment: Value,
}

/// Credential metadata list output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CredentialsOutput {
    /// Number of returned credentials.
    pub count: usize,
    /// Credential metadata records as JSON.
    pub credentials: Vec<Value>,
}

/// Credential reference mutation output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CredentialMutationOutput {
    /// Whether a new row was created.
    pub created: bool,
    /// Credential metadata as JSON.
    pub credential: Value,
}

/// Access path mutation output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct AccessPathMutationOutput {
    /// Whether a new row was created.
    pub created: bool,
    /// Access path as JSON.
    pub access_path: Value,
    /// Duplicate avoidance policy used.
    pub duplicate_policy: String,
}

/// Duplicate host candidates output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DuplicateHostsOutput {
    /// Number of returned candidates.
    pub count: usize,
    /// Candidate records.
    pub candidates: Vec<DuplicateHostCandidateOutput>,
}

/// Duplicate host candidate output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DuplicateHostCandidateOutput {
    /// Host record as JSON.
    pub host: Value,
    /// Access paths as JSON.
    pub access_paths: Vec<Value>,
    /// Match signals.
    pub signals: Vec<String>,
    /// Confidence from 0.0 to 1.0.
    pub confidence: f32,
}

/// Knowledge search output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SearchKnowledgeOutput {
    /// Number of returned items.
    pub count: usize,
    /// Knowledge records as JSON.
    pub items: Vec<Value>,
}

/// Host fact output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HostFactOutput {
    /// Host fact as JSON.
    pub fact: Value,
}

/// Knowledge item output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct KnowledgeItemOutput {
    /// Knowledge item as JSON.
    pub item: Value,
}

/// Resolve access tool output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ResolveAccessOutput {
    /// Selected access path as JSON.
    pub selected_access_path: Value,
    /// Selection reason.
    pub reason: String,
    /// Whether cached state contributed to the resolution.
    pub used_cached_state: bool,
}

/// Host state tool output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HostStateOutput {
    /// Host id.
    pub host_id: String,
    /// Aggregate state as JSON.
    pub aggregate: Value,
    /// Host facts as JSON.
    pub facts: Vec<Value>,
    /// Access path health snapshots as JSON.
    pub access_path_health: Vec<Value>,
    /// Connection sessions as JSON.
    pub sessions: Vec<Value>,
    /// Connector snapshots.
    pub connector_snapshots: Vec<ConnectorSnapshotOutput>,
}

/// Snapshot-first host runtime output for agent bootstrap and diagnosis.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct HostRuntimeSnapshotOutput {
    /// Snapshot schema version.
    pub snapshot_version: u32,
    /// State-event cursor captured before snapshot reads begin.
    pub event_cursor: u64,
    /// Snapshot generation timestamp.
    pub generated_at: String,
    /// Current MCP client-session identity and isolation scope.
    pub agent_session: Value,
    /// Host record as JSON.
    pub host: Value,
    /// Aggregate host state as JSON.
    pub aggregate: Value,
    /// Connector records with freshness-aware state snapshots.
    pub connector_snapshots: Vec<ConnectorSnapshotOutput>,
    /// Enabled access paths paired with health snapshots.
    pub access_paths: Vec<RuntimeAccessPathSnapshotOutput>,
    /// SSH connection sessions as JSON.
    pub connection_sessions: Vec<Value>,
    /// Host-level write coordination state for this conversation.
    pub write_lease: Value,
    /// Logical Workspace capacity, independent from per-access-path SSH channel capacity.
    pub workspace_capacity: WorkspaceCapacityOutput,
    /// Workspace, PTY, and recent operation snapshots.
    pub workspaces: Vec<RuntimeWorkspaceSnapshotOutput>,
    /// Runtime conditions that need agent attention.
    pub attention: Vec<RuntimeAttentionOutput>,
}

/// Access path and its optional health snapshot.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RuntimeAccessPathSnapshotOutput {
    /// Access path record as JSON.
    pub access_path: Value,
    /// Optional health snapshot as JSON.
    pub health: Option<Value>,
    /// Optional automatic authorized-key bootstrap state as JSON.
    pub authorized_key_bootstrap: Option<Value>,
    /// Latest persisted connector-local SSH transport runtime as JSON.
    pub transport_runtime: Option<Value>,
    /// Scheduler-visible SSH channel capacity and reservations.
    pub channel_capacity: RuntimeChannelCapacityOutput,
}

/// Scheduler-visible SSH channel capacity for one access path.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RuntimeChannelCapacityOutput {
    /// Configured maximum channels for the pooled SSH transport.
    pub configured_limit: u32,
    /// Non-expired operation claims currently reserving channels.
    pub running_operations: u32,
    /// Persistent PTY backends currently holding channels.
    pub active_ptys: u32,
    /// Activatable PTYs waiting to reserve channels.
    pub pending_ptys: u32,
    /// Total active and pending scheduler reservations.
    pub reserved_channels: u32,
    /// Channels currently available to the scheduler.
    pub available_channels: u32,
    /// Capacity state: `available`, `saturated`, or `oversubscribed`.
    pub state: String,
}

impl RuntimeChannelCapacityOutput {
    fn new(configured_limit: u16, usage: remote_hosts_db::AccessPathChannelUsage) -> Self {
        let configured_limit = u32::from(configured_limit.max(1));
        let reserved_channels = usage
            .running_operations
            .saturating_add(usage.active_ptys)
            .saturating_add(usage.pending_ptys);
        let available_channels = configured_limit.saturating_sub(reserved_channels);
        let state = match reserved_channels.cmp(&configured_limit) {
            std::cmp::Ordering::Greater => "oversubscribed",
            std::cmp::Ordering::Equal => "saturated",
            std::cmp::Ordering::Less => "available",
        };
        Self {
            configured_limit,
            running_operations: usage.running_operations,
            active_ptys: usage.active_ptys,
            pending_ptys: usage.pending_ptys,
            reserved_channels,
            available_channels,
            state: state.to_owned(),
        }
    }
}

/// Workspace snapshot with bounded child runtime state.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RuntimeWorkspaceSnapshotOutput {
    /// Workspace record as JSON.
    pub workspace: Value,
    /// PTY session records as JSON.
    pub pty_sessions: Vec<Value>,
    /// Up to ten recent operation records as JSON.
    pub recent_operations: Vec<Value>,
}

/// Actionable runtime condition highlighted for an agent.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RuntimeAttentionOutput {
    /// Stable machine-readable condition code.
    pub code: String,
    /// Entity category.
    pub entity_type: String,
    /// Entity identifier.
    pub entity_id: String,
    /// Human-readable condition summary.
    pub message: String,
    /// Stable recommended action.
    pub recommended_action: String,
}

/// Connector snapshot output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ConnectorSnapshotOutput {
    /// Connector record as JSON.
    pub connector: Value,
    /// State snapshot as JSON.
    pub snapshot: Value,
}

impl ConnectorSnapshotOutput {
    fn state_snapshot(&self) -> Option<StateSnapshot> {
        serde_json::from_value(self.snapshot.clone()).ok()
    }

    fn snapshot_state(&self) -> Option<EntityState> {
        self.snapshot
            .get("state")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
    }
}

/// Connector heartbeat tool output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ConnectorHeartbeatOutput {
    /// Updated connector record as JSON.
    pub connector: Value,
    /// Connector state snapshot as JSON.
    pub snapshot: Value,
    /// Optional state transition event as JSON.
    pub event: Option<Value>,
}

/// State events output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct StateEventsOutput {
    /// Number of returned events.
    pub count: usize,
    /// State events as JSON.
    pub events: Vec<Value>,
}

/// Cursor-aware runtime event wait output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RuntimeEventsOutput {
    /// Cursor from which this wait began.
    pub start_cursor: u64,
    /// Cursor to pass to the next `after_cursor` wait.
    pub next_cursor: u64,
    /// Whether the wait ended without a matching event.
    pub timed_out: bool,
    /// Number of returned events.
    pub count: usize,
    /// Sequenced state events as JSON.
    pub events: Vec<Value>,
}

/// Server protection output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ServerProtectionOutput {
    /// Default policy as JSON.
    pub policy: Value,
    /// Number of currently registered MCP tools.
    pub registered_tool_count: usize,
}

/// Command profiles output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CommandProfilesOutput {
    /// Number of returned profiles.
    pub count: usize,
    /// Built-in profile descriptions as JSON.
    pub profiles: Vec<Value>,
    /// Number of currently registered MCP tools.
    pub registered_tool_count: usize,
}

/// Workspaces output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkspacesOutput {
    /// Number of returned workspaces.
    pub count: usize,
    /// Workspace records as JSON.
    pub workspaces: Vec<Value>,
}

/// Single workspace output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkspaceOutput {
    /// Workspace record as JSON.
    pub workspace: Value,
}

/// Transparent logical Workspace capacity and stale-state reconciliation counters.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
pub struct WorkspaceCapacityOutput {
    /// Maximum idle or working Workspace records allowed for one host.
    pub limit: u32,
    /// Idle or working Workspace rows recorded after the latest reconciliation.
    pub recorded_active: u32,
    /// Recorded rows that still count after excluding safely reapable history.
    pub effective_active: u32,
    /// Expired rows currently safe to close.
    pub expired_reapable: u32,
    /// Expired rows automatically closed during this operation.
    pub expired_reaped: u64,
    /// Effective active rows after Workspace preparation.
    pub active_after_prepare: u32,
    /// Effective active rows owned by this Agent Session.
    pub current_agent_session_active: u32,
    /// Effective active rows owned by other or legacy Agent Sessions.
    pub other_agent_sessions_active: u32,
}

impl WorkspaceCapacityOutput {
    fn new(status: WorkspaceCapacityStatus, expired_reaped: u64) -> Self {
        Self {
            limit: ServerProtectionPolicy::default().max_active_workspaces_per_host,
            recorded_active: status.recorded_active,
            effective_active: status.effective_active,
            expired_reapable: status.expired_reapable,
            expired_reaped,
            active_after_prepare: status.effective_active,
            current_agent_session_active: status.current_agent_session_active,
            other_agent_sessions_active: status.other_agent_sessions_active,
        }
    }
}

/// Prepared workspace and its task-oriented execution context.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PreparedWorkspaceOutput {
    /// Whether an existing idle or working workspace was reused.
    pub reused: bool,
    /// Stable next action for normal agent execution.
    pub next_action: String,
    /// Current MCP client-session identity and isolation scope.
    pub agent_session: Value,
    /// Selected workspace record as JSON.
    pub workspace: Value,
    /// Logical Workspace capacity and automatic stale-state recovery performed for this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_capacity: Option<WorkspaceCapacityOutput>,
    /// Fresh host runtime snapshot captured after workspace preparation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_snapshot: Option<HostRuntimeSnapshotOutput>,
    /// Number of available structured command profiles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_profile_count: Option<usize>,
    /// Available structured command profiles as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_profiles: Option<Vec<Value>>,
}

/// PTY sessions output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PtySessionsOutput {
    /// Number of returned sessions.
    pub count: usize,
    /// PTY session records as JSON.
    pub pty_sessions: Vec<Value>,
}

/// Single PTY session output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PtySessionOutput {
    /// PTY session record as JSON.
    pub pty_session: Value,
    /// Whether the connector has activated a backend with known capabilities.
    pub backend_ready: bool,
    /// Stable next action for the agent.
    pub recommended_action: String,
    /// Suggested delay before polling when activation or output is still pending.
    pub poll_after_ms: Option<u64>,
}

fn pty_session_output(pty_session: &PtySession) -> Result<PtySessionOutput, String> {
    let (backend_ready, recommended_action, poll_after_ms) = match pty_session.backend_state {
        PtyBackendState::Pending | PtyBackendState::Unknown => {
            (false, "wait_for_pty_activation", Some(750))
        }
        PtyBackendState::Active => (true, "read_pty_output", None),
        PtyBackendState::Failed => (false, "inspect_runtime_snapshot", None),
        PtyBackendState::Closed => (false, "none", None),
    };
    Ok(PtySessionOutput {
        pty_session: to_json_value(pty_session)?,
        backend_ready,
        recommended_action: recommended_action.to_owned(),
        poll_after_ms,
    })
}

/// PTY output chunks.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PtyOutputChunksOutput {
    /// PTY session record as JSON.
    pub pty_session: Value,
    /// Number of returned chunks.
    pub count: usize,
    /// Redacted PTY output chunks as JSON.
    pub chunks: Vec<Value>,
}

/// PTY input event output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PtyInputEventOutput {
    /// Public PTY input event metadata as JSON.
    pub input_event: Value,
    /// Whether an earlier event was returned for the same retry key.
    pub idempotency_reused: bool,
}

/// PTY input events output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PtyInputEventsOutput {
    /// PTY session record as JSON.
    pub pty_session: Value,
    /// Number of returned input events.
    pub count: usize,
    /// Public PTY input event metadata records as JSON.
    pub input_events: Vec<Value>,
}

/// Queued operation output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct QueuedOperationOutput {
    /// Stable next action.
    pub next_action: String,
    /// Suggested poll delay for non-terminal work.
    pub retry_after_ms: Option<u64>,
    /// Queued operation record as JSON.
    pub operation: Value,
    /// Updated workspace record as JSON.
    pub workspace: Value,
    /// Initial system output chunk as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_output_chunk: Option<Value>,
    /// Protection decision as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection_decision: Option<Value>,
    /// Whether an earlier operation was returned for the same retry key.
    pub idempotency_reused: bool,
    /// Exact-operation completion observed during an optional atomic submit-and-wait.
    pub completion: Option<OperationCompletionOutput>,
}

/// Bounded completion observation for one exact operation.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OperationCompletionOutput {
    /// Whether the exact operation reached a terminal state during the wait.
    pub completed: bool,
    /// Latest operation state.
    pub state: String,
    /// Exit code when available.
    pub exit_code: Option<i32>,
    /// Redacted result or error summary.
    pub summary: Option<String>,
    /// Stable next action.
    pub next_action: String,
    /// Latest operation record as JSON for admin and full profiles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<Value>,
    /// Latest workspace record as JSON for admin and full profiles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<Value>,
    /// Elapsed wait time in milliseconds.
    pub elapsed_ms: u64,
    /// Suggested next poll delay when the operation remains non-terminal.
    pub retry_after_ms: Option<u64>,
}

/// Workspace output chunks.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkspaceOutputChunksOutput {
    /// Workspace record as JSON.
    pub workspace: Value,
    /// Number of returned chunks.
    pub count: usize,
    /// Output chunks as JSON.
    pub chunks: Vec<Value>,
    /// Recent workspace operations as JSON.
    pub recent_operations: Vec<Value>,
}

/// Combined bounded result for one workspace or operation.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkspaceResultOutput {
    /// Workspace record as JSON.
    pub workspace: Value,
    /// Number of returned redacted output chunks.
    pub chunk_count: usize,
    /// Redacted output chunks as JSON.
    pub chunks: Vec<Value>,
    /// Recent workspace operations as JSON.
    pub recent_operations: Vec<Value>,
    /// Number of returned large-output artifacts.
    pub artifact_count: usize,
    /// File-backed output artifact metadata and previews as JSON.
    pub artifacts: Vec<Value>,
}

/// Workspace output artifacts.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WorkspaceOutputArtifactsOutput {
    /// Workspace record as JSON.
    pub workspace: Value,
    /// Number of returned artifacts.
    pub count: usize,
    /// File-backed output artifact records as JSON.
    pub artifacts: Vec<Value>,
}

/// Single output artifact metadata.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OutputArtifactOutput {
    /// File-backed output artifact record as JSON.
    pub artifact: Value,
}

/// Bounded UTF-8 content from one redacted output artifact.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OutputArtifactContentOutput {
    /// Output artifact id.
    pub artifact_id: String,
    /// Byte offset used for this chunk.
    pub offset: u64,
    /// Next byte offset to request.
    pub next_offset: u64,
    /// Bytes returned in this chunk.
    pub bytes_read: usize,
    /// Whether the complete artifact has been read.
    pub eof: bool,
    /// SHA-256 digest of the complete redacted artifact.
    pub sha256: String,
    /// Redacted UTF-8 content chunk.
    pub content: String,
}

/// Wait workspace state output.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WaitWorkspaceStateOutput {
    /// Whether the workspace reached a desired state before timeout.
    pub matched: bool,
    /// Latest workspace record as JSON.
    pub workspace: Value,
    /// Desired states as JSON values.
    pub desired_states: Vec<Value>,
    /// Elapsed wait time in milliseconds.
    pub elapsed_ms: u64,
    /// Suggested retry delay if not matched.
    pub retry_after_ms: Option<u64>,
}

fn required_trimmed(input: &str, field: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(trimmed.to_owned())
}

fn normalize_idempotency_key(input: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = trim_optional(input) else {
        return Ok(None);
    };
    if value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._:/".contains(&byte))
    {
        return Err(
            "idempotency_key must be 1..=128 ASCII letters, digits, or `-._:/` characters"
                .to_owned(),
        );
    }
    Ok(Some(value))
}

fn ensure_matching_pty_idempotent_request(
    existing: &PtyInputEvent,
    requested: &PtyInputEvent,
    idempotency_key: &str,
) -> Result<(), String> {
    if existing.pty_session_id != requested.pty_session_id
        || existing.payload_kind != requested.payload_kind
        || existing.input_fingerprint != requested.input_fingerprint
        || existing.requested_by != requested.requested_by
    {
        return Err(format!(
            "idempotency_key `{idempotency_key}` is already bound to a different PTY request in this conversation"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn file_transfer_spec(
    direction: SftpDirection,
    local_path: &str,
    remote_path: &str,
    overwrite: Option<&str>,
    mode: Option<&str>,
    max_size_bytes: Option<u64>,
    expected_sha256: Option<String>,
    timeout_seconds: Option<u64>,
) -> Result<FileTransferSpec, String> {
    let overwrite = match overwrite.map_or("deny", str::trim) {
        "deny" => SftpOverwritePolicy::Deny,
        "replace" => SftpOverwritePolicy::Replace,
        _ => return Err("overwrite must be `deny` or `replace`".to_owned()),
    };
    let mode = mode.map(parse_octal_mode).transpose()?;
    let expected_sha256 = expected_sha256
        .map(|digest| digest.trim().to_ascii_lowercase())
        .filter(|digest| !digest.is_empty());
    let spec = FileTransferSpec {
        direction,
        local_path: required_trimmed(local_path, "local_path")?,
        remote_path: required_trimmed(remote_path, "remote_path")?,
        overwrite,
        mode,
        max_size_bytes: max_size_bytes.unwrap_or(DEFAULT_SFTP_MAX_SIZE_BYTES),
        expected_sha256,
        timeout_seconds: timeout_seconds.unwrap_or(DEFAULT_SFTP_TIMEOUT_SECONDS),
    };
    spec.validate().map_err(|error| error.to_string())?;
    Ok(spec)
}

fn parse_octal_mode(value: &str) -> Result<u32, String> {
    let value = value.trim();
    let digits = value.strip_prefix("0o").unwrap_or(value);
    if !(3..=4).contains(&digits.len()) || !digits.bytes().all(|byte| matches!(byte, b'0'..=b'7')) {
        return Err(
            "mode must be a 3- or 4-digit octal value such as `600`, `0600`, or `0755`".to_owned(),
        );
    }
    u32::from_str_radix(digits, 8).map_err(|error| format!("parse mode: {error}"))
}

async fn read_artifact_utf8_chunk(
    root: &Path,
    relative_path: &str,
    expected_byte_len: u64,
    offset: u64,
    max_bytes: usize,
) -> Result<String, String> {
    if relative_path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err("artifact path is invalid".to_owned());
    }
    let canonical_root = tokio::fs::canonicalize(root)
        .await
        .map_err(|error| format!("canonicalize artifact root: {error}"))?;
    let canonical_path = tokio::fs::canonicalize(root.join(relative_path))
        .await
        .map_err(|error| format!("canonicalize artifact path: {error}"))?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err("artifact path escapes the configured artifact root".to_owned());
    }
    let metadata = tokio::fs::metadata(&canonical_path)
        .await
        .map_err(|error| format!("read artifact metadata: {error}"))?;
    if !metadata.is_file() || metadata.len() != expected_byte_len {
        return Err(format!(
            "artifact file metadata does not match its database record: expected_bytes={expected_byte_len}, actual_bytes={}",
            metadata.len()
        ));
    }
    if offset == expected_byte_len {
        return Ok(String::new());
    }

    let mut file = tokio::fs::File::open(&canonical_path)
        .await
        .map_err(|error| format!("open artifact: {error}"))?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|error| format!("seek artifact: {error}"))?;
    let read_capacity = max_bytes.saturating_add(4);
    let mut bytes = vec![0_u8; read_capacity];
    let read = file
        .read(&mut bytes)
        .await
        .map_err(|error| format!("read artifact: {error}"))?;
    let candidate = read.min(max_bytes);
    let used = match std::str::from_utf8(&bytes[..candidate]) {
        Ok(_) => candidate,
        Err(error) if error.error_len().is_none() => error.valid_up_to(),
        Err(_) => {
            return Err(
                "artifact content is not valid UTF-8 at the requested offset; use a previous next_offset"
                    .to_owned(),
            );
        }
    };
    if used == 0 && read > 0 {
        return Err(
            "artifact chunk is too small for the next UTF-8 character; increase max_bytes"
                .to_owned(),
        );
    }
    String::from_utf8(bytes[..used].to_vec())
        .map_err(|error| format!("decode redacted artifact content: {error}"))
}

const fn default_true() -> bool {
    true
}

fn is_openssh_agent_reference(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("external_reference")
        && value.get("external_ref").and_then(Value::as_str) == Some("openssh-agent")
}

fn credential_kind_for_secret(host_kind: &HostKind, secret: &CredentialSecret) -> CredentialKind {
    if matches!(host_kind, HostKind::Windows) && secret.password.is_some() {
        CredentialKind::WindowsPassword
    } else if secret.private_key_pem.is_some() && secret.private_key_passphrase.is_some() {
        CredentialKind::SshPrivateKeyWithPassphrase
    } else if secret.private_key_pem.is_some() {
        CredentialKind::SshPrivateKey
    } else if secret.password.is_some() {
        CredentialKind::SshPassword
    } else if secret.sudo_password.is_some() && !secret.use_ssh_agent {
        CredentialKind::SudoPassword
    } else {
        CredentialKind::SshPrivateKey
    }
}

fn host_credential_name(host: &Host, username: &str) -> String {
    let mut normalized_user = username
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while normalized_user.contains("--") {
        normalized_user = normalized_user.replace("--", "-");
    }
    let normalized_user = normalized_user.trim_matches('-');
    let normalized_user = if normalized_user.is_empty() {
        "user"
    } else {
        normalized_user
    };
    let normalized_user = normalized_user.chars().take(24).collect::<String>();
    let suffix = format!("-{normalized_user}-ssh");
    let host_budget = 64_usize.saturating_sub(suffix.len()).max(1);
    let host_name = host.name.chars().take(host_budget).collect::<String>();
    format!("{host_name}{suffix}")
}

fn trim_optional(input: Option<String>) -> Option<String> {
    input.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn normalize_slug(input: &str, field: &str) -> Result<String, String> {
    let value = required_trimmed(input, field)?.to_lowercase();
    if value.len() > 64 {
        return Err(format!("{field} must be at most 64 characters"));
    }
    if value.starts_with('-') || value.ends_with('-') {
        return Err(format!("{field} must not start or end with '-'"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(format!(
            "{field} must contain only lowercase letters, digits, and hyphens"
        ));
    }
    Ok(value)
}

fn normalize_candidate_name(input: &str) -> Result<String, String> {
    normalize_slug(input, "name")
}

fn normalize_tags(tags: Vec<String>) -> Result<Vec<String>, String> {
    let mut normalized = BTreeSet::new();
    for tag in tags {
        let tag = tag.trim().to_lowercase();
        if tag.is_empty() {
            continue;
        }
        if tag.len() > 64 {
            return Err("tag must be at most 64 characters".to_owned());
        }
        normalized.insert(tag);
    }
    Ok(normalized.into_iter().collect())
}

fn normalized_event_filter(value: Option<String>, field: &str) -> Result<Option<String>, String> {
    value
        .map(|value| {
            let value = required_trimmed(&value, field)?;
            if value.len() > 128 {
                return Err(format!("{field} must be at most 128 characters"));
            }
            Ok(value)
        })
        .transpose()
}

fn normalize_proxy_chain(proxy_chain: Vec<String>) -> Result<Vec<String>, String> {
    proxy_chain
        .into_iter()
        .map(|entry| required_trimmed(&entry, "proxy_chain entry"))
        .collect()
}

fn prepare_ensure_access(request: EnsureHostAccessRequest) -> Result<PreparedEnsureAccess, String> {
    let port = request.port.unwrap_or(22);
    if port == 0 {
        return Err("access.port must be greater than 0".to_owned());
    }
    let address = required_trimmed(&request.address, "access.address")?.to_lowercase();
    let username = required_trimmed(&request.username, "access.username")?;
    let environment_name = normalize_slug(&request.environment_name, "access.environment_name")?;
    let credential_name = request
        .credential_name
        .map(|name| normalize_slug(&name, "access.credential_name"))
        .transpose()?;

    Ok(PreparedEnsureAccess {
        address,
        port,
        username,
        environment_name,
        environment_kind: parse_environment_kind(&request.environment_kind)?,
        trust_level: parse_trust_level(&request.trust_level)?,
        route_type: parse_route_type(&request.route_type)?,
        connector_id: request
            .connector_id
            .as_deref()
            .map(parse_connector_id)
            .transpose()?,
        credential_name,
        credential_kind: request
            .credential_kind
            .as_deref()
            .map(parse_credential_kind)
            .transpose()?,
        credential_secret: request.credential_secret,
        proxy_chain: normalize_proxy_chain(request.proxy_chain.unwrap_or_default())?,
        priority: request.priority,
        enabled: request.enabled,
        connection_mode: parse_connection_mode(request.connection_mode.as_deref())?,
        idle_ttl_seconds: request.idle_ttl_seconds,
        keepalive_seconds: request.keepalive_seconds,
        max_concurrent_channels: request.max_concurrent_channels,
        max_new_connections_per_minute: request.max_new_connections_per_minute,
        requires_tty: request.requires_tty,
        notes: trim_optional(request.notes),
    })
}

fn new_openssh_agent_credential(
    name: String,
    requested_kind: Option<CredentialKind>,
    username: &str,
    now: time::OffsetDateTime,
) -> Result<StoredCredential, String> {
    let kind = requested_kind.unwrap_or(CredentialKind::SshPrivateKey);
    if !matches!(
        kind,
        CredentialKind::SshPrivateKey | CredentialKind::SshPrivateKeyWithPassphrase
    ) {
        return Err(
            "ensure_host can create only an OpenSSH-agent key reference; create password credentials through the internal vault first"
                .to_owned(),
        );
    }
    Ok(StoredCredential {
        metadata: CredentialMetadata {
            id: CredentialId::new(),
            name,
            kind,
            username_hint: Some(username.to_owned()),
            created_at: now,
            updated_at: now,
            last_used_at: None,
        },
        encrypted_blob_json: json!({
            "type": "external_reference",
            "external_ref": "openssh-agent",
            "notes": "Created by task-level host registration"
        }),
    })
}

fn connector_is_available(connector: &Connector) -> bool {
    let heartbeat_is_fresh = connector
        .last_seen_at
        .is_some_and(|last_seen| last_seen >= now_utc() - time::Duration::seconds(90));
    heartbeat_is_fresh
        && !matches!(
            connector.state,
            EntityState::ConnectorOffline | EntityState::CircuitOpen | EntityState::Maintenance
        )
}

fn ensure_no_secret_like_json(value: &Value, field: &str) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                ensure_not_secret_key(key, field)?;
                ensure_no_secret_like_json(value, field)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                ensure_no_secret_like_json(item, field)?;
            }
        }
        Value::String(text) => ensure_no_secret_like_text(text, field)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn ensure_not_secret_key(key: &str, field: &str) -> Result<(), String> {
    let lower = key.to_ascii_lowercase();
    let secret_key = [
        "password",
        "passwd",
        "private_key",
        "token",
        "secret",
        "sudo",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if secret_key {
        return Err(format!(
            "{field} contains a secret-like key; store only non-secret metadata"
        ));
    }
    Ok(())
}

fn ensure_no_secret_like_text(text: &str, field: &str) -> Result<(), String> {
    let lower = text.to_ascii_lowercase();
    let secret_text = [
        "-----begin ",
        "private key",
        "password=",
        "passwd=",
        "api_token=",
        "token=",
        "secret=",
        "sudo_password=",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if secret_text {
        return Err(format!(
            "{field} appears to contain secret material; store only redacted metadata"
        ));
    }
    Ok(())
}

fn parse_host_kind(input: &str) -> Result<HostKind, String> {
    match input.trim() {
        "macos" => Ok(HostKind::Macos),
        "windows" => Ok(HostKind::Windows),
        "linux" => Ok(HostKind::Linux),
        "gpu_server" => Ok(HostKind::GpuServer),
        "jump_host" => Ok(HostKind::JumpHost),
        "customer_server" => Ok(HostKind::CustomerServer),
        other if other.starts_with("other:") => {
            let value = required_trimmed(other.trim_start_matches("other:"), "host kind")?;
            Ok(HostKind::Other(value))
        }
        other => Err(format!("invalid host kind `{other}`")),
    }
}

fn parse_risk_level(input: &str) -> Result<RiskLevel, String> {
    parse_string_enum(input, "risk_level")
}

fn parse_environment_kind(input: &str) -> Result<EnvironmentKind, String> {
    parse_string_enum(input, "environment kind")
}

fn parse_trust_level(input: &str) -> Result<TrustLevel, String> {
    parse_string_enum(input, "trust_level")
}

fn parse_credential_kind(input: &str) -> Result<CredentialKind, String> {
    parse_string_enum(input, "credential kind")
}

fn parse_route_type(input: &str) -> Result<RouteType, String> {
    parse_string_enum(input, "route_type")
}

fn parse_connection_mode(input: Option<&str>) -> Result<ConnectionMode, String> {
    input.map_or(Ok(ConnectionMode::Pooled), |value| {
        parse_string_enum(value, "connection_mode")
    })
}

fn parse_fact_source(input: &str) -> Result<FactSource, String> {
    parse_string_enum(input, "source")
}

fn parse_instance_sync_collections(
    collections: Option<Vec<String>>,
) -> Result<Vec<InstanceSyncCollection>, String> {
    let collections = collections.unwrap_or_else(|| {
        vec![
            "inventory".to_owned(),
            "knowledge".to_owned(),
            "credentials".to_owned(),
        ]
    });
    let mut parsed = BTreeSet::new();
    for collection in collections {
        let collection = collection.trim().to_ascii_lowercase();
        let parsed_collection = match collection.as_str() {
            "inventory" => InstanceSyncCollection::Inventory,
            "knowledge" => InstanceSyncCollection::Knowledge,
            "credentials" => InstanceSyncCollection::Credentials,
            "topology" => InstanceSyncCollection::Topology,
            "artifacts" => InstanceSyncCollection::Artifacts,
            _ => {
                return Err(format!(
                    "unknown instance sync collection `{collection}`; use inventory, knowledge, or credentials"
                ));
            }
        };
        parsed.insert(parsed_collection);
    }
    if parsed.is_empty() {
        return Err("at least one instance sync collection is required".to_owned());
    }
    Ok(parsed.into_iter().collect())
}

fn parse_string_enum<T>(input: &str, field: &str) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(Value::String(input.trim().to_owned()))
        .map_err(|error| format!("invalid {field} `{input}`: {error}"))
}

fn duplicate_confidence(signals: &[String]) -> f32 {
    if signals.iter().any(|signal| signal == "name")
        || (signals.iter().any(|signal| signal == "display_name")
            && signals.iter().any(|signal| signal == "access_path"))
    {
        0.95
    } else if signals.iter().any(|signal| signal == "access_path") {
        0.85
    } else {
        0.65
    }
}

fn ensure_exists<F>(exists: bool, message: F) -> Result<(), String>
where
    F: FnOnce() -> String,
{
    if exists { Ok(()) } else { Err(message()) }
}

fn parse_host_id(input: &str) -> Result<HostId, String> {
    Ok(HostId::from(parse_uuid(input, "host_id")?))
}

fn parse_environment_id(input: &str) -> Result<EnvironmentId, String> {
    Ok(EnvironmentId::from(parse_uuid(input, "environment_id")?))
}

fn parse_connector_id(input: &str) -> Result<ConnectorId, String> {
    Ok(ConnectorId::from(parse_uuid(input, "connector_id")?))
}

fn parse_credential_id(input: &str) -> Result<CredentialId, String> {
    Ok(CredentialId::from(parse_uuid(input, "credential_id")?))
}

fn parse_workspace_id(input: &str) -> Result<WorkspaceId, String> {
    Ok(WorkspaceId::from(parse_uuid(input, "workspace_id")?))
}

fn parse_session_id(input: &str) -> Result<SessionId, String> {
    Ok(SessionId::from(parse_uuid(input, "session_id")?))
}

fn parse_pty_session_id(input: &str) -> Result<PtySessionId, String> {
    Ok(PtySessionId::from(parse_uuid(input, "pty_session_id")?))
}

fn parse_operation_id(input: &str) -> Result<OperationId, String> {
    Ok(OperationId::from(parse_uuid(input, "operation_id")?))
}

fn parse_output_artifact_id(input: &str) -> Result<OperationOutputArtifactId, String> {
    Ok(OperationOutputArtifactId::from(parse_uuid(
        input,
        "artifact_id",
    )?))
}

fn parse_access_path_id(input: &str) -> Result<remote_hosts_domain::AccessPathId, String> {
    Ok(remote_hosts_domain::AccessPathId::from(parse_uuid(
        input,
        "access_path_id",
    )?))
}

fn parse_host_ids(inputs: Vec<String>) -> Result<Vec<HostId>, String> {
    inputs
        .into_iter()
        .map(|input| parse_host_id(&input))
        .collect()
}

fn parse_access_path_ids(inputs: Vec<String>) -> Result<Vec<AccessPathId>, String> {
    inputs
        .into_iter()
        .map(|input| parse_access_path_id(&input))
        .collect()
}

fn parse_software_install_ids(inputs: Vec<String>) -> Result<Vec<SoftwareInstallId>, String> {
    inputs
        .into_iter()
        .map(|input| Ok(SoftwareInstallId::from(parse_uuid(&input, "software_id")?)))
        .collect()
}

fn parse_operation_ids(inputs: Vec<String>) -> Result<Vec<OperationId>, String> {
    inputs
        .into_iter()
        .map(|input| parse_operation_id(&input))
        .collect()
}

fn parse_uuid(input: &str, field: &str) -> Result<Uuid, String> {
    Uuid::parse_str(input).map_err(|error| format!("invalid {field}: {error}"))
}

fn parse_entity_state(input: &str) -> Result<EntityState, String> {
    serde_json::from_value(Value::String(input.to_owned()))
        .map_err(|error| format!("invalid connector state `{input}`: {error}"))
}

fn parse_workspace_state(input: &str) -> Result<WorkspaceState, String> {
    serde_json::from_value(Value::String(input.to_owned()))
        .map_err(|error| format!("invalid workspace state `{input}`: {error}"))
}

fn parse_desired_workspace_states(
    states: Option<Vec<String>>,
) -> Result<Vec<WorkspaceState>, String> {
    let states = states.unwrap_or_else(|| {
        ["idle", "done", "failed", "throttled", "blocked", "closed"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    });
    states
        .into_iter()
        .map(|state| parse_workspace_state(&state))
        .collect()
}

fn workspace_states_to_values(states: &[WorkspaceState]) -> Result<Vec<Value>, String> {
    states.iter().map(to_json_value).collect()
}

fn elapsed_ms(started_at: std::time::Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn is_terminal_operation_state(state: &OperationState) -> bool {
    matches!(
        state,
        OperationState::Succeeded
            | OperationState::Failed
            | OperationState::TimedOut
            | OperationState::Cancelled
            | OperationState::Rejected
            | OperationState::Exhausted
    )
}

fn operation_state_name(state: &OperationState) -> &'static str {
    match state {
        OperationState::Queued => "queued",
        OperationState::Running => "running",
        OperationState::Succeeded => "succeeded",
        OperationState::Failed => "failed",
        OperationState::TimedOut => "timed_out",
        OperationState::Cancelled => "cancelled",
        OperationState::Rejected => "rejected",
        OperationState::Exhausted => "exhausted",
    }
}

fn retain_runtime_connection_sessions(
    sessions: &mut Vec<ConnectionSession>,
    access_paths: &[AccessPath],
) {
    let enabled_paths: BTreeSet<_> = access_paths.iter().map(|path| path.id).collect();
    let mut latest_paths = BTreeSet::new();
    let mut reusable_paths = BTreeSet::new();
    sessions.retain(|session| {
        if !enabled_paths.contains(&session.access_path_id) {
            return false;
        }
        let latest_for_path = latest_paths.insert(session.access_path_id);
        let reusable_for_path =
            matches!(session.state, EntityState::Connected | EntityState::Healthy)
                && reusable_paths.insert(session.access_path_id);
        latest_for_path
            || reusable_for_path
            || session.open_channels > 0
            || session.state == EntityState::Resolving
    });
}

fn values<T: Serialize>(items: &[T]) -> Result<Vec<Value>, String> {
    items.iter().map(to_json_value).collect()
}

fn public_operation_values(items: &[OperationRun]) -> Result<Vec<Value>, String> {
    items.iter().map(public_operation_value).collect()
}

fn compact_agent_session_value(session: &AgentSession) -> Value {
    json!({
        "id": session.id,
        "client_kind": session.client_kind,
        "project_key": session.project_key,
        "conversation_key": session.conversation_key
    })
}

fn compact_workspace_value(workspace: &AgentWorkspace) -> Value {
    json!({
        "id": workspace.id,
        "host_id": workspace.host_id,
        "access_path_id": workspace.access_path_id,
        "state": workspace.state,
        "label": workspace.label,
        "cwd": workspace.cwd,
        "coordination_scope": workspace.coordination_scope
    })
}

fn compact_workspace_status_value(workspace: &AgentWorkspace) -> Value {
    json!({
        "id": workspace.id,
        "state": workspace.state
    })
}

fn compact_pty_session_value(session: &PtySession) -> Value {
    json!({
        "pty_session_id": session.pty_session_id,
        "workspace_id": session.workspace_id,
        "state": session.state,
        "backend_state": session.backend_state,
        "coordination_scopes": session.coordination_scopes,
        "foreground_process": session.foreground_process,
        "input_allowed": session.input_allowed,
        "interaction": session.interaction,
        "last_exit_code": session.last_exit_code
    })
}

fn compact_pty_chunk_value(chunk: &remote_hosts_domain::PtyOutputChunk) -> Value {
    json!({
        "sequence": chunk.sequence,
        "stream": chunk.stream,
        "text": chunk.redacted_text,
        "truncated": chunk.truncated
    })
}

fn compact_operation_chunk_value(chunk: &remote_hosts_domain::OperationOutputChunk) -> Value {
    json!({
        "sequence": chunk.sequence,
        "stream": chunk.stream,
        "text": chunk.redacted_text,
        "truncated": chunk.truncated
    })
}

fn compact_artifact_value(artifact: &remote_hosts_domain::OperationOutputArtifact) -> Value {
    json!({
        "id": artifact.id,
        "operation_id": artifact.operation_id,
        "stream": artifact.stream,
        "byte_len": artifact.byte_len,
        "sha256": artifact.sha256,
        "preview": artifact.redacted_preview,
        "truncated": artifact.truncated
    })
}

fn compact_operation_value(operation: &OperationRun) -> Value {
    json!({
        "id": operation.id,
        "workspace_id": operation.workspace_id,
        "state": operation.state,
        "exit_code": operation.exit_code,
        "intent": operation.intent,
        "requires_write_lease": operation.requires_write_lease,
        "coordination_scope": operation.coordination_scope,
        "coordination_scopes": operation_coordination_scopes(operation),
        "command_preview": operation.redacted_command_summary,
        "summary": operation_result_summary(operation),
        "last_error": operation.last_error,
        "started_at": operation.started_at,
        "finished_at": operation.finished_at
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

fn operation_next_action(state: &OperationState) -> &'static str {
    if is_terminal_operation_state(state) {
        "none"
    } else {
        "get_workspace_result"
    }
}

fn operation_retry_after_ms(state: &OperationState) -> Option<u64> {
    (!is_terminal_operation_state(state)).then_some(250)
}

fn operation_result_summary(operation: &OperationRun) -> Option<String> {
    operation
        .last_error
        .clone()
        .or_else(|| operation.redacted_output_summary.clone())
}

fn write_lease_snapshot_value(
    leases: &[HostWriteLease],
    current_agent_session_id: AgentSessionId,
    disclose_holder: bool,
    observed_at: time::OffsetDateTime,
) -> Value {
    if leases.is_empty() {
        return json!({
            "state": "available",
            "retry_after_seconds": null,
            "expires_at": null,
            "active_leases": []
        });
    }
    let mut has_current = false;
    let mut has_other = false;
    let mut retry_after_seconds: Option<u64> = None;
    let mut latest_expiry = observed_at;
    let active_leases = leases
        .iter()
        .map(|lease| {
            let held_by_current = lease.holder_agent_session_id == current_agent_session_id;
            has_current |= held_by_current;
            has_other |= !held_by_current;
            latest_expiry = latest_expiry.max(lease.expires_at);
            let retry = u64::try_from((lease.expires_at - observed_at).whole_seconds().max(0))
                .unwrap_or(u64::MAX);
            if !held_by_current {
                retry_after_seconds = Some(retry_after_seconds.map_or(retry, |old| old.min(retry)));
            }
            let mut value = json!({
                "coordination_scope": lease.coordination_scope,
                "state": if held_by_current {
                    "held_by_current_session"
                } else {
                    "held_by_other_session"
                },
                "retry_after_seconds": if held_by_current { Value::Null } else { json!(retry) },
                "acquired_at": lease.acquired_at,
                "heartbeat_at": lease.heartbeat_at,
                "expires_at": lease.expires_at
            });
            if (held_by_current || disclose_holder)
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "holder_agent_session_id".to_owned(),
                    json!(lease.holder_agent_session_id.to_string()),
                );
                object.insert(
                    "holder_workspace_id".to_owned(),
                    json!(lease.holder_workspace_id.to_string()),
                );
            }
            value
        })
        .collect::<Vec<_>>();
    json!({
        "state": match (has_current, has_other) {
            (true, true) => "mixed",
            (true, false) => "held_by_current_session",
            (false, true) => "held_by_other_session",
            (false, false) => "available",
        },
        "retry_after_seconds": retry_after_seconds,
        "expires_at": latest_expiry,
        "active_leases": active_leases
    })
}

fn coordination_scopes_overlap(left: &str, right: &str) -> bool {
    left == "host"
        || right == "host"
        || left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn public_operation_value(operation: &OperationRun) -> Result<Value, String> {
    let mut value = to_json_value(operation)?;
    let Some(profile) = value
        .get_mut("command_profile_json")
        .and_then(Value::as_object_mut)
    else {
        return Ok(value);
    };
    let shell_profile = profile
        .get("name")
        .and_then(Value::as_str)
        .is_some_and(|name| matches!(name, "shell.posix" | "shell.powershell"));
    if operation.operation_type == remote_hosts_domain::OperationType::Sftp {
        let local_file = profile
            .get("local_path")
            .and_then(Value::as_str)
            .and_then(|path| Path::new(path).file_name())
            .and_then(|name| name.to_str())
            .unwrap_or("<invalid>")
            .to_owned();
        let remote_file = profile
            .get("remote_path")
            .and_then(Value::as_str)
            .and_then(|path| path.rsplit('/').next())
            .unwrap_or("<invalid>")
            .to_owned();
        profile.remove("local_path");
        profile.remove("remote_path");
        profile.insert("local_file".to_owned(), json!(local_file));
        profile.insert("remote_file".to_owned(), json!(remote_file));
        profile.insert("paths_redacted".to_owned(), json!(true));
        return Ok(value);
    }
    if !shell_profile {
        return Ok(value);
    }
    let script_bytes = profile
        .get("args")
        .and_then(Value::as_array)
        .and_then(|args| args.last())
        .and_then(Value::as_str)
        .map_or(0, str::len);
    profile.insert(
        "args".to_owned(),
        json!([format!("<managed-script:{script_bytes}-bytes>")]),
    );
    profile.insert("script_redacted".to_owned(), json!(true));
    Ok(value)
}

fn optional_value<T: Serialize>(item: Option<&T>) -> Result<Option<Value>, String> {
    item.map(to_json_value).transpose()
}

fn to_json_value<T: Serialize>(item: &T) -> Result<Value, String> {
    serde_json::to_value(item).map_err(|error| format!("failed to serialize tool output: {error}"))
}

fn tool_error(error: &DbError) -> String {
    format!("database error: {error}")
}

fn workspace_capacity_error(
    reason: &str,
    capacity: &WorkspaceCapacityStatus,
    policy: &ServerProtectionPolicy,
) -> String {
    format!(
        "{reason}; logical Workspace capacity is independent from SSH channel capacity: limit={}, recorded_active={}, effective_active={}, expired_reapable={}, current_agent_session_active={}, other_agent_sessions_active={}; close an owned Workspace or wait for live operations/PTYs to finish",
        policy.max_active_workspaces_per_host,
        capacity.recorded_active,
        capacity.effective_active,
        capacity.expired_reapable,
        capacity.current_agent_session_active,
        capacity.other_agent_sessions_active,
    )
}

fn resolution_error(error: &AccessResolutionError) -> String {
    format!(
        "{}; state={:?}; reason={:?}; hint={:?}; retry_after_seconds={:?}",
        error.human_message,
        error.state,
        error.reason_code,
        error.agent_hint,
        error.retry_after_seconds
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use remote_hosts_db::{Repositories, connect_sqlite, migrate};
    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId,
        AgentSessionState, AgentWorkspace, AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason,
        AuthorizedKeyBootstrapState, ConnectionMode, ConnectionSession, Connector, ConnectorId,
        CredentialId, CredentialKind, CredentialMetadata, EntityState, Environment, EnvironmentId,
        EnvironmentKind, Host, HostId, HostKind, OperationId, OperationOutputArtifact,
        OperationOutputArtifactId, OperationRun, OperationState, OperationType, OutputStream,
        Protocol, PtyBackendCapabilities, PtyBackendState, PtyInteraction, PtyInteractionKind,
        PtyOutputChunk, PtyOutputChunkId, PtySession, PtySessionId, RiskLevel, RouteType,
        SessionId, SshFileTransferMode, SshTransportBackend, SshTransportCapabilities,
        SshTransportRuntime, SshTransportRuntimeId, SshTransportRuntimeState,
        SshTransportTelemetry, StateReasonCode, StoredCredential, TrustLevel, WorkspaceId,
        WorkspaceState, now_utc,
    };
    use remote_hosts_vault::{CredentialVault, EncryptedCredentialBlob};
    use rmcp::{
        ServiceExt,
        model::{CallToolRequestParams, ClientInfo},
    };
    use secrecy::{ExposeSecret, SecretString};
    use serde_json::{Value, json};

    use super::{RemoteHostsMcpServer, ToolProfile, tools};

    #[tokio::test]
    async fn mcp_tool_profiles_expose_bounded_task_oriented_surfaces()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let admin =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Admin);
        let full =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Full);
        let agent_names = agent
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<BTreeSet<_>>();
        let admin_names = admin
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<BTreeSet<_>>();

        assert_eq!(agent_names.len(), 21);
        assert!(agent_names.contains("remote_hosts_ensure_host"));
        assert!(agent_names.contains(tools::STORE_HOST_CREDENTIAL));
        assert!(agent_names.contains(tools::PREPARE_WORKSPACE));
        assert!(agent_names.contains(tools::GET_WORKSPACE_RESULT));
        assert!(agent_names.contains(tools::UPLOAD_FILE));
        assert!(agent_names.contains(tools::DOWNLOAD_FILE));
        assert!(agent_names.contains(tools::READ_OUTPUT_ARTIFACT_CONTENT));
        assert!(agent_names.contains(tools::HEARTBEAT_PTY_SESSION));
        assert!(agent_names.contains(tools::CONFIGURE_INSTANCE_SYNC_PEER));
        assert!(agent_names.contains(tools::SYNC_INSTANCE_PEER));
        assert!(!agent_names.contains(tools::UPSERT_HOST));
        assert!(admin_names.is_superset(&agent_names));
        assert!(admin_names.contains(tools::UPSERT_HOST));
        assert!(admin_names.contains(tools::FIND_HOST_DUPLICATES));
        assert!(admin_names.len() < full.tool_router.list_all().len());
        assert_eq!(full.tool_router.list_all().len(), 47);
        let Err(hidden_error) = call_tool_raw(agent, tools::UPSERT_HOST, None).await else {
            return Err("agent profile unexpectedly exposed an admin tool".into());
        };
        assert!(hidden_error.to_string().contains("tool not found"));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_agent_ensure_host_registers_and_reuses_one_canonical_machine()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let request = Some(json!({
            "name": "CUBEX",
            "display_name": "CUBEX",
            "kind": "windows",
            "risk_level": "development",
            "tags": ["windows", "builder"],
            "access": {
                "address": "hackerlife.fun",
                "port": 3333,
                "username": "liang",
                "environment_name": "public-internet",
                "environment_kind": "public_internet",
                "trust_level": "untrusted",
                "route_type": "public"
            }
        }));

        let created = call_tool(agent.clone(), "remote_hosts_ensure_host", request).await?;
        assert_eq!(created["host_created"], json!(true));
        assert_eq!(created["environment_created"], json!(true));
        assert_eq!(created["credential_created"], json!(true));
        assert_eq!(created["access_path_created"], json!(true));
        assert_eq!(created["host"]["name"], json!("cubex"));
        assert_eq!(created["access_path"]["address"], json!("hackerlife.fun"));
        assert_eq!(created["access_path"]["port"], json!(3333));
        assert_eq!(created["duplicate_signals"], json!([]));
        assert_eq!(
            created["defaults_applied"],
            json!(["credential:openssh-default", "connector:single-healthy"])
        );
        assert!(!created.to_string().contains("openssh-agent"));

        let host_id = created["host"]["id"]
            .as_str()
            .ok_or("host id should be a string")?;
        let access_path_id = created["access_path"]["id"]
            .as_str()
            .ok_or("access path id should be a string")?;
        let reused = call_tool(
            agent.clone(),
            "remote_hosts_ensure_host",
            Some(json!({
                "name": "windows-builder",
                "display_name": "CUBEX Build Machine",
                "kind": "windows",
                "risk_level": "development",
                "tags": ["release"],
                "access": {
                    "address": "HACKERLIFE.FUN",
                    "port": 3333,
                    "username": "Liang",
                    "environment_name": "public-internet",
                    "environment_kind": "public_internet",
                    "trust_level": "untrusted",
                    "route_type": "public"
                }
            })),
        )
        .await?;

        assert_eq!(reused["host_created"], json!(false));
        assert_eq!(reused["access_path_created"], json!(false));
        assert_eq!(reused["host"]["id"], json!(host_id));
        assert_eq!(reused["host"]["name"], json!("cubex"));
        assert_eq!(reused["access_path"]["id"], json!(access_path_id));
        assert_eq!(reused["duplicate_signals"], json!(["access_path"]));
        assert_eq!(
            reused["host"]["tags"],
            json!(["builder", "release", "windows"])
        );

        let ambiguous = call_tool_raw(
            agent.clone(),
            "remote_hosts_ensure_host",
            Some(json!({
                "name": "company-4090-mcp",
                "kind": "windows",
                "risk_level": "development",
                "access": {
                    "address": "hackerlife.fun",
                    "port": 3333,
                    "username": "liang",
                    "environment_name": "public-internet",
                    "environment_kind": "public_internet",
                    "trust_level": "untrusted",
                    "route_type": "public"
                }
            })),
        )
        .await?;
        assert_eq!(ambiguous.is_error, Some(true));

        let rejected_secret = call_tool_raw(
            agent.clone(),
            "remote_hosts_ensure_host",
            Some(json!({
                "name": "secret-test",
                "kind": "windows",
                "risk_level": "development",
                "access": {
                    "address": "example.invalid",
                    "username": "user",
                    "environment_name": "public-internet",
                    "environment_kind": "public_internet",
                    "trust_level": "untrusted",
                    "route_type": "public",
                    "password": "hunter2"
                }
            })),
        )
        .await?;
        assert_eq!(rejected_secret.is_error, Some(true));
        assert!(!format!("{rejected_secret:?}").contains("hunter2"));

        let hosts = call_tool(agent, tools::LIST_HOSTS, None).await?;
        assert_eq!(hosts["count"], json!(2));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_agent_ensure_host_prefers_exact_identity_for_shared_bastion_endpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        call_tool(
            agent.clone(),
            tools::ENSURE_HOST,
            Some(json!({
                "name": "shared-bastion",
                "display_name": "Shared Bastion",
                "kind": "jump_host",
                "risk_level": "customer_site",
                "access": {
                    "address": "10.36.31.20",
                    "username": "operator",
                    "environment_name": "customer-vpn",
                    "environment_kind": "vpn",
                    "trust_level": "trusted",
                    "route_type": "bastion",
                    "requires_tty": true
                }
            })),
        )
        .await?;
        let target = call_tool(
            agent.clone(),
            tools::ENSURE_HOST,
            Some(json!({
                "name": "shared-target",
                "display_name": "Shared Target",
                "kind": "linux",
                "risk_level": "customer_site"
            })),
        )
        .await?;
        let target_id = target["host"]["id"]
            .as_str()
            .ok_or("target host id should be a string")?;

        let routed_target = call_tool(
            agent,
            tools::ENSURE_HOST,
            Some(json!({
                "name": "shared-target",
                "display_name": "Shared Target",
                "kind": "linux",
                "risk_level": "customer_site",
                "access": {
                    "address": "10.36.31.20",
                    "username": "operator",
                    "environment_name": "customer-vpn",
                    "environment_kind": "vpn",
                    "trust_level": "trusted",
                    "route_type": "bastion",
                    "requires_tty": true
                }
            })),
        )
        .await?;

        assert_eq!(routed_target["host"]["id"], json!(target_id));
        assert_eq!(routed_target["access_path_created"], json!(true));
        assert_eq!(
            routed_target["duplicate_signals"],
            json!(["name", "display_name"])
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_agent_stores_and_updates_host_password_without_returning_plaintext()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let master = SecretString::from("test-local-vault-master".to_owned());
        let agent = RemoteHostsMcpServer::with_profile_and_vault(
            fixture.repositories.clone(),
            ToolProfile::Agent,
            Some(master.clone()),
        );
        let created = call_tool(
            agent.clone(),
            "remote_hosts_ensure_host",
            Some(json!({
                "name": "password-windows",
                "display_name": "Password Windows",
                "kind": "windows",
                "risk_level": "development",
                "access": {
                    "address": "windows.example.test",
                    "port": 2222,
                    "username": "builder",
                    "environment_name": "public-internet",
                    "environment_kind": "public_internet",
                    "trust_level": "untrusted",
                    "route_type": "public",
                    "credential_secret": {
                        "password": "initial-password",
                        "sudo_password": "initial-admin-password"
                    }
                }
            })),
        )
        .await?;
        assert_eq!(created["credential_status"], json!("created"));
        assert_eq!(created["credential"]["kind"], json!("windows_password"));
        assert!(!created.to_string().contains("initial-password"));
        assert!(!created.to_string().contains("initial-admin-password"));

        let credential_id = created["credential"]["id"]
            .as_str()
            .ok_or("credential id should be a string")?;
        let stored = fixture
            .repositories
            .credentials
            .get(CredentialId::from(uuid::Uuid::parse_str(credential_id)?))
            .await?
            .ok_or("stored credential should exist")?;
        let stored_json = stored.encrypted_blob_json.to_string();
        assert!(!stored_json.contains("initial-password"));
        assert!(!stored_json.contains("initial-admin-password"));
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)?;
        let decrypted = CredentialVault::decrypt(&master, &blob)?;
        assert_eq!(decrypted.password.as_deref(), Some("initial-password"));
        assert_eq!(
            decrypted.sudo_password.as_deref(),
            Some("initial-admin-password")
        );

        let access_path_id = AccessPathId::from(uuid::Uuid::parse_str(
            created["access_path"]["id"]
                .as_str()
                .ok_or("access path id should be a string")?,
        )?);
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id,
                state: EntityState::AuthFailed,
                last_checked_at: Some(now_utc()),
                latency_ms: None,
                failure_count: 1,
                last_error_code: Some(StateReasonCode::SshAuthFailed),
                next_retry_at: None,
            })
            .await?;

        let updated = call_tool(
            agent,
            "remote_hosts_store_host_credential",
            Some(json!({
                "host_id": created["host"]["id"],
                "password": "updated-password"
            })),
        )
        .await?;
        assert_eq!(updated["credential_status"], json!("updated"));
        assert_eq!(updated["stored_fields"], json!(["password"]));
        assert!(!updated.to_string().contains("updated-password"));
        let updated_blob: EncryptedCredentialBlob = serde_json::from_value(
            fixture
                .repositories
                .credentials
                .get(CredentialId::from(uuid::Uuid::parse_str(credential_id)?))
                .await?
                .ok_or("updated credential should exist")?
                .encrypted_blob_json,
        )?;
        let decrypted = CredentialVault::decrypt(&master, &updated_blob)?;
        assert_eq!(decrypted.password.as_deref(), Some("updated-password"));
        assert_eq!(
            decrypted.sudo_password.as_deref(),
            Some("initial-admin-password")
        );
        assert_ne!(decrypted.password.as_deref(), Some(master.expose_secret()));
        let health = fixture
            .repositories
            .access_path_health
            .get(access_path_id)
            .await?
            .ok_or("access path health should exist")?;
        assert_eq!(health.state, EntityState::Unknown);
        assert_eq!(health.failure_count, 0);
        assert!(health.last_error_code.is_none());
        assert!(health.next_retry_at.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mcp_agent_ensure_host_creates_named_password_credential_in_one_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let master = SecretString::from("test-local-vault-master".to_owned());
        let agent = RemoteHostsMcpServer::with_profile_and_vault(
            fixture.repositories.clone(),
            ToolProfile::Agent,
            Some(master.clone()),
        );

        let created = call_tool(
            agent,
            tools::ENSURE_HOST,
            Some(json!({
                "name": "single-call-password-host",
                "kind": "linux",
                "risk_level": "development",
                "access": {
                    "address": "single-call.example.test",
                    "username": "operator",
                    "environment_name": "single-call-vpn",
                    "environment_kind": "vpn",
                    "trust_level": "trusted",
                    "route_type": "vpn",
                    "credential_name": "single-call-password",
                    "credential_kind": "ssh_password",
                    "credential_secret": {
                        "password": "one-call-password",
                        "use_ssh_agent": true
                    }
                }
            })),
        )
        .await?;

        assert_eq!(created["credential_status"], json!("created"));
        assert_eq!(created["credential"]["name"], json!("single-call-password"));
        assert_eq!(created["credential"]["kind"], json!("ssh_password"));
        assert_eq!(created["stored_credential_fields"], json!(["password"]));
        assert!(!created.to_string().contains("one-call-password"));

        let credential_id = created["credential"]["id"]
            .as_str()
            .ok_or("credential id should be a string")?;
        let stored = fixture
            .repositories
            .credentials
            .get(CredentialId::from(uuid::Uuid::parse_str(credential_id)?))
            .await?
            .ok_or("credential should be stored")?;
        let blob: EncryptedCredentialBlob = serde_json::from_value(stored.encrypted_blob_json)?;
        let decrypted = CredentialVault::decrypt(&master, &blob)?;
        assert_eq!(decrypted.password.as_deref(), Some("one-call-password"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_agent_reuses_environment_and_reclassifies_same_endpoint_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let initial = call_tool(
            agent.clone(),
            tools::ENSURE_HOST,
            Some(json!({
                "name": "bastion-direct-login",
                "kind": "jump_host",
                "risk_level": "production",
                "access": {
                    "address": "10.36.31.20",
                    "username": "user/10.36.36.100/root",
                    "environment_name": "inode-vpn-test",
                    "environment_kind": "vpn",
                    "trust_level": "trusted",
                    "route_type": "vpn"
                }
            })),
        )
        .await?;
        let access_path_id = initial["access_path"]["id"]
            .as_str()
            .ok_or("access path id should be a string")?;
        let environment_count = fixture.repositories.environments.list().await?.len();

        let corrected = call_tool(
            agent,
            tools::ENSURE_HOST,
            Some(json!({
                "name": "bastion-direct-login",
                "kind": "jump_host",
                "risk_level": "production",
                "access": {
                    "address": "10.36.31.20",
                    "username": "user/10.36.36.100/root",
                    "environment_name": "mistaken-prod-environment",
                    "environment_kind": "company_lan",
                    "trust_level": "external",
                    "route_type": "bastion"
                }
            })),
        )
        .await?;

        assert_eq!(corrected["access_path_created"], json!(false));
        assert_eq!(corrected["access_path"]["id"], json!(access_path_id));
        assert_eq!(corrected["access_path"]["route_type"], json!("bastion"));
        assert_eq!(corrected["environment"]["name"], json!("inode-vpn-test"));
        assert_eq!(corrected["environment"]["kind"], json!("vpn"));
        assert_eq!(corrected["environment"]["trust_level"], json!("trusted"));
        assert!(
            corrected["defaults_applied"]
                .as_array()
                .is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| item == "access_path:preserved_environment")
                })
        );
        let host_id = corrected["host"]["id"]
            .as_str()
            .ok_or("host id should be a string")?;
        let paths = fixture
            .repositories
            .access_paths
            .list_for_host(HostId::from(uuid::Uuid::parse_str(host_id)?))
            .await?;
        assert_eq!(paths.len(), 1);
        assert_eq!(
            fixture.repositories.environments.list().await?.len(),
            environment_count
        );
        Ok(())
    }

    #[tokio::test]
    async fn mcp_list_hosts_returns_structured_content_without_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let result = call_tool(fixture.server(), tools::LIST_HOSTS, None).await?;

        assert_eq!(result["count"], json!(1));
        assert_eq!(result["hosts"][0]["name"], json!("company-4090-mcp"));
        assert!(result.to_string().find("encrypted_blob_json").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mcp_host_runtime_snapshot_bootstraps_agent_state_in_one_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "snapshot-workspace",
                "cwd": "/tmp"
            })),
        )
        .await?;
        assert_eq!(created["workspace"]["state"], json!("idle"));
        let now = now_utc();
        fixture
            .repositories
            .authorized_key_bootstrap
            .upsert(&AuthorizedKeyBootstrap {
                access_path_id: fixture.access_path_id,
                state: AuthorizedKeyBootstrapState::Deferred,
                reason: Some(AuthorizedKeyBootstrapReason::Timeout),
                public_key_fingerprint: Some("SHA256:mcp-test".to_owned()),
                failure_count: 1,
                attempted_at: now,
                next_retry_at: Some(now + time::Duration::minutes(15)),
                updated_at: now,
            })
            .await?;
        fixture
            .repositories
            .ssh_transport_runtimes
            .upsert(&SshTransportRuntime {
                access_path_id: fixture.access_path_id,
                connector_id: fixture.connector_id,
                telemetry: SshTransportTelemetry {
                    runtime_id: SshTransportRuntimeId::new(),
                    backend: SshTransportBackend::Russh,
                    state: SshTransportRuntimeState::Ready,
                    generation: 1,
                    connection_attempt_count: 1,
                    successful_handshake_count: 1,
                    reuse_count: 4,
                    last_handshake_at: Some(now),
                    last_validated_at: Some(now),
                    capabilities: SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
                },
                updated_at: now,
            })
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert_eq!(snapshot["snapshot_version"], json!(11));
        assert_eq!(snapshot["workspace_capacity"]["limit"], json!(32));
        assert_eq!(snapshot["workspace_capacity"]["effective_active"], json!(1));
        assert_eq!(snapshot["event_cursor"], json!(0));
        assert_eq!(snapshot["host"]["id"], json!(fixture.host_id.to_string()));
        assert_eq!(snapshot["aggregate"]["overall"], json!("healthy"));
        assert_eq!(snapshot["access_paths"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            snapshot["access_paths"][0]["authorized_key_bootstrap"]["state"],
            json!("deferred")
        );
        assert_eq!(
            snapshot["access_paths"][0]["transport_runtime"]["telemetry"]["backend"],
            json!("russh")
        );
        assert_eq!(
            snapshot["access_paths"][0]["transport_runtime"]["telemetry"]["reuse_count"],
            json!(4)
        );
        assert_eq!(
            snapshot["connection_sessions"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            snapshot["connector_snapshots"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(snapshot["workspaces"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            snapshot["workspaces"][0]["workspace"]["label"],
            json!("snapshot-workspace")
        );
        assert_eq!(snapshot["workspaces"][0]["pty_sessions"], json!([]));
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention.iter().any(|item| {
                item["code"] == json!("authorized_key_bootstrap_deferred")
                    && item["recommended_action"] == json!("wait_for_bootstrap_retry")
            })
        }));
        assert!(snapshot["generated_at"].as_str().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_exposes_saturated_ssh_channel_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let server = fixture.server();
        let created = call_tool(
            server.clone(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "capacity-snapshot"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        call_tool(
            server.clone(),
            tools::OPEN_WORKSPACE_PTY_SESSION,
            Some(json!({"workspace_id": workspace_id})),
        )
        .await?;

        let snapshot = call_tool(
            server,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert_eq!(snapshot["snapshot_version"], json!(11));
        assert_eq!(
            snapshot["access_paths"][0]["channel_capacity"],
            json!({
                "configured_limit": 1,
                "running_operations": 0,
                "active_ptys": 0,
                "pending_ptys": 1,
                "reserved_channels": 1,
                "available_channels": 0,
                "state": "saturated"
            })
        );
        assert_eq!(snapshot["workspace_capacity"]["limit"], json!(32));
        assert_eq!(snapshot["workspace_capacity"]["effective_active"], json!(1));
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention.iter().any(|item| {
                item["code"] == json!("ssh_channel_capacity_saturated")
                    && item["recommended_action"] == json!("wait_for_channel_or_raise_limit")
            }) && attention
                .iter()
                .all(|item| item["code"] != json!("logical_workspace_capacity_saturated"))
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_ignores_sessions_from_disabled_access_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(fixture.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.enabled = false;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let mut session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        session.state = EntityState::SshHandshakeFailed;
        fixture
            .repositories
            .connection_sessions
            .upsert(&session)
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert_eq!(snapshot["access_paths"], json!([]));
        assert_eq!(snapshot["connection_sessions"], json!([]));
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention
                .iter()
                .all(|item| item["code"] != json!("connection_unhealthy"))
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_compacts_historical_connection_sessions()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let current = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        for age_minutes in 1..=20 {
            let mut historical = current.clone();
            historical.session_id = SessionId::new();
            historical.state = EntityState::Unknown;
            historical.open_channels = 0;
            historical.created_at -= time::Duration::minutes(age_minutes);
            historical.last_used_at -= time::Duration::minutes(age_minutes);
            fixture
                .repositories
                .connection_sessions
                .upsert(&historical)
                .await?;
        }

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert_eq!(
            snapshot["connection_sessions"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            snapshot["connection_sessions"][0]["session_id"],
            json!(fixture.session_id.to_string())
        );
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention
                .iter()
                .all(|item| item["code"] != json!("connection_unhealthy"))
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_exposes_local_handshake_wait_without_target_blame()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: fixture.access_path_id,
                state: EntityState::Throttled,
                last_checked_at: Some(now),
                latency_ms: None,
                failure_count: 0,
                last_error_code: Some(StateReasonCode::LocalHandshakeBudgetExhausted),
                next_retry_at: Some(now + time::Duration::seconds(164)),
            })
            .await?;
        let mut session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        session.state = EntityState::Throttled;
        session.failure_count = 0;
        fixture
            .repositories
            .connection_sessions
            .upsert(&session)
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let attention = snapshot["attention"]
            .as_array()
            .ok_or("attention should be an array")?;
        assert_eq!(
            snapshot["aggregate"]["reason_code"],
            json!("local_handshake_budget_exhausted")
        );
        assert!(
            snapshot["aggregate"]["human_message"]
                .as_str()
                .is_some_and(|message| message.contains("connector local SSH handshake budget"))
        );
        let local_budget = attention
            .iter()
            .find(|item| item["code"] == json!("local_handshake_budget_exhausted"))
            .ok_or("local handshake attention should exist")?;
        assert_eq!(
            local_budget["recommended_action"],
            json!("wait_for_local_handshake_budget")
        );
        assert!(
            local_budget["message"]
                .as_str()
                .is_some_and(|message| message.contains("retry_after_seconds=16"))
        );
        assert!(attention.iter().all(|item| {
            item["code"] != json!("connection_unhealthy")
                && item["code"] != json!("target_sshd_rate_limited")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_guides_pooled_transport_recovery_without_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: fixture.access_path_id,
                state: EntityState::Degraded,
                last_checked_at: Some(now),
                latency_ms: None,
                failure_count: 1,
                last_error_code: Some(StateReasonCode::PooledTransportInvalidated),
                next_retry_at: Some(now - time::Duration::seconds(1)),
            })
            .await?;
        let mut session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        session.state = EntityState::Degraded;
        session.last_error = Some("pooled SSH session was invalidated".to_owned());
        fixture
            .repositories
            .connection_sessions
            .upsert(&session)
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let attention = snapshot["attention"]
            .as_array()
            .ok_or("attention should be an array")?;
        let recovery = attention
            .iter()
            .find(|item| item["code"] == json!("pooled_transport_reconnect_ready"))
            .ok_or("pooled transport recovery attention should exist")?;
        assert_eq!(
            recovery["recommended_action"],
            json!("prepare_fresh_workspace_and_retry_once")
        );
        assert!(
            recovery["message"]
                .as_str()
                .is_some_and(|message| message.contains("without restarting the connector"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_marks_expired_local_handshake_budget_ready()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        fixture
            .repositories
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: fixture.access_path_id,
                state: EntityState::Throttled,
                last_checked_at: Some(now - time::Duration::minutes(2)),
                latency_ms: None,
                failure_count: 0,
                last_error_code: Some(StateReasonCode::LocalHandshakeBudgetExhausted),
                next_retry_at: Some(now - time::Duration::seconds(1)),
            })
            .await?;
        let mut session = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("connection session should exist")?;
        session.state = EntityState::Throttled;
        session.failure_count = 0;
        fixture
            .repositories
            .connection_sessions
            .upsert(&session)
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let attention = snapshot["attention"]
            .as_array()
            .ok_or("attention should be an array")?;
        assert_eq!(snapshot["aggregate"]["overall"], json!("unknown"));
        assert_eq!(
            snapshot["access_paths"][0]["health"]["state"],
            json!("unknown")
        );
        assert_eq!(
            snapshot["access_paths"][0]["health"]["last_error_code"],
            Value::Null
        );
        assert_eq!(
            snapshot["connection_sessions"][0]["state"],
            json!("unknown")
        );
        let ready = attention
            .iter()
            .find(|item| item["code"] == json!("local_handshake_budget_ready"))
            .ok_or("expired local handshake budget should be retryable")?;
        assert_eq!(ready["recommended_action"], json!("retry_connection_once"));
        assert!(attention.iter().all(|item| {
            item["code"] != json!("local_handshake_budget_exhausted")
                && item["code"] != json!("connection_unhealthy")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_snapshot_warns_agents_before_multi_hop_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(fixture.access_path_id)
            .await?
            .ok_or("access path should exist")?;
        access_path.route_type = RouteType::Bastion;
        access_path.proxy_chain = Vec::new();
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;

        let direct_bastion_snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert!(
            direct_bastion_snapshot["attention"]
                .as_array()
                .is_some_and(|attention| {
                    attention
                        .iter()
                        .all(|item| item["code"] != json!("ssh_route_unsupported"))
                })
        );

        access_path.proxy_chain = vec!["ops@jump.example:22".to_owned()];
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention.iter().any(|item| {
                item["code"] == json!("ssh_route_unsupported")
                    && item["recommended_action"] == json!("configure_proxy_aware_route")
            })
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_prepare_workspace_reuses_one_workspace_and_returns_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let arguments = Some(json!({
            "host_id": fixture.host_id.to_string(),
            "label": "prepared-agent",
            "cwd": "/tmp"
        }));
        let created = call_tool(
            fixture.server(),
            tools::PREPARE_WORKSPACE,
            arguments.clone(),
        )
        .await?;
        let reused = call_tool(fixture.server(), tools::PREPARE_WORKSPACE, arguments).await?;

        assert_eq!(created["reused"], json!(false));
        assert_eq!(reused["reused"], json!(true));
        assert_eq!(created["workspace"]["id"], reused["workspace"]["id"]);
        assert_eq!(
            created["runtime_snapshot"]["workspaces"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert!(
            created["command_profiles"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );

        let scoped_arguments = Some(json!({
            "host_id": fixture.host_id.to_string(),
            "label": "prepared-agent",
            "cwd": "/tmp",
            "coordination_scope": "k8s/test/service/api"
        }));
        let scoped = call_tool(
            fixture.server(),
            tools::PREPARE_WORKSPACE,
            scoped_arguments.clone(),
        )
        .await?;
        let scoped_reused =
            call_tool(fixture.server(), tools::PREPARE_WORKSPACE, scoped_arguments).await?;
        assert_eq!(scoped["reused"], json!(false));
        assert_eq!(scoped_reused["reused"], json!(true));
        assert_ne!(created["workspace"]["id"], scoped["workspace"]["id"]);
        assert_eq!(
            scoped["workspace"]["coordination_scope"],
            json!("k8s/test/service/api")
        );
        Ok(())
    }

    #[tokio::test]
    async fn mcp_agent_sessions_isolate_workspaces_on_one_shared_host()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent_a =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let agent_b =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let arguments = Some(json!({
            "host_id": fixture.host_id.to_string(),
            "label": "isolated-agent",
            "cwd": "/tmp"
        }));

        let workspace_a =
            call_tool(agent_a.clone(), tools::PREPARE_WORKSPACE, arguments.clone()).await?;
        let workspace_a_reused =
            call_tool(agent_a.clone(), tools::PREPARE_WORKSPACE, arguments.clone()).await?;
        let workspace_b = call_tool(agent_b.clone(), tools::PREPARE_WORKSPACE, arguments).await?;

        assert_eq!(workspace_a_reused["reused"], json!(true));
        assert_eq!(
            workspace_a["workspace"]["id"],
            workspace_a_reused["workspace"]["id"]
        );
        assert_eq!(workspace_b["reused"], json!(false));
        assert_ne!(
            workspace_a["workspace"]["id"],
            workspace_b["workspace"]["id"]
        );
        assert_ne!(
            workspace_a["agent_session"]["id"],
            workspace_b["agent_session"]["id"]
        );
        assert!(workspace_a.get("runtime_snapshot").is_none());
        assert!(workspace_b.get("command_profiles").is_none());
        assert!(serde_json::to_vec(&workspace_a)?.len() < 1_200);

        let foreign_workspace_id = workspace_b["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        let foreign_run = call_tool_raw(
            agent_a,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": foreign_workspace_id,
                "command_profile": "host.identity",
                "args": []
            })),
        )
        .await?;
        assert_eq!(foreign_run.is_error, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_three_agent_sessions_can_queue_declared_readonly_shell_work_concurrently()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agents = [
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent),
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent),
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent),
        ];

        for (index, agent) in agents.iter().enumerate() {
            let prepared = call_tool(
                agent.clone(),
                tools::PREPARE_WORKSPACE,
                Some(json!({"host_id": fixture.host_id.to_string()})),
            )
            .await?;
            let queued = call_tool(
                agent.clone(),
                tools::RUN_IN_WORKSPACE,
                Some(json!({
                    "workspace_id": prepared["workspace"]["id"],
                    "command_profile": "shell.posix",
                    "args": [format!("printf 'readonly-{index}\\n'")],
                    "intent": format!("read independent state for conversation {index}"),
                    "coordination_mode": "read_only",
                    "idempotency_key": format!("readonly-conversation-{index}")
                })),
            )
            .await?;
            assert_eq!(queued["operation"]["requires_write_lease"], json!(false));
        }

        let snapshot = call_tool(
            agents[0].clone(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert_eq!(
            snapshot["write_lease"]["active_leases"]
                .as_array()
                .map(Vec::len),
            Some(0)
        );
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention
                .iter()
                .all(|item| item["code"] != json!("host_write_lease_wait"))
        }));
        Ok(())
    }

    #[test]
    fn explicit_agent_conversation_context_is_stable_and_isolated() {
        let context = super::AgentSessionContext {
            client_kind: Some("codex".to_owned()),
            client_instance_id: Some("desktop-main".to_owned()),
            project_key: Some("/workspace/project-a".to_owned()),
            conversation_key: Some("conversation-1".to_owned()),
        };
        let restarted = context.clone().into_session();
        let original = context.into_session();
        let other_conversation = super::AgentSessionContext {
            client_kind: Some("codex".to_owned()),
            client_instance_id: Some("desktop-main".to_owned()),
            project_key: Some("/workspace/project-a".to_owned()),
            conversation_key: Some("conversation-2".to_owned()),
        }
        .into_session();
        let generated_a = super::AgentSessionContext::default().into_session();
        let generated_b = super::AgentSessionContext::default().into_session();

        assert_eq!(original.id, restarted.id);
        assert_ne!(original.id, other_conversation.id);
        assert_ne!(generated_a.id, generated_b.id);
    }

    #[test]
    fn project_only_agent_context_does_not_merge_conversations() {
        let context = super::AgentSessionContext {
            client_kind: Some("codex".to_owned()),
            project_key: Some("/workspace/project-a".to_owned()),
            ..super::AgentSessionContext::default()
        };

        let process_a = context.clone().into_session();
        let process_b = context.into_session();

        assert_ne!(process_a.id, process_b.id);
        assert_eq!(
            process_a.project_key.as_deref(),
            Some("/workspace/project-a")
        );
        assert_eq!(
            process_b.project_key.as_deref(),
            Some("/workspace/project-a")
        );
    }

    #[tokio::test]
    async fn mcp_operation_idempotency_is_scoped_to_one_agent_session()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let prepared = call_tool(
            agent.clone(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let workspace_id = prepared["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        let request = Some(json!({
            "workspace_id": workspace_id,
            "command_profile": "shell.posix",
            "args": ["printf '%s\\n' hello"],
            "intent": "apply one idempotent change",
            "coordination_mode": "mutating",
            "coordination_scope": "service/example",
            "idempotency_key": "deploy-step-1"
        }));

        let first = call_tool(agent.clone(), tools::RUN_IN_WORKSPACE, request.clone()).await?;
        let retried = call_tool(agent.clone(), tools::RUN_IN_WORKSPACE, request).await?;

        assert_eq!(first["operation"]["id"], retried["operation"]["id"]);
        assert_eq!(first["idempotency_reused"], json!(false));
        assert_eq!(retried["idempotency_reused"], json!(true));
        let mismatched_retry = call_tool_raw(
            agent.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "shell.posix",
                "args": ["printf '%s\\n' changed"],
                "idempotency_key": "deploy-step-1"
            })),
        )
        .await?;
        assert_eq!(mismatched_retry.is_error, Some(true));

        let changed_coordination = call_tool_raw(
            agent,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "shell.posix",
                "args": ["printf '%s\\n' hello"],
                "intent": "apply one idempotent change",
                "coordination_mode": "read_only",
                "coordination_scope": "service/example",
                "idempotency_key": "deploy-step-1"
            })),
        )
        .await?;
        assert_eq!(changed_coordination.is_error, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_mutations_coordinate_by_host_without_blocking_readonly_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let agent_a =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let agent_b =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let arguments = Some(json!({"host_id": fixture.host_id.to_string()}));
        let prepared_a =
            call_tool(agent_a.clone(), tools::PREPARE_WORKSPACE, arguments.clone()).await?;
        let prepared_b = call_tool(agent_b.clone(), tools::PREPARE_WORKSPACE, arguments).await?;

        let mutation_a = call_tool(
            agent_a,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": prepared_a["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["touch /tmp/remote-hosts-a"],
                "idempotency_key": "agent-a-mutation"
            })),
        )
        .await?;
        let mutation_b = call_tool(
            agent_b.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": prepared_b["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["touch /tmp/remote-hosts-b"],
                "idempotency_key": "agent-b-mutation"
            })),
        )
        .await?;
        let readonly_b = call_tool(
            agent_b.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": prepared_b["workspace"]["id"],
                "command_profile": "host.identity",
                "args": [],
                "idempotency_key": "agent-b-read"
            })),
        )
        .await?;
        let snapshot = call_tool(
            agent_b,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;

        assert!(
            mutation_a["operation"]["command_preview"]
                .as_str()
                .is_some_and(|preview| preview.contains("touch /tmp/remote-hosts-a"))
        );
        assert!(
            mutation_b["operation"]["command_preview"]
                .as_str()
                .is_some_and(|preview| preview.contains("touch /tmp/remote-hosts-b"))
        );
        assert!(
            readonly_b["operation"]["command_preview"]
                .as_str()
                .is_some_and(|preview| preview.contains("hostname"))
        );
        assert!(mutation_a.get("protection_decision").is_none());
        assert_eq!(
            snapshot["write_lease"]["state"],
            json!("held_by_other_session")
        );
        assert_eq!(snapshot["snapshot_version"], json!(11));
        assert!(snapshot["attention"].as_array().is_some_and(|attention| {
            attention
                .iter()
                .any(|item| item["code"] == json!("host_write_lease_wait"))
        }));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_scoped_mutations_allow_siblings_but_block_parent_scopes()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let service_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let deployment_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let parent_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let service = call_tool(
            service_agent.clone(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let deployment = call_tool(
            deployment_agent.clone(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        call_tool(
            service_agent,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": service["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["touch /tmp/scoped-service"],
                "coordination_mode": "mutating",
                "coordination_scope": "k8s/datatool-dev/service/file-gateway",
                "idempotency_key": "scoped-service"
            })),
        )
        .await?;
        call_tool(
            deployment_agent.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": deployment["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["touch /tmp/scoped-deployment"],
                "coordination_mode": "mutating",
                "coordination_scope": "k8s/datatool-dev/deployment/report-worker",
                "idempotency_key": "scoped-deployment"
            })),
        )
        .await?;
        let sibling_snapshot = call_tool(
            deployment_agent,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert_eq!(sibling_snapshot["snapshot_version"], json!(11));
        assert_eq!(sibling_snapshot["write_lease"]["state"], json!("mixed"));
        assert_eq!(
            sibling_snapshot["write_lease"]["active_leases"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert!(
            sibling_snapshot["attention"]
                .as_array()
                .is_some_and(|attention| attention
                    .iter()
                    .all(|item| item["code"] != json!("host_write_lease_wait")))
        );

        let parent = call_tool(
            parent_agent.clone(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        call_tool(
            parent_agent.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": parent["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["touch /tmp/scoped-parent"],
                "coordination_mode": "mutating",
                "coordination_scope": "k8s/datatool-dev",
                "idempotency_key": "scoped-parent"
            })),
        )
        .await?;
        let parent_snapshot = call_tool(
            parent_agent,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert!(
            parent_snapshot["attention"]
                .as_array()
                .is_some_and(|attention| attention.iter().any(|item| {
                    item["code"] == json!("host_write_lease_wait")
                        && item["recommended_action"]
                            == json!("wait_for_overlapping_scope_or_refine_scope")
                }))
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_multi_resource_mutation_does_not_block_unrelated_production_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let cleanup_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let deployment_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let overlapping_agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let arguments = Some(json!({"host_id": fixture.host_id.to_string()}));
        let cleanup = call_tool(
            cleanup_agent.clone(),
            tools::PREPARE_WORKSPACE,
            arguments.clone(),
        )
        .await?;
        let deployment = call_tool(
            deployment_agent.clone(),
            tools::PREPARE_WORKSPACE,
            arguments.clone(),
        )
        .await?;
        let overlapping = call_tool(
            overlapping_agent.clone(),
            tools::PREPARE_WORKSPACE,
            arguments,
        )
        .await?;

        let cleanup_run = call_tool(
            cleanup_agent,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": cleanup["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["./cleanup-rejected-data"],
                "coordination_mode": "mutating",
                "coordination_scopes": [
                    "prod/datatool-dev/storage/minio/rejected-data",
                    "prod/datatool-dev/database/mysql/rejected-data",
                    "prod/datatool-dev/search/elasticsearch/rejected-data"
                ],
                "idempotency_key": "cleanup-rejected-data"
            })),
        )
        .await?;
        assert_eq!(
            cleanup_run["operation"]["coordination_scope"],
            json!("prod/datatool-dev")
        );
        assert_eq!(
            cleanup_run["operation"]["coordination_scopes"]
                .as_array()
                .map(Vec::len),
            Some(3)
        );

        call_tool(
            deployment_agent.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": deployment["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["./deploy-lichtblick"],
                "coordination_mode": "mutating",
                "coordination_scope": "prod/datatool-dev/deployment/lichtblick",
                "idempotency_key": "deploy-lichtblick"
            })),
        )
        .await?;
        let sibling_snapshot = call_tool(
            deployment_agent,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert!(
            sibling_snapshot["attention"]
                .as_array()
                .is_some_and(|attention| attention
                    .iter()
                    .all(|item| item["code"] != json!("host_write_lease_wait")))
        );

        call_tool(
            overlapping_agent.clone(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": overlapping["workspace"]["id"],
                "command_profile": "shell.posix",
                "args": ["rm -f one-rejected-object"],
                "coordination_mode": "mutating",
                "coordination_scope": "prod/datatool-dev/storage/minio/rejected-data/object-42",
                "idempotency_key": "overlap-minio-cleanup"
            })),
        )
        .await?;
        let overlapping_snapshot = call_tool(
            overlapping_agent,
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        assert!(
            overlapping_snapshot["attention"]
                .as_array()
                .is_some_and(|attention| attention
                    .iter()
                    .any(|item| item["code"] == json!("host_write_lease_wait")))
        );
        Ok(())
    }

    #[tokio::test]
    async fn mcp_agent_rejects_legacy_unowned_workspace_while_admin_can_inspect_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        let workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: None,
            host_id: fixture.host_id,
            access_path_id: fixture.access_path_id,
            connector_id: fixture.connector_id,
            label: "legacy-unowned".to_owned(),
            cwd: Some("/tmp".to_owned()),
            state: WorkspaceState::Idle,
            policy_profile: "default".to_owned(),
            coordination_scope: "host".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        };
        fixture.repositories.workspaces.insert(&workspace).await?;

        let agent =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent);
        let rejected = call_tool_raw(
            agent,
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace.id.to_string(),
                "command_profile": "host.identity",
                "args": []
            })),
        )
        .await?;
        assert_eq!(rejected.is_error, Some(true));

        let admin =
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Admin);
        let inspected = call_tool(
            admin,
            tools::GET_WORKSPACE,
            Some(json!({"workspace_id": workspace.id.to_string()})),
        )
        .await?;
        assert_eq!(
            inspected["workspace"]["id"],
            json!(workspace.id.to_string())
        );
        assert_eq!(inspected["workspace"]["agent_session_id"], Value::Null);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_prepare_workspace_does_not_reuse_unavailable_workspaces()
    -> Result<(), Box<dyn std::error::Error>> {
        for unavailable_state in [
            WorkspaceState::Blocked,
            WorkspaceState::Throttled,
            WorkspaceState::Failed,
            WorkspaceState::Done,
            WorkspaceState::Closed,
        ] {
            let fixture = TestFixture::new().await?;
            let arguments = Some(json!({
                "host_id": fixture.host_id.to_string(),
                "label": "prepared-agent",
                "cwd": "/tmp"
            }));
            let created = call_tool(
                fixture.server(),
                tools::PREPARE_WORKSPACE,
                arguments.clone(),
            )
            .await?;
            let workspace_id = created["workspace"]["id"]
                .as_str()
                .ok_or("workspace id should be a string")?;
            fixture
                .repositories
                .workspaces
                .update_state(
                    WorkspaceId::from(uuid::Uuid::parse_str(workspace_id)?),
                    unavailable_state,
                    now_utc(),
                )
                .await?;

            let replacement =
                call_tool(fixture.server(), tools::PREPARE_WORKSPACE, arguments).await?;

            assert_eq!(replacement["reused"], json!(false));
            assert_ne!(replacement["workspace"]["id"], created["workspace"]["id"]);
            assert_eq!(replacement["workspace"]["state"], json!("idle"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn mcp_prepare_workspace_reaps_expired_foreign_capacity_and_explains_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        let foreign_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "expired-conversation".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("expired-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: now - time::Duration::hours(3),
            last_seen_at: now - time::Duration::hours(3),
            expires_at: now - time::Duration::hours(2),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&foreign_session)
            .await?;
        for index in 0..32 {
            fixture
                .repositories
                .workspaces
                .insert(&AgentWorkspace {
                    id: WorkspaceId::new(),
                    agent_session_id: Some(foreign_session.id),
                    host_id: fixture.host_id,
                    access_path_id: fixture.access_path_id,
                    connector_id: fixture.connector_id,
                    label: format!("expired-{index}"),
                    cwd: Some("/tmp".to_owned()),
                    state: WorkspaceState::Idle,
                    policy_profile: "default".to_owned(),
                    coordination_scope: "host".to_owned(),
                    created_at: now - time::Duration::hours(2),
                    last_activity_at: now - time::Duration::hours(2),
                    ttl_seconds: 60,
                })
                .await?;
        }

        let prepared = call_tool(
            RemoteHostsMcpServer::with_profile(fixture.repositories.clone(), ToolProfile::Agent),
            tools::PREPARE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "label": "current-conversation"
            })),
        )
        .await?;

        assert_eq!(prepared["reused"], json!(false));
        assert_eq!(prepared["workspace"]["state"], json!("idle"));
        assert!(prepared.get("workspace_capacity").is_none());
        assert!(prepared.get("runtime_snapshot").is_none());
        assert!(prepared.get("command_profiles").is_none());
        assert!(serde_json::to_vec(&prepared)?.len() < 1_200);
        assert_eq!(
            fixture
                .repositories
                .agent_sessions
                .get(foreign_session.id)
                .await?
                .ok_or("foreign Agent Session history should remain inspectable")?
                .state,
            AgentSessionState::Expired
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn expired_workspace_reconciliation_preserves_live_operation_and_pty()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        let expired_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "expired-owner".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("expired-owner".to_owned()),
            state: AgentSessionState::Expired,
            created_at: now - time::Duration::hours(3),
            last_seen_at: now - time::Duration::hours(3),
            expires_at: now - time::Duration::hours(2),
        };
        fixture
            .repositories
            .agent_sessions
            .upsert(&expired_session)
            .await?;
        let expired_workspace = |label: &str| AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(expired_session.id),
            host_id: fixture.host_id,
            access_path_id: fixture.access_path_id,
            connector_id: fixture.connector_id,
            label: label.to_owned(),
            cwd: Some("/tmp".to_owned()),
            state: WorkspaceState::Idle,
            policy_profile: "default".to_owned(),
            coordination_scope: "host".to_owned(),
            created_at: now - time::Duration::hours(2),
            last_activity_at: now - time::Duration::hours(2),
            ttl_seconds: 60,
        };
        let operation_workspace = expired_workspace("protected-operation");
        let pty_workspace = expired_workspace("protected-pty");
        fixture
            .repositories
            .workspaces
            .insert(&operation_workspace)
            .await?;
        fixture
            .repositories
            .workspaces
            .insert(&pty_workspace)
            .await?;
        let operation = OperationRun {
            id: OperationId::new(),
            host_id: fixture.host_id,
            access_path_id: fixture.access_path_id,
            connector_id: fixture.connector_id,
            session_id: None,
            workspace_id: Some(operation_workspace.id),
            agent_session_id: Some(expired_session.id),
            idempotency_key: Some("protected-operation".to_owned()),
            requires_write_lease: false,
            coordination_scope: "host".to_owned(),
            coordination_scopes: vec!["host".to_owned()],
            operation_type: OperationType::ReadonlyExec,
            intent: "preserve queued work".to_owned(),
            state: OperationState::Queued,
            started_at: now - time::Duration::hours(2),
            finished_at: None,
            exit_code: None,
            timeout_seconds: 30,
            redacted_command_summary: "uptime".to_owned(),
            command_profile_json: Some(json!({"name": "host.uptime"})),
            transport_evidence: None,
            redacted_output_summary: None,
            log_ref: None,
            attempt_count: 0,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        };
        fixture.repositories.operations.insert(&operation).await?;
        fixture
            .repositories
            .pty_sessions
            .upsert(&PtySession {
                pty_session_id: PtySessionId::new(),
                workspace_id: pty_workspace.id,
                session_id: fixture.session_id,
                coordination_scopes: vec!["host".to_owned()],
                state: WorkspaceState::Idle,
                foreground_process: None,
                cwd: Some("/tmp".to_owned()),
                recent_output_ref: None,
                last_exit_code: None,
                input_allowed: true,
                backend_state: PtyBackendState::Active,
                backend_capabilities: PtyBackendCapabilities::unknown(),
                interaction: None,
                transport_evidence: None,
                created_at: now - time::Duration::hours(2),
                last_activity_at: now - time::Duration::hours(2),
            })
            .await?;

        assert_eq!(
            fixture
                .repositories
                .workspaces
                .reconcile_expired_for_host(fixture.host_id, now, 100)
                .await?,
            0
        );
        let capacity = fixture
            .repositories
            .workspaces
            .capacity_for_host(fixture.host_id, None, now)
            .await?;
        assert_eq!(capacity.recorded_active, 2);
        assert_eq!(capacity.effective_active, 2);
        assert_eq!(capacity.expired_reapable, 0);
        assert_eq!(
            fixture
                .repositories
                .workspaces
                .get(operation_workspace.id)
                .await?
                .ok_or("operation workspace should exist")?
                .state,
            WorkspaceState::Idle
        );
        assert_eq!(
            fixture
                .repositories
                .workspaces
                .get(pty_workspace.id)
                .await?
                .ok_or("PTY workspace should exist")?
                .state,
            WorkspaceState::Idle
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_workspace_inserts_cannot_exceed_logical_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let now = now_utc();
        let mut inserts = tokio::task::JoinSet::new();
        for index in 0..64 {
            let repository = fixture.repositories.workspaces.clone();
            let workspace = AgentWorkspace {
                id: WorkspaceId::new(),
                agent_session_id: None,
                host_id: fixture.host_id,
                access_path_id: fixture.access_path_id,
                connector_id: fixture.connector_id,
                label: format!("concurrent-{index}"),
                cwd: Some("/tmp".to_owned()),
                state: WorkspaceState::Idle,
                policy_profile: "default".to_owned(),
                coordination_scope: "host".to_owned(),
                created_at: now,
                last_activity_at: now,
                ttl_seconds: 3_600,
            };
            inserts
                .spawn(async move { repository.insert_below_active_limit(&workspace, 32).await });
        }
        let mut inserted = 0;
        while let Some(result) = inserts.join_next().await {
            if result?? {
                inserted += 1;
            }
        }

        assert_eq!(inserted, 32);
        let capacity = fixture
            .repositories
            .workspaces
            .capacity_for_host(fixture.host_id, None, now)
            .await?;
        assert_eq!(capacity.recorded_active, 32);
        assert_eq!(capacity.effective_active, 32);
        assert_eq!(capacity.expired_reapable, 0);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_upload_file_queues_sftp_and_redacts_paths_from_public_audit()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let prepared = call_tool(
            fixture.server(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let workspace_id = prepared["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        let queued = call_tool(
            fixture.server(),
            tools::UPLOAD_FILE,
            Some(json!({
                "workspace_id": workspace_id,
                "local_path": "/tmp/deploy/manifest.yaml",
                "remote_path": "/var/tmp/manifest.yaml",
                "overwrite": "replace",
                "mode": "0600",
                "expected_sha256": "a".repeat(64),
                "intent": "upload deployment manifest"
            })),
        )
        .await?;

        assert_eq!(queued["operation"]["operation_type"], json!("sftp"));
        assert_eq!(
            queued["operation"]["command_profile_json"]["direction"],
            json!("upload")
        );
        assert_eq!(
            queued["operation"]["command_profile_json"]["local_file"],
            json!("manifest.yaml")
        );
        assert_eq!(
            queued["operation"]["command_profile_json"]["remote_file"],
            json!("manifest.yaml")
        );
        assert_eq!(
            queued["operation"]["command_profile_json"]["paths_redacted"],
            json!(true)
        );
        assert!(!queued.to_string().contains("/tmp/deploy"));
        assert!(!queued.to_string().contains("/var/tmp"));

        let operation_id = super::parse_operation_id(
            queued["operation"]["id"]
                .as_str()
                .ok_or("operation id should be a string")?,
        )?;
        let stored = fixture
            .repositories
            .operations
            .get(operation_id)
            .await?
            .ok_or("stored operation should exist")?;
        assert_eq!(
            stored
                .command_profile_json
                .as_ref()
                .and_then(|value| value.get("local_path")),
            Some(&json!("/tmp/deploy/manifest.yaml"))
        );

        let invalid = call_tool_raw(
            fixture.server(),
            tools::DOWNLOAD_FILE,
            Some(json!({
                "workspace_id": workspace_id,
                "remote_path": "/var/tmp/../secret",
                "local_path": "/tmp/secret"
            })),
        )
        .await?;
        assert_eq!(invalid.is_error, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_get_workspace_result_combines_output_and_artifacts()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let prepared = call_tool(
            fixture.server(),
            tools::PREPARE_WORKSPACE,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let workspace_id = prepared["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        let queued = call_tool(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "host.identity",
                "args": []
            })),
        )
        .await?;
        let operation_id = queued["operation"]["id"]
            .as_str()
            .ok_or("operation id should be a string")?;

        let result = call_tool(
            fixture.server(),
            tools::GET_WORKSPACE_RESULT,
            Some(json!({
                "workspace_id": workspace_id,
                "operation_id": operation_id
            })),
        )
        .await?;
        assert_eq!(result["workspace"]["id"], json!(workspace_id));
        assert_eq!(result["chunk_count"], json!(1));
        assert_eq!(result["recent_operations"][0]["id"], json!(operation_id));
        assert_eq!(result["artifact_count"], json!(0));
        assert_eq!(result["artifacts"], json!([]));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_runtime_event_wait_distinguishes_live_only_from_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        call_tool(
            fixture.server(),
            tools::CONNECTOR_HEARTBEAT,
            Some(json!({
                "connector_id": fixture.connector_id.to_string(),
                "state": "degraded"
            })),
        )
        .await?;

        let live = call_tool(
            fixture.server(),
            tools::WAIT_RUNTIME_EVENTS,
            Some(json!({
                "start_mode": "live_only",
                "entity_type": "connector",
                "entity_id": fixture.connector_id.to_string(),
                "timeout_ms": 0
            })),
        )
        .await?;
        assert_eq!(live["start_cursor"], json!(1));
        assert_eq!(live["next_cursor"], json!(1));
        assert_eq!(live["timed_out"], json!(true));
        assert_eq!(live["events"], json!([]));

        let replay = call_tool(
            fixture.server(),
            tools::WAIT_RUNTIME_EVENTS,
            Some(json!({
                "start_mode": "after_cursor",
                "after_cursor": 0,
                "entity_type": "connector",
                "entity_id": fixture.connector_id.to_string(),
                "timeout_ms": 0
            })),
        )
        .await?;
        assert_eq!(replay["start_cursor"], json!(0));
        assert_eq!(replay["next_cursor"], json!(1));
        assert_eq!(replay["timed_out"], json!(false));
        assert_eq!(replay["count"], json!(1));
        assert_eq!(replay["events"][0]["sequence"], json!(1));
        assert_eq!(replay["events"][0]["entity_type"], json!("connector"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_live_runtime_event_wait_observes_future_transition()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let wait = call_tool(
            fixture.server(),
            tools::WAIT_RUNTIME_EVENTS,
            Some(json!({
                "start_mode": "live_only",
                "entity_type": "connector",
                "entity_id": fixture.connector_id.to_string(),
                "timeout_ms": 1_000
            })),
        );
        let heartbeat = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            call_tool(
                fixture.server(),
                tools::CONNECTOR_HEARTBEAT,
                Some(json!({
                    "connector_id": fixture.connector_id.to_string(),
                    "state": "degraded"
                })),
            )
            .await
        };
        let (waited, heartbeat_result) = tokio::join!(wait, heartbeat);
        heartbeat_result?;
        let waited = waited?;

        assert_eq!(waited["start_cursor"], json!(0));
        assert_eq!(waited["next_cursor"], json!(1));
        assert_eq!(waited["timed_out"], json!(false));
        assert_eq!(waited["events"][0]["new_state"], json!("degraded"));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_registry_tools_upsert_records_without_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;

        let environment = call_tool(
            fixture.server(),
            tools::UPSERT_ENVIRONMENT,
            Some(json!({
                "name": "home-lan",
                "kind": "home_lan",
                "trust_level": "owned",
                "description": "Home LAN"
            })),
        )
        .await?;
        assert_eq!(environment["created"], json!(true));
        let environment_id = environment["environment"]["id"]
            .as_str()
            .ok_or("environment id should be a string")?;

        let same_environment = call_tool(
            fixture.server(),
            tools::UPSERT_ENVIRONMENT,
            Some(json!({
                "name": "home-lan",
                "kind": "home_lan",
                "trust_level": "owned",
                "description": "Home LAN updated"
            })),
        )
        .await?;
        assert_eq!(same_environment["created"], json!(false));
        assert_eq!(same_environment["environment"]["id"], json!(environment_id));
        assert_eq!(
            same_environment["environment"]["description"],
            json!("Home LAN updated")
        );

        let credential = call_tool(
            fixture.server(),
            tools::UPSERT_CREDENTIAL_REF,
            Some(json!({
                "name": "openssh-default",
                "kind": "ssh_private_key",
                "username_hint": "jin",
                "external_ref": "openssh-agent"
            })),
        )
        .await?;
        assert_eq!(credential["created"], json!(true));
        assert!(!credential.to_string().contains("encrypted_blob_json"));
        assert!(!credential.to_string().contains("openssh-agent"));
        let credential_id = credential["credential"]["id"]
            .as_str()
            .ok_or("credential id should be a string")?;

        let rejected_secret = call_tool_raw(
            fixture.server(),
            tools::UPSERT_CREDENTIAL_REF,
            Some(json!({
                "name": "bad-secret",
                "kind": "ssh_password",
                "password": "hunter2"
            })),
        )
        .await?;
        assert_eq!(rejected_secret.is_error, Some(true));
        assert!(!format!("{rejected_secret:?}").contains("hunter2"));

        let host = call_tool(
            fixture.server(),
            tools::UPSERT_HOST,
            Some(json!({
                "name": "macstudio",
                "display_name": "Mac Studio",
                "kind": "macos",
                "risk_level": "personal",
                "owner": "jin",
                "tags": ["home", "apple"]
            })),
        )
        .await?;
        assert_eq!(host["created"], json!(true));
        let host_id = host["host"]["id"]
            .as_str()
            .ok_or("host id should be a string")?;

        let same_host = call_tool(
            fixture.server(),
            tools::UPSERT_HOST,
            Some(json!({
                "name": "macstudio",
                "display_name": "Mac Studio M2 Ultra",
                "kind": "macos",
                "risk_level": "personal",
                "owner": "jin",
                "tags": ["home", "apple", "desktop"]
            })),
        )
        .await?;
        assert_eq!(same_host["created"], json!(false));
        assert_eq!(same_host["host"]["id"], json!(host_id));
        assert_eq!(
            same_host["host"]["display_name"],
            json!("Mac Studio M2 Ultra")
        );

        let access_path = call_tool(
            fixture.server(),
            tools::UPSERT_ACCESS_PATH,
            Some(json!({
                "host_id": host_id,
                "environment_id": environment_id,
                "credential_id": credential_id,
                "address": "192.168.31.20",
                "port": 22,
                "username": "jin",
                "route_type": "lan",
                "priority": 10,
                "notes": "home direct route"
            })),
        )
        .await?;
        assert_eq!(access_path["created"], json!(true));
        let access_path_id = access_path["access_path"]["id"]
            .as_str()
            .ok_or("access path id should be a string")?;

        let same_access_path = call_tool(
            fixture.server(),
            tools::UPSERT_ACCESS_PATH,
            Some(json!({
                "host_id": host_id,
                "environment_id": environment_id,
                "credential_id": credential_id,
                "address": "192.168.31.20",
                "port": 22,
                "username": "jin",
                "route_type": "lan",
                "priority": 5,
                "notes": "home direct route, preferred"
            })),
        )
        .await?;
        assert_eq!(same_access_path["created"], json!(false));
        assert_eq!(same_access_path["access_path"]["id"], json!(access_path_id));
        assert_eq!(same_access_path["access_path"]["priority"], json!(5));

        let duplicates = call_tool(
            fixture.server(),
            tools::FIND_HOST_DUPLICATES,
            Some(json!({
                "name": "macstudio",
                "address": "192.168.31.20",
                "port": 22,
                "username": "jin"
            })),
        )
        .await?;
        assert_eq!(duplicates["count"], json!(1));
        assert_eq!(duplicates["candidates"][0]["host"]["id"], json!(host_id));
        assert!(
            duplicates["candidates"][0]["signals"]
                .as_array()
                .ok_or("signals should be an array")?
                .iter()
                .any(|signal| signal == "name")
        );
        assert!(
            duplicates["candidates"][0]["signals"]
                .as_array()
                .ok_or("signals should be an array")?
                .iter()
                .any(|signal| signal == "access_path")
        );

        let credentials = call_tool(fixture.server(), tools::LIST_CREDENTIALS, None).await?;
        assert!(credentials["count"].as_u64().unwrap_or_default() >= 2);
        assert!(!credentials.to_string().contains("encrypted_blob_json"));

        let environments = call_tool(fixture.server(), tools::LIST_ENVIRONMENTS, None).await?;
        assert!(environments["count"].as_u64().unwrap_or_default() >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_registry_record_tools_reject_secret_like_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;

        let rejected_fact = call_tool_raw(
            fixture.server(),
            tools::RECORD_HOST_FACT,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "namespace": "auth",
                "key": "login",
                "value": {"password": "hunter2"}
            })),
        )
        .await?;
        assert_eq!(rejected_fact.is_error, Some(true));
        assert!(!format!("{rejected_fact:?}").contains("hunter2"));

        let rejected_knowledge = call_tool_raw(
            fixture.server(),
            tools::RECORD_KNOWLEDGE,
            Some(json!({
                "title": "bad note",
                "body": "API_TOKEN=hunter2",
                "linked_host_ids": [fixture.host_id.to_string()]
            })),
        )
        .await?;
        assert_eq!(rejected_knowledge.is_error, Some(true));
        assert!(!format!("{rejected_knowledge:?}").contains("hunter2"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_knowledge_search_treats_punctuation_as_literal_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        call_tool(
            fixture.server(),
            tools::RECORD_KNOWLEDGE,
            Some(json!({
                "title": "hacker-s-news deployment",
                "body": "C++ server on NAS/家庭服务器 uses foo:bar routing",
                "linked_host_ids": [fixture.host_id.to_string()]
            })),
        )
        .await?;

        for query in [
            "hacker-s-news deployment",
            "NAS/家庭服务器",
            "C++ server",
            "foo:bar",
        ] {
            let result = call_tool(
                fixture.server(),
                tools::SEARCH_KNOWLEDGE,
                Some(json!({"query": query})),
            )
            .await?;
            assert_eq!(result["count"], json!(1), "query {query:?} should match");
        }

        let punctuation_only = call_tool(
            fixture.server(),
            tools::SEARCH_KNOWLEDGE,
            Some(json!({"query": "- / + :"})),
        )
        .await?;
        assert_eq!(punctuation_only["count"], json!(0));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_create_workspace_supports_multiple_logical_workspaces()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let arguments = json!({
            "host_id": fixture.host_id.to_string(),
            "access_path_id": fixture.access_path_id.to_string(),
            "label": "agent-main",
            "cwd": "/tmp"
        });

        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(arguments.clone()),
        )
        .await?;
        assert_eq!(created["workspace"]["state"], json!("idle"));

        let second = call_tool(fixture.server(), tools::CREATE_WORKSPACE, Some(arguments)).await?;
        assert_eq!(second["workspace"]["state"], json!("idle"));
        assert_ne!(created["workspace"]["id"], second["workspace"]["id"]);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_run_in_workspace_queues_operation_and_exposes_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "agent-main",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("created workspace id should be a string")?;

        let queued = call_tool(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "host.uptime",
                "args": [],
                "intent": "check whether the host is responsive"
            })),
        )
        .await?;

        assert_eq!(queued["operation"]["state"], json!("queued"));
        assert_eq!(queued["workspace"]["state"], json!("working"));
        assert_eq!(queued["operation"]["workspace_id"], json!(workspace_id));
        let operation_id = queued["operation"]["id"]
            .as_str()
            .ok_or("queued operation id should be a string")?;

        let output = call_tool(
            fixture.server(),
            tools::READ_WORKSPACE_OUTPUT,
            Some(json!({
                "workspace_id": workspace_id,
                "operation_id": operation_id,
                "limit": 10
            })),
        )
        .await?;
        assert_eq!(output["chunks"][0]["stream"], json!("system"));
        assert!(
            output["chunks"][0]["redacted_text"]
                .as_str()
                .unwrap_or_default()
                .contains("queued")
        );
        assert!(!output.to_string().contains("hunter2"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_run_in_workspace_can_atomically_wait_for_its_exact_operation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "atomic-wait",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = WorkspaceId::from(uuid::Uuid::parse_str(
            created["workspace"]["id"]
                .as_str()
                .ok_or("created workspace id should be a string")?,
        )?);
        let repositories = fixture.repositories.clone();
        let completer = tokio::spawn(async move {
            for _ in 0..100 {
                let operations = repositories
                    .operations
                    .list_for_workspace(workspace_id, 10)
                    .await?;
                if let Some(operation) = operations.into_iter().next() {
                    repositories
                        .operations
                        .update_state(
                            operation.id,
                            OperationState::Succeeded,
                            Some(now_utc()),
                            Some(0),
                            Some("test completion"),
                        )
                        .await?;
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(operation.id);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err::<OperationId, Box<dyn std::error::Error + Send + Sync>>(
                "queued operation did not appear".into(),
            )
        });

        let wait_result = call_tool(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id.to_string(),
                "command_profile": "host.uptime",
                "args": [],
                "wait_timeout_ms": 1_000
            })),
        )
        .await?;
        let completed_operation_id = match completer.await? {
            Ok(operation_id) => operation_id,
            Err(error) => return Err(std::io::Error::other(error.to_string()).into()),
        };
        assert_eq!(wait_result["completion"]["completed"], json!(true));
        assert_eq!(wait_result["completion"]["retry_after_ms"], Value::Null);
        assert_eq!(
            wait_result["completion"]["operation"]["id"],
            json!(completed_operation_id.to_string())
        );
        assert_eq!(
            wait_result["operation"]["id"],
            wait_result["completion"]["operation"]["id"]
        );

        let timed_out = call_tool(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id.to_string(),
                "command_profile": "host.uptime",
                "args": [],
                "wait_timeout_ms": 0
            })),
        )
        .await?;
        assert_eq!(timed_out["completion"]["completed"], json!(false));
        assert_eq!(
            timed_out["completion"]["operation"]["state"],
            json!("queued")
        );
        assert_eq!(timed_out["completion"]["retry_after_ms"], json!(100));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_run_in_workspace_rejects_exec_for_tty_only_access_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let mut access_path = fixture
            .repositories
            .access_paths
            .get(fixture.access_path_id)
            .await?
            .ok_or("fixture access path should exist")?;
        access_path.requires_tty = true;
        access_path.route_type = RouteType::Bastion;
        fixture
            .repositories
            .access_paths
            .upsert(&access_path)
            .await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "tty-only"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("created workspace id should be a string")?;

        let result = call_tool_raw(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "host.uptime",
                "args": [],
                "intent": "verify tty-only route guard"
            })),
        )
        .await?;

        assert_eq!(result.is_error, Some(true));
        assert!(format!("{result:?}").contains("requires a persistent PTY"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_run_in_workspace_supports_shell_profile_but_rejects_disguised_shell_arguments()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "agent-main",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("created workspace id should be a string")?;

        let unknown = call_tool_raw(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "raw.shell",
                "args": ["rm -rf /"]
            })),
        )
        .await?;
        assert_eq!(unknown.is_error, Some(true));

        let shell = call_tool(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "shell.posix",
                "args": ["set -e\nkubectl get pods | head -n 20"],
                "intent": "Inspect a bounded deployment status through the pooled workspace.",
                "timeout_seconds": 1200,
                "output_limit_bytes": 2_097_152
            })),
        )
        .await?;
        assert_eq!(
            shell["operation"]["command_profile_json"]["name"],
            json!("shell.posix")
        );
        assert_eq!(
            shell["operation"]["command_profile_json"]["program"],
            json!("sh")
        );
        assert_eq!(
            shell["operation"]["command_profile_json"]["class"],
            json!("sensitive")
        );
        assert_eq!(shell["operation"]["timeout_seconds"], json!(1200));
        assert_eq!(
            shell["operation"]["command_profile_json"]["output_limit_bytes"],
            json!(2_097_152)
        );
        assert_eq!(
            shell["operation"]["command_profile_json"]["script_redacted"],
            json!(true)
        );
        assert!(shell.to_string().contains("kubectl get pods"));
        assert!(
            shell["operation"]["redacted_command_summary"]
                .as_str()
                .is_some_and(|summary| {
                    summary.contains("shell.posix") && summary.contains("kubectl get pods")
                })
        );

        let shell_like = call_tool_raw(
            fixture.server(),
            tools::RUN_IN_WORKSPACE,
            Some(json!({
                "workspace_id": workspace_id,
                "command_profile": "disk.usage",
                "args": ["/tmp; cat /etc/shadow"]
            })),
        )
        .await?;
        assert_eq!(shell_like.is_error, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_wait_workspace_state_returns_visible_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "agent-main",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("created workspace id should be a string")?;

        let waited = call_tool(
            fixture.server(),
            tools::WAIT_WORKSPACE_STATE,
            Some(json!({
                "workspace_id": workspace_id,
                "desired_states": ["idle"],
                "timeout_ms": 50,
                "poll_interval_ms": 10
            })),
        )
        .await?;
        assert_eq!(waited["matched"], json!(true));
        assert_eq!(waited["workspace"]["state"], json!("idle"));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_pty_input_preacquires_exact_pty_scopes_without_host_widening()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "scoped-interactive-session",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = WorkspaceId::from(uuid::Uuid::parse_str(
            created["workspace"]["id"]
                .as_str()
                .ok_or("created workspace id should be a string")?,
        )?);
        let exact_scopes = vec![
            "k8s/prod/datatool-dev/job/maintenance-1".to_owned(),
            "prod/datatool-dev/minio/list-diagnostic".to_owned(),
        ];
        let now = now_utc();
        let pty_session_id = PtySessionId::new();
        fixture
            .repositories
            .pty_sessions
            .upsert(&PtySession {
                pty_session_id,
                workspace_id,
                session_id: fixture.session_id,
                coordination_scopes: exact_scopes.clone(),
                state: WorkspaceState::Working,
                foreground_process: None,
                cwd: Some("/tmp".to_owned()),
                recent_output_ref: None,
                last_exit_code: None,
                input_allowed: true,
                backend_state: PtyBackendState::Active,
                backend_capabilities: PtyBackendCapabilities::unknown(),
                interaction: None,
                transport_evidence: None,
                created_at: now,
                last_activity_at: now,
            })
            .await?;

        call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id.to_string(),
                "input": "printf 'ready\\n'\n",
                "idempotency_key": "scoped-pty-input-1"
            })),
        )
        .await?;

        let leases = fixture
            .repositories
            .host_write_leases
            .list_active(fixture.host_id, now_utc())
            .await?;
        let mut acquired_scopes = leases
            .into_iter()
            .map(|lease| lease.coordination_scope)
            .collect::<Vec<_>>();
        acquired_scopes.sort();
        let mut expected_scopes = exact_scopes;
        expected_scopes.sort();
        assert_eq!(acquired_scopes, expected_scopes);
        assert!(!acquired_scopes.iter().any(|scope| scope == "host"));
        Ok(())
    }

    #[tokio::test]
    async fn runtime_snapshot_exposes_live_pty_input_without_generic_workspace_block()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "interactive-prompt",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = WorkspaceId::from(uuid::Uuid::parse_str(
            created["workspace"]["id"]
                .as_str()
                .ok_or("created workspace id should be a string")?,
        )?);
        let now = now_utc();
        let pty_session_id = PtySessionId::new();
        fixture
            .repositories
            .pty_sessions
            .upsert(&PtySession {
                pty_session_id,
                workspace_id,
                session_id: fixture.session_id,
                coordination_scopes: vec!["host".to_owned()],
                state: WorkspaceState::Working,
                foreground_process: Some("sudo apt update".to_owned()),
                cwd: Some("/tmp".to_owned()),
                recent_output_ref: None,
                last_exit_code: None,
                input_allowed: true,
                backend_state: PtyBackendState::Active,
                backend_capabilities: PtyBackendCapabilities::unknown(),
                interaction: Some(PtyInteraction {
                    kind: PtyInteractionKind::SudoPassword,
                    confidence: 100,
                    observed_at: now,
                }),
                transport_evidence: None,
                created_at: now,
                last_activity_at: now,
            })
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(workspace_id, WorkspaceState::Blocked, now)
            .await?;

        let snapshot = call_tool(
            fixture.server(),
            tools::GET_HOST_RUNTIME_SNAPSHOT,
            Some(json!({"host_id": fixture.host_id.to_string()})),
        )
        .await?;
        let attention = snapshot["attention"]
            .as_array()
            .ok_or("attention should be an array")?;
        assert!(attention.iter().any(|item| {
            item["code"] == json!("pty_input_required")
                && item["entity_type"] == json!("pty_session")
                && item["entity_id"] == json!(pty_session_id.to_string())
                && item["recommended_action"] == json!("read_pty_output_then_queue_input")
        }));
        assert!(!attention.iter().any(|item| {
            item["code"] == json!("workspace_blocked")
                && item["entity_id"] == json!(workspace_id.to_string())
        }));

        let queued_sudo = call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id.to_string(),
                "use_stored_sudo_password": true,
                "idempotency_key": "sudo-prompt-1"
            })),
        )
        .await?;
        assert_eq!(
            queued_sudo["input_event"]["payload_kind"],
            json!("stored_sudo_password")
        );
        assert_eq!(queued_sudo["input_event"]["byte_len"], json!(0));
        assert_eq!(
            queued_sudo["input_event"]["redacted_input_summary"],
            json!("stored sudo password queued for pty input")
        );
        assert!(!queued_sudo.to_string().contains("password="));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_queues_target_host_password_without_exposing_target_or_secret()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "nested-ssh-prompt",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = WorkspaceId::from(uuid::Uuid::parse_str(
            created["workspace"]["id"]
                .as_str()
                .ok_or("created workspace id should be a string")?,
        )?);
        let now = now_utc();
        let pty_session_id = PtySessionId::new();
        fixture
            .repositories
            .pty_sessions
            .upsert(&PtySession {
                pty_session_id,
                workspace_id,
                session_id: fixture.session_id,
                coordination_scopes: vec!["host".to_owned()],
                state: WorkspaceState::Blocked,
                foreground_process: Some("ssh target".to_owned()),
                cwd: Some("/tmp".to_owned()),
                recent_output_ref: None,
                last_exit_code: None,
                input_allowed: true,
                backend_state: PtyBackendState::Active,
                backend_capabilities: PtyBackendCapabilities::unknown(),
                interaction: Some(PtyInteraction {
                    kind: PtyInteractionKind::Password,
                    confidence: 92,
                    observed_at: now,
                }),
                transport_evidence: None,
                created_at: now,
                last_activity_at: now,
            })
            .await?;
        fixture
            .repositories
            .workspaces
            .update_state(workspace_id, WorkspaceState::Blocked, now)
            .await?;

        let queued_ssh = call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id.to_string(),
                "use_stored_password_from_host_id": fixture.host_id.to_string(),
                "idempotency_key": "nested-ssh-prompt-1"
            })),
        )
        .await?;
        assert_eq!(
            queued_ssh["input_event"]["payload_kind"],
            json!("stored_ssh_password")
        );
        assert_eq!(queued_ssh["input_event"]["byte_len"], json!(0));
        assert_eq!(
            queued_ssh["input_event"]["redacted_input_summary"],
            json!("stored SSH password queued for PTY input")
        );
        assert!(
            !queued_ssh
                .to_string()
                .contains(&fixture.access_path_id.to_string())
        );

        let conflicting = call_tool_raw(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id.to_string(),
                "input": "must-not-be-queued",
                "use_stored_password_from_host_id": fixture.host_id.to_string()
            })),
        )
        .await?;
        assert_eq!(conflicting.is_error, Some(true));

        let mut sudo_pty = fixture
            .repositories
            .pty_sessions
            .get(pty_session_id)
            .await?
            .ok_or("PTY should exist")?;
        sudo_pty.interaction = Some(PtyInteraction {
            kind: PtyInteractionKind::SudoPassword,
            confidence: 100,
            observed_at: now_utc(),
        });
        fixture.repositories.pty_sessions.upsert(&sudo_pty).await?;
        let queued_target_sudo = call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id.to_string(),
                "use_stored_sudo_password_from_host_id": fixture.host_id.to_string(),
                "idempotency_key": "nested-target-sudo-prompt-1"
            })),
        )
        .await?;
        assert_eq!(
            queued_target_sudo["input_event"]["payload_kind"],
            json!("stored_target_sudo_password")
        );
        assert_eq!(queued_target_sudo["input_event"]["byte_len"], json!(0));
        assert!(
            !queued_target_sudo
                .to_string()
                .contains(&fixture.access_path_id.to_string())
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_output_artifact_tools_return_metadata_and_bounded_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let artifact_root = tempfile::tempdir()?;
        let agent = RemoteHostsMcpServer::with_profile_vault_and_artifact_root(
            fixture.repositories.clone(),
            ToolProfile::Agent,
            None,
            artifact_root.path(),
        );
        let full = RemoteHostsMcpServer::with_profile_vault_and_artifact_root(
            fixture.repositories.clone(),
            ToolProfile::Full,
            None,
            artifact_root.path(),
        );
        let created = call_tool(
            agent.clone(),
            tools::PREPARE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "artifact-workspace",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = super::parse_workspace_id(
            created["workspace"]["id"]
                .as_str()
                .ok_or("created workspace id should be a string")?,
        )?;
        let relative_path = "aa/bb/artifact.stdout.txt";
        let artifact_path = artifact_root.path().join(relative_path);
        tokio::fs::create_dir_all(
            artifact_path
                .parent()
                .ok_or("artifact test path should have parent")?,
        )
        .await?;
        let content = "redacted log line: 部署完成\n".repeat(200);
        tokio::fs::write(&artifact_path, content.as_bytes()).await?;
        let now = now_utc();
        let operation = OperationRun {
            id: OperationId::new(),
            host_id: fixture.host_id,
            access_path_id: fixture.access_path_id,
            connector_id: fixture.connector_id,
            session_id: None,
            workspace_id: Some(workspace_id),
            agent_session_id: None,
            idempotency_key: None,
            requires_write_lease: false,
            coordination_scope: "host".to_owned(),
            coordination_scopes: vec!["host".to_owned()],
            operation_type: OperationType::ReadonlyExec,
            intent: "capture long output".to_owned(),
            state: OperationState::Succeeded,
            started_at: now,
            finished_at: Some(now),
            exit_code: Some(0),
            timeout_seconds: 30,
            redacted_command_summary: "du -sh /var/log".to_owned(),
            command_profile_json: Some(json!({"name": "disk.usage"})),
            transport_evidence: None,
            redacted_output_summary: Some("stored as artifact".to_owned()),
            log_ref: None,
            attempt_count: 1,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        };
        fixture.repositories.operations.insert(&operation).await?;
        let artifact = OperationOutputArtifact {
            id: OperationOutputArtifactId::new(),
            operation_id: operation.id,
            workspace_id,
            stream: OutputStream::Stdout,
            relative_path: relative_path.to_owned(),
            byte_len: u64::try_from(content.len())?,
            sha256: "d2".repeat(32),
            redacted_preview: "first lines with [REDACTED]".to_owned(),
            truncated: false,
            created_at: now,
        };
        fixture
            .repositories
            .operation_output_artifacts
            .insert(&artifact)
            .await?;

        let listed = call_tool(
            full.clone(),
            tools::LIST_WORKSPACE_OUTPUT_ARTIFACTS,
            Some(json!({
                "workspace_id": workspace_id.to_string(),
                "operation_id": operation.id.to_string(),
                "limit": 10
            })),
        )
        .await?;
        assert_eq!(listed["count"], json!(1));
        assert_eq!(listed["artifacts"][0]["id"], json!(artifact.id.to_string()));
        assert_eq!(
            listed["artifacts"][0]["redacted_preview"],
            json!("first lines with [REDACTED]")
        );
        assert!(!listed.to_string().contains("hunter2"));

        let loaded = call_tool(
            full,
            tools::GET_OUTPUT_ARTIFACT,
            Some(json!({
                "artifact_id": artifact.id.to_string()
            })),
        )
        .await?;
        assert_eq!(
            loaded["artifact"]["relative_path"],
            json!("aa/bb/artifact.stdout.txt")
        );
        assert_eq!(
            loaded["artifact"]["byte_len"],
            json!(u64::try_from(content.len())?)
        );

        let first = call_tool(
            agent.clone(),
            tools::READ_OUTPUT_ARTIFACT_CONTENT,
            Some(json!({
                "artifact_id": artifact.id.to_string(),
                "offset": 0,
                "max_bytes": 1024
            })),
        )
        .await?;
        assert_eq!(first["offset"], json!(0));
        assert_eq!(first["eof"], json!(false));
        assert!(
            first["content"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        let next_offset = first["next_offset"]
            .as_u64()
            .ok_or("next offset should be an integer")?;
        let second = call_tool(
            agent,
            tools::READ_OUTPUT_ARTIFACT_CONTENT,
            Some(json!({
                "artifact_id": artifact.id.to_string(),
                "offset": next_offset,
                "max_bytes": 1024
            })),
        )
        .await?;
        assert_eq!(second["offset"], json!(next_offset));
        assert!(
            second["next_offset"]
                .as_u64()
                .is_some_and(|offset| offset > next_offset)
        );
        assert_eq!(second["sha256"], json!("d2".repeat(32)));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn mcp_pty_lifecycle_tools_manage_agent_visible_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "pty-workspace",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("created workspace id should be a string")?;

        let opened = call_tool(
            fixture.server(),
            tools::OPEN_WORKSPACE_PTY_SESSION,
            Some(json!({
                "workspace_id": workspace_id,
                "cwd": "/tmp"
            })),
        )
        .await?;
        let pty_session_id = opened["pty_session"]["pty_session_id"]
            .as_str()
            .ok_or("pty session id should be a string")?;
        assert_eq!(opened["pty_session"]["input_allowed"], json!(true));
        assert_eq!(
            opened["pty_session"]["session_id"],
            json!(fixture.session_id.to_string())
        );
        let pty_session_id_value = PtySessionId::from(uuid::Uuid::parse_str(pty_session_id)?);
        fixture
            .repositories
            .pty_output_chunks
            .insert(&PtyOutputChunk {
                id: PtyOutputChunkId::new(),
                pty_session_id: pty_session_id_value,
                workspace_id: WorkspaceId::from(uuid::Uuid::parse_str(workspace_id)?),
                stream: OutputStream::Stdout,
                sequence: 0,
                redacted_text: "shell ready password=[REDACTED]".to_owned(),
                byte_len: 31,
                truncated: false,
                created_at: now_utc(),
            })
            .await?;

        let pty_output = call_tool(
            fixture.server(),
            tools::READ_PTY_OUTPUT,
            Some(json!({
                "pty_session_id": pty_session_id,
                "limit": 10
            })),
        )
        .await?;
        assert_eq!(pty_output["count"], json!(1));
        assert_eq!(pty_output["chunks"][0]["stream"], json!("stdout"));
        assert!(
            pty_output["chunks"][0]["redacted_text"]
                .as_str()
                .unwrap_or_default()
                .contains("[REDACTED]")
        );
        assert!(!pty_output.to_string().contains("hunter2"));

        let after_first = call_tool(
            fixture.server(),
            tools::READ_PTY_OUTPUT,
            Some(json!({
                "pty_session_id": pty_session_id,
                "after_sequence": 0,
                "limit": 10
            })),
        )
        .await?;
        assert_eq!(after_first["count"], json!(0));

        let queued_input = call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id,
                "input": "echo hello\n",
                "requested_by": "agent",
                "idempotency_key": "pty-input-1"
            })),
        )
        .await?;
        assert_eq!(queued_input["input_event"]["state"], json!("queued"));
        assert_eq!(
            queued_input["input_event"]["redacted_input_summary"],
            json!("echo hello\n")
        );
        assert!(queued_input.to_string().contains("echo hello"));
        let retried_input = call_tool(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id,
                "input": "echo hello\n",
                "requested_by": "agent",
                "idempotency_key": "pty-input-1"
            })),
        )
        .await?;
        assert_eq!(
            retried_input["input_event"]["id"],
            queued_input["input_event"]["id"]
        );
        assert_eq!(retried_input["idempotency_reused"], json!(true));
        let mismatched_retry = call_tool_raw(
            fixture.server(),
            tools::QUEUE_PTY_INPUT,
            Some(json!({
                "pty_session_id": pty_session_id,
                "input": "echo world\n",
                "requested_by": "agent",
                "idempotency_key": "pty-input-1"
            })),
        )
        .await?;
        assert_eq!(mismatched_retry.is_error, Some(true));

        let input_events = call_tool(
            fixture.server(),
            tools::LIST_PTY_INPUT_EVENTS,
            Some(json!({
                "pty_session_id": pty_session_id,
                "limit": 10
            })),
        )
        .await?;
        assert_eq!(input_events["count"], json!(1));
        assert_eq!(input_events["input_events"][0]["state"], json!("queued"));
        assert!(input_events.to_string().contains("echo hello"));

        let heartbeat = call_tool(
            fixture.server(),
            tools::HEARTBEAT_PTY_SESSION,
            Some(json!({
                "pty_session_id": pty_session_id,
                "state": "working",
                "foreground_process": "python train.py",
                "cwd": "/srv/app",
                "recent_output_ref": "artifact:latest",
                "last_exit_code": null,
                "input_allowed": true
            })),
        )
        .await?;
        assert_eq!(heartbeat["pty_session"]["state"], json!("working"));
        assert_eq!(
            heartbeat["pty_session"]["foreground_process"],
            json!("python train.py")
        );

        let waited = call_tool(
            fixture.server(),
            tools::WAIT_WORKSPACE_STATE,
            Some(json!({
                "workspace_id": workspace_id,
                "desired_states": ["working"],
                "timeout_ms": 50,
                "poll_interval_ms": 10
            })),
        )
        .await?;
        assert_eq!(waited["matched"], json!(true));

        let closed = call_tool(
            fixture.server(),
            tools::CLOSE_PTY_SESSION,
            Some(json!({
                "pty_session_id": pty_session_id,
                "last_exit_code": 0
            })),
        )
        .await?;
        assert_eq!(closed["pty_session"]["state"], json!("closed"));
        assert_eq!(closed["pty_session"]["input_allowed"], json!(false));
        assert_eq!(closed["pty_session"]["last_exit_code"], json!(0));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_open_pty_creates_resolving_connection_when_none_is_reusable()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let mut failed = fixture
            .repositories
            .connection_sessions
            .get(fixture.session_id)
            .await?
            .ok_or("fixture connection should exist")?;
        failed.state = EntityState::CircuitOpen;
        fixture
            .repositories
            .connection_sessions
            .upsert(&failed)
            .await?;
        let created = call_tool(
            fixture.server(),
            tools::CREATE_WORKSPACE,
            Some(json!({
                "host_id": fixture.host_id.to_string(),
                "access_path_id": fixture.access_path_id.to_string(),
                "label": "first-connect-pty",
                "cwd": "/tmp"
            })),
        )
        .await?;
        let workspace_id = created["workspace"]["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;
        let opened = call_tool(
            fixture.server(),
            tools::OPEN_WORKSPACE_PTY_SESSION,
            Some(json!({"workspace_id": workspace_id, "cwd": "/tmp"})),
        )
        .await?;
        let session_id = opened["pty_session"]["session_id"]
            .as_str()
            .ok_or("connection session id should be a string")?;
        assert_ne!(session_id, fixture.session_id.to_string());
        let session = fixture
            .repositories
            .connection_sessions
            .get(SessionId::from(uuid::Uuid::parse_str(session_id)?))
            .await?
            .ok_or("created connection should exist")?;
        assert_eq!(session.state, EntityState::Resolving);
        assert_eq!(opened["pty_session"]["backend_state"], json!("pending"));
        assert_eq!(opened["backend_ready"], json!(false));
        assert_eq!(
            opened["recommended_action"],
            json!("wait_for_pty_activation")
        );
        assert_eq!(opened["poll_after_ms"], json!(750));
        Ok(())
    }

    #[tokio::test]
    async fn mcp_tool_list_exposes_safety_annotations() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = TestFixture::new().await?;
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);
        let server = fixture.server();
        let server_handle = tokio::spawn(async move {
            let service = server
                .serve(server_transport)
                .await
                .map_err(|error| format!("serve MCP server: {error}"))?;
            service
                .waiting()
                .await
                .map_err(|error| format!("wait MCP server: {error}"))?;
            Ok::<(), String>(())
        });
        let client = ClientInfo::default().serve(client_transport).await?;
        let tools_result = client.peer().list_tools(None).await?;
        let tool = tools_result
            .tools
            .into_iter()
            .find(|tool| tool.name == tools::LIST_HOSTS)
            .ok_or("list hosts tool should be registered")?;
        let annotations = tool.annotations.ok_or("tool annotations should exist")?;
        assert_eq!(annotations.read_only_hint, Some(true));
        assert_eq!(annotations.destructive_hint, Some(false));
        drop(client);
        server_handle.abort();
        Ok(())
    }

    async fn call_tool(
        server: RemoteHostsMcpServer,
        name: &'static str,
        arguments: Option<Value>,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let result = call_tool_raw(server, name, arguments).await?;
        result
            .structured_content
            .clone()
            .ok_or_else(|| format!("expected structured content from {name}: {result:?}").into())
    }

    async fn call_tool_raw(
        server: RemoteHostsMcpServer,
        name: &'static str,
        arguments: Option<Value>,
    ) -> Result<rmcp::model::CallToolResult, Box<dyn std::error::Error>> {
        let (server_transport, client_transport) = tokio::io::duplex(32 * 1024);
        let server_handle = tokio::spawn(async move {
            let service = server
                .serve(server_transport)
                .await
                .map_err(|error| format!("serve MCP server: {error}"))?;
            service
                .waiting()
                .await
                .map_err(|error| format!("wait MCP server: {error}"))?;
            Ok::<(), String>(())
        });
        let client = ClientInfo::default().serve(client_transport).await?;
        let mut request = CallToolRequestParams::new(name);
        if let Some(arguments) = arguments {
            let object = arguments
                .as_object()
                .cloned()
                .ok_or("tool arguments must be a JSON object")?;
            request = request.with_arguments(object);
        }
        let result = client.peer().call_tool(request).await?;
        drop(client);
        server_handle.abort();
        Ok(result)
    }

    struct TestFixture {
        repositories: Repositories,
        host_id: HostId,
        access_path_id: AccessPathId,
        connector_id: ConnectorId,
        session_id: SessionId,
    }

    impl TestFixture {
        #[allow(clippy::too_many_lines)]
        async fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let pool = connect_sqlite("sqlite::memory:").await?;
            migrate(&pool).await?;
            let repositories = Repositories::new(pool);
            let now = now_utc();

            let host = Host {
                id: HostId::new(),
                name: "company-4090-mcp".to_owned(),
                display_name: "Company 4090 MCP".to_owned(),
                kind: HostKind::GpuServer,
                owner: None,
                tags: vec!["gpu".to_owned()],
                description: None,
                risk_level: RiskLevel::Development,
                created_at: now,
                updated_at: now,
            };
            repositories.hosts.insert(&host).await?;
            let environment = Environment {
                id: EnvironmentId::new(),
                name: format!("company-lan-{}", host.id),
                kind: EnvironmentKind::CompanyLan,
                description: None,
                trust_level: TrustLevel::Trusted,
                notes: None,
            };
            repositories.environments.insert(&environment).await?;
            let connector = Connector {
                id: ConnectorId::new(),
                name: format!("office-connector-{}", host.id),
                environment_id: environment.id,
                host_id: None,
                version: "0.1.0".to_owned(),
                state: EntityState::Healthy,
                last_seen_at: Some(now),
                current_network: Some("company".to_owned()),
            };
            repositories.connectors.upsert(&connector).await?;
            let credential_id = CredentialId::new();
            repositories
                .credentials
                .insert(&StoredCredential {
                    metadata: CredentialMetadata {
                        id: credential_id,
                        name: format!("mcp ssh {}", host.id),
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
                address: "10.0.0.30".to_owned(),
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
            repositories
                .access_path_health
                .upsert(&AccessPathHealth {
                    access_path_id: access_path.id,
                    state: EntityState::Healthy,
                    last_checked_at: Some(now),
                    latency_ms: Some(7),
                    failure_count: 0,
                    last_error_code: None,
                    next_retry_at: None,
                })
                .await?;
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

            Ok(Self {
                repositories,
                host_id: host.id,
                access_path_id: access_path.id,
                connector_id: connector.id,
                session_id: session.session_id,
            })
        }

        fn server(&self) -> RemoteHostsMcpServer {
            RemoteHostsMcpServer::new(self.repositories.clone())
        }
    }
}

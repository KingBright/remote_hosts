//! Database access layer and migrations.

use std::{io, str::FromStr};

use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId, AgentWorkspace,
    AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason, AuthorizedKeyBootstrapState,
    ClaimedPtyInputEvent, ConnectionSession, Connector, ConnectorId, CredentialBinding,
    CredentialBindingView, CredentialId, CredentialKind, CredentialMetadata, EntityState,
    Environment, EnvironmentId, Host, HostFact, HostId, HostWriteLease, KnowledgeItem, OperationId,
    OperationOutputArtifact, OperationOutputArtifactId, OperationOutputChunk, OperationRun,
    OperationState, PtyBackendCapabilities, PtyBackendState, PtyInputEvent, PtyInputEventId,
    PtyInputEventState, PtyOutputChunk, PtySession, PtySessionId, SequencedStateEvent, SessionId,
    SoftwareInstall, SshChannelTransportEvidence, SshTransportRuntime, SshTransportRuntimeId,
    SshTransportRuntimeState, StateEvent, StateReasonCode, StoredCredential, TopologyEdge,
    TopologyNode, TopologyNodeId, TopologySyncRun, TopologySyncRunId, WorkspaceId, WorkspaceState,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;
use time::OffsetDateTime;

/// Embedded database migrator.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Database errors.
#[derive(Debug, Error)]
pub enum DbError {
    /// `SQLx` error.
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Migration error.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// JSON error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// UUID parse error.
    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    /// Integer conversion error.
    #[error("integer conversion error: {0}")]
    Int(#[from] std::num::TryFromIntError),
}

/// Creates a `SQLite` pool.
///
/// # Errors
///
/// Returns an error if the database connection cannot be established.
pub async fn connect_sqlite(database_url: &str) -> Result<SqlitePool, DbError> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);

    SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
        .map_err(DbError::from)
}

/// Runs all embedded migrations.
///
/// # Errors
///
/// Returns an error if any migration fails.
pub async fn migrate(pool: &SqlitePool) -> Result<(), DbError> {
    MIGRATOR.run(pool).await.map_err(DbError::from)
}

/// Repository bundle for the main application.
#[derive(Clone)]
pub struct Repositories {
    /// Host repository.
    pub hosts: HostRepository,
    /// Environment repository.
    pub environments: EnvironmentRepository,
    /// Connector repository.
    pub connectors: ConnectorRepository,
    /// Credential repository.
    pub credentials: CredentialRepository,
    /// Access path repository.
    pub access_paths: AccessPathRepository,
    /// Access path health repository.
    pub access_path_health: AccessPathHealthRepository,
    /// Automatic authorized-key bootstrap state repository.
    pub authorized_key_bootstrap: AuthorizedKeyBootstrapRepository,
    /// Host fact repository.
    pub host_facts: HostFactRepository,
    /// Software install repository.
    pub software_installs: SoftwareInstallRepository,
    /// Connection session repository.
    pub connection_sessions: ConnectionSessionRepository,
    /// Connector-local SSH transport runtime repository.
    pub ssh_transport_runtimes: SshTransportRuntimeRepository,
    /// Agent-client session repository.
    pub agent_sessions: AgentSessionRepository,
    /// Agent workspace repository.
    pub workspaces: AgentWorkspaceRepository,
    /// Host-level cross-agent write coordination leases.
    pub host_write_leases: HostWriteLeaseRepository,
    /// PTY session repository.
    pub pty_sessions: PtySessionRepository,
    /// PTY output chunk repository.
    pub pty_output_chunks: PtyOutputChunkRepository,
    /// PTY input event repository.
    pub pty_input_events: PtyInputEventRepository,
    /// Operation repository.
    pub operations: OperationRunRepository,
    /// Operation output chunk repository.
    pub operation_output_chunks: OperationOutputChunkRepository,
    /// Operation output artifact repository.
    pub operation_output_artifacts: OperationOutputArtifactRepository,
    /// Knowledge repository.
    pub knowledge: KnowledgeItemRepository,
    /// State event repository.
    pub state_events: StateEventRepository,
    /// Infrastructure topology graph and snapshot reconciliation repository.
    pub topology: TopologyRepository,
    /// Links topology resources to encrypted credentials.
    pub credential_bindings: CredentialBindingRepository,
}

impl Repositories {
    /// Builds all repositories for a pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            hosts: HostRepository::new(pool.clone()),
            environments: EnvironmentRepository::new(pool.clone()),
            connectors: ConnectorRepository::new(pool.clone()),
            credentials: CredentialRepository::new(pool.clone()),
            access_paths: AccessPathRepository::new(pool.clone()),
            access_path_health: AccessPathHealthRepository::new(pool.clone()),
            authorized_key_bootstrap: AuthorizedKeyBootstrapRepository::new(pool.clone()),
            host_facts: HostFactRepository::new(pool.clone()),
            software_installs: SoftwareInstallRepository::new(pool.clone()),
            connection_sessions: ConnectionSessionRepository::new(pool.clone()),
            ssh_transport_runtimes: SshTransportRuntimeRepository::new(pool.clone()),
            agent_sessions: AgentSessionRepository::new(pool.clone()),
            workspaces: AgentWorkspaceRepository::new(pool.clone()),
            host_write_leases: HostWriteLeaseRepository::new(pool.clone()),
            pty_sessions: PtySessionRepository::new(pool.clone()),
            pty_output_chunks: PtyOutputChunkRepository::new(pool.clone()),
            pty_input_events: PtyInputEventRepository::new(pool.clone()),
            operations: OperationRunRepository::new(pool.clone()),
            operation_output_chunks: OperationOutputChunkRepository::new(pool.clone()),
            operation_output_artifacts: OperationOutputArtifactRepository::new(pool.clone()),
            knowledge: KnowledgeItemRepository::new(pool.clone()),
            state_events: StateEventRepository::new(pool.clone()),
            topology: TopologyRepository::new(pool.clone()),
            credential_bindings: CredentialBindingRepository::new(pool),
        }
    }
}

macro_rules! repo {
    ($name:ident) => {
        #[doc = concat!(stringify!($name), " backed by `SQLx`.")]
        #[derive(Clone)]
        pub struct $name {
            pool: SqlitePool,
        }

        impl $name {
            /// Creates a repository.
            pub fn new(pool: SqlitePool) -> Self {
                Self { pool }
            }
        }
    };
}

repo!(HostRepository);
repo!(EnvironmentRepository);
repo!(ConnectorRepository);
repo!(CredentialRepository);
repo!(AccessPathRepository);
repo!(AccessPathHealthRepository);
repo!(AuthorizedKeyBootstrapRepository);
repo!(HostFactRepository);
repo!(SoftwareInstallRepository);
repo!(ConnectionSessionRepository);
repo!(SshTransportRuntimeRepository);
repo!(AgentSessionRepository);
repo!(AgentWorkspaceRepository);
repo!(HostWriteLeaseRepository);
repo!(PtySessionRepository);
repo!(PtyOutputChunkRepository);
repo!(PtyInputEventRepository);
repo!(OperationRunRepository);
repo!(OperationOutputChunkRepository);
repo!(OperationOutputArtifactRepository);
repo!(KnowledgeItemRepository);
repo!(StateEventRepository);
repo!(TopologyRepository);
repo!(CredentialBindingRepository);

/// Update used to finish an operation while the connector still owns the claim.
#[derive(Clone, Debug)]
pub struct ClaimedOperationFinish<'a> {
    /// Operation id.
    pub id: OperationId,
    /// Claim token held by the connector.
    pub claim_token: &'a str,
    /// Final operation state.
    pub state: OperationState,
    /// Finish timestamp.
    pub finished_at: OffsetDateTime,
    /// Exit code, when available.
    pub exit_code: Option<i32>,
    /// Redacted output summary.
    pub redacted_output_summary: Option<&'a str>,
    /// Redacted final error.
    pub last_error: Option<&'a str>,
}

impl HostRepository {
    /// Inserts a host.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, host: &Host) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO hosts (
                id, name, display_name, kind_json, owner, tags_json, description,
                risk_level_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(host.id.to_string())
        .bind(&host.name)
        .bind(&host.display_name)
        .bind(to_json(&host.kind)?)
        .bind(&host.owner)
        .bind(to_json(&host.tags)?)
        .bind(&host.description)
        .bind(to_json(&host.risk_level)?)
        .bind(host.created_at)
        .bind(host.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserts or updates a host by id.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, host: &Host) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO hosts (
                id, name, display_name, kind_json, owner, tags_json, description,
                risk_level_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                display_name = excluded.display_name,
                kind_json = excluded.kind_json,
                owner = excluded.owner,
                tags_json = excluded.tags_json,
                description = excluded.description,
                risk_level_json = excluded.risk_level_json,
                updated_at = excluded.updated_at
            ",
        )
        .bind(host.id.to_string())
        .bind(&host.name)
        .bind(&host.display_name)
        .bind(to_json(&host.kind)?)
        .bind(&host.owner)
        .bind(to_json(&host.tags)?)
        .bind(&host.description)
        .bind(to_json(&host.risk_level)?)
        .bind(host.created_at)
        .bind(host.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Gets a host by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: HostId) -> Result<Option<Host>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, display_name, kind_json, owner, tags_json, description,
                   risk_level_json, created_at, updated_at
            FROM hosts
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_host).transpose()
    }

    /// Gets a host by stable name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<Host>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, display_name, kind_json, owner, tags_json, description,
                   risk_level_json, created_at, updated_at
            FROM hosts
            WHERE name = ?
            ",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_host).transpose()
    }

    /// Lists all hosts ordered by name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list(&self) -> Result<Vec<Host>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, name, display_name, kind_json, owner, tags_json, description,
                   risk_level_json, created_at, updated_at
            FROM hosts
            ORDER BY name
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_host).collect()
    }
}

impl EnvironmentRepository {
    /// Inserts an environment.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, environment: &Environment) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO environments (id, name, kind_json, description, trust_level_json, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(environment.id.to_string())
        .bind(&environment.name)
        .bind(to_json(&environment.kind)?)
        .bind(&environment.description)
        .bind(to_json(&environment.trust_level)?)
        .bind(&environment.notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts or updates an environment by id.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, environment: &Environment) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO environments (id, name, kind_json, description, trust_level_json, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                kind_json = excluded.kind_json,
                description = excluded.description,
                trust_level_json = excluded.trust_level_json,
                notes = excluded.notes
            ",
        )
        .bind(environment.id.to_string())
        .bind(&environment.name)
        .bind(to_json(&environment.kind)?)
        .bind(&environment.description)
        .bind(to_json(&environment.trust_level)?)
        .bind(&environment.notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets an environment by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: EnvironmentId) -> Result<Option<Environment>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, kind_json, description, trust_level_json, notes
            FROM environments
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_environment).transpose()
    }

    /// Gets an environment by name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<Environment>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, kind_json, description, trust_level_json, notes
            FROM environments
            WHERE name = ?
            ",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_environment).transpose()
    }

    /// Lists environments ordered by name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list(&self) -> Result<Vec<Environment>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, name, kind_json, description, trust_level_json, notes
            FROM environments
            ORDER BY name
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_environment).collect()
    }
}

impl ConnectorRepository {
    /// Inserts or replaces a connector.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, connector: &Connector) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO connectors (
                id, name, environment_id, host_id, version, state_json, last_seen_at, current_network
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                environment_id = excluded.environment_id,
                host_id = excluded.host_id,
                version = excluded.version,
                state_json = excluded.state_json,
                last_seen_at = excluded.last_seen_at,
                current_network = excluded.current_network
            ",
        )
        .bind(connector.id.to_string())
        .bind(&connector.name)
        .bind(connector.environment_id.to_string())
        .bind(connector.host_id.map(|id| id.to_string()))
        .bind(&connector.version)
        .bind(to_json(&connector.state)?)
        .bind(connector.last_seen_at)
        .bind(&connector.current_network)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets a connector by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: ConnectorId) -> Result<Option<Connector>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, environment_id, host_id, version, state_json, last_seen_at, current_network
            FROM connectors
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_connector).transpose()
    }

    /// Lists connectors ordered by name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list(&self) -> Result<Vec<Connector>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, name, environment_id, host_id, version, state_json, last_seen_at, current_network
            FROM connectors
            ORDER BY name
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_connector).collect()
    }

    /// Updates connector heartbeat fields and returns the previous state plus updated connector.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn update_heartbeat(
        &self,
        id: ConnectorId,
        state: EntityState,
        version: Option<&str>,
        current_network: Option<&str>,
        last_seen_at: OffsetDateTime,
    ) -> Result<Option<(EntityState, Connector)>, DbError> {
        let Some(mut connector) = self.get(id).await? else {
            return Ok(None);
        };

        let old_state = connector.state.clone();
        connector.state = state;
        connector.last_seen_at = Some(last_seen_at);
        if let Some(version) = version {
            connector.version = version.to_owned();
        }
        if let Some(current_network) = current_network {
            connector.current_network = Some(current_network.to_owned());
        }

        self.upsert(&connector).await?;
        Ok(Some((old_state, connector)))
    }
}

impl CredentialRepository {
    /// Inserts a stored encrypted credential.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, credential: &StoredCredential) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO credentials (
                id, name, kind_json, username_hint, encrypted_blob_json,
                created_at, updated_at, last_used_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(credential.metadata.id.to_string())
        .bind(&credential.metadata.name)
        .bind(to_json(&credential.metadata.kind)?)
        .bind(&credential.metadata.username_hint)
        .bind(to_json(&credential.encrypted_blob_json)?)
        .bind(credential.metadata.created_at)
        .bind(credential.metadata.updated_at)
        .bind(credential.metadata.last_used_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts or updates a stored credential by id.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, credential: &StoredCredential) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO credentials (
                id, name, kind_json, username_hint, encrypted_blob_json,
                created_at, updated_at, last_used_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                kind_json = excluded.kind_json,
                username_hint = excluded.username_hint,
                encrypted_blob_json = excluded.encrypted_blob_json,
                updated_at = excluded.updated_at,
                last_used_at = excluded.last_used_at
            ",
        )
        .bind(credential.metadata.id.to_string())
        .bind(&credential.metadata.name)
        .bind(to_json(&credential.metadata.kind)?)
        .bind(&credential.metadata.username_hint)
        .bind(to_json(&credential.encrypted_blob_json)?)
        .bind(credential.metadata.created_at)
        .bind(credential.metadata.updated_at)
        .bind(credential.metadata.last_used_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets a stored credential by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: CredentialId) -> Result<Option<StoredCredential>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, kind_json, username_hint, encrypted_blob_json,
                   created_at, updated_at, last_used_at
            FROM credentials
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_stored_credential).transpose()
    }

    /// Gets a stored credential by name.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<StoredCredential>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, name, kind_json, username_hint, encrypted_blob_json,
                   created_at, updated_at, last_used_at
            FROM credentials
            WHERE name = ?
            ",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_stored_credential).transpose()
    }

    /// Lists credential metadata without encrypted blobs.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_metadata(&self) -> Result<Vec<CredentialMetadata>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, name, kind_json, username_hint, created_at, updated_at, last_used_at
            FROM credentials
            ORDER BY name
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_credential_metadata).collect()
    }
}

impl AccessPathRepository {
    /// Inserts an access path.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, path: &AccessPath) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO access_paths (
                id, host_id, environment_id, connector_id, protocol_json, address, port, username,
                credential_id, route_type_json, proxy_chain_json, priority, enabled,
                connection_mode_json, idle_ttl_seconds, keepalive_seconds, max_concurrent_channels,
                max_new_connections_per_minute, requires_tty, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(path.id.to_string())
        .bind(path.host_id.to_string())
        .bind(path.environment_id.to_string())
        .bind(path.connector_id.map(|id| id.to_string()))
        .bind(to_json(&path.protocol)?)
        .bind(&path.address)
        .bind(i64::from(path.port))
        .bind(&path.username)
        .bind(path.credential_id.to_string())
        .bind(to_json(&path.route_type)?)
        .bind(to_json(&path.proxy_chain)?)
        .bind(path.priority)
        .bind(bool_to_i64(path.enabled))
        .bind(to_json(&path.connection_mode)?)
        .bind(u64_to_i64(path.idle_ttl_seconds)?)
        .bind(u64_to_i64(path.keepalive_seconds)?)
        .bind(i64::from(path.max_concurrent_channels))
        .bind(i64::from(path.max_new_connections_per_minute))
        .bind(bool_to_i64(path.requires_tty))
        .bind(&path.notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts or updates an access path by id.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, path: &AccessPath) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO access_paths (
                id, host_id, environment_id, connector_id, protocol_json, address, port, username,
                credential_id, route_type_json, proxy_chain_json, priority, enabled,
                connection_mode_json, idle_ttl_seconds, keepalive_seconds, max_concurrent_channels,
                max_new_connections_per_minute, requires_tty, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                host_id = excluded.host_id,
                environment_id = excluded.environment_id,
                connector_id = excluded.connector_id,
                protocol_json = excluded.protocol_json,
                address = excluded.address,
                port = excluded.port,
                username = excluded.username,
                credential_id = excluded.credential_id,
                route_type_json = excluded.route_type_json,
                proxy_chain_json = excluded.proxy_chain_json,
                priority = excluded.priority,
                enabled = excluded.enabled,
                connection_mode_json = excluded.connection_mode_json,
                idle_ttl_seconds = excluded.idle_ttl_seconds,
                keepalive_seconds = excluded.keepalive_seconds,
                max_concurrent_channels = excluded.max_concurrent_channels,
                max_new_connections_per_minute = excluded.max_new_connections_per_minute,
                requires_tty = excluded.requires_tty,
                notes = excluded.notes
            ",
        )
        .bind(path.id.to_string())
        .bind(path.host_id.to_string())
        .bind(path.environment_id.to_string())
        .bind(path.connector_id.map(|id| id.to_string()))
        .bind(to_json(&path.protocol)?)
        .bind(&path.address)
        .bind(i64::from(path.port))
        .bind(&path.username)
        .bind(path.credential_id.to_string())
        .bind(to_json(&path.route_type)?)
        .bind(to_json(&path.proxy_chain)?)
        .bind(path.priority)
        .bind(bool_to_i64(path.enabled))
        .bind(to_json(&path.connection_mode)?)
        .bind(u64_to_i64(path.idle_ttl_seconds)?)
        .bind(u64_to_i64(path.keepalive_seconds)?)
        .bind(i64::from(path.max_concurrent_channels))
        .bind(i64::from(path.max_new_connections_per_minute))
        .bind(bool_to_i64(path.requires_tty))
        .bind(&path.notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets an access path by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: AccessPathId) -> Result<Option<AccessPath>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM access_paths
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_access_path).transpose()
    }

    /// Lists enabled access paths for a host ordered by priority.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_enabled_for_host(&self, host_id: HostId) -> Result<Vec<AccessPath>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM access_paths
            WHERE host_id = ? AND enabled = 1
            ORDER BY priority ASC, id ASC
            ",
        )
        .bind(host_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_access_path).collect()
    }

    /// Lists all access paths for a host ordered by priority.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_host(&self, host_id: HostId) -> Result<Vec<AccessPath>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM access_paths
            WHERE host_id = ?
            ORDER BY priority ASC, id ASC
            ",
        )
        .bind(host_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_access_path).collect()
    }
}

impl AccessPathHealthRepository {
    /// Upserts access path health.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, health: &AccessPathHealth) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO access_path_health (
                access_path_id, state_json, last_checked_at, latency_ms, failure_count,
                last_error_code_json, next_retry_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(access_path_id) DO UPDATE SET
                state_json = excluded.state_json,
                last_checked_at = excluded.last_checked_at,
                latency_ms = excluded.latency_ms,
                failure_count = excluded.failure_count,
                last_error_code_json = excluded.last_error_code_json,
                next_retry_at = excluded.next_retry_at
            ",
        )
        .bind(health.access_path_id.to_string())
        .bind(to_json(&health.state)?)
        .bind(health.last_checked_at)
        .bind(health.latency_ms.map(u64_to_i64).transpose()?)
        .bind(u32_to_i64(health.failure_count))
        .bind(optional_json(health.last_error_code.as_ref())?)
        .bind(health.next_retry_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets health by access path id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: AccessPathId) -> Result<Option<AccessPathHealth>, DbError> {
        let row = sqlx::query(
            r"
            SELECT access_path_id, state_json, last_checked_at, latency_ms, failure_count,
                   last_error_code_json, next_retry_at
            FROM access_path_health
            WHERE access_path_id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_access_path_health).transpose()
    }
}

impl AuthorizedKeyBootstrapRepository {
    /// Inserts or updates automatic public-key bootstrap state for one access path.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, state: &AuthorizedKeyBootstrap) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO authorized_key_bootstrap (
                access_path_id, state_json, reason_json, public_key_fingerprint,
                failure_count, attempted_at, next_retry_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(access_path_id) DO UPDATE SET
                state_json = excluded.state_json,
                reason_json = excluded.reason_json,
                public_key_fingerprint = excluded.public_key_fingerprint,
                failure_count = excluded.failure_count,
                attempted_at = excluded.attempted_at,
                next_retry_at = excluded.next_retry_at,
                updated_at = excluded.updated_at
            ",
        )
        .bind(state.access_path_id.to_string())
        .bind(to_json(&state.state)?)
        .bind(optional_json(state.reason.as_ref())?)
        .bind(&state.public_key_fingerprint)
        .bind(u32_to_i64(state.failure_count))
        .bind(state.attempted_at)
        .bind(state.next_retry_at)
        .bind(state.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets automatic public-key bootstrap state by access path id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(
        &self,
        access_path_id: AccessPathId,
    ) -> Result<Option<AuthorizedKeyBootstrap>, DbError> {
        let row = sqlx::query(
            r"
            SELECT access_path_id, state_json, reason_json, public_key_fingerprint,
                   failure_count, attempted_at, next_retry_at, updated_at
            FROM authorized_key_bootstrap
            WHERE access_path_id = ?
            ",
        )
        .bind(access_path_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(row_to_authorized_key_bootstrap)
            .transpose()
    }
}

impl HostFactRepository {
    /// Inserts a host fact.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, fact: &HostFact) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO host_facts (
                id, host_id, namespace, key, value_json, source_json,
                observed_at, expires_at, confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(fact.id.to_string())
        .bind(fact.host_id.to_string())
        .bind(&fact.namespace)
        .bind(&fact.key)
        .bind(to_json(&fact.value_json)?)
        .bind(to_json(&fact.source)?)
        .bind(fact.observed_at)
        .bind(fact.expires_at)
        .bind(f64::from(fact.confidence))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists facts for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_host(&self, host_id: HostId) -> Result<Vec<HostFact>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, host_id, namespace, key, value_json, source_json,
                   observed_at, expires_at, confidence
            FROM host_facts
            WHERE host_id = ?
            ORDER BY namespace, key, observed_at DESC
            ",
        )
        .bind(host_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_host_fact).collect()
    }
}

impl SoftwareInstallRepository {
    /// Inserts a software install record.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, install: &SoftwareInstall) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO software_installs (
                id, host_id, name, version, install_path, config_paths_json,
                service_names_json, ports_json, installed_by_operation_id, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(install.id.to_string())
        .bind(install.host_id.to_string())
        .bind(&install.name)
        .bind(&install.version)
        .bind(&install.install_path)
        .bind(to_json(&install.config_paths)?)
        .bind(to_json(&install.service_names)?)
        .bind(to_json(&install.ports)?)
        .bind(install.installed_by_operation_id.map(|id| id.to_string()))
        .bind(&install.notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl ConnectionSessionRepository {
    /// Upserts a connection session.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, session: &ConnectionSession) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO connection_sessions (
                session_id, access_path_id, connector_id, state_json, created_at, last_used_at,
                open_channels, reused_count, failure_count, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                state_json = excluded.state_json,
                last_used_at = excluded.last_used_at,
                open_channels = excluded.open_channels,
                reused_count = excluded.reused_count,
                failure_count = excluded.failure_count,
                last_error = excluded.last_error
            ",
        )
        .bind(session.session_id.to_string())
        .bind(session.access_path_id.to_string())
        .bind(session.connector_id.to_string())
        .bind(to_json(&session.state)?)
        .bind(session.created_at)
        .bind(session.last_used_at)
        .bind(u32_to_i64(session.open_channels))
        .bind(u64_to_i64(session.reused_count)?)
        .bind(u32_to_i64(session.failure_count))
        .bind(&session.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets a connection session by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: SessionId) -> Result<Option<ConnectionSession>, DbError> {
        let row = sqlx::query(
            r"
            SELECT session_id, access_path_id, connector_id, state_json, created_at, last_used_at,
                   open_channels, reused_count, failure_count, last_error
            FROM connection_sessions
            WHERE session_id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_connection_session).transpose()
    }

    /// Finds the newest reusable logical session for an access path and connector.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn find_reusable(
        &self,
        access_path_id: AccessPathId,
        connector_id: ConnectorId,
    ) -> Result<Option<ConnectionSession>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT session_id, access_path_id, connector_id, state_json, created_at, last_used_at,
                   open_channels, reused_count, failure_count, last_error
            FROM connection_sessions
            WHERE access_path_id = ? AND connector_id = ?
            ORDER BY last_used_at DESC, session_id ASC
            ",
        )
        .bind(access_path_id.to_string())
        .bind(connector_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        let sessions = rows
            .iter()
            .map(row_to_connection_session)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(sessions.into_iter().find(|session| {
            matches!(
                session.state,
                EntityState::Connected | EntityState::Healthy | EntityState::Resolving
            )
        }))
    }

    /// Marks connector-local SSH sessions unavailable after the owning connector restarts.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn mark_runtime_lost_for_connector(
        &self,
        connector_id: ConnectorId,
        observed_at: OffsetDateTime,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            r"
            UPDATE connection_sessions
            SET state_json = ?,
                last_used_at = ?,
                open_channels = 0,
                last_error = ?
            WHERE connector_id = ?
              AND (
                state_json IN (?, ?, ?)
                OR open_channels > 0
              )
            ",
        )
        .bind(to_json(&EntityState::Unknown)?)
        .bind(observed_at)
        .bind("connector runtime restarted; SSH transport continuity was lost")
        .bind(connector_id.to_string())
        .bind(to_json(&EntityState::Resolving)?)
        .bind(to_json(&EntityState::Connected)?)
        .bind(to_json(&EntityState::Healthy)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Lists connection sessions for a host through its access paths.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_host(&self, host_id: HostId) -> Result<Vec<ConnectionSession>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT cs.*
            FROM connection_sessions cs
            JOIN access_paths ap ON ap.id = cs.access_path_id
            WHERE ap.host_id = ?
            ORDER BY cs.last_used_at DESC, cs.session_id ASC
            ",
        )
        .bind(host_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_connection_session).collect()
    }
}

impl SshTransportRuntimeRepository {
    /// Upserts the latest connector-local SSH transport telemetry for one access path.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, runtime: &SshTransportRuntime) -> Result<(), DbError> {
        let telemetry = &runtime.telemetry;
        sqlx::query(
            r"
            INSERT INTO ssh_transport_runtimes (
                access_path_id, connector_id, runtime_id, backend_json, state_json, generation,
                connection_attempt_count, successful_handshake_count, reuse_count,
                last_handshake_at, last_validated_at, capabilities_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(access_path_id, connector_id) DO UPDATE SET
                runtime_id = excluded.runtime_id,
                backend_json = excluded.backend_json,
                state_json = excluded.state_json,
                generation = excluded.generation,
                connection_attempt_count = excluded.connection_attempt_count,
                successful_handshake_count = excluded.successful_handshake_count,
                reuse_count = excluded.reuse_count,
                last_handshake_at = excluded.last_handshake_at,
                last_validated_at = excluded.last_validated_at,
                capabilities_json = excluded.capabilities_json,
                updated_at = excluded.updated_at
            ",
        )
        .bind(runtime.access_path_id.to_string())
        .bind(runtime.connector_id.to_string())
        .bind(telemetry.runtime_id.to_string())
        .bind(to_json(&telemetry.backend)?)
        .bind(to_json(&telemetry.state)?)
        .bind(u64_to_i64(telemetry.generation)?)
        .bind(u64_to_i64(telemetry.connection_attempt_count)?)
        .bind(u64_to_i64(telemetry.successful_handshake_count)?)
        .bind(u64_to_i64(telemetry.reuse_count)?)
        .bind(telemetry.last_handshake_at)
        .bind(telemetry.last_validated_at)
        .bind(to_json(&telemetry.capabilities)?)
        .bind(runtime.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets the latest transport runtime for an access path and connector.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(
        &self,
        access_path_id: AccessPathId,
        connector_id: ConnectorId,
    ) -> Result<Option<SshTransportRuntime>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM ssh_transport_runtimes
            WHERE access_path_id = ? AND connector_id = ?
            ",
        )
        .bind(access_path_id.to_string())
        .bind(connector_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_ssh_transport_runtime).transpose()
    }

    /// Marks all in-memory transport runtimes lost when their connector process restarts.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn mark_runtime_lost_for_connector(
        &self,
        connector_id: ConnectorId,
        observed_at: OffsetDateTime,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            r"
            UPDATE ssh_transport_runtimes
            SET state_json = ?, updated_at = ?
            WHERE connector_id = ? AND state_json != ?
            ",
        )
        .bind(to_json(&SshTransportRuntimeState::RuntimeLost)?)
        .bind(observed_at)
        .bind(connector_id.to_string())
        .bind(to_json(&SshTransportRuntimeState::RuntimeLost)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

impl AgentSessionRepository {
    /// Inserts or refreshes one agent-client session.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the write.
    pub async fn upsert(&self, session: &AgentSession) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO agent_sessions (
                id, client_kind, client_instance_id, project_key, conversation_key,
                state_json, created_at, last_seen_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                client_kind = excluded.client_kind,
                client_instance_id = excluded.client_instance_id,
                project_key = excluded.project_key,
                conversation_key = excluded.conversation_key,
                state_json = excluded.state_json,
                last_seen_at = excluded.last_seen_at,
                expires_at = excluded.expires_at
            ",
        )
        .bind(session.id.to_string())
        .bind(&session.client_kind)
        .bind(&session.client_instance_id)
        .bind(&session.project_key)
        .bind(&session.conversation_key)
        .bind(to_json(&session.state)?)
        .bind(session.created_at)
        .bind(session.last_seen_at)
        .bind(session.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets one agent-client session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: AgentSessionId) -> Result<Option<AgentSession>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, client_kind, client_instance_id, project_key, conversation_key,
                   state_json, created_at, last_seen_at, expires_at
            FROM agent_sessions
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_agent_session).transpose()
    }
}

impl AgentWorkspaceRepository {
    /// Inserts an agent workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, workspace: &AgentWorkspace) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO agent_workspaces (
                workspace_id, agent_session_id, host_id, access_path_id, connector_id, label, cwd,
                state_json, policy_profile, created_at, last_activity_at, ttl_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(workspace.id.to_string())
        .bind(workspace.agent_session_id.map(|id| id.to_string()))
        .bind(workspace.host_id.to_string())
        .bind(workspace.access_path_id.to_string())
        .bind(workspace.connector_id.to_string())
        .bind(&workspace.label)
        .bind(&workspace.cwd)
        .bind(to_json(&workspace.state)?)
        .bind(&workspace.policy_profile)
        .bind(workspace.created_at)
        .bind(workspace.last_activity_at)
        .bind(u64_to_i64(workspace.ttl_seconds)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts or updates an agent workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, workspace: &AgentWorkspace) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO agent_workspaces (
                workspace_id, agent_session_id, host_id, access_path_id, connector_id, label, cwd,
                state_json, policy_profile, created_at, last_activity_at, ttl_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
                agent_session_id = excluded.agent_session_id,
                label = excluded.label,
                cwd = excluded.cwd,
                state_json = excluded.state_json,
                policy_profile = excluded.policy_profile,
                last_activity_at = excluded.last_activity_at,
                ttl_seconds = excluded.ttl_seconds
            ",
        )
        .bind(workspace.id.to_string())
        .bind(workspace.agent_session_id.map(|id| id.to_string()))
        .bind(workspace.host_id.to_string())
        .bind(workspace.access_path_id.to_string())
        .bind(workspace.connector_id.to_string())
        .bind(&workspace.label)
        .bind(&workspace.cwd)
        .bind(to_json(&workspace.state)?)
        .bind(&workspace.policy_profile)
        .bind(workspace.created_at)
        .bind(workspace.last_activity_at)
        .bind(u64_to_i64(workspace.ttl_seconds)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets a workspace by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: WorkspaceId) -> Result<Option<AgentWorkspace>, DbError> {
        let row = sqlx::query(
            r"
            SELECT workspace_id, agent_session_id, host_id, access_path_id, connector_id, label,
                   cwd, state_json, policy_profile, created_at, last_activity_at, ttl_seconds
            FROM agent_workspaces
            WHERE workspace_id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_agent_workspace).transpose()
    }

    /// Lists workspaces for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_host(&self, host_id: HostId) -> Result<Vec<AgentWorkspace>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT workspace_id, agent_session_id, host_id, access_path_id, connector_id, label,
                   cwd, state_json, policy_profile, created_at, last_activity_at, ttl_seconds
            FROM agent_workspaces
            WHERE host_id = ?
            ORDER BY last_activity_at DESC
            ",
        )
        .bind(host_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_agent_workspace).collect()
    }

    /// Lists workspaces owned by one agent session for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_host_and_agent_session(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
    ) -> Result<Vec<AgentWorkspace>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT workspace_id, agent_session_id, host_id, access_path_id, connector_id, label,
                   cwd, state_json, policy_profile, created_at, last_activity_at, ttl_seconds
            FROM agent_workspaces
            WHERE host_id = ? AND agent_session_id = ?
            ORDER BY last_activity_at DESC
            ",
        )
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_agent_workspace).collect()
    }

    /// Updates workspace state and returns the updated workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn update_state(
        &self,
        id: WorkspaceId,
        state: WorkspaceState,
        last_activity_at: OffsetDateTime,
    ) -> Result<Option<AgentWorkspace>, DbError> {
        let Some(mut workspace) = self.get(id).await? else {
            return Ok(None);
        };
        workspace.state = state;
        workspace.last_activity_at = last_activity_at;
        self.upsert(&workspace).await?;
        Ok(Some(workspace))
    }
}

impl HostWriteLeaseRepository {
    /// Acquires or refreshes a host write lease when it is free, expired, or already held by the
    /// same agent session.
    ///
    /// Returns `None` when another live agent session owns the lease.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or decoding fails.
    pub async fn try_acquire(
        &self,
        lease: &HostWriteLease,
        observed_at: OffsetDateTime,
    ) -> Result<Option<HostWriteLease>, DbError> {
        let row = sqlx::query(
            r"
            INSERT INTO host_write_leases (
                host_id, holder_agent_session_id, holder_workspace_id,
                acquired_at, heartbeat_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id) DO UPDATE SET
                holder_agent_session_id = excluded.holder_agent_session_id,
                holder_workspace_id = excluded.holder_workspace_id,
                acquired_at = excluded.acquired_at,
                heartbeat_at = excluded.heartbeat_at,
                expires_at = excluded.expires_at
            WHERE host_write_leases.holder_agent_session_id = excluded.holder_agent_session_id
               OR host_write_leases.expires_at <= ?
            RETURNING *
            ",
        )
        .bind(lease.host_id.to_string())
        .bind(lease.holder_agent_session_id.to_string())
        .bind(lease.holder_workspace_id.to_string())
        .bind(lease.acquired_at)
        .bind(lease.heartbeat_at)
        .bind(lease.expires_at)
        .bind(observed_at)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_host_write_lease).transpose()
    }

    /// Gets the active write lease for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or decoding fails.
    pub async fn get_active(
        &self,
        host_id: HostId,
        observed_at: OffsetDateTime,
    ) -> Result<Option<HostWriteLease>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM host_write_leases
            WHERE host_id = ? AND expires_at > ?
            ",
        )
        .bind(host_id.to_string())
        .bind(observed_at)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_host_write_lease).transpose()
    }

    /// Renews a lease only while the expected agent session still owns it.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn renew(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
        workspace_id: WorkspaceId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE host_write_leases
            SET holder_workspace_id = ?, heartbeat_at = ?, expires_at = ?
            WHERE host_id = ? AND holder_agent_session_id = ?
            ",
        )
        .bind(workspace_id.to_string())
        .bind(heartbeat_at)
        .bind(expires_at)
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Shortens an owned lease to a bounded handoff grace period.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn shorten(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE host_write_leases
            SET heartbeat_at = ?, expires_at = MIN(expires_at, ?)
            WHERE host_id = ? AND holder_agent_session_id = ?
            ",
        )
        .bind(heartbeat_at)
        .bind(expires_at)
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Returns whether the holder still has queued or running mutating work.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn has_pending_write_work(
        &self,
        host_id: HostId,
        agent_session_id: AgentSessionId,
    ) -> Result<bool, DbError> {
        let count: i64 = sqlx::query_scalar(
            r"
            SELECT
                (
                    SELECT COUNT(*)
                    FROM operation_runs
                    WHERE host_id = ?
                      AND agent_session_id = ?
                      AND requires_write_lease = 1
                      AND state_json IN (?, ?)
                )
                +
                (
                    SELECT COUNT(*)
                    FROM pty_input_events
                    WHERE host_id = ?
                      AND agent_session_id = ?
                      AND state_json IN (?, ?)
                )
            ",
        )
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .bind(to_json(&OperationState::Queued)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .bind(to_json(&PtyInputEventState::Queued)?)
        .bind(to_json(&PtyInputEventState::Claimed)?)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }
}

impl PtySessionRepository {
    /// Upserts a PTY session.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the upsert.
    pub async fn upsert(&self, pty: &PtySession) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO pty_sessions (
                pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pty_session_id) DO UPDATE SET
                state_json = excluded.state_json,
                foreground_process = excluded.foreground_process,
                cwd = excluded.cwd,
                recent_output_ref = excluded.recent_output_ref,
                last_exit_code = excluded.last_exit_code,
                input_allowed = excluded.input_allowed,
                backend_state_json = excluded.backend_state_json,
                backend_capabilities_json = excluded.backend_capabilities_json,
                transport_evidence_json = excluded.transport_evidence_json,
                last_activity_at = excluded.last_activity_at
            ",
        )
        .bind(pty.pty_session_id.to_string())
        .bind(pty.workspace_id.to_string())
        .bind(pty.session_id.to_string())
        .bind(to_json(&pty.state)?)
        .bind(&pty.foreground_process)
        .bind(&pty.cwd)
        .bind(&pty.recent_output_ref)
        .bind(pty.last_exit_code)
        .bind(bool_to_i64(pty.input_allowed))
        .bind(to_json(&pty.backend_state)?)
        .bind(to_json(&pty.backend_capabilities)?)
        .bind(optional_json(pty.transport_evidence.as_ref())?)
        .bind(pty.created_at)
        .bind(pty.last_activity_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets a PTY session by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: PtySessionId) -> Result<Option<PtySession>, DbError> {
        let row = sqlx::query(
            r"
            SELECT pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                   recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                   backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            FROM pty_sessions
            WHERE pty_session_id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_session).transpose()
    }

    /// Updates the activity timestamp only while the PTY backend is still active.
    ///
    /// Returns `false` when the PTY has already closed or failed, which prevents late buffered
    /// output from reviving its host write lease.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn touch_activity_if_active(
        &self,
        id: PtySessionId,
        last_activity_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE pty_sessions
            SET last_activity_at = ?
            WHERE pty_session_id = ?
              AND backend_state_json = ?
              AND input_allowed = 1
            ",
        )
        .bind(last_activity_at)
        .bind(id.to_string())
        .bind(to_json(&PtyBackendState::Active)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Lists PTY sessions for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<PtySession>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                   recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                   backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            FROM pty_sessions
            WHERE workspace_id = ?
            ORDER BY last_activity_at DESC, pty_session_id ASC
            ",
        )
        .bind(workspace_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
    }

    /// Returns the oldest connector-owned PTY waiting for backend activation.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn next_pending_for_connector(
        &self,
        connector_id: ConnectorId,
    ) -> Result<Option<PtySession>, DbError> {
        let row = sqlx::query(
            r"
            SELECT ps.pty_session_id, ps.workspace_id, ps.session_id, ps.state_json,
                   ps.foreground_process, ps.cwd, ps.recent_output_ref, ps.last_exit_code,
                   ps.input_allowed, ps.backend_state_json, ps.backend_capabilities_json,
                   ps.transport_evidence_json, ps.created_at, ps.last_activity_at
            FROM pty_sessions ps
            JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
            WHERE aw.connector_id = ?
              AND ps.backend_state_json = ?
              AND ps.input_allowed = 1
              AND aw.state_json IN (?, ?)
              AND ps.state_json IN (?, ?)
            ORDER BY ps.created_at ASC, ps.pty_session_id ASC
            LIMIT 1
            ",
        )
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_session).transpose()
    }

    /// Counts active PTY sessions for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn count_active_for_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<u32, DbError> {
        let count: i64 = sqlx::query_scalar(
            r"
            SELECT COUNT(*)
            FROM pty_sessions
            WHERE workspace_id = ?
              AND state_json != ?
            ",
        )
        .bind(workspace_id.to_string())
        .bind(to_json(&WorkspaceState::Closed)?)
        .fetch_one(&self.pool)
        .await?;
        i64_to_u32(count)
    }

    /// Marks connector-owned PTY backends as lost after a connector runtime restart.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn mark_active_backends_lost_for_connector(
        &self,
        connector_id: ConnectorId,
        observed_at: OffsetDateTime,
    ) -> Result<Vec<PtySession>, DbError> {
        let rows = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                input_allowed = 0,
                backend_state_json = ?,
                last_activity_at = ?
            WHERE pty_session_id IN (
                SELECT ps.pty_session_id
                FROM pty_sessions ps
                JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                WHERE aw.connector_id = ?
                  AND ps.backend_state_json = ?
                  AND ps.state_json != ?
            )
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&PtyBackendState::Failed)?)
        .bind(observed_at)
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Closed)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
    }

    /// Closes one PTY session and returns the updated row.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn close(
        &self,
        id: PtySessionId,
        last_exit_code: Option<i32>,
        closed_at: OffsetDateTime,
    ) -> Result<Option<PtySession>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                last_exit_code = COALESCE(?, last_exit_code),
                input_allowed = 0,
                backend_state_json = ?,
                last_activity_at = ?
            WHERE pty_session_id = ?
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(last_exit_code)
        .bind(to_json(&PtyBackendState::Closed)?)
        .bind(closed_at)
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_session).transpose()
    }

    /// Closes all PTY sessions for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn close_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        closed_at: OffsetDateTime,
    ) -> Result<Vec<PtySession>, DbError> {
        let rows = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                input_allowed = 0,
                backend_state_json = ?,
                last_activity_at = ?
            WHERE workspace_id = ?
              AND state_json != ?
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&PtyBackendState::Closed)?)
        .bind(closed_at)
        .bind(workspace_id.to_string())
        .bind(to_json(&WorkspaceState::Closed)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
    }

    /// Closes expired active PTY sessions.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn close_expired(
        &self,
        now: OffsetDateTime,
        idle_ttl_seconds: u64,
        limit: u32,
    ) -> Result<Vec<PtySession>, DbError> {
        let cutoff = now - time::Duration::seconds(i64::try_from(idle_ttl_seconds)?);
        let rows = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                input_allowed = 0,
                backend_state_json = ?,
                last_activity_at = ?
            WHERE pty_session_id IN (
                SELECT pty_session_id
                FROM pty_sessions
                WHERE state_json != ?
                  AND last_activity_at <= ?
                ORDER BY last_activity_at ASC, pty_session_id ASC
                LIMIT ?
            )
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, transport_evidence_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&PtyBackendState::Closed)?)
        .bind(now)
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(cutoff)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
    }
}

impl PtyOutputChunkRepository {
    /// Inserts a PTY output chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, chunk: &PtyOutputChunk) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO pty_output_chunks (
                id, pty_session_id, workspace_id, stream_json, sequence, redacted_text,
                byte_len, truncated, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(chunk.id.to_string())
        .bind(chunk.pty_session_id.to_string())
        .bind(chunk.workspace_id.to_string())
        .bind(to_json(&chunk.stream)?)
        .bind(u64_to_i64(chunk.sequence)?)
        .bind(&chunk.redacted_text)
        .bind(u64_to_i64(chunk.byte_len)?)
        .bind(bool_to_i64(chunk.truncated))
        .bind(chunk.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists PTY output chunks for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_session(
        &self,
        pty_session_id: PtySessionId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<PtyOutputChunk>, DbError> {
        let start_sequence = after_sequence.map_or(0, |sequence| sequence.saturating_add(1));
        let rows = sqlx::query(
            r"
            SELECT id, pty_session_id, workspace_id, stream_json, sequence, redacted_text,
                   byte_len, truncated, created_at
            FROM pty_output_chunks
            WHERE pty_session_id = ? AND sequence >= ?
            ORDER BY sequence ASC
            LIMIT ?
            ",
        )
        .bind(pty_session_id.to_string())
        .bind(u64_to_i64(start_sequence)?)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_output_chunk).collect()
    }

    /// Returns the next PTY output sequence number for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or integer conversion fails.
    pub async fn next_sequence(&self, pty_session_id: PtySessionId) -> Result<u64, DbError> {
        let current: Option<i64> = sqlx::query_scalar(
            r"
            SELECT MAX(sequence)
            FROM pty_output_chunks
            WHERE pty_session_id = ?
            ",
        )
        .bind(pty_session_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(match current {
            Some(value) => i64_to_u64(value)?.saturating_add(1),
            None => 0,
        })
    }
}

impl PtyInputEventRepository {
    /// Inserts a queued PTY input event with its raw payload.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, event: &PtyInputEvent, input_text: &str) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO pty_input_events (
                id, pty_session_id, workspace_id, connector_id, host_id, agent_session_id,
                idempotency_key, input_fingerprint, state_json, sequence, input_text,
                redacted_input_summary, byte_len, requested_by, created_at, claimed_at,
                lease_expires_at, delivered_at, failed_at, attempt_count, claim_token, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(event.id.to_string())
        .bind(event.pty_session_id.to_string())
        .bind(event.workspace_id.to_string())
        .bind(event.connector_id.to_string())
        .bind(event.host_id.to_string())
        .bind(event.agent_session_id.map(|id| id.to_string()))
        .bind(&event.idempotency_key)
        .bind(&event.input_fingerprint)
        .bind(to_json(&event.state)?)
        .bind(u64_to_i64(event.sequence)?)
        .bind(input_text)
        .bind(&event.redacted_input_summary)
        .bind(u64_to_i64(event.byte_len)?)
        .bind(&event.requested_by)
        .bind(event.created_at)
        .bind(event.claimed_at)
        .bind(event.lease_expires_at)
        .bind(event.delivered_at)
        .bind(event.failed_at)
        .bind(u32_to_i64(event.attempt_count))
        .bind(Option::<String>::None)
        .bind(&event.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets public PTY input event metadata by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: PtyInputEventId) -> Result<Option<PtyInputEvent>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM pty_input_events
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_input_event).transpose()
    }

    /// Gets one PTY input event by a caller retry key scoped to an agent session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_by_agent_session_and_idempotency_key(
        &self,
        agent_session_id: AgentSessionId,
        idempotency_key: &str,
    ) -> Result<Option<PtyInputEvent>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM pty_input_events
            WHERE agent_session_id = ? AND idempotency_key = ?
            ",
        )
        .bind(agent_session_id.to_string())
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_input_event).transpose()
    }

    /// Lists public PTY input metadata for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_session(
        &self,
        pty_session_id: PtySessionId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<PtyInputEvent>, DbError> {
        let start_sequence = after_sequence.map_or(0, |sequence| sequence.saturating_add(1));
        let rows = sqlx::query(
            r"
            SELECT *
            FROM pty_input_events
            WHERE pty_session_id = ? AND sequence >= ?
            ORDER BY sequence ASC
            LIMIT ?
            ",
        )
        .bind(pty_session_id.to_string())
        .bind(u64_to_i64(start_sequence)?)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_input_event).collect()
    }

    /// Returns the next PTY input sequence number for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or integer conversion fails.
    pub async fn next_sequence(&self, pty_session_id: PtySessionId) -> Result<u64, DbError> {
        let current: Option<i64> = sqlx::query_scalar(
            r"
            SELECT MAX(sequence)
            FROM pty_input_events
            WHERE pty_session_id = ?
            ",
        )
        .bind(pty_session_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(match current {
            Some(value) => i64_to_u64(value)?.saturating_add(1),
            None => 0,
        })
    }

    /// Atomically claims the oldest eligible PTY input event for a connector.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn claim_next_for_connector(
        &self,
        connector_id: ConnectorId,
        claim_token: &str,
        claimed_at: OffsetDateTime,
        lease_expires_at: OffsetDateTime,
        max_attempts: u32,
    ) -> Result<Option<ClaimedPtyInputEvent>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE pty_input_events
            SET state_json = ?,
                claim_token = ?,
                claimed_at = ?,
                lease_expires_at = ?,
                attempt_count = attempt_count + 1,
                last_error = NULL
            WHERE id = (
                SELECT id
                FROM pty_input_events
                WHERE connector_id = ?
                  AND input_text IS NOT NULL
                  AND attempt_count < ?
                  AND (
                    agent_session_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM host_write_leases
                        WHERE host_write_leases.host_id = pty_input_events.host_id
                          AND host_write_leases.expires_at > ?
                          AND host_write_leases.holder_agent_session_id
                              != pty_input_events.agent_session_id
                    )
                  )
                  AND (
                    state_json = ?
                    OR (
                        state_json = ?
                        AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?
                    )
                  )
                ORDER BY created_at ASC, sequence ASC, id ASC
                LIMIT 1
            )
            RETURNING *
            ",
        )
        .bind(to_json(&PtyInputEventState::Claimed)?)
        .bind(claim_token)
        .bind(claimed_at)
        .bind(lease_expires_at)
        .bind(connector_id.to_string())
        .bind(u32_to_i64(max_attempts))
        .bind(claimed_at)
        .bind(to_json(&PtyInputEventState::Queued)?)
        .bind(to_json(&PtyInputEventState::Claimed)?)
        .bind(claimed_at)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_claimed_pty_input_event).transpose()
    }

    /// Returns a claimed PTY input to the queue without consuming a retry attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or updating fails.
    pub async fn defer_claimed_for_write_lease(
        &self,
        id: PtyInputEventId,
        claim_token: &str,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE pty_input_events
            SET state_json = ?,
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                attempt_count = MAX(attempt_count - 1, 0),
                last_error = NULL
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(&PtyInputEventState::Queued)?)
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Marks a claimed PTY input event as delivered and clears the raw payload.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or updating fails.
    pub async fn finish_claimed(
        &self,
        id: PtyInputEventId,
        claim_token: &str,
        delivered_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE pty_input_events
            SET state_json = ?,
                input_text = NULL,
                delivered_at = ?,
                failed_at = NULL,
                claim_token = NULL,
                lease_expires_at = NULL,
                last_error = NULL
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(&PtyInputEventState::Delivered)?)
        .bind(delivered_at)
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Marks a claimed PTY input event as failed and clears the raw payload.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or updating fails.
    pub async fn fail_claimed(
        &self,
        id: PtyInputEventId,
        claim_token: &str,
        failed_at: OffsetDateTime,
        last_error: &str,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE pty_input_events
            SET state_json = ?,
                input_text = NULL,
                failed_at = ?,
                claim_token = NULL,
                lease_expires_at = NULL,
                last_error = ?
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(&PtyInputEventState::Failed)?)
        .bind(failed_at)
        .bind(last_error)
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}

impl OperationRunRepository {
    /// Inserts an operation run.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, operation: &OperationRun) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO operation_runs (
                id, host_id, access_path_id, connector_id, session_id, workspace_id,
                agent_session_id, idempotency_key, requires_write_lease,
                operation_type_json, intent, state_json, started_at, finished_at, exit_code,
                timeout_seconds, redacted_command_summary, command_profile_json,
                transport_evidence_json,
                redacted_output_summary, log_ref, attempt_count, claim_token, claimed_at,
                lease_expires_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(operation.id.to_string())
        .bind(operation.host_id.to_string())
        .bind(operation.access_path_id.to_string())
        .bind(operation.connector_id.to_string())
        .bind(operation.session_id.map(|id| id.to_string()))
        .bind(operation.workspace_id.map(|id| id.to_string()))
        .bind(operation.agent_session_id.map(|id| id.to_string()))
        .bind(&operation.idempotency_key)
        .bind(operation.requires_write_lease)
        .bind(to_json(&operation.operation_type)?)
        .bind(&operation.intent)
        .bind(to_json(&operation.state)?)
        .bind(operation.started_at)
        .bind(operation.finished_at)
        .bind(operation.exit_code)
        .bind(u64_to_i64(operation.timeout_seconds)?)
        .bind(&operation.redacted_command_summary)
        .bind(optional_json(operation.command_profile_json.as_ref())?)
        .bind(optional_json(operation.transport_evidence.as_ref())?)
        .bind(&operation.redacted_output_summary)
        .bind(&operation.log_ref)
        .bind(u32_to_i64(operation.attempt_count))
        .bind(&operation.claim_token)
        .bind(operation.claimed_at)
        .bind(operation.lease_expires_at)
        .bind(&operation.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets one operation by a caller retry key scoped to an agent session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_by_agent_session_and_idempotency_key(
        &self,
        agent_session_id: AgentSessionId,
        idempotency_key: &str,
    ) -> Result<Option<OperationRun>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM operation_runs
            WHERE agent_session_id = ? AND idempotency_key = ?
            ",
        )
        .bind(agent_session_id.to_string())
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_operation_run).transpose()
    }

    /// Updates operation state.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn update_state(
        &self,
        id: OperationId,
        state: OperationState,
        finished_at: Option<OffsetDateTime>,
        exit_code: Option<i32>,
        redacted_output_summary: Option<&str>,
    ) -> Result<(), DbError> {
        sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?, finished_at = ?, exit_code = ?, redacted_output_summary = ?
            WHERE id = ?
            ",
        )
        .bind(to_json(&state)?)
        .bind(finished_at)
        .bind(exit_code)
        .bind(redacted_output_summary)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically claims the oldest eligible operation for a connector.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn claim_next_for_connector(
        &self,
        connector_id: ConnectorId,
        claim_token: &str,
        claimed_at: OffsetDateTime,
        lease_expires_at: OffsetDateTime,
        max_attempts: u32,
    ) -> Result<Option<OperationRun>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?,
                claim_token = ?,
                claimed_at = ?,
                lease_expires_at = ?,
                attempt_count = attempt_count + 1,
                finished_at = NULL,
                exit_code = NULL,
                last_error = NULL,
                redacted_output_summary = ?
            WHERE id = (
                SELECT id
                FROM operation_runs
                WHERE connector_id = ?
                  AND workspace_id IS NOT NULL
                  AND command_profile_json IS NOT NULL
                  AND attempt_count < ?
                  AND (
                    requires_write_lease = 0
                    OR agent_session_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM host_write_leases
                        WHERE host_write_leases.host_id = operation_runs.host_id
                          AND host_write_leases.expires_at > ?
                          AND host_write_leases.holder_agent_session_id
                              != operation_runs.agent_session_id
                    )
                  )
                  AND (
                    state_json = ?
                    OR (
                        state_json = ?
                        AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?
                    )
                  )
                ORDER BY started_at ASC, id ASC
                LIMIT 1
            )
            RETURNING *
            ",
        )
        .bind(to_json(&OperationState::Running)?)
        .bind(claim_token)
        .bind(claimed_at)
        .bind(lease_expires_at)
        .bind("claimed by connector worker")
        .bind(connector_id.to_string())
        .bind(u32_to_i64(max_attempts))
        .bind(claimed_at)
        .bind(to_json(&OperationState::Queued)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(claimed_at)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_operation_run).transpose()
    }

    /// Atomically marks the oldest expired operation that exhausted its claim budget.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn exhaust_next_for_connector(
        &self,
        connector_id: ConnectorId,
        observed_at: OffsetDateTime,
        max_attempts: u32,
        redacted_output_summary: &str,
        last_error: &str,
    ) -> Result<Option<OperationRun>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?,
                finished_at = ?,
                exit_code = NULL,
                redacted_output_summary = ?,
                last_error = ?,
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL
            WHERE id = (
                SELECT id
                FROM operation_runs
                WHERE connector_id = ?
                  AND workspace_id IS NOT NULL
                  AND command_profile_json IS NOT NULL
                  AND attempt_count >= ?
                  AND (
                    state_json = ?
                    OR (
                        state_json = ?
                        AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?
                    )
                  )
                ORDER BY started_at ASC, id ASC
                LIMIT 1
            )
            RETURNING *
            ",
        )
        .bind(to_json(&OperationState::Exhausted)?)
        .bind(observed_at)
        .bind(redacted_output_summary)
        .bind(last_error)
        .bind(connector_id.to_string())
        .bind(u32_to_i64(max_attempts))
        .bind(to_json(&OperationState::Queued)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(observed_at)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_operation_run).transpose()
    }

    /// Finishes an operation and clears its connector lease.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn finish(
        &self,
        id: OperationId,
        state: OperationState,
        finished_at: OffsetDateTime,
        exit_code: Option<i32>,
        redacted_output_summary: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<(), DbError> {
        sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?,
                finished_at = ?,
                exit_code = ?,
                redacted_output_summary = ?,
                last_error = ?,
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL
            WHERE id = ?
            ",
        )
        .bind(to_json(&state)?)
        .bind(finished_at)
        .bind(exit_code)
        .bind(redacted_output_summary)
        .bind(last_error)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Renews a running operation claim.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or updating fails.
    pub async fn renew_claim(
        &self,
        id: OperationId,
        claim_token: &str,
        lease_expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE operation_runs
            SET lease_expires_at = ?
            WHERE id = ?
              AND claim_token = ?
              AND state_json = ?
            ",
        )
        .bind(lease_expires_at)
        .bind(id.to_string())
        .bind(claim_token)
        .bind(to_json(&OperationState::Running)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Returns a claimed operation to the queue without consuming a retry attempt.
    ///
    /// Used when another agent wins a host write-lease race after this connector selected the
    /// operation.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or updating fails.
    pub async fn defer_claimed_for_write_lease(
        &self,
        id: OperationId,
        claim_token: &str,
        redacted_output_summary: &str,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?,
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                attempt_count = MAX(attempt_count - 1, 0),
                redacted_output_summary = ?,
                last_error = NULL
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(&OperationState::Queued)?)
        .bind(redacted_output_summary)
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Attaches structured SSH transport evidence while the connector still owns the claim.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn attach_transport_evidence(
        &self,
        id: OperationId,
        claim_token: &str,
        evidence: &SshChannelTransportEvidence,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE operation_runs
            SET transport_evidence_json = ?
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(evidence)?)
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Finishes an operation only if the caller still owns the claim.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn finish_claimed(
        &self,
        finish: ClaimedOperationFinish<'_>,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE operation_runs
            SET state_json = ?,
                finished_at = ?,
                exit_code = ?,
                redacted_output_summary = ?,
                last_error = ?,
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(to_json(&finish.state)?)
        .bind(finish.finished_at)
        .bind(finish.exit_code)
        .bind(finish.redacted_output_summary)
        .bind(finish.last_error)
        .bind(finish.id.to_string())
        .bind(finish.claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Attaches a claimed operation to the logical SSH session used for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn attach_session(
        &self,
        id: OperationId,
        claim_token: &str,
        session_id: SessionId,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            UPDATE operation_runs
            SET session_id = ?
            WHERE id = ? AND claim_token = ?
            ",
        )
        .bind(session_id.to_string())
        .bind(id.to_string())
        .bind(claim_token)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Lists recent operations for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_recent_for_host(
        &self,
        host_id: HostId,
        limit: u32,
    ) -> Result<Vec<OperationRun>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM operation_runs
            WHERE host_id = ?
            ORDER BY started_at DESC
            LIMIT ?
            ",
        )
        .bind(host_id.to_string())
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_operation_run).collect()
    }

    /// Gets an operation by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(&self, id: OperationId) -> Result<Option<OperationRun>, DbError> {
        let row = sqlx::query(
            r"
            SELECT *
            FROM operation_runs
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_operation_run).transpose()
    }

    /// Lists recent operations for a workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<Vec<OperationRun>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM operation_runs
            WHERE workspace_id = ?
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            ",
        )
        .bind(workspace_id.to_string())
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_operation_run).collect()
    }

    /// Counts queued operations for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn count_queued_for_host(&self, host_id: HostId) -> Result<u32, DbError> {
        self.count_for_host_by_state(host_id, OperationState::Queued)
            .await
    }

    /// Counts running operations for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn count_running_for_host(&self, host_id: HostId) -> Result<u32, DbError> {
        self.count_for_host_by_state(host_id, OperationState::Running)
            .await
    }

    async fn count_for_host_by_state(
        &self,
        host_id: HostId,
        state: OperationState,
    ) -> Result<u32, DbError> {
        let count: i64 = sqlx::query_scalar(
            r"
            SELECT COUNT(*)
            FROM operation_runs
            WHERE host_id = ? AND state_json = ?
            ",
        )
        .bind(host_id.to_string())
        .bind(to_json(&state)?)
        .fetch_one(&self.pool)
        .await?;
        i64_to_u32(count)
    }
}

impl OperationOutputChunkRepository {
    /// Inserts an output chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, chunk: &OperationOutputChunk) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO operation_output_chunks (
                id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                byte_len, truncated, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(chunk.id.to_string())
        .bind(chunk.operation_id.to_string())
        .bind(chunk.workspace_id.to_string())
        .bind(to_json(&chunk.stream)?)
        .bind(u64_to_i64(chunk.sequence)?)
        .bind(&chunk.redacted_text)
        .bind(u64_to_i64(chunk.byte_len)?)
        .bind(bool_to_i64(chunk.truncated))
        .bind(chunk.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns the next sequence number for an operation.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or integer conversion fails.
    pub async fn next_sequence(&self, operation_id: OperationId) -> Result<u64, DbError> {
        let next: i64 = sqlx::query_scalar(
            r"
            SELECT COALESCE(MAX(sequence), -1) + 1
            FROM operation_output_chunks
            WHERE operation_id = ?
            ",
        )
        .bind(operation_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        i64_to_u64(next)
    }

    /// Lists output chunks for a workspace, optionally scoped to an operation.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        operation_id: Option<OperationId>,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<Vec<OperationOutputChunk>, DbError> {
        let start_sequence = after_sequence.map_or(0, |sequence| sequence.saturating_add(1));
        let rows = if let Some(operation_id) = operation_id {
            sqlx::query(
                r"
                SELECT id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                       byte_len, truncated, created_at
                FROM operation_output_chunks
                WHERE workspace_id = ? AND operation_id = ? AND sequence >= ?
                ORDER BY created_at ASC, sequence ASC
                LIMIT ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(operation_id.to_string())
            .bind(u64_to_i64(start_sequence)?)
            .bind(u32_to_i64(limit))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r"
                SELECT id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                       byte_len, truncated, created_at
                FROM operation_output_chunks
                WHERE workspace_id = ? AND sequence >= ?
                ORDER BY created_at ASC, sequence ASC
                LIMIT ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(u64_to_i64(start_sequence)?)
            .bind(u32_to_i64(limit))
            .fetch_all(&self.pool)
            .await?
        };
        rows.iter().map(row_to_operation_output_chunk).collect()
    }
}

impl OperationOutputArtifactRepository {
    /// Inserts an output artifact record.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, artifact: &OperationOutputArtifact) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO operation_output_artifacts (
                id, operation_id, workspace_id, stream_json, relative_path, byte_len,
                sha256, redacted_preview, truncated, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(artifact.id.to_string())
        .bind(artifact.operation_id.to_string())
        .bind(artifact.workspace_id.to_string())
        .bind(to_json(&artifact.stream)?)
        .bind(&artifact.relative_path)
        .bind(u64_to_i64(artifact.byte_len)?)
        .bind(&artifact.sha256)
        .bind(&artifact.redacted_preview)
        .bind(bool_to_i64(artifact.truncated))
        .bind(artifact.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Gets one artifact by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(
        &self,
        id: OperationOutputArtifactId,
    ) -> Result<Option<OperationOutputArtifact>, DbError> {
        let row = sqlx::query(
            r"
            SELECT id, operation_id, workspace_id, stream_json, relative_path, byte_len,
                   sha256, redacted_preview, truncated, created_at
            FROM operation_output_artifacts
            WHERE id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(row_to_operation_output_artifact)
            .transpose()
    }

    /// Lists artifacts for a workspace, optionally scoped to an operation.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        operation_id: Option<OperationId>,
        limit: u32,
    ) -> Result<Vec<OperationOutputArtifact>, DbError> {
        let rows = if let Some(operation_id) = operation_id {
            sqlx::query(
                r"
                SELECT id, operation_id, workspace_id, stream_json, relative_path, byte_len,
                       sha256, redacted_preview, truncated, created_at
                FROM operation_output_artifacts
                WHERE workspace_id = ? AND operation_id = ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(operation_id.to_string())
            .bind(u32_to_i64(limit))
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r"
                SELECT id, operation_id, workspace_id, stream_json, relative_path, byte_len,
                       sha256, redacted_preview, truncated, created_at
                FROM operation_output_artifacts
                WHERE workspace_id = ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(u32_to_i64(limit))
            .fetch_all(&self.pool)
            .await?
        };
        rows.iter().map(row_to_operation_output_artifact).collect()
    }
}

impl KnowledgeItemRepository {
    /// Inserts a knowledge item.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, item: &KnowledgeItem) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO knowledge_items (
                id, title, body, source_json, linked_host_ids_json,
                linked_access_path_ids_json, linked_software_ids_json, linked_operation_ids_json,
                tags_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(item.id.to_string())
        .bind(&item.title)
        .bind(&item.body)
        .bind(to_json(&item.source)?)
        .bind(to_json(&ids_to_strings(&item.linked_host_ids))?)
        .bind(to_json(&ids_to_strings(&item.linked_access_path_ids))?)
        .bind(to_json(&ids_to_strings(&item.linked_software_ids))?)
        .bind(to_json(&ids_to_strings(&item.linked_operation_ids))?)
        .bind(to_json(&item.tags)?)
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r"
            INSERT INTO knowledge_items_fts(rowid, title, body, tags)
            VALUES (
                (SELECT rowid FROM knowledge_items WHERE id = ?),
                ?,
                ?,
                ?
            )
            ",
        )
        .bind(item.id.to_string())
        .bind(&item.title)
        .bind(&item.body)
        .bind(item.tags.join(" "))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Searches knowledge items.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn search(&self, query: &str, limit: u32) -> Result<Vec<KnowledgeItem>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT ki.*
            FROM knowledge_items_fts fts
            JOIN knowledge_items ki ON ki.rowid = fts.rowid
            WHERE knowledge_items_fts MATCH ?
            LIMIT ?
            ",
        )
        .bind(query)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_knowledge_item).collect()
    }
}

impl TopologyRepository {
    /// Gets a topology node by its public stable key.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_node_by_external_key(
        &self,
        external_key: &str,
    ) -> Result<Option<TopologyNode>, DbError> {
        let row = sqlx::query(
            r"
            SELECT n.*,
                   EXISTS(
                       SELECT 1 FROM topology_node_memberships m
                       WHERE m.node_id = n.id AND m.active = 1
                   ) AS active
            FROM topology_nodes n
            WHERE n.external_key = ?
            ",
        )
        .bind(external_key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_topology_node).transpose()
    }

    /// Gets a topology node by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_node(&self, id: TopologyNodeId) -> Result<Option<TopologyNode>, DbError> {
        let row = sqlx::query(
            r"
            SELECT n.*,
                   EXISTS(
                       SELECT 1 FROM topology_node_memberships m
                       WHERE m.node_id = n.id AND m.active = 1
                   ) AS active
            FROM topology_nodes n
            WHERE n.id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_topology_node).transpose()
    }

    /// Gets a topology edge by its public stable key.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_edge_by_external_key(
        &self,
        external_key: &str,
    ) -> Result<Option<TopologyEdge>, DbError> {
        let row = sqlx::query(
            r"
            SELECT e.*,
                   (
                       EXISTS(
                           SELECT 1 FROM topology_edge_memberships m
                           WHERE m.edge_id = e.id AND m.active = 1
                       )
                       AND EXISTS(
                           SELECT 1 FROM topology_node_memberships source_membership
                           WHERE source_membership.node_id = e.source_node_id
                             AND source_membership.active = 1
                       )
                       AND EXISTS(
                           SELECT 1 FROM topology_node_memberships target_membership
                           WHERE target_membership.node_id = e.target_node_id
                             AND target_membership.active = 1
                       )
                   ) AS active
            FROM topology_edges e
            WHERE e.external_key = ?
            ",
        )
        .bind(external_key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_topology_edge).transpose()
    }

    /// Lists the topology graph, optionally including stale observations.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_graph(
        &self,
        include_inactive: bool,
    ) -> Result<(Vec<TopologyNode>, Vec<TopologyEdge>), DbError> {
        let nodes = if include_inactive {
            sqlx::query(
                r"
                SELECT n.*,
                       EXISTS(
                           SELECT 1 FROM topology_node_memberships m
                           WHERE m.node_id = n.id AND m.active = 1
                       ) AS active
                FROM topology_nodes n
                ORDER BY n.name, n.external_key
                ",
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r"
                SELECT n.*, 1 AS active
                FROM topology_nodes n
                WHERE EXISTS(
                    SELECT 1 FROM topology_node_memberships m
                    WHERE m.node_id = n.id AND m.active = 1
                )
                ORDER BY n.name, n.external_key
                ",
            )
            .fetch_all(&self.pool)
            .await?
        };
        let edges = if include_inactive {
            sqlx::query(
                r"
                SELECT e.*,
                       (
                           EXISTS(
                               SELECT 1 FROM topology_edge_memberships m
                               WHERE m.edge_id = e.id AND m.active = 1
                           )
                           AND EXISTS(
                               SELECT 1 FROM topology_node_memberships source_membership
                               WHERE source_membership.node_id = e.source_node_id
                                 AND source_membership.active = 1
                           )
                           AND EXISTS(
                               SELECT 1 FROM topology_node_memberships target_membership
                               WHERE target_membership.node_id = e.target_node_id
                                 AND target_membership.active = 1
                           )
                       ) AS active
                FROM topology_edges e
                ORDER BY e.external_key
                ",
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r"
                SELECT e.*, 1 AS active
                FROM topology_edges e
                WHERE EXISTS(
                    SELECT 1 FROM topology_edge_memberships m
                    WHERE m.edge_id = e.id AND m.active = 1
                )
                AND EXISTS(
                    SELECT 1 FROM topology_node_memberships source_membership
                    WHERE source_membership.node_id = e.source_node_id
                      AND source_membership.active = 1
                )
                AND EXISTS(
                    SELECT 1 FROM topology_node_memberships target_membership
                    WHERE target_membership.node_id = e.target_node_id
                      AND target_membership.active = 1
                )
                ORDER BY e.external_key
                ",
            )
            .fetch_all(&self.pool)
            .await?
        };
        Ok((
            nodes
                .iter()
                .map(row_to_topology_node)
                .collect::<Result<_, _>>()?,
            edges
                .iter()
                .map(row_to_topology_edge)
                .collect::<Result<_, _>>()?,
        ))
    }

    /// Reconciles one source-owned, scope-owned topology snapshot atomically.
    ///
    /// Items absent from the new snapshot are made inactive only for this source and scope. The
    /// underlying graph objects remain available for history and can stay active through another
    /// source.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the transaction cannot be committed.
    #[allow(clippy::too_many_lines)]
    pub async fn sync_snapshot(
        &self,
        scope_key: &str,
        source: &str,
        nodes: &[TopologyNode],
        edges: &[TopologyEdge],
        run_id: TopologySyncRunId,
        observed_at: OffsetDateTime,
    ) -> Result<TopologySyncRun, DbError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            INSERT INTO topology_sync_runs (
                id, scope_key, source, active_node_count, inactive_node_count,
                active_edge_count, inactive_edge_count, completed_at
            )
            VALUES (?, ?, ?, 0, 0, 0, 0, ?)
            ",
        )
        .bind(run_id.to_string())
        .bind(scope_key)
        .bind(source)
        .bind(observed_at)
        .execute(&mut *transaction)
        .await?;

        for node in nodes {
            sqlx::query(
                r"
                INSERT INTO topology_nodes (
                    id, external_key, host_id, name, kind_json, status_json, address,
                    ports_json, metadata_json, created_at, updated_at, last_observed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_key) DO UPDATE SET
                    host_id = excluded.host_id,
                    name = excluded.name,
                    kind_json = excluded.kind_json,
                    status_json = excluded.status_json,
                    address = excluded.address,
                    ports_json = excluded.ports_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at,
                    last_observed_at = excluded.last_observed_at
                ",
            )
            .bind(node.id.to_string())
            .bind(&node.external_key)
            .bind(node.host_id.map(|id| id.to_string()))
            .bind(&node.name)
            .bind(to_json(&node.kind)?)
            .bind(to_json(&node.status)?)
            .bind(&node.address)
            .bind(to_json(&node.ports)?)
            .bind(to_json(&node.metadata)?)
            .bind(node.created_at)
            .bind(node.updated_at)
            .bind(node.last_observed_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                r"
                INSERT INTO topology_node_memberships (
                    scope_key, source, node_id, last_sync_run_id, active, observed_at
                )
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(scope_key, source, node_id) DO UPDATE SET
                    last_sync_run_id = excluded.last_sync_run_id,
                    active = 1,
                    observed_at = excluded.observed_at
                ",
            )
            .bind(scope_key)
            .bind(source)
            .bind(node.id.to_string())
            .bind(run_id.to_string())
            .bind(observed_at)
            .execute(&mut *transaction)
            .await?;
        }

        let inactive_node_count = u32::try_from(
            sqlx::query(
                r"
                UPDATE topology_node_memberships
                SET active = 0
                WHERE scope_key = ? AND source = ? AND active = 1 AND last_sync_run_id <> ?
                ",
            )
            .bind(scope_key)
            .bind(source)
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
        )?;

        for edge in edges {
            sqlx::query(
                r"
                INSERT INTO topology_edges (
                    id, external_key, source_node_id, target_node_id, relation_json,
                    metadata_json, created_at, updated_at, last_observed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_key) DO UPDATE SET
                    source_node_id = excluded.source_node_id,
                    target_node_id = excluded.target_node_id,
                    relation_json = excluded.relation_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at,
                    last_observed_at = excluded.last_observed_at
                ",
            )
            .bind(edge.id.to_string())
            .bind(&edge.external_key)
            .bind(edge.source_node_id.to_string())
            .bind(edge.target_node_id.to_string())
            .bind(to_json(&edge.relation)?)
            .bind(to_json(&edge.metadata)?)
            .bind(edge.created_at)
            .bind(edge.updated_at)
            .bind(edge.last_observed_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                r"
                INSERT INTO topology_edge_memberships (
                    scope_key, source, edge_id, last_sync_run_id, active, observed_at
                )
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(scope_key, source, edge_id) DO UPDATE SET
                    last_sync_run_id = excluded.last_sync_run_id,
                    active = 1,
                    observed_at = excluded.observed_at
                ",
            )
            .bind(scope_key)
            .bind(source)
            .bind(edge.id.to_string())
            .bind(run_id.to_string())
            .bind(observed_at)
            .execute(&mut *transaction)
            .await?;
        }

        let inactive_edge_count = u32::try_from(
            sqlx::query(
                r"
                UPDATE topology_edge_memberships
                SET active = 0
                WHERE scope_key = ? AND source = ? AND active = 1 AND last_sync_run_id <> ?
                ",
            )
            .bind(scope_key)
            .bind(source)
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
        )?;
        let active_node_count = u32::try_from(nodes.len())?;
        let active_edge_count = u32::try_from(edges.len())?;
        sqlx::query(
            r"
            UPDATE topology_sync_runs
            SET active_node_count = ?, inactive_node_count = ?,
                active_edge_count = ?, inactive_edge_count = ?
            WHERE id = ?
            ",
        )
        .bind(u32_to_i64(active_node_count))
        .bind(u32_to_i64(inactive_node_count))
        .bind(u32_to_i64(active_edge_count))
        .bind(u32_to_i64(inactive_edge_count))
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(TopologySyncRun {
            id: run_id,
            scope_key: scope_key.to_owned(),
            source: source.to_owned(),
            active_node_count,
            inactive_node_count,
            active_edge_count,
            inactive_edge_count,
            completed_at: observed_at,
        })
    }
}

impl CredentialBindingRepository {
    /// Inserts a topology credential binding if it is not already present.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the insert.
    pub async fn insert(&self, binding: &CredentialBinding) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO credential_bindings (
                id, topology_node_id, credential_id, purpose, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(topology_node_id, credential_id, purpose) DO NOTHING
            ",
        )
        .bind(binding.id.to_string())
        .bind(binding.topology_node_id.to_string())
        .bind(binding.credential_id.to_string())
        .bind(&binding.purpose)
        .bind(binding.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists public credential metadata bound to topology resources.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_views(&self) -> Result<Vec<CredentialBindingView>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT b.id AS binding_id, b.topology_node_id, b.purpose, b.created_at AS binding_created_at,
                   c.id AS credential_id, c.name AS credential_name, c.kind_json,
                   c.username_hint, c.created_at AS credential_created_at,
                   c.updated_at AS credential_updated_at, c.last_used_at
            FROM credential_bindings b
            JOIN credentials c ON c.id = b.credential_id
            ORDER BY b.topology_node_id, b.purpose, c.name
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_credential_binding_view).collect()
    }
}

impl StateEventRepository {
    /// Inserts a state event.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, event: &StateEvent) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO state_events (
                id, entity_type, entity_id, old_state_json, new_state_json,
                reason_code_json, observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(event.id.to_string())
        .bind(&event.entity_type)
        .bind(&event.entity_id)
        .bind(to_json(&event.old_state)?)
        .bind(to_json(&event.new_state)?)
        .bind(to_json(&event.reason_code)?)
        .bind(event.observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists state events for an entity from newest to oldest.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_for_entity(
        &self,
        entity_type: &str,
        entity_id: &str,
        limit: u32,
    ) -> Result<Vec<StateEvent>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT id, entity_type, entity_id, old_state_json, new_state_json,
                   reason_code_json, observed_at
            FROM state_events
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT ?
            ",
        )
        .bind(entity_type)
        .bind(entity_id)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_state_event).collect()
    }

    /// Returns the latest global state-event cursor, or zero when the log is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or converting the cursor fails.
    pub async fn latest_sequence(&self) -> Result<u64, DbError> {
        let sequence: i64 = sqlx::query_scalar(
            r"
            SELECT COALESCE(MAX(sequence), 0)
            FROM state_events
            ",
        )
        .fetch_one(&self.pool)
        .await?;
        i64_to_u64(sequence)
    }

    /// Lists state events after a global cursor in ascending sequence order.
    ///
    /// Entity type and id filters are optional. An entity id should only be supplied together
    /// with an entity type.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, converting, or deserializing events fails.
    pub async fn list_after(
        &self,
        after_sequence: u64,
        entity_type: Option<&str>,
        entity_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SequencedStateEvent>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT sequence, id, entity_type, entity_id, old_state_json, new_state_json,
                   reason_code_json, observed_at
            FROM state_events
            WHERE sequence > ?
              AND (? IS NULL OR entity_type = ?)
              AND (? IS NULL OR entity_id = ?)
            ORDER BY sequence ASC
            LIMIT ?
            ",
        )
        .bind(u64_to_i64(after_sequence)?)
        .bind(entity_type)
        .bind(entity_type)
        .bind(entity_id)
        .bind(entity_id)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_sequenced_state_event).collect()
    }
}

fn row_to_host(row: &SqliteRow) -> Result<Host, DbError> {
    Ok(Host {
        id: parse_id(row, "id")?,
        name: row.try_get("name")?,
        display_name: row.try_get("display_name")?,
        kind: from_json_col(row, "kind_json")?,
        owner: row.try_get("owner")?,
        tags: from_json_col(row, "tags_json")?,
        description: row.try_get("description")?,
        risk_level: from_json_col(row, "risk_level_json")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_environment(row: &SqliteRow) -> Result<Environment, DbError> {
    Ok(Environment {
        id: parse_id(row, "id")?,
        name: row.try_get("name")?,
        kind: from_json_col(row, "kind_json")?,
        description: row.try_get("description")?,
        trust_level: from_json_col(row, "trust_level_json")?,
        notes: row.try_get("notes")?,
    })
}

fn row_to_connector(row: &SqliteRow) -> Result<Connector, DbError> {
    Ok(Connector {
        id: parse_id(row, "id")?,
        name: row.try_get("name")?,
        environment_id: parse_id(row, "environment_id")?,
        host_id: parse_optional_id(row, "host_id")?,
        version: row.try_get("version")?,
        state: from_json_col(row, "state_json")?,
        last_seen_at: row.try_get("last_seen_at")?,
        current_network: row.try_get("current_network")?,
    })
}

fn row_to_credential_metadata(row: &SqliteRow) -> Result<CredentialMetadata, DbError> {
    Ok(CredentialMetadata {
        id: parse_id(row, "id")?,
        name: row.try_get("name")?,
        kind: from_json_col::<CredentialKind>(row, "kind_json")?,
        username_hint: row.try_get("username_hint")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        last_used_at: row.try_get("last_used_at")?,
    })
}

fn row_to_stored_credential(row: &SqliteRow) -> Result<StoredCredential, DbError> {
    Ok(StoredCredential {
        metadata: row_to_credential_metadata(row)?,
        encrypted_blob_json: from_json_col::<Value>(row, "encrypted_blob_json")?,
    })
}

fn row_to_access_path(row: &SqliteRow) -> Result<AccessPath, DbError> {
    Ok(AccessPath {
        id: parse_id(row, "id")?,
        host_id: parse_id(row, "host_id")?,
        environment_id: parse_id(row, "environment_id")?,
        connector_id: parse_optional_id(row, "connector_id")?,
        protocol: from_json_col(row, "protocol_json")?,
        address: row.try_get("address")?,
        port: i64_to_u16(row.try_get("port")?)?,
        username: row.try_get("username")?,
        credential_id: parse_id(row, "credential_id")?,
        route_type: from_json_col(row, "route_type_json")?,
        proxy_chain: from_json_col(row, "proxy_chain_json")?,
        priority: row.try_get("priority")?,
        enabled: i64_to_bool(row.try_get("enabled")?),
        connection_mode: from_json_col(row, "connection_mode_json")?,
        idle_ttl_seconds: i64_to_u64(row.try_get("idle_ttl_seconds")?)?,
        keepalive_seconds: i64_to_u64(row.try_get("keepalive_seconds")?)?,
        max_concurrent_channels: i64_to_u16(row.try_get("max_concurrent_channels")?)?,
        max_new_connections_per_minute: i64_to_u16(row.try_get("max_new_connections_per_minute")?)?,
        requires_tty: i64_to_bool(row.try_get("requires_tty")?),
        notes: row.try_get("notes")?,
    })
}

fn row_to_access_path_health(row: &SqliteRow) -> Result<AccessPathHealth, DbError> {
    Ok(AccessPathHealth {
        access_path_id: parse_id(row, "access_path_id")?,
        state: from_json_col(row, "state_json")?,
        last_checked_at: row.try_get("last_checked_at")?,
        latency_ms: row
            .try_get::<Option<i64>, _>("latency_ms")?
            .map(i64_to_u64)
            .transpose()?,
        failure_count: i64_to_u32(row.try_get("failure_count")?)?,
        last_error_code: optional_json_col::<StateReasonCode>(row, "last_error_code_json")?,
        next_retry_at: row.try_get("next_retry_at")?,
    })
}

fn row_to_authorized_key_bootstrap(row: &SqliteRow) -> Result<AuthorizedKeyBootstrap, DbError> {
    Ok(AuthorizedKeyBootstrap {
        access_path_id: parse_id(row, "access_path_id")?,
        state: from_json_col::<AuthorizedKeyBootstrapState>(row, "state_json")?,
        reason: optional_json_col::<AuthorizedKeyBootstrapReason>(row, "reason_json")?,
        public_key_fingerprint: row.try_get("public_key_fingerprint")?,
        failure_count: i64_to_u32(row.try_get("failure_count")?)?,
        attempted_at: row.try_get("attempted_at")?,
        next_retry_at: row.try_get("next_retry_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_host_fact(row: &SqliteRow) -> Result<HostFact, DbError> {
    Ok(HostFact {
        id: parse_id(row, "id")?,
        host_id: parse_id(row, "host_id")?,
        namespace: row.try_get("namespace")?,
        key: row.try_get("key")?,
        value_json: from_json_col(row, "value_json")?,
        source: from_json_col(row, "source_json")?,
        observed_at: row.try_get("observed_at")?,
        expires_at: row.try_get("expires_at")?,
        confidence: row.try_get("confidence")?,
    })
}

fn row_to_agent_session(row: &SqliteRow) -> Result<AgentSession, DbError> {
    Ok(AgentSession {
        id: parse_id(row, "id")?,
        client_kind: row.try_get("client_kind")?,
        client_instance_id: row.try_get("client_instance_id")?,
        project_key: row.try_get("project_key")?,
        conversation_key: row.try_get("conversation_key")?,
        state: from_json_col(row, "state_json")?,
        created_at: row.try_get("created_at")?,
        last_seen_at: row.try_get("last_seen_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn row_to_agent_workspace(row: &SqliteRow) -> Result<AgentWorkspace, DbError> {
    Ok(AgentWorkspace {
        id: parse_id(row, "workspace_id")?,
        agent_session_id: parse_optional_id(row, "agent_session_id")?,
        host_id: parse_id(row, "host_id")?,
        access_path_id: parse_id(row, "access_path_id")?,
        connector_id: parse_id(row, "connector_id")?,
        label: row.try_get("label")?,
        cwd: row.try_get("cwd")?,
        state: from_json_col(row, "state_json")?,
        policy_profile: row.try_get("policy_profile")?,
        created_at: row.try_get("created_at")?,
        last_activity_at: row.try_get("last_activity_at")?,
        ttl_seconds: i64_to_u64(row.try_get("ttl_seconds")?)?,
    })
}

fn row_to_host_write_lease(row: &SqliteRow) -> Result<HostWriteLease, DbError> {
    Ok(HostWriteLease {
        host_id: parse_id(row, "host_id")?,
        holder_agent_session_id: parse_id(row, "holder_agent_session_id")?,
        holder_workspace_id: parse_id(row, "holder_workspace_id")?,
        acquired_at: row.try_get("acquired_at")?,
        heartbeat_at: row.try_get("heartbeat_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn row_to_pty_session(row: &SqliteRow) -> Result<PtySession, DbError> {
    Ok(PtySession {
        pty_session_id: parse_id(row, "pty_session_id")?,
        workspace_id: parse_id(row, "workspace_id")?,
        session_id: parse_id(row, "session_id")?,
        state: from_json_col(row, "state_json")?,
        foreground_process: row.try_get("foreground_process")?,
        cwd: row.try_get("cwd")?,
        recent_output_ref: row.try_get("recent_output_ref")?,
        last_exit_code: row.try_get("last_exit_code")?,
        input_allowed: i64_to_bool(row.try_get("input_allowed")?),
        backend_state: optional_json_col(row, "backend_state_json")?
            .unwrap_or(PtyBackendState::Unknown),
        backend_capabilities: optional_json_col(row, "backend_capabilities_json")?
            .unwrap_or_else(PtyBackendCapabilities::unknown),
        transport_evidence: optional_json_col(row, "transport_evidence_json")?,
        created_at: row.try_get("created_at")?,
        last_activity_at: row.try_get("last_activity_at")?,
    })
}

fn row_to_connection_session(row: &SqliteRow) -> Result<ConnectionSession, DbError> {
    Ok(ConnectionSession {
        session_id: parse_id(row, "session_id")?,
        access_path_id: parse_id(row, "access_path_id")?,
        connector_id: parse_id(row, "connector_id")?,
        state: from_json_col(row, "state_json")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        open_channels: i64_to_u32(row.try_get("open_channels")?)?,
        reused_count: i64_to_u64(row.try_get("reused_count")?)?,
        failure_count: i64_to_u32(row.try_get("failure_count")?)?,
        last_error: row.try_get("last_error")?,
    })
}

fn row_to_ssh_transport_runtime(row: &SqliteRow) -> Result<SshTransportRuntime, DbError> {
    Ok(SshTransportRuntime {
        access_path_id: parse_id(row, "access_path_id")?,
        connector_id: parse_id(row, "connector_id")?,
        telemetry: remote_hosts_domain::SshTransportTelemetry {
            runtime_id: parse_id::<SshTransportRuntimeId>(row, "runtime_id")?,
            backend: from_json_col(row, "backend_json")?,
            state: from_json_col(row, "state_json")?,
            generation: i64_to_u64(row.try_get("generation")?)?,
            connection_attempt_count: i64_to_u64(row.try_get("connection_attempt_count")?)?,
            successful_handshake_count: i64_to_u64(row.try_get("successful_handshake_count")?)?,
            reuse_count: i64_to_u64(row.try_get("reuse_count")?)?,
            last_handshake_at: row.try_get("last_handshake_at")?,
            last_validated_at: row.try_get("last_validated_at")?,
            capabilities: from_json_col(row, "capabilities_json")?,
        },
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_pty_output_chunk(row: &SqliteRow) -> Result<PtyOutputChunk, DbError> {
    Ok(PtyOutputChunk {
        id: parse_id(row, "id")?,
        pty_session_id: parse_id(row, "pty_session_id")?,
        workspace_id: parse_id(row, "workspace_id")?,
        stream: from_json_col(row, "stream_json")?,
        sequence: i64_to_u64(row.try_get("sequence")?)?,
        redacted_text: row.try_get("redacted_text")?,
        byte_len: i64_to_u64(row.try_get("byte_len")?)?,
        truncated: i64_to_bool(row.try_get("truncated")?),
        created_at: row.try_get("created_at")?,
    })
}

fn row_to_pty_input_event(row: &SqliteRow) -> Result<PtyInputEvent, DbError> {
    Ok(PtyInputEvent {
        id: parse_id(row, "id")?,
        pty_session_id: parse_id(row, "pty_session_id")?,
        workspace_id: parse_id(row, "workspace_id")?,
        connector_id: parse_id(row, "connector_id")?,
        host_id: parse_id(row, "host_id")?,
        agent_session_id: parse_optional_id(row, "agent_session_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        input_fingerprint: row.try_get("input_fingerprint")?,
        state: from_json_col(row, "state_json")?,
        sequence: i64_to_u64(row.try_get("sequence")?)?,
        redacted_input_summary: row.try_get("redacted_input_summary")?,
        byte_len: i64_to_u64(row.try_get("byte_len")?)?,
        requested_by: row.try_get("requested_by")?,
        created_at: row.try_get("created_at")?,
        claimed_at: row.try_get("claimed_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        delivered_at: row.try_get("delivered_at")?,
        failed_at: row.try_get("failed_at")?,
        attempt_count: i64_to_u32(row.try_get("attempt_count")?)?,
        last_error: row.try_get("last_error")?,
    })
}

fn row_to_claimed_pty_input_event(row: &SqliteRow) -> Result<ClaimedPtyInputEvent, DbError> {
    Ok(ClaimedPtyInputEvent {
        event: row_to_pty_input_event(row)?,
        input_text: row
            .try_get::<Option<String>, _>("input_text")?
            .ok_or_else(|| sqlx::Error::ColumnDecode {
                index: "input_text".to_owned(),
                source: io::Error::other("claimed PTY input event has no payload").into(),
            })?,
        claim_token: row
            .try_get::<Option<String>, _>("claim_token")?
            .ok_or_else(|| sqlx::Error::ColumnDecode {
                index: "claim_token".to_owned(),
                source: io::Error::other("claimed PTY input event has no claim token").into(),
            })?,
    })
}

fn row_to_operation_run(row: &SqliteRow) -> Result<OperationRun, DbError> {
    Ok(OperationRun {
        id: parse_id(row, "id")?,
        host_id: parse_id(row, "host_id")?,
        access_path_id: parse_id(row, "access_path_id")?,
        connector_id: parse_id(row, "connector_id")?,
        session_id: parse_optional_id(row, "session_id")?,
        workspace_id: parse_optional_id(row, "workspace_id")?,
        agent_session_id: parse_optional_id(row, "agent_session_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        requires_write_lease: i64_to_bool(row.try_get("requires_write_lease")?),
        operation_type: from_json_col(row, "operation_type_json")?,
        intent: row.try_get("intent")?,
        state: from_json_col(row, "state_json")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        exit_code: row.try_get("exit_code")?,
        timeout_seconds: i64_to_u64(row.try_get("timeout_seconds")?)?,
        redacted_command_summary: row.try_get("redacted_command_summary")?,
        command_profile_json: optional_json_col(row, "command_profile_json")?,
        transport_evidence: optional_json_col(row, "transport_evidence_json")?,
        redacted_output_summary: row.try_get("redacted_output_summary")?,
        log_ref: row.try_get("log_ref")?,
        attempt_count: i64_to_u32(row.try_get("attempt_count")?)?,
        claim_token: row.try_get("claim_token")?,
        claimed_at: row.try_get("claimed_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        last_error: row.try_get("last_error")?,
    })
}

fn row_to_operation_output_chunk(row: &SqliteRow) -> Result<OperationOutputChunk, DbError> {
    Ok(OperationOutputChunk {
        id: parse_id(row, "id")?,
        operation_id: parse_id(row, "operation_id")?,
        workspace_id: parse_id(row, "workspace_id")?,
        stream: from_json_col(row, "stream_json")?,
        sequence: i64_to_u64(row.try_get("sequence")?)?,
        redacted_text: row.try_get("redacted_text")?,
        byte_len: i64_to_u64(row.try_get("byte_len")?)?,
        truncated: i64_to_bool(row.try_get("truncated")?),
        created_at: row.try_get("created_at")?,
    })
}

fn row_to_operation_output_artifact(row: &SqliteRow) -> Result<OperationOutputArtifact, DbError> {
    Ok(OperationOutputArtifact {
        id: parse_id(row, "id")?,
        operation_id: parse_id(row, "operation_id")?,
        workspace_id: parse_id(row, "workspace_id")?,
        stream: from_json_col(row, "stream_json")?,
        relative_path: row.try_get("relative_path")?,
        byte_len: i64_to_u64(row.try_get("byte_len")?)?,
        sha256: row.try_get("sha256")?,
        redacted_preview: row.try_get("redacted_preview")?,
        truncated: i64_to_bool(row.try_get("truncated")?),
        created_at: row.try_get("created_at")?,
    })
}

fn row_to_knowledge_item(row: &SqliteRow) -> Result<KnowledgeItem, DbError> {
    Ok(KnowledgeItem {
        id: parse_id(row, "id")?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        source: from_json_col(row, "source_json")?,
        linked_host_ids: parse_ids(from_json_col::<Vec<String>>(row, "linked_host_ids_json")?)?,
        linked_access_path_ids: parse_ids(from_json_col::<Vec<String>>(
            row,
            "linked_access_path_ids_json",
        )?)?,
        linked_software_ids: parse_ids(from_json_col::<Vec<String>>(
            row,
            "linked_software_ids_json",
        )?)?,
        linked_operation_ids: parse_ids(from_json_col::<Vec<String>>(
            row,
            "linked_operation_ids_json",
        )?)?,
        tags: from_json_col(row, "tags_json")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_topology_node(row: &SqliteRow) -> Result<TopologyNode, DbError> {
    Ok(TopologyNode {
        id: parse_id(row, "id")?,
        external_key: row.try_get("external_key")?,
        host_id: parse_optional_id(row, "host_id")?,
        name: row.try_get("name")?,
        kind: from_json_col(row, "kind_json")?,
        status: from_json_col(row, "status_json")?,
        address: row.try_get("address")?,
        ports: from_json_col(row, "ports_json")?,
        metadata: from_json_col(row, "metadata_json")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        last_observed_at: row.try_get("last_observed_at")?,
        active: i64_to_bool(row.try_get("active")?),
    })
}

fn row_to_topology_edge(row: &SqliteRow) -> Result<TopologyEdge, DbError> {
    Ok(TopologyEdge {
        id: parse_id(row, "id")?,
        external_key: row.try_get("external_key")?,
        source_node_id: parse_id(row, "source_node_id")?,
        target_node_id: parse_id(row, "target_node_id")?,
        relation: from_json_col(row, "relation_json")?,
        metadata: from_json_col(row, "metadata_json")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        last_observed_at: row.try_get("last_observed_at")?,
        active: i64_to_bool(row.try_get("active")?),
    })
}

fn row_to_credential_binding_view(row: &SqliteRow) -> Result<CredentialBindingView, DbError> {
    Ok(CredentialBindingView {
        id: parse_id(row, "binding_id")?,
        topology_node_id: parse_id(row, "topology_node_id")?,
        purpose: row.try_get("purpose")?,
        credential: CredentialMetadata {
            id: parse_id(row, "credential_id")?,
            name: row.try_get("credential_name")?,
            kind: from_json_col(row, "kind_json")?,
            username_hint: row.try_get("username_hint")?,
            created_at: row.try_get("credential_created_at")?,
            updated_at: row.try_get("credential_updated_at")?,
            last_used_at: row.try_get("last_used_at")?,
        },
        created_at: row.try_get("binding_created_at")?,
    })
}

fn row_to_state_event(row: &SqliteRow) -> Result<StateEvent, DbError> {
    Ok(StateEvent {
        id: parse_id(row, "id")?,
        entity_type: row.try_get("entity_type")?,
        entity_id: row.try_get("entity_id")?,
        old_state: from_json_col(row, "old_state_json")?,
        new_state: from_json_col(row, "new_state_json")?,
        reason_code: from_json_col(row, "reason_code_json")?,
        observed_at: row.try_get("observed_at")?,
    })
}

fn row_to_sequenced_state_event(row: &SqliteRow) -> Result<SequencedStateEvent, DbError> {
    Ok(SequencedStateEvent {
        sequence: i64_to_u64(row.try_get("sequence")?)?,
        event: row_to_state_event(row)?,
    })
}

fn parse_id<T>(row: &SqliteRow, column: &str) -> Result<T, DbError>
where
    T: From<uuid::Uuid>,
{
    let value: String = row.try_get(column)?;
    Ok(T::from(uuid::Uuid::parse_str(&value)?))
}

fn parse_optional_id<T>(row: &SqliteRow, column: &str) -> Result<Option<T>, DbError>
where
    T: From<uuid::Uuid>,
{
    let value: Option<String> = row.try_get(column)?;
    value
        .map(|id| {
            uuid::Uuid::parse_str(&id)
                .map(T::from)
                .map_err(DbError::from)
        })
        .transpose()
}

fn parse_ids<T>(ids: Vec<String>) -> Result<Vec<T>, DbError>
where
    T: From<uuid::Uuid>,
{
    ids.into_iter()
        .map(|id| {
            uuid::Uuid::parse_str(&id)
                .map(T::from)
                .map_err(DbError::from)
        })
        .collect()
}

fn ids_to_strings<T: ToString>(ids: &[T]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

fn to_json<T: serde::Serialize>(value: &T) -> Result<String, DbError> {
    serde_json::to_string(value).map_err(DbError::from)
}

fn optional_json<T: serde::Serialize>(value: Option<&T>) -> Result<Option<String>, DbError> {
    value.map(to_json).transpose()
}

fn from_json_col<T: DeserializeOwned>(row: &SqliteRow, column: &str) -> Result<T, DbError> {
    let value: String = row.try_get(column)?;
    serde_json::from_str(&value).map_err(DbError::from)
}

fn optional_json_col<T: DeserializeOwned>(
    row: &SqliteRow,
    column: &str,
) -> Result<Option<T>, DbError> {
    let value: Option<String> = row.try_get(column)?;
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(DbError::from)
}

fn bool_to_i64(value: bool) -> i64 {
    i64::from(value)
}

fn i64_to_bool(value: i64) -> bool {
    value != 0
}

fn u32_to_i64(value: u32) -> i64 {
    i64::from(value)
}

fn u64_to_i64(value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(DbError::from)
}

fn i64_to_u64(value: i64) -> Result<u64, DbError> {
    u64::try_from(value).map_err(DbError::from)
}

fn i64_to_u32(value: i64) -> Result<u32, DbError> {
    u32::try_from(value).map_err(DbError::from)
}

fn i64_to_u16(value: i64) -> Result<u16, DbError> {
    u16::try_from(value).map_err(DbError::from)
}

#[cfg(test)]
mod tests {
    use std::io;

    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId,
        AgentSessionState, AgentWorkspace, AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason,
        AuthorizedKeyBootstrapState, ConnectionMode, ConnectionSession, Connector, ConnectorId,
        CredentialId, CredentialKind, CredentialMetadata, EntityState, Environment, EnvironmentId,
        EnvironmentKind, FactSource, Host, HostFact, HostFactId, HostId, HostKind, HostWriteLease,
        KnowledgeItem, KnowledgeItemId, OperationId, OperationOutputArtifact,
        OperationOutputArtifactId, OperationOutputChunk, OperationOutputChunkId, OperationRun,
        OperationState, OperationType, OutputStream, Protocol, PtyBackendCapabilities,
        PtyBackendState, PtyInputEvent, PtyInputEventId, PtyInputEventState, PtyOutputChunk,
        PtyOutputChunkId, PtySession, PtySessionId, RiskLevel, RouteType, SessionId,
        SshChannelKind, SshChannelTransportEvidence, SshFileTransferMode, SshTransportBackend,
        SshTransportCapabilities, SshTransportRuntime, SshTransportRuntimeId,
        SshTransportRuntimeState, SshTransportTelemetry, StateEvent, StateReasonCode,
        StoredCredential, TrustLevel, WorkspaceId, WorkspaceState, now_utc,
    };
    use serde_json::json;

    use super::{ClaimedOperationFinish, Repositories, connect_sqlite, migrate};

    #[tokio::test]
    async fn state_event_cursor_is_monotonic_and_filterable()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let connector_id = ConnectorId::new().to_string();
        let observed_at = now_utc();

        let first = StateEvent {
            id: remote_hosts_domain::StateEventId::new(),
            entity_type: "connector".to_owned(),
            entity_id: connector_id.clone(),
            old_state: EntityState::Unknown,
            new_state: EntityState::Connected,
            reason_code: StateReasonCode::None,
            observed_at,
        };
        repos.state_events.insert(&first).await?;
        let first_cursor = repos.state_events.latest_sequence().await?;

        let unrelated = StateEvent {
            id: remote_hosts_domain::StateEventId::new(),
            entity_type: "connector".to_owned(),
            entity_id: ConnectorId::new().to_string(),
            old_state: EntityState::Unknown,
            new_state: EntityState::Connected,
            reason_code: StateReasonCode::None,
            observed_at,
        };
        repos.state_events.insert(&unrelated).await?;
        let second_cursor = repos.state_events.latest_sequence().await?;

        assert_eq!(first_cursor, 1);
        assert_eq!(second_cursor, 2);
        let records = repos
            .state_events
            .list_after(0, Some("connector"), Some(connector_id.as_str()), 10)
            .await?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence, first_cursor);
        assert_eq!(records[0].event.id, first.id);
        assert!(
            repos
                .state_events
                .list_after(
                    first_cursor,
                    Some("connector"),
                    Some(connector_id.as_str()),
                    10,
                )
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn registry_repositories_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "company-4090-a".to_owned(),
            display_name: "Company 4090 A".to_owned(),
            kind: HostKind::GpuServer,
            owner: Some("ops".to_owned()),
            tags: vec!["gpu".to_owned(), "cuda".to_owned()],
            description: Some("primary gpu box".to_owned()),
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;

        let environment = Environment {
            id: EnvironmentId::new(),
            name: "company-lan".to_owned(),
            kind: EnvironmentKind::CompanyLan,
            description: None,
            trust_level: TrustLevel::Trusted,
            notes: None,
        };
        repos.environments.insert(&environment).await?;

        let connector = Connector {
            id: ConnectorId::new(),
            name: "office-connector".to_owned(),
            environment_id: environment.id,
            host_id: Some(host.id),
            version: "0.1.0".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("company".to_owned()),
        };
        repos.connectors.upsert(&connector).await?;
        let (old_connector_state, updated_connector) = repos
            .connectors
            .update_heartbeat(
                connector.id,
                EntityState::Connected,
                Some("0.2.0"),
                Some("company-wifi"),
                now,
            )
            .await?
            .ok_or_else(|| io::Error::other("connector exists"))?;
        assert_eq!(old_connector_state, EntityState::Healthy);
        assert_eq!(updated_connector.state, EntityState::Connected);
        assert_eq!(updated_connector.version, "0.2.0");
        assert_eq!(
            updated_connector.current_network.as_deref(),
            Some("company-wifi")
        );

        let credential = StoredCredential {
            metadata: CredentialMetadata {
                id: CredentialId::new(),
                name: "4090 ssh".to_owned(),
                kind: CredentialKind::SshPrivateKey,
                username_hint: Some("ops".to_owned()),
                created_at: now,
                updated_at: now,
                last_used_at: None,
            },
            encrypted_blob_json: json!({"version": 1, "ciphertext": "redacted"}),
        };
        repos.credentials.insert(&credential).await?;

        let path = AccessPath {
            id: AccessPathId::new(),
            host_id: host.id,
            environment_id: environment.id,
            connector_id: Some(connector.id),
            protocol: Protocol::Ssh,
            address: "10.0.0.10".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id: credential.metadata.id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 10,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repos.access_paths.insert(&path).await?;

        let loaded_host = repos
            .hosts
            .get(host.id)
            .await?
            .ok_or_else(|| io::Error::other("host exists"))?;
        assert_eq!(loaded_host.tags, host.tags);

        let paths = repos.access_paths.list_enabled_for_host(host.id).await?;
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].address, path.address);
        assert_eq!(
            repos
                .access_paths
                .get(path.id)
                .await?
                .ok_or_else(|| io::Error::other("path exists"))?
                .username,
            "ops"
        );

        let health = AccessPathHealth {
            access_path_id: path.id,
            state: EntityState::Connected,
            last_checked_at: Some(now),
            latency_ms: Some(12),
            failure_count: 0,
            last_error_code: None,
            next_retry_at: None,
        };
        repos.access_path_health.upsert(&health).await?;
        let loaded_health = repos
            .access_path_health
            .get(path.id)
            .await?
            .ok_or_else(|| io::Error::other("health exists"))?;
        assert_eq!(loaded_health.latency_ms, Some(12));

        let bootstrap = AuthorizedKeyBootstrap {
            access_path_id: path.id,
            state: AuthorizedKeyBootstrapState::Deferred,
            reason: Some(AuthorizedKeyBootstrapReason::Timeout),
            public_key_fingerprint: Some("SHA256:test-fingerprint".to_owned()),
            failure_count: 2,
            attempted_at: now,
            next_retry_at: Some(now + time::Duration::minutes(15)),
            updated_at: now,
        };
        repos.authorized_key_bootstrap.upsert(&bootstrap).await?;
        let loaded_bootstrap = repos
            .authorized_key_bootstrap
            .get(path.id)
            .await?
            .ok_or_else(|| io::Error::other("authorized-key bootstrap exists"))?;
        assert_eq!(loaded_bootstrap.state, bootstrap.state);
        assert_eq!(loaded_bootstrap.reason, bootstrap.reason);
        assert_eq!(
            loaded_bootstrap.public_key_fingerprint,
            bootstrap.public_key_fingerprint
        );
        assert_eq!(loaded_bootstrap.failure_count, 2);
        assert_eq!(loaded_bootstrap.next_retry_at, bootstrap.next_retry_at);

        let fact = HostFact {
            id: HostFactId::new(),
            host_id: host.id,
            namespace: "gpu".to_owned(),
            key: "count".to_owned(),
            value_json: json!(4),
            source: FactSource::Probe,
            observed_at: now,
            expires_at: None,
            confidence: 0.9,
        };
        repos.host_facts.insert(&fact).await?;
        assert_eq!(repos.host_facts.list_for_host(host.id).await?.len(), 1);

        let agent_session = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "task-a".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("conversation-a".to_owned()),
            state: AgentSessionState::Active,
            created_at: now,
            last_seen_at: now,
            expires_at: now + time::Duration::hours(24),
        };
        repos.agent_sessions.upsert(&agent_session).await?;
        assert_eq!(
            repos.agent_sessions.get(agent_session.id).await?,
            Some(agent_session.clone())
        );

        let workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(agent_session.id),
            host_id: host.id,
            access_path_id: path.id,
            connector_id: connector.id,
            label: "agent-main".to_owned(),
            cwd: Some("/tmp".to_owned()),
            state: WorkspaceState::Idle,
            policy_profile: "default".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        };
        repos.workspaces.insert(&workspace).await?;
        assert_eq!(repos.workspaces.list_for_host(host.id).await?.len(), 1);
        let scoped_workspaces = repos
            .workspaces
            .list_for_host_and_agent_session(host.id, agent_session.id)
            .await?;
        assert_eq!(scoped_workspaces.len(), 1);
        assert_eq!(scoped_workspaces[0].id, workspace.id);
        let lease_a = HostWriteLease {
            host_id: host.id,
            holder_agent_session_id: agent_session.id,
            holder_workspace_id: workspace.id,
            acquired_at: now,
            heartbeat_at: now,
            expires_at: now + time::Duration::minutes(5),
        };
        assert_eq!(
            repos.host_write_leases.try_acquire(&lease_a, now).await?,
            Some(lease_a)
        );
        let agent_session_b = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "task-b".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("conversation-b".to_owned()),
            state: AgentSessionState::Active,
            created_at: now,
            last_seen_at: now,
            expires_at: now + time::Duration::hours(24),
        };
        repos.agent_sessions.upsert(&agent_session_b).await?;
        let workspace_b = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(agent_session_b.id),
            label: "agent-b".to_owned(),
            ..workspace.clone()
        };
        repos.workspaces.insert(&workspace_b).await?;
        let lease_b = HostWriteLease {
            host_id: host.id,
            holder_agent_session_id: agent_session_b.id,
            holder_workspace_id: workspace_b.id,
            acquired_at: now,
            heartbeat_at: now,
            expires_at: now + time::Duration::minutes(5),
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&lease_b, now)
                .await?
                .is_none()
        );
        let takeover_at = now + time::Duration::minutes(6);
        let lease_b_after_expiry = HostWriteLease {
            acquired_at: takeover_at,
            heartbeat_at: takeover_at,
            expires_at: takeover_at + time::Duration::minutes(5),
            ..lease_b
        };
        assert_eq!(
            repos
                .host_write_leases
                .try_acquire(&lease_b_after_expiry, takeover_at)
                .await?,
            Some(lease_b_after_expiry)
        );
        repos
            .host_write_leases
            .shorten(host.id, agent_session_b.id, takeover_at, now)
            .await?;
        let updated_workspace = repos
            .workspaces
            .update_state(workspace.id, WorkspaceState::Working, now)
            .await?
            .ok_or_else(|| io::Error::other("workspace exists"))?;
        assert_eq!(updated_workspace.state, WorkspaceState::Working);

        let session = ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: path.id,
            connector_id: connector.id,
            state: EntityState::Connected,
            created_at: now,
            last_used_at: now,
            open_channels: 1,
            reused_count: 3,
            failure_count: 0,
            last_error: None,
        };
        repos.connection_sessions.upsert(&session).await?;
        assert_eq!(
            repos
                .connection_sessions
                .list_for_host(host.id)
                .await?
                .len(),
            1
        );

        let pty = PtySession {
            pty_session_id: PtySessionId::new(),
            workspace_id: workspace.id,
            session_id: session.session_id,
            state: WorkspaceState::Idle,
            foreground_process: None,
            cwd: Some("/tmp".to_owned()),
            recent_output_ref: None,
            last_exit_code: None,
            input_allowed: true,
            backend_state: PtyBackendState::Pending,
            backend_capabilities: PtyBackendCapabilities::unknown(),
            transport_evidence: None,
            created_at: now,
            last_activity_at: now,
        };
        repos.pty_sessions.upsert(&pty).await?;
        assert_eq!(
            repos
                .pty_sessions
                .list_for_workspace(workspace.id)
                .await?
                .len(),
            1
        );
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_workspace(workspace.id)
                .await?,
            1
        );
        let closed_pty = repos
            .pty_sessions
            .close(pty.pty_session_id, Some(0), now)
            .await?
            .ok_or_else(|| io::Error::other("pty exists"))?;
        assert_eq!(closed_pty.state, WorkspaceState::Closed);
        assert!(!closed_pty.input_allowed);
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_workspace(workspace.id)
                .await?,
            0
        );
        let pty_chunk = PtyOutputChunk {
            id: PtyOutputChunkId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: workspace.id,
            stream: OutputStream::Stdout,
            sequence: 0,
            redacted_text: "pty hello".to_owned(),
            byte_len: 9,
            truncated: false,
            created_at: now,
        };
        repos.pty_output_chunks.insert(&pty_chunk).await?;
        assert_eq!(
            repos
                .pty_output_chunks
                .next_sequence(pty.pty_session_id)
                .await?,
            1
        );
        let pty_chunks = repos
            .pty_output_chunks
            .list_for_session(pty.pty_session_id, None, 10)
            .await?;
        assert_eq!(pty_chunks.len(), 1);
        assert_eq!(pty_chunks[0].redacted_text, "pty hello");
        let pty_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: workspace.id,
            connector_id: connector.id,
            host_id: host.id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: None,
            input_fingerprint: None,
            state: PtyInputEventState::Queued,
            sequence: repos
                .pty_input_events
                .next_sequence(pty.pty_session_id)
                .await?,
            redacted_input_summary: "11 bytes queued for pty input".to_owned(),
            byte_len: 11,
            requested_by: Some("agent".to_owned()),
            created_at: now,
            claimed_at: None,
            lease_expires_at: None,
            delivered_at: None,
            failed_at: None,
            attempt_count: 0,
            last_error: None,
        };
        repos
            .pty_input_events
            .insert(&pty_input, "echo hello\n")
            .await?;
        let input_events = repos
            .pty_input_events
            .list_for_session(pty.pty_session_id, None, 10)
            .await?;
        assert_eq!(input_events.len(), 1);
        assert_eq!(
            input_events[0].redacted_input_summary,
            pty_input.redacted_input_summary
        );
        let claimed_input = repos
            .pty_input_events
            .claim_next_for_connector(
                connector.id,
                "pty-claim-1",
                now,
                now + time::Duration::seconds(30),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("pty input should be claimable"))?;
        assert_eq!(claimed_input.event.id, pty_input.id);
        assert_eq!(claimed_input.event.state, PtyInputEventState::Claimed);
        assert_eq!(claimed_input.event.attempt_count, 1);
        assert_eq!(claimed_input.input_text, "echo hello\n");
        assert_eq!(claimed_input.claim_token, "pty-claim-1");
        assert!(
            repos
                .pty_input_events
                .claim_next_for_connector(
                    connector.id,
                    "pty-claim-2",
                    now,
                    now + time::Duration::seconds(30),
                    3,
                )
                .await?
                .is_none()
        );
        assert!(
            repos
                .pty_input_events
                .finish_claimed(pty_input.id, "pty-claim-1", now)
                .await?
        );
        let delivered_input = repos
            .pty_input_events
            .get(pty_input.id)
            .await?
            .ok_or_else(|| io::Error::other("pty input exists"))?;
        assert_eq!(delivered_input.state, PtyInputEventState::Delivered);
        assert_eq!(delivered_input.delivered_at, Some(now));
        assert!(
            repos
                .pty_input_events
                .claim_next_for_connector(
                    connector.id,
                    "pty-claim-3",
                    now,
                    now + time::Duration::seconds(30),
                    3,
                )
                .await?
                .is_none()
        );

        let operation = OperationRun {
            id: OperationId::new(),
            host_id: host.id,
            access_path_id: path.id,
            connector_id: connector.id,
            session_id: None,
            workspace_id: Some(workspace.id),
            agent_session_id: workspace.agent_session_id,
            idempotency_key: None,
            requires_write_lease: false,
            operation_type: OperationType::Probe,
            intent: "check gpu".to_owned(),
            state: OperationState::Queued,
            started_at: now,
            finished_at: None,
            exit_code: None,
            timeout_seconds: 30,
            redacted_command_summary: "nvidia-smi".to_owned(),
            command_profile_json: Some(json!({"name": "gpu.nvidia_smi"})),
            transport_evidence: None,
            redacted_output_summary: None,
            log_ref: None,
            attempt_count: 0,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            last_error: None,
        };
        repos.operations.insert(&operation).await?;
        let claimed = repos
            .operations
            .claim_next_for_connector(
                connector.id,
                "claim-1",
                now,
                now + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("operation should be claimable"))?;
        assert_eq!(claimed.id, operation.id);
        assert_eq!(claimed.state, OperationState::Running);
        assert_eq!(claimed.attempt_count, 1);
        assert_eq!(claimed.claim_token.as_deref(), Some("claim-1"));
        assert!(
            repos
                .operations
                .renew_claim(operation.id, "claim-1", now + time::Duration::minutes(10))
                .await?
        );
        assert!(
            !repos
                .operations
                .renew_claim(
                    operation.id,
                    "wrong-claim",
                    now + time::Duration::minutes(10)
                )
                .await?
        );
        assert!(
            repos
                .operations
                .claim_next_for_connector(
                    connector.id,
                    "claim-2",
                    now,
                    now + time::Duration::minutes(5),
                    3,
                )
                .await?
                .is_none()
        );
        let telemetry = SshTransportTelemetry {
            runtime_id: SshTransportRuntimeId::new(),
            backend: SshTransportBackend::Russh,
            state: SshTransportRuntimeState::Ready,
            generation: 1,
            connection_attempt_count: 1,
            successful_handshake_count: 1,
            reuse_count: 2,
            last_handshake_at: Some(now),
            last_validated_at: Some(now),
            capabilities: SshTransportCapabilities::pooled(SshFileTransferMode::Sftp),
        };
        let runtime = SshTransportRuntime {
            access_path_id: path.id,
            connector_id: connector.id,
            telemetry: telemetry.clone(),
            updated_at: now,
        };
        repos.ssh_transport_runtimes.upsert(&runtime).await?;
        assert_eq!(
            repos
                .ssh_transport_runtimes
                .get(path.id, connector.id)
                .await?,
            Some(runtime)
        );
        let evidence =
            SshChannelTransportEvidence::between(SshChannelKind::Exec, None, &telemetry, now);
        assert!(
            repos
                .operations
                .attach_transport_evidence(operation.id, "claim-1", &evidence)
                .await?
        );
        assert!(
            repos
                .operations
                .finish_claimed(ClaimedOperationFinish {
                    id: operation.id,
                    claim_token: "claim-1",
                    state: OperationState::Succeeded,
                    finished_at: now,
                    exit_code: Some(0),
                    redacted_output_summary: Some("ok"),
                    last_error: None,
                },)
                .await?
        );
        assert_eq!(
            repos.operations.list_recent_for_host(host.id, 10).await?[0].state,
            OperationState::Succeeded
        );
        assert_eq!(
            repos
                .operations
                .get(operation.id)
                .await?
                .and_then(|run| run.transport_evidence),
            Some(evidence)
        );
        assert_eq!(
            repos
                .operations
                .list_for_workspace(workspace.id, 10)
                .await?
                .len(),
            1
        );
        let lock_at = now + time::Duration::seconds(1);
        let active_a_lease = HostWriteLease {
            host_id: host.id,
            holder_agent_session_id: agent_session.id,
            holder_workspace_id: workspace.id,
            acquired_at: lock_at,
            heartbeat_at: lock_at,
            expires_at: lock_at + time::Duration::minutes(5),
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&active_a_lease, lock_at)
                .await?
                .is_some()
        );
        let mut mutating_b = operation.clone();
        mutating_b.id = OperationId::new();
        mutating_b.workspace_id = Some(workspace_b.id);
        mutating_b.agent_session_id = Some(agent_session_b.id);
        mutating_b.idempotency_key = Some("agent-b-write".to_owned());
        mutating_b.requires_write_lease = true;
        mutating_b.operation_type = OperationType::Runbook;
        mutating_b.state = OperationState::Queued;
        mutating_b.started_at = lock_at;
        mutating_b.finished_at = None;
        mutating_b.claim_token = None;
        mutating_b.claimed_at = None;
        mutating_b.lease_expires_at = None;
        mutating_b.attempt_count = 0;
        mutating_b.command_profile_json = Some(json!({"name": "shell.posix"}));
        repos.operations.insert(&mutating_b).await?;
        let mut readonly_b = mutating_b.clone();
        readonly_b.id = OperationId::new();
        readonly_b.idempotency_key = Some("agent-b-read".to_owned());
        readonly_b.requires_write_lease = false;
        readonly_b.operation_type = OperationType::ReadonlyExec;
        readonly_b.started_at = lock_at + time::Duration::seconds(1);
        readonly_b.command_profile_json = Some(json!({"name": "host.identity"}));
        repos.operations.insert(&readonly_b).await?;
        let claimed_readonly = repos
            .operations
            .claim_next_for_connector(
                connector.id,
                "claim-readonly-b",
                lock_at,
                lock_at + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("readonly work should bypass foreign write lease"))?;
        assert_eq!(claimed_readonly.id, readonly_b.id);
        repos
            .operations
            .finish_claimed(ClaimedOperationFinish {
                id: readonly_b.id,
                claim_token: "claim-readonly-b",
                state: OperationState::Succeeded,
                finished_at: lock_at,
                exit_code: Some(0),
                redacted_output_summary: Some("ok"),
                last_error: None,
            })
            .await?;
        assert!(
            repos
                .operations
                .claim_next_for_connector(
                    connector.id,
                    "claim-blocked-b",
                    lock_at,
                    lock_at + time::Duration::minutes(5),
                    3,
                )
                .await?
                .is_none()
        );
        repos
            .host_write_leases
            .shorten(host.id, agent_session.id, lock_at, lock_at)
            .await?;
        let claimed_mutation = repos
            .operations
            .claim_next_for_connector(
                connector.id,
                "claim-mutation-b",
                lock_at,
                lock_at + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("mutation should run after lease expiry"))?;
        assert_eq!(claimed_mutation.id, mutating_b.id);

        let chunk = OperationOutputChunk {
            id: OperationOutputChunkId::new(),
            operation_id: operation.id,
            workspace_id: workspace.id,
            stream: OutputStream::System,
            sequence: 0,
            redacted_text: "queued".to_owned(),
            byte_len: 6,
            truncated: false,
            created_at: now,
        };
        repos.operation_output_chunks.insert(&chunk).await?;
        let chunks = repos
            .operation_output_chunks
            .list_for_workspace(workspace.id, Some(operation.id), None, 10)
            .await?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].stream, OutputStream::System);

        let artifact = OperationOutputArtifact {
            id: OperationOutputArtifactId::new(),
            operation_id: operation.id,
            workspace_id: workspace.id,
            stream: OutputStream::Stdout,
            relative_path: format!("{}/{}.log", workspace.id, operation.id),
            byte_len: 1024,
            sha256: "a".repeat(64),
            redacted_preview: "first lines".to_owned(),
            truncated: false,
            created_at: now,
        };
        repos.operation_output_artifacts.insert(&artifact).await?;
        let loaded_artifact = repos
            .operation_output_artifacts
            .get(artifact.id)
            .await?
            .ok_or_else(|| io::Error::other("artifact exists"))?;
        assert_eq!(loaded_artifact.relative_path, artifact.relative_path);
        let artifacts = repos
            .operation_output_artifacts
            .list_for_workspace(workspace.id, Some(operation.id), 10)
            .await?;
        assert_eq!(artifacts.len(), 1);

        let knowledge = KnowledgeItem {
            id: KnowledgeItemId::new(),
            title: "CUDA installed".to_owned(),
            body: "CUDA 12.4 is available on this host".to_owned(),
            source: FactSource::Manual,
            linked_host_ids: vec![host.id],
            linked_access_path_ids: vec![path.id],
            linked_software_ids: Vec::new(),
            linked_operation_ids: Vec::new(),
            tags: vec!["cuda".to_owned()],
            created_at: now,
            updated_at: now,
        };
        repos.knowledge.insert(&knowledge).await?;
        assert_eq!(repos.knowledge.search("CUDA", 10).await?.len(), 1);

        let event = StateEvent {
            id: remote_hosts_domain::StateEventId::new(),
            entity_type: "access_path".to_owned(),
            entity_id: path.id.to_string(),
            old_state: EntityState::Unknown,
            new_state: EntityState::Connected,
            reason_code: StateReasonCode::None,
            observed_at: now,
        };
        repos.state_events.insert(&event).await?;
        let events = repos
            .state_events
            .list_for_entity("access_path", &path.id.to_string(), 10)
            .await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].new_state, EntityState::Connected);

        Ok(())
    }
}

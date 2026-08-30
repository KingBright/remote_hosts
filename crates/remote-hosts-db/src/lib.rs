//! Database access layer and migrations.

use std::{collections::BTreeSet, future::Future, io, str::FromStr, time::Duration as StdDuration};

use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId, AgentWorkspace,
    AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason, AuthorizedKeyBootstrapState,
    ClaimedPtyInputEvent, ConnectionSession, Connector, ConnectorId, CredentialBinding,
    CredentialBindingView, CredentialId, CredentialKind, CredentialMetadata, EntityState,
    Environment, EnvironmentId, Host, HostFact, HostId, HostWriteLease, InstanceIdentity,
    InstancePeer, InstancePeerId, InstancePeerState, InstanceSyncCollection, InstanceSyncConflict,
    KnowledgeItem, OperationId, OperationOutputArtifact, OperationOutputArtifactId,
    OperationOutputChunk, OperationRun, OperationState, PtyBackendCapabilities, PtyBackendState,
    PtyInputEvent, PtyInputEventId, PtyInputEventState, PtyOutputChunk, PtySession, PtySessionId,
    SequencedStateEvent, SessionId, SoftwareInstall, SshChannelTransportEvidence,
    SshTransportRuntime, SshTransportRuntimeId, SshTransportRuntimeState, StateEvent,
    StateReasonCode, StoredCredential, TopologyEdge, TopologyNode, TopologyNodeId, TopologySyncRun,
    TopologySyncRunId, WorkspaceId, WorkspaceState,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;
use time::OffsetDateTime;

/// Embedded database migrator.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Maximum time one `SQLite` statement waits for the current writer before returning `BUSY`.
pub const SQLITE_BUSY_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const SQLITE_CONTENTION_RETRY_DELAYS: [StdDuration; 2] =
    [StdDuration::from_millis(25), StdDuration::from_millis(100)];

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
    /// Compact output serialization error.
    #[error("output serialization error: {0}")]
    Postcard(#[from] postcard::Error),
    /// Output compression or decompression error.
    #[error("output compression error: {0}")]
    Io(#[from] io::Error),
    /// Compressed output segment failed structural validation.
    #[error("invalid compressed output segment: {0}")]
    InvalidOutputSegment(String),
    /// A multi-resource write lease request contains inconsistent ownership metadata.
    #[error("invalid host write lease set: {0}")]
    InvalidHostWriteLeaseSet(String),
}

impl DbError {
    /// Returns whether the error is `SQLite`'s transient single-writer contention signal.
    pub fn is_sqlite_contention(&self) -> bool {
        let Self::Sqlx(sqlx::Error::Database(error)) = self else {
            return false;
        };
        let code = error.code();
        if code
            .as_deref()
            .is_some_and(|code| matches!(code, "5" | "6" | "SQLITE_BUSY" | "SQLITE_LOCKED"))
        {
            return true;
        }
        let message = error.message().to_ascii_lowercase();
        message.contains("database is locked") || message.contains("database table is locked")
    }
}

/// Retries one idempotent database action when `SQLite` reports transient writer contention.
///
/// The connection-level busy timeout handles ordinary short overlap. These additional bounded
/// yields cover `SQLITE_LOCKED` variants and contention that crosses one timeout boundary.
///
/// # Errors
///
/// Returns the operation error immediately when it is not transient `SQLite` contention, or after
/// the bounded contention retry budget is exhausted.
pub async fn retry_sqlite_contention<T, F, Fut>(mut operation: F) -> Result<T, DbError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DbError>>,
{
    let mut delays = SQLITE_CONTENTION_RETRY_DELAYS.into_iter();
    loop {
        match operation().await {
            Err(error) if error.is_sqlite_contention() => {
                let Some(delay) = delays.next() else {
                    return Err(error);
                };
                tokio::time::sleep(delay).await;
            }
            result => return result,
        }
    }
}

/// Result of one bounded legacy PTY output compaction transaction.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyOutputCompactionBatch {
    /// Legacy chunk rows moved into compressed storage.
    pub legacy_chunks: u64,
    /// Compressed segment rows written by this batch.
    pub segments_written: u64,
    /// UTF-8 text bytes represented by the legacy rows.
    pub original_storage_bytes: u64,
    /// Compact binary bytes before Zstandard compression.
    pub encoded_bytes: u64,
    /// Bytes stored in the compressed payload.
    pub compressed_bytes: u64,
}

/// Aggregate physical and logical size counters for durable PTY output.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PtyOutputStorageStats {
    /// Rows still using the legacy one-chunk-per-row table.
    pub legacy_chunks: u64,
    /// UTF-8 bytes retained in legacy rows.
    pub legacy_text_bytes: u64,
    /// Rows in compressed segment storage.
    pub compressed_segments: u64,
    /// Logical chunks represented by compressed segments.
    pub compressed_chunks: u64,
    /// UTF-8 bytes represented by compressed segments.
    pub compressed_text_bytes: u64,
    /// Compact binary bytes before Zstandard compression.
    pub encoded_bytes: u64,
    /// Physical Zstandard payload bytes.
    pub compressed_bytes: u64,
}

/// Result of one bounded legacy command-output compaction transaction.
pub type OperationOutputCompactionBatch = PtyOutputCompactionBatch;

/// Aggregate physical and logical size counters for durable command output.
pub type OperationOutputStorageStats = PtyOutputStorageStats;

/// `SQLite` page counters used to report real file reclamation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SqliteStorageStats {
    /// Database page size in bytes.
    pub page_size: u64,
    /// Total pages in the main database file.
    pub page_count: u64,
    /// Currently reusable pages inside the main database file.
    pub freelist_count: u64,
    /// Main database bytes represented by all pages.
    pub database_bytes: u64,
    /// Bytes represented by reusable pages.
    pub reusable_bytes: u64,
}

/// Scheduler-visible channel reservations for one SSH access path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccessPathChannelUsage {
    /// Non-expired operation claims currently reserving a channel.
    pub running_operations: u32,
    /// Persistent PTY backends currently holding a channel.
    pub active_ptys: u32,
    /// Activatable PTYs waiting to reserve a channel.
    pub pending_ptys: u32,
}

/// Counts durable work that would be interrupted by restarting local services.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActiveWorkSummary {
    /// Operations waiting for or currently using a connector channel.
    pub queued_or_running_operations: i64,
    /// PTYs waiting for activation or backed by a live connector process.
    pub pending_or_active_ptys: i64,
    /// PTY input events not yet delivered to the live backend.
    pub queued_or_claimed_pty_inputs: i64,
    /// Unexpired mutation coordination leases.
    pub unexpired_write_leases: i64,
}

/// Durable work removed after its owning Workspace was explicitly closed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClosedWorkspaceWorkReconciliation {
    /// Queued or running operations transitioned to cancelled.
    pub cancelled_operations: u64,
    /// Write leases released because their holder Workspace was closed with active work.
    pub released_write_leases: u64,
}

/// Logical workspace capacity after classifying expired, safely reapable records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceCapacityStatus {
    /// Idle or working workspace rows currently recorded for the host.
    pub recorded_active: u32,
    /// Active rows that still count after excluding safely reapable history.
    pub effective_active: u32,
    /// Expired active rows with no queued/running operation or active PTY.
    pub expired_reapable: u32,
    /// Effective active rows owned by the requesting agent session.
    pub current_agent_session_active: u32,
    /// Effective active rows owned by other or legacy agent sessions.
    pub other_agent_sessions_active: u32,
}

impl ActiveWorkSummary {
    /// Returns true when a service restart will not interrupt durable conversation work.
    pub fn is_idle(&self) -> bool {
        self.queued_or_running_operations == 0
            && self.pending_or_active_ptys == 0
            && self.queued_or_claimed_pty_inputs == 0
            && self.unexpired_write_leases == 0
    }
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
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(SQLITE_BUSY_TIMEOUT);

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
    /// Instance identity and approved peer-sync state.
    pub instance_sync: InstanceSyncRepository,
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
            credential_bindings: CredentialBindingRepository::new(pool.clone()),
            instance_sync: InstanceSyncRepository::new(pool),
        }
    }

    /// Returns whether new PTY and command output may use compressed-only storage.
    ///
    /// The setting defaults to disabled so replacing the API or connector binary cannot make
    /// output disappear from already-running legacy MCP children.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable setting cannot be queried.
    pub async fn compressed_output_writes_enabled(&self) -> Result<bool, DbError> {
        compressed_output_writes_enabled(&self.operations.pool).await
    }

    /// Activates compressed-only output writes after all MCP clients have been reloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable setting cannot be updated.
    pub async fn activate_compressed_output_writes(&self) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO system_settings(setting_key, value)
            VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET value = excluded.value
            ",
        )
        .bind(COMPRESSED_OUTPUT_WRITES_SETTING)
        .bind(COMPRESSED_OUTPUT_WRITES_ENABLED)
        .execute(&self.operations.pool)
        .await?;
        Ok(())
    }

    /// Counts active work that must drain before a local service restart.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity query fails.
    pub async fn active_work_summary(&self) -> Result<ActiveWorkSummary, DbError> {
        let row = sqlx::query(
            r#"
            SELECT
                (SELECT COUNT(*) FROM operation_runs active_operation
                 LEFT JOIN agent_workspaces operation_workspace
                   ON operation_workspace.workspace_id = active_operation.workspace_id
                 WHERE active_operation.state_json IN ('"queued"', '"running"')
                   AND (
                     active_operation.workspace_id IS NULL
                     OR operation_workspace.state_json != '"closed"'
                   )) AS operations,
                (SELECT COUNT(*) FROM pty_sessions ps
                 JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                 WHERE aw.state_json IN ('"idle"', '"working"', '"blocked"')
                   AND ps.backend_state_json IN ('"pending"', '"active"')
                   AND ps.input_allowed = 1) AS ptys,
                (SELECT COUNT(*) FROM pty_input_events active_input
                 JOIN agent_workspaces input_workspace
                   ON input_workspace.workspace_id = active_input.workspace_id
                 WHERE active_input.state_json IN ('"queued"', '"claimed"')
                   AND input_workspace.state_json IN ('"idle"', '"working"', '"blocked"'))
                  AS pty_inputs,
                (SELECT COUNT(*) FROM host_write_leases active_lease
                 JOIN agent_workspaces lease_workspace
                   ON lease_workspace.workspace_id = active_lease.holder_workspace_id
                 WHERE active_lease.expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                   AND lease_workspace.state_json != '"closed"')
                  AS write_leases
            "#,
        )
        .fetch_one(&self.operations.pool)
        .await?;
        Ok(ActiveWorkSummary {
            queued_or_running_operations: row.try_get("operations")?,
            pending_or_active_ptys: row.try_get("ptys")?,
            queued_or_claimed_pty_inputs: row.try_get("pty_inputs")?,
            unexpired_write_leases: row.try_get("write_leases")?,
        })
    }

    /// Cancels operations and releases write leases owned by explicitly closed Workspaces.
    ///
    /// The transaction invalidates connector claim renewal before releasing coordination leases,
    /// so a resident older connector cannot revive work after its owner closed the Workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if the reconciliation transaction cannot complete.
    pub async fn reconcile_closed_workspace_work(
        &self,
        observed_at: OffsetDateTime,
    ) -> Result<ClosedWorkspaceWorkReconciliation, DbError> {
        let mut transaction = self.operations.pool.begin().await?;
        let released_write_leases = sqlx::query(
            r#"
            DELETE FROM host_write_leases
            WHERE holder_workspace_id IN (
                SELECT closed_workspace.workspace_id
                FROM agent_workspaces closed_workspace
                WHERE closed_workspace.state_json = '"closed"'
                  AND EXISTS (
                    SELECT 1
                    FROM operation_runs active_operation
                    WHERE active_operation.workspace_id = closed_workspace.workspace_id
                      AND active_operation.state_json IN ('"queued"', '"running"')
                  )
              )
            "#,
        )
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let cancelled_operations = sqlx::query(
            r#"
            UPDATE operation_runs
            SET state_json = '"cancelled"',
                finished_at = ?,
                exit_code = NULL,
                redacted_output_summary = 'cancelled because Workspace was closed',
                last_error = 'workspace_closed',
                claim_token = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL
            WHERE state_json IN ('"queued"', '"running"')
              AND workspace_id IN (
                SELECT workspace_id
                FROM agent_workspaces
                WHERE state_json = '"closed"'
              )
            "#,
        )
        .bind(observed_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(ClosedWorkspaceWorkReconciliation {
            cancelled_operations,
            released_write_leases,
        })
    }

    /// Returns physical `SQLite` page and freelist counters.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` pragmas cannot be queried or converted.
    pub async fn sqlite_storage_stats(&self) -> Result<SqliteStorageStats, DbError> {
        let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&self.operations.pool)
            .await?;
        let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&self.operations.pool)
            .await?;
        let freelist_count: i64 = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(&self.operations.pool)
            .await?;
        let page_size = i64_to_u64(page_size)?;
        let page_count = i64_to_u64(page_count)?;
        let freelist_count = i64_to_u64(freelist_count)?;
        Ok(SqliteStorageStats {
            page_size,
            page_count,
            freelist_count,
            database_bytes: page_size.saturating_mul(page_count),
            reusable_bytes: page_size.saturating_mul(freelist_count),
        })
    }

    /// Updates `SQLite` planner statistics and optionally rebuilds the file to reclaim free pages.
    ///
    /// # Errors
    ///
    /// Returns an error if optimization, vacuuming, or WAL checkpointing fails.
    pub async fn optimize_sqlite(&self, vacuum: bool) -> Result<(), DbError> {
        sqlx::query("PRAGMA optimize")
            .execute(&self.operations.pool)
            .await?;
        if vacuum {
            sqlx::query("VACUUM").execute(&self.operations.pool).await?;
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .fetch_all(&self.operations.pool)
                .await?;
        } else {
            sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .fetch_all(&self.operations.pool)
                .await?;
        }
        Ok(())
    }
}

async fn compressed_output_writes_enabled(pool: &SqlitePool) -> Result<bool, DbError> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM system_settings WHERE setting_key = ?")
            .bind(COMPRESSED_OUTPUT_WRITES_SETTING)
            .fetch_optional(pool)
            .await?;
    Ok(value.as_deref() == Some(COMPRESSED_OUTPUT_WRITES_ENABLED))
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

    /// Returns scheduler-visible channel usage for one access path.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or integer conversion fails.
    pub async fn channel_usage(
        &self,
        access_path_id: AccessPathId,
        observed_at: OffsetDateTime,
    ) -> Result<AccessPathChannelUsage, DbError> {
        let row = sqlx::query(
            r"
            SELECT
                (
                    SELECT COUNT(*)
                    FROM operation_runs op
                    WHERE op.access_path_id = ?
                      AND op.state_json = ?
                      AND op.lease_expires_at IS NOT NULL
                      AND op.lease_expires_at > ?
                ) AS running_operations,
                (
                    SELECT COUNT(*)
                    FROM pty_sessions ps
                    JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                    WHERE aw.access_path_id = ?
                      AND ps.backend_state_json = ?
                      AND ps.input_allowed = 1
                      AND ps.state_json IN (?, ?)
                      AND aw.state_json IN (?, ?, ?)
                ) AS active_ptys,
                (
                    SELECT COUNT(*)
                    FROM pty_sessions ps
                    JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                    WHERE aw.access_path_id = ?
                      AND ps.backend_state_json = ?
                      AND ps.input_allowed = 1
                      AND ps.state_json IN (?, ?)
                      AND aw.state_json IN (?, ?, ?)
                ) AS pending_ptys
            ",
        )
        .bind(access_path_id.to_string())
        .bind(to_json(&OperationState::Running)?)
        .bind(observed_at)
        .bind(access_path_id.to_string())
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(access_path_id.to_string())
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .fetch_one(&self.pool)
        .await?;
        Ok(AccessPathChannelUsage {
            running_operations: i64_to_u32(row.try_get("running_operations")?)?,
            active_ptys: i64_to_u32(row.try_get("active_ptys")?)?,
            pending_ptys: i64_to_u32(row.try_get("pending_ptys")?)?,
        })
    }

    /// Upgrades the historical one-channel default after a new connector process starts.
    ///
    /// The migration records whether legacy rows existed but deliberately leaves them unchanged
    /// while an old connector may still be running with one-channel in-memory semaphores.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be completed.
    pub async fn upgrade_legacy_channel_default(&self) -> Result<u64, DbError> {
        let mut transaction = self.pool.begin().await?;
        let state: Option<String> = sqlx::query_scalar(
            r"
            SELECT value
            FROM system_settings
            WHERE setting_key = 'legacy_channel_default_v1'
            ",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if state.as_deref() != Some("pending") {
            transaction.commit().await?;
            return Ok(0);
        }
        let updated = sqlx::query(
            r"
            UPDATE access_paths
            SET max_concurrent_channels = 8
            WHERE max_concurrent_channels = 1
            ",
        )
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        sqlx::query(
            r"
            UPDATE system_settings
            SET value = 'done'
            WHERE setting_key = 'legacy_channel_default_v1'
              AND value = 'pending'
            ",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(updated)
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

    /// Atomically records a newly opened channel on a logical connection session.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn open_channel(
        &self,
        session: &ConnectionSession,
        reused: bool,
        reset_failures: bool,
    ) -> Result<ConnectionSession, DbError> {
        let row = sqlx::query(
            r"
            INSERT INTO connection_sessions (
                session_id, access_path_id, connector_id, state_json, created_at, last_used_at,
                open_channels, reused_count, failure_count, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                state_json = excluded.state_json,
                last_used_at = excluded.last_used_at,
                open_channels = connection_sessions.open_channels + 1,
                reused_count = connection_sessions.reused_count + excluded.reused_count,
                failure_count = CASE
                    WHEN ? THEN 0
                    ELSE connection_sessions.failure_count
                END,
                last_error = CASE
                    WHEN ? THEN NULL
                    ELSE connection_sessions.last_error
                END
            RETURNING *
            ",
        )
        .bind(session.session_id.to_string())
        .bind(session.access_path_id.to_string())
        .bind(session.connector_id.to_string())
        .bind(to_json(&session.state)?)
        .bind(session.created_at)
        .bind(session.last_used_at)
        .bind(i64::from(reused))
        .bind(if reset_failures {
            0_i64
        } else {
            u32_to_i64(session.failure_count)
        })
        .bind(if reset_failures {
            None
        } else {
            session.last_error.as_deref()
        })
        .bind(reset_failures)
        .bind(reset_failures)
        .fetch_one(&self.pool)
        .await?;
        row_to_connection_session(&row)
    }

    /// Atomically releases one open channel while preserving the session health state.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn close_channel(
        &self,
        session_id: SessionId,
        observed_at: OffsetDateTime,
    ) -> Result<Option<ConnectionSession>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE connection_sessions
            SET last_used_at = ?,
                open_channels = MAX(open_channels - 1, 0)
            WHERE session_id = ?
            RETURNING *
            ",
        )
        .bind(observed_at)
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_connection_session).transpose()
    }

    /// Atomically releases a successful channel and clears prior connection failures.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    pub async fn close_channel_success(
        &self,
        session_id: SessionId,
        observed_at: OffsetDateTime,
    ) -> Result<Option<ConnectionSession>, DbError> {
        let row = sqlx::query(
            r"
            UPDATE connection_sessions
            SET state_json = ?,
                last_used_at = ?,
                open_channels = MAX(open_channels - 1, 0),
                failure_count = 0,
                last_error = NULL
            WHERE session_id = ?
            RETURNING *
            ",
        )
        .bind(to_json(&EntityState::Connected)?)
        .bind(observed_at)
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_connection_session).transpose()
    }

    /// Atomically records a connection failure and optionally releases its channel reservation.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the update.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_failure(
        &self,
        session_id: SessionId,
        observed_at: OffsetDateTime,
        state: EntityState,
        last_error: &str,
        increment_failure: bool,
        close_channel: bool,
        circuit_breaker_eligible: bool,
        circuit_breaker_threshold: u32,
    ) -> Result<Option<ConnectionSession>, DbError> {
        let increment = i64::from(increment_failure);
        let row = sqlx::query(
            r"
            UPDATE connection_sessions
            SET state_json = CASE
                    WHEN ? AND failure_count + ? >= ? THEN ?
                    ELSE ?
                END,
                last_used_at = ?,
                open_channels = CASE
                    WHEN ? THEN MAX(open_channels - 1, 0)
                    ELSE open_channels
                END,
                failure_count = failure_count + ?,
                last_error = ?
            WHERE session_id = ?
            RETURNING *
            ",
        )
        .bind(circuit_breaker_eligible)
        .bind(increment)
        .bind(u32_to_i64(circuit_breaker_threshold))
        .bind(to_json(&EntityState::CircuitOpen)?)
        .bind(to_json(&state)?)
        .bind(observed_at)
        .bind(close_channel)
        .bind(increment)
        .bind(last_error)
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_connection_session).transpose()
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

    /// Gets a bounded set of Agent sessions in one query.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_by_ids(
        &self,
        ids: &BTreeSet<AgentSessionId>,
    ) -> Result<Vec<AgentSession>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            r"
            SELECT id, client_kind, client_instance_id, project_key, conversation_key,
                   state_json, created_at, last_seen_at, expires_at
            FROM agent_sessions
            WHERE id IN (
            ",
        );
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(row_to_agent_session).collect()
    }

    /// Persists expiry for a bounded number of stale active Agent Sessions.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn reconcile_expired(
        &self,
        observed_at: OffsetDateTime,
        limit: u32,
    ) -> Result<u64, DbError> {
        let result = sqlx::query(
            r"
            UPDATE agent_sessions
            SET state_json = ?
            WHERE id IN (
                SELECT id
                FROM agent_sessions
                WHERE state_json = ?
                  AND julianday(expires_at) <= julianday(?)
                ORDER BY expires_at ASC
                LIMIT ?
            )
            ",
        )
        .bind(to_json(&remote_hosts_domain::AgentSessionState::Expired)?)
        .bind(to_json(&remote_hosts_domain::AgentSessionState::Active)?)
        .bind(observed_at)
        .bind(i64::from(limit.max(1)))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
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
                state_json, policy_profile, coordination_scope, created_at, last_activity_at,
                ttl_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        .bind(&workspace.coordination_scope)
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
                state_json, policy_profile, coordination_scope, created_at, last_activity_at,
                ttl_seconds
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
                agent_session_id = excluded.agent_session_id,
                label = excluded.label,
                cwd = excluded.cwd,
                state_json = excluded.state_json,
                policy_profile = excluded.policy_profile,
                coordination_scope = excluded.coordination_scope,
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
        .bind(&workspace.coordination_scope)
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
                   cwd, state_json, policy_profile, coordination_scope, created_at,
                   last_activity_at, ttl_seconds
            FROM agent_workspaces
            WHERE workspace_id = ?
            ",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_agent_workspace).transpose()
    }

    /// Gets a bounded set of Workspaces in one query.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_by_ids(
        &self,
        ids: &BTreeSet<WorkspaceId>,
    ) -> Result<Vec<AgentWorkspace>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            r"
            SELECT workspace_id, agent_session_id, host_id, access_path_id, connector_id, label,
                   cwd, state_json, policy_profile, coordination_scope, created_at,
                   last_activity_at, ttl_seconds
            FROM agent_workspaces
            WHERE workspace_id IN (
            ",
        );
        let mut separated = query.separated(", ");
        for id in ids {
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(row_to_agent_workspace).collect()
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
                   cwd, state_json, policy_profile, coordination_scope, created_at,
                   last_activity_at, ttl_seconds
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
                   cwd, state_json, policy_profile, coordination_scope, created_at,
                   last_activity_at, ttl_seconds
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

    /// Classifies logical workspace capacity without mutating history.
    ///
    /// An expired workspace remains effective while it owns queued/running work or an active PTY.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn capacity_for_host(
        &self,
        host_id: HostId,
        current_agent_session_id: Option<AgentSessionId>,
        observed_at: OffsetDateTime,
    ) -> Result<WorkspaceCapacityStatus, DbError> {
        let row = sqlx::query(
            r"
            WITH classified AS (
                SELECT aw.agent_session_id,
                       CASE WHEN (
                           (
                               julianday(aw.last_activity_at)
                                   + (CAST(aw.ttl_seconds AS REAL) / 86400.0)
                                   <= julianday(?)
                               OR (
                                   session.id IS NOT NULL
                                   AND (
                                       julianday(session.expires_at) <= julianday(?)
                                       OR session.state_json IN (?, ?)
                                   )
                               )
                           )
                           AND NOT EXISTS (
                               SELECT 1
                               FROM operation_runs operation
                               WHERE operation.workspace_id = aw.workspace_id
                                 AND operation.state_json IN (?, ?)
                           )
                           AND NOT EXISTS (
                               SELECT 1
                               FROM pty_sessions pty
                               WHERE pty.workspace_id = aw.workspace_id
                                 AND pty.state_json IN (?, ?)
                                 AND pty.input_allowed = 1
                                 AND pty.backend_state_json IN (?, ?)
                           )
                       ) THEN 1 ELSE 0 END AS reapable
                FROM agent_workspaces aw
                LEFT JOIN agent_sessions session ON session.id = aw.agent_session_id
                WHERE aw.host_id = ?
                  AND aw.state_json IN (?, ?, ?)
            )
            SELECT COUNT(*) AS recorded_active,
                   COALESCE(SUM(CASE WHEN reapable = 0 THEN 1 ELSE 0 END), 0)
                       AS effective_active,
                   COALESCE(SUM(reapable), 0) AS expired_reapable,
                   COALESCE(SUM(
                       CASE WHEN reapable = 0 AND agent_session_id = ? THEN 1 ELSE 0 END
                   ), 0) AS current_agent_session_active
            FROM classified
            ",
        )
        .bind(observed_at)
        .bind(observed_at)
        .bind(to_json(&remote_hosts_domain::AgentSessionState::Expired)?)
        .bind(to_json(&remote_hosts_domain::AgentSessionState::Closed)?)
        .bind(to_json(&OperationState::Queued)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(host_id.to_string())
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(current_agent_session_id.map(|id| id.to_string()))
        .fetch_one(&self.pool)
        .await?;
        let recorded_active = i64_to_u32(row.try_get("recorded_active")?)?;
        let effective_active = i64_to_u32(row.try_get("effective_active")?)?;
        let expired_reapable = i64_to_u32(row.try_get("expired_reapable")?)?;
        let current_agent_session_active =
            i64_to_u32(row.try_get("current_agent_session_active")?)?;
        Ok(WorkspaceCapacityStatus {
            recorded_active,
            effective_active,
            expired_reapable,
            current_agent_session_active,
            other_agent_sessions_active: effective_active
                .saturating_sub(current_agent_session_active),
        })
    }

    /// Closes a bounded number of expired workspaces that own no active durable work.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn reconcile_expired_for_host(
        &self,
        host_id: HostId,
        observed_at: OffsetDateTime,
        limit: u32,
    ) -> Result<u64, DbError> {
        self.reconcile_expired_matching(Some(host_id), observed_at, limit)
            .await
    }

    /// Closes a bounded number of expired workspaces across all hosts.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn reconcile_expired(
        &self,
        observed_at: OffsetDateTime,
        limit: u32,
    ) -> Result<u64, DbError> {
        self.reconcile_expired_matching(None, observed_at, limit)
            .await
    }

    async fn reconcile_expired_matching(
        &self,
        host_id: Option<HostId>,
        observed_at: OffsetDateTime,
        limit: u32,
    ) -> Result<u64, DbError> {
        let mut query = QueryBuilder::<Sqlite>::new(
            r"
            UPDATE agent_workspaces
            SET state_json = ",
        );
        query.push_bind(to_json(&WorkspaceState::Closed)?);
        query.push(", last_activity_at = ");
        query.push_bind(observed_at);
        query.push(
            r"
            WHERE workspace_id IN (
                SELECT aw.workspace_id
                FROM agent_workspaces aw
                LEFT JOIN agent_sessions session ON session.id = aw.agent_session_id
                WHERE aw.state_json IN (",
        );
        query.push_bind(to_json(&WorkspaceState::Idle)?);
        query.push(", ");
        query.push_bind(to_json(&WorkspaceState::Working)?);
        query.push(", ");
        query.push_bind(to_json(&WorkspaceState::Blocked)?);
        query.push(") AND (");
        query.push(
            r"
                    julianday(aw.last_activity_at)
                        + (CAST(aw.ttl_seconds AS REAL) / 86400.0)
                        <= julianday(",
        );
        query.push_bind(observed_at);
        query.push(") OR (session.id IS NOT NULL AND (julianday(session.expires_at) <= julianday(");
        query.push_bind(observed_at);
        query.push(") OR session.state_json IN (");
        query.push_bind(to_json(&remote_hosts_domain::AgentSessionState::Expired)?);
        query.push(", ");
        query.push_bind(to_json(&remote_hosts_domain::AgentSessionState::Closed)?);
        query.push("))))");
        if let Some(host_id) = host_id {
            query.push(" AND aw.host_id = ");
            query.push_bind(host_id.to_string());
        }
        query.push(
            r"
                AND NOT EXISTS (
                    SELECT 1
                    FROM operation_runs operation
                    WHERE operation.workspace_id = aw.workspace_id
                      AND operation.state_json IN (",
        );
        query.push_bind(to_json(&OperationState::Queued)?);
        query.push(", ");
        query.push_bind(to_json(&OperationState::Running)?);
        query.push(
            r"))
                AND NOT EXISTS (
                    SELECT 1
                    FROM pty_sessions pty
                    WHERE pty.workspace_id = aw.workspace_id
                      AND pty.state_json IN (",
        );
        query.push_bind(to_json(&WorkspaceState::Idle)?);
        query.push(", ");
        query.push_bind(to_json(&WorkspaceState::Working)?);
        query.push(") AND pty.input_allowed = 1 AND pty.backend_state_json IN (");
        query.push_bind(to_json(&PtyBackendState::Pending)?);
        query.push(", ");
        query.push_bind(to_json(&PtyBackendState::Active)?);
        query.push(") ) ORDER BY aw.last_activity_at ASC LIMIT ");
        query.push_bind(i64::from(limit.max(1)));
        query.push(")");
        let result = query.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Inserts a workspace only while the host remains below the logical capacity limit.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert_below_active_limit(
        &self,
        workspace: &AgentWorkspace,
        active_limit: u32,
    ) -> Result<bool, DbError> {
        let result = sqlx::query(
            r"
            INSERT INTO agent_workspaces (
                workspace_id, agent_session_id, host_id, access_path_id, connector_id, label, cwd,
                state_json, policy_profile, coordination_scope, created_at, last_activity_at,
                ttl_seconds
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE (
                SELECT COUNT(*)
                FROM agent_workspaces
                WHERE host_id = ? AND state_json IN (?, ?, ?)
            ) < ?
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
        .bind(&workspace.coordination_scope)
        .bind(workspace.created_at)
        .bind(workspace.last_activity_at)
        .bind(u64_to_i64(workspace.ttl_seconds)?)
        .bind(workspace.host_id.to_string())
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(i64::from(active_limit))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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
    /// Acquires or refreshes a scoped host write lease when no foreign overlapping lease is live.
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
        let mut acquired = self
            .try_acquire_many(std::slice::from_ref(lease), observed_at)
            .await?;
        Ok(acquired.as_mut().and_then(Vec::pop))
    }

    /// Atomically acquires or refreshes an exact set of scoped host write leases.
    ///
    /// Returns `None` without retaining any partial leases when one requested scope overlaps a
    /// live lease held by another agent session.
    ///
    /// # Errors
    ///
    /// Returns an error if the set is empty, mixes holders, or the database operation fails.
    pub async fn try_acquire_many(
        &self,
        leases: &[HostWriteLease],
        observed_at: OffsetDateTime,
    ) -> Result<Option<Vec<HostWriteLease>>, DbError> {
        let Some(first) = leases.first() else {
            return Err(DbError::InvalidHostWriteLeaseSet(
                "at least one scope is required".to_owned(),
            ));
        };
        if leases.iter().any(|lease| {
            lease.host_id != first.host_id
                || lease.holder_agent_session_id != first.holder_agent_session_id
                || lease.holder_workspace_id != first.holder_workspace_id
        }) {
            return Err(DbError::InvalidHostWriteLeaseSet(
                "all scopes must share one host, agent session, and workspace".to_owned(),
            ));
        }

        let mut transaction = self.pool.begin().await?;
        let mut acquired = Vec::with_capacity(leases.len());
        for lease in leases {
            let row = sqlx::query(
                r"
            INSERT INTO host_write_leases (
                host_id, coordination_scope, holder_agent_session_id, holder_workspace_id,
                acquired_at, heartbeat_at, expires_at
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM host_write_leases conflicting
                WHERE conflicting.host_id = ?
                  AND conflicting.expires_at > ?
                  AND conflicting.holder_agent_session_id != ?
                  AND (
                    conflicting.coordination_scope = 'host'
                    OR ? = 'host'
                    OR conflicting.coordination_scope = ?
                    OR substr(
                        conflicting.coordination_scope,
                        1,
                        length(?) + 1
                    ) = ? || '/'
                    OR substr(
                        ?,
                        1,
                        length(conflicting.coordination_scope) + 1
                    ) = conflicting.coordination_scope || '/'
                  )
            )
            ON CONFLICT(host_id, coordination_scope) DO UPDATE SET
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
            .bind(&lease.coordination_scope)
            .bind(lease.holder_agent_session_id.to_string())
            .bind(lease.holder_workspace_id.to_string())
            .bind(lease.acquired_at)
            .bind(lease.heartbeat_at)
            .bind(lease.expires_at)
            .bind(lease.host_id.to_string())
            .bind(observed_at)
            .bind(lease.holder_agent_session_id.to_string())
            .bind(&lease.coordination_scope)
            .bind(&lease.coordination_scope)
            .bind(&lease.coordination_scope)
            .bind(&lease.coordination_scope)
            .bind(&lease.coordination_scope)
            .bind(observed_at)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                transaction.rollback().await?;
                return Ok(None);
            };
            acquired.push(row_to_host_write_lease(&row)?);
        }
        transaction.commit().await?;
        Ok(Some(acquired))
    }

    /// Lists active write leases for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or decoding fails.
    pub async fn list_active(
        &self,
        host_id: HostId,
        observed_at: OffsetDateTime,
    ) -> Result<Vec<HostWriteLease>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM host_write_leases
            WHERE host_id = ? AND expires_at > ?
            ORDER BY coordination_scope ASC
            ",
        )
        .bind(host_id.to_string())
        .bind(observed_at)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_host_write_lease).collect()
    }

    /// Renews a lease only while the expected agent session still owns it.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn renew(
        &self,
        host_id: HostId,
        coordination_scope: &str,
        agent_session_id: AgentSessionId,
        workspace_id: WorkspaceId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        self.renew_many(
            host_id,
            &[coordination_scope.to_owned()],
            agent_session_id,
            workspace_id,
            heartbeat_at,
            expires_at,
        )
        .await
    }

    /// Atomically renews every exact scope while the expected agent session still owns them.
    ///
    /// # Errors
    ///
    /// Returns an error if the database transaction cannot complete.
    pub async fn renew_many(
        &self,
        host_id: HostId,
        coordination_scopes: &[String],
        agent_session_id: AgentSessionId,
        workspace_id: WorkspaceId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        if coordination_scopes.is_empty() {
            return Ok(false);
        }
        let mut transaction = self.pool.begin().await?;
        for coordination_scope in coordination_scopes {
            let result = sqlx::query(
                r"
                UPDATE host_write_leases
                SET holder_workspace_id = ?, heartbeat_at = ?, expires_at = ?
                WHERE host_id = ?
                  AND coordination_scope = ?
                  AND holder_agent_session_id = ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(heartbeat_at)
            .bind(expires_at)
            .bind(host_id.to_string())
            .bind(coordination_scope)
            .bind(agent_session_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                transaction.rollback().await?;
                return Ok(false);
            }
        }
        transaction.commit().await?;
        Ok(true)
    }

    /// Shortens an owned lease to a bounded handoff grace period.
    ///
    /// # Errors
    ///
    /// Returns an error if the database rejects the update.
    pub async fn shorten(
        &self,
        host_id: HostId,
        coordination_scope: &str,
        agent_session_id: AgentSessionId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        self.shorten_many(
            host_id,
            &[coordination_scope.to_owned()],
            agent_session_id,
            heartbeat_at,
            expires_at,
        )
        .await
    }

    /// Atomically shortens every owned exact scope to a bounded handoff grace period.
    ///
    /// # Errors
    ///
    /// Returns an error if the database transaction cannot complete.
    pub async fn shorten_many(
        &self,
        host_id: HostId,
        coordination_scopes: &[String],
        agent_session_id: AgentSessionId,
        heartbeat_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<bool, DbError> {
        if coordination_scopes.is_empty() {
            return Ok(false);
        }
        let mut transaction = self.pool.begin().await?;
        let mut all_owned = true;
        for coordination_scope in coordination_scopes {
            let result = sqlx::query(
                r"
                UPDATE host_write_leases
                SET heartbeat_at = ?, expires_at = MIN(expires_at, ?)
                WHERE host_id = ?
                  AND coordination_scope = ?
                  AND holder_agent_session_id = ?
                ",
            )
            .bind(heartbeat_at)
            .bind(expires_at)
            .bind(host_id.to_string())
            .bind(coordination_scope)
            .bind(agent_session_id.to_string())
            .execute(&mut *transaction)
            .await?;
            all_owned &= result.rows_affected() == 1;
        }
        transaction.commit().await?;
        Ok(all_owned)
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
        coordination_scopes: &[String],
    ) -> Result<bool, DbError> {
        if coordination_scopes.is_empty() {
            return Ok(false);
        }
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
                      AND EXISTS (
                        SELECT 1
                        FROM json_each(
                            CASE
                                WHEN json_array_length(coordination_scopes_json) > 0
                                THEN coordination_scopes_json
                                ELSE json_array(coordination_scope)
                            END
                        ) operation_scope
                        JOIN json_each(?) lease_scope
                        WHERE operation_scope.value = 'host'
                           OR lease_scope.value = 'host'
                           OR operation_scope.value = lease_scope.value
                           OR substr(operation_scope.value, 1, length(lease_scope.value) + 1)
                                = lease_scope.value || '/'
                           OR substr(lease_scope.value, 1, length(operation_scope.value) + 1)
                                = operation_scope.value || '/'
                      )
                )
                +
                (
                    SELECT COUNT(*)
                    FROM pty_input_events input
                    JOIN pty_sessions pty
                      ON pty.pty_session_id = input.pty_session_id
                    WHERE input.host_id = ?
                      AND input.agent_session_id = ?
                      AND input.state_json IN (?, ?)
                      AND EXISTS (
                        SELECT 1
                        FROM json_each(pty.coordination_scopes_json) pty_scope
                        JOIN json_each(?) lease_scope
                        WHERE pty_scope.value = 'host'
                           OR lease_scope.value = 'host'
                           OR pty_scope.value = lease_scope.value
                           OR substr(pty_scope.value, 1, length(lease_scope.value) + 1)
                                = lease_scope.value || '/'
                           OR substr(lease_scope.value, 1, length(pty_scope.value) + 1)
                                = pty_scope.value || '/'
                      )
                )
            ",
        )
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .bind(to_json(&OperationState::Queued)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(to_json(&coordination_scopes)?)
        .bind(host_id.to_string())
        .bind(agent_session_id.to_string())
        .bind(to_json(&PtyInputEventState::Queued)?)
        .bind(to_json(&PtyInputEventState::Claimed)?)
        .bind(to_json(&coordination_scopes)?)
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
                backend_capabilities_json, interaction_json, transport_evidence_json,
                coordination_scopes_json, created_at, last_activity_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pty_session_id) DO UPDATE SET
                state_json = excluded.state_json,
                foreground_process = excluded.foreground_process,
                cwd = excluded.cwd,
                recent_output_ref = excluded.recent_output_ref,
                last_exit_code = excluded.last_exit_code,
                input_allowed = excluded.input_allowed,
                backend_state_json = excluded.backend_state_json,
                backend_capabilities_json = excluded.backend_capabilities_json,
                interaction_json = excluded.interaction_json,
                transport_evidence_json = excluded.transport_evidence_json,
                coordination_scopes_json = excluded.coordination_scopes_json,
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
        .bind(optional_json(pty.interaction.as_ref())?)
        .bind(optional_json(pty.transport_evidence.as_ref())?)
        .bind(to_json(&pty.coordination_scopes)?)
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
                   backend_capabilities_json, interaction_json, transport_evidence_json,
                   coordination_scopes_json, created_at, last_activity_at
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
                   backend_capabilities_json, interaction_json, transport_evidence_json,
                   coordination_scopes_json, created_at, last_activity_at
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
        let observed_at = OffsetDateTime::now_utc();
        let row = sqlx::query(
            r"
            SELECT ps.pty_session_id, ps.workspace_id, ps.session_id, ps.state_json,
                   ps.foreground_process, ps.cwd, ps.recent_output_ref, ps.last_exit_code,
                   ps.input_allowed, ps.backend_state_json, ps.backend_capabilities_json,
                   ps.interaction_json, ps.transport_evidence_json, ps.coordination_scopes_json,
                   ps.created_at, ps.last_activity_at
            FROM pty_sessions ps
            JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
            JOIN access_paths ap ON ap.id = aw.access_path_id
            WHERE aw.connector_id = ?
              AND ps.backend_state_json = ?
              AND ps.input_allowed = 1
              AND aw.state_json IN (?, ?, ?)
              AND ps.state_json IN (?, ?)
              AND (
                (
                    SELECT COUNT(*)
                    FROM pty_sessions active_ps
                    JOIN agent_workspaces active_aw
                      ON active_aw.workspace_id = active_ps.workspace_id
                    WHERE active_aw.access_path_id = aw.access_path_id
                      AND active_ps.backend_state_json = ?
                      AND active_ps.input_allowed = 1
                      AND active_ps.state_json IN (?, ?)
                      AND active_aw.state_json IN (?, ?, ?)
                )
                +
                (
                    SELECT COUNT(*)
                    FROM operation_runs running_op
                    WHERE running_op.access_path_id = aw.access_path_id
                      AND running_op.state_json = ?
                      AND running_op.lease_expires_at IS NOT NULL
                      AND running_op.lease_expires_at > ?
                )
              ) < CASE
                    WHEN ap.max_concurrent_channels > 0
                    THEN ap.max_concurrent_channels
                    ELSE 1
                  END
            ORDER BY ps.created_at ASC, ps.pty_session_id ASC
            LIMIT 1
            ",
        )
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(observed_at)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_session).transpose()
    }

    /// Lists pending PTYs that are blocked only by local SSH channel capacity.
    ///
    /// The connector uses these rows to write one agent-visible wait message rather than leaving
    /// an unactivated terminal with no output.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_pending_waiting_for_channel(
        &self,
        connector_id: ConnectorId,
        limit: u32,
    ) -> Result<Vec<PtySession>, DbError> {
        let observed_at = OffsetDateTime::now_utc();
        let rows = sqlx::query(
            r"
            SELECT ps.pty_session_id, ps.workspace_id, ps.session_id, ps.state_json,
                   ps.foreground_process, ps.cwd, ps.recent_output_ref, ps.last_exit_code,
                   ps.input_allowed, ps.backend_state_json, ps.backend_capabilities_json,
                   ps.interaction_json, ps.transport_evidence_json, ps.coordination_scopes_json,
                   ps.created_at, ps.last_activity_at
            FROM pty_sessions ps
            JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
            JOIN access_paths ap ON ap.id = aw.access_path_id
            WHERE aw.connector_id = ?
              AND ps.backend_state_json = ?
              AND ps.input_allowed = 1
              AND aw.state_json IN (?, ?, ?)
              AND ps.state_json IN (?, ?)
              AND (
                (
                    SELECT COUNT(*)
                    FROM pty_sessions active_ps
                    JOIN agent_workspaces active_aw
                      ON active_aw.workspace_id = active_ps.workspace_id
                    WHERE active_aw.access_path_id = aw.access_path_id
                      AND active_ps.backend_state_json = ?
                      AND active_ps.input_allowed = 1
                      AND active_ps.state_json IN (?, ?)
                      AND active_aw.state_json IN (?, ?, ?)
                )
                +
                (
                    SELECT COUNT(*)
                    FROM operation_runs running_op
                    WHERE running_op.access_path_id = aw.access_path_id
                      AND running_op.state_json = ?
                      AND running_op.lease_expires_at IS NOT NULL
                      AND running_op.lease_expires_at > ?
                )
              ) >= CASE
                    WHEN ap.max_concurrent_channels > 0
                    THEN ap.max_concurrent_channels
                    ELSE 1
                  END
            ORDER BY ps.created_at ASC, ps.pty_session_id ASC
            LIMIT ?
            ",
        )
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(observed_at)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
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
              AND state_json IN (?, ?)
              AND input_allowed = 1
              AND backend_state_json IN (?, ?)
            ",
        )
        .bind(workspace_id.to_string())
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&PtyBackendState::Active)?)
        .fetch_one(&self.pool)
        .await?;
        i64_to_u32(count)
    }

    /// Counts active PTY sessions across all workspaces for a host.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or serialization fails.
    pub async fn count_active_for_host(&self, host_id: HostId) -> Result<u32, DbError> {
        let count: i64 = sqlx::query_scalar(
            r"
            SELECT COUNT(*)
            FROM pty_sessions ps
            JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
            WHERE aw.host_id = ?
              AND aw.state_json IN (?, ?, ?)
              AND ps.state_json IN (?, ?)
              AND ps.input_allowed = 1
              AND ps.backend_state_json IN (?, ?)
            ",
        )
        .bind(host_id.to_string())
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&PtyBackendState::Active)?)
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
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
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

    /// Closes pending PTYs whose workspace no longer permits a shell to start.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn close_pending_for_terminal_workspaces(
        &self,
        connector_id: ConnectorId,
        closed_at: OffsetDateTime,
    ) -> Result<Vec<PtySession>, DbError> {
        let rows = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                input_allowed = 0,
                backend_state_json = ?,
                interaction_json = NULL,
                last_activity_at = ?
            WHERE pty_session_id IN (
                SELECT ps.pty_session_id
                FROM pty_sessions ps
                JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                WHERE aw.connector_id = ?
                  AND ps.backend_state_json = ?
                  AND ps.state_json != ?
                  AND aw.state_json IN (?, ?, ?, ?)
            )
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&PtyBackendState::Closed)?)
        .bind(closed_at)
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&WorkspaceState::Done)?)
        .bind(to_json(&WorkspaceState::Failed)?)
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&WorkspaceState::Throttled)?)
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
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
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
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
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
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
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

    /// Atomically closes idle connector-owned PTYs while preserving active work and queued input.
    ///
    /// PTYs with a declared foreground process receive the longer busy TTL. A zero TTL disables
    /// the corresponding class. The returned rows are already closed so the connector can release
    /// matching in-memory backend handles without racing a newly queued input event.
    ///
    /// # Errors
    ///
    /// Returns an error if querying, serialization, or updating fails.
    pub async fn close_idle_for_connector(
        &self,
        connector_id: ConnectorId,
        now: OffsetDateTime,
        idle_ttl_seconds: u64,
        busy_ttl_seconds: u64,
        limit: u32,
    ) -> Result<Vec<PtySession>, DbError> {
        if idle_ttl_seconds == 0 && busy_ttl_seconds == 0 {
            return Ok(Vec::new());
        }
        let idle_cutoff = now - time::Duration::seconds(i64::try_from(idle_ttl_seconds.max(1))?);
        let busy_cutoff = now - time::Duration::seconds(i64::try_from(busy_ttl_seconds.max(1))?);
        let rows = sqlx::query(
            r"
            UPDATE pty_sessions
            SET state_json = ?,
                foreground_process = NULL,
                input_allowed = 0,
                backend_state_json = ?,
                interaction_json = NULL,
                last_activity_at = ?
            WHERE pty_session_id IN (
                SELECT ps.pty_session_id
                FROM pty_sessions ps
                JOIN agent_workspaces aw ON aw.workspace_id = ps.workspace_id
                WHERE aw.connector_id = ?
                  AND ps.input_allowed = 1
                  AND ps.backend_state_json IN (?, ?)
                  AND ps.state_json IN (?, ?, ?)
                  AND aw.state_json IN (?, ?, ?)
                  AND (
                    (? > 0 AND ps.foreground_process IS NULL AND ps.last_activity_at <= ?)
                    OR
                    (? > 0 AND ps.foreground_process IS NOT NULL AND ps.last_activity_at <= ?)
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM pty_input_events pie
                    WHERE pie.pty_session_id = ps.pty_session_id
                      AND pie.state_json IN (?, ?)
                  )
                ORDER BY ps.last_activity_at ASC, ps.pty_session_id ASC
                LIMIT ?
            )
            RETURNING pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                      recent_output_ref, last_exit_code, input_allowed, backend_state_json,
                      backend_capabilities_json, interaction_json, transport_evidence_json,
                      coordination_scopes_json, created_at, last_activity_at
            ",
        )
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(to_json(&PtyBackendState::Closed)?)
        .bind(now)
        .bind(connector_id.to_string())
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(i64::from(idle_ttl_seconds > 0))
        .bind(idle_cutoff)
        .bind(i64::from(busy_ttl_seconds > 0))
        .bind(busy_cutoff)
        .bind(to_json(&PtyInputEventState::Queued)?)
        .bind(to_json(&PtyInputEventState::Claimed)?)
        .bind(u32_to_i64(limit.max(1)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_pty_session).collect()
    }
}

const PTY_OUTPUT_SEGMENT_ENCODING: &str = "postcard+zstd-v1";
const PTY_OUTPUT_SEGMENT_VERSION: u8 = 1;
const MAX_PTY_OUTPUT_SEGMENT_BYTES: usize = 16 * 1024 * 1024;
const PTY_OUTPUT_COMPRESSION_LEVEL: i32 = 9;
const COMPRESSED_OUTPUT_WRITES_SETTING: &str = "compressed_output_writes_v1";
const COMPRESSED_OUTPUT_WRITES_ENABLED: &str = "enabled";

#[derive(Serialize)]
struct PtyOutputSegmentPayloadRef<'a> {
    version: u8,
    chunks: &'a [PtyOutputChunk],
}

#[derive(Deserialize)]
struct PtyOutputSegmentPayload {
    version: u8,
    chunks: Vec<PtyOutputChunk>,
}

struct EncodedPtyOutputSegment {
    pty_session_id: PtySessionId,
    workspace_id: WorkspaceId,
    first_sequence: u64,
    last_sequence: u64,
    chunk_count: u64,
    original_text_bytes: u64,
    encoded_bytes: u64,
    compressed_bytes: u64,
    payload: Vec<u8>,
    created_at: OffsetDateTime,
}

impl PtyOutputChunkRepository {
    /// Inserts a PTY output chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, chunk: &PtyOutputChunk) -> Result<(), DbError> {
        self.insert_batch(std::slice::from_ref(chunk)).await
    }

    /// Inserts one compressed PTY output segment containing multiple logical chunks.
    ///
    /// # Errors
    ///
    /// Returns an error when chunks span sessions, are not ordered, cannot be compressed, or the
    /// database rejects the segment.
    pub async fn insert_batch(&self, chunks: &[PtyOutputChunk]) -> Result<(), DbError> {
        if chunks.is_empty() {
            return Ok(());
        }
        if !compressed_output_writes_enabled(&self.pool).await? {
            let mut transaction = self.pool.begin().await?;
            for chunk in chunks {
                sqlx::query(
                    r"
                    INSERT INTO pty_output_chunks (
                        id, pty_session_id, workspace_id, stream_json, sequence, redacted_text,
                        byte_len, truncated, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            return Ok(());
        }
        let segment = encode_pty_output_segment(chunks)?;
        sqlx::query(
            r"
            INSERT INTO pty_output_segments (
                pty_session_id, workspace_id, first_sequence, last_sequence, chunk_count,
                encoding, original_text_byte_len, uncompressed_byte_len, compressed_byte_len,
                payload, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(segment.pty_session_id.to_string())
        .bind(segment.workspace_id.to_string())
        .bind(u64_to_i64(segment.first_sequence)?)
        .bind(u64_to_i64(segment.last_sequence)?)
        .bind(u64_to_i64(segment.chunk_count)?)
        .bind(PTY_OUTPUT_SEGMENT_ENCODING)
        .bind(u64_to_i64(segment.original_text_bytes)?)
        .bind(u64_to_i64(segment.encoded_bytes)?)
        .bind(u64_to_i64(segment.compressed_bytes)?)
        .bind(segment.payload)
        .bind(segment.created_at)
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
        let legacy_rows = sqlx::query(
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
        let segment_rows = sqlx::query(
            r"
            SELECT pty_session_id, workspace_id, first_sequence, last_sequence, chunk_count,
                   encoding, original_text_byte_len, uncompressed_byte_len,
                   compressed_byte_len, payload, created_at
            FROM pty_output_segments
            WHERE pty_session_id = ? AND last_sequence >= ?
            ORDER BY first_sequence ASC
            LIMIT ?
            ",
        )
        .bind(pty_session_id.to_string())
        .bind(u64_to_i64(start_sequence)?)
        .bind(u32_to_i64(limit))
        .fetch_all(&self.pool)
        .await?;

        let mut chunks = legacy_rows
            .iter()
            .map(row_to_pty_output_chunk)
            .collect::<Result<Vec<_>, _>>()?;
        for row in &segment_rows {
            chunks.extend(
                decode_pty_output_segment(row)?
                    .into_iter()
                    .filter(|chunk| chunk.sequence >= start_sequence),
            );
        }
        chunks.sort_by_key(|chunk| (chunk.sequence, chunk.id));
        chunks.dedup_by_key(|chunk| chunk.sequence);
        chunks.truncate(usize::try_from(limit)?);
        Ok(chunks)
    }

    /// Returns the next PTY output sequence number for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or integer conversion fails.
    pub async fn next_sequence(&self, pty_session_id: PtySessionId) -> Result<u64, DbError> {
        let current: Option<i64> = sqlx::query_scalar(
            r"
            SELECT MAX(last_sequence)
            FROM (
                SELECT MAX(sequence) AS last_sequence
                FROM pty_output_chunks
                WHERE pty_session_id = ?
                UNION ALL
                SELECT MAX(last_sequence) AS last_sequence
                FROM pty_output_segments
                WHERE pty_session_id = ?
            )
            ",
        )
        .bind(pty_session_id.to_string())
        .bind(pty_session_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(match current {
            Some(value) => i64_to_u64(value)?.saturating_add(1),
            None => 0,
        })
    }

    /// Returns aggregate logical and compressed PTY output counters.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage counters cannot be queried or converted.
    pub async fn storage_stats(&self) -> Result<PtyOutputStorageStats, DbError> {
        let row = sqlx::query(
            r"
            SELECT
                (SELECT COUNT(*) FROM pty_output_chunks) AS legacy_chunks,
                (SELECT COALESCE(SUM(byte_len), 0) FROM pty_output_chunks)
                    AS legacy_text_bytes,
                (SELECT COUNT(*) FROM pty_output_segments) AS compressed_segments,
                (SELECT COALESCE(SUM(chunk_count), 0) FROM pty_output_segments)
                    AS compressed_chunks,
                (SELECT COALESCE(SUM(original_text_byte_len), 0) FROM pty_output_segments)
                    AS compressed_text_bytes,
                (SELECT COALESCE(SUM(uncompressed_byte_len), 0) FROM pty_output_segments)
                    AS encoded_bytes,
                (SELECT COALESCE(SUM(compressed_byte_len), 0) FROM pty_output_segments)
                    AS compressed_bytes
            ",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(PtyOutputStorageStats {
            legacy_chunks: i64_to_u64(row.try_get("legacy_chunks")?)?,
            legacy_text_bytes: i64_to_u64(row.try_get("legacy_text_bytes")?)?,
            compressed_segments: i64_to_u64(row.try_get("compressed_segments")?)?,
            compressed_chunks: i64_to_u64(row.try_get("compressed_chunks")?)?,
            compressed_text_bytes: i64_to_u64(row.try_get("compressed_text_bytes")?)?,
            encoded_bytes: i64_to_u64(row.try_get("encoded_bytes")?)?,
            compressed_bytes: i64_to_u64(row.try_get("compressed_bytes")?)?,
        })
    }

    /// Moves one bounded batch of legacy PTY rows into a compressed segment transactionally.
    ///
    /// The method is repeatable and selects exact legacy row ids before deletion, so output
    /// appended concurrently at a higher sequence remains untouched.
    ///
    /// # Errors
    ///
    /// Returns an error if rows cannot be read, encoded, inserted, or deleted atomically.
    pub async fn compact_legacy_batch(
        &self,
        max_chunks: u32,
        target_uncompressed_bytes: u64,
    ) -> Result<PtyOutputCompactionBatch, DbError> {
        let max_chunks = max_chunks.clamp(1, 10_000);
        let target_uncompressed_bytes =
            target_uncompressed_bytes.clamp(1, u64::try_from(MAX_PTY_OUTPUT_SEGMENT_BYTES)?);
        let Some(pty_session_id): Option<String> = sqlx::query_scalar(
            r"
            SELECT pty_session_id
            FROM pty_output_chunks
            ORDER BY created_at ASC, sequence ASC, id ASC
            LIMIT 1
            ",
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(PtyOutputCompactionBatch::default());
        };
        let rows = sqlx::query(
            r"
            SELECT id, pty_session_id, workspace_id, stream_json, sequence, redacted_text,
                   byte_len, truncated, created_at
            FROM pty_output_chunks
            WHERE pty_session_id = ?
            ORDER BY sequence ASC, id ASC
            LIMIT ?
            ",
        )
        .bind(pty_session_id)
        .bind(u32_to_i64(max_chunks))
        .fetch_all(&self.pool)
        .await?;
        let mut chunks = Vec::with_capacity(rows.len());
        let mut logical_bytes = 0_u64;
        for row in &rows {
            let chunk = row_to_pty_output_chunk(row)?;
            if !chunks.is_empty() && logical_bytes >= target_uncompressed_bytes {
                break;
            }
            logical_bytes = logical_bytes.saturating_add(chunk.byte_len);
            chunks.push(chunk);
        }
        if chunks.is_empty() {
            return Ok(PtyOutputCompactionBatch::default());
        }
        let segment = encode_pty_output_segment(&chunks)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            INSERT INTO pty_output_segments (
                pty_session_id, workspace_id, first_sequence, last_sequence, chunk_count,
                encoding, original_text_byte_len, uncompressed_byte_len, compressed_byte_len,
                payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(segment.pty_session_id.to_string())
        .bind(segment.workspace_id.to_string())
        .bind(u64_to_i64(segment.first_sequence)?)
        .bind(u64_to_i64(segment.last_sequence)?)
        .bind(u64_to_i64(segment.chunk_count)?)
        .bind(PTY_OUTPUT_SEGMENT_ENCODING)
        .bind(u64_to_i64(segment.original_text_bytes)?)
        .bind(u64_to_i64(segment.encoded_bytes)?)
        .bind(u64_to_i64(segment.compressed_bytes)?)
        .bind(&segment.payload)
        .bind(segment.created_at)
        .execute(&mut *transaction)
        .await?;

        let mut delete = QueryBuilder::<Sqlite>::new("DELETE FROM pty_output_chunks WHERE id IN (");
        let mut ids = delete.separated(", ");
        for chunk in &chunks {
            ids.push_bind(chunk.id.to_string());
        }
        ids.push_unseparated(")");
        let deleted = delete.build().execute(&mut *transaction).await?;
        if deleted.rows_affected() != u64::try_from(chunks.len())? {
            return Err(DbError::InvalidOutputSegment(format!(
                "legacy compaction selected {} rows but deleted {}",
                chunks.len(),
                deleted.rows_affected()
            )));
        }
        transaction.commit().await?;
        Ok(PtyOutputCompactionBatch {
            legacy_chunks: u64::try_from(chunks.len())?,
            segments_written: 1,
            original_storage_bytes: segment.original_text_bytes,
            encoded_bytes: segment.encoded_bytes,
            compressed_bytes: segment.compressed_bytes,
        })
    }
}

fn encode_pty_output_segment(
    chunks: &[PtyOutputChunk],
) -> Result<EncodedPtyOutputSegment, DbError> {
    let first = chunks
        .first()
        .ok_or_else(|| DbError::InvalidOutputSegment("segment cannot be empty".to_owned()))?;
    let last = chunks
        .last()
        .ok_or_else(|| DbError::InvalidOutputSegment("segment cannot be empty".to_owned()))?;
    let mut previous_sequence = None;
    let mut original_text_bytes = 0_u64;
    for chunk in chunks {
        if chunk.pty_session_id != first.pty_session_id || chunk.workspace_id != first.workspace_id
        {
            return Err(DbError::InvalidOutputSegment(
                "all chunks in a segment must share one PTY session and workspace".to_owned(),
            ));
        }
        if previous_sequence.is_some_and(|previous| chunk.sequence <= previous) {
            return Err(DbError::InvalidOutputSegment(
                "chunk sequences must be strictly increasing".to_owned(),
            ));
        }
        if chunk.byte_len != u64::try_from(chunk.redacted_text.len())? {
            return Err(DbError::InvalidOutputSegment(format!(
                "chunk {} byte length does not match its UTF-8 payload",
                chunk.id
            )));
        }
        previous_sequence = Some(chunk.sequence);
        original_text_bytes = original_text_bytes.saturating_add(chunk.byte_len);
    }
    let encoded = postcard::to_allocvec(&PtyOutputSegmentPayloadRef {
        version: PTY_OUTPUT_SEGMENT_VERSION,
        chunks,
    })?;
    if encoded.len() > MAX_PTY_OUTPUT_SEGMENT_BYTES {
        return Err(DbError::InvalidOutputSegment(format!(
            "encoded segment exceeds {MAX_PTY_OUTPUT_SEGMENT_BYTES} bytes"
        )));
    }
    let payload = zstd::bulk::compress(&encoded, PTY_OUTPUT_COMPRESSION_LEVEL)?;
    Ok(EncodedPtyOutputSegment {
        pty_session_id: first.pty_session_id,
        workspace_id: first.workspace_id,
        first_sequence: first.sequence,
        last_sequence: last.sequence,
        chunk_count: u64::try_from(chunks.len())?,
        original_text_bytes,
        encoded_bytes: u64::try_from(encoded.len())?,
        compressed_bytes: u64::try_from(payload.len())?,
        payload,
        created_at: first.created_at,
    })
}

fn decode_pty_output_segment(row: &SqliteRow) -> Result<Vec<PtyOutputChunk>, DbError> {
    let encoding: String = row.try_get("encoding")?;
    if encoding != PTY_OUTPUT_SEGMENT_ENCODING {
        return Err(DbError::InvalidOutputSegment(format!(
            "unsupported encoding {encoding}"
        )));
    }
    let expected_bytes = i64_to_u64(row.try_get("uncompressed_byte_len")?)?;
    let expected_bytes = usize::try_from(expected_bytes)?;
    if expected_bytes == 0 || expected_bytes > MAX_PTY_OUTPUT_SEGMENT_BYTES {
        return Err(DbError::InvalidOutputSegment(format!(
            "invalid uncompressed length {expected_bytes}"
        )));
    }
    let compressed: Vec<u8> = row.try_get("payload")?;
    let decoded = zstd::bulk::decompress(&compressed, expected_bytes)?;
    if decoded.len() != expected_bytes {
        return Err(DbError::InvalidOutputSegment(format!(
            "decoded {} bytes but expected {expected_bytes}",
            decoded.len()
        )));
    }
    let payload: PtyOutputSegmentPayload = postcard::from_bytes(&decoded)?;
    if payload.version != PTY_OUTPUT_SEGMENT_VERSION {
        return Err(DbError::InvalidOutputSegment(format!(
            "unsupported payload version {}",
            payload.version
        )));
    }
    let pty_session_id: PtySessionId = row.try_get::<String, _>("pty_session_id")?.parse()?;
    let workspace_id: WorkspaceId = row.try_get::<String, _>("workspace_id")?.parse()?;
    let first_sequence = i64_to_u64(row.try_get("first_sequence")?)?;
    let last_sequence = i64_to_u64(row.try_get("last_sequence")?)?;
    let chunk_count = i64_to_u64(row.try_get("chunk_count")?)?;
    if payload.chunks.len() != usize::try_from(chunk_count)?
        || payload.chunks.first().map(|chunk| chunk.sequence) != Some(first_sequence)
        || payload.chunks.last().map(|chunk| chunk.sequence) != Some(last_sequence)
        || payload.chunks.iter().any(|chunk| {
            chunk.pty_session_id != pty_session_id || chunk.workspace_id != workspace_id
        })
        || payload
            .chunks
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(DbError::InvalidOutputSegment(
            "payload metadata does not match its segment row".to_owned(),
        ));
    }
    Ok(payload.chunks)
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
                idempotency_key, payload_kind_json, input_fingerprint, state_json, sequence, input_text,
                redacted_input_summary, byte_len, requested_by, created_at, claimed_at,
                lease_expires_at, delivered_at, failed_at, attempt_count, claim_token, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(event.id.to_string())
        .bind(event.pty_session_id.to_string())
        .bind(event.workspace_id.to_string())
        .bind(event.connector_id.to_string())
        .bind(event.host_id.to_string())
        .bind(event.agent_session_id.map(|id| id.to_string()))
        .bind(&event.idempotency_key)
        .bind(to_json(&event.payload_kind)?)
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

    /// Gets the metadata for the immediately preceding delivered PTY input.
    ///
    /// The raw input is deliberately erased after delivery. The connector can bind a later
    /// credential response to the one-way input fingerprint without retaining command text.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_preceding_delivered_input(
        &self,
        pty_session_id: PtySessionId,
        sequence: u64,
    ) -> Result<Option<PtyInputEvent>, DbError> {
        let Some(previous_sequence) = sequence.checked_sub(1) else {
            return Ok(None);
        };
        let row = sqlx::query(
            r"
            SELECT *
            FROM pty_input_events
            WHERE pty_session_id = ?
              AND sequence = ?
              AND state_json = ?
            LIMIT 1
            ",
        )
        .bind(pty_session_id.to_string())
        .bind(u64_to_i64(previous_sequence)?)
        .bind(to_json(&PtyInputEventState::Delivered)?)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_pty_input_event).transpose()
    }

    /// Lists recent public PTY input metadata across all sessions.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_recent(&self, limit: u32) -> Result<Vec<PtyInputEvent>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM pty_input_events
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            ",
        )
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
                SELECT queued_input.id
                FROM pty_input_events queued_input
                JOIN pty_sessions target_pty
                  ON target_pty.pty_session_id = queued_input.pty_session_id
                JOIN agent_workspaces target_workspace
                  ON target_workspace.workspace_id = target_pty.workspace_id
                WHERE queued_input.connector_id = ?
                  AND queued_input.input_text IS NOT NULL
                  AND target_pty.backend_state_json = ?
                  AND target_pty.input_allowed = 1
                  AND target_pty.state_json IN (?, ?)
                  AND target_workspace.state_json IN (?, ?, ?)
                  AND queued_input.attempt_count < ?
                  AND (
                    queued_input.agent_session_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM host_write_leases
                        WHERE host_write_leases.host_id = queued_input.host_id
                          AND host_write_leases.expires_at > ?
                          AND host_write_leases.holder_agent_session_id
                              != queued_input.agent_session_id
                          AND EXISTS (
                            SELECT 1
                            FROM json_each(
                                CASE
                                    WHEN json_array_length(target_pty.coordination_scopes_json) > 0
                                    THEN target_pty.coordination_scopes_json
                                    ELSE json_array(target_workspace.coordination_scope)
                                END
                            ) target_scope
                            WHERE host_write_leases.coordination_scope = 'host'
                               OR target_scope.value = 'host'
                               OR host_write_leases.coordination_scope = target_scope.value
                               OR substr(
                                    host_write_leases.coordination_scope,
                                    1,
                                    length(target_scope.value) + 1
                               ) = target_scope.value || '/'
                               OR substr(
                                    target_scope.value,
                                    1,
                                    length(host_write_leases.coordination_scope) + 1
                               ) = host_write_leases.coordination_scope || '/'
                          )
                    )
                  )
                  AND (
                    queued_input.state_json = ?
                    OR (
                        queued_input.state_json = ?
                        AND queued_input.lease_expires_at IS NOT NULL
                        AND queued_input.lease_expires_at <= ?
                    )
                  )
                ORDER BY queued_input.created_at ASC, queued_input.sequence ASC, queued_input.id ASC
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
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
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
                agent_session_id, idempotency_key, requires_write_lease, coordination_scope,
                coordination_scopes_json, operation_type_json, intent, state_json, started_at,
                finished_at, exit_code,
                timeout_seconds, redacted_command_summary, command_profile_json,
                transport_evidence_json,
                redacted_output_summary, log_ref, attempt_count, claim_token, claimed_at,
                lease_expires_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        .bind(&operation.coordination_scope)
        .bind(to_json(&operation.coordination_scopes)?)
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
    #[allow(clippy::too_many_lines)]
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
                SELECT candidate.id
                FROM operation_runs candidate
                JOIN access_paths candidate_path
                  ON candidate_path.id = candidate.access_path_id
                JOIN agent_workspaces candidate_workspace
                  ON candidate_workspace.workspace_id = candidate.workspace_id
                WHERE candidate.connector_id = ?
                  AND candidate.workspace_id IS NOT NULL
                  AND candidate.command_profile_json IS NOT NULL
                  AND candidate_workspace.state_json != ?
                  AND candidate.attempt_count < ?
                  AND (
                    (
                        SELECT COUNT(*)
                        FROM operation_runs running_capacity
                        WHERE running_capacity.access_path_id = candidate.access_path_id
                          AND running_capacity.id != candidate.id
                          AND running_capacity.state_json = ?
                          AND running_capacity.lease_expires_at IS NOT NULL
                          AND running_capacity.lease_expires_at > ?
                    )
                    +
                    (
                        SELECT COUNT(*)
                        FROM pty_sessions reserved_pty
                        JOIN agent_workspaces reserved_workspace
                          ON reserved_workspace.workspace_id = reserved_pty.workspace_id
                        WHERE reserved_workspace.access_path_id = candidate.access_path_id
                          AND reserved_pty.backend_state_json IN (?, ?)
                          AND reserved_pty.input_allowed = 1
                          AND reserved_pty.state_json IN (?, ?)
                          AND reserved_workspace.state_json IN (?, ?, ?)
                    )
                  ) < CASE
                        WHEN candidate_path.max_concurrent_channels > 0
                        THEN candidate_path.max_concurrent_channels
                        ELSE 1
                      END
                  AND (
                    candidate.requires_write_lease = 0
                    OR NOT EXISTS (
                        SELECT 1
                        FROM operation_runs running_write
                        WHERE running_write.host_id = candidate.host_id
                          AND running_write.requires_write_lease = 1
                          AND running_write.state_json = ?
                          AND running_write.lease_expires_at IS NOT NULL
                          AND running_write.lease_expires_at > ?
                          AND EXISTS (
                            SELECT 1
                            FROM json_each(
                                CASE
                                    WHEN json_array_length(running_write.coordination_scopes_json) > 0
                                    THEN running_write.coordination_scopes_json
                                    ELSE json_array(running_write.coordination_scope)
                                END
                            ) running_scope
                            JOIN json_each(
                                CASE
                                    WHEN json_array_length(candidate.coordination_scopes_json) > 0
                                    THEN candidate.coordination_scopes_json
                                    ELSE json_array(candidate.coordination_scope)
                                END
                            ) candidate_scope
                            WHERE running_scope.value = 'host'
                               OR candidate_scope.value = 'host'
                               OR running_scope.value = candidate_scope.value
                               OR substr(
                                    running_scope.value,
                                    1,
                                    length(candidate_scope.value) + 1
                               ) = candidate_scope.value || '/'
                               OR substr(
                                    candidate_scope.value,
                                    1,
                                    length(running_scope.value) + 1
                               ) = running_scope.value || '/'
                          )
                    )
                  )
                  AND (
                    candidate.requires_write_lease = 0
                    OR candidate.agent_session_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM host_write_leases
                        WHERE host_write_leases.host_id = candidate.host_id
                          AND host_write_leases.expires_at > ?
                          AND host_write_leases.holder_agent_session_id
                              != candidate.agent_session_id
                          AND EXISTS (
                            SELECT 1
                            FROM json_each(
                                CASE
                                    WHEN json_array_length(candidate.coordination_scopes_json) > 0
                                    THEN candidate.coordination_scopes_json
                                    ELSE json_array(candidate.coordination_scope)
                                END
                            ) candidate_scope
                            WHERE host_write_leases.coordination_scope = 'host'
                               OR candidate_scope.value = 'host'
                               OR host_write_leases.coordination_scope = candidate_scope.value
                               OR substr(
                                    host_write_leases.coordination_scope,
                                    1,
                                    length(candidate_scope.value) + 1
                               ) = candidate_scope.value || '/'
                               OR substr(
                                    candidate_scope.value,
                                    1,
                                    length(host_write_leases.coordination_scope) + 1
                               ) = host_write_leases.coordination_scope || '/'
                          )
                    )
                  )
                  AND (
                    candidate.state_json = ?
                    OR (
                        candidate.state_json = ?
                        AND candidate.lease_expires_at IS NOT NULL
                        AND candidate.lease_expires_at <= ?
                    )
                  )
                ORDER BY candidate.started_at ASC, candidate.id ASC
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
        .bind(to_json(&WorkspaceState::Closed)?)
        .bind(u32_to_i64(max_attempts))
        .bind(to_json(&OperationState::Running)?)
        .bind(claimed_at)
        .bind(to_json(&PtyBackendState::Pending)?)
        .bind(to_json(&PtyBackendState::Active)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Idle)?)
        .bind(to_json(&WorkspaceState::Working)?)
        .bind(to_json(&WorkspaceState::Blocked)?)
        .bind(to_json(&OperationState::Running)?)
        .bind(claimed_at)
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
                FROM operation_runs exhausted_candidate
                JOIN agent_workspaces exhausted_workspace
                  ON exhausted_workspace.workspace_id = exhausted_candidate.workspace_id
                WHERE exhausted_candidate.connector_id = ?
                  AND exhausted_candidate.workspace_id IS NOT NULL
                  AND exhausted_candidate.command_profile_json IS NOT NULL
                  AND exhausted_workspace.state_json != ?
                  AND exhausted_candidate.attempt_count >= ?
                  AND (
                    exhausted_candidate.state_json = ?
                    OR (
                        exhausted_candidate.state_json = ?
                        AND exhausted_candidate.lease_expires_at IS NOT NULL
                        AND exhausted_candidate.lease_expires_at <= ?
                    )
                  )
                ORDER BY exhausted_candidate.started_at ASC, exhausted_candidate.id ASC
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
        .bind(to_json(&WorkspaceState::Closed)?)
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
    /// Used when another agent wins an overlapping write-scope race after this connector selected the
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

    /// Lists recent operations across all hosts.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_recent(&self, limit: u32) -> Result<Vec<OperationRun>, DbError> {
        let rows = sqlx::query(
            r"
            SELECT *
            FROM operation_runs
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            ",
        )
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

const OPERATION_OUTPUT_APPEND_TARGET_BYTES: u64 = 64 * 1024;

#[derive(Serialize)]
struct OperationOutputSegmentPayloadRef<'a> {
    version: u8,
    chunks: &'a [OperationOutputChunk],
}

#[derive(Deserialize)]
struct OperationOutputSegmentPayload {
    version: u8,
    chunks: Vec<OperationOutputChunk>,
}

struct EncodedOperationOutputSegment {
    operation_id: OperationId,
    workspace_id: WorkspaceId,
    first_sequence: u64,
    last_sequence: u64,
    chunk_count: u64,
    original_text_bytes: u64,
    encoded_bytes: u64,
    compressed_bytes: u64,
    payload: Vec<u8>,
    created_at: OffsetDateTime,
}

impl OperationOutputChunkRepository {
    /// Inserts an output chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the insert.
    pub async fn insert(&self, chunk: &OperationOutputChunk) -> Result<(), DbError> {
        if !compressed_output_writes_enabled(&self.pool).await? {
            sqlx::query(
                r"
                INSERT INTO operation_output_chunks (
                    id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                    byte_len, truncated, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            return Ok(());
        }
        let latest = sqlx::query(
            r"
            SELECT segment_id, operation_id, workspace_id, first_sequence, last_sequence,
                   chunk_count, encoding, original_text_byte_len, uncompressed_byte_len,
                   compressed_byte_len, payload, created_at
            FROM operation_output_segments
            WHERE operation_id = ?
            ORDER BY last_sequence DESC
            LIMIT 1
            ",
        )
        .bind(chunk.operation_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = latest {
            let segment_id: i64 = row.try_get("segment_id")?;
            let previous_last = i64_to_u64(row.try_get("last_sequence")?)?;
            let original_text_bytes = i64_to_u64(row.try_get("original_text_byte_len")?)?;
            if previous_last < chunk.sequence
                && original_text_bytes.saturating_add(chunk.byte_len)
                    <= OPERATION_OUTPUT_APPEND_TARGET_BYTES
            {
                let mut chunks = decode_operation_output_segment(&row)?;
                chunks.push(chunk.clone());
                let segment = encode_operation_output_segment(&chunks)?;
                let updated = sqlx::query(
                    r"
                    UPDATE operation_output_segments
                    SET last_sequence = ?, chunk_count = ?, original_text_byte_len = ?,
                        uncompressed_byte_len = ?, compressed_byte_len = ?, payload = ?
                    WHERE segment_id = ? AND last_sequence = ?
                    ",
                )
                .bind(u64_to_i64(segment.last_sequence)?)
                .bind(u64_to_i64(segment.chunk_count)?)
                .bind(u64_to_i64(segment.original_text_bytes)?)
                .bind(u64_to_i64(segment.encoded_bytes)?)
                .bind(u64_to_i64(segment.compressed_bytes)?)
                .bind(segment.payload)
                .bind(segment_id)
                .bind(u64_to_i64(previous_last)?)
                .execute(&self.pool)
                .await?;
                if updated.rows_affected() == 1 {
                    return Ok(());
                }
            }
        }
        self.insert_batch(std::slice::from_ref(chunk)).await
    }

    /// Inserts one compressed command-output segment.
    ///
    /// # Errors
    ///
    /// Returns an error when chunks do not share an operation, are unordered, cannot be encoded,
    /// or the database rejects the segment.
    pub async fn insert_batch(&self, chunks: &[OperationOutputChunk]) -> Result<(), DbError> {
        if chunks.is_empty() {
            return Ok(());
        }
        let segment = encode_operation_output_segment(chunks)?;
        sqlx::query(
            r"
            INSERT INTO operation_output_segments (
                operation_id, workspace_id, first_sequence, last_sequence, chunk_count,
                encoding, original_text_byte_len, uncompressed_byte_len, compressed_byte_len,
                payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(segment.operation_id.to_string())
        .bind(segment.workspace_id.to_string())
        .bind(u64_to_i64(segment.first_sequence)?)
        .bind(u64_to_i64(segment.last_sequence)?)
        .bind(u64_to_i64(segment.chunk_count)?)
        .bind(PTY_OUTPUT_SEGMENT_ENCODING)
        .bind(u64_to_i64(segment.original_text_bytes)?)
        .bind(u64_to_i64(segment.encoded_bytes)?)
        .bind(u64_to_i64(segment.compressed_bytes)?)
        .bind(segment.payload)
        .bind(segment.created_at)
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
            SELECT COALESCE(MAX(last_sequence), -1) + 1
            FROM (
                SELECT MAX(sequence) AS last_sequence
                FROM operation_output_chunks
                WHERE operation_id = ?
                UNION ALL
                SELECT MAX(last_sequence) AS last_sequence
                FROM operation_output_segments
                WHERE operation_id = ?
            )
            ",
        )
        .bind(operation_id.to_string())
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
        let legacy_rows = if let Some(operation_id) = operation_id {
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
        let segment_rows = if let Some(operation_id) = operation_id {
            sqlx::query(
                r"
                SELECT operation_id, workspace_id, first_sequence, last_sequence, chunk_count,
                       encoding, original_text_byte_len, uncompressed_byte_len,
                       compressed_byte_len, payload, created_at
                FROM operation_output_segments
                WHERE workspace_id = ? AND operation_id = ? AND last_sequence >= ?
                ORDER BY created_at ASC, first_sequence ASC
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
                SELECT operation_id, workspace_id, first_sequence, last_sequence, chunk_count,
                       encoding, original_text_byte_len, uncompressed_byte_len,
                       compressed_byte_len, payload, created_at
                FROM operation_output_segments
                WHERE workspace_id = ? AND last_sequence >= ?
                ORDER BY created_at ASC, first_sequence ASC
                LIMIT ?
                ",
            )
            .bind(workspace_id.to_string())
            .bind(u64_to_i64(start_sequence)?)
            .bind(u32_to_i64(limit))
            .fetch_all(&self.pool)
            .await?
        };
        let mut chunks = legacy_rows
            .iter()
            .map(row_to_operation_output_chunk)
            .collect::<Result<Vec<_>, _>>()?;
        for row in &segment_rows {
            chunks.extend(
                decode_operation_output_segment(row)?
                    .into_iter()
                    .filter(|chunk| chunk.sequence >= start_sequence),
            );
        }
        chunks.sort_by_key(|chunk| {
            (
                chunk.created_at,
                chunk.sequence,
                chunk.operation_id,
                chunk.id,
            )
        });
        chunks.dedup_by_key(|chunk| (chunk.operation_id, chunk.sequence));
        chunks.truncate(usize::try_from(limit)?);
        Ok(chunks)
    }

    /// Returns aggregate logical and compressed command-output counters.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage counters cannot be queried or converted.
    pub async fn storage_stats(&self) -> Result<OperationOutputStorageStats, DbError> {
        let row = sqlx::query(
            r"
            SELECT
                (SELECT COUNT(*) FROM operation_output_chunks) AS legacy_chunks,
                (SELECT COALESCE(SUM(byte_len), 0) FROM operation_output_chunks)
                    AS legacy_text_bytes,
                (SELECT COUNT(*) FROM operation_output_segments) AS compressed_segments,
                (SELECT COALESCE(SUM(chunk_count), 0) FROM operation_output_segments)
                    AS compressed_chunks,
                (SELECT COALESCE(SUM(original_text_byte_len), 0)
                 FROM operation_output_segments) AS compressed_text_bytes,
                (SELECT COALESCE(SUM(uncompressed_byte_len), 0)
                 FROM operation_output_segments) AS encoded_bytes,
                (SELECT COALESCE(SUM(compressed_byte_len), 0)
                 FROM operation_output_segments) AS compressed_bytes
            ",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(OperationOutputStorageStats {
            legacy_chunks: i64_to_u64(row.try_get("legacy_chunks")?)?,
            legacy_text_bytes: i64_to_u64(row.try_get("legacy_text_bytes")?)?,
            compressed_segments: i64_to_u64(row.try_get("compressed_segments")?)?,
            compressed_chunks: i64_to_u64(row.try_get("compressed_chunks")?)?,
            compressed_text_bytes: i64_to_u64(row.try_get("compressed_text_bytes")?)?,
            encoded_bytes: i64_to_u64(row.try_get("encoded_bytes")?)?,
            compressed_bytes: i64_to_u64(row.try_get("compressed_bytes")?)?,
        })
    }

    /// Moves one bounded operation's legacy output rows into compressed storage atomically.
    ///
    /// # Errors
    ///
    /// Returns an error if rows cannot be read, encoded, inserted, or deleted atomically.
    pub async fn compact_legacy_batch(
        &self,
        max_chunks: u32,
        target_uncompressed_bytes: u64,
    ) -> Result<OperationOutputCompactionBatch, DbError> {
        let max_chunks = max_chunks.clamp(1, 10_000);
        let target_uncompressed_bytes =
            target_uncompressed_bytes.clamp(1, u64::try_from(MAX_PTY_OUTPUT_SEGMENT_BYTES)?);
        let Some(operation_id): Option<String> = sqlx::query_scalar(
            r"
            SELECT operation_id
            FROM operation_output_chunks
            ORDER BY created_at ASC, sequence ASC, id ASC
            LIMIT 1
            ",
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(OperationOutputCompactionBatch::default());
        };
        let rows = sqlx::query(
            r"
            SELECT id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                   byte_len, truncated, created_at
            FROM operation_output_chunks
            WHERE operation_id = ?
            ORDER BY sequence ASC, id ASC
            LIMIT ?
            ",
        )
        .bind(operation_id)
        .bind(u32_to_i64(max_chunks))
        .fetch_all(&self.pool)
        .await?;
        let mut chunks = Vec::with_capacity(rows.len());
        let mut logical_bytes = 0_u64;
        for row in &rows {
            let chunk = row_to_operation_output_chunk(row)?;
            if !chunks.is_empty() && logical_bytes >= target_uncompressed_bytes {
                break;
            }
            logical_bytes = logical_bytes.saturating_add(chunk.byte_len);
            chunks.push(chunk);
        }
        if chunks.is_empty() {
            return Ok(OperationOutputCompactionBatch::default());
        }
        let segment = encode_operation_output_segment(&chunks)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            INSERT INTO operation_output_segments (
                operation_id, workspace_id, first_sequence, last_sequence, chunk_count,
                encoding, original_text_byte_len, uncompressed_byte_len, compressed_byte_len,
                payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(segment.operation_id.to_string())
        .bind(segment.workspace_id.to_string())
        .bind(u64_to_i64(segment.first_sequence)?)
        .bind(u64_to_i64(segment.last_sequence)?)
        .bind(u64_to_i64(segment.chunk_count)?)
        .bind(PTY_OUTPUT_SEGMENT_ENCODING)
        .bind(u64_to_i64(segment.original_text_bytes)?)
        .bind(u64_to_i64(segment.encoded_bytes)?)
        .bind(u64_to_i64(segment.compressed_bytes)?)
        .bind(&segment.payload)
        .bind(segment.created_at)
        .execute(&mut *transaction)
        .await?;
        let mut delete =
            QueryBuilder::<Sqlite>::new("DELETE FROM operation_output_chunks WHERE id IN (");
        let mut ids = delete.separated(", ");
        for chunk in &chunks {
            ids.push_bind(chunk.id.to_string());
        }
        ids.push_unseparated(")");
        let deleted = delete.build().execute(&mut *transaction).await?;
        if deleted.rows_affected() != u64::try_from(chunks.len())? {
            return Err(DbError::InvalidOutputSegment(format!(
                "command-output compaction selected {} rows but deleted {}",
                chunks.len(),
                deleted.rows_affected()
            )));
        }
        transaction.commit().await?;
        Ok(OperationOutputCompactionBatch {
            legacy_chunks: u64::try_from(chunks.len())?,
            segments_written: 1,
            original_storage_bytes: segment.original_text_bytes,
            encoded_bytes: segment.encoded_bytes,
            compressed_bytes: segment.compressed_bytes,
        })
    }
}

fn encode_operation_output_segment(
    chunks: &[OperationOutputChunk],
) -> Result<EncodedOperationOutputSegment, DbError> {
    let first = chunks
        .first()
        .ok_or_else(|| DbError::InvalidOutputSegment("segment cannot be empty".to_owned()))?;
    let last = chunks
        .last()
        .ok_or_else(|| DbError::InvalidOutputSegment("segment cannot be empty".to_owned()))?;
    let mut previous_sequence = None;
    let mut original_text_bytes = 0_u64;
    for chunk in chunks {
        if chunk.operation_id != first.operation_id || chunk.workspace_id != first.workspace_id {
            return Err(DbError::InvalidOutputSegment(
                "all chunks in a segment must share one operation and workspace".to_owned(),
            ));
        }
        if previous_sequence.is_some_and(|previous| chunk.sequence <= previous) {
            return Err(DbError::InvalidOutputSegment(
                "chunk sequences must be strictly increasing".to_owned(),
            ));
        }
        if chunk.byte_len != u64::try_from(chunk.redacted_text.len())? {
            return Err(DbError::InvalidOutputSegment(format!(
                "chunk {} byte length does not match its UTF-8 payload",
                chunk.id
            )));
        }
        previous_sequence = Some(chunk.sequence);
        original_text_bytes = original_text_bytes.saturating_add(chunk.byte_len);
    }
    let encoded = postcard::to_allocvec(&OperationOutputSegmentPayloadRef {
        version: PTY_OUTPUT_SEGMENT_VERSION,
        chunks,
    })?;
    if encoded.len() > MAX_PTY_OUTPUT_SEGMENT_BYTES {
        return Err(DbError::InvalidOutputSegment(format!(
            "encoded segment exceeds {MAX_PTY_OUTPUT_SEGMENT_BYTES} bytes"
        )));
    }
    let payload = zstd::bulk::compress(&encoded, PTY_OUTPUT_COMPRESSION_LEVEL)?;
    Ok(EncodedOperationOutputSegment {
        operation_id: first.operation_id,
        workspace_id: first.workspace_id,
        first_sequence: first.sequence,
        last_sequence: last.sequence,
        chunk_count: u64::try_from(chunks.len())?,
        original_text_bytes,
        encoded_bytes: u64::try_from(encoded.len())?,
        compressed_bytes: u64::try_from(payload.len())?,
        payload,
        created_at: first.created_at,
    })
}

fn decode_operation_output_segment(row: &SqliteRow) -> Result<Vec<OperationOutputChunk>, DbError> {
    let encoding: String = row.try_get("encoding")?;
    if encoding != PTY_OUTPUT_SEGMENT_ENCODING {
        return Err(DbError::InvalidOutputSegment(format!(
            "unsupported encoding {encoding}"
        )));
    }
    let expected_bytes = usize::try_from(i64_to_u64(row.try_get("uncompressed_byte_len")?)?)?;
    if expected_bytes == 0 || expected_bytes > MAX_PTY_OUTPUT_SEGMENT_BYTES {
        return Err(DbError::InvalidOutputSegment(format!(
            "invalid uncompressed length {expected_bytes}"
        )));
    }
    let compressed: Vec<u8> = row.try_get("payload")?;
    let decoded = zstd::bulk::decompress(&compressed, expected_bytes)?;
    if decoded.len() != expected_bytes {
        return Err(DbError::InvalidOutputSegment(format!(
            "decoded {} bytes but expected {expected_bytes}",
            decoded.len()
        )));
    }
    let payload: OperationOutputSegmentPayload = postcard::from_bytes(&decoded)?;
    if payload.version != PTY_OUTPUT_SEGMENT_VERSION {
        return Err(DbError::InvalidOutputSegment(format!(
            "unsupported payload version {}",
            payload.version
        )));
    }
    let operation_id: OperationId = row.try_get::<String, _>("operation_id")?.parse()?;
    let workspace_id: WorkspaceId = row.try_get::<String, _>("workspace_id")?.parse()?;
    let first_sequence = i64_to_u64(row.try_get("first_sequence")?)?;
    let last_sequence = i64_to_u64(row.try_get("last_sequence")?)?;
    let chunk_count = usize::try_from(i64_to_u64(row.try_get("chunk_count")?)?)?;
    if payload.chunks.len() != chunk_count
        || payload.chunks.first().map(|chunk| chunk.sequence) != Some(first_sequence)
        || payload.chunks.last().map(|chunk| chunk.sequence) != Some(last_sequence)
        || payload
            .chunks
            .iter()
            .any(|chunk| chunk.operation_id != operation_id || chunk.workspace_id != workspace_id)
        || payload
            .chunks
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(DbError::InvalidOutputSegment(
            "payload metadata does not match its segment row".to_owned(),
        ));
    }
    Ok(payload.chunks)
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
        let Some(query) = compile_knowledge_fts_query(query) else {
            return Ok(Vec::new());
        };
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

    /// Inserts or updates a knowledge item and rebuilds its corresponding FTS row.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the transaction.
    pub async fn upsert(&self, item: &KnowledgeItem) -> Result<(), DbError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            INSERT INTO knowledge_items (
                id, title, body, source_json, linked_host_ids_json,
                linked_access_path_ids_json, linked_software_ids_json, linked_operation_ids_json,
                tags_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title, body = excluded.body, source_json = excluded.source_json,
                linked_host_ids_json = excluded.linked_host_ids_json,
                linked_access_path_ids_json = excluded.linked_access_path_ids_json,
                linked_software_ids_json = excluded.linked_software_ids_json,
                linked_operation_ids_json = excluded.linked_operation_ids_json,
                tags_json = excluded.tags_json, updated_at = excluded.updated_at
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
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM knowledge_items_fts WHERE rowid = (SELECT rowid FROM knowledge_items WHERE id = ?)",
        )
        .bind(item.id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO knowledge_items_fts(rowid, title, body, tags) VALUES ((SELECT rowid FROM knowledge_items WHERE id = ?), ?, ?, ?)",
        )
        .bind(item.id.to_string())
        .bind(&item.title)
        .bind(&item.body)
        .bind(item.tags.join(" "))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Gets one knowledge item by id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get(
        &self,
        id: remote_hosts_domain::KnowledgeItemId,
    ) -> Result<Option<KnowledgeItem>, DbError> {
        let row = sqlx::query("SELECT * FROM knowledge_items WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_to_knowledge_item).transpose()
    }

    /// Lists every knowledge item in deterministic update order.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list(&self) -> Result<Vec<KnowledgeItem>, DbError> {
        let rows = sqlx::query("SELECT * FROM knowledge_items ORDER BY updated_at ASC, id ASC")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_to_knowledge_item).collect()
    }
}

fn compile_knowledge_fts_query(query: &str) -> Option<String> {
    let tokens = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| format!(r#""{token}""#))
        .collect::<Vec<_>>();

    (!tokens.is_empty()).then(|| tokens.join(" AND "))
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

fn row_to_instance_identity(row: &SqliteRow) -> Result<InstanceIdentity, DbError> {
    Ok(InstanceIdentity {
        instance_id: uuid::Uuid::parse_str(row.try_get::<&str, _>("instance_id")?)?,
        display_name: row.try_get("display_name")?,
        protocol_version: u16::try_from(row.try_get::<i64, _>("protocol_version")?)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_instance_peer(row: &SqliteRow) -> Result<InstancePeer, DbError> {
    let peer_instance_id = row
        .try_get::<Option<String>, _>("peer_instance_id")?
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()?;
    Ok(InstancePeer {
        id: parse_id(row, "id")?,
        peer_instance_id,
        display_name: row.try_get("display_name")?,
        endpoint: row.try_get("endpoint")?,
        outbound_credential_id: parse_id(row, "outbound_credential_id")?,
        inbound_token_sha256: row.try_get("inbound_token_sha256")?,
        allowed_collections: from_json_col(row, "allowed_collections_json")?,
        state: from_json_col::<InstancePeerState>(row, "state_json")?,
        last_pushed_at: row.try_get("last_pushed_at")?,
        last_pulled_at: row.try_get("last_pulled_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_instance_sync_conflict(row: &SqliteRow) -> Result<InstanceSyncConflict, DbError> {
    Ok(InstanceSyncConflict {
        id: uuid::Uuid::parse_str(row.try_get::<&str, _>("id")?)?,
        origin_instance_id: uuid::Uuid::parse_str(row.try_get::<&str, _>("origin_instance_id")?)?,
        collection: from_json_col(row, "collection_json")?,
        entity_type: row.try_get("entity_type")?,
        entity_key: row.try_get("entity_key")?,
        local_updated_at: row.try_get("local_updated_at")?,
        remote_updated_at: row.try_get("remote_updated_at")?,
        local_payload_sha256: row.try_get("local_payload_sha256")?,
        remote_payload_sha256: row.try_get("remote_payload_sha256")?,
        created_at: row.try_get("created_at")?,
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
        coordination_scope: row.try_get("coordination_scope")?,
        created_at: row.try_get("created_at")?,
        last_activity_at: row.try_get("last_activity_at")?,
        ttl_seconds: i64_to_u64(row.try_get("ttl_seconds")?)?,
    })
}

fn row_to_host_write_lease(row: &SqliteRow) -> Result<HostWriteLease, DbError> {
    Ok(HostWriteLease {
        host_id: parse_id(row, "host_id")?,
        coordination_scope: row.try_get("coordination_scope")?,
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
        interaction: optional_json_col(row, "interaction_json")?,
        transport_evidence: optional_json_col(row, "transport_evidence_json")?,
        coordination_scopes: from_json_col(row, "coordination_scopes_json")?,
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
        payload_kind: from_json_col(row, "payload_kind_json")?,
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
        coordination_scope: row.try_get("coordination_scope")?,
        coordination_scopes: from_json_col(row, "coordination_scopes_json")?,
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
    use std::{io, str::FromStr, time::Duration as StdDuration};

    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, AgentSession, AgentSessionId,
        AgentSessionState, AgentWorkspace, AuthorizedKeyBootstrap, AuthorizedKeyBootstrapReason,
        AuthorizedKeyBootstrapState, ConnectionMode, ConnectionSession, Connector, ConnectorId,
        CredentialId, CredentialKind, CredentialMetadata, EntityState, Environment, EnvironmentId,
        EnvironmentKind, FactSource, Host, HostFact, HostFactId, HostId, HostKind, HostWriteLease,
        KnowledgeItem, KnowledgeItemId, OperationId, OperationOutputArtifact,
        OperationOutputArtifactId, OperationOutputChunk, OperationOutputChunkId, OperationRun,
        OperationState, OperationType, OutputStream, Protocol, PtyBackendCapabilities,
        PtyBackendState, PtyInputEvent, PtyInputEventId, PtyInputEventState, PtyInputPayloadKind,
        PtyOutputChunk, PtyOutputChunkId, PtySession, PtySessionId, RiskLevel, RouteType,
        SessionId, SshChannelKind, SshChannelTransportEvidence, SshFileTransferMode,
        SshTransportBackend, SshTransportCapabilities, SshTransportRuntime, SshTransportRuntimeId,
        SshTransportRuntimeState, SshTransportTelemetry, StateEvent, StateReasonCode,
        StoredCredential, TrustLevel, WorkspaceId, WorkspaceState, now_utc,
    };
    use serde_json::json;
    use sqlx::{Connection as _, Row as _, sqlite::SqliteConnectOptions};

    use super::{
        ClaimedOperationFinish, Repositories, SQLITE_BUSY_TIMEOUT, compile_knowledge_fts_query,
        connect_sqlite, migrate, retry_sqlite_contention,
    };

    #[tokio::test]
    async fn sqlite_connections_use_a_contention_tolerant_busy_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        let busy_timeout_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&pool)
            .await?;

        assert_eq!(
            busy_timeout_ms,
            i64::try_from(SQLITE_BUSY_TIMEOUT.as_millis())?
        );
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_contention_retry_recovers_after_writer_releases_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let database_path = directory.path().join("contention.sqlite");
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", database_path.display()))?
                .create_if_missing(true)
                .busy_timeout(StdDuration::from_millis(1));
        let mut locking_connection = sqlx::SqliteConnection::connect_with(&options).await?;
        let retry_pool = sqlx::SqlitePool::connect_with(options).await?;
        sqlx::query("CREATE TABLE contention_test (value INTEGER NOT NULL)")
            .execute(&mut locking_connection)
            .await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut locking_connection)
            .await?;

        let release = tokio::spawn(async move {
            tokio::time::sleep(StdDuration::from_millis(40)).await;
            sqlx::query("COMMIT").execute(&mut locking_connection).await
        });
        retry_sqlite_contention(|| async {
            sqlx::query("INSERT INTO contention_test (value) VALUES (1)")
                .execute(&retry_pool)
                .await
                .map(|_| ())
                .map_err(super::DbError::from)
        })
        .await?;
        release.await??;

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM contention_test")
            .fetch_one(&retry_pool)
            .await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn knowledge_search_compiles_natural_language_as_literal_fts_tokens() {
        assert_eq!(
            compile_knowledge_fts_query("hacker-s-news deployment"),
            Some(r#""hacker" AND "s" AND "news" AND "deployment""#.to_owned())
        );
        assert_eq!(
            compile_knowledge_fts_query("NAS/家庭服务器"),
            Some(r#""NAS" AND "家庭服务器""#.to_owned())
        );
        assert_eq!(
            compile_knowledge_fts_query("C++ server"),
            Some(r#""C" AND "server""#.to_owned())
        );
        assert_eq!(
            compile_knowledge_fts_query("foo:bar"),
            Some(r#""foo" AND "bar""#.to_owned())
        );
        assert_eq!(
            compile_knowledge_fts_query(r#""quoted" value"#),
            Some(r#""quoted" AND "value""#.to_owned())
        );
        assert_eq!(compile_knowledge_fts_query(r#"- / + : ""#), None);
    }

    async fn apply_pre_scoped_lease_schema(
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        for migration in [
            include_str!("../../../migrations/0001_initial.sql"),
            include_str!("../../../migrations/0002_workspace_operations.sql"),
            include_str!("../../../migrations/0003_operation_claim_leases.sql"),
            include_str!("../../../migrations/0004_output_artifacts.sql"),
            include_str!("../../../migrations/0005_pty_output_chunks.sql"),
            include_str!("../../../migrations/0006_pty_input_events.sql"),
            include_str!("../../../migrations/0007_pty_backend_capabilities.sql"),
            include_str!("../../../migrations/0008_state_event_sequence.sql"),
            include_str!("../../../migrations/0009_authorized_key_bootstrap.sql"),
            include_str!("../../../migrations/0010_ssh_transport_runtime.sql"),
            include_str!("../../../migrations/0011_agent_session_isolation.sql"),
            include_str!("../../../migrations/0012_agent_operation_coordination.sql"),
            include_str!("../../../migrations/0013_infrastructure_topology.sql"),
            include_str!("../../../migrations/0014_channel_capacity_scheduler.sql"),
        ] {
            sqlx::raw_sql(migration).execute(&mut *connection).await?;
        }
        Ok(())
    }

    async fn seed_legacy_scoped_lease_fixture(
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(
            r#"
            INSERT INTO hosts VALUES (
                'host-1', 'legacy-host', 'Legacy Host', '"linux"', NULL, '[]', NULL,
                '"development"', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
            );
            INSERT INTO environments VALUES (
                'env-1', 'legacy-env', '"company_lan"', NULL, '"trusted"', NULL
            );
            INSERT INTO connectors VALUES (
                'connector-1', 'legacy-connector', 'env-1', NULL, '0.1.0', '"healthy"',
                '2026-07-31T00:00:00Z', 'test'
            );
            INSERT INTO credentials VALUES (
                'credential-1', 'legacy-credential', '"ssh_private_key"', 'ops', '{}',
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', NULL
            );
            INSERT INTO access_paths VALUES (
                'path-1', 'host-1', 'env-1', 'connector-1', '"ssh"', '10.0.0.1', 22,
                'ops', 'credential-1', '"lan"', '[]', 1, 1, '"pooled"', 600, 30, 8, 3,
                0, NULL
            );
            INSERT INTO agent_sessions VALUES (
                'agent-1', 'codex', 'legacy-client', 'remote-hosts', 'legacy-task', '"active"',
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', '2026-08-01T00:00:00Z'
            );
            INSERT INTO agent_workspaces (
                workspace_id, host_id, access_path_id, connector_id, label, cwd, state_json,
                policy_profile, created_at, last_activity_at, ttl_seconds, agent_session_id
            ) VALUES (
                'workspace-1', 'host-1', 'path-1', 'connector-1', 'legacy', '/tmp', '"idle"',
                'default', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', 3600, 'agent-1'
            );
            INSERT INTO host_write_leases VALUES (
                'host-1', 'agent-1', 'workspace-1', '2026-07-31T00:00:00Z',
                '2026-07-31T00:00:01Z', '2026-07-31T00:05:00Z'
            );
            "#,
        )
        .execute(connection)
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn scoped_write_lease_migration_preserves_an_active_legacy_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        let mut connection = pool.acquire().await?;
        apply_pre_scoped_lease_schema(&mut connection).await?;
        seed_legacy_scoped_lease_fixture(&mut connection).await?;

        sqlx::raw_sql(include_str!(
            "../../../migrations/0015_scoped_write_leases.sql"
        ))
        .execute(&mut *connection)
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/0016_pty_input_legacy_idempotency_compat.sql"
        ))
        .execute(&mut *connection)
        .await?;

        // The MCP process can outlive the local service during a rolling update.
        // Verify an older client can still prepare its original conflict target.
        sqlx::query(
            r#"
            EXPLAIN INSERT INTO pty_input_events (
                id, pty_session_id, workspace_id, connector_id, state_json, sequence,
                redacted_input_summary, byte_len, created_at
            ) VALUES (
                'event-1', 'pty-1', 'workspace-1', 'connector-1', '"queued"', 0,
                'queued PTY input', 1, '2026-07-31T00:00:00Z'
            ) ON CONFLICT(pty_session_id, idempotency_key) DO NOTHING
            "#,
        )
        .fetch_all(&mut *connection)
        .await?;

        let lease = sqlx::query(
            "SELECT coordination_scope, holder_agent_session_id, expires_at FROM host_write_leases WHERE host_id = 'host-1'",
        )
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(lease.try_get::<String, _>("coordination_scope")?, "host");
        assert_eq!(
            lease.try_get::<String, _>("holder_agent_session_id")?,
            "agent-1"
        );
        assert_eq!(
            lease.try_get::<String, _>("expires_at")?,
            "2026-07-31T00:05:00Z"
        );
        sqlx::query(
            r"
            INSERT INTO host_write_leases (
                host_id, coordination_scope, holder_agent_session_id, holder_workspace_id,
                acquired_at, heartbeat_at, expires_at
            ) VALUES (
                'host-1', 'k8s/test/service/api', 'agent-1', 'workspace-1',
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:01Z', '2026-07-31T00:05:00Z'
            )
            ",
        )
        .execute(&mut *connection)
        .await?;
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM host_write_leases WHERE host_id = 'host-1'",
        )
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(count, 2);

        for migration in [
            include_str!("../../../migrations/0017_compressed_pty_output_segments.sql"),
            include_str!(
                "../../../migrations/0018_output_storage_compatibility_and_pty_input_cleanup.sql"
            ),
            include_str!("../../../migrations/0019_pty_interaction_state.sql"),
            include_str!("../../../migrations/0020_pty_stored_sudo_input.sql"),
            include_str!("../../../migrations/0021_instance_sync.sql"),
        ] {
            sqlx::raw_sql(migration).execute(&mut *connection).await?;
        }
        sqlx::raw_sql(
            r#"
            INSERT INTO connection_sessions VALUES (
                'session-1', 'path-1', 'connector-1', '"connected"',
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', 1, 0, 0, NULL
            );
            INSERT INTO pty_sessions (
                pty_session_id, workspace_id, session_id, state_json, foreground_process, cwd,
                recent_output_ref, last_exit_code, input_allowed, created_at, last_activity_at,
                backend_state_json, backend_capabilities_json, transport_evidence_json,
                interaction_json
            ) VALUES (
                'pty-1', 'workspace-1', 'session-1', '"idle"', NULL, '/tmp', NULL, NULL, 1,
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', '"active"',
                '{"kind":"unknown","terminal_semantics":"unknown","allocates_tty":false,"reuses_ssh_transport":false,"supports_window_resize":false,"supports_signal":false,"supports_streaming_input":false,"supports_streaming_output":false}',
                NULL, NULL
            );
            INSERT INTO operation_runs (
                id, host_id, access_path_id, connector_id, session_id, operation_type_json,
                intent, state_json, started_at, finished_at, exit_code, timeout_seconds,
                redacted_command_summary, redacted_output_summary, log_ref, workspace_id,
                command_profile_json, attempt_count, claim_token, claimed_at, lease_expires_at,
                last_error, transport_evidence_json, agent_session_id, idempotency_key,
                requires_write_lease, coordination_scope
            ) VALUES (
                'operation-1', 'host-1', 'path-1', 'connector-1', NULL, '"mutating_exec"',
                'legacy deployment', '"queued"', '2026-07-31T00:00:00Z', NULL, NULL, 30,
                'deploy', NULL, NULL, 'workspace-1', '{}', 0, NULL, NULL, NULL, NULL, NULL,
                'agent-1', 'legacy-operation', 1, 'prod/datatool-dev/deployment/lichtblick'
            );
            "#,
        )
        .execute(&mut *connection)
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/0022_multi_resource_coordination.sql"
        ))
        .execute(&mut *connection)
        .await?;
        let operation_scopes: String = sqlx::query_scalar(
            "SELECT coordination_scopes_json FROM operation_runs WHERE id = 'operation-1'",
        )
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&operation_scopes)?,
            vec!["prod/datatool-dev/deployment/lichtblick"]
        );
        let pty_scopes: String = sqlx::query_scalar(
            "SELECT coordination_scopes_json FROM pty_sessions WHERE pty_session_id = 'pty-1'",
        )
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&pty_scopes)?,
            vec!["host"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn state_event_cursor_is_monotonic_and_filterable()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool.clone());
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
        let repos = Repositories::new(pool.clone());
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
        sqlx::query(
            r"
            UPDATE system_settings
            SET value = 'pending'
            WHERE setting_key = 'legacy_channel_default_v1'
            ",
        )
        .execute(&repos.access_paths.pool)
        .await?;
        assert_eq!(
            repos.access_paths.upgrade_legacy_channel_default().await?,
            1
        );
        assert_eq!(
            repos
                .access_paths
                .get(path.id)
                .await?
                .ok_or_else(|| io::Error::other("upgraded path exists"))?
                .max_concurrent_channels,
            8
        );
        assert_eq!(
            repos.access_paths.upgrade_legacy_channel_default().await?,
            0,
            "the guarded legacy upgrade must run only once"
        );

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
            coordination_scope: "host".to_owned(),
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
            coordination_scope: "host".to_owned(),
            holder_agent_session_id: agent_session.id,
            holder_workspace_id: workspace.id,
            acquired_at: now,
            heartbeat_at: now,
            expires_at: now + time::Duration::minutes(5),
        };
        assert_eq!(
            repos.host_write_leases.try_acquire(&lease_a, now).await?,
            Some(lease_a.clone())
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
            coordination_scope: "host".to_owned(),
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
            ..lease_b.clone()
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
            .shorten(host.id, "host", agent_session_b.id, takeover_at, now)
            .await?;
        let scoped_at = takeover_at + time::Duration::minutes(6);
        let service_scope = HostWriteLease {
            coordination_scope: "k8s/datatool-dev/service/file-gateway".to_owned(),
            acquired_at: scoped_at,
            heartbeat_at: scoped_at,
            expires_at: scoped_at + time::Duration::minutes(5),
            ..lease_a.clone()
        };
        assert_eq!(
            repos
                .host_write_leases
                .try_acquire(&service_scope, scoped_at)
                .await?,
            Some(service_scope.clone())
        );
        let deployment_scope = HostWriteLease {
            coordination_scope: "k8s/datatool-dev/deployment/report-worker".to_owned(),
            acquired_at: scoped_at,
            heartbeat_at: scoped_at,
            expires_at: scoped_at + time::Duration::minutes(5),
            ..lease_b.clone()
        };
        assert_eq!(
            repos
                .host_write_leases
                .try_acquire(&deployment_scope, scoped_at)
                .await?,
            Some(deployment_scope.clone()),
            "sibling resource scopes should execute concurrently"
        );
        let namespace_scope = HostWriteLease {
            coordination_scope: "k8s/datatool-dev".to_owned(),
            ..deployment_scope.clone()
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&namespace_scope, scoped_at)
                .await?
                .is_none(),
            "a parent namespace scope must conflict with a foreign child scope"
        );
        let broad_scope = HostWriteLease {
            coordination_scope: "host".to_owned(),
            ..deployment_scope.clone()
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&broad_scope, scoped_at)
                .await?
                .is_none(),
            "the default host scope must continue to conflict with every resource scope"
        );
        assert_eq!(
            repos
                .host_write_leases
                .list_active(host.id, scoped_at)
                .await?
                .len(),
            2
        );
        repos
            .host_write_leases
            .shorten(
                host.id,
                &service_scope.coordination_scope,
                agent_session.id,
                now,
                now,
            )
            .await?;
        repos
            .host_write_leases
            .shorten(
                host.id,
                &deployment_scope.coordination_scope,
                agent_session_b.id,
                now,
                now,
            )
            .await?;
        let multi_at = scoped_at + time::Duration::minutes(1);
        let cleanup_scopes = [
            "prod/datatool-dev/storage/minio/rejected-data",
            "prod/datatool-dev/database/mysql/rejected-data",
            "prod/datatool-dev/search/elasticsearch/rejected-data",
        ];
        let cleanup_leases = cleanup_scopes
            .iter()
            .map(|scope| HostWriteLease {
                host_id: host.id,
                coordination_scope: (*scope).to_owned(),
                holder_agent_session_id: agent_session.id,
                holder_workspace_id: workspace.id,
                acquired_at: multi_at,
                heartbeat_at: multi_at,
                expires_at: multi_at + time::Duration::minutes(5),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            repos
                .host_write_leases
                .try_acquire_many(&cleanup_leases, multi_at)
                .await?
                .map(|leases| leases.len()),
            Some(3)
        );
        for scope in [
            "prod/datatool-dev/deployment/lichtblick",
            "prod/datatool-dev/pipeline-recovery/clean",
        ] {
            let unrelated = HostWriteLease {
                host_id: host.id,
                coordination_scope: scope.to_owned(),
                holder_agent_session_id: agent_session_b.id,
                holder_workspace_id: workspace_b.id,
                acquired_at: multi_at,
                heartbeat_at: multi_at,
                expires_at: multi_at + time::Duration::minutes(5),
            };
            assert!(
                repos
                    .host_write_leases
                    .try_acquire(&unrelated, multi_at)
                    .await?
                    .is_some(),
                "unrelated production resources should proceed concurrently"
            );
        }
        let partially_conflicting = [
            "prod/datatool-dev/diagnostics/free-before-rollback",
            "prod/datatool-dev/storage/minio/rejected-data/object-42",
        ]
        .into_iter()
        .map(|scope| HostWriteLease {
            host_id: host.id,
            coordination_scope: scope.to_owned(),
            holder_agent_session_id: agent_session_b.id,
            holder_workspace_id: workspace_b.id,
            acquired_at: multi_at,
            heartbeat_at: multi_at,
            expires_at: multi_at + time::Duration::minutes(5),
        })
        .collect::<Vec<_>>();
        assert!(
            repos
                .host_write_leases
                .try_acquire_many(&partially_conflicting, multi_at)
                .await?
                .is_none(),
            "one overlapping resource must reject the complete lease set"
        );
        assert!(
            repos
                .host_write_leases
                .list_active(host.id, multi_at)
                .await?
                .iter()
                .all(|lease| lease.coordination_scope
                    != "prod/datatool-dev/diagnostics/free-before-rollback"),
            "a rejected multi-resource request must not retain a partial lease"
        );
        repos
            .host_write_leases
            .shorten_many(
                host.id,
                &cleanup_scopes.map(str::to_owned),
                agent_session.id,
                now,
                now,
            )
            .await?;
        repos
            .host_write_leases
            .shorten_many(
                host.id,
                &[
                    "prod/datatool-dev/deployment/lichtblick".to_owned(),
                    "prod/datatool-dev/pipeline-recovery/clean".to_owned(),
                ],
                agent_session_b.id,
                now,
                now,
            )
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
        let (first_open, second_open) = tokio::join!(
            repos
                .connection_sessions
                .open_channel(&session, true, false),
            repos
                .connection_sessions
                .open_channel(&session, true, false),
        );
        first_open?;
        second_open?;
        let concurrently_opened = repos
            .connection_sessions
            .get(session.session_id)
            .await?
            .ok_or_else(|| io::Error::other("connection session exists"))?;
        assert_eq!(concurrently_opened.open_channels, 3);
        assert_eq!(concurrently_opened.reused_count, 5);
        let (first_close, second_close) = tokio::join!(
            repos
                .connection_sessions
                .close_channel(session.session_id, now),
            repos
                .connection_sessions
                .close_channel(session.session_id, now),
        );
        first_close?;
        second_close?;
        let concurrently_closed = repos
            .connection_sessions
            .get(session.session_id)
            .await?
            .ok_or_else(|| io::Error::other("connection session exists"))?;
        assert_eq!(concurrently_closed.open_channels, 1);
        repos
            .connection_sessions
            .open_channel(&session, true, false)
            .await?;
        repos
            .connection_sessions
            .open_channel(&session, true, false)
            .await?;
        let (first_failure, second_failure) = tokio::join!(
            repos.connection_sessions.record_failure(
                session.session_id,
                now,
                EntityState::Degraded,
                "concurrent failure",
                true,
                true,
                true,
                3,
            ),
            repos.connection_sessions.record_failure(
                session.session_id,
                now,
                EntityState::Degraded,
                "concurrent failure",
                true,
                true,
                true,
                3,
            ),
        );
        first_failure?;
        second_failure?;
        let twice_failed = repos
            .connection_sessions
            .get(session.session_id)
            .await?
            .ok_or_else(|| io::Error::other("connection session exists"))?;
        assert_eq!(twice_failed.open_channels, 1);
        assert_eq!(twice_failed.failure_count, 2);
        assert_eq!(twice_failed.state, EntityState::Degraded);
        repos
            .connection_sessions
            .open_channel(&session, true, false)
            .await?;
        let circuit_open = repos
            .connection_sessions
            .record_failure(
                session.session_id,
                now,
                EntityState::Degraded,
                "threshold failure",
                true,
                true,
                true,
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("connection session exists"))?;
        assert_eq!(circuit_open.open_channels, 1);
        assert_eq!(circuit_open.failure_count, 3);
        assert_eq!(circuit_open.state, EntityState::CircuitOpen);
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
            coordination_scopes: vec![workspace.coordination_scope.clone()],
            state: WorkspaceState::Idle,
            foreground_process: None,
            cwd: Some("/tmp".to_owned()),
            recent_output_ref: None,
            last_exit_code: None,
            input_allowed: true,
            backend_state: PtyBackendState::Pending,
            backend_capabilities: PtyBackendCapabilities::unknown(),
            interaction: None,
            transport_evidence: None,
            created_at: now,
            last_activity_at: now,
        };
        repos.pty_sessions.upsert(&pty).await?;
        let mut second_workspace = workspace.clone();
        second_workspace.id = WorkspaceId::new();
        second_workspace.label = "agent-secondary".to_owned();
        repos.workspaces.insert(&second_workspace).await?;
        let mut second_pty = pty.clone();
        second_pty.pty_session_id = PtySessionId::new();
        second_pty.workspace_id = second_workspace.id;
        repos.pty_sessions.upsert(&second_pty).await?;
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
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_host(workspace.host_id)
                .await?,
            2
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
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_host(workspace.host_id)
                .await?,
            1
        );
        repos
            .workspaces
            .update_state(second_workspace.id, WorkspaceState::Done, now)
            .await?;
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_host(workspace.host_id)
                .await?,
            0,
            "an active PTY belonging to a terminal workspace must not consume host capacity"
        );
        assert_eq!(
            repos.active_work_summary().await?.pending_or_active_ptys,
            0,
            "terminal workspace PTYs must not block a service update"
        );
        second_pty.state = WorkspaceState::Done;
        second_pty.input_allowed = false;
        second_pty.backend_state = PtyBackendState::Closed;
        repos.pty_sessions.upsert(&second_pty).await?;
        assert_eq!(
            repos
                .pty_sessions
                .count_active_for_host(workspace.host_id)
                .await?,
            0,
            "terminal PTY history must not consume the host concurrency limit"
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
        assert!(
            !repos.compressed_output_writes_enabled().await?,
            "a routine binary upgrade must keep output visible to resident legacy MCP readers"
        );
        let legacy_visible_text: String = sqlx::query_scalar(
            "SELECT redacted_text FROM pty_output_chunks WHERE pty_session_id = ? AND sequence = 0",
        )
        .bind(pty.pty_session_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(legacy_visible_text, "pty hello");
        let compressed_before_activation: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pty_output_segments")
                .fetch_one(&pool)
                .await?;
        assert_eq!(compressed_before_activation, 0);
        repos.activate_compressed_output_writes().await?;
        assert!(repos.compressed_output_writes_enabled().await?);
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
        let batched_chunks = (1_u64..=3)
            .map(
                |sequence| -> Result<PtyOutputChunk, std::num::TryFromIntError> {
                    let redacted_text = format!("repeated terminal output {sequence}\n").repeat(32);
                    Ok(PtyOutputChunk {
                        id: PtyOutputChunkId::new(),
                        pty_session_id: pty.pty_session_id,
                        workspace_id: workspace.id,
                        stream: OutputStream::Stdout,
                        sequence,
                        byte_len: u64::try_from(redacted_text.len())?,
                        redacted_text,
                        truncated: false,
                        created_at: now + time::Duration::milliseconds(i64::try_from(sequence)?),
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        repos
            .pty_output_chunks
            .insert_batch(&batched_chunks)
            .await?;
        assert_eq!(
            repos
                .pty_output_chunks
                .next_sequence(pty.pty_session_id)
                .await?,
            4
        );
        let after_first = repos
            .pty_output_chunks
            .list_for_session(pty.pty_session_id, Some(0), 2)
            .await?;
        assert_eq!(
            after_first
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            after_first
                .iter()
                .all(|chunk| chunk.redacted_text.starts_with("repeated terminal output"))
        );

        for sequence in 4_u64..=7 {
            let redacted_text = format!("legacy output {sequence}\n").repeat(16);
            let legacy = PtyOutputChunk {
                id: PtyOutputChunkId::new(),
                pty_session_id: pty.pty_session_id,
                workspace_id: workspace.id,
                stream: OutputStream::Stderr,
                sequence,
                byte_len: u64::try_from(redacted_text.len())?,
                redacted_text,
                truncated: sequence == 7,
                created_at: now + time::Duration::milliseconds(i64::try_from(sequence)?),
            };
            sqlx::query(
                r"
                INSERT INTO pty_output_chunks (
                    id, pty_session_id, workspace_id, stream_json, sequence, redacted_text,
                    byte_len, truncated, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ",
            )
            .bind(legacy.id.to_string())
            .bind(legacy.pty_session_id.to_string())
            .bind(legacy.workspace_id.to_string())
            .bind(serde_json::to_string(&legacy.stream)?)
            .bind(i64::try_from(legacy.sequence)?)
            .bind(&legacy.redacted_text)
            .bind(i64::try_from(legacy.byte_len)?)
            .bind(i64::from(legacy.truncated))
            .bind(legacy.created_at)
            .execute(&pool)
            .await?;
        }
        let compacted = repos
            .pty_output_chunks
            .compact_legacy_batch(100, 1024 * 1024)
            .await?;
        assert_eq!(compacted.legacy_chunks, 5);
        assert_eq!(compacted.segments_written, 1);
        assert!(compacted.compressed_bytes < compacted.original_storage_bytes);
        let legacy_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pty_output_chunks")
            .fetch_one(&pool)
            .await?;
        assert_eq!(legacy_rows, 0);
        let all_pty_chunks = repos
            .pty_output_chunks
            .list_for_session(pty.pty_session_id, None, 20)
            .await?;
        assert_eq!(
            all_pty_chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            (0_u64..=7).collect::<Vec<_>>()
        );
        assert!(all_pty_chunks[7].truncated);
        let mut input_pty = pty.clone();
        input_pty.state = WorkspaceState::Working;
        input_pty.input_allowed = true;
        input_pty.backend_state = PtyBackendState::Active;
        repos.pty_sessions.upsert(&input_pty).await?;
        let pty_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: pty.pty_session_id,
            workspace_id: workspace.id,
            connector_id: connector.id,
            host_id: host.id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: None,
            payload_kind: PtyInputPayloadKind::Text,
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
        let undeliverable_input = PtyInputEvent {
            id: PtyInputEventId::new(),
            pty_session_id: input_pty.pty_session_id,
            workspace_id: workspace.id,
            connector_id: connector.id,
            host_id: host.id,
            agent_session_id: workspace.agent_session_id,
            idempotency_key: Some("close-before-delivery".to_owned()),
            payload_kind: PtyInputPayloadKind::Text,
            input_fingerprint: Some("close-before-delivery-fingerprint".to_owned()),
            state: PtyInputEventState::Queued,
            sequence: 1,
            redacted_input_summary: "queued input awaiting close".to_owned(),
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
            .insert(&undeliverable_input, "never send\n")
            .await?;
        repos
            .pty_sessions
            .close(input_pty.pty_session_id, Some(0), now)
            .await?;
        let failed_input = repos
            .pty_input_events
            .get(undeliverable_input.id)
            .await?
            .ok_or_else(|| io::Error::other("failed PTY input metadata exists"))?;
        assert_eq!(failed_input.state, PtyInputEventState::Failed);
        assert!(failed_input.failed_at.is_some());
        assert_eq!(
            failed_input.last_error.as_deref(),
            Some("pty_input_delivery_unavailable")
        );
        let raw_input: Option<String> =
            sqlx::query_scalar("SELECT input_text FROM pty_input_events WHERE id = ?")
                .bind(undeliverable_input.id.to_string())
                .fetch_one(&pool)
                .await?;
        assert!(raw_input.is_none());
        let mut raced_input = undeliverable_input.clone();
        raced_input.id = PtyInputEventId::new();
        raced_input.sequence = 2;
        raced_input.idempotency_key = Some("insert-after-close".to_owned());
        repos
            .pty_input_events
            .insert(&raced_input, "also never send\n")
            .await?;
        let raced_input = repos
            .pty_input_events
            .get(raced_input.id)
            .await?
            .ok_or_else(|| io::Error::other("raced PTY input metadata exists"))?;
        assert_eq!(raced_input.state, PtyInputEventState::Failed);
        assert!(raced_input.failed_at.is_some());

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
            coordination_scope: workspace.coordination_scope.clone(),
            coordination_scopes: vec![workspace.coordination_scope.clone()],
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
            coordination_scope: "host".to_owned(),
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
            .shorten(host.id, "host", agent_session.id, lock_at, lock_at)
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
        let mut same_session_mutation = mutating_b.clone();
        same_session_mutation.id = OperationId::new();
        same_session_mutation.idempotency_key = Some("agent-b-write-2".to_owned());
        same_session_mutation.state = OperationState::Queued;
        same_session_mutation.started_at = lock_at + time::Duration::seconds(2);
        same_session_mutation.claim_token = None;
        same_session_mutation.claimed_at = None;
        same_session_mutation.lease_expires_at = None;
        same_session_mutation.attempt_count = 0;
        repos.operations.insert(&same_session_mutation).await?;
        assert!(
            repos
                .operations
                .claim_next_for_connector(
                    connector.id,
                    "claim-same-session-write-2",
                    lock_at,
                    lock_at + time::Duration::minutes(5),
                    3,
                )
                .await?
                .is_none(),
            "same-session host mutations must not overlap"
        );
        repos
            .operations
            .finish_claimed(ClaimedOperationFinish {
                id: claimed_mutation.id,
                claim_token: "claim-mutation-b",
                state: OperationState::Succeeded,
                finished_at: lock_at,
                exit_code: Some(0),
                redacted_output_summary: Some("ok"),
                last_error: None,
            })
            .await?;
        let claimed_second_mutation = repos
            .operations
            .claim_next_for_connector(
                connector.id,
                "claim-same-session-write-2",
                lock_at,
                lock_at + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("next mutation should run after prior completion"))?;
        assert_eq!(claimed_second_mutation.id, same_session_mutation.id);
        repos
            .operations
            .finish_claimed(ClaimedOperationFinish {
                id: claimed_second_mutation.id,
                claim_token: "claim-same-session-write-2",
                state: OperationState::Succeeded,
                finished_at: lock_at,
                exit_code: Some(0),
                redacted_output_summary: Some("ok"),
                last_error: None,
            })
            .await?;

        let scoped_schedule_at = lock_at + time::Duration::minutes(10);
        let scoped_service_lease = HostWriteLease {
            host_id: host.id,
            coordination_scope: "k8s/datatool-dev/service/file-gateway".to_owned(),
            holder_agent_session_id: agent_session.id,
            holder_workspace_id: workspace.id,
            acquired_at: scoped_schedule_at,
            heartbeat_at: scoped_schedule_at,
            expires_at: scoped_schedule_at + time::Duration::minutes(5),
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&scoped_service_lease, scoped_schedule_at)
                .await?
                .is_some()
        );
        let mut sibling_mutation = mutating_b.clone();
        sibling_mutation.id = OperationId::new();
        sibling_mutation.coordination_scope =
            "k8s/datatool-dev/deployment/report-worker".to_owned();
        sibling_mutation.coordination_scopes = vec![sibling_mutation.coordination_scope.clone()];
        sibling_mutation.idempotency_key = Some("scoped-sibling-write".to_owned());
        sibling_mutation.state = OperationState::Queued;
        sibling_mutation.started_at = scoped_schedule_at;
        sibling_mutation.finished_at = None;
        sibling_mutation.claim_token = None;
        sibling_mutation.claimed_at = None;
        sibling_mutation.lease_expires_at = None;
        sibling_mutation.attempt_count = 0;
        repos.operations.insert(&sibling_mutation).await?;
        let claimed_sibling = repos
            .operations
            .claim_next_for_connector(
                connector.id,
                "claim-scoped-sibling",
                scoped_schedule_at,
                scoped_schedule_at + time::Duration::minutes(5),
                3,
            )
            .await?
            .ok_or_else(|| io::Error::other("sibling resource mutation should run concurrently"))?;
        assert_eq!(claimed_sibling.id, sibling_mutation.id);
        repos
            .operations
            .finish_claimed(ClaimedOperationFinish {
                id: claimed_sibling.id,
                claim_token: "claim-scoped-sibling",
                state: OperationState::Succeeded,
                finished_at: scoped_schedule_at,
                exit_code: Some(0),
                redacted_output_summary: Some("ok"),
                last_error: None,
            })
            .await?;

        let mut parent_mutation = sibling_mutation.clone();
        parent_mutation.id = OperationId::new();
        parent_mutation.coordination_scope = "k8s/datatool-dev".to_owned();
        parent_mutation.coordination_scopes = vec![parent_mutation.coordination_scope.clone()];
        parent_mutation.idempotency_key = Some("scoped-parent-write".to_owned());
        parent_mutation.state = OperationState::Queued;
        parent_mutation.started_at = scoped_schedule_at + time::Duration::seconds(1);
        parent_mutation.finished_at = None;
        parent_mutation.claim_token = None;
        parent_mutation.claimed_at = None;
        parent_mutation.lease_expires_at = None;
        parent_mutation.attempt_count = 0;
        repos.operations.insert(&parent_mutation).await?;
        assert!(
            repos
                .operations
                .claim_next_for_connector(
                    connector.id,
                    "claim-scoped-parent",
                    scoped_schedule_at,
                    scoped_schedule_at + time::Duration::minutes(5),
                    3,
                )
                .await?
                .is_none(),
            "a parent scope mutation must wait for an active child resource lease"
        );

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
        sqlx::query(
            "UPDATE system_settings SET value = 'disabled' WHERE setting_key = 'compressed_output_writes_v1'",
        )
        .execute(&pool)
        .await?;
        repos.operation_output_chunks.insert(&chunk).await?;
        let legacy_command_text: String = sqlx::query_scalar(
            "SELECT redacted_text FROM operation_output_chunks WHERE operation_id = ? AND sequence = 0",
        )
        .bind(operation.id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(legacy_command_text, "queued");
        repos.activate_compressed_output_writes().await?;
        let chunks = repos
            .operation_output_chunks
            .list_for_workspace(workspace.id, Some(operation.id), None, 10)
            .await?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].stream, OutputStream::System);
        for sequence in 1_u64..=3 {
            let redacted_text = format!("compressed command output {sequence}\n").repeat(16);
            repos
                .operation_output_chunks
                .insert(&OperationOutputChunk {
                    id: OperationOutputChunkId::new(),
                    operation_id: operation.id,
                    workspace_id: workspace.id,
                    stream: OutputStream::Stdout,
                    sequence,
                    byte_len: u64::try_from(redacted_text.len())?,
                    redacted_text,
                    truncated: false,
                    created_at: now + time::Duration::milliseconds(i64::try_from(sequence)?),
                })
                .await?;
        }
        assert_eq!(
            repos
                .operation_output_chunks
                .next_sequence(operation.id)
                .await?,
            4
        );
        let operation_storage = repos.operation_output_chunks.storage_stats().await?;
        assert_eq!(operation_storage.compressed_chunks, 3);
        assert_eq!(operation_storage.compressed_segments, 1);
        let after_operation_sequence = repos
            .operation_output_chunks
            .list_for_workspace(workspace.id, Some(operation.id), Some(1), 10)
            .await?;
        assert_eq!(
            after_operation_sequence
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        for sequence in 4_u64..=6 {
            let redacted_text = format!("legacy command output {sequence}\n").repeat(16);
            let legacy = OperationOutputChunk {
                id: OperationOutputChunkId::new(),
                operation_id: operation.id,
                workspace_id: workspace.id,
                stream: OutputStream::Stderr,
                sequence,
                byte_len: u64::try_from(redacted_text.len())?,
                redacted_text,
                truncated: sequence == 6,
                created_at: now + time::Duration::milliseconds(i64::try_from(sequence)?),
            };
            sqlx::query(
                r"
                INSERT INTO operation_output_chunks (
                    id, operation_id, workspace_id, stream_json, sequence, redacted_text,
                    byte_len, truncated, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ",
            )
            .bind(legacy.id.to_string())
            .bind(legacy.operation_id.to_string())
            .bind(legacy.workspace_id.to_string())
            .bind(serde_json::to_string(&legacy.stream)?)
            .bind(i64::try_from(legacy.sequence)?)
            .bind(&legacy.redacted_text)
            .bind(i64::try_from(legacy.byte_len)?)
            .bind(i64::from(legacy.truncated))
            .bind(legacy.created_at)
            .execute(&pool)
            .await?;
        }
        let compacted_operation_output = repos
            .operation_output_chunks
            .compact_legacy_batch(100, 1024 * 1024)
            .await?;
        assert_eq!(compacted_operation_output.legacy_chunks, 4);
        let all_operation_chunks = repos
            .operation_output_chunks
            .list_for_workspace(workspace.id, Some(operation.id), None, 20)
            .await?;
        assert_eq!(
            all_operation_chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            (0_u64..=6).collect::<Vec<_>>()
        );
        assert!(all_operation_chunks[6].truncated);

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
        let punctuated_knowledge = KnowledgeItem {
            id: KnowledgeItemId::new(),
            title: "hacker-s-news deployment on NAS/家庭服务器".to_owned(),
            body: "A C++ server exposes foo:bar routing metadata".to_owned(),
            source: FactSource::Manual,
            linked_host_ids: vec![host.id],
            linked_access_path_ids: vec![path.id],
            linked_software_ids: Vec::new(),
            linked_operation_ids: Vec::new(),
            tags: vec!["quoted".to_owned(), "value".to_owned()],
            created_at: now,
            updated_at: now,
        };
        repos.knowledge.insert(&punctuated_knowledge).await?;
        for query in [
            "hacker-s-news deployment",
            "NAS/家庭服务器",
            "C++ server",
            "foo:bar",
            r#""quoted" value"#,
        ] {
            let matches = repos.knowledge.search(query, 10).await?;
            assert_eq!(
                matches.len(),
                1,
                "query {query:?} should return exactly one literal match"
            );
            assert_eq!(matches[0].id, punctuated_knowledge.id);
        }
        assert!(repos.knowledge.search(r#"- / + : ""#, 10).await?.is_empty());

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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn closed_workspace_work_is_reconciled_without_touching_active_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "restart-reconciliation".to_owned(),
            display_name: "Restart Reconciliation".to_owned(),
            kind: HostKind::Linux,
            owner: None,
            tags: Vec::new(),
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;
        let environment = Environment {
            id: EnvironmentId::new(),
            name: "restart-reconciliation".to_owned(),
            kind: EnvironmentKind::HomeLan,
            description: None,
            trust_level: TrustLevel::Owned,
            notes: None,
        };
        repos.environments.insert(&environment).await?;
        let connector = Connector {
            id: ConnectorId::new(),
            name: "restart-reconciliation".to_owned(),
            environment_id: environment.id,
            host_id: None,
            version: "test".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("test".to_owned()),
        };
        repos.connectors.upsert(&connector).await?;
        let credential = StoredCredential {
            metadata: CredentialMetadata {
                id: CredentialId::new(),
                name: "restart-reconciliation".to_owned(),
                kind: CredentialKind::SshPassword,
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
            address: "127.0.0.1".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id: credential.metadata.id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 8,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repos.access_paths.insert(&path).await?;
        let connection = ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: path.id,
            connector_id: connector.id,
            state: EntityState::Connected,
            created_at: now,
            last_used_at: now,
            open_channels: 1,
            reused_count: 0,
            failure_count: 0,
            last_error: None,
        };
        repos.connection_sessions.upsert(&connection).await?;

        let stale_agent = AgentSession {
            id: AgentSessionId::new(),
            client_kind: "codex".to_owned(),
            client_instance_id: "stale-task".to_owned(),
            project_key: Some("remote-hosts".to_owned()),
            conversation_key: Some("stale-conversation".to_owned()),
            state: AgentSessionState::Active,
            created_at: now,
            last_seen_at: now,
            expires_at: now + time::Duration::hours(1),
        };
        repos.agent_sessions.upsert(&stale_agent).await?;
        let stale_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(stale_agent.id),
            host_id: host.id,
            access_path_id: path.id,
            connector_id: connector.id,
            label: "stale-workspace".to_owned(),
            cwd: None,
            state: WorkspaceState::Closed,
            policy_profile: "default".to_owned(),
            coordination_scope: "prod/cleanup".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        };
        repos.workspaces.insert(&stale_workspace).await?;
        let stale_operation = OperationRun {
            id: OperationId::new(),
            host_id: host.id,
            access_path_id: path.id,
            connector_id: connector.id,
            session_id: Some(connection.session_id),
            workspace_id: Some(stale_workspace.id),
            agent_session_id: Some(stale_agent.id),
            idempotency_key: Some("stale-operation".to_owned()),
            requires_write_lease: true,
            coordination_scope: stale_workspace.coordination_scope.clone(),
            coordination_scopes: vec![stale_workspace.coordination_scope.clone()],
            operation_type: OperationType::Runbook,
            intent: "stale mutation".to_owned(),
            state: OperationState::Running,
            started_at: now,
            finished_at: None,
            exit_code: None,
            timeout_seconds: 3600,
            redacted_command_summary: "stale command".to_owned(),
            command_profile_json: Some(json!({"name": "shell.posix"})),
            transport_evidence: None,
            redacted_output_summary: Some("claimed by connector worker".to_owned()),
            log_ref: None,
            attempt_count: 1,
            claim_token: Some("stale-claim".to_owned()),
            claimed_at: Some(now),
            lease_expires_at: Some(now + time::Duration::minutes(5)),
            last_error: None,
        };
        repos.operations.insert(&stale_operation).await?;
        let stale_lease = HostWriteLease {
            host_id: host.id,
            coordination_scope: stale_workspace.coordination_scope.clone(),
            holder_agent_session_id: stale_agent.id,
            holder_workspace_id: stale_workspace.id,
            acquired_at: now,
            heartbeat_at: now,
            expires_at: now + time::Duration::minutes(5),
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&stale_lease, now)
                .await?
                .is_some()
        );

        let active_agent = AgentSession {
            id: AgentSessionId::new(),
            client_instance_id: "active-task".to_owned(),
            conversation_key: Some("active-conversation".to_owned()),
            ..stale_agent.clone()
        };
        repos.agent_sessions.upsert(&active_agent).await?;
        let active_workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: Some(active_agent.id),
            label: "active-workspace".to_owned(),
            state: WorkspaceState::Blocked,
            coordination_scope: "prod/audit".to_owned(),
            ..stale_workspace.clone()
        };
        repos.workspaces.insert(&active_workspace).await?;
        let active_pty = PtySession {
            pty_session_id: PtySessionId::new(),
            workspace_id: active_workspace.id,
            session_id: connection.session_id,
            coordination_scopes: vec![active_workspace.coordination_scope.clone()],
            state: WorkspaceState::Blocked,
            foreground_process: None,
            cwd: None,
            recent_output_ref: None,
            last_exit_code: None,
            input_allowed: true,
            backend_state: PtyBackendState::Active,
            backend_capabilities: PtyBackendCapabilities::unknown(),
            interaction: None,
            transport_evidence: None,
            created_at: now,
            last_activity_at: now,
        };
        repos.pty_sessions.upsert(&active_pty).await?;

        let reconciled = repos.reconcile_closed_workspace_work(now).await?;
        assert_eq!(reconciled.cancelled_operations, 1);
        assert_eq!(reconciled.released_write_leases, 1);
        let cancelled = repos
            .operations
            .get(stale_operation.id)
            .await?
            .ok_or_else(|| io::Error::other("cancelled operation exists"))?;
        assert_eq!(cancelled.state, OperationState::Cancelled);
        assert_eq!(cancelled.finished_at, Some(now));
        assert!(cancelled.claim_token.is_none());
        assert!(cancelled.claimed_at.is_none());
        assert!(cancelled.lease_expires_at.is_none());
        assert!(
            !repos
                .operations
                .renew_claim(
                    stale_operation.id,
                    "stale-claim",
                    now + time::Duration::minutes(10),
                )
                .await?
        );
        assert!(
            repos
                .host_write_leases
                .list_active(host.id, now)
                .await?
                .is_empty()
        );

        let post_reconciliation_operation = OperationRun {
            id: OperationId::new(),
            idempotency_key: Some("terminal-workspace-race".to_owned()),
            state: OperationState::Queued,
            session_id: None,
            claim_token: None,
            claimed_at: None,
            lease_expires_at: None,
            attempt_count: 0,
            ..stale_operation.clone()
        };
        repos
            .operations
            .insert(&post_reconciliation_operation)
            .await?;
        assert!(
            repos
                .operations
                .claim_next_for_connector(
                    connector.id,
                    "must-not-claim-terminal-workspace",
                    now,
                    now + time::Duration::minutes(5),
                    3,
                )
                .await?
                .is_none(),
            "the scheduler must never reclaim work from a terminal Workspace"
        );
        assert_eq!(
            repos
                .reconcile_closed_workspace_work(now)
                .await?
                .cancelled_operations,
            1
        );

        let summary = repos.active_work_summary().await?;
        assert_eq!(summary.queued_or_running_operations, 0);
        assert_eq!(summary.unexpired_write_leases, 0);
        assert_eq!(summary.pending_or_active_ptys, 1);
        assert!(!summary.is_idle());
        let retained_pty = repos
            .pty_sessions
            .get(active_pty.pty_session_id)
            .await?
            .ok_or_else(|| io::Error::other("active PTY remains"))?;
        assert_eq!(retained_pty.pty_session_id, active_pty.pty_session_id);
        assert_eq!(retained_pty.backend_state, PtyBackendState::Active);

        let trigger_operation = OperationRun {
            id: OperationId::new(),
            workspace_id: Some(active_workspace.id),
            agent_session_id: Some(active_agent.id),
            coordination_scope: active_workspace.coordination_scope.clone(),
            coordination_scopes: vec![active_workspace.coordination_scope.clone()],
            idempotency_key: Some("terminal-workspace-trigger".to_owned()),
            ..stale_operation
        };
        repos.operations.insert(&trigger_operation).await?;
        let trigger_lease = HostWriteLease {
            coordination_scope: active_workspace.coordination_scope.clone(),
            holder_agent_session_id: active_agent.id,
            holder_workspace_id: active_workspace.id,
            ..stale_lease
        };
        assert!(
            repos
                .host_write_leases
                .try_acquire(&trigger_lease, now)
                .await?
                .is_some()
        );
        repos
            .workspaces
            .update_state(active_workspace.id, WorkspaceState::Closed, now)
            .await?;
        let trigger_cancelled = repos
            .operations
            .get(trigger_operation.id)
            .await?
            .ok_or_else(|| io::Error::other("trigger-cancelled operation exists"))?;
        assert_eq!(trigger_cancelled.state, OperationState::Cancelled);
        assert!(trigger_cancelled.claim_token.is_none());
        assert!(
            repos
                .host_write_leases
                .list_active(host.id, now)
                .await?
                .is_empty(),
            "the migration trigger must release the terminal Workspace lease immediately"
        );
        Ok(())
    }
}

/// Repository for the local instance identity and direct peer-sync bookkeeping.
#[derive(Clone)]
pub struct InstanceSyncRepository {
    pool: SqlitePool,
}

impl InstanceSyncRepository {
    /// Creates a repository.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Gets the local identity when initialized.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_identity(&self) -> Result<Option<InstanceIdentity>, DbError> {
        let row = sqlx::query(
            "SELECT instance_id, display_name, protocol_version, created_at, updated_at FROM instance_identity WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_instance_identity).transpose()
    }

    /// Gets the identity or creates it exactly once.
    ///
    /// # Errors
    ///
    /// Returns an error if the identity cannot be read or inserted.
    pub async fn get_or_create_identity(
        &self,
        display_name: &str,
        now: OffsetDateTime,
    ) -> Result<InstanceIdentity, DbError> {
        if let Some(identity) = self.get_identity().await? {
            return Ok(identity);
        }
        let identity = InstanceIdentity {
            instance_id: uuid::Uuid::now_v7(),
            display_name: display_name.to_owned(),
            protocol_version: 1,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            r"
            INSERT INTO instance_identity (
                singleton, instance_id, display_name, protocol_version, created_at, updated_at
            ) VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(singleton) DO NOTHING
            ",
        )
        .bind(identity.instance_id.to_string())
        .bind(&identity.display_name)
        .bind(i64::from(identity.protocol_version))
        .bind(identity.created_at)
        .bind(identity.updated_at)
        .execute(&self.pool)
        .await?;
        self.get_identity().await?.ok_or_else(|| {
            DbError::InvalidOutputSegment("instance identity disappeared".to_owned())
        })
    }

    /// Lists peer records in display-name order.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_peers(&self) -> Result<Vec<InstancePeer>, DbError> {
        let rows = sqlx::query("SELECT * FROM instance_sync_peers ORDER BY display_name")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_to_instance_peer).collect()
    }

    /// Gets a peer by local id.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_peer(&self, id: InstancePeerId) -> Result<Option<InstancePeer>, DbError> {
        let row = sqlx::query("SELECT * FROM instance_sync_peers WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_to_instance_peer).transpose()
    }

    /// Gets a peer by its stable local display label.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_peer_by_display_name(
        &self,
        display_name: &str,
    ) -> Result<Option<InstancePeer>, DbError> {
        let row = sqlx::query("SELECT * FROM instance_sync_peers WHERE display_name = ?")
            .bind(display_name)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_to_instance_peer).transpose()
    }

    /// Gets an active peer accepting the supplied inbound token digest.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn get_active_peer_by_inbound_token_sha256(
        &self,
        token_sha256: &str,
    ) -> Result<Option<InstancePeer>, DbError> {
        let row = sqlx::query(
            r"
            SELECT * FROM instance_sync_peers
            WHERE inbound_token_sha256 = ? AND state_json = ?
            ",
        )
        .bind(token_sha256)
        .bind(to_json(&InstancePeerState::Active)?)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_instance_peer).transpose()
    }

    /// Inserts or updates an approved peer by id.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the write.
    pub async fn upsert_peer(&self, peer: &InstancePeer) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO instance_sync_peers (
                id, peer_instance_id, display_name, endpoint, outbound_credential_id,
                inbound_token_sha256, allowed_collections_json, state_json, last_pushed_at,
                last_pulled_at, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                peer_instance_id = excluded.peer_instance_id,
                display_name = excluded.display_name,
                endpoint = excluded.endpoint,
                outbound_credential_id = excluded.outbound_credential_id,
                inbound_token_sha256 = excluded.inbound_token_sha256,
                allowed_collections_json = excluded.allowed_collections_json,
                state_json = excluded.state_json,
                last_pushed_at = excluded.last_pushed_at,
                last_pulled_at = excluded.last_pulled_at,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            ",
        )
        .bind(peer.id.to_string())
        .bind(peer.peer_instance_id.map(|id| id.to_string()))
        .bind(&peer.display_name)
        .bind(&peer.endpoint)
        .bind(peer.outbound_credential_id.to_string())
        .bind(&peer.inbound_token_sha256)
        .bind(to_json(&peer.allowed_collections)?)
        .bind(to_json(&peer.state)?)
        .bind(peer.last_pushed_at)
        .bind(peer.last_pulled_at)
        .bind(&peer.last_error)
        .bind(peer.created_at)
        .bind(peer.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records an idempotency receipt. `true` means this payload was already received.
    ///
    /// # Errors
    ///
    /// Returns an error if querying fails.
    pub async fn has_receipt(
        &self,
        origin_instance_id: uuid::Uuid,
        collection: InstanceSyncCollection,
        entity_type: &str,
        entity_key: &str,
        payload_sha256: &str,
    ) -> Result<bool, DbError> {
        let row = sqlx::query(
            r"
            SELECT 1 FROM instance_sync_receipts
            WHERE origin_instance_id = ? AND collection_json = ? AND entity_type = ?
              AND entity_key = ? AND payload_sha256 = ?
            ",
        )
        .bind(origin_instance_id.to_string())
        .bind(to_json(&collection)?)
        .bind(entity_type)
        .bind(entity_key)
        .bind(payload_sha256)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    /// Records a successfully applied or conflict-suppressed source payload.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the write.
    pub async fn insert_receipt(
        &self,
        origin_instance_id: uuid::Uuid,
        collection: InstanceSyncCollection,
        entity_type: &str,
        entity_key: &str,
        payload_sha256: &str,
        received_at: OffsetDateTime,
    ) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT OR IGNORE INTO instance_sync_receipts (
                origin_instance_id, collection_json, entity_type, entity_key, payload_sha256, received_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(origin_instance_id.to_string())
        .bind(to_json(&collection)?)
        .bind(entity_type)
        .bind(entity_key)
        .bind(payload_sha256)
        .bind(received_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Saves the local identity selected for a remote entity.
    ///
    /// # Errors
    ///
    /// Returns an error if the mapping cannot be persisted.
    pub async fn upsert_entity_mapping(
        &self,
        origin_instance_id: uuid::Uuid,
        entity_type: &str,
        remote_entity_key: &str,
        local_entity_key: &str,
        now: OffsetDateTime,
    ) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO instance_sync_entity_mappings (
                origin_instance_id, entity_type, remote_entity_key, local_entity_key, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(origin_instance_id, entity_type, remote_entity_key) DO UPDATE SET
                local_entity_key = excluded.local_entity_key,
                updated_at = excluded.updated_at
            ",
        )
        .bind(origin_instance_id.to_string())
        .bind(entity_type)
        .bind(remote_entity_key)
        .bind(local_entity_key)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Resolves one previously imported remote entity to its local identity.
    ///
    /// # Errors
    ///
    /// Returns an error if querying fails.
    pub async fn get_entity_mapping(
        &self,
        origin_instance_id: uuid::Uuid,
        entity_type: &str,
        remote_entity_key: &str,
    ) -> Result<Option<String>, DbError> {
        let row = sqlx::query(
            r"
            SELECT local_entity_key FROM instance_sync_entity_mappings
            WHERE origin_instance_id = ? AND entity_type = ? AND remote_entity_key = ?
            ",
        )
        .bind(origin_instance_id.to_string())
        .bind(entity_type)
        .bind(remote_entity_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(|value| value.try_get("local_entity_key"))
            .transpose()?)
    }

    /// Lists visible conflicts, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or deserialization fails.
    pub async fn list_conflicts(&self, limit: u32) -> Result<Vec<InstanceSyncConflict>, DbError> {
        let rows =
            sqlx::query("SELECT * FROM instance_sync_conflicts ORDER BY created_at DESC LIMIT ?")
                .bind(u32_to_i64(limit))
                .fetch_all(&self.pool)
                .await?;
        rows.iter().map(row_to_instance_sync_conflict).collect()
    }

    /// Persists a visible conflict record.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the database rejects the write.
    pub async fn insert_conflict(&self, conflict: &InstanceSyncConflict) -> Result<(), DbError> {
        sqlx::query(
            r"
            INSERT INTO instance_sync_conflicts (
                id, origin_instance_id, collection_json, entity_type, entity_key, local_updated_at,
                remote_updated_at, local_payload_sha256, remote_payload_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(conflict.id.to_string())
        .bind(conflict.origin_instance_id.to_string())
        .bind(to_json(&conflict.collection)?)
        .bind(&conflict.entity_type)
        .bind(&conflict.entity_key)
        .bind(conflict.local_updated_at)
        .bind(conflict.remote_updated_at)
        .bind(&conflict.local_payload_sha256)
        .bind(&conflict.remote_payload_sha256)
        .bind(conflict.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

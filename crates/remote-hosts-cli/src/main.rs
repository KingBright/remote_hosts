//! CLI entrypoint for remote-hosts.

use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use remote_hosts_connector::{
    ConnectorDaemon, ConnectorDaemonConfig, ConnectorOperationWorker,
    ConnectorOperationWorkerConfig, ConnectorPtyManager, ConnectorPtyManagerConfig,
    FileOutputArtifactStore, HostKeyPolicy, IdleTransportReaper, InteractiveFileTransferBackend,
    QueuedPtyInputPump, RusshPtyBackendFactory, RusshTransportPool, RusshTransportProvider,
    SshCredentialProvider, VaultSshCredentialProvider,
};
#[cfg(unix)]
use remote_hosts_connector::{
    OpenSshManagedPtyBackendMode, OpenSshPtyBackendFactory, OpenSshTransportPool,
    OpenSshTransportProvider,
};
use remote_hosts_core::ServerProtectionPolicy;
use remote_hosts_domain::{
    Connector, ConnectorId, EntityState, Environment, EnvironmentId, EnvironmentKind, TrustLevel,
};
use secrecy::SecretString;
use tokio::sync::watch;

/// Remote hosts management CLI.
#[derive(Debug, Parser)]
#[command(name = "remote-hosts")]
#[command(about = "Remote SSH host knowledge, state, and execution center")]
struct Cli {
    /// Command to run.
    #[command(subcommand)]
    command: Command,
}

/// CLI commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Run a local dependency and configuration doctor.
    Doctor,
    /// Run database migrations.
    Migrate {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
    },
    /// Compress legacy PTY output and reclaim redundant `SQLite` storage.
    OptimizeStorage {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// Maximum legacy chunks moved by one short transaction.
        #[arg(long, default_value_t = 2048)]
        batch_chunks: u32,
        /// Logical UTF-8 bytes targeted per compressed history segment.
        #[arg(long, default_value_t = 1024 * 1024)]
        segment_target_bytes: u64,
        /// Rebuild `SQLite` after compaction so reusable pages leave the physical file.
        #[arg(long)]
        vacuum: bool,
        /// Confirm every MCP child has reloaded, migrate legacy rows, and enable compressed writes.
        #[arg(long)]
        activate_compressed_writes: bool,
        /// Allow a requested vacuum even while conversation work is active.
        #[arg(long, requires = "vacuum")]
        force: bool,
    },
    /// Check whether local services can restart without interrupting active work.
    RestartReadiness {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
    },
    /// Idempotently register the local environment and connector.
    BootstrapConnector {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// Stable connector id as UUID.
        #[arg(long)]
        connector_id: String,
        /// Human-facing connector name.
        #[arg(long)]
        connector_name: String,
        /// Stable environment id as UUID.
        #[arg(long)]
        environment_id: String,
        /// Human-facing environment name.
        #[arg(long)]
        environment_name: String,
        /// Environment category.
        #[arg(long, value_enum, default_value_t = EnvironmentKindArg::HomeLan)]
        environment_kind: EnvironmentKindArg,
        /// Environment trust level.
        #[arg(long, value_enum, default_value_t = TrustLevelArg::Owned)]
        trust_level: TrustLevelArg,
        /// Optional environment description.
        #[arg(long)]
        environment_description: Option<String>,
        /// Optional environment notes.
        #[arg(long)]
        environment_notes: Option<String>,
        /// Current network label reported by the connector.
        #[arg(long, default_value = "local")]
        current_network: String,
        /// Connector version stored before the first heartbeat.
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        version: String,
    },
    /// Serve the MCP server over stdio for local agents.
    McpStdio {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// MCP tool visibility profile.
        #[arg(long, value_enum, default_value_t = McpToolProfile::Agent)]
        tool_profile: McpToolProfile,
        /// Local vault master password file used to encrypt agent-supplied credentials.
        #[arg(long, env = "REMOTE_HOSTS_VAULT_MASTER_PASSWORD_FILE")]
        vault_master_password_file: Option<PathBuf>,
        /// Directory containing connector-written redacted output artifacts.
        #[arg(
            long,
            env = "REMOTE_HOSTS_ARTIFACT_ROOT",
            default_value = "remote-hosts-artifacts"
        )]
        artifact_root: PathBuf,
        /// Agent client family, for example codex or antigravity.
        #[arg(long, env = "REMOTE_HOSTS_AGENT_CLIENT_KIND", default_value = "mcp")]
        agent_client_kind: String,
        /// Optional stable client-instance key. A unique id is generated when omitted.
        #[arg(long, env = "REMOTE_HOSTS_AGENT_CLIENT_INSTANCE_ID")]
        agent_client_instance_id: Option<String>,
        /// Optional project-level isolation key.
        #[arg(long, env = "REMOTE_HOSTS_AGENT_PROJECT_KEY")]
        agent_project_key: Option<String>,
        /// Optional conversation-level isolation key.
        #[arg(long, env = "REMOTE_HOSTS_AGENT_CONVERSATION_KEY")]
        agent_conversation_key: Option<String>,
    },
    /// Serve the HTTP API.
    Serve {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// Bind address.
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
        /// Local vault master password file required for credential writes in the admin UI.
        #[arg(long, env = "REMOTE_HOSTS_VAULT_MASTER_PASSWORD_FILE")]
        vault_master_password_file: Option<PathBuf>,
    },
    /// Claim and execute one queued operation for a connector.
    WorkerOnce {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// Connector id as UUID.
        #[arg(long)]
        connector_id: String,
        /// OpenSSH host key policy: strict, add, or accept.
        #[arg(long, default_value = "add")]
        host_key_policy: String,
        /// OpenSSH connect timeout in seconds.
        #[arg(long, default_value_t = 10)]
        connect_timeout_seconds: u64,
        /// SSH transport backend for queued operations: openssh or russh.
        #[arg(long, default_value = "russh")]
        ssh_backend: String,
        /// Vault master password file required by the russh backend.
        #[arg(long, env = "REMOTE_HOSTS_VAULT_MASTER_PASSWORD_FILE")]
        vault_master_password_file: Option<PathBuf>,
        /// `known_hosts` file used by the russh backend. Defaults to `~/.ssh/known_hosts`.
        #[arg(long)]
        known_hosts_path: Option<PathBuf>,
        /// Inactivity timeout in seconds for native russh sessions.
        #[arg(long, default_value_t = 30)]
        russh_inactivity_timeout_seconds: u64,
        /// Claim lease in seconds.
        #[arg(long, default_value_t = 300)]
        lease_seconds: u64,
        /// Maximum attempts per operation.
        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
        /// Directory for file-backed output artifacts.
        #[arg(long, default_value = "remote-hosts-artifacts")]
        artifact_root: PathBuf,
        /// Output size threshold above which content is stored as a file artifact.
        #[arg(long, default_value_t = 64 * 1024)]
        artifact_threshold_bytes: usize,
        /// Preview bytes kept in chunk summaries and artifact metadata.
        #[arg(long, default_value_t = 4 * 1024)]
        artifact_preview_bytes: usize,
    },
    /// Run a long-lived connector daemon until Ctrl-C.
    WorkerDaemon {
        /// Database URL, for example `<sqlite://remote-hosts.db>`.
        #[arg(long, env = "REMOTE_HOSTS_DATABASE_URL")]
        database_url: String,
        /// Connector id as UUID.
        #[arg(long)]
        connector_id: String,
        /// Connector version to report in heartbeats.
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        version: String,
        /// Current network label to report in heartbeats.
        #[arg(long)]
        current_network: Option<String>,
        /// OpenSSH host key policy: strict, add, or accept.
        #[arg(long, default_value = "add")]
        host_key_policy: String,
        /// OpenSSH connect timeout in seconds.
        #[arg(long, default_value_t = 10)]
        connect_timeout_seconds: u64,
        /// SSH transport backend for queued operations: openssh or russh.
        #[arg(long, default_value = "russh")]
        ssh_backend: String,
        /// Vault master password file required by the russh backend.
        #[arg(long, env = "REMOTE_HOSTS_VAULT_MASTER_PASSWORD_FILE")]
        vault_master_password_file: Option<PathBuf>,
        /// `known_hosts` file used by the russh backend. Defaults to `~/.ssh/known_hosts`.
        #[arg(long)]
        known_hosts_path: Option<PathBuf>,
        /// Inactivity timeout in seconds for native russh sessions.
        #[arg(long, default_value_t = 30)]
        russh_inactivity_timeout_seconds: u64,
        /// Claim lease in seconds.
        #[arg(long, default_value_t = 300)]
        lease_seconds: u64,
        /// Maximum attempts per operation.
        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
        /// Maximum queued operations executed concurrently.
        #[arg(
            long,
            env = "REMOTE_HOSTS_MAX_CONCURRENT_OPERATIONS",
            default_value_t = 16
        )]
        max_concurrent_operations: usize,
        /// Claim lease in seconds for queued PTY input events.
        #[arg(long, default_value_t = 30)]
        pty_input_lease_seconds: u64,
        /// Maximum attempts per queued PTY input event.
        #[arg(long, default_value_t = 3)]
        pty_input_max_attempts: u32,
        /// Close an inactive PTY after this many seconds. Zero disables idle PTY reaping.
        #[arg(
            long,
            env = "REMOTE_HOSTS_PTY_IDLE_TTL_SECONDS",
            default_value_t = 3_600
        )]
        pty_idle_ttl_seconds: u64,
        /// Maximum silence for a PTY declaring a foreground process. Zero disables this class.
        #[arg(
            long,
            env = "REMOTE_HOSTS_PTY_BUSY_TTL_SECONDS",
            default_value_t = 86_400
        )]
        pty_busy_ttl_seconds: u64,
        /// PTY backend mode: auto, control-master-tty, pipe-shell, or russh-native-pty.
        #[arg(long, default_value = "auto")]
        pty_backend_mode: String,
        /// Heartbeat interval in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        heartbeat_interval_ms: u64,
        /// Minimum idle poll delay in milliseconds.
        #[arg(long, default_value_t = 250)]
        idle_min_delay_ms: u64,
        /// Maximum idle poll delay in milliseconds.
        #[arg(long, default_value_t = 5_000)]
        idle_max_delay_ms: u64,
        /// Backoff after infrastructure errors in milliseconds.
        #[arg(long, default_value_t = 2_000)]
        error_backoff_ms: u64,
        /// Directory for file-backed output artifacts.
        #[arg(long, default_value = "remote-hosts-artifacts")]
        artifact_root: PathBuf,
        /// Output size threshold above which content is stored as a file artifact.
        #[arg(long, default_value_t = 64 * 1024)]
        artifact_threshold_bytes: usize,
        /// Preview bytes kept in chunk summaries and artifact metadata.
        #[arg(long, default_value_t = 4 * 1024)]
        artifact_preview_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum EnvironmentKindArg {
    HomeLan,
    CompanyLan,
    CustomerSite,
    PublicInternet,
    Vpn,
    Frp,
}

impl From<EnvironmentKindArg> for EnvironmentKind {
    fn from(value: EnvironmentKindArg) -> Self {
        match value {
            EnvironmentKindArg::HomeLan => Self::HomeLan,
            EnvironmentKindArg::CompanyLan => Self::CompanyLan,
            EnvironmentKindArg::CustomerSite => Self::CustomerSite,
            EnvironmentKindArg::PublicInternet => Self::PublicInternet,
            EnvironmentKindArg::Vpn => Self::Vpn,
            EnvironmentKindArg::Frp => Self::Frp,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TrustLevelArg {
    Owned,
    Trusted,
    External,
    Untrusted,
}

impl From<TrustLevelArg> for TrustLevel {
    fn from(value: TrustLevelArg) -> Self {
        match value {
            TrustLevelArg::Owned => Self::Owned,
            TrustLevelArg::Trusted => Self::Trusted,
            TrustLevelArg::External => Self::External,
            TrustLevelArg::Untrusted => Self::Untrusted,
        }
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "remote_hosts=info,remote_hosts_cli=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Doctor => {
            println!("remote-hosts doctor: ok");
            println!("rust target: {}", std::env::consts::OS);
        }
        Command::Migrate { database_url } => {
            migrate_database(&database_url).await?;
            println!("migrations applied");
        }
        Command::OptimizeStorage {
            database_url,
            batch_chunks,
            segment_target_bytes,
            vacuum,
            activate_compressed_writes,
            force,
        } => {
            let report = optimize_storage(
                &database_url,
                batch_chunks,
                segment_target_bytes,
                vacuum,
                activate_compressed_writes,
                force,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::RestartReadiness { database_url } => {
            let repositories = connect_repositories(&database_url).await?;
            let summary = repositories
                .active_work_summary()
                .await
                .context("check active work before restart")?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.is_idle() {
                anyhow::bail!("active conversation work must drain before restart");
            }
        }
        Command::BootstrapConnector {
            database_url,
            connector_id,
            connector_name,
            environment_id,
            environment_name,
            environment_kind,
            trust_level,
            environment_description,
            environment_notes,
            current_network,
            version,
        } => {
            let result = bootstrap_connector(BootstrapConnectorArgs {
                database_url,
                connector_id,
                connector_name,
                environment_id,
                environment_name,
                environment_kind,
                trust_level,
                environment_description,
                environment_notes,
                current_network,
                version,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::McpStdio {
            database_url,
            tool_profile,
            vault_master_password_file,
            artifact_root,
            agent_client_kind,
            agent_client_instance_id,
            agent_project_key,
            agent_conversation_key,
        } => {
            let repositories = connect_repositories(&database_url).await?;
            let vault_master_password =
                read_optional_vault_master_password(vault_master_password_file.as_ref())?;
            remote_hosts_mcp::serve_stdio_with_profile_vault_artifact_root_and_agent_context(
                repositories,
                tool_profile.into(),
                vault_master_password,
                artifact_root,
                remote_hosts_mcp::AgentSessionContext {
                    client_kind: Some(agent_client_kind),
                    client_instance_id: agent_client_instance_id,
                    project_key: agent_project_key,
                    conversation_key: agent_conversation_key,
                },
            )
            .await
            .map_err(anyhow::Error::msg)?;
        }
        Command::Serve {
            database_url,
            bind,
            vault_master_password_file,
        } => {
            let repositories = connect_repositories(&database_url).await?;
            let vault_master_password =
                read_optional_vault_master_password(vault_master_password_file.as_ref())?;
            ensure_safe_api_bind(bind, vault_master_password.is_some())?;
            let api_state = match vault_master_password {
                Some(master_password) => remote_hosts_api::ApiState::with_vault_master_password(
                    repositories,
                    master_password,
                ),
                None => remote_hosts_api::ApiState::new(repositories),
            };
            tracing::info!(%bind, "starting remote-hosts api");
            remote_hosts_api::serve(bind, api_state)
                .await
                .context("serve api")?;
        }
        Command::WorkerOnce {
            database_url,
            connector_id,
            host_key_policy,
            connect_timeout_seconds,
            ssh_backend,
            vault_master_password_file,
            known_hosts_path,
            russh_inactivity_timeout_seconds,
            lease_seconds,
            max_attempts,
            artifact_root,
            artifact_threshold_bytes,
            artifact_preview_bytes,
        } => {
            run_worker_once(WorkerOnceArgs {
                database_url,
                connector_id,
                host_key_policy,
                connect_timeout_seconds,
                ssh_backend,
                vault_master_password_file,
                known_hosts_path,
                russh_inactivity_timeout_seconds,
                lease_seconds,
                max_attempts,
                artifact_root,
                artifact_threshold_bytes,
                artifact_preview_bytes,
            })
            .await?;
        }
        Command::WorkerDaemon {
            database_url,
            connector_id,
            version,
            current_network,
            host_key_policy,
            connect_timeout_seconds,
            ssh_backend,
            vault_master_password_file,
            known_hosts_path,
            russh_inactivity_timeout_seconds,
            lease_seconds,
            max_attempts,
            max_concurrent_operations,
            pty_input_lease_seconds,
            pty_input_max_attempts,
            pty_idle_ttl_seconds,
            pty_busy_ttl_seconds,
            pty_backend_mode,
            heartbeat_interval_ms,
            idle_min_delay_ms,
            idle_max_delay_ms,
            error_backoff_ms,
            artifact_root,
            artifact_threshold_bytes,
            artifact_preview_bytes,
        } => {
            run_worker_daemon(WorkerDaemonArgs {
                database_url,
                connector_id,
                version,
                current_network,
                host_key_policy,
                connect_timeout_seconds,
                ssh_backend,
                vault_master_password_file,
                known_hosts_path,
                russh_inactivity_timeout_seconds,
                lease_seconds,
                max_attempts,
                max_concurrent_operations,
                pty_input_lease_seconds,
                pty_input_max_attempts,
                pty_idle_ttl_seconds,
                pty_busy_ttl_seconds,
                pty_backend_mode,
                heartbeat_interval_ms,
                idle_min_delay_ms,
                idle_max_delay_ms,
                error_backoff_ms,
                artifact_root,
                artifact_threshold_bytes,
                artifact_preview_bytes,
            })
            .await?;
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum McpToolProfile {
    Agent,
    Admin,
    Full,
}

impl From<McpToolProfile> for remote_hosts_mcp::ToolProfile {
    fn from(value: McpToolProfile) -> Self {
        match value {
            McpToolProfile::Agent => Self::Agent,
            McpToolProfile::Admin => Self::Admin,
            McpToolProfile::Full => Self::Full,
        }
    }
}

async fn migrate_database(database_url: &str) -> anyhow::Result<()> {
    let pool = remote_hosts_db::connect_sqlite(database_url)
        .await
        .with_context(|| format!("connect database {database_url}"))?;
    remote_hosts_db::migrate(&pool)
        .await
        .context("run migrations")
}

async fn connect_repositories(database_url: &str) -> anyhow::Result<remote_hosts_db::Repositories> {
    let pool = remote_hosts_db::connect_sqlite(database_url)
        .await
        .with_context(|| format!("connect database {database_url}"))?;
    remote_hosts_db::migrate(&pool)
        .await
        .context("run migrations")?;
    Ok(remote_hosts_db::Repositories::new(pool))
}

async fn optimize_storage(
    database_url: &str,
    batch_chunks: u32,
    segment_target_bytes: u64,
    vacuum: bool,
    activate_compressed_writes: bool,
    force: bool,
) -> anyhow::Result<serde_json::Value> {
    let repositories = connect_repositories(database_url).await?;
    let compressed_writes_before = repositories.compressed_output_writes_enabled().await?;
    if !compressed_writes_before && !activate_compressed_writes {
        anyhow::bail!(
            "compressed output storage is not active; reload every MCP child, then rerun with --activate-compressed-writes"
        );
    }
    if (vacuum || !compressed_writes_before) && !force {
        require_idle_for_storage_transition(
            &repositories,
            "refusing to migrate or vacuum output while conversation work is active; wait for drain",
        )
        .await?;
    }
    let before_output = repositories.pty_output_chunks.storage_stats().await?;
    let before_operation_output = repositories.operation_output_chunks.storage_stats().await?;
    let before_sqlite = repositories.sqlite_storage_stats().await?;
    let (pty_batches, compacted) =
        compact_pty_output(&repositories, batch_chunks, segment_target_bytes).await?;
    let (operation_batches, operation_compacted) =
        compact_operation_output(&repositories, batch_chunks, segment_target_bytes).await?;
    let migrated_output = repositories.pty_output_chunks.storage_stats().await?;
    let migrated_operation_output = repositories.operation_output_chunks.storage_stats().await?;
    if migrated_output.legacy_chunks != 0 || migrated_operation_output.legacy_chunks != 0 {
        anyhow::bail!(
            "output migration did not drain all legacy rows; compressed writes remain unchanged"
        );
    }
    if !compressed_writes_before && !force {
        require_idle_for_storage_transition(
            &repositories,
            "conversation work started during output migration; compressed writes remain disabled",
        )
        .await?;
    }
    let vacuumed = if vacuum && !force {
        repositories
            .active_work_summary()
            .await
            .context("recheck active work before vacuum")?
            .is_idle()
    } else {
        vacuum
    };
    repositories.optimize_sqlite(vacuumed).await?;
    if !compressed_writes_before {
        repositories.activate_compressed_output_writes().await?;
    }
    let compressed_writes_after = repositories.compressed_output_writes_enabled().await?;
    let after_output = repositories.pty_output_chunks.storage_stats().await?;
    let after_operation_output = repositories.operation_output_chunks.storage_stats().await?;
    let after_sqlite = repositories.sqlite_storage_stats().await?;
    let total_original_bytes = compacted
        .original_storage_bytes
        .saturating_add(operation_compacted.original_storage_bytes);
    let total_compressed_bytes = compacted
        .compressed_bytes
        .saturating_add(operation_compacted.compressed_bytes);
    let payload_ratio_basis_points = if total_original_bytes == 0 {
        10_000
    } else {
        total_compressed_bytes
            .saturating_mul(10_000)
            .checked_div(total_original_bytes)
            .unwrap_or(10_000)
    };
    Ok(serde_json::json!({
        "batches": {
            "pty": pty_batches,
            "operation": operation_batches,
        },
        "vacuum_requested": vacuum,
        "vacuumed": vacuumed,
        "vacuum_deferred_for_active_work": vacuum && !vacuumed,
        "compressed_writes_before": compressed_writes_before,
        "compressed_writes_after": compressed_writes_after,
        "compaction": {
            "pty": compacted,
            "operation": operation_compacted,
        },
        "compressed_payload_ratio_basis_points": payload_ratio_basis_points,
        "before": {
            "pty_output": before_output,
            "operation_output": before_operation_output,
            "sqlite": before_sqlite,
        },
        "after": {
            "pty_output": after_output,
            "operation_output": after_operation_output,
            "sqlite": after_sqlite,
        }
    }))
}

async fn require_idle_for_storage_transition(
    repositories: &remote_hosts_db::Repositories,
    failure_message: &str,
) -> anyhow::Result<()> {
    let active = repositories
        .active_work_summary()
        .await
        .context("check active work before storage transition")?;
    if !active.is_idle() {
        anyhow::bail!(failure_message.to_owned());
    }
    Ok(())
}

async fn compact_pty_output(
    repositories: &remote_hosts_db::Repositories,
    batch_chunks: u32,
    segment_target_bytes: u64,
) -> anyhow::Result<(u64, remote_hosts_db::PtyOutputCompactionBatch)> {
    let mut total = remote_hosts_db::PtyOutputCompactionBatch::default();
    let mut batches = 0_u64;
    loop {
        let batch = repositories
            .pty_output_chunks
            .compact_legacy_batch(batch_chunks, segment_target_bytes)
            .await?;
        if batch.legacy_chunks == 0 {
            break;
        }
        batches = batches.saturating_add(1);
        add_compaction_batch(&mut total, &batch);
        if batches.is_multiple_of(100) {
            tracing::info!(
                batches,
                legacy_chunks = total.legacy_chunks,
                compressed_bytes = total.compressed_bytes,
                "PTY output compaction progress"
            );
        }
        tokio::task::yield_now().await;
    }
    Ok((batches, total))
}

async fn compact_operation_output(
    repositories: &remote_hosts_db::Repositories,
    batch_chunks: u32,
    segment_target_bytes: u64,
) -> anyhow::Result<(u64, remote_hosts_db::OperationOutputCompactionBatch)> {
    let mut total = remote_hosts_db::OperationOutputCompactionBatch::default();
    let mut batches = 0_u64;
    loop {
        let batch = repositories
            .operation_output_chunks
            .compact_legacy_batch(batch_chunks, segment_target_bytes)
            .await?;
        if batch.legacy_chunks == 0 {
            break;
        }
        batches = batches.saturating_add(1);
        add_compaction_batch(&mut total, &batch);
        tokio::task::yield_now().await;
    }
    Ok((batches, total))
}

fn add_compaction_batch(
    total: &mut remote_hosts_db::PtyOutputCompactionBatch,
    batch: &remote_hosts_db::PtyOutputCompactionBatch,
) {
    total.legacy_chunks = total.legacy_chunks.saturating_add(batch.legacy_chunks);
    total.segments_written = total
        .segments_written
        .saturating_add(batch.segments_written);
    total.original_storage_bytes = total
        .original_storage_bytes
        .saturating_add(batch.original_storage_bytes);
    total.encoded_bytes = total.encoded_bytes.saturating_add(batch.encoded_bytes);
    total.compressed_bytes = total
        .compressed_bytes
        .saturating_add(batch.compressed_bytes);
}

#[derive(Clone)]
struct BootstrapConnectorArgs {
    database_url: String,
    connector_id: String,
    connector_name: String,
    environment_id: String,
    environment_name: String,
    environment_kind: EnvironmentKindArg,
    trust_level: TrustLevelArg,
    environment_description: Option<String>,
    environment_notes: Option<String>,
    current_network: String,
    version: String,
}

async fn bootstrap_connector(args: BootstrapConnectorArgs) -> anyhow::Result<serde_json::Value> {
    let repositories = connect_repositories(&args.database_url).await?;
    upsert_bootstrap_connector(&repositories, args).await
}

async fn upsert_bootstrap_connector(
    repositories: &remote_hosts_db::Repositories,
    args: BootstrapConnectorArgs,
) -> anyhow::Result<serde_json::Value> {
    let environment_id =
        EnvironmentId::from_str(&args.environment_id).context("parse local environment id")?;
    let connector_id =
        ConnectorId::from_str(&args.connector_id).context("parse local connector id")?;
    let environment = Environment {
        id: environment_id,
        name: args.environment_name,
        kind: args.environment_kind.into(),
        description: args.environment_description,
        trust_level: args.trust_level.into(),
        notes: args.environment_notes,
    };
    repositories
        .environments
        .upsert(&environment)
        .await
        .context("upsert local environment")?;

    let existing = repositories
        .connectors
        .get(connector_id)
        .await
        .context("load existing local connector")?;
    let connector = Connector {
        id: connector_id,
        name: args.connector_name,
        environment_id,
        host_id: existing.as_ref().and_then(|value| value.host_id),
        version: args.version,
        state: existing
            .as_ref()
            .map_or(EntityState::NotConfigured, |value| value.state.clone()),
        last_seen_at: existing.as_ref().and_then(|value| value.last_seen_at),
        current_network: Some(args.current_network),
    };
    repositories
        .connectors
        .upsert(&connector)
        .await
        .context("upsert local connector")?;

    Ok(serde_json::json!({
        "environment": environment,
        "connector": connector,
    }))
}

struct WorkerOnceArgs {
    database_url: String,
    connector_id: String,
    host_key_policy: String,
    connect_timeout_seconds: u64,
    ssh_backend: String,
    vault_master_password_file: Option<PathBuf>,
    known_hosts_path: Option<PathBuf>,
    russh_inactivity_timeout_seconds: u64,
    lease_seconds: u64,
    max_attempts: u32,
    artifact_root: PathBuf,
    artifact_threshold_bytes: usize,
    artifact_preview_bytes: usize,
}

async fn run_worker_once(args: WorkerOnceArgs) -> anyhow::Result<()> {
    let repositories = connect_repositories(&args.database_url).await?;
    let connector_id = ConnectorId::from_str(&args.connector_id).context("parse connector id")?;
    let host_key_policy = parse_host_key_policy(&args.host_key_policy)?;
    let worker_config = ConnectorOperationWorkerConfig {
        connector_id,
        lease_seconds: args.lease_seconds,
        max_attempts: args.max_attempts,
        artifact_threshold_bytes: args.artifact_threshold_bytes,
        artifact_preview_bytes: args.artifact_preview_bytes,
    };
    let artifact_store = Arc::new(FileOutputArtifactStore::new(args.artifact_root));
    let outcome = match parse_ssh_backend(&args.ssh_backend)? {
        #[cfg(unix)]
        SshBackend::OpenSsh => {
            let provider = OpenSshTransportProvider::new(
                repositories.clone(),
                host_key_policy,
                args.connect_timeout_seconds,
                ServerProtectionPolicy::default(),
            );
            ConnectorOperationWorker::with_artifact_store(
                repositories,
                provider,
                worker_config,
                artifact_store,
            )
            .run_once()
            .await
        }
        SshBackend::Russh => {
            let credentials = Arc::new(VaultSshCredentialProvider::new(
                repositories.clone(),
                read_required_vault_master_password(args.vault_master_password_file.as_ref())?,
            ));
            let provider = RusshTransportProvider::new(
                repositories.clone(),
                credentials,
                host_key_policy,
                args.known_hosts_path,
                args.connect_timeout_seconds,
                args.russh_inactivity_timeout_seconds,
                ServerProtectionPolicy::default(),
            );
            ConnectorOperationWorker::with_artifact_store(
                repositories,
                provider,
                worker_config,
                artifact_store,
            )
            .run_once()
            .await
        }
    }
    .context("run connector worker once")?;
    match outcome {
        Some(outcome) => println!("{}", serde_json::to_string_pretty(&outcome)?),
        None => println!("no queued operation"),
    }
    Ok(())
}

async fn run_worker_daemon_with_provider<P>(
    repositories: remote_hosts_db::Repositories,
    provider: P,
    pty_services: ConnectorPtyServices,
    idle_transport_reaper: Option<Arc<dyn IdleTransportReaper>>,
    args: WorkerDaemonArgs,
    connector_id: ConnectorId,
) -> anyhow::Result<()>
where
    P: remote_hosts_connector::RemoteTransportProvider + 'static,
{
    let daemon = ConnectorDaemon::with_artifact_store(
        repositories,
        provider,
        ConnectorOperationWorkerConfig {
            connector_id,
            lease_seconds: args.lease_seconds,
            max_attempts: args.max_attempts,
            artifact_threshold_bytes: args.artifact_threshold_bytes,
            artifact_preview_bytes: args.artifact_preview_bytes,
        },
        ConnectorDaemonConfig {
            connector_id,
            version: args.version,
            current_network: args.current_network,
            max_concurrent_operations: args.max_concurrent_operations,
            heartbeat_interval_ms: args.heartbeat_interval_ms,
            idle_min_delay_ms: args.idle_min_delay_ms,
            idle_max_delay_ms: args.idle_max_delay_ms,
            error_backoff_ms: args.error_backoff_ms,
        },
        Arc::new(FileOutputArtifactStore::new(args.artifact_root)),
    );
    let daemon = daemon
        .with_pty_input_pump(pty_services.input_pump)
        .with_pty_idle_policy(args.pty_idle_ttl_seconds, args.pty_busy_ttl_seconds)
        .with_interactive_file_transfer(pty_services.interactive_file_transfer);
    let daemon = if let Some(reaper) = idle_transport_reaper {
        daemon.with_idle_transport_reaper(reaper)
    } else {
        daemon
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for ctrl-c");
        }
        let _ = stop_tx.send(true);
    });
    let report = daemon
        .run_until_stopped(stop_rx)
        .await
        .context("run connector daemon")?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

struct WorkerDaemonArgs {
    database_url: String,
    connector_id: String,
    version: String,
    current_network: Option<String>,
    host_key_policy: String,
    connect_timeout_seconds: u64,
    ssh_backend: String,
    vault_master_password_file: Option<PathBuf>,
    known_hosts_path: Option<PathBuf>,
    russh_inactivity_timeout_seconds: u64,
    lease_seconds: u64,
    max_attempts: u32,
    max_concurrent_operations: usize,
    pty_input_lease_seconds: u64,
    pty_input_max_attempts: u32,
    pty_idle_ttl_seconds: u64,
    pty_busy_ttl_seconds: u64,
    pty_backend_mode: String,
    heartbeat_interval_ms: u64,
    idle_min_delay_ms: u64,
    idle_max_delay_ms: u64,
    error_backoff_ms: u64,
    artifact_root: PathBuf,
    artifact_threshold_bytes: usize,
    artifact_preview_bytes: usize,
}

async fn run_worker_daemon(args: WorkerDaemonArgs) -> anyhow::Result<()> {
    let repositories = connect_repositories(&args.database_url).await?;
    let connector_id = ConnectorId::from_str(&args.connector_id).context("parse connector id")?;
    let host_key_policy = parse_host_key_policy(&args.host_key_policy)?;
    let policy = ServerProtectionPolicy::default();
    let ssh_backend = parse_ssh_backend(&args.ssh_backend)?;
    let pty_backend_mode = parse_pty_backend_mode(&args.pty_backend_mode, ssh_backend)?;
    match ssh_backend {
        #[cfg(unix)]
        SshBackend::OpenSsh => {
            let sudo_credential_provider = read_optional_vault_master_password(
                args.vault_master_password_file.as_ref(),
            )?
            .map(|master_password| {
                Arc::new(VaultSshCredentialProvider::new(
                    repositories.clone(),
                    master_password,
                )) as Arc<dyn SshCredentialProvider>
            });
            let openssh_pool = Arc::new(OpenSshTransportPool::new(
                repositories.clone(),
                host_key_policy,
                args.connect_timeout_seconds,
                policy.max_new_ssh_handshakes_per_10_min,
            ));
            let provider =
                OpenSshTransportProvider::with_pool(Arc::clone(&openssh_pool), policy.clone());
            let pty_input_pump = build_pty_input_pump(
                repositories.clone(),
                connector_id,
                &policy,
                &args,
                pty_backend_mode,
                SharedPtyTransportPool::OpenSsh(openssh_pool),
                sudo_credential_provider,
            )?;
            run_worker_daemon_with_provider(
                repositories,
                provider,
                pty_input_pump,
                None,
                args,
                connector_id,
            )
            .await
        }
        SshBackend::Russh => {
            let credentials = Arc::new(VaultSshCredentialProvider::new(
                repositories.clone(),
                read_required_vault_master_password(args.vault_master_password_file.as_ref())?,
            ));
            let sudo_credential_provider: Arc<dyn SshCredentialProvider> = credentials.clone();
            let russh_pool = Arc::new(RusshTransportPool::new(
                repositories.clone(),
                credentials,
                host_key_policy,
                args.known_hosts_path.clone(),
                args.connect_timeout_seconds,
                args.russh_inactivity_timeout_seconds,
                policy.max_new_ssh_handshakes_per_10_min,
            ));
            let provider =
                RusshTransportProvider::with_pool(Arc::clone(&russh_pool), policy.clone());
            let idle_transport_reaper: Arc<dyn IdleTransportReaper> = russh_pool.clone();
            let pty_input_pump = build_pty_input_pump(
                repositories.clone(),
                connector_id,
                &policy,
                &args,
                pty_backend_mode,
                SharedPtyTransportPool::Russh(russh_pool),
                Some(sudo_credential_provider),
            )?;
            run_worker_daemon_with_provider(
                repositories,
                provider,
                pty_input_pump,
                Some(idle_transport_reaper),
                args,
                connector_id,
            )
            .await
        }
    }
}

enum SharedPtyTransportPool {
    #[cfg(unix)]
    OpenSsh(Arc<OpenSshTransportPool>),
    Russh(Arc<RusshTransportPool<VaultSshCredentialProvider>>),
}

struct ConnectorPtyServices {
    input_pump: Arc<dyn QueuedPtyInputPump>,
    interactive_file_transfer: Arc<dyn InteractiveFileTransferBackend>,
}

#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
fn build_pty_input_pump(
    repositories: remote_hosts_db::Repositories,
    connector_id: ConnectorId,
    policy: &ServerProtectionPolicy,
    args: &WorkerDaemonArgs,
    mode: PtyBackendMode,
    shared_pool: SharedPtyTransportPool,
    sudo_credential_provider: Option<Arc<dyn SshCredentialProvider>>,
) -> anyhow::Result<ConnectorPtyServices> {
    let config = ConnectorPtyManagerConfig {
        connector_id,
        max_input_bytes: policy.max_pty_input_bytes,
        output_limit_bytes: policy.default_output_limit_bytes,
        input_lease_seconds: args.pty_input_lease_seconds,
        input_max_attempts: args.pty_input_max_attempts,
    };
    match (mode, shared_pool) {
        #[cfg(unix)]
        (PtyBackendMode::OpenSsh(mode), SharedPtyTransportPool::OpenSsh(pool)) => {
            let backend =
                OpenSshPtyBackendFactory::with_pool(repositories.clone(), pool).with_mode(mode);
            let manager = ConnectorPtyManager::new(repositories, backend, config);
            let manager = match sudo_credential_provider {
                Some(provider) => manager.with_credential_provider(provider),
                None => manager,
            };
            let manager = Arc::new(manager);
            Ok(ConnectorPtyServices {
                input_pump: manager.clone(),
                interactive_file_transfer: manager,
            })
        }
        (PtyBackendMode::RusshNativePty, SharedPtyTransportPool::Russh(pool)) => {
            let backend = RusshPtyBackendFactory::with_pool(repositories.clone(), pool);
            let manager = ConnectorPtyManager::new(repositories, backend, config);
            let manager = match sudo_credential_provider {
                Some(provider) => manager.with_credential_provider(provider),
                None => manager,
            };
            let manager = Arc::new(manager);
            Ok(ConnectorPtyServices {
                input_pump: manager.clone(),
                interactive_file_transfer: manager,
            })
        }
        #[cfg(unix)]
        _ => anyhow::bail!("PTY backend mode does not match the shared SSH transport pool"),
    }
}

fn parse_host_key_policy(input: &str) -> anyhow::Result<HostKeyPolicy> {
    match input {
        "strict" => Ok(HostKeyPolicy::Strict),
        "add" => Ok(HostKeyPolicy::Add),
        "accept" => Ok(HostKeyPolicy::Accept),
        other => anyhow::bail!("invalid host key policy `{other}`; use strict, add, or accept"),
    }
}

fn parse_pty_backend_mode(input: &str, ssh_backend: SshBackend) -> anyhow::Result<PtyBackendMode> {
    match input {
        "auto" => match ssh_backend {
            #[cfg(unix)]
            SshBackend::OpenSsh => Ok(PtyBackendMode::OpenSsh(
                OpenSshManagedPtyBackendMode::ControlMasterTty,
            )),
            SshBackend::Russh => Ok(PtyBackendMode::RusshNativePty),
        },
        #[cfg(unix)]
        "pipe-shell" => Ok(PtyBackendMode::OpenSsh(
            OpenSshManagedPtyBackendMode::PipeShell,
        )),
        #[cfg(unix)]
        "control-master-tty" => Ok(PtyBackendMode::OpenSsh(
            OpenSshManagedPtyBackendMode::ControlMasterTty,
        )),
        #[cfg(not(unix))]
        "pipe-shell" | "control-master-tty" => anyhow::bail!(
            "OpenSSH PTY modes are unavailable on {}; use auto or russh-native-pty",
            std::env::consts::OS
        ),
        "russh-native-pty" => Ok(PtyBackendMode::RusshNativePty),
        #[cfg(unix)]
        other => anyhow::bail!(
            "invalid pty backend mode `{other}`; use auto, control-master-tty, pipe-shell, or russh-native-pty"
        ),
        #[cfg(not(unix))]
        other => anyhow::bail!("invalid pty backend mode `{other}`; use auto or russh-native-pty"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PtyBackendMode {
    #[cfg(unix)]
    OpenSsh(OpenSshManagedPtyBackendMode),
    RusshNativePty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SshBackend {
    #[cfg(unix)]
    OpenSsh,
    Russh,
}

fn parse_ssh_backend(input: &str) -> anyhow::Result<SshBackend> {
    match input {
        #[cfg(unix)]
        "openssh" => Ok(SshBackend::OpenSsh),
        #[cfg(not(unix))]
        "openssh" => anyhow::bail!(
            "the openssh native-mux backend is unavailable on {}; use russh",
            std::env::consts::OS
        ),
        "russh" => Ok(SshBackend::Russh),
        #[cfg(unix)]
        other => anyhow::bail!("invalid ssh backend `{other}`; use openssh or russh"),
        #[cfg(not(unix))]
        other => anyhow::bail!("invalid ssh backend `{other}`; use russh"),
    }
}

fn read_required_vault_master_password(path: Option<&PathBuf>) -> anyhow::Result<SecretString> {
    let path = path.context("the russh backend requires --vault-master-password-file")?;
    read_vault_master_password(path)
}

fn read_optional_vault_master_password(
    path: Option<&PathBuf>,
) -> anyhow::Result<Option<SecretString>> {
    path.map(read_vault_master_password).transpose()
}

fn ensure_safe_api_bind(bind: SocketAddr, vault_unlocked: bool) -> anyhow::Result<()> {
    if vault_unlocked && !bind.ip().is_loopback() {
        anyhow::bail!(
            "refusing to expose an unlocked credential vault on non-loopback bind {bind}; \
             bind to 127.0.0.1 and use an SSH tunnel"
        );
    }
    Ok(())
}

fn read_vault_master_password(path: &PathBuf) -> anyhow::Result<SecretString> {
    let value = std::fs::read_to_string(path)
        .with_context(|| format!("read vault master password file {}", path.display()))?;
    let value = value.trim_end_matches(['\r', '\n']).to_owned();
    if value.is_empty() {
        anyhow::bail!("vault master password file is empty");
    }
    Ok(SecretString::from(value))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[cfg(unix)]
    use super::OpenSshManagedPtyBackendMode;
    use super::{
        BootstrapConnectorArgs, Cli, Command, EnvironmentKindArg, McpToolProfile, PtyBackendMode,
        SshBackend, TrustLevelArg, connect_repositories, ensure_safe_api_bind,
        parse_pty_backend_mode, upsert_bootstrap_connector,
    };
    use remote_hosts_domain::{ConnectorId, EntityState, EnvironmentId};
    use std::str::FromStr;

    #[test]
    fn mcp_stdio_defaults_to_agent_profile_and_accepts_full() -> anyhow::Result<()> {
        let default = Cli::try_parse_from([
            "remote-hosts",
            "mcp-stdio",
            "--database-url",
            "sqlite::memory:",
        ])?;
        let Command::McpStdio { tool_profile, .. } = default.command else {
            anyhow::bail!("expected mcp-stdio command");
        };
        assert_eq!(tool_profile, McpToolProfile::Agent);

        let full = Cli::try_parse_from([
            "remote-hosts",
            "mcp-stdio",
            "--database-url",
            "sqlite::memory:",
            "--tool-profile",
            "full",
        ])?;
        let Command::McpStdio { tool_profile, .. } = full.command else {
            anyhow::bail!("expected mcp-stdio command");
        };
        assert_eq!(tool_profile, McpToolProfile::Full);
        Ok(())
    }

    #[test]
    fn optimize_storage_uses_bounded_batches_and_requires_vacuum_for_force() -> anyhow::Result<()> {
        let parsed = Cli::try_parse_from([
            "remote-hosts",
            "optimize-storage",
            "--database-url",
            "sqlite::memory:",
        ])?;
        let Command::OptimizeStorage {
            batch_chunks,
            segment_target_bytes,
            vacuum,
            force,
            activate_compressed_writes,
            ..
        } = parsed.command
        else {
            anyhow::bail!("expected optimize-storage command");
        };
        assert_eq!(batch_chunks, 2048);
        assert_eq!(segment_target_bytes, 1024 * 1024);
        assert!(!vacuum);
        assert!(!force);
        assert!(!activate_compressed_writes);
        let activation = Cli::try_parse_from([
            "remote-hosts",
            "optimize-storage",
            "--database-url",
            "sqlite::memory:",
            "--activate-compressed-writes",
        ])?;
        let Command::OptimizeStorage {
            activate_compressed_writes,
            ..
        } = activation.command
        else {
            anyhow::bail!("expected optimize-storage command");
        };
        assert!(activate_compressed_writes);
        assert!(
            Cli::try_parse_from([
                "remote-hosts",
                "optimize-storage",
                "--database-url",
                "sqlite::memory:",
                "--force",
            ])
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn unlocked_http_vault_requires_loopback_bind() -> anyhow::Result<()> {
        ensure_safe_api_bind("127.0.0.1:8787".parse()?, true)?;
        ensure_safe_api_bind("0.0.0.0:8787".parse()?, false)?;
        assert!(ensure_safe_api_bind("0.0.0.0:8787".parse()?, true).is_err());
        Ok(())
    }

    #[test]
    fn mcp_stdio_accepts_stable_agent_conversation_context() -> anyhow::Result<()> {
        let parsed = Cli::try_parse_from([
            "remote-hosts",
            "mcp-stdio",
            "--database-url",
            "sqlite::memory:",
            "--agent-client-kind",
            "codex",
            "--agent-client-instance-id",
            "desktop-main",
            "--agent-project-key",
            "/workspace/remote-hosts",
            "--agent-conversation-key",
            "conversation-42",
        ])?;
        let Command::McpStdio {
            agent_client_kind,
            agent_client_instance_id,
            agent_project_key,
            agent_conversation_key,
            ..
        } = parsed.command
        else {
            anyhow::bail!("expected mcp-stdio command");
        };
        assert_eq!(agent_client_kind, "codex");
        assert_eq!(agent_client_instance_id.as_deref(), Some("desktop-main"));
        assert_eq!(
            agent_project_key.as_deref(),
            Some("/workspace/remote-hosts")
        );
        assert_eq!(agent_conversation_key.as_deref(), Some("conversation-42"));
        Ok(())
    }

    #[test]
    fn worker_defaults_to_native_russh_backend() -> anyhow::Result<()> {
        let parsed = Cli::try_parse_from([
            "remote-hosts",
            "worker-once",
            "--database-url",
            "sqlite::memory:",
            "--connector-id",
            "019f0000-0000-7000-8000-000000000001",
        ])?;
        let Command::WorkerOnce { ssh_backend, .. } = parsed.command else {
            anyhow::bail!("expected worker-once command");
        };
        assert_eq!(ssh_backend, "russh");
        Ok(())
    }

    #[tokio::test]
    async fn connector_bootstrap_is_idempotent_and_preserves_runtime_state() -> anyhow::Result<()> {
        let repositories = connect_repositories("sqlite::memory:").await?;
        let environment_id = EnvironmentId::new();
        let connector_id = ConnectorId::new();
        let mut args = BootstrapConnectorArgs {
            database_url: "sqlite::memory:".to_owned(),
            connector_id: connector_id.to_string(),
            connector_name: "local-windows-connector".to_owned(),
            environment_id: environment_id.to_string(),
            environment_name: "local-windows".to_owned(),
            environment_kind: EnvironmentKindArg::HomeLan,
            trust_level: TrustLevelArg::Owned,
            environment_description: Some("Windows user service".to_owned()),
            environment_notes: Some("test bootstrap".to_owned()),
            current_network: "local".to_owned(),
            version: "0.1.0".to_owned(),
        };

        upsert_bootstrap_connector(&repositories, args.clone()).await?;
        let mut active = repositories
            .connectors
            .get(ConnectorId::from_str(&args.connector_id)?)
            .await?
            .ok_or_else(|| anyhow::anyhow!("bootstrapped connector is missing"))?;
        active.state = EntityState::Healthy;
        repositories.connectors.upsert(&active).await?;

        args.connector_name = "renamed-windows-connector".to_owned();
        args.current_network = "office".to_owned();
        upsert_bootstrap_connector(&repositories, args).await?;

        let updated = repositories
            .connectors
            .get(connector_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("updated connector is missing"))?;
        assert_eq!(updated.name, "renamed-windows-connector");
        assert_eq!(updated.current_network.as_deref(), Some("office"));
        assert_eq!(updated.state, EntityState::Healthy);
        assert_eq!(updated.environment_id, environment_id);
        Ok(())
    }

    #[tokio::test]
    async fn new_database_is_ready_for_service_restart() -> anyhow::Result<()> {
        let repositories = connect_repositories("sqlite::memory:").await?;
        let summary = repositories.active_work_summary().await?;
        assert!(summary.is_idle());
        assert_eq!(summary.queued_or_running_operations, 0);
        assert_eq!(summary.pending_or_active_ptys, 0);
        assert_eq!(summary.queued_or_claimed_pty_inputs, 0);
        assert_eq!(summary.unexpired_write_leases, 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn auto_pty_backend_tracks_operation_ssh_backend() -> anyhow::Result<()> {
        assert_eq!(
            parse_pty_backend_mode("auto", SshBackend::OpenSsh)?,
            PtyBackendMode::OpenSsh(OpenSshManagedPtyBackendMode::ControlMasterTty)
        );
        assert_eq!(
            parse_pty_backend_mode("auto", SshBackend::Russh)?,
            PtyBackendMode::RusshNativePty
        );
        Ok(())
    }

    #[test]
    fn explicit_russh_native_pty_mode_is_supported() -> anyhow::Result<()> {
        assert_eq!(
            parse_pty_backend_mode("russh-native-pty", SshBackend::Russh)?,
            PtyBackendMode::RusshNativePty
        );
        Ok(())
    }
}

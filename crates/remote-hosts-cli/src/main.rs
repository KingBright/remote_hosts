//! CLI entrypoint for remote-hosts.

use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use remote_hosts_connector::{
    ConnectorDaemon, ConnectorDaemonConfig, ConnectorOperationWorker,
    ConnectorOperationWorkerConfig, ConnectorPtyManager, ConnectorPtyManagerConfig,
    FileOutputArtifactStore, HostKeyPolicy, OpenSshManagedPtyBackendMode, OpenSshPtyBackendFactory,
    OpenSshTransportPool, OpenSshTransportProvider, QueuedPtyInputPump, RusshPtyBackendFactory,
    RusshTransportPool, RusshTransportProvider, VaultSshCredentialProvider,
};
use remote_hosts_core::ServerProtectionPolicy;
use remote_hosts_domain::ConnectorId;
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
        #[arg(long, default_value = "openssh")]
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
        #[arg(long, default_value = "openssh")]
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
        /// Claim lease in seconds for queued PTY input events.
        #[arg(long, default_value_t = 30)]
        pty_input_lease_seconds: u64,
        /// Maximum attempts per queued PTY input event.
        #[arg(long, default_value_t = 3)]
        pty_input_max_attempts: u32,
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
            pty_input_lease_seconds,
            pty_input_max_attempts,
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
                pty_input_lease_seconds,
                pty_input_max_attempts,
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
    pty_input_pump: Arc<dyn QueuedPtyInputPump>,
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
            heartbeat_interval_ms: args.heartbeat_interval_ms,
            idle_min_delay_ms: args.idle_min_delay_ms,
            idle_max_delay_ms: args.idle_max_delay_ms,
            error_backoff_ms: args.error_backoff_ms,
        },
        Arc::new(FileOutputArtifactStore::new(args.artifact_root)),
    );
    let daemon = daemon.with_pty_input_pump(pty_input_pump);
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
    pty_input_lease_seconds: u64,
    pty_input_max_attempts: u32,
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
        SshBackend::OpenSsh => {
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
            )?;
            run_worker_daemon_with_provider(
                repositories,
                provider,
                pty_input_pump,
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
            let pty_input_pump = build_pty_input_pump(
                repositories.clone(),
                connector_id,
                &policy,
                &args,
                pty_backend_mode,
                SharedPtyTransportPool::Russh(russh_pool),
            )?;
            run_worker_daemon_with_provider(
                repositories,
                provider,
                pty_input_pump,
                args,
                connector_id,
            )
            .await
        }
    }
}

enum SharedPtyTransportPool {
    OpenSsh(Arc<OpenSshTransportPool>),
    Russh(Arc<RusshTransportPool<VaultSshCredentialProvider>>),
}

fn build_pty_input_pump(
    repositories: remote_hosts_db::Repositories,
    connector_id: ConnectorId,
    policy: &ServerProtectionPolicy,
    args: &WorkerDaemonArgs,
    mode: PtyBackendMode,
    shared_pool: SharedPtyTransportPool,
) -> anyhow::Result<Arc<dyn QueuedPtyInputPump>> {
    let config = ConnectorPtyManagerConfig {
        connector_id,
        max_input_bytes: policy.max_pty_input_bytes,
        output_limit_bytes: policy.default_output_limit_bytes,
        input_lease_seconds: args.pty_input_lease_seconds,
        input_max_attempts: args.pty_input_max_attempts,
    };
    match (mode, shared_pool) {
        (PtyBackendMode::OpenSsh(mode), SharedPtyTransportPool::OpenSsh(pool)) => {
            let backend =
                OpenSshPtyBackendFactory::with_pool(repositories.clone(), pool).with_mode(mode);
            Ok(Arc::new(ConnectorPtyManager::new(
                repositories,
                backend,
                config,
            )))
        }
        (PtyBackendMode::RusshNativePty, SharedPtyTransportPool::Russh(pool)) => {
            let backend = RusshPtyBackendFactory::with_pool(repositories.clone(), pool);
            Ok(Arc::new(ConnectorPtyManager::new(
                repositories,
                backend,
                config,
            )))
        }
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
            SshBackend::OpenSsh => Ok(PtyBackendMode::OpenSsh(
                OpenSshManagedPtyBackendMode::ControlMasterTty,
            )),
            SshBackend::Russh => Ok(PtyBackendMode::RusshNativePty),
        },
        "pipe-shell" => Ok(PtyBackendMode::OpenSsh(
            OpenSshManagedPtyBackendMode::PipeShell,
        )),
        "control-master-tty" => Ok(PtyBackendMode::OpenSsh(
            OpenSshManagedPtyBackendMode::ControlMasterTty,
        )),
        "russh-native-pty" => Ok(PtyBackendMode::RusshNativePty),
        other => anyhow::bail!(
            "invalid pty backend mode `{other}`; use auto, control-master-tty, pipe-shell, or russh-native-pty"
        ),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PtyBackendMode {
    OpenSsh(OpenSshManagedPtyBackendMode),
    RusshNativePty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SshBackend {
    OpenSsh,
    Russh,
}

fn parse_ssh_backend(input: &str) -> anyhow::Result<SshBackend> {
    match input {
        "openssh" => Ok(SshBackend::OpenSsh),
        "russh" => Ok(SshBackend::Russh),
        other => anyhow::bail!("invalid ssh backend `{other}`; use openssh or russh"),
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

    use super::{
        Cli, Command, McpToolProfile, OpenSshManagedPtyBackendMode, PtyBackendMode, SshBackend,
        ensure_safe_api_bind, parse_pty_backend_mode,
    };

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
            parse_pty_backend_mode("russh-native-pty", SshBackend::OpenSsh)?,
            PtyBackendMode::RusshNativePty
        );
        Ok(())
    }
}

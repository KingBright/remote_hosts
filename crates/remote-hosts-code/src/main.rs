//! Standalone gateway/device service alongside the existing Remote Hosts connector.
#![cfg_attr(windows, windows_subsystem = "windows")]
use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig, hash, random, read_config, write_private,
};
use std::{fs::OpenOptions, net::SocketAddr, path::PathBuf};

#[derive(Parser)]
#[command(
    name = "remote-hosts-code",
    version,
    about = "Remote Hosts personal code gateway and device agent"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Serve OAuth, MCP and device relay behind a TLS reverse proxy.
    Gateway {
        #[arg(long)]
        config: PathBuf,
    },
    /// Connect this device outbound and execute authorized code tools locally.
    Agent {
        #[arg(long)]
        config: PathBuf,
        /// Also write diagnostics to the invoking console. Default Windows service mode has no window.
        #[arg(long)]
        foreground: bool,
    },
    /// Create private gateway configuration and a separate owner login password file.
    InitGateway {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        public_url: String,
        #[arg(long, default_value = "127.0.0.1:18787")]
        bind: String,
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long, default_value = "jinliang")]
        owner: String,
        #[arg(long)]
        password_file: PathBuf,
    },
    /// Add one independently revocable device and write its private agent config.
    Enroll {
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        agent_config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long, required = true)]
        root: Vec<PathBuf>,
        #[arg(long)]
        allow_write: bool,
        #[arg(long)]
        allow_exec: bool,
        #[arg(long, default_value = "/bin/zsh")]
        shell: PathBuf,
    },
    /// Pre-register an OAuth client and save credentials to a new private file.
    /// The exact callback must already be allowed in the gateway configuration.
    RegisterOauthClient {
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long, required = true)]
        redirect_uri: Vec<String>,
        #[arg(long, default_value = "client_secret_basic", value_parser = ["none", "client_secret_basic", "client_secret_post"])]
        auth_method: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Print the static machine-readable tool/protocol contract used by release packaging.
    ReleaseManifest,
    /// Serve the generated tools over stdio with durable request receipts and automatic catalog reports.
    Adapter {
        #[arg(long)]
        config: PathBuf,
    },
    /// Generate or validate the adapter contract from the compiled Gateway catalog.
    AdapterContract {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        check: bool,
    },
    /// Validate configuration without starting services or printing credentials.
    Check {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        agent: bool,
    },
}
fn lock(dir: &std::path::Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(dir)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("service.lock"))?;
    fs2::FileExt::try_lock_exclusive(&file)
        .context("another service is already using this state directory")?;
    Ok(file)
}
#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(windows)]
    {
        let args: Vec<_> = std::env::args_os().skip(1).collect();
        let background = args.first().is_some_and(|v| v == "agent")
            && !args
                .iter()
                .any(|v| v == "--foreground" || v == "--help" || v == "-h");
        if !background {
            remote_hosts_code::agent_log::attach_parent_console();
        }
    }
    let command = Cli::parse().command;
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if let Command::Agent { config, foreground } = &command {
        use tracing_subscriber::fmt::writer::{BoxMakeWriter, MakeWriterExt};
        let c: AgentConfig = read_config(config)?;
        let _lock = lock(&c.state_dir)?;
        let file =
            std::sync::Mutex::new(remote_hosts_code::agent_log::AgentLog::open(&c.state_dir)?);
        let writer = if *foreground || !cfg!(windows) {
            BoxMakeWriter::new(file.and(std::io::stderr))
        } else {
            BoxMakeWriter::new(file)
        };
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(writer)
            .init();
        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            pid = std::process::id(),
            console = (*foreground || !cfg!(windows)),
            "agent starting; bounded local logs enabled"
        );
        let result = async { remote_hosts_code::agent::Agent::new(c).await?.run().await }.await;
        if result.is_err() {
            tracing::error!("agent stopped with error; inspect preceding local diagnostics");
        }
        return result;
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    match command {
        Command::Gateway { config } => {
            let c: GatewayConfig = read_config(&config)?;
            let bind: SocketAddr = c.bind.parse()?;
            ensure!(
                bind.ip().is_loopback(),
                "gateway must bind loopback behind Caddy"
            );
            let _lock = lock(&c.state_dir)?;
            let gateway =
                remote_hosts_code::gateway::Gateway::new_with_config_path(c, config.clone())
                    .await?;
            let listener = tokio::net::TcpListener::bind(bind).await?;
            tracing::info!(%bind,"gateway listening");
            let router = gateway.router()?;
            let serving = async {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = tokio::signal::ctrl_c().await;
                    })
                    .await
            };
            tokio::select! {
                result = serving => result?,
                result = gateway.maintain_history() => result?,
            }
        }
        Command::Agent { .. } => unreachable!("agent handled before service logger initialization"),
        Command::InitGateway {
            config,
            public_url,
            bind,
            state_dir,
            owner,
            password_file,
        } => {
            remote_hosts_code::validate_url(&public_url)?;
            ensure!(
                !config.exists() && !password_file.exists(),
                "refusing to overwrite existing configuration or password"
            );
            use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
            let password = random();
            let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes())?;
            let password_hash = Argon2::default()
                .hash_password(password.as_bytes(), &salt)?
                .to_string();
            let c = GatewayConfig {
                public_url,
                bind,
                state_dir,
                owner,
                password_hash,
                devices: vec![],
                redirect_uris: vec!["https://chatgpt.com/connector_platform_oauth_redirect".into()],
                allowed_origins: remote_hosts_code::default_mcp_client_origins(),
            };
            write_private(&password_file, password.as_bytes())?;
            write_private(&config, &serde_json::to_vec_pretty(&c)?)?;
            println!("Created gateway config and password file (values not displayed).");
        }
        Command::Enroll {
            gateway_config,
            agent_config,
            name,
            state_dir,
            root,
            allow_write,
            allow_exec,
            shell,
        } => {
            ensure!(
                !agent_config.exists(),
                "refusing to overwrite device identity"
            );
            let mut g: GatewayConfig = read_config(&gateway_config)?;
            ensure!(
                !g.devices.iter().any(|d| d.name == name),
                "device name already enrolled"
            );
            let id = uuid::Uuid::new_v4().to_string();
            let token = random();
            let mut scopes = vec!["code:read".into()];
            if allow_write {
                scopes.push("code:write".into())
            }
            if allow_exec {
                scopes.push("terminal:exec".into())
            }
            g.devices.push(DeviceRegistration {
                id: id.clone(),
                name,
                token_hash: hash(&token),
                scopes,
            });
            let a = AgentConfig {
                gateway_url: g.public_url.clone(),
                device_id: id.clone(),
                device_token: token,
                state_dir,
                roots: root,
                allow_write,
                allow_exec,
                shell,
            };
            write_private(&agent_config, &serde_json::to_vec_pretty(&a)?)?;
            write_private(&gateway_config, &serde_json::to_vec_pretty(&g)?)?;
            println!("Enrolled device {id}; reload gateway configuration before connecting.");
        }
        Command::RegisterOauthClient {
            gateway_config,
            name,
            redirect_uri,
            auth_method,
            output,
        } => {
            use std::io::Write;
            ensure!(!output.exists(), "refusing to overwrite OAuth credentials");
            let parent = output
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            std::fs::create_dir_all(parent)?;
            let mut file = tempfile::NamedTempFile::new_in(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.as_file()
                    .set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            let c: GatewayConfig = read_config(&gateway_config)?;
            let gateway = remote_hosts_code::gateway::Gateway::new(c).await?;
            let client = gateway
                .auth
                .register_client(&serde_json::json!({
                    "client_name": name,
                    "redirect_uris": redirect_uri,
                    "token_endpoint_auth_method": auth_method,
                }))
                .await
                .map_err(|(status, error)| {
                    anyhow::anyhow!("OAuth client registration failed: {} {}", status, error.0)
                })?;
            let saved = (|| -> Result<()> {
                file.write_all(&serde_json::to_vec_pretty(&client)?)?;
                file.as_file().sync_all()?;
                file.persist_noclobber(&output)?;
                Ok(())
            })();
            if saved.is_err() {
                // A credential file failure must not leave a usable orphan client.
                if let Some(id) = client.get("client_id").and_then(serde_json::Value::as_str) {
                    gateway
                        .store
                        .take::<serde_json::Value>(
                            &remote_hosts_code::auth::oauth_state_kind("client", id),
                            id,
                        )
                        .await?;
                }
            }
            saved?;
            println!(
                "OAuth client created; credentials saved to the requested private file (values not displayed)."
            );
        }
        Command::Adapter { config } => {
            remote_hosts_code::adapter::serve(&config).await?;
        }
        Command::AdapterContract { output, check } => {
            remote_hosts_code::contract::export(&output, check)?;
            println!(
                "{}",
                serde_json::json!({"state":"passed","checked":check,"path":output})
            );
        }
        Command::ReleaseManifest => {
            println!(
                "{}",
                serde_json::to_string(&remote_hosts_code::release_manifest())?
            );
        }
        Command::Check { config, agent } => {
            if agent {
                let c: AgentConfig = read_config(&config)?;
                remote_hosts_code::validate_url(&c.gateway_url)?;
                uuid::Uuid::parse_str(&c.device_id)?;
                ensure!(!c.roots.is_empty(), "no project roots");
            } else {
                let c: GatewayConfig = read_config(&config)?;
                c.validate_oauth_policy()?;
            }
            println!("Configuration valid; secrets not displayed.");
        }
    }
    Ok(())
}

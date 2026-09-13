//! Standalone gateway/device service alongside the existing Remote Hosts connector.
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
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
            axum::serve(listener, gateway.router()?)
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
        }
        Command::Agent { config } => {
            let c: AgentConfig = read_config(&config)?;
            let _lock = lock(&c.state_dir)?;
            remote_hosts_code::agent::Agent::new(c).await?.run().await?;
        }
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
        Command::Check { config, agent } => {
            if agent {
                let c: AgentConfig = read_config(&config)?;
                remote_hosts_code::validate_url(&c.gateway_url)?;
                uuid::Uuid::parse_str(&c.device_id)?;
                ensure!(!c.roots.is_empty(), "no project roots");
            } else {
                let c: GatewayConfig = read_config(&config)?;
                remote_hosts_code::validate_url(&c.public_url)?;
            }
            println!("Configuration valid; secrets not displayed.");
        }
    }
    Ok(())
}

//! Shared CLI: invoke through either Remote Hosts terminal transport, never sudo from an agent.
#![forbid(unsafe_code)]
#[cfg(unix)]
mod unix_cli {
    use anyhow::Result;
    use clap::{Parser, Subcommand};
    use remote_hosts_admin::{protocol::Request, transport};
    use std::path::PathBuf;
    #[derive(Parser)]
    #[command(
        version,
        about = "Restricted, separately authorized Remote Hosts maintenance"
    )]
    struct Cli {
        #[arg(long,global=true,default_value=transport::default_socket())]
        socket: PathBuf,
        #[command(subcommand)]
        command: Action,
    }
    #[derive(Subcommand)]
    enum Action {
        /// Root-only service entry. Installation never executes cleanup automatically.
        Serve {
            #[arg(long)]
            policy: PathBuf,
            #[arg(long)]
            state_dir: PathBuf,
        },
        Status,
        /// Persist a read-only preflight plan. Reuse the UUID when retrying this request.
        Plan {
            #[arg(long)]
            request_id: String,
        },
        /// Execute exactly a saved plan; rejected if stale or changed.
        Apply {
            #[arg(long)]
            request_id: String,
            #[arg(long)]
            plan_sha256: String,
        },
        Receipt {
            #[arg(long)]
            request_id: String,
        },
        /// Disable the installed grant. Re-enabling requires a separate administrator action.
        Revoke,
    }
    pub async fn run() -> Result<()> {
        let cli = Cli::parse();
        let is_apply = matches!(&cli.command, Action::Apply { .. });
        let req = match cli.command {
            Action::Serve { policy, state_dir } => {
                return transport::serve(&policy, &state_dir, &cli.socket).await;
            }
            Action::Status => Request::Status,
            Action::Plan { request_id } => Request::Plan { request_id },
            Action::Apply {
                request_id,
                plan_sha256,
            } => Request::Apply {
                request_id,
                plan_sha256,
            },
            Action::Receipt { request_id } => Request::Receipt { request_id },
            Action::Revoke => Request::Revoke,
        };
        match transport::request(&cli.socket, &req).await {
            Ok(v) => {
                println!("{}", serde_json::to_string_pretty(&v)?);
                if v["ok"] != true || (is_apply && v["result"]["state"] != "succeeded") {
                    std::process::exit(2);
                }
            }
            Err(e) => {
                println!(
                    "{}",
                    serde_json::json!({"ok":false,"error":format!("{e:#}"),"system_changes_confirmed":false,"next_action":"Check installation or query the original receipt; never replay with a new request_id."})
                );
                std::process::exit(2);
            }
        }
        Ok(())
    }
}
#[cfg(unix)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unix_cli::run().await
}
#[cfg(not(unix))]
fn main() {
    eprintln!(
        "remote-hosts-admin 0.1 supports macOS/Linux only; no Windows privilege changes are made."
    );
    std::process::exit(2);
}

//! Candidate CLI exposes only inspect/prepare/receipt/verify; no execution subcommand.
use crate::{
    engine::unix_time,
    ho5::{self, Action, Backend, Intent},
    ho5_bus::{ReadOnlyBus, SystemReadBus},
};
use anyhow::{Result, ensure};
use clap::Subcommand;
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Read fixed OS metadata; no journal creation or maintenance.
    Inspect,
    /// Persist a local-user plan; does not install or reboot.
    Prepare {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        request_id: String,
        #[arg(long, value_enum)]
        action: Action,
        #[arg(long)]
        target_checksum: Option<String>,
    },
    Receipt {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
    },
    /// Observe and persist reconciliation of the original receipt; never redispatch.
    Verify {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
    },
}
pub async fn run(command: Command) -> Result<Value> {
    let uid = nix::unistd::geteuid().as_raw();
    ensure!(uid != 0, "ordinary_non_root_caller_required");
    let mut bus = ReadOnlyBus(SystemReadBus);
    let record = match command {
        Command::Inspect => {
            return Ok(json!({"protocol":1,"device_id":ho5::DEVICE,
            "device_identity_evidence":"caller_device_binding_not_independently_authenticated",
            "execution_enabled":false,"dns_enabled":false,"observation":bus.inspect().await?}));
        }
        Command::Prepare {
            state_dir,
            task_id,
            request_id,
            action,
            target_checksum,
        } => {
            ho5::coordinator(&state_dir)
                .prepare(
                    &mut bus,
                    Intent {
                        task_id,
                        request_id,
                        action,
                        target_checksum,
                    },
                    unix_time(),
                )
                .await?
        }
        Command::Receipt {
            state_dir,
            request_id,
        } => {
            let value = ho5::coordinator(&state_dir).receipt(&request_id)?;
            return Ok(json!({"execution_enabled":false,"receipt":value}));
        }
        Command::Verify {
            state_dir,
            request_id,
        } => {
            ho5::coordinator(&state_dir)
                .reconcile(&mut bus, &request_id, unix_time())
                .await?
        }
    };
    Ok(
        json!({"execution_enabled":false,"dns_enabled":false,"next_action":record.next_action(),"receipt":record}),
    )
}

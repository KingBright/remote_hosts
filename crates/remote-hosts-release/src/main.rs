//! Native build control and portable verification probes.
#![forbid(unsafe_code)]
use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use remote_hosts_release::{evidence, executor, pipeline, toolchain, updater};
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Parser)]
#[command(
    name = "rh-release",
    version,
    about = "Native release build execution. No deployment or service restart."
)]
struct Cli {
    #[command(subcommand)]
    action: Action,
}
#[derive(Subcommand)]
enum Action {
    Doctor {
        #[arg(long, default_value = "1.94.1")]
        toolchain: String,
    },
    Prepare {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        run_dir: PathBuf,
        #[arg(long, default_value = "remote-hosts-code")]
        package: String,
        #[arg(long, default_value = "1.94.1")]
        toolchain: String,
    },
    Run {
        #[arg(long)]
        run_dir: PathBuf,
    },
    Resume {
        #[arg(long)]
        run_dir: PathBuf,
    },
    Status {
        #[arg(long)]
        run_dir: PathBuf,
    },
    Cancel {
        #[arg(long)]
        run_dir: PathBuf,
    },
    VerifyPackage {
        #[arg(long)]
        package: PathBuf,
    },
    Install {
        #[arg(long)]
        request: PathBuf,
    },
    Probe {
        #[command(subcommand)]
        action: Probe,
    },
}
#[derive(Subcommand)]
enum Probe {
    Echo {
        #[arg(long, default_value = "42")]
        text: String,
    },
    Pattern {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 2097152)]
        bytes: u64,
    },
    Hash {
        #[arg(long)]
        path: PathBuf,
    },
    #[command(hide = true)]
    Sleep {
        #[arg(long)]
        ready: PathBuf,
        #[arg(long)]
        finished: PathBuf,
        #[arg(long, default_value_t = 30000)]
        millis: u64,
    },
    #[command(hide = true)]
    Parent {
        #[arg(long)]
        ready: PathBuf,
        #[arg(long)]
        finished: PathBuf,
    },
}
fn show<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}
async fn action(action: Action, cancel: &executor::Cancel) -> Result<bool> {
    match action {
        Action::Doctor { toolchain } => {
            let temp = tempfile::tempdir()?;
            let result = toolchain::doctor(&toolchain, temp.path()).await?;
            show(
                &serde_json::json!({"schema":1,"state":"passed","host":result.host,"toolchain":result.requested,"rustc":result.rustc_version,"cargo":result.cargo_version,"native_linker":"passed","file_limit":executor::raise_file_limit()?.1,"python_required":false}),
            )?;
        }
        Action::Prepare {
            source,
            run_dir,
            package,
            toolchain,
        } => {
            let dir = if run_dir.is_absolute() {
                run_dir
            } else {
                std::env::current_dir()?.join(run_dir)
            };
            show(&pipeline::prepare(&source, &dir, &package, &toolchain).await?)?;
        }
        Action::Run { run_dir } | Action::Resume { run_dir } => {
            let state = pipeline::resume(&run_dir, cancel).await?;
            let passed = state.state == "passed";
            show(&state)?;
            return Ok(passed);
        }
        Action::Status { run_dir } => {
            let (_, mut receipt) = pipeline::load_run(&run_dir)?;
            // No spawned processes, no mutations, and no stale JSON pretending to be a live PID.
            let running = receipt
                .attempts
                .values()
                .filter_map(|v| v.last())
                .find(|a| a.state == "running");
            let observation = running
                .and_then(|a| a.process_group)
                .map(|pid| executor::group_alive(pid).unwrap_or(true));
            if receipt.state == "running" && observation == Some(false) {
                receipt.state = "interrupted_requires_resume".into();
            }
            show(
                &serde_json::json!({"schema":1,"receipt":receipt,"observed_ms":evidence::now_ms(),"child_group_present":observation,"mutates_run":false}),
            )?;
        }
        Action::Cancel { run_dir } => {
            pipeline::request_cancel(&run_dir)?;
            show(
                &serde_json::json!({"schema":1,"state":"cancel_requested","run":run_dir,"scope":"cooperative request; no historical PID killed"}),
            )?;
        }
        Action::VerifyPackage { package } => show(&pipeline::verify_package(&package)?)?,
        Action::Install { request } => {
            let request: updater::InstallRequest = evidence::load(&request)?;
            show(&updater::install(&request)?)?;
        }
        Action::Probe { action } => match action {
            Probe::Echo { text } => {
                println!("{text}");
            }
            Probe::Hash { path } => show(&evidence::hash_file(&path)?)?,
            Probe::Pattern { path, bytes } => {
                ensure!(bytes <= 64 * 1024 * 1024, "probe_size_limit");
                let mut file = evidence::log_file(&path)?;
                let chunk: Vec<u8> = (0..65536).map(|n| (n % 256) as u8).collect();
                let mut left = bytes;
                while left > 0 {
                    let n = left.min(chunk.len() as u64) as usize;
                    file.write_all(&chunk[..n])?;
                    left -= n as u64;
                }
                file.sync_all()?;
                show(&evidence::hash_file(&path)?)?;
            }
            Probe::Sleep {
                ready,
                finished,
                millis,
            } => {
                ensure!(millis <= 60000, "probe_sleep_limit");
                evidence::log_file(&ready)?.write_all(std::process::id().to_string().as_bytes())?;
                tokio::time::sleep(Duration::from_millis(millis)).await;
                evidence::log_file(&finished)?.write_all(b"completed")?;
            }
            Probe::Parent { ready, finished } => {
                let mut child = std::process::Command::new(std::env::current_exe()?)
                    .args(["probe", "sleep", "--ready"])
                    .arg(&ready)
                    .arg("--finished")
                    .arg(&finished)
                    .spawn()
                    .context("fixture_child_spawn")?;
                // This fixture intentionally leaves a descendant for the executor's process-group test.
                let status = child.wait()?;
                ensure!(status.success(), "fixture_child_failed");
            }
        },
    }
    Ok(true)
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cancel = Arc::new(AtomicBool::new(false));
    let signal_cancel = cancel.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            if let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! { _ = term.recv() => (), _ = tokio::signal::ctrl_c() => () }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        signal_cancel.store(true, Ordering::Relaxed);
    });
    match action(Cli::parse().action, &cancel).await {
        Ok(true) => (),
        Ok(false) => std::process::exit(1),
        Err(error) => {
            let _ = show(
                &serde_json::json!({"schema":1,"state":"error","error":format!("{error:#}"),"deployed":false}),
            );
            std::process::exit(1);
        }
    }
}

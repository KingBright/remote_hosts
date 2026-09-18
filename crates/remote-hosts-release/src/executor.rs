//! Direct argv execution with bounded logs and owned process-group cleanup.
use crate::evidence;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub type Cancel = Arc<AtomicBool>;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub timeout_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub state: String,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub process_group: u32,
    pub cleanup_confirmed: bool,
}
const MAX_LOG: u64 = 32 * 1024 * 1024;
struct OwnedChild {
    child: Child,
    pgid: u32,
    cleaned: bool,
}
#[cfg(unix)]
pub fn group_alive(pgid: u32) -> Result<bool> {
    use nix::{errno::Errno, sys::signal::killpg, unistd::Pid};
    ensure!(pgid > 1 && pgid <= i32::MAX as u32, "invalid_process_group");
    match killpg(Pid::from_raw(pgid as i32), None) {
        Ok(()) | Err(Errno::EPERM) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(e) => Err(e.into()),
    }
}
#[cfg(not(unix))]
pub fn group_alive(_pgid: u32) -> Result<bool> {
    anyhow::bail!("native_executor_requires_unix; Windows job-object executor not implemented")
}
impl OwnedChild {
    fn kill_group(&mut self) {
        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, killpg},
                unistd::Pid,
            };
            let _ = killpg(Pid::from_raw(self.pgid as i32), Signal::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    async fn finish(&mut self) -> bool {
        // Descendants may retain stdout or continue writing after their direct parent exits.
        self.kill_group();
        for _ in 0..50 {
            if group_alive(self.pgid).is_ok_and(|alive| !alive) {
                self.cleaned = true;
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.cleaned {
            self.kill_group();
        }
    }
}
pub fn raise_file_limit() -> Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, getrlimit, setrlimit};
        let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE)?;
        let desired = soft.max(8192).min(hard);
        ensure!(
            desired >= 2048,
            "insufficient_fd_hard_limit: existing hard limit is {hard}"
        );
        if desired != soft {
            setrlimit(Resource::RLIMIT_NOFILE, desired, hard)?;
        }
        Ok((soft, desired))
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("native_executor_requires_unix")
    }
}
pub async fn execute<F>(
    spec: &CommandSpec,
    out: &Path,
    err: &Path,
    cancel_path: Option<&Path>,
    cancel: &Cancel,
    on_spawn: F,
) -> Result<Outcome>
where
    F: FnOnce(u32) -> Result<()>,
{
    ensure!(cfg!(unix), "native_executor_requires_unix");
    ensure!(
        spec.program.is_absolute() && spec.cwd.is_absolute(),
        "absolute_program_and_cwd_required"
    );
    ensure!(
        (1..=7200).contains(&spec.timeout_seconds),
        "invalid_execution_budget"
    );
    let stdout = evidence::log_file(out)?;
    let stderr = evidence::log_file(err)?;
    let stdout_sync = stdout.try_clone()?;
    let stderr_sync = stderr.try_clone()?;
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .env_clear()
        .envs(&spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let started = Instant::now();
    let child = command
        .spawn()
        .with_context(|| format!("spawn_failed: {}", spec.program.display()))?;
    let mut owned = OwnedChild {
        pgid: child.id(),
        child,
        cleaned: false,
    };
    on_spawn(owned.pgid)?;
    let (mut state, exit_code) = loop {
        if let Some(exit) = owned.child.try_wait()? {
            break (
                if exit.success() { "passed" } else { "failed" }.to_string(),
                exit.code(),
            );
        }
        if cancel.load(Ordering::Relaxed) || cancel_path.is_some_and(Path::exists) {
            break ("cancelled".to_string(), None);
        }
        if started.elapsed() >= Duration::from_secs(spec.timeout_seconds) {
            break ("timed_out".to_string(), None);
        }
        if fs::metadata(out)?.len() > MAX_LOG || fs::metadata(err)?.len() > MAX_LOG {
            break ("log_budget_exceeded".to_string(), None);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    };
    let cleanup_confirmed = owned.finish().await;
    stdout_sync.sync_all()?;
    stderr_sync.sync_all()?;
    if !cleanup_confirmed {
        state = "cleanup_unconfirmed".into();
    }
    Ok(Outcome {
        state,
        exit_code,
        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        process_group: owned.pgid,
        cleanup_confirmed,
    })
}
pub fn environment() -> BTreeMap<String, String> {
    // Secrets and arbitrary RUSTFLAGS/CARGO_* settings are not inherited implicitly.
    let mut values = BTreeMap::new();
    for key in [
        "HOME",
        "USER",
        "PATH",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SYSTEMROOT",
        "SystemRoot",
        "SystemDrive",
        "USERPROFILE",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "SDKROOT",
        "MACOSX_DEPLOYMENT_TARGET",
        "LIB",
        "INCLUDE",
    ] {
        if let Ok(value) = std::env::var(key) {
            values.insert(key.into(), value);
        }
    }
    values.insert("CARGO_TERM_COLOR".into(), "never".into());
    values.insert("LC_ALL".into(), "C".into());
    values
}
pub async fn probe_output(
    program: &Path,
    args: &[&str],
    env: &BTreeMap<String, String>,
) -> Result<String> {
    let temp = tempfile::tempdir()?;
    let cwd = temp.path().canonicalize()?;
    let out = cwd.join("out");
    let err = cwd.join("err");
    let result = execute(
        &CommandSpec {
            program: program.into(),
            args: args.iter().map(|v| (*v).into()).collect(),
            cwd,
            env: env.clone(),
            timeout_seconds: 30,
        },
        &out,
        &err,
        None,
        &Arc::new(AtomicBool::new(false)),
        |_| Ok(()),
    )
    .await?;
    ensure!(
        result.state == "passed",
        "toolchain_probe_failed: {}; {}",
        program.display(),
        fs::read_to_string(err)?
            .chars()
            .take(500)
            .collect::<String>()
    );
    ensure!(
        fs::metadata(&out)?.len() <= 65536,
        "toolchain_probe_output_budget"
    );
    Ok(fs::read_to_string(out)?.trim().to_string())
}

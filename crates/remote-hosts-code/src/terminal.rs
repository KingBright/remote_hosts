//! Persistent interactive PTYs and noninteractive pipes with stable, bounded output.
use crate::{
    AgentConfig,
    files::{self, Workspace},
    now,
    store::Store,
    terminal_output::{Capture, OUTPUT_CAP, read_page},
    token_output::{self, OutputProfile},
};
use anyhow::{Context, Result, ensure};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
#[derive(Clone, Serialize, Deserialize)]
pub struct Status {
    pub id: String,
    pub workspace_id: String,
    pub state: String,
    pub exit_code: Option<i64>,
    pub output_truncated: bool,
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    /// Spawn-time process identity. Historical PIDs never authorize signalling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    /// Initial working directory; a command may change directory after spawning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub pty: bool,
    /// 0 is the old raw log, 1 is an append-only sanitized UTF-8 log.
    #[serde(default)]
    pub log_format: u8,
    /// Capture is closed. Inspect output_truncated/output_error for completeness.
    #[serde(default)]
    pub output_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_error: Option<String>,
    /// Safe enum only; the original command is intentionally not persisted here.
    #[serde(default, skip_serializing_if = "OutputProfile::is_generic")]
    pub(crate) output_profile: OutputProfile,
}
type TerminalInput = Arc<Mutex<Box<dyn Write + Send>>>;
struct Live {
    input: Option<TerminalInput>,
    #[cfg(not(unix))]
    killer: Box<dyn portable_pty::ChildKiller + Send + Sync>,
    pid: Option<u32>,
    capture: Arc<Mutex<Capture>>,
}
#[derive(Clone)]
pub struct Terminals {
    store: Store,
    dir: PathBuf,
    live: Arc<Mutex<HashMap<String, Live>>>,
    secret: String,
    slots: Arc<tokio::sync::Semaphore>,
}
impl Terminals {
    pub async fn new(store: Store, dir: PathBuf, secret: String) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        // Recover every interrupted terminal atomically, regardless of history
        // size or key order. Do not load a truncated history page and miss live work.
        sqlx::query("UPDATE kv SET value=json_set(value,'$.state','runtime_lost','$.updated_at',?) WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting')")
            .bind(now()).execute(&store.pool).await?;
        Ok(Self {
            store,
            dir,
            live: Arc::new(Mutex::new(HashMap::new())),
            secret,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }
    /// Reconcile durable terminal rows that can no longer be owned by this runtime.
    /// A grace window prevents racing a freshly inserted `starting` row before its
    /// child handle reaches `live`. This never kills an OS process; unknown prior-
    /// runtime descendants are reported as ownership loss rather than guessed dead.
    pub async fn reconcile_orphans(&self, grace_seconds: i64) -> Result<usize> {
        ensure!(
            (5..=3600).contains(&grace_seconds),
            "invalid terminal reconcile grace"
        );
        let cutoff = now() - grace_seconds;
        let owned: HashSet<String> = self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?
            .keys()
            .cloned()
            .collect();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key,value FROM kv WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting') AND COALESCE(json_extract(value,'$.created_at'),0)<=?",
        )
        .bind(cutoff)
        .fetch_all(&self.store.pool)
        .await?;
        let mut reconciled = 0usize;
        for (id, text) in rows {
            if owned.contains(&id) {
                continue;
            }
            let mut status: Status = serde_json::from_str(&text)?;
            status.state = "runtime_lost".into();
            status.updated_at = now();
            status.output_complete = true;
            status.output_error = Some("terminal_runtime_ownership_lost".into());
            let saved = sqlx::query(
                "UPDATE kv SET value=? WHERE kind='terminal' AND key=? AND json_extract(value,'$.state') IN ('running','starting') AND COALESCE(json_extract(value,'$.created_at'),0)<=?",
            )
            .bind(serde_json::to_string(&status)?)
            .bind(&id)
            .bind(cutoff)
            .execute(&self.store.pool)
            .await?;
            if saved.rows_affected() == 0 {
                continue;
            }
            sqlx::query(
                "UPDATE kv SET value=json_set(value,'$.state','unknown','$.updated_at',?) WHERE kind='local_operation' AND key=? AND json_extract(value,'$.state')='running' AND json_extract(value,'$.tool')='terminal_exec'",
            )
            .bind(now())
            .bind(&id)
            .execute(&self.store.pool)
            .await?;
            reconciled += 1;
        }
        Ok(reconciled)
    }
    pub async fn start(
        &self,
        config: &AgentConfig,
        ws: &Workspace,
        v: &Value,
        id: &str,
    ) -> Result<Value> {
        let command = files::text(v, "command")?;
        ensure!(command.len() <= 65536, "command too large");
        let timeout = files::number(v, "timeout_seconds", 600, 1, 7200)?;
        // Default to a short bounded wait so fast commands usually finish in the
        // initiating tool call even when an older host schema cannot send wait_ms.
        // Long commands still return the same durable terminal handle.
        let wait_ms = files::number(v, "wait_ms", 1000, 0, 2000)?;
        let interactive = v.get("pty").and_then(Value::as_bool).unwrap_or(false);
        let rows = files::number(v, "rows", 40, 5, 200)? as u16;
        let cols = files::number(v, "cols", 120, 20, 500)? as u16;
        uuid::Uuid::parse_str(id).context("invalid terminal id")?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("device terminal capacity reached")?;
        let created_at = now();
        let status = Status {
            id: id.into(),
            workspace_id: ws.id.clone(),
            state: "starting".into(),
            exit_code: None,
            output_truncated: false,
            created_at,
            updated_at: created_at,
            process_id: None,
            working_directory: Some(ws.root.to_string_lossy().into_owned()),
            pty: interactive,
            log_format: 1,
            output_complete: false,
            output_error: None,
            output_profile: token_output::classify(command, interactive),
        };
        self.store.put("terminal", id, &status, i64::MAX).await?;
        // Prepare private output and all fallible I/O handles before spawning.
        let setup = (|| -> Result<_> {
            let capture = Arc::new(Mutex::new(Capture::new(
                &self.dir.join(format!("{id}.log")),
                self.secret.clone(),
            )?));
            let process = spawn(config, ws, command, interactive, rows, cols)?;
            Ok((capture, process))
        })();
        let mut status = status;
        let (
            capture,
            Spawned {
                mut child,
                mut reader,
                input,
                master,
            },
        ) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                status.state = "failed".into();
                status.updated_at = now();
                status.output_complete = true;
                self.store.put("terminal", id, &status, i64::MAX).await?;
                return Err(error);
            }
        };
        let pid = child.process_id();
        status.process_id = pid;
        status.state = "running".into();
        status.updated_at = now();
        if let Err(error) = self.store.put("terminal", id, &status, i64::MAX).await {
            let _ = kill_group(pid);
            let _ = child.kill();
            let _ = tokio::task::spawn_blocking(move || {
                let result = child.wait();
                drop(master);
                result
            })
            .await;
            return Err(error);
        }
        let input = input.map(|writer| Arc::new(Mutex::new(writer)));
        self.live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?
            .insert(
                id.into(),
                Live {
                    input: input.clone(),
                    #[cfg(not(unix))]
                    killer: child.clone_killer(),
                    pid,
                    capture: capture.clone(),
                },
            );
        let output = capture.clone();
        let auto_cursor_input = if interactive && is_powershell_shell(&config.shell) {
            input.clone()
        } else {
            None
        };
        let mut reader_task = tokio::task::spawn_blocking(move || {
            let mut buf = [0u8; 8192];
            let mut dsr = DsrResponder::default();
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let replies = dsr.feed(&buf[..n]);
                        if replies > 0
                            && let Some(input) = auto_cursor_input.as_ref()
                            && let Ok(mut writer) = input.lock()
                        {
                            for _ in 0..replies {
                                if writer.write_all(b"\x1b[1;1R").is_err() {
                                    break;
                                }
                            }
                            let _ = writer.flush();
                        }
                        if let Ok(mut output) = output.lock() {
                            output.push(&buf[..n]);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) if pty_eof(&error, interactive) => break,
                    Err(_) => {
                        if let Ok(mut output) = output.lock() {
                            output.fail("output_read_failed");
                        }
                        break;
                    }
                }
            }
            if let Ok(mut output) = output.lock() {
                output.finish();
            }
        });
        let this = self.clone();
        let key = id.to_owned();
        // This invocation waits on its own finalization, not on 50 SQL reads/sec.
        // The durable row remains authoritative; notifications only wake the reader.
        let completion = Arc::new(tokio::sync::Notify::new());
        let finalized = completion.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let mut waiter = tokio::task::spawn_blocking(move || {
                let result = child.wait();
                drop(master);
                result
            });
            let result = match tokio::time::timeout(
                std::time::Duration::from_secs(timeout as u64),
                &mut waiter,
            )
            .await
            {
                Ok(r) => r.ok().and_then(Result::ok),
                Err(_) => {
                    let _ = this.kill(&key);
                    status.state = "timed_out".into();
                    waiter.await.ok().and_then(Result::ok)
                }
            };
            // Descendants in this group may retain output handles after the shell exits.
            // Do not send a second single-PID signal after reaping the shell.
            let _ = kill_group(pid);
            if let Ok(mut live) = this.live.lock()
                && let Some(terminal) = live.get_mut(&key)
            {
                terminal.pid = None;
                terminal.input.take();
            }
            let drained = matches!(
                tokio::time::timeout(Duration::from_secs(3), &mut reader_task).await,
                Ok(Ok(()))
            );
            if let Ok(mut output) = capture.lock() {
                if !drained {
                    output.fail("output_drain_timeout");
                }
                output.finish(); // Also seals the log against a detached late reader.
                status.output_truncated = output.truncated;
                status.output_error = output.error.map(str::to_owned);
                status.output_complete = output.complete;
            } else {
                status.output_error = Some("output_lock_poisoned".into());
            }
            if status.state == "running" {
                status.state = if result.is_some() { "exited" } else { "failed" }.into();
            }
            status.updated_at = now();
            status.exit_code = result.map(|s| i64::from(s.exit_code()));
            // Cancellation and finalization race through one conditional SQL update,
            // not a read/modify/write that could overwrite an accepted cancellation.
            let saved = match serde_json::to_string(&status) {
                Ok(value) => sqlx::query("UPDATE kv SET value=json_set(?,'$.state',CASE WHEN json_extract(value,'$.state')='cancelled' THEN 'cancelled' ELSE ? END) WHERE kind='terminal' AND key=?")
                    .bind(value).bind(&status.state).bind(&key).execute(&this.store.pool).await.is_ok(),
                Err(_) => false,
            };
            if !saved {
                tracing::error!(terminal_id=%key,"terminal status persistence failed");
            }
            if let Ok(mut live) = this.live.lock() {
                live.remove(&key);
            }
            finalized.notify_waiters();
        });
        let mut result = json!({"terminal_id":id,"state":"running","cursor":0,"next_action":"operation_get","pty":interactive});
        if wait_ms > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms as u64);
            loop {
                // Register before reading so a concurrent final commit is not lost.
                let changed = completion.notified();
                let status = match tokio::time::timeout_at(deadline, self.status(ws, id)).await {
                    Ok(Ok(status)) => status,
                    _ => break,
                };
                if (status.output_complete
                    && status.state != "running"
                    && status.state != "starting")
                    || tokio::time::Instant::now() >= deadline
                {
                    break;
                }
                tokio::select! {
                    _ = changed => {},
                    _ = tokio::time::sleep_until(deadline) => break,
                }
            }
            match self
                .read(ws, &json!({"terminal_id":id,"max_bytes":16000}))
                .await
            {
                Ok(page) => {
                    result["state"] = page["terminal"]["state"].clone();
                    result["terminal"] = page["terminal"].clone();
                    for key in [
                        "output",
                        "cursor",
                        "has_more",
                        "cursor_format",
                        "output_stream",
                        "output_view",
                        "raw_cursor_start",
                        "compression",
                    ] {
                        if page.get(key).is_some() {
                            result[key] = page[key].clone();
                        }
                    }
                    if page["terminal"]["output_complete"] == true
                        && page["has_more"] == false
                        && !matches!(
                            page["terminal"]["state"].as_str(),
                            Some("starting" | "running")
                        )
                    {
                        result["next_action"] = Value::Null;
                    }
                }
                Err(_) => {
                    result["observation_error"] =
                        json!("initial_output_unavailable; query the same terminal_id");
                }
            }
        }
        Ok(result)
    }
    fn kill(&self, id: &str) -> Result<()> {
        let mut live = self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?;
        if let Some(t) = live.get_mut(id) {
            // Stop the owned process group before releasing its input. A PTY
            // writer must never try to enqueue EOF into a full input queue here.
            kill_group(t.pid)?;
            #[cfg(not(unix))]
            t.killer.kill()?;
            t.input.take();
        }
        Ok(())
    }
    async fn status(&self, ws: &Workspace, id: &str) -> Result<Status> {
        let s: Status = self
            .store
            .get("terminal", id)
            .await?
            .context("terminal not found")?;
        ensure!(
            s.workspace_id == ws.id,
            "terminal belongs to another workspace"
        );
        Ok(s)
    }
    pub async fn read(&self, ws: &Workspace, v: &Value) -> Result<Value> {
        let id = files::text(v, "terminal_id")?;
        let mut status = self.status(ws, id).await?;
        let cursor = files::number(v, "cursor", 0, 0, 100000000)?;
        let output_mode = v
            .get("output_mode")
            .and_then(Value::as_str)
            .unwrap_or(if status.pty { "full" } else { "compact" });
        ensure!(
            matches!(output_mode, "compact" | "full"),
            "invalid output_mode"
        );
        // For recognized high-noise commands, consume a much larger raw page before
        // semantic compaction. This reduces MCP round trips and repeated cursor/status
        // envelopes without exposing larger unknown-command output. Generic/full views
        // retain the conservative 16 KB default.
        let default_max = if output_mode == "compact"
            && status.output_profile != OutputProfile::Generic
            && status.log_format == 1
        {
            65_536
        } else {
            16_000
        };
        let max = files::number(v, "max_bytes", default_max, 1024, 65536)?;
        let capture = self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?
            .get(id)
            .map(|live| live.capture.clone());
        let committed = if let Some(capture) = capture {
            let output = capture
                .lock()
                .map_err(|_| anyhow::anyhow!("output lock poisoned"))?;
            status.output_truncated = output.truncated;
            status.output_error = output.error.map(str::to_owned);
            status.output_complete = output.complete;
            Some(output.written)
        } else {
            None
        };
        let path = self.dir.join(format!("{id}.log"));
        let format = status.log_format;
        let secret = self.secret.clone();
        let (chunk, cursor, has_more) = tokio::task::spawn_blocking(move || {
            if format == 1 {
                return read_page(&path, cursor, max, committed);
            }
            ensure!(format == 0, "unsupported terminal log format");
            // Legacy logs are raw: keep their original redaction/cursor algorithm.
            // New captures never use this path. Do not silently treat raw logs as safe.
            let file = std::fs::File::open(&path).context("terminal output unavailable")?;
            let mut bytes = Vec::new();
            file.take((OUTPUT_CAP + 1) as u64).read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= OUTPUT_CAP,
                "legacy terminal log exceeds capture limit"
            );
            let content = String::from_utf8_lossy(&bytes).replace(&secret, "[REDACTED]");
            static LEGACY: LazyLock<regex::Regex> = LazyLock::new(|| {
                regex::Regex::new(
                    r#"(?i)(password|token|secret|api[_-]?key)\s*[:=]\s*("[^"]*"|'[^']*'|[^\s]+)"#,
                )
                .expect("static legacy redaction pattern")
            });
            let content = LEGACY.replace_all(&content, "$1=[REDACTED]");
            ensure!(
                cursor <= content.len() && content.is_char_boundary(cursor),
                "invalid output cursor"
            );
            let (chunk, more) = files::bounded(&content[cursor..], max);
            Ok((chunk.to_owned(), cursor + chunk.len(), more))
        })
        .await??;
        let raw_cursor_start = v.get("cursor").and_then(Value::as_u64).unwrap_or(0);
        let (output, compression) = if output_mode == "compact" && format == 1 {
            let compacted = token_output::compact(status.output_profile, &chunk, status.exit_code);
            (compacted.output, Some(compacted.metadata))
        } else {
            (chunk, None)
        };
        let mut result = json!({"terminal":status,"output":output,"cursor":cursor,"has_more":has_more,"retry_after_ms":500,"cursor_format":if format == 1 {"sanitized_utf8_v1"} else {"legacy_transformed"},"output_stream":"combined","output_view":output_mode,"raw_cursor_start":raw_cursor_start});
        if let Some(compression) = compression {
            result["compression"] = compression;
        }
        Ok(result)
    }
    pub async fn input(&self, ws: &Workspace, v: &Value) -> Result<Value> {
        let id = files::text(v, "terminal_id")?;
        let status = self.status(ws, id).await?;
        ensure!(status.state == "running", "terminal is not running");
        ensure!(
            status.pty,
            "non-interactive terminal has stdin EOF; use pty=true for terminal_input"
        );
        let text = files::text(v, "text")?;
        ensure!(text.len() <= 16384, "input too large");
        let input = self
            .live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?
            .get(id)
            .and_then(|x| x.input.clone())
            .context("runtime_lost: terminal cannot be resumed after agent restart")?;
        let text = text.to_owned();
        let accepted = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut writer = input
                .lock()
                .map_err(|_| anyhow::anyhow!("terminal input lock poisoned"))?;
            writer.write_all(text.as_bytes())?;
            writer.flush()?;
            Ok(text.len())
        })
        .await??;
        Ok(json!({"terminal_id":id,"accepted_bytes":accepted}))
    }
    pub async fn cancel(&self, ws: &Workspace, v: &Value) -> Result<Value> {
        let id = files::text(v, "terminal_id")?;
        self.status(ws, id).await?;
        let changed: Option<(String,)> = sqlx::query_as("UPDATE kv SET value=json_set(value,'$.state','cancelled','$.updated_at',?) WHERE kind='terminal' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.state')='running' RETURNING value")
            .bind(now()).bind(id).bind(&ws.id).fetch_optional(&self.store.pool).await?;
        let status = if let Some((value,)) = changed {
            self.kill(id)?;
            serde_json::from_str::<Status>(&value)?
        } else {
            self.status(ws, id).await?
        };
        Ok(json!({"terminal":status}))
    }
}
struct Spawned {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    reader: Box<dyn Read + Send>,
    input: Option<Box<dyn Write + Send>>,
    master: Option<Box<dyn portable_pty::MasterPty + Send>>,
}
fn spawn(
    config: &AgentConfig,
    ws: &Workspace,
    command: &str,
    interactive: bool,
    rows: u16,
    cols: u16,
) -> Result<Spawned> {
    if interactive {
        let pair = native_pty_system().openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        let reader = pair.master.try_clone_reader()?;
        #[cfg(unix)]
        let input: Box<dyn Write + Send> = {
            use std::os::fd::BorrowedFd;
            let fd = pair
                .master
                .as_raw_fd()
                .context("native PTY master descriptor unavailable")?;
            // SAFETY: the master owns fd throughout this borrow; try_clone_to_owned
            // duplicates it with independent ownership. File::drop only closes it.
            // portable-pty's writer Drop sends newline+EOF, which can block forever
            // when a raw-mode child does not read and its input queue is full.
            let owned = unsafe { BorrowedFd::borrow_raw(fd) }.try_clone_to_owned()?;
            Box::new(std::fs::File::from(owned))
        };
        #[cfg(not(unix))]
        let input = pair.master.take_writer()?;
        let mut cmd = CommandBuilder::new(&config.shell);
        for arg in shell_args(&config.shell, command, true) {
            cmd.arg(arg);
        }
        cmd.cwd(&ws.root);
        cmd.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(cmd)?;
        drop(pair.slave);
        Ok(Spawned {
            child,
            reader,
            input: Some(input),
            master: Some(pair.master),
        })
    } else {
        // A single kernel pipe preserves the observed stdout/stderr write order.
        // It is intentionally a combined stream, not falsely labeled stdout-only.
        let (reader, writer) = std::io::pipe()?;
        let mut cmd = std::process::Command::new(&config.shell);
        cmd.args(shell_args(&config.shell, command, false))
            .current_dir(&ws.root)
            .stdin(std::process::Stdio::null())
            .stdout(writer.try_clone()?)
            .stderr(writer)
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CLICOLOR", "0")
            .env("PAGER", "cat")
            .env("GIT_PAGER", "cat");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd.spawn()?;
        drop(cmd); // Close the parent's pipe writers, or EOF would never arrive.
        Ok(Spawned {
            child: Box::new(child),
            reader: Box::new(reader),
            input: None,
            master: None,
        })
    }
}
fn kill_group(pid: Option<u32>) -> Result<()> {
    #[cfg(unix)]
    if let Some(pid) = pid {
        use nix::{
            errno::Errno,
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        ensure!(pid > 1 && pid <= i32::MAX as u32, "invalid process group");
        match killpg(Pid::from_raw(pid as i32), Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => (),
            Err(error) => return Err(error.into()),
        }
    }
    #[cfg(not(unix))]
    let _ = pid;
    Ok(())
}
fn pty_eof(error: &std::io::Error, interactive: bool) -> bool {
    #[cfg(unix)]
    {
        interactive && error.raw_os_error() == Some(nix::errno::Errno::EIO as i32)
    }
    #[cfg(not(unix))]
    {
        let _ = (error, interactive);
        false
    }
}
#[derive(Default)]
struct DsrResponder {
    matched: usize,
}
impl DsrResponder {
    fn feed(&mut self, bytes: &[u8]) -> usize {
        const QUERY: &[u8] = b"\x1b[6n";
        let mut replies = 0usize;
        for &byte in bytes {
            if byte == QUERY[self.matched] {
                self.matched += 1;
                if self.matched == QUERY.len() {
                    replies += 1;
                    self.matched = 0;
                }
            } else {
                self.matched = usize::from(byte == QUERY[0]);
            }
        }
        replies
    }
}
fn shell_name(shell: &Path) -> String {
    shell
        .to_string_lossy()
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}
fn is_powershell_shell(shell: &Path) -> bool {
    matches!(
        shell_name(shell).as_str(),
        "powershell" | "powershell.exe" | "pwsh" | "pwsh.exe"
    )
}
fn shell_args(shell: &Path, command: &str, interactive: bool) -> Vec<String> {
    let name = shell_name(shell);
    if is_powershell_shell(shell) {
        let mut args = vec!["-NoLogo".into(), "-NoProfile".into()];
        if !interactive {
            args.push("-NonInteractive".into());
        }
        if !command.is_empty() {
            args.push("-Command".into());
            args.push(command.into());
        }
        return args;
    }
    if matches!(name.as_str(), "cmd" | "cmd.exe") {
        if command.is_empty() {
            return vec!["/D".into()];
        }
        return vec!["/D".into(), "/S".into(), "/C".into(), command.into()];
    }
    vec![
        "-lc".into(),
        if interactive && command.is_empty() {
            format!("exec {} -l", shell_quote(&shell.to_string_lossy()))
        } else {
            command.into()
        },
    ]
}
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsr_responder_handles_split_and_repeated_queries() {
        let mut responder = DsrResponder::default();
        assert_eq!(responder.feed(b"prefix\x1b["), 0);
        assert_eq!(responder.feed(b"6n"), 1);
        assert_eq!(responder.feed(b"\x1b[6ntext\x1b[6n"), 2);
        assert_eq!(responder.feed(b"\x1b[x\x1b[6n"), 1);
    }

    #[test]
    fn shell_arguments_are_native_for_windows_and_unix_shells() {
        assert_eq!(
            shell_args(
                Path::new(r"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"),
                "Write-Output ok",
                false
            ),
            vec![
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Write-Output ok"
            ]
        );
        assert_eq!(
            shell_args(Path::new("pwsh.exe"), "Write-Output ok", true),
            vec!["-NoLogo", "-NoProfile", "-Command", "Write-Output ok"]
        );
        assert_eq!(
            shell_args(Path::new("cmd.exe"), "echo ok", false),
            vec!["/D", "/S", "/C", "echo ok"]
        );
        assert_eq!(
            shell_args(Path::new("/bin/zsh"), "printf ok", false),
            vec!["-lc", "printf ok"]
        );
        assert_eq!(
            shell_args(Path::new("/bin/zsh"), "", true),
            vec!["-lc", "exec '/bin/zsh' -l"]
        );
    }

    fn status(id: &str, created_at: i64) -> Status {
        Status {
            id: id.into(),
            workspace_id: "workspace".into(),
            state: "running".into(),
            exit_code: None,
            output_truncated: false,
            created_at,
            updated_at: created_at,
            process_id: None,
            working_directory: None,
            pty: false,
            log_format: 1,
            output_complete: false,
            output_error: None,
            output_profile: OutputProfile::Generic,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn short_noninteractive_command_returns_exit_and_output_without_explicit_wait() {
        let d = tempfile::tempdir().unwrap();
        let store = Store::open(d.path()).await.unwrap();
        let terminals = Terminals::new(
            store,
            d.path().join("terminals"),
            "synthetic-terminal-secret-0123456789".into(),
        )
        .await
        .unwrap();
        let ws = Workspace {
            id: "workspace".into(),
            device_id: "device".into(),
            root: d.path().to_path_buf(),
        };
        let config = AgentConfig {
            gateway_url: "https://unused.example".into(),
            device_id: "device".into(),
            device_token: "unused".into(),
            state_dir: d.path().join("state"),
            roots: vec![d.path().to_path_buf()],
            allow_write: true,
            allow_exec: true,
            shell: PathBuf::from("/bin/sh"),
        };
        let id = uuid::Uuid::new_v4().to_string();
        let out = terminals
            .start(&config, &ws, &json!({"command":"printf one-call"}), &id)
            .await
            .unwrap();
        assert_eq!(out["terminal"]["exit_code"], 0, "{out}");
        assert_eq!(out["terminal"]["output_complete"], true, "{out}");
        assert_eq!(out["has_more"], false, "{out}");
        assert_eq!(out["output"], "one-call", "{out}");
        assert!(out["next_action"].is_null(), "{out}");
    }

    #[tokio::test]
    async fn terminal_read_defaults_to_compact_and_full_recovers_exact_log() {
        let d = tempfile::tempdir().unwrap();
        let store = Store::open(d.path()).await.unwrap();
        let dir = d.path().join("terminals");
        let terminals = Terminals::new(
            store.clone(),
            dir.clone(),
            "synthetic-terminal-secret-0123456789".into(),
        )
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let raw = "   Compiling demo v0.1.0\nrunning 2 tests\ntest tests::a ... ok\ntest tests::b ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n";
        crate::write_private(&dir.join(format!("{id}.log")), raw.as_bytes()).unwrap();
        let mut saved = status(&id, now());
        saved.state = "exited".into();
        saved.exit_code = Some(0);
        saved.output_complete = true;
        saved.output_profile = OutputProfile::CargoTest;
        store.put("terminal", &id, &saved, i64::MAX).await.unwrap();
        let ws = Workspace {
            id: "workspace".into(),
            device_id: "device".into(),
            root: d.path().to_path_buf(),
        };

        let compacted = terminals
            .read(&ws, &json!({"terminal_id":id,"cursor":0,"max_bytes":65536}))
            .await
            .unwrap();
        assert_eq!(compacted["output_view"], "compact");
        assert!(
            compacted["output"]
                .as_str()
                .unwrap()
                .contains("2 passed, 0 failed, 0 ignored")
        );
        assert!(
            !compacted["output"]
                .as_str()
                .unwrap()
                .contains("tests::a ... ok")
        );
        assert_eq!(compacted["raw_cursor_start"], 0);
        assert_eq!(compacted["cursor"].as_u64().unwrap(), raw.len() as u64);
        assert!(compacted["compression"]["saved_tokens"].as_u64().unwrap() > 0);

        let full = terminals
            .read(
                &ws,
                &json!({"terminal_id":id,"cursor":0,"max_bytes":65536,"output_mode":"full"}),
            )
            .await
            .unwrap();
        assert_eq!(full["output_view"], "full");
        assert_eq!(full["output"], raw);
        assert!(full.get("compression").is_none());
    }

    #[tokio::test]
    async fn recognized_compact_read_consumes_large_raw_page_by_default() {
        let d = tempfile::tempdir().unwrap();
        let store = Store::open(d.path()).await.unwrap();
        let dir = d.path().join("terminals");
        let terminals = Terminals::new(
            store.clone(),
            dir.clone(),
            "synthetic-terminal-secret-0123456789".into(),
        )
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let mut raw = String::from("running 700 tests\n");
        for n in 0..700 {
            raw.push_str(&format!(
                "test integration::long_case_{n:04}_that_is_routine ... ok\n"
            ));
        }
        raw.push_str(
            "test result: ok. 700 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        );
        assert!(raw.len() > 16_000 && raw.len() < 65_536);
        crate::write_private(&dir.join(format!("{id}.log")), raw.as_bytes()).unwrap();
        let mut saved = status(&id, now());
        saved.state = "exited".into();
        saved.exit_code = Some(0);
        saved.output_complete = true;
        saved.output_profile = OutputProfile::CargoTest;
        store.put("terminal", &id, &saved, i64::MAX).await.unwrap();
        let ws = Workspace {
            id: "workspace".into(),
            device_id: "device".into(),
            root: d.path().to_path_buf(),
        };

        let compacted = terminals
            .read(&ws, &json!({"terminal_id":id,"cursor":0}))
            .await
            .unwrap();
        assert_eq!(compacted["cursor"].as_u64().unwrap(), raw.len() as u64);
        assert_eq!(compacted["has_more"], false);
        assert!(compacted["output"].as_str().unwrap().len() < 800);
        assert!(
            compacted["output"]
                .as_str()
                .unwrap()
                .contains("700 passed, 0 failed, 0 ignored")
        );

        let full = terminals
            .read(
                &ws,
                &json!({"terminal_id":id,"cursor":0,"output_mode":"full"}),
            )
            .await
            .unwrap();
        assert_eq!(full["cursor"], 16_000);
        assert_eq!(full["has_more"], true);
    }

    #[tokio::test]
    async fn reconciliation_closes_only_old_unowned_terminal_state() {
        let d = tempfile::tempdir().unwrap();
        let store = Store::open(d.path()).await.unwrap();
        let terminals = Terminals::new(
            store.clone(),
            d.path().join("terminals"),
            "synthetic-terminal-secret-0123456789".into(),
        )
        .await
        .unwrap();
        let old = uuid::Uuid::new_v4().to_string();
        let recent = uuid::Uuid::new_v4().to_string();
        store
            .put("terminal", &old, &status(&old, now() - 120), i64::MAX)
            .await
            .unwrap();
        store
            .put("terminal", &recent, &status(&recent, now()), i64::MAX)
            .await
            .unwrap();
        store
            .put(
                "local_operation",
                &old,
                &json!({"state":"running","tool":"terminal_exec","updated_at":now()-120}),
                i64::MAX,
            )
            .await
            .unwrap();

        assert_eq!(terminals.reconcile_orphans(30).await.unwrap(), 1);
        let old_status = store
            .get::<Status>("terminal", &old)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old_status.state, "runtime_lost");
        assert!(old_status.output_complete);
        assert_eq!(
            old_status.output_error.as_deref(),
            Some("terminal_runtime_ownership_lost")
        );
        let recent_status = store
            .get::<Status>("terminal", &recent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recent_status.state, "running");
        let op = store
            .get::<Value>("local_operation", &old)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(op["state"], "unknown");
    }
}

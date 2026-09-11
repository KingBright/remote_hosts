//! Persistent interactive PTYs and noninteractive pipes with stable, bounded output.
use crate::{
    AgentConfig,
    files::{self, Workspace},
    now,
    store::Store,
    terminal_output::{Capture, OUTPUT_CAP, read_page},
};
use anyhow::{Context, Result, ensure};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::PathBuf,
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
    pub pty: bool,
    /// 0 is the old raw log, 1 is an append-only sanitized UTF-8 log.
    #[serde(default)]
    pub log_format: u8,
    /// Capture is closed. Inspect output_truncated/output_error for completeness.
    #[serde(default)]
    pub output_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_error: Option<String>,
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
        sqlx::query("UPDATE kv SET value=json_set(value,'$.state','runtime_lost') WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting')")
            .execute(&store.pool).await?;
        Ok(Self {
            store,
            dir,
            live: Arc::new(Mutex::new(HashMap::new())),
            secret,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })
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
        let wait_ms = files::number(v, "wait_ms", 0, 0, 2000)?;
        let interactive = v.get("pty").and_then(Value::as_bool).unwrap_or(false);
        let rows = files::number(v, "rows", 40, 5, 200)? as u16;
        let cols = files::number(v, "cols", 120, 20, 500)? as u16;
        uuid::Uuid::parse_str(id).context("invalid terminal id")?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("device terminal capacity reached")?;
        let status = Status {
            id: id.into(),
            workspace_id: ws.id.clone(),
            state: "starting".into(),
            exit_code: None,
            output_truncated: false,
            created_at: now(),
            pty: interactive,
            log_format: 1,
            output_complete: false,
            output_error: None,
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
                status.output_complete = true;
                self.store.put("terminal", id, &status, i64::MAX).await?;
                return Err(error);
            }
        };
        let pid = child.process_id();
        status.state = "running".into();
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
        self.live
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal lock poisoned"))?
            .insert(
                id.into(),
                Live {
                    input: input.map(|writer| Arc::new(Mutex::new(writer))),
                    #[cfg(not(unix))]
                    killer: child.clone_killer(),
                    pid,
                    capture: capture.clone(),
                },
            );
        let output = capture.clone();
        let mut reader_task = tokio::task::spawn_blocking(move || {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
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
        });
        let mut result = json!({"terminal_id":id,"state":"running","cursor":0,"next_action":"terminal_read","pty":interactive});
        if wait_ms > 0 {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms as u64);
            loop {
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
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(20)),
                )
                .await;
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
                    ] {
                        result[key] = page[key].clone();
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
            t.input.take();
            kill_group(t.pid)?;
            #[cfg(not(unix))]
            t.killer.kill()?;
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
        let max = files::number(v, "max_bytes", 16000, 1024, 65536)?;
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
        Ok(
            json!({"terminal":status,"output":chunk,"cursor":cursor,"has_more":has_more,"retry_after_ms":500,"cursor_format":if format == 1 {"sanitized_utf8_v1"} else {"legacy_transformed"},"output_stream":"combined"}),
        )
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
        let changed: Option<(String,)> = sqlx::query_as("UPDATE kv SET value=json_set(value,'$.state','cancelled') WHERE kind='terminal' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.state')='running' RETURNING value")
            .bind(id).bind(&ws.id).fetch_optional(&self.store.pool).await?;
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
        let input = pair.master.take_writer()?;
        let mut cmd = CommandBuilder::new(&config.shell);
        cmd.arg("-lc");
        cmd.arg(if command.is_empty() {
            format!("exec {} -l", shell_quote(&config.shell.to_string_lossy()))
        } else {
            command.into()
        });
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
        cmd.args(["-lc", command])
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
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

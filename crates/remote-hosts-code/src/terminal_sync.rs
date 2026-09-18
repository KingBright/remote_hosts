//! Bounded terminal-state and output-preview replication. Observation never launches a read job.
use crate::{gateway::Gateway, now, store::Store, terminal::Status};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

const PREVIEW_BYTES: usize = 2048;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preview {
    pub operation_id: String,
    pub cursor_start: u64,
    pub cursor_end: u64,
    pub output: String,
    pub truncated_before: bool,
}

pub(crate) fn valid(s: &Status) -> bool {
    uuid::Uuid::parse_str(&s.id).is_ok()
        && s.workspace_id.len() <= 160
        && matches!(
            s.state.as_str(),
            "starting"
                | "running"
                | "exited"
                | "cancelled"
                | "timed_out"
                | "runtime_lost"
                | "failed"
        )
        && s.output_error.as_ref().is_none_or(|s| s.len() <= 100)
        && s.working_directory
            .as_ref()
            .is_none_or(|s| s.len() <= 4096 && !s.contains('\0'))
}

pub(crate) fn valid_preview(p: &Preview) -> bool {
    uuid::Uuid::parse_str(&p.operation_id).is_ok()
        && p.cursor_start <= p.cursor_end
        && p.output.len() <= PREVIEW_BYTES
        && p.cursor_end <= crate::terminal_output::OUTPUT_CAP as u64
        && p.truncated_before == (p.cursor_start > 0)
        && p.cursor_end.saturating_sub(p.cursor_start) == p.output.len() as u64
}

pub(crate) async fn collect(s: &Store) -> Result<Vec<Status>> {
    // Terminal replication is live state, not an audit-log query. Keep active
    // terminals first, then the most recently changed terminal rows directly.
    let rows:Vec<(String,)>=sqlx::query_as("SELECT value FROM kv WHERE kind='terminal' ORDER BY (json_extract(value,'$.state') IN ('running','starting')) DESC,COALESCE(json_extract(value,'$.updated_at'),json_extract(value,'$.created_at'),0) DESC,key LIMIT 24").fetch_all(&s.pool).await?;
    rows.into_iter()
        .map(|(v,)| Ok(serde_json::from_str(&v)?))
        .collect()
}

fn tail(path: &Path) -> Result<(String, u64, u64, bool)> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len() as usize;
    anyhow::ensure!(
        len <= crate::terminal_output::OUTPUT_CAP,
        "terminal_log_over_capture_limit"
    );
    let probe_start = len.saturating_sub(PREVIEW_BYTES + 3);
    file.seek(SeekFrom::Start(probe_start as u64))?;
    let mut bytes = Vec::with_capacity(len.saturating_sub(probe_start));
    // Read exactly the observed extent: a concurrent writer cannot move our end
    // cursor or turn a 2 KiB preview into an unbounded read.
    file.take((len - probe_start) as u64)
        .read_to_end(&mut bytes)?;
    let boundary = bytes
        .iter()
        .position(|byte| byte & 0xc0 != 0x80)
        .unwrap_or(bytes.len());
    let text = match std::str::from_utf8(&bytes[boundary..]) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() => {
            std::str::from_utf8(&bytes[boundary..boundary + error.valid_up_to()])?
        }
        Err(error) => return Err(error.into()),
    };
    let mut suffix = text.len().saturating_sub(PREVIEW_BYTES);
    while suffix < text.len() && !text.is_char_boundary(suffix) {
        suffix += 1;
    }
    let output = text[suffix..].to_owned();
    let start = probe_start + boundary + suffix;
    let end = start + output.len();
    Ok((output, start as u64, end as u64, start > 0))
}

pub(crate) async fn previews(statuses: &[Status], dir: PathBuf) -> Result<Vec<Preview>> {
    let statuses = statuses.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut previews = Vec::with_capacity(statuses.len());
        for status in statuses {
            // Legacy format 0 needs its older read-time redaction transform, so do
            // not replicate bytes from it into the Gateway preview cache.
            if status.log_format != 1 {
                continue;
            }
            let path = dir.join(format!("{}.log", status.id));
            let Ok((output, cursor_start, cursor_end, truncated_before)) = tail(&path) else {
                continue;
            };
            previews.push(Preview {
                operation_id: status.id,
                cursor_start,
                cursor_end,
                output,
                truncated_before,
            });
        }
        Ok(previews)
    })
    .await?
}

fn ended(state: &Value) -> bool {
    matches!(
        state.as_str(),
        Some("exited" | "cancelled" | "timed_out" | "runtime_lost" | "failed")
    )
}

fn merge(
    previous: Option<&Value>,
    status: &Status,
    preview: Option<&Preview>,
    session: &str,
) -> Option<Value> {
    let terminal = serde_json::to_value(status).ok()?;
    if let Some(previous) = previous {
        let old = &previous["terminal"];
        // Terminal IDs are execution identities, not reusable slots. Preserve a
        // terminal outcome even when packets arrive out of order or a new Agent
        // session reports recovered historical rows.
        if (ended(&old["state"]) && old["state"] != terminal["state"])
            || (old["state"] == "running" && terminal["state"] == "starting")
            || old["updated_at"].as_i64().unwrap_or(0) > status.updated_at
            || (old["output_complete"] == true && !status.output_complete)
            || (old["exit_code"].is_i64() && old["exit_code"] != terminal["exit_code"])
            || (old["output_truncated"] == true && !status.output_truncated)
            || (old["output_error"].is_string() && old["output_error"] != terminal["output_error"])
        {
            return None;
        }
    }
    let mut value = previous.cloned().unwrap_or_else(|| json!({}));
    value["terminal"] = terminal;
    value["reported_at"] = json!(now());
    value["session"] = json!(session);
    if let Some(preview) = preview.filter(|_| status.log_format == 1) {
        let old_end = value["output_cursor_end"].as_u64().unwrap_or(0);
        let old_end_known = value["output_cursor_end"].is_u64();
        if !old_end_known || preview.cursor_end > old_end {
            value["output_preview"] = json!(preview.output);
            value["output_cursor_start"] = json!(preview.cursor_start);
            value["output_cursor_end"] = json!(preview.cursor_end);
            value["output_truncated_before"] = json!(preview.truncated_before);
        }
    }
    let signature = |v: &Value| {
        json!({
            "state":v["terminal"]["state"],"exit":v["terminal"]["exit_code"],
            "complete":v["terminal"]["output_complete"],"truncated":v["terminal"]["output_truncated"],
            "error":v["terminal"]["output_error"],
            // Repeated identical log lines are output, but not new meaningful progress.
            "latest_line":v["output_preview"].as_str().and_then(|s|s.lines().rev().find(|line|!line.trim().is_empty())).map(str::trim)
        })
    };
    if previous.is_none_or(|old| signature(old) != signature(&value)) {
        value["last_progress_at"] = json!(now());
    }
    Some(value)
}

pub(crate) async fn save(
    g: &Gateway,
    device: &str,
    session: &str,
    statuses: &[Status],
    previews: &[Preview],
) -> Result<()> {
    if statuses.is_empty() {
        return Ok(());
    }
    anyhow::ensure!(
        statuses.len() <= 24 && statuses.iter().all(valid),
        "invalid_terminal_updates"
    );
    anyhow::ensure!(
        previews.len() <= 24 && previews.iter().all(valid_preview),
        "invalid_terminal_previews"
    );
    // A single write transaction serializes heartbeat and poll snapshots. Each
    // row is re-authorized against its original job and the current device session.
    let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
    // Fetch authorization and previous snapshots in one bounded query rather
    // than running three statements for every terminal on every heartbeat.
    let rows:Vec<(String,Option<String>)>=sqlx::query_as(
        "SELECT j.id,p.value FROM json_each(?) s JOIN jobs j ON j.id=json_extract(s.value,'$.id') AND j.device=? AND json_extract(j.request,'$.tool')='terminal_exec' AND json_extract(j.request,'$.arguments.workspace_id')=json_extract(s.value,'$.workspace_id') JOIN kv o ON o.kind='online' AND o.key=j.device AND json_extract(o.value,'$.hello.session')=? LEFT JOIN kv p ON p.kind='terminal_observation' AND p.key=j.id")
        .bind(serde_json::to_string(statuses)?).bind(device).bind(session).fetch_all(&mut *tx).await?;
    let mut updates = Vec::with_capacity(rows.len());
    for (id, previous) in rows {
        let Some(status) = statuses.iter().find(|s| s.id == id) else {
            continue;
        };
        let previous: Option<Value> = previous.as_deref().map(serde_json::from_str).transpose()?;
        let preview = previews.iter().find(|p| p.operation_id == id);
        if let Some(value) = merge(previous.as_ref(), status, preview, session) {
            updates.push(json!({"id":id,"value":value}));
        }
    }
    if !updates.is_empty() {
        sqlx::query("INSERT INTO kv(kind,key,value,expires) SELECT 'terminal_observation',json_extract(u.value,'$.id'),json_extract(u.value,'$.value'),? FROM json_each(?) u WHERE true ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
            .bind(crate::receipts::RETAIN_UNTIL_EXPLICIT_CLEANUP).bind(serde_json::to_string(&updates)?).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    g.observation_changed.notify_waiters();
    Ok(())
}

pub(crate) async fn observed(g: &Gateway, id: &str) -> Result<Option<Value>> {
    Ok(g.store.get::<Value>("terminal_observation",id).await?.map(|v|json!({
        "terminal":v["terminal"],"reported_at":v["reported_at"],
        "stale":!ended(&v["terminal"]["state"]) && !(0..=45).contains(&(now()-v["reported_at"].as_i64().unwrap_or(0))),
        "last_progress_at":v["last_progress_at"],
        "progress_semantics":"terminal transition or changed final nonempty output line; not business acceptance",
        "output":v["output_preview"],"output_cursor_start":v["output_cursor_start"],
        "output_cursor_end":v["output_cursor_end"],"output_gap":v["output_truncated_before"],
        "output_next_action":"operation_get for live preview; terminal_read only for full history/recovery",
        "protocol":2
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(state: &str, updated: i64, complete: bool) -> Status {
        serde_json::from_value(json!({"id":uuid::Uuid::nil().to_string(),"workspace_id":"w", "state":state,
            "exit_code":if state=="exited"{json!(0)}else{Value::Null},"created_at":1,"updated_at":updated,
            "output_complete":complete,"output_truncated":false,"log_format":1})).unwrap()
    }

    #[test]
    fn completed_terminal_rejects_late_running_and_keeps_preview() {
        let done = fixture("exited", 20, true);
        let preview = Preview {
            operation_id: done.id.clone(),
            cursor_start: 0,
            cursor_end: 3,
            output: "end".into(),
            truncated_before: false,
        };
        let old = merge(None, &done, Some(&preview), "session").unwrap();
        assert!(merge(Some(&old), &fixture("running", 10, false), None, "session").is_none());
        assert!(merge(Some(&old), &fixture("running", 30, false), None, "session").is_none());
        let renewed = merge(Some(&old), &done, None, "session").unwrap();
        assert_eq!(renewed["output_preview"], "end");
        assert_eq!(renewed["last_progress_at"], old["last_progress_at"]);
    }

    #[test]
    fn output_cursor_never_regresses_and_heartbeat_is_not_progress() {
        let running = fixture("running", 10, false);
        let p = Preview {
            operation_id: running.id.clone(),
            cursor_start: 0,
            cursor_end: 4,
            output: "abcd".into(),
            truncated_before: false,
        };
        let mut old = merge(None, &running, Some(&p), "session").unwrap();
        old["last_progress_at"] = json!(7);
        let backward = Preview {
            cursor_end: 2,
            output: "ab".into(),
            ..p.clone()
        };
        let next = merge(Some(&old), &running, Some(&backward), "session").unwrap();
        assert_eq!(next["output_cursor_end"], 4);
        assert_eq!(next["last_progress_at"], 7);
    }

    #[test]
    fn repeated_identical_lines_advance_output_but_not_meaningful_progress() {
        let running = fixture("running", 10, false);
        let p = Preview {
            operation_id: running.id.clone(),
            cursor_start: 0,
            cursor_end: 5,
            output: "tick\n".into(),
            truncated_before: false,
        };
        let mut old = merge(None, &running, Some(&p), "session").unwrap();
        old["last_progress_at"] = json!(7);
        let repeated = Preview {
            cursor_end: 10,
            output: "tick\ntick\n".into(),
            ..p
        };
        let next = merge(Some(&old), &running, Some(&repeated), "session").unwrap();
        assert_eq!(next["output_cursor_end"], 10);
        assert_eq!(next["last_progress_at"], 7);
    }

    #[test]
    fn partial_utf8_tail_uses_only_completed_characters() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("log");
        std::fs::write(&path, b"ok\xe4\xbd").unwrap();
        let (text, start, end, _) = tail(&path).unwrap();
        assert_eq!((text, start, end), ("ok".into(), 0, 2));
    }

    #[test]
    fn preview_tail_is_bounded_utf8_and_cursor_exact() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("terminal.log");
        let text = format!("{}你🧪end", "x".repeat(3000));
        std::fs::write(&path, &text).unwrap();
        let (output, start, end, gap) = tail(&path).unwrap();
        assert!(output.len() <= PREVIEW_BYTES);
        assert!(output.ends_with("你🧪end"));
        assert_eq!(end as usize, text.len());
        assert_eq!(end - start, output.len() as u64);
        assert!(gap);
        assert!(std::str::from_utf8(output.as_bytes()).is_ok());
    }
}

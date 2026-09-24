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

/// Disposable observation cache: no execution, authorization or receipt truth.
/// Idle ticks do not reread historical rows or reopen 24 completed log files.
#[derive(Default)]
pub(crate) struct SnapshotCache {
    pub terminals: Vec<Status>,
    pub previews: Vec<Preview>,
    revision: Option<u64>,
    refreshed_at: Option<tokio::time::Instant>,
}
impl SnapshotCache {
    pub async fn refresh(
        &mut self,
        store: &Store,
        dir: PathBuf,
        revision: u64,
        has_live: bool,
    ) -> Result<bool> {
        if self.revision == Some(revision)
            && !has_live
            && self
                .refreshed_at
                .is_some_and(|at| at.elapsed() < std::time::Duration::from_secs(30))
        {
            return Ok(false);
        }
        let snapshot = async {
            let terminals = collect(store).await?;
            let previews = previews(&terminals, dir).await?;
            Ok::<_, anyhow::Error>((terminals, previews))
        };
        let (terminals, previews) =
            tokio::time::timeout(std::time::Duration::from_millis(500), snapshot)
                .await
                .map_err(|_| anyhow::anyhow!("terminal_snapshot_timeout"))??;
        self.terminals = terminals;
        self.previews = previews;
        self.revision = Some(revision);
        self.refreshed_at = Some(tokio::time::Instant::now());
        Ok(true)
    }
}

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

pub(crate) const COLLECT_SQL: &str = "SELECT value FROM kv WHERE kind='terminal' ORDER BY (json_extract(value,'$.state') IN ('running','starting')) DESC,COALESCE(json_extract(value,'$.updated_at'),json_extract(value,'$.created_at'),0) DESC,key LIMIT 24";

pub(crate) async fn collect(s: &Store) -> Result<Vec<Status>> {
    // Terminal replication is live state, not an audit-log query. Keep active
    // terminals first, then the most recently changed terminal rows directly.
    let rows: Vec<(String,)> = sqlx::query_as(COLLECT_SQL).fetch_all(&s.pool).await?;
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
            // A clock rollback must not suppress a first authoritative terminal
            // outcome. Within the same phase, older snapshots remain rejected.
            || (old["updated_at"].as_i64().unwrap_or(0) > status.updated_at
                && (!ended(&terminal["state"]) || ended(&old["state"])))
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
    if let Some(old) = previous {
        let same = old["terminal"] == value["terminal"]
            && old["session"] == value["session"]
            && old["output_cursor_end"] == value["output_cursor_end"]
            && old["output_preview"] == value["output_preview"];
        let age = now() - old["reported_at"].as_i64().unwrap_or(0);
        // Immutable final snapshots need no rewrite. Live snapshots retain a
        // bounded freshness renewal, independent from meaningful progress.
        if same && (ended(&value["terminal"]["state"]) || (0..5).contains(&age)) {
            return None;
        }
    }
    Some(value)
}

pub(crate) async fn save_in_transaction(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
    // The caller's write transaction binds session renewal, job leases and all
    // observations to a single durable commit. Authorization stays in SQL.
    // Fetch authorization and previous snapshots in one bounded query rather
    // than running three statements for every terminal on every heartbeat.
    let rows:Vec<(String,Option<String>)>=sqlx::query_as(
        "SELECT j.id,p.value FROM json_each(?) s JOIN jobs j ON j.id=json_extract(s.value,'$.id') AND j.device=? AND json_extract(j.request,'$.tool')='terminal_exec' AND json_extract(j.request,'$.arguments.workspace_id')=json_extract(s.value,'$.workspace_id') JOIN kv o ON o.kind='online' AND o.key=j.device AND json_extract(o.value,'$.hello.session')=? LEFT JOIN kv p ON p.kind='terminal_observation' AND p.key=j.id")
        .bind(serde_json::to_string(statuses)?).bind(device).bind(session).fetch_all(&mut **tx).await?;
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
            .bind(crate::receipts::RETAIN_UNTIL_EXPLICIT_CLEANUP).bind(serde_json::to_string(&updates)?).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Project a durable submission plus the newest process observation into one
/// consistent response. This never rewrites the original job receipt.
pub(crate) fn project_result(result: &mut Value, observed: Value) {
    let latest = &observed["terminal"];
    if !latest.is_object() {
        return;
    }
    // A submission may already contain the final process result. An older
    // running heartbeat must not regress that result during delivery races.
    if ended(&result["terminal"]["state"]) && !ended(&latest["state"]) {
        return;
    }
    let mut complete_output_returned = result["terminal"]["output_complete"] == true
        && result["output"].is_string()
        && result["has_more"] == false
        && result["terminal"]["output_truncated"] != true
        && !result["terminal"]["output_error"].is_string();
    let mut terminal = result["terminal"].as_object().cloned().unwrap_or_default();
    for (key, value) in latest.as_object().expect("checked object") {
        terminal.insert(key.clone(), value.clone());
    }
    result["terminal"] = Value::Object(terminal);
    result["state"] = if observed["stale"] == true && !ended(&latest["state"]) {
        json!("outcome_unknown")
    } else {
        latest["state"].clone()
    };
    // Only promote a complete contiguous prefix. A tail preview never becomes
    // full stdout, and its cursor must not rewind the submission's byte cursor.
    if let (Some(output), Some(0), Some(end)) = (
        observed["output"].as_str(),
        observed["output_cursor_start"].as_u64(),
        observed["output_cursor_end"].as_u64(),
    ) && observed["output_gap"] == false
        && end == output.len() as u64
        && end >= result["cursor"].as_u64().unwrap_or(0)
    {
        result["output"] = json!(output);
        result["cursor"] = json!(end);
        result["raw_cursor_start"] = json!(0);
        result["has_more"] = json!(false);
        result["cursor_format"] = json!("sanitized_utf8_v1");
        complete_output_returned = latest["output_complete"] == true;
        // The preview is exact sanitized text, not the old compressed prefix.
        result["compression"] = json!({"profile":"identity","raw_bytes":end,"output_bytes":output.len(),"saved_tokens":0,"full_output_available":true});
        result["output_view"] = json!("full");
    }
    result["terminal_observation"] = observed;
    if complete_output_returned {
        result
            .as_object_mut()
            .expect("result object")
            .remove("result_omitted");
    }
    if ended(&result["terminal"]["state"]) && !complete_output_returned {
        // Older Agents may confirm exit without transmitting any log preview.
        // Captured output is not the same as output returned to the caller.
        result["result_omitted"] = json!(true);
    }
    let decision = crate::receipts::decision(result, None, result["operation_id"].as_str(), now());
    result["next_action"] = match decision["next_action"].as_str() {
        Some("observe_original" | "observe_original_and_reconcile") => json!("operation_get"),
        Some("read_original_output") => json!("terminal_read"),
        _ => decision["next_action"].clone(),
    };
}

pub(crate) fn observation_value(v: &Value, observed_at: i64) -> Value {
    json!({
        "terminal":v["terminal"],"reported_at":v["reported_at"],
        "stale":!ended(&v["terminal"]["state"]) && !(0..=45).contains(&(observed_at-v["reported_at"].as_i64().unwrap_or(0))),
        "last_progress_at":v["last_progress_at"],
        "progress_semantics":"terminal transition or changed final nonempty output line; not business acceptance",
        "output":v["output_preview"],"output_cursor_start":v["output_cursor_start"],
        "output_cursor_end":v["output_cursor_end"],"output_gap":v["output_truncated_before"],
        "output_next_action":"operation_get for live preview; terminal_read only for full history/recovery",
        "protocol":2
    })
}

pub(crate) async fn observed(g: &Gateway, id: &str) -> Result<Option<Value>> {
    Ok(g.store
        .get::<Value>("terminal_observation", id)
        .await?
        .map(|v| observation_value(&v, now())))
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
    fn response_projection_does_not_regress_a_final_submission() {
        let mut result = json!({"state":"exited","terminal":{"state":"exited","exit_code":0,"output_complete":true},"output":"final"});
        let original = result.clone();
        project_result(
            &mut result,
            json!({"terminal":{"state":"running","exit_code":null},"stale":false}),
        );
        assert_eq!(result, original);
    }

    #[test]
    fn completed_legacy_agent_without_preview_requires_original_output() {
        let mut result = json!({"state":"running","cursor":0,"output":"","has_more":false,
            "terminal":{"state":"running","output_complete":false}});
        project_result(
            &mut result,
            json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true},
            "stale":false,"output":null,"output_cursor_start":null,"output_cursor_end":null}),
        );
        assert_eq!(result["state"], "exited");
        assert_eq!(result["next_action"], "terminal_read");
        assert_eq!(result["result_omitted"], true);
        assert_eq!(
            crate::receipts::decision(&result, None, None, now())["evidence_complete"],
            false
        );
    }

    #[test]
    fn response_projection_keeps_tail_gaps_and_staleness_visible() {
        let mut result = json!({"state":"running","cursor":0,"output":""});
        project_result(
            &mut result,
            json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true},
            "output":"tail","output_cursor_start":100,"output_cursor_end":104,"output_gap":true,"stale":false}),
        );
        assert_eq!(result["state"], "exited");
        assert_eq!(result["cursor"], 0);
        assert_eq!(result["output"], "");
        assert_eq!(result["next_action"], "terminal_read");
        assert_eq!(
            crate::receipts::decision(&result, None, None, now())["evidence_complete"],
            false
        );
        let mut stale = json!({"state":"running"});
        project_result(
            &mut stale,
            json!({"terminal":{"state":"running","exit_code":null,"output_complete":false},"stale":true}),
        );
        assert_eq!(stale["state"], "outcome_unknown");
        assert_eq!(stale["next_action"], "operation_get");
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
        assert!(merge(Some(&old), &done, None, "session").is_none());
        assert_eq!(old["output_preview"], "end");
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
        old["reported_at"] = json!(now() - 6);
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
    fn clock_rollback_does_not_hide_final_exit() {
        let old = merge(None, &fixture("running", 100, false), None, "s").unwrap();
        let final_row = merge(Some(&old), &fixture("exited", 50, true), None, "s").unwrap();
        assert_eq!(final_row["terminal"]["state"], "exited");
        assert!(merge(Some(&final_row), &fixture("running", 110, false), None, "s").is_none());
    }

    #[test]
    fn identical_live_snapshot_is_coalesced_but_freshness_is_renewed() {
        let running = fixture("running", 10, false);
        let mut old = merge(None, &running, None, "s").unwrap();
        assert!(merge(Some(&old), &running, None, "s").is_none());
        old["reported_at"] = json!(now() - 6);
        let next = merge(Some(&old), &running, None, "s").unwrap();
        assert_eq!(next["last_progress_at"], old["last_progress_at"]);
        assert!(next["reported_at"].as_i64().unwrap() > old["reported_at"].as_i64().unwrap());
    }

    #[test]
    fn projected_output_replaces_obsolete_compression_statistics() {
        let mut value = json!({"terminal":{"state":"running","output_complete":false},"cursor":0,"output":"","compression":{"raw_bytes":0,"output_bytes":0}});
        project_result(
            &mut value,
            json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true},"stale":false,"output":"done","output_cursor_start":0,"output_cursor_end":4,"output_gap":false}),
        );
        assert_eq!(value["output"], "done");
        assert_eq!(value["compression"]["output_bytes"], 4);
        assert_eq!(value["compression"]["raw_bytes"], 4);
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

#[cfg(test)]
#[path = "terminal_sync_cost_tests.rs"]
mod cost_tests;

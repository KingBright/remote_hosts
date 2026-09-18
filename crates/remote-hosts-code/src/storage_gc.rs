//! Explicit workspace-scoped cleanup of finished state; active recovery and undelivered receipts are protected.
use crate::{
    AgentConfig,
    files::{self, Workspace},
    hash, now,
    store::Store,
    transfer_journal::Journal,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Clone)]
struct Candidate {
    kind: &'static str,
    id: String,
    path: PathBuf,
    bytes: u64,
}
impl Candidate {
    fn view(&self) -> Value {
        json!({"kind":self.kind,"id":self.id,"bytes":self.bytes})
    }
}

fn regular_size(path: &std::path::Path) -> Result<Option<u64>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            ensure!(
                !meta.file_type().is_symlink() && meta.is_file(),
                "gc refuses special or symlink state files"
            );
            Ok(Some(meta.len()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn collect(
    config: &AgentConfig,
    connection: &mut sqlx::SqliteConnection,
    ws: &Workspace,
    cutoff: i64,
    max: usize,
) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    let terminal_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT key FROM kv WHERE kind='terminal' AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.state') NOT IN ('running','starting') AND json_extract(value,'$.output_complete')=1 AND COALESCE(NULLIF(json_extract(value,'$.updated_at'),0),json_extract(value,'$.created_at'),0)>0 AND COALESCE(NULLIF(json_extract(value,'$.updated_at'),0),json_extract(value,'$.created_at'),0)<=? AND NOT EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=kv.key) AND NOT EXISTS(SELECT 1 FROM kv o WHERE o.kind='local_operation' AND o.key=kv.key AND COALESCE(json_extract(o.value,'$.state'),'unknown')<>'done') ORDER BY COALESCE(NULLIF(json_extract(value,'$.updated_at'),0),json_extract(value,'$.created_at'),0),key LIMIT ?")
        .bind(&ws.id).bind(cutoff).bind(max as i64).fetch_all(&mut *connection).await?;
    for (id,) in terminal_rows {
        if out.len() >= max {
            break;
        }
        let path = config.state_dir.join("terminals").join(format!("{id}.log"));
        let Some(bytes) = regular_size(&path)? else {
            // Missing output is still eligible for pruning its finished status row.
            out.push(Candidate {
                kind: "terminal",
                id,
                path,
                bytes: 0,
            });
            continue;
        };
        out.push(Candidate {
            kind: "terminal",
            id,
            path,
            bytes,
        });
    }
    if out.len() < max {
        let rows: Vec<(String,String)> = sqlx::query_as(
            "SELECT key,value FROM kv WHERE kind='transfer_local' AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.phase') IN ('completed','cancelled','failed','expired') AND json_extract(value,'$.updated_at')>0 AND json_extract(value,'$.updated_at')<=? AND NOT EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=kv.key) AND NOT EXISTS(SELECT 1 FROM kv o WHERE o.kind='local_operation' AND o.key=kv.key AND COALESCE(json_extract(o.value,'$.state'),'unknown')<>'done') ORDER BY json_extract(value,'$.updated_at'),key LIMIT ?")
            .bind(&ws.id).bind(cutoff).bind((max-out.len()) as i64).fetch_all(&mut *connection).await?;
        for (id, text) in rows {
            let journal: Journal = serde_json::from_str(&text)?;
            ensure!(
                journal.operation_id == id
                    && journal.workspace_id == ws.id
                    && journal.device_id == ws.device_id,
                "gc transfer identity conflict"
            );
            let path = journal.data_path(config);
            let bytes = regular_size(&path)?.unwrap_or(0);
            out.push(Candidate {
                kind: "transfer_checkpoint",
                id,
                path,
                bytes,
            });
        }
    }
    if out.len() < max {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT key FROM kv WHERE kind='local_operation' AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.tool')='code_apply_edits' AND json_extract(value,'$.state')='done' AND json_extract(value,'$.updated_at')>0 AND json_extract(value,'$.updated_at')<=? AND json_extract(value,'$.result.change_set.state')='completed' AND NOT EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=kv.key) ORDER BY json_extract(value,'$.updated_at'),key LIMIT ?")
            .bind(&ws.id).bind(cutoff).bind((max-out.len()) as i64).fetch_all(&mut *connection).await?;
        for (id,) in rows {
            let path = config.state_dir.join("edits").join(format!("{id}.json"));
            if let Some(bytes) = regular_size(&path)? {
                out.push(Candidate {
                    kind: "completed_change_journal",
                    id,
                    path,
                    bytes,
                });
            }
        }
    }
    Ok(out)
}

fn preview_identity(ws: &Workspace, older: usize, max: usize, candidates: &[Candidate]) -> String {
    hash(
        serde_json::to_vec(
            &json!({"workspace_id":ws.id,"older_than_seconds":older,"max_items":max,
        "items":candidates.iter().map(Candidate::view).collect::<Vec<_>>() }),
        )
        .expect("bounded preview"),
    )
}

pub(crate) async fn run(
    config: &AgentConfig,
    store: &Store,
    ws: &Workspace,
    args: &Value,
) -> Result<Value> {
    let action = files::text(args, "action")?;
    ensure!(matches!(action, "preview" | "apply"), "invalid gc action");
    let older = files::number(args, "older_than_seconds", 7 * 86400, 3600, 30 * 86400)?;
    let max = files::number(args, "max_items", 100, 1, 1000)?;
    let cutoff = now().saturating_sub(older as i64);
    // Freeze lifecycle/receipt state from selection through deletion, not just
    // during the final DELETE. A recovery transition cannot race file removal.
    let mut tx = store
        .pool
        .begin_with(if action == "apply" {
            "BEGIN IMMEDIATE"
        } else {
            "BEGIN"
        })
        .await?;
    let candidates = collect(config, &mut tx, ws, cutoff, max).await?;
    let preview_id = preview_identity(ws, older, max, &candidates);
    let bytes = candidates
        .iter()
        .fold(0u64, |sum, c| sum.saturating_add(c.bytes));
    let counts = {
        let mut map = serde_json::Map::new();
        for c in &candidates {
            let n = map.get(c.kind).and_then(Value::as_u64).unwrap_or(0);
            map.insert(c.kind.into(), json!(n + 1));
        }
        Value::Object(map)
    };
    if action == "preview" {
        tx.commit().await?;
        return Ok(
            json!({"state":"preview","preview_id":preview_id,"workspace_id":ws.id,
            "policy":{"older_than_seconds":older,"max_items":max},"candidate_count":candidates.len(),
            "candidate_bytes":bytes,"counts":counts,"items":candidates.iter().map(Candidate::view).collect::<Vec<_>>(),
            "protected":["running_or_starting_terminals","unfinished_terminal_output","active_or_resumable_transfers","partial_change_sets","undelivered_receipts","legacy_records_without_lifecycle_timestamp"],
            "gc_protocol":1}),
        );
    }
    let supplied = args
        .get("preview_id")
        .and_then(Value::as_str)
        .context("invalid_arguments: workspace_gc apply requires preview_id")?;
    ensure!(
        supplied == preview_id,
        "gc_preview_changed: run preview again before apply"
    );
    let mut removed = Vec::new();
    let mut freed = 0u64;
    for candidate in candidates {
        match std::fs::remove_file(&candidate.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("gc state-file removal failed"),
        }
        match candidate.kind {
            "terminal" => {
                sqlx::query("DELETE FROM kv WHERE kind='terminal' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.state') NOT IN ('running','starting')")
                    .bind(&candidate.id).bind(&ws.id).execute(&mut *tx).await?;
                sqlx::query("UPDATE kv SET value=json_set(json_remove(value,'$.result'),'$.gateway_accepted',json('true')) WHERE kind='local_operation' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.tool')='terminal_exec' AND json_extract(value,'$.state')='done'")
                    .bind(&candidate.id).bind(&ws.id).execute(&mut *tx).await?;
            }
            "transfer_checkpoint" => {
                sqlx::query("DELETE FROM kv WHERE kind='transfer_local' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.phase') IN ('completed','cancelled','failed','expired')")
                    .bind(&candidate.id).bind(&ws.id).execute(&mut *tx).await?;
            }
            "completed_change_journal" => {
                sqlx::query("UPDATE kv SET value=json_set(json_remove(value,'$.result'),'$.gateway_accepted',json('true')) WHERE kind='local_operation' AND key=? AND json_extract(value,'$.workspace_id')=? AND json_extract(value,'$.tool')='code_apply_edits' AND json_extract(value,'$.state')='done' AND json_extract(value,'$.result.change_set.state')='completed'")
                    .bind(&candidate.id).bind(&ws.id).execute(&mut *tx).await?;
            }
            _ => unreachable!(),
        }
        freed = freed.saturating_add(candidate.bytes);
        removed.push(candidate.view());
    }
    tx.commit().await?;
    Ok(
        json!({"state":"completed","preview_id":preview_id,"workspace_id":ws.id,"removed_count":removed.len(),
        "freed_bytes":freed,"removed":removed,"active_recovery_records_preserved":true,"gc_protocol":1}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hash, random};
    #[tokio::test]
    async fn preview_is_bound_and_apply_preserves_active_and_partial_state() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: format!("{}:{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4()),
            device_id: uuid::Uuid::new_v4().to_string(),
            root: root.path().canonicalize().unwrap(),
        };
        let config = AgentConfig {
            gateway_url: "https://example.test".into(),
            device_id: ws.device_id.clone(),
            device_token: random(),
            state_dir: state.path().to_path_buf(),
            roots: vec![ws.root.clone()],
            allow_write: true,
            allow_exec: true,
            shell: "/bin/sh".into(),
        };
        let store = Store::open(state.path()).await.unwrap();
        sqlx::query("CREATE TABLE receipt_outbox (id TEXT PRIMARY KEY)")
            .execute(&store.pool)
            .await
            .unwrap();
        std::fs::create_dir_all(state.path().join("terminals")).unwrap();
        let old = now() - 90000;
        for (id, status) in [("done", "exited"), ("live", "running")] {
            let key = uuid::Uuid::new_v4().to_string();
            store.put("terminal",&key,&json!({"id":key,"workspace_id":ws.id,"state":status,"exit_code":0,"output_truncated":false,"created_at":old,"pty":false,"log_format":1,"output_complete":status=="exited"}),i64::MAX).await.unwrap();
            store.put("local_operation",&key,&json!({"fingerprint":hash(id),"state":if status=="exited"{"done"}else{"running"},"result":{},"resumable":false,"workspace_id":ws.id,"tool":"terminal_exec","updated_at":old}),i64::MAX).await.unwrap();
            std::fs::write(
                state.path().join("terminals").join(format!("{key}.log")),
                id,
            )
            .unwrap();
        }
        let legacy = uuid::Uuid::new_v4().to_string();
        store.put("terminal",&legacy,&json!({"id":legacy,"workspace_id":ws.id,"state":"exited","exit_code":0,"output_truncated":false,"created_at":old,"pty":false,"log_format":1,"output_complete":true}),i64::MAX).await.unwrap();
        store.put("local_operation",&legacy,&json!({"fingerprint":hash("legacy"),"state":"done","result":{},"resumable":false,"workspace_id":ws.id,"tool":"terminal_exec"}),i64::MAX).await.unwrap();
        std::fs::write(
            state.path().join("terminals").join(format!("{legacy}.log")),
            "legacy",
        )
        .unwrap();
        let mut protected = Vec::new();
        for (name, status, complete, created, updated, op_state, pending) in [
            (
                "pending",
                "exited",
                true,
                Some(old),
                Some(old),
                "done",
                true,
            ),
            (
                "streaming",
                "cancelled",
                false,
                Some(old),
                Some(old),
                "done",
                false,
            ),
            ("timeless", "exited", true, None, None, "done", false),
            (
                "recent",
                "exited",
                true,
                Some(old),
                Some(now()),
                "done",
                false,
            ),
            (
                "recovery",
                "exited",
                true,
                Some(old),
                Some(old),
                "unknown",
                false,
            ),
        ] {
            let key = uuid::Uuid::new_v4().to_string();
            store.put("terminal", &key, &json!({"id":key,"workspace_id":ws.id,"state":status,"created_at":created,"updated_at":updated,"output_complete":complete}), i64::MAX).await.unwrap();
            store.put("local_operation", &key, &json!({"workspace_id":ws.id,"tool":"terminal_exec","state":op_state,"updated_at":old}), i64::MAX).await.unwrap();
            let path = state.path().join("terminals").join(format!("{key}.log"));
            std::fs::write(&path, name).unwrap();
            if pending {
                sqlx::query("INSERT INTO receipt_outbox(id) VALUES(?)")
                    .bind(&key)
                    .execute(&store.pool)
                    .await
                    .unwrap();
            }
            protected.push((key, path));
        }
        std::fs::create_dir_all(state.path().join("edits")).unwrap();
        for (change_state, pending) in [("completed", true), ("partial", false)] {
            let key = uuid::Uuid::new_v4().to_string();
            store.put("local_operation", &key, &json!({"workspace_id":ws.id,"tool":"code_apply_edits","state":"done","updated_at":old,"result":{"change_set":{"state":change_state}}}), i64::MAX).await.unwrap();
            let path = state.path().join("edits").join(format!("{key}.json"));
            std::fs::write(&path, "{}").unwrap();
            if pending {
                sqlx::query("INSERT INTO receipt_outbox(id) VALUES(?)")
                    .bind(&key)
                    .execute(&store.pool)
                    .await
                    .unwrap();
            }
            protected.push((key, path));
        }
        let preview = run(
            &config,
            &store,
            &ws,
            &json!({"action":"preview","older_than_seconds":3600,"max_items":100}),
        )
        .await
        .unwrap();
        assert_eq!(preview["candidate_count"], 2);
        assert_eq!(preview["counts"]["terminal"], 2);
        assert!(run(&config,&store,&ws,&json!({"action":"apply","older_than_seconds":3600,"max_items":100,"preview_id":"bad"})).await.is_err());
        let applied=run(&config,&store,&ws,&json!({"action":"apply","older_than_seconds":3600,"max_items":100,"preview_id":preview["preview_id"]})).await.unwrap();
        assert_eq!(applied["removed_count"], 2);
        let (terminal_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='terminal'")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(
            terminal_count, 6,
            "running, unfinished, unacknowledged, recent and unknown-age terminals remain protected"
        );
        let (ops,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='local_operation'")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(
            ops,
            3 + protected.len() as i64,
            "active recovery and current-runtime deduplication receipts are retained"
        );
        let (compact,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='local_operation' AND json_extract(value,'$.gateway_accepted')=1 AND json_extract(value,'$.result') IS NULL").fetch_one(&store.pool).await.unwrap();
        assert_eq!(
            compact, 2,
            "collected terminals retain only compact deduplication receipts"
        );
        for (key, path) in protected {
            assert!(path.exists());
            assert!(
                store
                    .get::<Value>("local_operation", &key)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let (receipts,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM receipt_outbox")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(receipts, 2);
    }
}

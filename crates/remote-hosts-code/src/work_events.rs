//! Durable, bounded workspace events. Triggers commit with the original state update.
use crate::{files::Workspace, hash, store::Store};
use anyhow::{Result, ensure};
use serde_json::{Value, json};

pub(crate) async fn install(store: &Store) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS work_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT NOT NULL,kind TEXT NOT NULL,entity TEXT NOT NULL,state TEXT,at INTEGER NOT NULL,detail TEXT NOT NULL)").execute(&store.pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS work_events_scoped ON work_events(workspace,seq)")
        .execute(&store.pool)
        .await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS work_event_floor(workspace TEXT PRIMARY KEY,seq INTEGER NOT NULL)").execute(&store.pool).await?;
    // Only state transitions, never polling requests, command text, output, or credentials.
    for statement in [
        r#"CREATE TRIGGER IF NOT EXISTS work_event_insert AFTER INSERT ON kv WHEN NEW.kind IN ('terminal','transfer_local','local_operation') AND json_valid(NEW.value) AND json_extract(NEW.value,'$.workspace_id') IS NOT NULL AND (NEW.kind<>'local_operation' OR json_extract(NEW.value,'$.tool') IN ('code_apply_edits','files_sync','terminal_exec','file_upload','file_download'))  BEGIN INSERT INTO work_events(workspace,kind,entity,state,at,detail) VALUES(json_extract(NEW.value,'$.workspace_id'),NEW.kind,NEW.key,COALESCE(json_extract(NEW.value,'$.state'),json_extract(NEW.value,'$.phase')),unixepoch(),json_object('tool',json_extract(NEW.value,'$.tool'),'exit_code',json_extract(NEW.value,'$.exit_code'),'output_complete',json_extract(NEW.value,'$.output_complete'),'error',json_extract(NEW.value,'$.result.error'),'journal_id',json_extract(NEW.value,'$.result.journal_id'))); INSERT INTO work_event_floor(workspace,seq) SELECT json_extract(NEW.value,'$.workspace_id'),COALESCE(MAX(seq),0) FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') AND seq NOT IN (SELECT seq FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') ORDER BY seq DESC LIMIT 4096) ON CONFLICT(workspace) DO UPDATE SET seq=MAX(work_event_floor.seq,excluded.seq); DELETE FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') AND seq<=(SELECT seq FROM work_event_floor WHERE workspace=json_extract(NEW.value,'$.workspace_id')); END"#,
        r#"CREATE TRIGGER IF NOT EXISTS work_event_update AFTER UPDATE ON kv WHEN NEW.kind IN ('terminal','transfer_local','local_operation') AND json_valid(NEW.value) AND json_extract(NEW.value,'$.workspace_id') IS NOT NULL AND (NEW.kind<>'local_operation' OR json_extract(NEW.value,'$.tool') IN ('code_apply_edits','files_sync','terminal_exec','file_upload','file_download')) AND (COALESCE(json_extract(OLD.value,'$.state'),json_extract(OLD.value,'$.phase'),'')<>COALESCE(json_extract(NEW.value,'$.state'),json_extract(NEW.value,'$.phase'),'') OR COALESCE(json_extract(OLD.value,'$.output_complete'),0)<>COALESCE(json_extract(NEW.value,'$.output_complete'),0)) BEGIN INSERT INTO work_events(workspace,kind,entity,state,at,detail) VALUES(json_extract(NEW.value,'$.workspace_id'),NEW.kind,NEW.key,COALESCE(json_extract(NEW.value,'$.state'),json_extract(NEW.value,'$.phase')),unixepoch(),json_object('tool',json_extract(NEW.value,'$.tool'),'exit_code',json_extract(NEW.value,'$.exit_code'),'output_complete',json_extract(NEW.value,'$.output_complete'),'error',json_extract(NEW.value,'$.result.error'),'journal_id',json_extract(NEW.value,'$.result.journal_id'))); INSERT INTO work_event_floor(workspace,seq) SELECT json_extract(NEW.value,'$.workspace_id'),COALESCE(MAX(seq),0) FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') AND seq NOT IN (SELECT seq FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') ORDER BY seq DESC LIMIT 4096) ON CONFLICT(workspace) DO UPDATE SET seq=MAX(work_event_floor.seq,excluded.seq); DELETE FROM work_events WHERE workspace=json_extract(NEW.value,'$.workspace_id') AND seq<=(SELECT seq FROM work_event_floor WHERE workspace=json_extract(NEW.value,'$.workspace_id')); END"#,
    ] {
        sqlx::query(statement).execute(&store.pool).await?;
    }
    Ok(())
}
pub(crate) async fn read(
    store: &Store,
    ws: &Workspace,
    after: Option<&str>,
    limit: usize,
) -> Result<Value> {
    ensure!((1..=100).contains(&limit), "invalid event limit");
    let binding = hash(format!("{}:{}", ws.device_id, ws.id));
    let after = after
        .map(|cursor| -> Result<i64> {
            ensure!(cursor.len() <= 100, "invalid event cursor");
            let (scope, n) = cursor
                .split_once('.')
                .ok_or_else(|| anyhow::anyhow!("invalid event cursor"))?;
            ensure!(scope == binding, "event cursor workspace mismatch");
            let n = n.parse::<i64>()?;
            ensure!(n >= 0, "invalid event cursor");
            Ok(n)
        })
        .transpose()?;
    let mut tx = store.pool.begin().await?;
    let (floor,): (i64,) =
        sqlx::query_as("SELECT COALESCE((SELECT seq FROM work_event_floor WHERE workspace=?),0)")
            .bind(&ws.id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(n) = after {
        ensure!(n >= floor, "cursor_expired: refresh workspace snapshot");
    }
    let (latest,): (i64,) =
        sqlx::query_as("SELECT COALESCE(MAX(seq),0) FROM work_events WHERE workspace=?")
            .bind(&ws.id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(n) = after {
        ensure!(n <= latest.max(floor), "invalid future event cursor");
    }
    let mut rows: Vec<(i64, String, String, Option<String>, i64, String)> = if let Some(n) = after {
        sqlx::query_as("SELECT seq,kind,entity,state,at,detail FROM work_events WHERE workspace=? AND seq>? ORDER BY seq LIMIT ?").bind(&ws.id).bind(n).bind((limit+1)as i64).fetch_all(&mut *tx).await?
    } else {
        let mut rows:Vec<(i64,String,String,Option<String>,i64,String)>=sqlx::query_as("SELECT seq,kind,entity,state,at,detail FROM work_events WHERE workspace=? ORDER BY seq DESC LIMIT ?").bind(&ws.id).bind(limit as i64).fetch_all(&mut *tx).await?;
        rows.reverse();
        rows
    };
    tx.commit().await?;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let next = rows
        .last()
        .map(|r| r.0)
        .unwrap_or(after.unwrap_or(latest.max(floor)));
    let events:Vec<Value>=rows.into_iter().map(|(seq,kind,id,state,at,detail)|->Result<Value>{Ok(json!({"seq":seq,"kind":kind,"entity_id":id,"state":state,"at":at,"detail":serde_json::from_str::<Value>(&detail)?}))}).collect::<Result<_>>()?;
    Ok(
        json!({"items":events,"cursor":format!("{binding}.{next}"),"has_more":more,"history_floor":floor,"latest_seq":latest,"retention":"latest 4096 state transitions per workspace","initial_view":if after.is_none(){"recent tail; use returned cursor for subsequent transitions"}else{"forward replay"},"protocol":1}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn events_are_atomic_scoped_and_survive_reopen() {
        let d = tempfile::tempdir().unwrap();
        let s = Store::open(d.path()).await.unwrap();
        let ws = Workspace {
            id: "one".into(),
            device_id: "d".into(),
            root: d.path().to_path_buf(),
        };
        let first = read(&s, &ws, None, 10).await.unwrap();
        s.put(
            "terminal",
            "t",
            &json!({"workspace_id":"one","state":"running","command":"secret"}),
            i64::MAX,
        )
        .await
        .unwrap();
        s.put("terminal","t",&json!({"workspace_id":"one","state":"exited","exit_code":0,"output_complete":true,"command":"secret"}),i64::MAX).await.unwrap();
        s.put(
            "terminal",
            "other",
            &json!({"workspace_id":"two","state":"running"}),
            i64::MAX,
        )
        .await
        .unwrap();
        let v = read(&s, &ws, first["cursor"].as_str(), 1).await.unwrap();
        assert!(v["has_more"].as_bool().unwrap());
        assert!(!v.to_string().contains("secret"));
        let reopened = Store::open(d.path()).await.unwrap();
        let v2 = read(&reopened, &ws, v["cursor"].as_str(), 10)
            .await
            .unwrap();
        assert_eq!(v2["items"].as_array().unwrap().len(), 1);
        assert_eq!(v2["items"][0]["state"], "exited");
        let mut wrong = ws;
        wrong.id = "two".into();
        assert!(read(&s, &wrong, v["cursor"].as_str(), 10).await.is_err());
        let mut tx = s.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO kv VALUES('terminal','rolled','{\"workspace_id\":\"two\",\"state\":\"exited\"}',9999999999)").execute(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        let v = read(&s, &wrong, None, 10).await.unwrap();
        assert_eq!(v["items"].as_array().unwrap().len(), 1);
    }
}

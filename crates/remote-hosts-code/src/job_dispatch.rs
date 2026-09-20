//! Empty polls are reads; actual delivery and its timing commit atomically.
//! The read is an optimization only. All authorization/lease/filter predicates
//! are re-evaluated by the write, so racing polls cannot dispatch the same job.
use crate::{now_ms, store::Store};
use anyhow::{Context, Result};
use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;
use sqlx::{Row, Sqlite, query::Query, sqlite::SqliteArguments};
// Keep one compile-time SQL literal for both paths. Values always use bindings;
// no runtime SQL concatenation or unchecked SQL-safety assertion is needed.
macro_rules! candidate_sql {
    () => { r#"SELECT id FROM (
    SELECT id,device,request,updated FROM jobs WHERE device=? AND state='queued'
    UNION ALL
    SELECT id,device,request,updated FROM jobs WHERE device=? AND state='dispatched' AND updated<?
) jobs
WHERE id NOT IN (SELECT value FROM json_each(?))
AND (json_extract(request,'$.tool') IN ('code_read','code_list','code_search','code_symbols','code_diff','workspace_context','terminal_read','terminal_cancel')
    OR EXISTS(SELECT 1 FROM kv c WHERE c.kind='transfer_control' AND c.key=jobs.id AND json_extract(c.value,'$.cancel_requested')=1)
    OR NOT EXISTS(SELECT 1 FROM kv WHERE kind='device_drain' AND key=jobs.device AND expires>unixepoch()))
AND (CASE WHEN json_extract(request,'$.tool') IN ('file_upload','file_download') THEN 'transfer'
    WHEN json_extract(request,'$.tool') IN ('terminal_read','terminal_cancel','workspace_gc') THEN 'control'
    WHEN json_extract(request,'$.tool') IN ('terminal_exec','terminal_input') THEN 'terminal'
    WHEN json_extract(request,'$.tool') IN ('workspace_open','code_apply_edits','change_resume','files_sync') THEN 'write'
    ELSE 'read' END) IN (SELECT value FROM json_each(?))
AND (json_extract(request,'$.tool') NOT IN ('code_apply_edits','change_resume','files_sync')
    OR (?=0 AND COALESCE(json_extract(request,'$.arguments.workspace_id'),'') NOT IN (SELECT value FROM json_each(?))))
AND (json_extract(request,'$.tool')<>'terminal_input'
    OR COALESCE(json_extract(request,'$.arguments.terminal_id'),'') NOT IN (SELECT value FROM json_each(?)))
AND EXISTS (SELECT 1 FROM kv WHERE kind='online' AND key=? AND json_extract(value,'$.hello.session')=?)
ORDER BY updated,id LIMIT 1"# };
}
const CANDIDATE: &str = candidate_sql!();
const CLAIM: &str = concat!(
    "UPDATE jobs SET state='dispatched',updated=? WHERE id=(",
    candidate_sql!(),
    ") RETURNING id,request"
);

pub(crate) struct Selection<'a> {
    pub device: &'a str,
    pub session: &'a str,
    pub active: &'a str,
    pub lanes: &'a str,
    pub defer_writes: bool,
    pub write_workspaces: &'a str,
    pub terminal_inputs: &'a str,
}
impl Selection<'_> {
    fn bind<'q>(
        &'q self,
        query: Query<'q, Sqlite, SqliteArguments>,
        at: i64,
    ) -> Query<'q, Sqlite, SqliteArguments> {
        query
            .bind(self.device)
            .bind(self.device)
            .bind(at - 30)
            .bind(self.active)
            .bind(self.lanes)
            .bind(self.defer_writes)
            .bind(self.write_workspaces)
            .bind(self.terminal_inputs)
            .bind(self.device)
            .bind(self.session)
    }
}

pub(crate) async fn claim(
    store: &Store,
    selection: &Selection<'_>,
    at: i64,
) -> Result<Option<(String, String)>> {
    // An UPDATE with zero affected rows still needs SQLite's writer lock. Most
    // long-poll wakeups are empty, so they must not occupy the writer/connection
    // queue just to find that there is nothing to do.
    let eligible = selection
        .bind(sqlx::query(CANDIDATE), at)
        .fetch_optional(&store.pool)
        .await
        .context("poll_queue_read")?;
    if eligible.is_none() {
        return Ok(None);
    }
    let mut tx = store
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("poll_claim_begin")?;
    let row = selection
        .bind(sqlx::query(CLAIM).bind(at), at)
        .fetch_optional(&mut *tx)
        .await
        .context("poll_claim_update")?;
    let result = if let Some(row) = row {
        let id: String = row.try_get("id")?;
        let request: String = row.try_get("request")?;
        // Do not leave a dispatched job behind if the timing write fails. The
        // legacy path committed the claim before this second fallible write.
        sqlx::query(
            "UPDATE operation_timing SET dispatched_ms=COALESCE(dispatched_ms,?) WHERE id=?",
        )
        .bind(now_ms())
        .bind(&id)
        .execute(&mut *tx)
        .await
        .context("poll_claim_timing")?;
        Some((id, request))
    } else {
        None
    };
    tx.commit().await.context("poll_claim_commit")?;
    Ok(result)
}

/// Log only stable error classes and numeric SQLite codes, never SQL, URLs,
/// response bodies or credentials. A retry here observes the queue, not a command.
pub(crate) fn storage_error(stage: &'static str, error: &anyhow::Error) -> Response {
    let sql = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>());
    let code = sql
        .and_then(sqlx::Error::as_database_error)
        .and_then(|db| db.code())
        .and_then(|code| code.parse::<u32>().ok());
    let busy = matches!(sql, Some(sqlx::Error::PoolTimedOut))
        || code.is_some_and(|n| matches!(n & 255, 5 | 6));
    let category = if busy {
        "storage_busy"
    } else if code.is_some_and(|n| n & 255 == 13) {
        "storage_full"
    } else if code.is_some_and(|n| n & 255 == 10) {
        "storage_io"
    } else {
        "storage_failure"
    };
    tracing::warn!(
        stage,
        category,
        sqlite_code = code,
        "device poll storage unavailable; no command replay"
    );
    let status = if busy {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let mut response = (
        status,
        Json(
            json!({"error":"device_poll_unavailable","category":category,"stage":stage,
        "sqlite_code":code,"observed_at":crate::now(),"command_replayed":false,
        "execution_state":"unknown","retry_policy":"retry_poll_only_never_replay_commands",
        "retry_after_seconds":if busy{Some(2)}else{None}}),
        ),
    )
        .into_response();
    if busy {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "2".parse().expect("static header"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn selection() -> Selection<'static> {
        Selection {
            device: "device",
            session: "session",
            active: "[]",
            lanes: r#"["read","write","terminal","control","transfer"]"#,
            defer_writes: false,
            write_workspaces: "[]",
            terminal_inputs: "[]",
        }
    }
    async fn fixture() -> (tempfile::TempDir, Store) {
        let d = tempfile::tempdir().unwrap();
        let s = Store::open(d.path()).await.unwrap();
        s.install_gateway_schema().await.unwrap();
        s.put(
            "online",
            "device",
            &json!({"hello":{"session":"session"},"last_seen":crate::now()}),
            i64::MAX,
        )
        .await
        .unwrap();
        // Open connections before fault injection, so the assertion measures
        // dequeue locking rather than connection-initialization PRAGMAs.
        let mut connections = Vec::new();
        for _ in 0..4 {
            connections.push(s.pool.acquire().await.unwrap());
        }
        drop(connections);
        (d, s)
    }
    async fn add(s: &Store, tool: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let request = json!({"id":id,"device_id":"device","owner":"owner","tool":tool,"arguments":{"workspace_id":"device:workspace","terminal_id":"terminal"}});
        sqlx::query("INSERT INTO jobs VALUES(?,'device',?,?,?,NULL,'queued',?)")
            .bind(&id)
            .bind(&id)
            .bind("fingerprint")
            .bind(request.to_string())
            .bind(crate::now())
            .execute(&s.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO operation_timing VALUES(?,?,NULL,NULL)")
            .bind(&id)
            .bind(now_ms())
            .execute(&s.pool)
            .await
            .unwrap();
        id
    }
    #[tokio::test]
    async fn empty_queue_does_not_wait_for_an_existing_writer() {
        let (_d, s) = fixture().await;
        let tx = s.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let read = tokio::time::timeout(
            Duration::from_secs(1),
            claim(&s, &selection(), crate::now()),
        )
        .await;
        tx.rollback().await.unwrap();
        assert!(
            read.expect("empty poll tried to acquire a writer lock")
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn occupied_other_lane_does_not_acquire_writer_lock() {
        let (_d, s) = fixture().await;
        add(&s, "file_upload").await;
        let tx = s.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let mut filtered = selection();
        filtered.lanes = r#"["read"]"#;
        let read =
            tokio::time::timeout(Duration::from_secs(1), claim(&s, &filtered, crate::now())).await;
        tx.rollback().await.unwrap();
        assert!(read.unwrap().unwrap().is_none());
    }
    #[tokio::test]
    async fn concurrent_polls_claim_one_job_once() {
        let (_d, s) = fixture().await;
        let id = add(&s, "code_read").await;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let s = s.clone();
            tasks.spawn(async move { claim(&s, &selection(), crate::now()).await.unwrap() });
        }
        let mut claimed = Vec::new();
        while let Some(v) = tasks.join_next().await {
            if let Some((id, _)) = v.unwrap() {
                claimed.push(id);
            }
        }
        assert_eq!(claimed, vec![id.clone()]);
        let at: Option<i64> =
            sqlx::query_scalar("SELECT dispatched_ms FROM operation_timing WHERE id=?")
                .bind(id)
                .fetch_one(&s.pool)
                .await
                .unwrap();
        assert!(at.is_some());
    }
    #[tokio::test]
    async fn failed_timing_write_rolls_back_delivery() {
        let (_d, s) = fixture().await;
        let id = add(&s, "terminal_exec").await;
        sqlx::query("CREATE TRIGGER reject_timing BEFORE UPDATE ON operation_timing BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&s.pool).await.unwrap();
        assert!(claim(&s, &selection(), crate::now()).await.is_err());
        let state: String = sqlx::query_scalar("SELECT state FROM jobs WHERE id=?")
            .bind(id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
        assert_eq!(state, "queued");
    }
    #[tokio::test]
    async fn filters_session_and_drain_still_prevent_execution() {
        let (_d, s) = fixture().await;
        let id = add(&s, "code_apply_edits").await;
        let mut f = selection();
        f.session = "foreign";
        assert!(claim(&s, &f, crate::now()).await.unwrap().is_none());
        f = selection();
        f.defer_writes = true;
        assert!(claim(&s, &f, crate::now()).await.unwrap().is_none());
        f = selection();
        f.write_workspaces = r#"["device:workspace"]"#;
        assert!(claim(&s, &f, crate::now()).await.unwrap().is_none());
        s.put("device_drain", "device", &json!({}), i64::MAX)
            .await
            .unwrap();
        assert!(
            claim(&s, &selection(), crate::now())
                .await
                .unwrap()
                .is_none()
        );
        s.take::<serde_json::Value>("device_drain", "device")
            .await
            .unwrap();
        assert_eq!(
            claim(&s, &selection(), crate::now())
                .await
                .unwrap()
                .unwrap()
                .0,
            id
        );
    }
    #[tokio::test]
    async fn active_handles_and_busy_terminal_inputs_are_not_claimed() {
        let (_d, s) = fixture().await;
        let id = add(&s, "terminal_input").await;
        let mut f = selection();
        f.terminal_inputs = r#"["terminal"]"#;
        assert!(claim(&s, &f, crate::now()).await.unwrap().is_none());
        let active = serde_json::to_string(&vec![&id]).unwrap();
        f = selection();
        f.active = &active;
        assert!(claim(&s, &f, crate::now()).await.unwrap().is_none());
        assert_eq!(
            claim(&s, &selection(), crate::now())
                .await
                .unwrap()
                .unwrap()
                .0,
            id
        );
    }
    #[tokio::test]
    async fn busy_response_is_typed_and_does_not_disclose_raw_errors() {
        let response = storage_error("poll_claim", &anyhow::Error::new(sqlx::Error::PoolTimedOut));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "2");
        let response = storage_error(
            "heartbeat",
            &anyhow::anyhow!("sensitive SQL token=do-not-disclose"),
        );
        let body = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains("do-not-disclose"));
        assert!(body.contains("storage_failure"));
    }
}

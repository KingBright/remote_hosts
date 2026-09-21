//! One durable commit for result, timing and reservation release.
//! Duplicate delivery is a read. An unacknowledged packet may be retried, never
//! the remote command; every fallible bookkeeping write participates in rollback.
use crate::{now, now_ms, store::Store};
use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

type Saved = (String, Option<String>, String, i64);
const SNAPSHOT: &str = "SELECT j.state,j.result,COALESCE(json_extract(j.request,'$.tool'),''),COALESCE(json_extract(c.value,'$.revision'),0) FROM jobs j LEFT JOIN kv c ON c.kind='transfer_control' AND c.key=j.id WHERE j.id=? AND j.device=?";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Accepted { duplicate: bool },
    Obsolete,
    NotFound,
    Conflict,
}

fn inspect(saved: Option<&Saved>, value: &Value, revision: Option<i64>) -> Result<Option<Outcome>> {
    let Some((state, previous, tool, current)) = saved else {
        return Ok(Some(Outcome::NotFound));
    };
    if revision.is_some_and(|v| v < *current) {
        return Ok(Some(Outcome::Obsolete));
    }
    if revision.unwrap_or(0) != *current {
        return Ok(Some(Outcome::Conflict));
    }
    if let Some(previous) = previous {
        let previous: Value =
            serde_json::from_str(previous).context("receipt_saved_result_invalid")?;
        return Ok(Some(if state == "done" && previous == *value {
            Outcome::Accepted { duplicate: true }
        } else {
            Outcome::Conflict
        }));
    }
    let eligible = state == "dispatched"
        || (state == "queued"
            && revision.is_some()
            && matches!(tool.as_str(), "file_upload" | "file_download"));
    Ok(if eligible {
        None
    } else {
        Some(Outcome::Conflict)
    })
}

pub(crate) async fn commit(
    store: &Store,
    device: &str,
    id: &str,
    value: &Value,
) -> Result<Outcome> {
    let revision = value.get("transfer_revision").and_then(Value::as_i64);
    ensure!(
        value.get("transfer_revision").is_none() || revision.is_some_and(|v| v >= 0),
        "invalid_receipt_revision"
    );
    let encoded = serde_json::to_string(value)?;
    // Do not acquire a writer just to acknowledge an already committed packet.
    // Ownership and generation are included in both the read and transaction.
    let before: Option<Saved> = sqlx::query_as(SNAPSHOT)
        .bind(id)
        .bind(device)
        .fetch_optional(&store.pool)
        .await
        .context("receipt_preflight_read")?;
    if let Some(outcome) = inspect(before.as_ref(), value, revision)? {
        return Ok(outcome);
    }
    let mut tx = store
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("receipt_begin")?;
    let saved: Option<Saved> = sqlx::query_as(SNAPSHOT)
        .bind(id)
        .bind(device)
        .fetch_optional(&mut *tx)
        .await
        .context("receipt_authoritative_read")?;
    if let Some(outcome) = inspect(saved.as_ref(), value, revision)? {
        tx.rollback().await?;
        return Ok(outcome);
    }
    let changed = sqlx::query("UPDATE jobs SET result=?,state='done',updated=? WHERE id=? AND device=? AND result IS NULL")
        .bind(encoded).bind(now()).bind(id).bind(device).execute(&mut *tx).await.context("receipt_result_write")?;
    ensure!(changed.rows_affected() == 1, "receipt_identity_changed");
    // operation_id has a UNIQUE index. Do not depend on a second KV mapping
    // lookup whose failed write could leave an otherwise completed job guarded.
    if value["error"] == "outcome_unknown" {
        sqlx::query(
            "UPDATE semantic_guards SET state='outcome_unknown',updated=? WHERE operation_id=?",
        )
        .bind(now())
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("receipt_guard_unknown")?;
    } else {
        sqlx::query("DELETE FROM semantic_guards WHERE operation_id=?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .context("receipt_guard_release")?;
        sqlx::query("DELETE FROM kv WHERE kind='operation_semantic' AND key=?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .context("receipt_mapping_release")?;
    }
    sqlx::query("UPDATE operation_timing SET result_ms=COALESCE(result_ms,?) WHERE id=?")
        .bind(now_ms())
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("receipt_timing_write")?;
    sqlx::query("DELETE FROM kv WHERE kind='file_source' AND key=?")
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("receipt_source_release")?;
    tx.commit().await.context("receipt_commit")?;
    Ok(Outcome::Accepted { duplicate: false })
}

pub(crate) fn storage_response(id: &str, error: &anyhow::Error) -> Response {
    let sql = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>());
    let code = sql
        .and_then(sqlx::Error::as_database_error)
        .and_then(|db| db.code())
        .and_then(|v| v.parse::<u32>().ok());
    let busy = matches!(sql, Some(sqlx::Error::PoolTimedOut))
        || code.is_some_and(|v| matches!(v & 255, 5 | 6));
    // Preserve the internal stage, but never log SQL, payloads or credentials.
    let stage = error
        .chain()
        .find_map(|cause| match cause.to_string().as_str() {
            "receipt_preflight_read" => Some("receipt_preflight_read"),
            "receipt_begin" => Some("receipt_begin"),
            "receipt_authoritative_read" => Some("receipt_authoritative_read"),
            "receipt_result_write" => Some("receipt_result_write"),
            "receipt_guard_unknown" => Some("receipt_guard_unknown"),
            "receipt_guard_release" => Some("receipt_guard_release"),
            "receipt_mapping_release" => Some("receipt_mapping_release"),
            "receipt_timing_write" => Some("receipt_timing_write"),
            "receipt_source_release" => Some("receipt_source_release"),
            "receipt_commit" => Some("receipt_commit"),
            _ => None,
        })
        .unwrap_or("receipt_storage");
    tracing::warn!(
        stage,
        sqlite_code = code,
        "device receipt storage unavailable; retry original packet only"
    );
    let mut response = (if busy {StatusCode::SERVICE_UNAVAILABLE} else {StatusCode::INTERNAL_SERVER_ERROR},
        Json(json!({"accepted":false,"error":"device_receipt_unavailable","operation_id":id,
            "failure_boundary":"gateway_storage","stage":stage,"sqlite_code":code,
            "last_confirmed_stage":"device_result_received","execution_state":"unknown",
            "retry_policy":"retry_same_receipt_only_never_replay_command","command_replayed":false}))).into_response();
    if busy {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().expect("static header"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    async fn fixture(tool: &str, state: &str) -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.install_gateway_schema().await.unwrap();
        let job = json!({"tool":tool});
        sqlx::query("INSERT INTO jobs VALUES('job','device','idem','fp',?,NULL,?,?)")
            .bind(job.to_string())
            .bind(state)
            .bind(now())
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO semantic_guards VALUES('guard','job','active',0)")
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO operation_timing VALUES('job',1,2,NULL)")
            .execute(&store.pool)
            .await
            .unwrap();
        store
            .put("operation_semantic", "job", &"guard", i64::MAX)
            .await
            .unwrap();
        store
            .put("file_source", "job", &json!({"synthetic":true}), i64::MAX)
            .await
            .unwrap();
        let mut connections = Vec::new();
        for _ in 0..4 {
            connections.push(store.pool.acquire().await.unwrap());
        }
        drop(connections);
        (dir, store)
    }
    async fn row(store: &Store) -> (String, Option<String>, Option<i64>, i64, i64) {
        sqlx::query_as("SELECT j.state,j.result,t.result_ms,(SELECT COUNT(*) FROM semantic_guards WHERE operation_id='job'),(SELECT COUNT(*) FROM kv WHERE kind='file_source' AND key='job') FROM jobs j JOIN operation_timing t ON j.id=t.id WHERE j.id='job'")
            .fetch_one(&store.pool).await.unwrap()
    }
    #[tokio::test]
    async fn result_and_all_bookkeeping_commit_together() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        let value = json!({"state":"running","output":""});
        assert_eq!(
            commit(&s, "device", "job", &value).await.unwrap(),
            Outcome::Accepted { duplicate: false }
        );
        let (state, result, time, guards, sources) = row(&s).await;
        assert_eq!(state, "done");
        assert_eq!(
            serde_json::from_str::<Value>(&result.unwrap()).unwrap(),
            value
        );
        assert!(time.is_some());
        assert_eq!((guards, sources), (0, 0));
        assert!(
            s.get::<String>("operation_semantic", "job")
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn timing_failure_rolls_back_result_and_guard_then_same_packet_recovers() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        sqlx::query("CREATE TRIGGER reject_timing BEFORE UPDATE ON operation_timing BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&s.pool).await.unwrap();
        let value = json!({"state":"exited","exit_code":7});
        assert!(commit(&s, "device", "job", &value).await.is_err());
        assert_eq!(row(&s).await, ("dispatched".into(), None, None, 1, 1));
        sqlx::query("DROP TRIGGER reject_timing")
            .execute(&s.pool)
            .await
            .unwrap();
        assert_eq!(
            commit(&s, "device", "job", &value).await.unwrap(),
            Outcome::Accepted { duplicate: false }
        );
        assert_eq!(
            commit(&s, "device", "job", &value).await.unwrap(),
            Outcome::Accepted { duplicate: true }
        );
    }
    #[tokio::test]
    async fn source_release_failure_cannot_publish_partial_success() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        sqlx::query("CREATE TRIGGER reject_source BEFORE DELETE ON kv WHEN OLD.kind='file_source' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&s.pool).await.unwrap();
        assert!(
            commit(&s, "device", "job", &json!({"ok":true}))
                .await
                .is_err()
        );
        assert_eq!(row(&s).await, ("dispatched".into(), None, None, 1, 1));
    }
    #[tokio::test]
    async fn duplicate_does_not_wait_for_or_write_through_another_writer() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        let value = json!({"ok":true});
        commit(&s, "device", "job", &value).await.unwrap();
        let before = row(&s).await;
        let tx = s.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let duplicate = tokio::time::timeout(
            Duration::from_millis(500),
            commit(&s, "device", "job", &value),
        )
        .await;
        tx.rollback().await.unwrap();
        assert_eq!(
            duplicate
                .expect("duplicate tried to acquire a writer")
                .unwrap(),
            Outcome::Accepted { duplicate: true }
        );
        assert_eq!(row(&s).await, before);
    }
    #[tokio::test]
    async fn concurrent_identical_receipts_commit_once() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        let value = json!({"ok":true});
        let (a, b) = tokio::join!(
            commit(&s, "device", "job", &value),
            commit(&s, "device", "job", &value)
        );
        let results = [a.unwrap(), b.unwrap()];
        assert_eq!(
            results
                .iter()
                .filter(|x| **x == Outcome::Accepted { duplicate: false })
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|x| **x == Outcome::Accepted { duplicate: true })
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn foreign_device_conflict_and_undispatched_command_do_not_mutate() {
        let (_dir, s) = fixture("terminal_exec", "queued").await;
        let value = json!({"ok":true});
        assert_eq!(
            commit(&s, "other", "job", &value).await.unwrap(),
            Outcome::NotFound
        );
        assert_eq!(
            commit(&s, "device", "job", &value).await.unwrap(),
            Outcome::Conflict
        );
        assert_eq!(row(&s).await, ("queued".into(), None, None, 1, 1));
    }
    #[tokio::test]
    async fn transfer_generation_is_revalidated_before_publication() {
        let (_dir, s) = fixture("file_upload", "queued").await;
        s.put(
            "transfer_control",
            "job",
            &json!({"revision":2,"cancel_requested":false}),
            i64::MAX,
        )
        .await
        .unwrap();
        assert_eq!(
            commit(&s, "device", "job", &json!({"transfer_revision":1}))
                .await
                .unwrap(),
            Outcome::Obsolete
        );
        assert_eq!(
            commit(&s, "device", "job", &json!({"transfer_revision":3}))
                .await
                .unwrap(),
            Outcome::Conflict
        );
        assert_eq!(
            commit(&s, "device", "job", &json!({})).await.unwrap(),
            Outcome::Conflict
        );
        assert!(
            commit(&s, "device", "job", &json!({"transfer_revision":-1}))
                .await
                .is_err()
        );
        assert_eq!(
            commit(&s, "device", "job", &json!({"transfer_revision":2}))
                .await
                .unwrap(),
            Outcome::Accepted { duplicate: false }
        );
        assert_eq!(
            commit(
                &s,
                "device",
                "job",
                &json!({"transfer_revision":2,"different":true})
            )
            .await
            .unwrap(),
            Outcome::Conflict
        );
    }
    #[tokio::test]
    async fn unknown_execution_keeps_guard_until_explicit_reconciliation() {
        let (_dir, s) = fixture("terminal_exec", "dispatched").await;
        commit(&s, "device", "job", &json!({"error":"outcome_unknown"}))
            .await
            .unwrap();
        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM semantic_guards WHERE operation_id='job'")
                .fetch_one(&s.pool)
                .await
                .unwrap();
        assert_eq!(state, "outcome_unknown");
        assert_eq!(row(&s).await.3, 1);
        assert_eq!(
            s.get::<String>("operation_semantic", "job").await.unwrap(),
            Some("guard".into())
        );
    }
}

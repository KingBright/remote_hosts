use super::*;
use std::time::Duration;

async fn fixture() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    store.install_agent_schema().await.unwrap();
    (dir, store)
}
fn status(id: &str, state: &str, time: i64) -> Status {
    serde_json::from_value(json!({"id":id,"workspace_id":"fixture","state":state,
        "exit_code":null,"output_truncated":false,"created_at":time,"updated_at":time}))
    .unwrap()
}

#[tokio::test]
async fn ten_thousand_history_rows_use_ordered_partial_index_without_temp_sort() {
    let (_dir, store) = fixture().await;
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO kv(kind,key,value,expires) SELECT 'terminal',printf('%08x-0000-4000-8000-%012x',x,x),json_object('id',printf('%08x-0000-4000-8000-%012x',x,x),'workspace_id','fixture','state','exited','exit_code',0,'output_truncated',json('false'),'created_at',x,'updated_at',x),9223372036854775807 FROM n")
        .execute(&store.pool).await.unwrap();
    let live = uuid::Uuid::new_v4().to_string();
    store
        .put("terminal", &live, &status(&live, "running", 0), i64::MAX)
        .await
        .unwrap();
    const PLAN_SQL: &str = "EXPLAIN QUERY PLAN SELECT value FROM kv WHERE kind='terminal' ORDER BY (json_extract(value,'$.state') IN ('running','starting')) DESC,COALESCE(json_extract(value,'$.updated_at'),json_extract(value,'$.created_at'),0) DESC,key LIMIT 24";
    assert_eq!(
        PLAN_SQL.strip_prefix("EXPLAIN QUERY PLAN "),
        Some(COLLECT_SQL)
    );
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(PLAN_SQL)
        .fetch_all(&store.pool)
        .await
        .unwrap();
    let text = plan
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    assert!(text.contains("terminal_sync_recent"), "{text}");
    assert!(!text.contains("TEMP B-TREE"), "{text}");
    let rows = collect(&store).await.unwrap();
    assert_eq!(rows.len(), 24);
    assert_eq!(rows[0].id, live);
    assert_eq!(rows[1].updated_at, 10000);
    assert_eq!(rows[23].updated_at, 9978);
}

#[tokio::test]
async fn idle_snapshot_reuses_cache_even_with_database_closed() {
    let (dir, store) = fixture().await;
    let mut cache = SnapshotCache::default();
    assert!(
        cache
            .refresh(&store, dir.path().into(), 0, false)
            .await
            .unwrap()
    );
    store.pool.close().await;
    assert!(
        !cache
            .refresh(&store, dir.path().into(), 0, false)
            .await
            .unwrap()
    );
    assert!(
        cache
            .refresh(&store, dir.path().into(), 1, false)
            .await
            .is_err()
    );
    assert_eq!(cache.revision, Some(0), "failed refresh must remain dirty");
}

#[tokio::test]
async fn dirty_or_live_snapshot_refreshes_and_failed_read_retains_last_good_state() {
    let (dir, store) = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    store
        .put("terminal", &id, &status(&id, "running", 1), i64::MAX)
        .await
        .unwrap();
    let mut cache = SnapshotCache::default();
    cache
        .refresh(&store, dir.path().into(), 0, true)
        .await
        .unwrap();
    store
        .put("terminal", &id, &status(&id, "exited", 2), i64::MAX)
        .await
        .unwrap();
    assert!(
        cache
            .refresh(&store, dir.path().into(), 1, false)
            .await
            .unwrap()
    );
    assert_eq!(cache.terminals[0].state, "exited");
    store.pool.close().await;
    assert!(
        cache
            .refresh(&store, dir.path().into(), 2, false)
            .await
            .is_err()
    );
    assert_eq!(cache.terminals[0].state, "exited");
    assert_eq!(cache.revision, Some(1));
}

#[tokio::test]
async fn periodic_refresh_expires_without_extending_stale_snapshot_age() {
    let (dir, store) = fixture().await;
    let mut cache = SnapshotCache::default();
    cache
        .refresh(&store, dir.path().into(), 0, false)
        .await
        .unwrap();
    cache.refreshed_at = Some(tokio::time::Instant::now() - Duration::from_secs(31));
    assert!(
        cache
            .refresh(&store, dir.path().into(), 0, false)
            .await
            .unwrap()
    );
    assert!(
        cache
            .refresh(&store, dir.path().into(), 0, true)
            .await
            .unwrap()
    );
}

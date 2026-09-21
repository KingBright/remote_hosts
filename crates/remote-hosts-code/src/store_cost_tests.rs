//! Deterministic storage-cost and retention checks; only temporary databases.
use super::*;
use serde_json::{Value, json};

#[tokio::test]
async fn expiry_cleanup_is_bounded_and_permanent_evidence_is_protected() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    for n in 0..1000 {
        sqlx::query("INSERT INTO kv VALUES('request_receipt',?,?,?)")
            .bind(format!("keep-{n}"))
            .bind(json!({"state":"outcome_unknown","id":n}).to_string())
            .bind(i64::MAX)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for n in 0..600 {
        sqlx::query("INSERT INTO kv VALUES('expired_probe',?,?,?)")
            .bind(format!("old-{n}"))
            .bind("{}")
            .bind(crate::now() - 1)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    store
        .put("access", "live", &json!({"live":true}), crate::now() + 3600)
        .await
        .unwrap();
    assert_eq!(store.prune_batch().await.unwrap(), 256);
    assert!(
        store
            .get::<Value>("expired_probe", "old-599")
            .await
            .unwrap()
            .is_none(),
        "physical retention must not extend logical validity"
    );
    assert_eq!(store.prune_batch().await.unwrap(), 256);
    assert_eq!(store.prune_batch().await.unwrap(), 88);
    assert_eq!(store.prune_batch().await.unwrap(), 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv WHERE kind='request_receipt'")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1000);
    assert!(
        store
            .get::<Value>("access", "live")
            .await
            .unwrap()
            .is_some()
    );
    let evidence: Value = store
        .get("request_receipt", "keep-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence["state"], "outcome_unknown");
}

#[tokio::test]
async fn empty_expiry_cleanup_does_not_acquire_a_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    store
        .put("request_receipt", "keep", &json!({}), i64::MAX)
        .await
        .unwrap();
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let tx = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), store.prune_batch()).await;
    tx.rollback().await.unwrap();
    assert_eq!(
        result
            .expect("empty prune tried to take the writer")
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn expiry_plan_searches_only_the_finite_lifetime_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let mut connection = store.pool.acquire().await.unwrap();
    let cold: Vec<(i64,i64,i64,String)> = sqlx::query_as("EXPLAIN QUERY PLAN SELECT kind,key FROM kv WHERE expires<9223372036854775807 AND expires<=? ORDER BY expires,kind,key LIMIT 256")
        .bind(crate::now()).fetch_all(&mut *connection).await.unwrap();
    let indexes: Vec<(String, String)> =
        sqlx::query_as("SELECT name,sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
    let engine: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let plan: Vec<(i64,i64,i64,String)> = sqlx::query_as("EXPLAIN QUERY PLAN SELECT kind,key FROM kv WHERE expires<9223372036854775807 AND expires<=? ORDER BY expires,kind,key LIMIT 256")
        .persistent(false).bind(crate::now()).fetch_all(&mut *connection).await.unwrap();
    println!("ENGINE {engine} INDEXES {indexes:?} COLD {cold:?} ACTUAL_SCHEMA_PLAN {plan:?}");
    assert!(
        plan.iter()
            .any(|row| row.3.contains("SEARCH") && row.3.contains("kv_expiring")),
        "unexpected plan: {plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|row| row.3.contains("SCAN kv") || row.3.contains("TEMP B-TREE")),
        "unbounded scan/sort: {plan:?}"
    );
    let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(journal, "wal");
    assert_eq!(sync, 2, "FULL durability must not be weakened");
}

#[tokio::test]
async fn expiry_failure_is_atomic_and_can_be_retried_without_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    for id in ["one", "two"] {
        store
            .put("expired_probe", id, &json!({}), crate::now() - 1)
            .await
            .unwrap();
    }
    sqlx::query("CREATE TRIGGER fail_expiry BEFORE DELETE ON kv WHEN OLD.key='two' BEGIN SELECT RAISE(ABORT,'injected expiry failure'); END")
        .execute(&store.pool).await.unwrap();
    assert!(store.prune_batch().await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    sqlx::query("DROP TRIGGER fail_expiry")
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(store.prune_batch().await.unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_rechecks_expiry_after_a_concurrent_renewal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    store
        .put("access", "renewed", &json!({}), crate::now() - 1)
        .await
        .unwrap();
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let mut tx = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE kv SET expires=? WHERE kind='access' AND key='renewed'")
        .bind(crate::now() + 3600)
        .execute(&mut *tx)
        .await
        .unwrap();
    let worker = {
        let store = store.clone();
        tokio::spawn(async move { store.prune_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    assert!(
        store
            .get::<Value>("access", "renewed")
            .await
            .unwrap()
            .is_some()
    );
}

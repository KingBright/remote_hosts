use super::*;
use serde_json::{Value, json};

async fn fixture() -> (tempfile::TempDir, Store, Readiness) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    store.put("runtime", "readiness", &json!({"session":"fixture","pid":std::process::id(),"lanes":{},"phase":"gateway_wait"}), i64::MAX).await.unwrap();
    sqlx::query("CREATE TABLE readiness_writes(n INTEGER)")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER count_readiness AFTER UPDATE ON kv WHEN NEW.kind='runtime' AND NEW.key='readiness' BEGIN INSERT INTO readiness_writes VALUES(1); END").execute(&store.pool).await.unwrap();
    let cache = Readiness::default();
    cache.start("fixture").unwrap();
    (dir, store, cache)
}
async fn count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM readiness_writes")
        .fetch_one(&store.pool)
        .await
        .unwrap()
}
async fn value(store: &Store) -> Value {
    store.get("runtime", "readiness").await.unwrap().unwrap()
}

#[tokio::test]
async fn twenty_five_acknowledgements_use_one_commit_and_idle_uses_none() {
    let (_dir, store, cache) = fixture().await;
    let clock = Instant::now();
    for n in 0..5 {
        for lane in ["read", "write", "terminal", "transfer", "control"] {
            cache.record("fixture", lane, 100 + n).unwrap();
        }
    }
    assert_eq!(count(&store).await, 0);
    assert!(cache.flush_at(&store, "fixture", clock).await.unwrap());
    assert_eq!(count(&store).await, 1);
    let saved = value(&store).await;
    assert_eq!(saved["lanes"].as_object().unwrap().len(), 5);
    assert!(
        saved["lanes"]
            .as_object()
            .unwrap()
            .values()
            .all(|v| *v == 104)
    );
    assert!(
        !cache
            .flush_at(&store, "fixture", clock + Duration::from_secs(60))
            .await
            .unwrap()
    );
    assert_eq!(count(&store).await, 1);
}

#[tokio::test]
async fn unchanged_membership_is_coalesced_without_refreshing_failed_lanes() {
    let (_dir, store, cache) = fixture().await;
    let clock = Instant::now();
    cache.record("fixture", "write", 100).unwrap();
    cache.record("fixture", "read", 100).unwrap();
    cache.flush_at(&store, "fixture", clock).await.unwrap();
    cache.record("fixture", "read", 160).unwrap();
    assert!(
        !cache
            .flush_at(&store, "fixture", clock + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert!(
        cache
            .flush_at(&store, "fixture", clock + Duration::from_secs(11))
            .await
            .unwrap()
    );
    let saved = value(&store).await;
    assert_eq!(saved["lanes"]["write"], 100);
    assert_eq!(saved["lanes"]["read"], 160);
    assert_eq!(count(&store).await, 2);
}

#[tokio::test]
async fn newly_ready_lane_is_visible_before_normal_period() {
    let (_dir, store, cache) = fixture().await;
    let clock = Instant::now();
    cache.record("fixture", "read", 100).unwrap();
    cache.flush_at(&store, "fixture", clock).await.unwrap();
    cache.record("fixture", "write", 101).unwrap();
    assert!(
        cache
            .flush_at(&store, "fixture", clock + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(value(&store).await["lanes"]["write"], 101);
}

#[tokio::test]
async fn failed_publication_retains_facts_and_does_not_advance_saved_cursor() {
    let (_dir, store, cache) = fixture().await;
    let clock = Instant::now();
    cache.record("fixture", "read", 100).unwrap();
    sqlx::query("CREATE TRIGGER refuse_readiness BEFORE UPDATE ON kv WHEN NEW.key='readiness' BEGIN SELECT RAISE(ABORT,'synthetic telemetry failure'); END").execute(&store.pool).await.unwrap();
    assert!(cache.flush_at(&store, "fixture", clock).await.is_err());
    assert_eq!(cache.pending.lock().unwrap().saved_revision, 0);
    assert_eq!(count(&store).await, 0);
    sqlx::query("DROP TRIGGER refuse_readiness")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        cache
            .flush_at(&store, "fixture", clock + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(value(&store).await["lanes"]["read"], 100);
}

#[tokio::test]
async fn stale_publication_cannot_overwrite_newer_persisted_snapshot() {
    let (_dir, store, cache) = fixture().await;
    cache.record("fixture", "read", 100).unwrap();
    sqlx::query("UPDATE kv SET value=json_set(value,'$.publication_revision',20,'$.lanes.read',200) WHERE kind='runtime' AND key='readiness'").execute(&store.pool).await.unwrap();
    assert!(
        cache
            .flush_at(&store, "fixture", Instant::now())
            .await
            .unwrap()
    );
    assert_eq!(value(&store).await["lanes"]["read"], 200);
    assert_eq!(count(&store).await, 1);
}

#[tokio::test]
async fn restarted_session_rejects_old_events_and_old_database_identity() {
    let (_dir, store, cache) = fixture().await;
    cache.record("fixture", "read", 100).unwrap();
    cache.start("new").unwrap();
    assert!(cache.record("fixture", "write", 101).is_err());
    cache.record("new", "terminal", 102).unwrap();
    assert!(cache.flush_at(&store, "new", Instant::now()).await.is_err());
    assert_eq!(count(&store).await, 0);
    assert_eq!(value(&store).await["session"], "fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_lock_does_not_block_recording_or_exceed_publication_wait_budget() {
    let (_dir, store, cache) = fixture().await;
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let tx = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    cache.record("fixture", "read", 100).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), cache.flush(&store, "fixture")).await;
    assert!(
        result
            .expect("telemetry delayed heartbeat past its budget")
            .is_err()
    );
    cache.record("fixture", "write", 101).unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(cache.pending.lock().unwrap().saved_revision, 0);
}

#[test]
fn invalid_lanes_and_old_session_events_do_not_enter_the_snapshot() {
    let cache = Readiness::default();
    cache.start("fixture").unwrap();
    assert!(cache.record("fixture", "unknown", 100).is_err());
    assert!(cache.record("fixture", "read", -1).is_err());
    assert!(cache.record("other", "read", 100).is_err());
    assert!(cache.pending.lock().unwrap().lanes.is_empty());
}

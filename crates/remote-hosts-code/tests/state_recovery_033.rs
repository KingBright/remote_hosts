//! Large history and bounded pagination; no real processes or production state.
use remote_hosts_code::{now, store::Store, terminal::Terminals};
use serde_json::{Value, json};

async fn fixture() -> (tempfile::TempDir, Store) {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).await.unwrap();
    (d, store)
}
async fn seed(store: &Store, kind: &str, count: usize) {
    let mut tx = store.pool.begin().await.unwrap();
    for n in 0..count {
        sqlx::query("INSERT INTO kv VALUES(?,?,?,?)")
            .bind(kind)
            .bind(format!("{n:06}"))
            .bind(json!({"n":n}).to_string())
            .bind(i64::MAX)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}
#[tokio::test]
async fn more_than_one_thousand_rows_are_not_silently_reported_as_complete() {
    let (_d, store) = fixture().await;
    seed(&store, "item", 1105).await;
    let error = store.list::<Value>("item").await.unwrap_err();
    assert!(error.to_string().starts_with("state_list_truncated"));
    let mut cursor = None;
    let mut values = vec![];
    loop {
        let page = store
            .list_page::<Value>("item", cursor.as_deref(), 127)
            .await
            .unwrap();
        assert!(page.entries.len() <= 127);
        values.extend(
            page.entries
                .into_iter()
                .map(|(_, v)| v["n"].as_u64().unwrap()),
        );
        match page.next_key {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(values, (0..1105).collect::<Vec<u64>>());
}
#[tokio::test]
async fn page_filters_expired_rows_and_validates_limits() {
    let (_d, store) = fixture().await;
    seed(&store, "item", 3).await;
    store
        .put("item", "expired", &json!({}), now() - 1)
        .await
        .unwrap();
    assert_eq!(
        store
            .list_page::<Value>("item", None, 3)
            .await
            .unwrap()
            .entries
            .len(),
        3
    );
    assert!(store.list_page::<Value>("item", None, 0).await.is_err());
    assert!(store.list_page::<Value>("item", None, 1001).await.is_err());
    assert!(
        store
            .list_page::<Value>("item", Some("zzzz"), 10)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        store
            .list_page::<Value>("item", Some("' OR 1=1 --"), 10)
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn terminal_restart_recovers_active_records_beyond_the_first_history_page() {
    let (d, store) = fixture().await;
    let mut tx = store.pool.begin().await.unwrap();
    for n in 0..1105 {
        let state = if n == 1103 {
            "running"
        } else if n == 1104 {
            "starting"
        } else {
            "exited"
        };
        let value = json!({"id":format!("{n:06}"),"workspace_id":"fixture","state":state,
            "exit_code":0,"output_truncated":false,"created_at":0,"preserved_field":"original"});
        sqlx::query("INSERT INTO kv VALUES('terminal',?,?,?)")
            .bind(format!("{n:06}"))
            .bind(value.to_string())
            .bind(i64::MAX)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let _terminals = Terminals::new(store.clone(), d.path().join("logs"), "synthetic".into())
        .await
        .unwrap();
    for n in [1103, 1104] {
        let value = store
            .get::<Value>("terminal", &format!("{n:06}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value["state"], "runtime_lost");
        assert_eq!(value["preserved_field"], "original");
    }
    assert_eq!(
        store
            .get::<Value>("terminal", "000001")
            .await
            .unwrap()
            .unwrap()["state"],
        "exited"
    );
    let (active,):(i64,)=sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting')").fetch_one(&store.pool).await.unwrap();
    assert_eq!(active, 0);
}

use super::*;

#[tokio::test]
async fn idle_outbox_and_unchanged_health_do_not_need_a_writer() {
    let (_tmp, d) = fixture().await;
    Delivery::status(&d.store, &d.config).await.unwrap();
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(d.store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let tx = d.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(1), async {
        assert!(d.claim().await.unwrap().is_none());
        Delivery::status(&d.store, &d.config).await.unwrap();
    })
    .await;
    tx.rollback().await.unwrap();
    read.expect("idle receipt polling attempted a SQLite write");
}

#[tokio::test]
async fn changed_receipt_health_is_read_from_queue_without_cooldown() {
    let (_tmp, d) = fixture().await;
    Delivery::status(&d.store, &d.config).await.unwrap();
    d.enqueue(&uuid::Uuid::new_v4().to_string(), &json!({"answer":1}))
        .await
        .unwrap();
    Delivery::status(&d.store, &d.config).await.unwrap();
    let v = Delivery::status(&d.store, &d.config).await.unwrap();
    assert_eq!(v.pending, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_responses_use_two_senders_and_do_not_allocate_unbounded_tasks() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let c = calls.clone();
    let p = permits.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(move || {
                    let c = c.clone();
                    let p = p.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        p.acquire_owned().await.unwrap().forget();
                        Json(json!({"accepted":true}))
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    for _ in 0..8 {
        d.enqueue(&uuid::Uuid::new_v4().to_string(), &json!({"answer":1}))
            .await
            .unwrap();
    }
    let running = d.clone();
    let worker = tokio::spawn(async move { running.run(&reqwest::Client::new()).await });
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let observed = calls.load(Ordering::SeqCst);
    permits.add_permits(8);
    let drained = tokio::time::timeout(Duration::from_secs(3), async {
        while !rows(&d).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.abort();
    let _ = worker.await;
    server.abort();
    assert!(first.is_ok());
    assert_eq!(observed, 2);
    assert!(drained.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn old_ack_cannot_delete_a_new_attempt_lease() {
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(|| async { Json(json!({"accepted":true})) }),
            ),
        )
        .await
        .unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    let saved = json!({"state":"done","tool":"terminal_exec","result":{"answer":1}});
    d.complete(&id, &saved, &json!({"answer":1})).await.unwrap();
    let old = d.claim().await.unwrap().unwrap();
    sqlx::query("UPDATE receipt_outbox SET next_attempt=? WHERE id=?")
        .bind(now() - 1)
        .bind(&id)
        .execute(&d.store.pool)
        .await
        .unwrap();
    let new = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), old).await.unwrap();
    assert_eq!(rows(&d).await.len(), 1);
    assert_eq!(
        d.store.get::<Value>("local_operation", &id).await.unwrap(),
        Some(saved)
    );
    d.deliver(&reqwest::Client::new(), new).await.unwrap();
    assert!(rows(&d).await.is_empty());
    let compact: Value = d.store.get("local_operation", &id).await.unwrap().unwrap();
    assert_eq!(compact["gateway_accepted"], true);
    assert!(compact.get("result").is_none());
    server.abort();
}
use axum::{Json, Router, routing::post};

async fn fixture() -> (tempfile::TempDir, Delivery) {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(AgentConfig {
        gateway_url: "http://127.0.0.1:1".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: crate::random(),
        state_dir: d.path().join("state"),
        roots: vec![d.path().join("root")],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    });
    let store = Store::open(&config.state_dir).await.unwrap();
    let delivery = Delivery::new(store, config).await.unwrap();
    (d, delivery)
}
async fn rows(d: &Delivery) -> Vec<(String, String, i64)> {
    sqlx::query_as("SELECT id,state,attempts FROM receipt_outbox ORDER BY id")
        .fetch_all(&d.store.pool)
        .await
        .unwrap()
}
#[tokio::test]
async fn reopening_prunes_only_gateway_owned_local_results() {
    let (_tmp, d) = fixture().await;
    let delivered = uuid::Uuid::new_v4().to_string();
    let pending = uuid::Uuid::new_v4().to_string();
    let edit = uuid::Uuid::new_v4().to_string();
    let retired_edit = uuid::Uuid::new_v4().to_string();
    d.store
        .put(
            "local_operation",
            &retired_edit,
            &json!({"state":"done","tool":"code_apply_edits","gateway_accepted":true}),
            i64::MAX,
        )
        .await
        .unwrap();
    d.store
        .put(
            "local_operation",
            &delivered,
            &json!({"state":"done","tool":"terminal_exec"}),
            i64::MAX,
        )
        .await
        .unwrap();
    d.store
        .put(
            "local_operation",
            &pending,
            &json!({"state":"done","tool":"terminal_exec"}),
            i64::MAX,
        )
        .await
        .unwrap();
    d.enqueue(&pending, &json!({"answer":1})).await.unwrap();
    d.store
        .put(
            "local_operation",
            &edit,
            &json!({"state":"done","tool":"code_apply_edits"}),
            i64::MAX,
        )
        .await
        .unwrap();
    let config = d.config.clone();
    let store = d.store.clone();
    drop(d);
    let reopened = Delivery::new(store, config).await.unwrap();
    assert!(
        reopened
            .store
            .get::<Value>("local_operation", &retired_edit)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        reopened
            .store
            .get::<Value>("local_operation", &delivered)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        reopened
            .store
            .get::<Value>("local_operation", &pending)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        reopened
            .store
            .get::<Value>("local_operation", &edit)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn final_result_and_receipt_intent_commit_together_and_conflicts_rollback() {
    let (_tmp, d) = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    let original = json!({"state":"running","fingerprint":"fixture"});
    d.store
        .put("local_operation", &id, &original, i64::MAX)
        .await
        .unwrap();
    d.enqueue(&id, &json!({"answer":1})).await.unwrap();
    assert!(
        d.complete(&id, &json!({"state":"done"}), &json!({"answer":2}))
            .await
            .is_err()
    );
    assert_eq!(
        d.store
            .get::<Value>("local_operation", &id)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    d.complete(
        &id,
        &json!({"state":"done","result":{"answer":1}}),
        &json!({"answer":1}),
    )
    .await
    .unwrap();
    assert_eq!(rows(&d).await.len(), 1);
    assert_eq!(
        d.store
            .get::<Value>("local_operation", &id)
            .await
            .unwrap()
            .unwrap()["state"],
        "done"
    );
}
#[tokio::test]
async fn pending_receipt_survives_reopening_storage_and_keeps_exact_bytes() {
    let (_tmp, d) = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    let value = json!({"result":"你好🧪","nested":{"a":[1,2]}});
    d.complete(&id, &json!({"state":"done","result":value}), &value)
        .await
        .unwrap();
    let config = d.config.clone();
    drop(d);
    let d = Delivery::new(Store::open(&config.state_dir).await.unwrap(), config)
        .await
        .unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&claim.payload).unwrap(),
        json!({"operation_id":id,"result":value})
    );
    assert_eq!(claim.attempt, 1);
    assert!(d.claim().await.unwrap().is_none());
    sqlx::query("UPDATE receipt_outbox SET next_attempt=? WHERE id=?")
        .bind(now() - 1)
        .bind(&id)
        .execute(&d.store.pool)
        .await
        .unwrap();
    let recovered = d.claim().await.unwrap().unwrap();
    assert_eq!(claim.payload, recovered.payload);
    assert_ne!(claim.lease, recovered.lease);
}
#[tokio::test]
async fn changing_destination_or_device_does_not_forward_saved_results() {
    let (_tmp, d) = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    d.enqueue(&id, &json!({"private":"fixture"})).await.unwrap();
    let mut config = (*d.config).clone();
    config.gateway_url = "http://127.0.0.1:2".into();
    let changed = Delivery::new(d.store.clone(), Arc::new(config))
        .await
        .unwrap();
    assert!(changed.claim().await.unwrap().is_none());
    assert!(
        changed
            .enqueue(&id, &json!({"private":"fixture"}))
            .await
            .is_err()
    );
    let mut config = (*d.config).clone();
    config.device_id = uuid::Uuid::new_v4().to_string();
    let changed = Delivery::new(d.store.clone(), Arc::new(config))
        .await
        .unwrap();
    assert!(changed.claim().await.unwrap().is_none());
}
#[tokio::test]
async fn http_success_without_explicit_acceptance_never_deletes_receipt() {
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(|| async { Json(json!({"accepted":false})) }),
            ),
        )
        .await
        .unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    d.enqueue(&id, &json!({"answer":1})).await.unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), claim).await.unwrap();
    assert_eq!(rows(&d).await[0].1, "pending");
    server.abort();
}
#[tokio::test]
async fn permanent_conflict_is_retained_without_automatic_retry_or_reexecution() {
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(|| async { axum::http::StatusCode::CONFLICT }),
            ),
        )
        .await
        .unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    d.enqueue(&id, &json!({"answer":1})).await.unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), claim).await.unwrap();
    d.enqueue(&id, &json!({"answer":1})).await.unwrap();
    assert!(d.claim().await.unwrap().is_none());
    assert_eq!(rows(&d).await[0].1, "blocked");
    assert_eq!(rows(&d).await[0].2, 1);
    Delivery::status(&d.store, &d.config).await.unwrap();
    assert_eq!(
        Delivery::status(&d.store, &d.config).await.unwrap().blocked,
        1
    );
    server.abort();
}
#[tokio::test]
async fn accepted_receipt_drops_result_body_but_keeps_runtime_deduplication() {
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(|| async { Json(json!({"accepted":true,"duplicate":true})) }),
            ),
        )
        .await
        .unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    let saved = json!({"state":"done","fingerprint":"fixture-fingerprint","tool":"terminal_exec","result":{"large":"x".repeat(32000)}});
    d.complete(&id, &saved, &json!({"answer":1})).await.unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), claim).await.unwrap();
    assert!(rows(&d).await.is_empty());
    let compact: Value = d.store.get("local_operation", &id).await.unwrap().unwrap();
    assert_eq!(compact["gateway_accepted"], true);
    assert_eq!(compact["fingerprint"], "fixture-fingerprint");
    assert!(compact.get("result").is_none());
    assert!(serde_json::to_vec(&compact).unwrap().len() < 300);
    server.abort();
}

#[tokio::test]
async fn accepted_edit_receipt_keeps_change_resume_anchor() {
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(|| async { Json(json!({"accepted":true})) }),
            ),
        )
        .await
        .unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    let saved = json!({"state":"done","tool":"code_apply_edits","workspace_id":"ws","result":{"change_set":{"state":"partial"}}});
    d.complete(&id, &saved, &json!({"change_set":{"state":"partial"}}))
        .await
        .unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), claim).await.unwrap();
    assert!(rows(&d).await.is_empty());
    let mut acknowledged = saved;
    acknowledged["gateway_accepted"] = json!(true);
    assert_eq!(
        d.store
            .get::<Value>("local_operation", &id)
            .await
            .unwrap()
            .unwrap(),
        acknowledged
    );
    // Delivery acknowledgement is not business completion. Startup must keep
    // the whole partial-edit recovery anchor even though its receipt arrived.
    let reopened = Delivery::new(d.store.clone(), d.config.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .store
            .get::<Value>("local_operation", &id)
            .await
            .unwrap()
            .unwrap(),
        acknowledged
    );
    server.abort();
}

#[tokio::test]
async fn telemetry_cost_budget_is_zero_writes_and_ignores_old_runtime_copy() {
    let (_tmp, d) = fixture().await;
    d.store
        .put(
            "runtime",
            "receipt_delivery",
            &json!({"pending":99,"reported_at":now()-120}),
            i64::MAX,
        )
        .await
        .unwrap();
    d.enqueue(&uuid::Uuid::new_v4().to_string(), &json!({"answer":1}))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE health_writes(n INTEGER)")
        .execute(&d.store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO health_writes VALUES(0)")
        .execute(&d.store.pool)
        .await
        .unwrap();
    for statement in [
        "CREATE TRIGGER count_health_insert AFTER INSERT ON kv BEGIN UPDATE health_writes SET n=n+1; END",
        "CREATE TRIGGER count_health_update AFTER UPDATE ON kv BEGIN UPDATE health_writes SET n=n+1; END",
        "CREATE TRIGGER count_health_delete AFTER DELETE ON kv BEGIN UPDATE health_writes SET n=n+1; END",
    ] {
        sqlx::query(statement).execute(&d.store.pool).await.unwrap();
    }
    for _ in 0..100 {
        let status = Delivery::status(&d.store, &d.config).await.unwrap();
        assert_eq!(status.pending, 1);
        assert_eq!(status.blocked, 0);
        assert!(status.valid());
    }
    let writes: i64 = sqlx::query_scalar("SELECT n FROM health_writes")
        .fetch_one(&d.store.pool)
        .await
        .unwrap();
    assert_eq!(
        writes, 0,
        "a sampled queue must not create a telemetry write"
    );
    assert_eq!(
        rows(&d).await.len(),
        1,
        "sampling must retain the delivery intent"
    );
}

#[tokio::test]
async fn queue_health_failure_is_unavailable_not_an_empty_queue() {
    let (_tmp, d) = fixture().await;
    d.store.pool.close().await;
    assert!(Delivery::status(&d.store, &d.config).await.is_err());
}

#[tokio::test]
async fn queue_health_rebuilds_after_restart_without_saved_telemetry() {
    let (_tmp, d) = fixture().await;
    d.enqueue(&uuid::Uuid::new_v4().to_string(), &json!({"answer":1}))
        .await
        .unwrap();
    let store = d.store.clone();
    let config = d.config.clone();
    drop(d);
    let reopened = Delivery::new(store, config).await.unwrap();
    let status = Delivery::status(&reopened.store, &reopened.config)
        .await
        .unwrap();
    assert_eq!(status.pending, 1);
    assert!(
        reopened
            .store
            .get::<Value>("runtime", "receipt_delivery")
            .await
            .unwrap()
            .is_none()
    );
    let mut other = (*reopened.config).clone();
    other.gateway_url = "http://127.0.0.1:2".into();
    assert_eq!(
        Delivery::status(&reopened.store, &other)
            .await
            .unwrap()
            .pending,
        0
    );
    other = (*reopened.config).clone();
    other.device_id = uuid::Uuid::new_v4().to_string();
    assert_eq!(
        Delivery::status(&reopened.store, &other)
            .await
            .unwrap()
            .pending,
        0
    );
}

#[test]
fn idle_retry_deadlines_are_bounded_without_a_busy_loop() {
    assert_eq!(wake_delay(None, 100), Duration::from_secs(5));
    assert_eq!(wake_delay(Some(102), 100), Duration::from_secs(2));
    assert_eq!(wake_delay(Some(200), 100), Duration::from_secs(5));
    assert_eq!(wake_delay(Some(99), 100), Duration::from_secs(1));
    assert_eq!(wake_delay(Some(i64::MAX), i64::MIN), Duration::from_secs(5));
}

#[tokio::test]
async fn idle_retry_deadline_does_not_take_the_writer() {
    let (_tmp, d) = fixture().await;
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(d.store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let tx = d.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let delay = tokio::time::timeout(Duration::from_secs(1), d.next_wake_delay()).await;
    tx.rollback().await.unwrap();
    assert_eq!(delay.unwrap().unwrap(), Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_receipt_interrupts_idle_deadline_and_retains_original_payload() {
    use axum::{Json, Router, routing::post};
    let (_tmp, mut d) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut d.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Value>(2);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/device/result",
                post(move |Json(value): Json<Value>| {
                    let tx = tx.clone();
                    async move {
                        tx.send(value).await.unwrap();
                        Json(json!({"accepted":true}))
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let worker = {
        let d = d.clone();
        tokio::spawn(async move { d.run(&reqwest::Client::new()).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let id = uuid::Uuid::new_v4().to_string();
    d.enqueue(&id, &json!({"answer":42})).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    worker.abort();
    let _ = worker.await;
    server.abort();
    let _ = server.await;
    let received = received
        .expect("enqueue waited for the five-second idle timer")
        .unwrap();
    assert_eq!(received["operation_id"], id);
    assert_eq!(received["result"]["answer"], 42);
}

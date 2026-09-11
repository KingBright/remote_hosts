use super::*;

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
    d.enqueue(&id, &json!({"answer":1})).await.unwrap();
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
    d.deliver(&reqwest::Client::new(), new).await.unwrap();
    assert!(rows(&d).await.is_empty());
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
    d.report().await.unwrap();
    assert_eq!(
        d.store
            .get::<Value>("runtime", "receipt_delivery")
            .await
            .unwrap()
            .unwrap()["blocked"],
        1
    );
    server.abort();
}
#[tokio::test]
async fn accepted_receipt_drops_only_delivery_intent_not_local_idempotency_record() {
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
    let saved = json!({"state":"done","result":{"answer":1}});
    d.complete(&id, &saved, &json!({"answer":1})).await.unwrap();
    let claim = d.claim().await.unwrap().unwrap();
    d.deliver(&reqwest::Client::new(), claim).await.unwrap();
    assert!(rows(&d).await.is_empty());
    assert_eq!(
        d.store
            .get::<Value>("local_operation", &id)
            .await
            .unwrap()
            .unwrap(),
        saved
    );
    server.abort();
}

//! Loopback HTTP and isolated SQLite fault-injection tests; no production state.
use super::*;
use crate::{
    AgentConfig,
    delivery::Delivery,
    scheduler::{ActiveResource, Lane},
};
use axum::{Json, Router, http::StatusCode, routing::post};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};
use tokio::sync::mpsc;

struct Fixture {
    agent: Agent,
    hello: DeviceHello,
    client: reqwest::Client,
    seen: mpsc::UnboundedReceiver<Value>,
    status: Arc<AtomicU16>,
    server: tokio::task::JoinHandle<()>,
    _root: tempfile::TempDir,
    _state: tempfile::TempDir,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let (send, seen) = mpsc::unbounded_channel();
    let status = Arc::new(AtomicU16::new(204));
    let response = status.clone();
    let poll_send = send.clone();
    let router = Router::new()
        .route(
            "/device/heartbeat",
            post(move |Json(body): Json<Value>| {
                let (send, response) = (send.clone(), response.clone());
                async move {
                    let _ = send.send(body);
                    StatusCode::from_u16(response.load(Ordering::SeqCst)).unwrap()
                }
            }),
        )
        .route(
            "/device/poll",
            post(move |Json(body): Json<Value>| {
                let send = poll_send.clone();
                async move {
                    let _ = send.send(body);
                    Json(json!({"job":null}))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut agent = Agent::new(AgentConfig {
        gateway_url: "https://fixture.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: crate::random(),
        state_dir: state.path().into(),
        roots: vec![root.path().into()],
        allow_write: true,
        allow_exec: true,
        shell: if cfg!(windows) { "cmd.exe" } else { "/bin/sh" }.into(),
    })
    .await
    .unwrap();
    Arc::make_mut(&mut agent.config).gateway_url = format!("http://{address}");
    Delivery::new(agent.store.clone(), agent.config.clone())
        .await
        .unwrap();
    let hello: DeviceHello = serde_json::from_value(json!({
        "version":env!("CARGO_PKG_VERSION"), "session":crate::random(),
        "roots":[root.path()], "allow_exec":true, "allow_write":true,
    }))
    .unwrap();
    agent.readiness.start(&hello.session).unwrap();
    agent
        .store
        .put(
            "runtime",
            "readiness",
            &json!({"session":hello.session,
        "pid":std::process::id(),"lanes":{}}),
            i64::MAX,
        )
        .await
        .unwrap();
    Fixture {
        agent,
        hello,
        client: reqwest::Client::builder().no_proxy().build().unwrap(),
        seen,
        status,
        server,
        _root: root,
        _state: state,
    }
}

#[test]
fn watchdog_uses_monotonic_contact_age_and_coalesces_recent_success() {
    let contact = Liveness::default();
    let now = Instant::now();
    assert!(contact.due_at(now));
    *contact.acknowledged_at.lock().unwrap() = Some(now);
    assert!(!contact.due_at(now + Duration::from_secs(9)));
    assert!(contact.due_at(now + Duration::from_secs(10)));
    assert!(!contact.due_at(now - Duration::from_secs(1)));
}

#[tokio::test]
async fn keepalive_succeeds_with_every_local_database_connection_held() {
    let mut f = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    let _lease = f
        .agent
        .active
        .enter_scoped(&id, ActiveResource::default())
        .unwrap()
        .unwrap();
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(f.agent.store.pool.acquire().await.unwrap());
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        f.agent.keepalive_once(&f.client, &f.hello),
    )
    .await
    .expect("keepalive waited for local SQLite")
    .unwrap();
    let body = f.seen.recv().await.unwrap();
    assert_eq!(body["active_operations"], json!([id]));
    assert!(body.get("terminal_updates").is_none());
    assert!(body.get("receipt_delivery").is_none());
    assert!(!f.agent.liveness.due_at(Instant::now()));
    drop(held);
    assert!(
        !f.agent
            .readiness
            .flush(&f.agent.store, &f.hello.session)
            .await
            .unwrap(),
        "keepalive must not fabricate successful poll-lane facts"
    );
}

#[tokio::test]
async fn independent_watchdog_runs_while_terminal_replication_is_blocked() {
    let mut f = fixture().await;
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(f.agent.store.pool.acquire().await.unwrap());
    }
    let replication_agent = f.agent.clone();
    let replication_hello = f.hello.clone();
    let replication_client = f.client.clone();
    let replication = tokio::spawn(async move {
        replication_agent
            .heartbeat_with_period(
                &replication_client,
                &replication_hello,
                Duration::from_millis(50),
            )
            .await
    });
    let agent = f.agent.clone();
    let hello = f.hello.clone();
    let client = f.client.clone();
    let watchdog = tokio::spawn(async move { agent.keepalive_with_client(client, &hello).await });
    let result = tokio::time::timeout(Duration::from_secs(1), f.seen.recv()).await;
    replication.abort();
    watchdog.abort();
    drop(held);
    let body = result.expect("blocked telemetry stalled watchdog").unwrap();
    assert!(body.get("terminal_updates").is_none());
}

#[tokio::test]
async fn optional_receipt_statistics_cannot_block_control_poll() {
    let mut f = fixture().await;
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(f.agent.store.pool.acquire().await.unwrap());
    }
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        f.agent.poll_job(&f.client, &f.hello, Lane::Control),
    )
    .await
    .expect("optional queue statistics blocked poll")
    .unwrap();
    assert!(result.is_none());
    let body = f.seen.recv().await.unwrap();
    assert!(
        body["receipt_delivery"].is_null(),
        "unavailable must not become a healthy zero queue"
    );
}

#[tokio::test]
async fn rejected_keepalive_is_not_contact_and_recovery_never_executes_work() {
    let f = fixture().await;
    for status in [401, 403, 409, 429, 503] {
        f.status.store(status, Ordering::SeqCst);
        let error = f
            .agent
            .keepalive_once(&f.client, &f.hello)
            .await
            .unwrap_err();
        let failure = Failure::classify(&error);
        assert_eq!(failure.http_status, Some(status));
        assert!(f.agent.liveness.due_at(Instant::now()));
        assert!(!error.to_string().contains(&f.agent.config.device_token));
    }
    f.status.store(204, Ordering::SeqCst);
    f.agent.keepalive_once(&f.client, &f.hello).await.unwrap();
    assert!(!f.agent.liveness.due_at(Instant::now()));
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind IN ('terminal','local_operation')")
            .fetch_one(&f.agent.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn poll_health_writes_are_bounded_when_sqlite_is_exhausted() {
    let f = fixture().await;
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(f.agent.store.pool.acquire().await.unwrap());
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        f.agent
            .save_poll_health(&f.hello, Lane::Control, json!({"state":"retrying"})),
    )
    .await
    .expect("optional log persistence blocked poll recovery");
}

#[tokio::test]
async fn late_poll_health_cannot_replace_newer_revision_or_another_session() {
    let f = fixture().await;
    f.agent.poll_health_revision.store(100, Ordering::Relaxed);
    f.agent
        .save_poll_health(&f.hello, Lane::Control, json!({"state":"recovered"}))
        .await;
    f.agent.poll_health_revision.store(1, Ordering::Relaxed);
    f.agent
        .save_poll_health(&f.hello, Lane::Control, json!({"state":"retrying"}))
        .await;
    let saved: Value = f
        .agent
        .store
        .get("runtime", "poll_health_control")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved["state"], "recovered");
    f.agent
        .store
        .put(
            "runtime",
            "readiness",
            &json!({"session":"new-session"}),
            i64::MAX,
        )
        .await
        .unwrap();
    f.agent.poll_health_revision.store(200, Ordering::Relaxed);
    f.agent
        .save_poll_health(&f.hello, Lane::Control, json!({"state":"retrying"}))
        .await;
    let later: Value = f
        .agent
        .store
        .get("runtime", "poll_health_control")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later, saved);
}

#[test]
fn late_long_poll_reply_does_not_invent_a_fresh_lease() {
    let contact = Liveness::default();
    let now = Instant::now();
    contact.observed(now - Duration::from_secs(30));
    assert!(
        contact.due_at(now),
        "reply age must not extend Gateway last_seen"
    );
    assert!(
        contact.observed_after(now - Duration::from_secs(1)),
        "fresh authenticated reply still resolves a startup session conflict"
    );
}

#[test]
fn reordered_replies_do_not_regress_a_newer_confirmed_contact() {
    let contact = Liveness::default();
    let now = Instant::now();
    contact.observed(now);
    contact.observed(now - Duration::from_secs(30));
    assert!(!contact.due_at(now + Duration::from_secs(9)));
    assert!(contact.due_at(now + Duration::from_secs(10)));
}

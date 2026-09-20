//! Verify write elimination without weakening session ownership or progress.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use remote_hosts_code::{DeviceRegistration, GatewayConfig, gateway::Gateway, hash, now, random};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;
struct Fixture {
    _dir: tempfile::TempDir,
    g: Gateway,
    device: String,
    token: String,
    hello: Value,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let token = random();
        let device = uuid::Uuid::new_v4().to_string();
        let g = Gateway::new(GatewayConfig {
            public_url: "https://poll-budget.example".into(),
            bind: "127.0.0.1:0".into(),
            state_dir: dir.path().join("state"),
            owner: "owner".into(),
            password_hash: "unused".into(),
            redirect_uris: vec![],
            allowed_origins: remote_hosts_code::default_mcp_client_origins(),
            devices: vec![DeviceRegistration {
                id: device.clone(),
                name: "synthetic".into(),
                token_hash: hash(&token),
                scopes: vec![
                    "code:read".into(),
                    "code:write".into(),
                    "terminal:exec".into(),
                ],
            }],
        })
        .await
        .unwrap();
        sqlx::query("CREATE TABLE poll_audit(n INTEGER NOT NULL)")
            .execute(&g.store.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO poll_audit VALUES(0)")
            .execute(&g.store.pool)
            .await
            .unwrap();
        sqlx::query("CREATE TRIGGER poll_count_insert AFTER INSERT ON kv WHEN NEW.kind='online' BEGIN UPDATE poll_audit SET n=n+1; END").execute(&g.store.pool).await.unwrap();
        sqlx::query("CREATE TRIGGER poll_count_update AFTER UPDATE ON kv WHEN NEW.kind='online' BEGIN UPDATE poll_audit SET n=n+1; END").execute(&g.store.pool).await.unwrap();
        let hello = json!({"version":"fixture","wire_protocol":2,"session":random(),"roots":[],"allow_write":true,"allow_exec":true,"lanes":["read"],"poll_wait_ms":100});
        Self {
            _dir: dir,
            g,
            device,
            token,
            hello,
        }
    }
    async fn poll(&self, body: Value) -> StatusCode {
        self.g
            .router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/device/poll")
                    .header("host", "poll-budget.example")
                    .header("authorization", format!("Bearer {}", self.token))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }
    async fn writes(&self) -> i64 {
        sqlx::query_scalar("SELECT n FROM poll_audit")
            .fetch_one(&self.g.store.pool)
            .await
            .unwrap()
    }
}
#[tokio::test]
async fn five_concurrent_lanes_commit_one_snapshot_not_five() {
    let f = Fixture::new().await;
    let results = futures_util::future::join_all(
        ["read", "write", "control", "terminal", "transfer"].map(|lane| {
            let mut body = f.hello.clone();
            body["lanes"] = json!([lane]);
            f.poll(body)
        }),
    )
    .await;
    assert!(results.iter().all(|s| *s == StatusCode::OK));
    assert_eq!(f.writes().await, 1);
}
#[tokio::test]
async fn cached_empty_poll_completes_while_writer_is_held() {
    let f = Fixture::new().await;
    assert_eq!(f.poll(f.hello.clone()).await, StatusCode::OK);
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(f.g.store.pool.acquire().await.unwrap());
    }
    drop(connections);
    let tx = f.g.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), f.poll(f.hello.clone())).await;
    tx.rollback().await.unwrap();
    assert_eq!(
        result.expect("duplicate snapshot tried to write"),
        StatusCode::OK
    );
    assert_eq!(f.writes().await, 1);
}
#[tokio::test]
async fn new_state_is_committed_and_stale_durable_lease_is_renewed() {
    let f = Fixture::new().await;
    assert_eq!(f.poll(f.hello.clone()).await, StatusCode::OK);
    let mut changed = f.hello.clone();
    changed["allow_exec"] = json!(false);
    assert_eq!(f.poll(changed.clone()).await, StatusCode::OK);
    assert_eq!(f.writes().await, 2);
    sqlx::query(
        "UPDATE kv SET value=json_set(value,'$.last_seen',?) WHERE kind='online' AND key=?",
    )
    .bind(now() - 11)
    .bind(&f.device)
    .execute(&f.g.store.pool)
    .await
    .unwrap();
    let before = f.writes().await;
    assert_eq!(f.poll(changed).await, StatusCode::OK);
    assert_eq!(f.writes().await, before + 1);
}
#[tokio::test]
async fn cached_identity_never_bypasses_a_changed_durable_session() {
    let f = Fixture::new().await;
    assert_eq!(f.poll(f.hello.clone()).await, StatusCode::OK);
    sqlx::query("UPDATE kv SET value=json_set(value,'$.hello.session',?,'$.last_seen',?) WHERE kind='online' AND key=?").bind(random()).bind(now()).bind(&f.device).execute(&f.g.store.pool).await.unwrap();
    assert_eq!(f.poll(f.hello.clone()).await, StatusCode::CONFLICT);
}
#[tokio::test]
async fn new_progress_is_not_suppressed_and_elapsed_only_does_not_write() {
    let f = Fixture::new().await;
    let id = uuid::Uuid::new_v4().to_string();
    let mut b = f.hello.clone();
    b["progress"] = json!([{"operation_id":id,"phase":"running","bytes_done":0,"total_bytes":null,"resumed_bytes":0,"retry_count":0,"elapsed_ms":10,"average_bps":0.0,"instantaneous_bps":0.0,"updated_at":now()}]);
    assert_eq!(f.poll(b.clone()).await, StatusCode::OK);
    b["progress"][0]["elapsed_ms"] = json!(999);
    assert_eq!(f.poll(b.clone()).await, StatusCode::OK);
    assert_eq!(f.writes().await, 1);
    b["progress"][0]["phase"] = json!("failed");
    assert_eq!(f.poll(b).await, StatusCode::OK);
    assert_eq!(f.writes().await, 2);
}

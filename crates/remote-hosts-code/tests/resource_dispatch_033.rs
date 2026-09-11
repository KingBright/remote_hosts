//! Resource-filter protocol compatibility and isolation without live devices.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig,
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Gateway, String, Value) {
    let d = tempfile::tempdir().unwrap();
    let token = random();
    let device = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: d.path().join("state"),
        owner: "test".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: device,
            name: "fixture".into(),
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
    let hello = json!({"version":"test","session":random(),"roots":[],"allow_write":true,"allow_exec":true,"poll_wait_ms":100});
    (d, g, token, hello)
}
fn ws(g: &Gateway) -> String {
    format!("{}:{}", g.config.devices[0].id, uuid::Uuid::new_v4())
}
async fn queue(g: &Gateway, tool: &str, args: Value, age: i64) -> String {
    let j = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: g.config.devices[0].id.clone(),
        owner: "test".into(),
        tool: tool.into(),
        arguments: args,
    };
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'queued',?)")
        .bind(&j.id)
        .bind(&j.device_id)
        .bind(random())
        .bind(random())
        .bind(serde_json::to_string(&j).unwrap())
        .bind(now() - age)
        .execute(&g.store.pool)
        .await
        .unwrap();
    j.id
}
async fn poll(g: &Gateway, token: &str, args: Value) -> (StatusCode, Value) {
    let out = g
        .router()
        .unwrap()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/device/poll")
                .header("host", "fixture.example")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(args.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = out.status();
    let bytes = to_bytes(out.into_body(), 65536).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
async fn blocked_terminal_input_does_not_hide_new_commands_or_cancel() {
    let (_d, g, token, mut hello) = fixture().await;
    let t = uuid::Uuid::new_v4().to_string();
    let slow = queue(&g, "terminal_input", json!({"terminal_id":t}), 30).await;
    let fast = queue(&g, "terminal_exec", json!({"command":"true"}), 20).await;
    let cancel = queue(&g, "terminal_cancel", json!({"terminal_id":t}), 10).await;
    hello["resource_filter"] = json!({"terminal_inputs":[t],"all_writes":true});
    hello["lanes"] = json!(["terminal"]);
    assert_eq!(poll(&g, &token, hello.clone()).await.1["job"]["id"], fast);
    hello["lanes"] = json!(["control"]);
    assert_eq!(poll(&g, &token, hello).await.1["job"]["id"], cancel);
    let (state,): (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id=?")
        .bind(slow)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(state, "queued");
}
#[tokio::test]
async fn write_filter_skips_busy_workspace_and_legacy_requests_can_resume_it() {
    let (_d, g, token, mut hello) = fixture().await;
    let a = ws(&g);
    let b = ws(&g);
    let first = queue(&g, "code_apply_edits", json!({"workspace_id":a}), 30).await;
    let second = queue(&g, "code_apply_edits", json!({"workspace_id":b}), 20).await;
    hello["lanes"] = json!(["write"]);
    hello["resource_filter"] = json!({"write_workspaces":[a]});
    assert_eq!(poll(&g, &token, hello.clone()).await.1["job"]["id"], second);
    hello.as_object_mut().unwrap().remove("resource_filter");
    hello.as_object_mut().unwrap().remove("poll_wait_ms");
    assert_eq!(poll(&g, &token, hello).await.1["job"]["id"], first);
}
#[tokio::test]
async fn malformed_or_foreign_resource_filters_are_rejected_before_claiming_jobs() {
    let (_d, g, token, hello) = fixture().await;
    let own = ws(&g);
    let foreign = format!("{}:{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    let id = queue(&g, "code_apply_edits", json!({"workspace_id":own}), 10).await;
    for filter in [
        json!({"write_workspaces":[foreign]}),
        json!({"terminal_inputs":["not-uuid"]}),
        json!({"write_workspaces":vec![own.clone();1025]}),
    ] {
        let mut request = hello.clone();
        request["resource_filter"] = filter;
        assert_eq!(poll(&g, &token, request).await.0, StatusCode::BAD_REQUEST);
    }
    let mut request = hello;
    request["poll_wait_ms"] = json!(1);
    assert_eq!(poll(&g, &token, request).await.0, StatusCode::BAD_REQUEST);
    let (state,): (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(state, "queued");
}
#[tokio::test]
async fn filtered_long_poll_ends_without_cancelling_or_rewriting_queued_work() {
    let (_d, g, token, mut hello) = fixture().await;
    let a = ws(&g);
    let id = queue(&g, "code_apply_edits", json!({"workspace_id":a}), 10).await;
    hello["lanes"] = json!(["write"]);
    hello["resource_filter"] = json!({"all_writes":true});
    let (status, out) =
        tokio::time::timeout(std::time::Duration::from_secs(1), poll(&g, &token, hello))
            .await
            .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(out["job"].is_null());
    let (state,): (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(state, "queued");
}

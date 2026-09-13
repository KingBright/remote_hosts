//! 0.10.2 reliability contracts: wire negotiation, fleet convergence and semantic replay guards.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig, auth::Principal, gateway::Gateway, hash, random,
};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Gateway, String, Principal, String) {
    let dir = tempfile::tempdir().unwrap();
    let token = random();
    let device = uuid::Uuid::new_v4().to_string();
    let root = dir.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let gateway = Gateway::new(GatewayConfig {
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().join("state"),
        owner: "owner".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: device.clone(),
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
    let principal = Principal {
        owner: "owner".into(),
        scopes: vec![
            "code:read".into(),
            "code:write".into(),
            "terminal:exec".into(),
        ],
    };
    (dir, gateway, token, principal, device)
}

async fn poll(gateway: &Gateway, token: &str, hello: Value) -> (StatusCode, Value) {
    let response = gateway
        .router()
        .unwrap()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/device/poll")
                .header("host", "fixture.example")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(hello.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

async fn current_hello(gateway: &Gateway, principal: &Principal, root: &str) -> Value {
    let devices = gateway
        .dispatch(principal, "devices_list", json!({}))
        .await
        .unwrap();
    json!({
        "version":env!("CARGO_PKG_VERSION"),
        "wire_protocol":devices["gateway"]["wire_protocol"],
        "tool_schema_revision":devices["gateway"]["tool_schema_revision"],
        "skill_revision":devices["gateway"]["skill_revision"],
        "skill_consistent":true,
        "platform":"macos","arch":"aarch64","home_dir":"/Users/test",
        "session":random(),"roots":[root],"allow_write":true,"allow_exec":true,"poll_wait_ms":100
    })
}

#[tokio::test]
async fn gateway_accepts_legacy_wire_but_rejects_future_wire() {
    let (dir, gateway, token, _principal, _device) = fixture().await;
    let legacy = json!({"version":"0.10.1","session":random(),"roots":[dir.path().join("root")],"allow_write":true,"allow_exec":true,"poll_wait_ms":100});
    assert_eq!(poll(&gateway, &token, legacy).await.0, StatusCode::OK);
    let future = json!({"version":"99.0.0","wire_protocol":3,"session":random(),"roots":[dir.path().join("root")],"allow_write":true,"allow_exec":true,"poll_wait_ms":100});
    let (status, body) = poll(&gateway, &token, future).await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(body["error"], "gateway_version_incompatible");
    assert_eq!(body["gateway_wire_protocol"], 2);
}

#[tokio::test]
async fn fleet_convergence_requires_wire_schema_and_skill_revision() {
    let (dir, gateway, token, principal, _device) = fixture().await;
    let root = dir.path().join("root").to_string_lossy().into_owned();
    let hello = current_hello(&gateway, &principal, &root).await;
    assert_eq!(
        poll(&gateway, &token, hello.clone()).await.0,
        StatusCode::OK
    );
    let fleet = gateway
        .dispatch(
            &principal,
            "fleet_status",
            json!({"desired_version":env!("CARGO_PKG_VERSION")}),
        )
        .await
        .unwrap();
    assert_eq!(fleet["all_converged"], true);
    assert_eq!(fleet["summary"]["devices_converged"], 1);
    assert_eq!(fleet["devices"][0]["capabilities"]["platform"], "macos");
    assert_eq!(fleet["devices"][0]["tool_schema_converged"], true);
    assert_eq!(fleet["devices"][0]["skill_converged"], true);

    let mut stale = hello;
    // Same live Agent session can update its reported Skill revision. A distinct
    // fresh session is intentionally rejected until the previous lease expires.
    stale["skill_revision"] = json!("0".repeat(64));
    assert_eq!(poll(&gateway, &token, stale).await.0, StatusCode::OK);
    let fleet = gateway
        .dispatch(
            &principal,
            "fleet_status",
            json!({"desired_version":env!("CARGO_PKG_VERSION")}),
        )
        .await
        .unwrap();
    assert_eq!(fleet["all_converged"], false);
    assert_eq!(fleet["devices"][0]["skill_converged"], false);
}

#[tokio::test]
async fn semantic_guard_blocks_new_keys_until_unknown_outcome_is_resolved() {
    let (dir, gateway, token, principal, device) = fixture().await;
    let root = dir.path().join("root").to_string_lossy().into_owned();
    let hello = current_hello(&gateway, &principal, &root).await;
    assert_eq!(poll(&gateway, &token, hello).await.0, StatusCode::OK);
    let workspace = format!("{}:{}", device, uuid::Uuid::new_v4());
    let first = gateway
        .dispatch(
            &principal,
            "terminal_exec",
            json!({
                "workspace_id":workspace,"idempotency_key":"first-key","command":"printf guarded"
            }),
        )
        .await
        .unwrap();
    let operation = first["operation_id"].as_str().unwrap().to_owned();

    let blocked = gateway.dispatch(&principal, "terminal_exec", json!({
        "workspace_id":workspace,"idempotency_key":"different-key","command":"printf guarded"
    })).await.unwrap_err().to_string();
    assert!(blocked.contains("semantic_operation_guarded"));
    assert!(blocked.contains(&operation));

    sqlx::query("UPDATE jobs SET result=?,state='done' WHERE id=?")
        .bind(r#"{"error":"outcome_unknown"}"#)
        .bind(&operation)
        .execute(&gateway.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE semantic_guards SET state='outcome_unknown' WHERE operation_id=?")
        .bind(&operation)
        .execute(&gateway.store.pool)
        .await
        .unwrap();

    let resolved = gateway.dispatch(&principal, "outcome_resolve", json!({
        "operation_id":operation,"resolution":"verified_not_applied","idempotency_key":"resolve-one"
    })).await.unwrap();
    assert_eq!(resolved["state"], "resolved");
    assert_eq!(resolved["replayed"], false);
    let duplicate = gateway.dispatch(&principal, "outcome_resolve", json!({
        "operation_id":operation,"resolution":"verified_not_applied","idempotency_key":"resolve-one"
    })).await.unwrap();
    assert_eq!(duplicate["duplicate"], true);

    let retry = gateway.dispatch(&principal, "terminal_exec", json!({
        "workspace_id":workspace,"idempotency_key":"after-resolution","command":"printf guarded"
    })).await.unwrap();
    assert_ne!(retry["operation_id"], operation);
}

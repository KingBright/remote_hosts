//! Opt-in local relay benchmark. No production credentials, network or device restarts.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::Principal,
    gateway::{Gateway, Job},
    hash, random,
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tower::ServiceExt;

#[tokio::test]
#[ignore = "local latency probe; run with --ignored --nocapture"]
async fn local_relay_latency() {
    let state = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("probe.rs"), "fn answer() -> u32 { 42 }\n").unwrap();
    let credential = random();
    let device = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: remote_hosts_code::default_mcp_client_origins(),
        public_url: "https://bench.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: state.path().into(),
        owner: "benchmark".into(),
        password_hash: "unused-device-only-fixture".into(),
        redirect_uris: vec!["https://chatgpt.com/connector_platform_oauth_redirect".into()],
        devices: vec![DeviceRegistration {
            id: device.clone(),
            name: "local-bench".into(),
            token_hash: hash(&credential),
            scopes: vec!["code:read".into()],
        }],
    })
    .await
    .unwrap();
    let a = Agent::new(AgentConfig {
        gateway_url: g.config.public_url.clone(),
        device_id: device.clone(),
        device_token: credential.clone(),
        state_dir: local.path().into(),
        roots: vec![root.path().into()],
        allow_write: false,
        allow_exec: false,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let router = g.router().unwrap();
    let session = random();
    let worker = tokio::spawn(async move {
        loop {
            let req = |path: &str, body: Value| {
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("host", "bench.example")
                    .header("authorization", format!("Bearer {credential}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap()
            };
            let response = router
                .clone()
                .oneshot(req(
                    "/device/poll",
                    json!({"session":session,"roots":[],"allow_write":false,"allow_exec":false}),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 512 * 1024).await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if value["job"].is_null() {
                continue;
            }
            let job: Job = serde_json::from_value(value["job"].clone()).unwrap();
            let result = a.execute(&job).await.unwrap();
            let response = router
                .clone()
                .oneshot(req(
                    "/device/result",
                    json!({"operation_id":job.id,"result":result}),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    });
    let p = Principal {
        owner: "benchmark".into(),
        scopes: vec!["code:read".into()],
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if g.dispatch(&p, "devices_list", json!({})).await.unwrap()["devices"][0]["online"] == true
        {
            break;
        }
        assert!(Instant::now() < deadline, "worker did not connect");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let ws = g
        .dispatch(
            &p,
            "workspace_open",
            json!({"device_id":device,"root":root.path(),"idempotency_key":"benchmark-open"}),
        )
        .await
        .unwrap()["workspace"]["id"]
        .clone();
    let mut elapsed = Vec::new();
    let mut response_bytes = 0;
    for sample in 0..23 {
        let start = Instant::now();
        let result=g.dispatch(&p,"code_read",json!({"workspace_id":ws,"requests":[{"path":"probe.rs","start_line":1,"end_line":1}]})).await.unwrap();
        assert_eq!(result["ranges"][0]["text"], "fn answer() -> u32 { 42 }\n");
        if sample >= 3 {
            elapsed.push(start.elapsed().as_secs_f64() * 1000.0);
            response_bytes += serde_json::to_vec(&result).unwrap().len();
        }
    }
    elapsed.sort_by(f64::total_cmp);
    println!(
        "{}",
        json!({"benchmark":"in_process_http_gateway_agent_sqlite_file_read","samples":elapsed.len(),"mean_ms":elapsed.iter().sum::<f64>()/elapsed.len() as f64,"p50_ms":elapsed[9],"p95_ms":elapsed[18],"total_response_bytes":response_bytes,"production_network":false})
    );
    worker.abort();
    let _ = worker.await;
}

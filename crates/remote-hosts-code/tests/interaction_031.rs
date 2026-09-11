//! Real process and HTTP boundary checks with temporary state and synthetic keys.
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    routing::{get, post},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    gateway::{Gateway, Job},
    hash, now, random, tools,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Agent, String) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let agent = Agent::new(AgentConfig {
        gateway_url: "https://example.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: random(),
        state_dir: d.path().join("state"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let out = agent
        .execute(&job(
            &agent,
            "workspace_open",
            json!({"device_id":agent.config.device_id,"root":root,"idempotency_key":"open"}),
        ))
        .await
        .unwrap();
    (d, agent, out["workspace"]["id"].as_str().unwrap().into())
}
fn job(a: &Agent, tool: &str, args: Value) -> Job {
    Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: a.config.device_id.clone(),
        owner: "test".into(),
        tool: tool.into(),
        arguments: args,
    }
}

#[tokio::test]
async fn short_command_returns_output_exit_and_replay_does_not_rerun() {
    let (_d, a, ws) = fixture().await;
    let request = job(
        &a,
        "terminal_exec",
        json!({"workspace_id":ws,"command":"printf x >> count; printf 'hello'; exit 7","wait_ms":2000,"idempotency_key":"once","timeout_seconds":5}),
    );
    let first = a.execute(&request).await.unwrap();
    assert_eq!(first["output"], "hello");
    assert_eq!(first["terminal"]["exit_code"], 7);
    assert_eq!(first["terminal"]["output_complete"], true);
    assert!(first["next_action"].is_null());
    assert_eq!(first, a.execute(&request).await.unwrap());
    assert_eq!(
        std::fs::read(a.config.roots[0].join("count")).unwrap(),
        b"x"
    );
}
#[tokio::test]
async fn short_wait_does_not_cancel_process_and_cursor_is_reusable() {
    let (_d, a, ws) = fixture().await;
    let request = job(
        &a,
        "terminal_exec",
        json!({"workspace_id":ws,"command":"printf a; sleep 0.2; printf b","wait_ms":50,"idempotency_key":"longer","timeout_seconds":5}),
    );
    let first = a.execute(&request).await.unwrap();
    let id = first["terminal_id"].as_str().unwrap();
    let mut cursor = first["cursor"].as_u64().unwrap();
    let mut text = first["output"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let out = a
                .execute(&job(
                    &a,
                    "terminal_read",
                    json!({"workspace_id":ws,"terminal_id":id,"cursor":cursor}),
                ))
                .await
                .unwrap();
            text.push_str(out["output"].as_str().unwrap());
            cursor = out["cursor"].as_u64().unwrap();
            if out["terminal"]["exit_code"] == 0 && out["terminal"]["output_complete"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(text, "ab");
}
#[tokio::test]
async fn readiness_endpoint_is_authenticated_scoped_and_does_not_refresh_online_lease() {
    let (d, a, _ws) = fixture().await;
    let other = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        public_url: "https://ready.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: d.path().join("gateway"),
        owner: "test".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![
            DeviceRegistration {
                id: a.config.device_id.clone(),
                name: "one".into(),
                token_hash: hash(&a.config.device_token),
                scopes: vec!["code:read".into()],
            },
            DeviceRegistration {
                id: other.clone(),
                name: "two".into(),
                token_hash: hash(random()),
                scopes: vec!["code:read".into()],
            },
        ],
    })
    .await
    .unwrap();
    let old = now() - 100;
    g.store.put("online",&a.config.device_id,&json!({"hello":{"version":"0.3.1","session":random(),"roots":[],"allow_write":true,"allow_exec":true},"last_seen":old}),i64::MAX).await.unwrap();
    let request = |token: &str| {
        Request::builder()
            .uri("/device/readiness")
            .header("host", "ready.example")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        g.router()
            .unwrap()
            .oneshot(request("wrong"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = g
        .router()
        .unwrap()
        .oneshot(request(&a.config.device_token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 32768).await.unwrap()).unwrap();
    assert_eq!(body["device_id"], a.config.device_id);
    assert_eq!(body["ready"], false);
    assert_eq!(body["last_seen"], old);
    assert_eq!(
        g.store
            .get::<Value>("online", &a.config.device_id)
            .await
            .unwrap()
            .unwrap()["last_seen"],
        old
    );
    assert!(
        g.store
            .get::<Value>("online", &other)
            .await
            .unwrap()
            .is_none()
    );
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    g.store.put("online", &a.config.device_id, &json!({"hello":{"version":"0.3.1","session":random(),"roots":[],"allow_write":true,"allow_exec":true},"last_seen":now()+300}), i64::MAX).await.unwrap();
    let future = g
        .router()
        .unwrap()
        .oneshot(request(&a.config.device_token))
        .await
        .unwrap();
    let body: Value =
        serde_json::from_slice(&to_bytes(future.into_body(), 32768).await.unwrap()).unwrap();
    assert_eq!(
        body["ready"], false,
        "a future-dated observation is not ready"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_failure_retries_in_same_process_then_records_all_lane_acknowledgements() {
    let (_d, mut a, _ws) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut a.config).gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let router = Router::new()
        .route(
            "/healthz",
            get(move || {
                let c = counter.clone();
                async move {
                    if c.fetch_add(1, Ordering::SeqCst) == 0 {
                        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})))
                    } else {
                        (
                            StatusCode::OK,
                            Json(json!({"dispatch_protocol":2,"resource_dispatch_protocol":1,"transfer_protocol":2})),
                        )
                    }
                }
            }),
        )
        .route(
            "/device/poll",
            post(|| async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Json(json!({"job":null}))
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let runner = a.clone();
    let task = tokio::spawn(async move { runner.run().await });
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(ready) = a.store.get::<Value>("runtime", "readiness").await.unwrap()
                && ready["lanes"].as_object().is_some_and(|v| v.len() == 5)
            {
                assert_eq!(ready["pid"], std::process::id());
                break;
            }
            assert!(
                !task.is_finished(),
                "transient startup failure exited the agent"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    task.abort();
    server.abort();
}
#[test]
fn new_read_and_wait_options_are_schema_checked() {
    assert!(
        tools::validate(
            "terminal_exec",
            &json!({"workspace_id":"w","idempotency_key":"a","command":"true","wait_ms":2001})
        )
        .is_err()
    );
    assert!(tools::validate("code_read",&json!({"workspace_id":"w","allow_partial":true,"requests":[{"path":"a","start_line":1,"end_line":1,"line_byte_offset":3}]})).is_ok());
}

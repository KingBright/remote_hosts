//! Loopback fault injection; no production credentials, files or services.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn check_delivery_loss(commit_before_error: bool) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = random();
    let device = uuid::Uuid::new_v4().to_string();
    let mut a = Agent::new(AgentConfig {
        gateway_url: format!("https://{address}"),
        device_id: device.clone(),
        device_token: token.clone(),
        state_dir: d.path().join("agent"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    Arc::make_mut(&mut a.config).gateway_url = format!("http://{address}");
    let g = Gateway::new(GatewayConfig {
        public_url: format!("https://{address}"),
        bind: "127.0.0.1:0".into(),
        state_dir: d.path().join("gateway"),
        owner: "test".into(),
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
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let router: Router = g.router().unwrap().layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let calls = c.clone();
            async move {
                if request.uri().path() == "/device/result"
                    && calls.fetch_add(1, Ordering::SeqCst) == 0
                {
                    if commit_before_error {
                        let _accepted = next.run(request).await;
                    }
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                next.run(request).await
            }
        },
    ));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let id = uuid::Uuid::new_v4().to_string();
    let j = Job {
        id: id.clone(),
        device_id: device.clone(),
        owner: "test".into(),
        tool: "workspace_open".into(),
        arguments: json!({"device_id":device,"root":root,"idempotency_key":"open-once"}),
    };
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'queued',?)")
        .bind(&id)
        .bind(&device)
        .bind(random())
        .bind(random())
        .bind(serde_json::to_string(&j).unwrap())
        .bind(now())
        .execute(&g.store.pool)
        .await
        .unwrap();
    let running = a.clone();
    let worker = tokio::spawn(async move { running.run().await });
    // Six seconds is well below the gateway's 30-second redispatch window.
    let completed = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let (result,): (Option<String>,) = sqlx::query_as("SELECT result FROM jobs WHERE id=?")
                .bind(&id)
                .fetch_one(&g.store.pool)
                .await
                .unwrap();
            if let Some(result) = result
                && calls.load(Ordering::SeqCst) >= 2
            {
                let (pending,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM receipt_outbox")
                    .fetch_one(&a.store.pool)
                    .await
                    .unwrap();
                if pending == 0 {
                    break serde_json::from_str::<Value>(&result).unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    worker.abort();
    let _ = worker.await;
    server.abort();
    let result = completed.expect(
        "lost receipt remained pending until gateway redispatch instead of being redelivered",
    );
    assert!(result.get("error").is_none(), "{result}");
    assert!(calls.load(Ordering::SeqCst) >= 2);
    let (workspaces,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='workspace'")
        .fetch_one(&a.store.pool)
        .await
        .unwrap();
    assert_eq!(
        workspaces, 1,
        "receipt retry must not create another workspace"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_receipt_failure_recovers_without_job_redispatch_or_reexecution() {
    check_delivery_loss(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_but_lost_ack_replays_exact_receipt_and_keeps_one_side_effect() {
    check_delivery_loss(true).await;
}

//! Deterministic concurrency probes; all devices, credentials and files are temporary.
use axum::{Router, http::StatusCode, routing::get};
use axum::{
    body::{Body, Bytes, to_bytes},
    http::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use remote_hosts_code::{AgentConfig, agent::Agent, gateway::Job, random};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig, auth::Principal, gateway::Gateway, hash, now,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Agent, String) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("probe.txt"), "read while transferring\n").unwrap();
    let agent = Agent::new(AgentConfig {
        gateway_url: "https://example.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: random(),
        state_dir: dir.path().join("state"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let result = agent
        .execute(&job(
            &agent,
            "workspace_open",
            json!({"device_id":agent.config.device_id,"root":root,"idempotency_key":"open"}),
        ))
        .await
        .unwrap();
    (
        dir,
        agent,
        result["workspace"]["id"].as_str().unwrap().into(),
    )
}
fn job(agent: &Agent, tool: &str, arguments: Value) -> Job {
    Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: agent.config.device_id.clone(),
        owner: "fixture".into(),
        tool: tool.into(),
        arguments,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_transfer_does_not_block_reads_or_terminal_start() {
    let (_dir, mut agent, ws) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::make_mut(&mut agent.config).gateway_url =
        format!("http://{}", listener.local_addr().unwrap());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/device/transfer-control/{id}",
                    get(|| async {
                        axum::Json(json!({"protocol":2,"revision":0,"cancel_requested":false}))
                    }),
                )
                .route(
                    "/device/transfer-status/{id}",
                    axum::routing::post(|| async { axum::Json(json!({"accepted":true})) }),
                )
                .route(
                    "/device/file-source/{id}",
                    get(move || {
                        let e = e.clone();
                        let r = r.clone();
                        async move {
                            e.notify_one();
                            r.notified().await;
                            StatusCode::GONE
                        }
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let transfer = job(
        &agent,
        "file_upload",
        json!({"workspace_id":ws,"path":"file.bin","idempotency_key":"slow","file":{"file_id":"synthetic","download_url":"https://files.oaiusercontent.com/f"}}),
    );
    let a = agent.clone();
    let slow = tokio::spawn(async move { a.execute(&transfer).await });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let read_job = job(
        &agent,
        "code_read",
        json!({"workspace_id":ws,"requests":[{"path":"probe.txt","start_line":1,"end_line":1}]}),
    );
    let term_job = job(
        &agent,
        "terminal_exec",
        json!({"workspace_id":ws,"idempotency_key":"fast","command":"printf fast","timeout_seconds":5}),
    );
    let (read, terminal) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(1), agent.execute(&read_job)),
        tokio::time::timeout(Duration::from_secs(1), agent.execute(&term_job))
    );
    release.notify_one();
    slow.await.unwrap().unwrap();
    server.abort();
    assert!(read.is_ok(), "a stalled transfer blocked code_read");
    assert!(terminal.is_ok(), "a stalled transfer blocked terminal_exec");
    let read = read.unwrap().unwrap();
    assert_eq!(read["ranges"][0]["text"], "read while transferring\n");
    let terminal = terminal.unwrap().unwrap();
    let terminal_id = terminal["terminal_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let out = agent
                .execute(&job(
                    &agent,
                    "terminal_read",
                    json!({"workspace_id":ws,"terminal_id":terminal_id}),
                ))
                .await
                .unwrap();
            if out["terminal"]["exit_code"] == 0 {
                assert_eq!(out["output"], "fast");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_duplicate_mutation_returns_one_durable_result() {
    let (_dir, agent, ws) = fixture().await;
    let create = job(
        &agent,
        "code_apply_edits",
        json!({"workspace_id":ws,"idempotency_key":"once","files":[{"path":"once.txt","expected_version":"absent","action":"create","content":"exactly once"}]}),
    );
    let (first, second) = tokio::join!(agent.execute(&create), agent.execute(&create));
    assert_eq!(first.as_ref().unwrap(), second.as_ref().unwrap());
    assert!(first.unwrap().get("error").is_none());
    let mut conflict = create.clone();
    conflict.arguments["files"][0]["content"] = json!("different");
    assert!(
        agent
            .execute(&conflict)
            .await
            .unwrap_err()
            .to_string()
            .contains("fingerprint")
    );
}

async fn gateway(agent: &Agent, dir: &std::path::Path, origin: String) -> (Gateway, String) {
    let other = random();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: remote_hosts_code::default_mcp_client_origins(),
        public_url: origin,
        bind: "127.0.0.1:0".into(),
        state_dir: dir.join("gateway"),
        owner: "fixture".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![
            DeviceRegistration {
                id: agent.config.device_id.clone(),
                name: "first".into(),
                token_hash: hash(&agent.config.device_token),
                scopes: vec![
                    "code:read".into(),
                    "code:write".into(),
                    "terminal:exec".into(),
                ],
            },
            DeviceRegistration {
                id: uuid::Uuid::new_v4().to_string(),
                name: "second".into(),
                token_hash: hash(&other),
                scopes: vec!["code:read".into(), "code:write".into()],
            },
        ],
    })
    .await
    .unwrap();
    (g, other)
}
async fn queued(
    g: &Gateway,
    device: &str,
    tool: &str,
    args: Value,
    state: &str,
    updated: i64,
) -> Job {
    let job = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: device.into(),
        owner: "fixture".into(),
        tool: tool.into(),
        arguments: args,
    };
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,?,?)")
        .bind(&job.id)
        .bind(device)
        .bind(random())
        .bind(random())
        .bind(serde_json::to_string(&job).unwrap())
        .bind(state)
        .bind(updated)
        .execute(&g.store.pool)
        .await
        .unwrap();
    job
}
async fn post(g: &Gateway, credential: &str, path: &str, body: Value) -> Response {
    let host = reqwest::Url::parse(&g.config.public_url)
        .unwrap()
        .authority()
        .to_owned();
    g.router()
        .unwrap()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("host", host)
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn lane_filter_skips_transfer_backlog_and_heartbeat_renews_only_own_active_jobs() {
    let (dir, agent, ws) = fixture().await;
    let (g, _) = gateway(&agent, dir.path(), "https://fixture.example".into()).await;
    let slow = queued(
        &g,
        &agent.config.device_id,
        "file_download",
        json!({"workspace_id":ws}),
        "dispatched",
        now() - 60,
    )
    .await;
    let other = queued(
        &g,
        &g.config.devices[1].id,
        "file_download",
        json!({}),
        "dispatched",
        now() - 60,
    )
    .await;
    let fast = queued(
        &g,
        &agent.config.device_id,
        "code_read",
        json!({"workspace_id":ws}),
        "queued",
        now(),
    )
    .await;
    let hello = json!({"version":"test","session":random(),"roots":[],"allow_write":true,"allow_exec":true,"lanes":["read"],"active_operations":[slow.id]});
    let response = post(
        &g,
        &agent.config.device_token,
        "/device/poll",
        hello.clone(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let output: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(output["job"]["id"], fast.id);
    let mut heartbeat = hello.clone();
    heartbeat["active_operations"] = json!([slow.id, other.id]);
    let response = post(
        &g,
        &agent.config.device_token,
        "/device/heartbeat",
        heartbeat.clone(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (own_updated,): (i64,) = sqlx::query_as("SELECT updated FROM jobs WHERE id=?")
        .bind(&slow.id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    let (other_updated,): (i64,) = sqlx::query_as("SELECT updated FROM jobs WHERE id=?")
        .bind(&other.id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert!(own_updated >= now() - 2);
    assert!(other_updated <= now() - 50);
    heartbeat["session"] = json!(random());
    assert_eq!(
        post(
            &g,
            &agent.config.device_token,
            "/device/heartbeat",
            heartbeat
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let mut bad = hello;
    bad["active_operations"] = json!(["not-an-operation-id"]);
    assert_eq!(
        post(&g, &agent.config.device_token, "/device/poll", bad)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_agent_poll_loop_keeps_serving_during_stalled_transfer() {
    let (dir, mut agent, ws) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (g, _) = gateway(
        &agent,
        dir.path(),
        format!("https://{}", listener.local_addr().unwrap()),
    )
    .await;
    Arc::make_mut(&mut agent.config).gateway_url =
        format!("http://{}", listener.local_addr().unwrap());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let router = g.router().unwrap().layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let e = e.clone();
            let r = r.clone();
            async move {
                if request.uri().path().starts_with("/device/file-source/") {
                    e.notify_one();
                    r.notified().await;
                    return StatusCode::GONE.into_response();
                }
                next.run(request).await
            }
        },
    ));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let a = agent.clone();
    let worker = tokio::spawn(async move { a.run_isolated().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while g
            .store
            .get::<Value>("online", &agent.config.device_id)
            .await
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let principal = Principal {
        owner: "fixture".into(),
        scopes: vec![
            "code:read".into(),
            "code:write".into(),
            "terminal:exec".into(),
        ],
    };
    let gg = g.clone();
    let p = principal.clone();
    let w = ws.clone();
    let transfer = tokio::spawn(async move {
        gg.dispatch(&p,"file_upload",json!({"workspace_id":w,"path":"slow.bin","idempotency_key":"slow-loop","file":{"file_id":"synthetic","download_url":"https://files.oaiusercontent.com/f"}})).await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let result=tokio::time::timeout(Duration::from_secs(2),g.dispatch(&principal,"code_read",json!({"workspace_id":ws,"requests":[{"path":"probe.txt","start_line":1,"end_line":1}]}))).await;
    let terminal=tokio::time::timeout(Duration::from_secs(2),g.dispatch(&principal,"terminal_exec",json!({"workspace_id":ws,"command":"printf loop-ok","idempotency_key":"term-loop","timeout_seconds":5}))).await;
    release.notify_one();
    transfer.await.unwrap().unwrap();
    let read = result.expect("poll loop blocked the read").unwrap();
    assert_eq!(read["ranges"][0]["text"], "read while transferring\n");
    let terminal = terminal.expect("poll loop blocked terminal_exec").unwrap();
    let id = terminal["terminal_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let out = agent
                .execute(&job(
                    &agent,
                    "terminal_read",
                    json!({"workspace_id":ws,"terminal_id":id}),
                ))
                .await
                .unwrap();
            if out["terminal"]["exit_code"] == 0 {
                assert_eq!(out["output"], "loop-ok");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    let _ = worker.await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_gateway_stream_does_not_block_another_device() {
    let (dir, agent, _) = fixture().await;
    let (g, other_credential) = gateway(&agent, dir.path(), "https://fixture.example".into()).await;
    let first = queued(
        &g,
        &agent.config.device_id,
        "file_download",
        json!({"path":"first.bin"}),
        "dispatched",
        now(),
    )
    .await;
    let second = queued(
        &g,
        &g.config.devices[1].id,
        "file_download",
        json!({"path":"second.bin"}),
        "dispatched",
        now(),
    )
    .await;
    let request = |id: &str, credential: &str, body: Body| {
        Request::builder()
            .method("POST")
            .uri(format!("/device/files/{id}"))
            .header("host", "fixture.example")
            .header("authorization", format!("Bearer {credential}"))
            .header("x-file-size", "3")
            .header("x-file-sha256", hash(b"abc"))
            .body(body)
            .unwrap()
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|value| (value, rx))
    });
    let req = request(
        &first.id,
        &agent.config.device_token,
        Body::from_stream(stream),
    );
    let router = g.router().unwrap();
    let blocked = tokio::spawn(async move { router.oneshot(req).await.unwrap() });
    tx.send(Ok(Bytes::from_static(b"a"))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let objects = g.config.state_dir.join("file-objects");
            if let Ok(mut entries) = std::fs::read_dir(objects)
                && let Some(Ok(entry)) = entries.next()
                && entry.metadata().is_ok_and(|meta| meta.len() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let second_response = g
        .router()
        .unwrap()
        .oneshot(request(&second.id, &other_credential, Body::from("abc")))
        .await
        .unwrap();
    tx.send(Ok(Bytes::from_static(b"bc"))).await.unwrap();
    drop(tx);
    assert_eq!(blocked.await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        second_response.status(),
        StatusCode::OK,
        "another device was blocked by the first stream"
    );
}

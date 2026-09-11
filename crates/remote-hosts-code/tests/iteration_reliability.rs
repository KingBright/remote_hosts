//! Control-path and telemetry tests with synthetic devices, never production credentials.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::Principal,
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

fn job(agent: &Agent, tool: &str, arguments: Value) -> Job {
    Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: agent.config.device_id.clone(),
        owner: "test-owner".into(),
        tool: tool.into(),
        arguments,
    }
}
async fn fixture() -> (tempfile::TempDir, Agent, String) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("a.txt"), "old").unwrap();
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
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_and_read_remain_available_when_terminal_inputs_are_busy() {
    let (_dir, agent, ws) = fixture().await;
    let term=agent.execute(&job(&agent,"terminal_exec",json!({"workspace_id":ws,"command":"stty raw -echo; printf 'ready\\n'; sleep 30","pty":true,"timeout_seconds":10,"idempotency_key":"start"}))).await.unwrap();
    let id = term["terminal_id"].as_str().unwrap().to_owned();
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
            if out["output"].as_str().unwrap().contains("ready") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut inputs = Vec::new();
    for n in 0..4 {
        let request = job(
            &agent,
            "terminal_input",
            json!({"workspace_id":ws,"terminal_id":id,"text":"x".repeat(16384),"idempotency_key":format!("input-{n}")}),
        );
        let a = agent.clone();
        inputs.push(tokio::spawn(async move { a.execute(&request).await }));
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    let blocked = inputs.iter().any(|task| !task.is_finished());
    let read = tokio::time::timeout(
        Duration::from_secs(1),
        agent.execute(&job(
            &agent,
            "terminal_read",
            json!({"workspace_id":ws,"terminal_id":id}),
        )),
    )
    .await;
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        agent.execute(&job(
            &agent,
            "terminal_cancel",
            json!({"workspace_id":ws,"terminal_id":id,"idempotency_key":"cancel"}),
        )),
    )
    .await;
    for task in inputs {
        let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    }
    assert!(blocked, "fixture did not exercise pending terminal input");
    assert!(read.is_ok(), "status read was blocked by terminal input");
    assert_eq!(
        cancelled.expect("cancel was blocked").unwrap()["terminal"]["state"],
        "cancelled"
    );
}

#[tokio::test]
async fn progress_is_observable_device_scoped_and_rejects_invalid_values() {
    let (dir, agent, _) = fixture().await;
    let other = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        public_url: "https://progress.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().join("gateway"),
        owner: "test-owner".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![
            DeviceRegistration {
                id: agent.config.device_id.clone(),
                name: "first".into(),
                token_hash: hash(&agent.config.device_token),
                scopes: vec!["code:read".into(), "code:write".into()],
            },
            DeviceRegistration {
                id: other.clone(),
                name: "second".into(),
                token_hash: hash(random()),
                scopes: vec!["code:read".into(), "code:write".into()],
            },
        ],
    })
    .await
    .unwrap();
    let mut ids = Vec::new();
    for device in [&agent.config.device_id, &other] {
        let request = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: device.clone(),
            owner: "test-owner".into(),
            tool: "file_upload".into(),
            arguments: json!({}),
        };
        sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'dispatched',?)")
            .bind(&request.id)
            .bind(device)
            .bind(random())
            .bind(random())
            .bind(serde_json::to_string(&request).unwrap())
            .bind(now())
            .execute(&g.store.pool)
            .await
            .unwrap();
        ids.push(request.id);
    }
    let session = random();
    g.store.put("online",&agent.config.device_id,&json!({"hello":{"session":session,"version":"test","roots":[],"allow_write":true,"allow_exec":true},"last_seen":now()}),i64::MAX).await.unwrap();
    let snapshot = |id: &str| json!({"operation_id":id,"phase":"transferring","bytes_done":17,"total_bytes":28,"resumed_bytes":10,"retry_count":1,"elapsed_ms":1000,"average_bps":17.0,"instantaneous_bps":7.0,"updated_at":now()});
    let request = json!({"session":session,"version":"test","roots":[],"allow_write":true,"allow_exec":true,"active_operations":ids,"progress":[snapshot(&ids[0]),snapshot(&ids[1])]});
    let post = |body: Value| {
        g.router().unwrap().oneshot(
            Request::builder()
                .method("POST")
                .uri("/device/heartbeat")
                .header("host", "progress.example")
                .header(
                    "authorization",
                    format!("Bearer {}", agent.config.device_token),
                )
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
    };
    assert_eq!(
        post(request.clone()).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    let p = Principal {
        owner: "test-owner".into(),
        scopes: vec!["code:read".into(), "code:write".into()],
    };
    let result = g
        .dispatch(&p, "operation_get", json!({"operation_id":ids[0]}))
        .await
        .unwrap();
    assert_eq!(result["progress"]["bytes_done"], 17);
    assert_eq!(result["progress"]["total_bytes"], 28);
    assert_eq!(result["progress_stale"], false);
    assert!(
        g.store
            .get::<Value>("operation_progress", &ids[1])
            .await
            .unwrap()
            .is_none()
    );
    let mut bad = request;
    bad["progress"][0]["bytes_done"] = json!(999999999);
    assert_eq!(post(bad).await.unwrap().status(), StatusCode::BAD_REQUEST);
}

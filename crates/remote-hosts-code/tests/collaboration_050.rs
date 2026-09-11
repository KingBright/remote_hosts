//! Actual HTTP/device-loop checks, isolated temporary state and synthetic credentials.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::Principal,
    files::Workspace,
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;
struct Fixture {
    _d: tempfile::TempDir,
    g: Gateway,
    a: Agent,
    ws: Workspace,
    token: String,
    listen: Option<tokio::net::TcpListener>,
}
impl Fixture {
    async fn new() -> Self {
        let d = tempfile::tempdir().unwrap();
        let listen = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listen.local_addr().unwrap();
        let device = uuid::Uuid::new_v4().to_string();
        let token = random();
        let root = d.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let g = Gateway::new(GatewayConfig {
            public_url: format!("https://{addr}"),
            bind: addr.to_string(),
            state_dir: d.path().join("gateway"),
            owner: "owner".into(),
            password_hash: "unused".into(),
            devices: vec![DeviceRegistration {
                id: device.clone(),
                name: "test".into(),
                token_hash: hash(&token),
                scopes: vec![
                    "code:read".into(),
                    "code:write".into(),
                    "terminal:exec".into(),
                ],
            }],
            redirect_uris: vec![],
        })
        .await
        .unwrap();
        let mut a = Agent::new(AgentConfig {
            gateway_url: format!("https://{addr}"),
            device_id: device.clone(),
            device_token: token.clone(),
            state_dir: d.path().join("agent"),
            roots: vec![root.canonicalize().unwrap()],
            allow_write: true,
            allow_exec: true,
            shell: "/bin/sh".into(),
        })
        .await
        .unwrap();
        Arc::make_mut(&mut a.config).gateway_url = format!("http://{addr}");
        let ws = Workspace {
            id: format!("{}:{}", device, uuid::Uuid::new_v4()),
            device_id: device,
            root: root.canonicalize().unwrap(),
        };
        a.store
            .put("workspace", &ws.id, &ws, i64::MAX)
            .await
            .unwrap();
        Self {
            _d: d,
            g,
            a,
            ws,
            token,
            listen: Some(listen),
        }
    }
    fn p(&self) -> Principal {
        Principal {
            owner: "owner".into(),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        }
    }
    async fn request(&self, path: &str, v: Value, token: &str) -> (StatusCode, Value) {
        let host = reqwest::Url::parse(&self.g.config.public_url)
            .unwrap()
            .authority()
            .to_owned();
        let r = self
            .g
            .router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .method("POST")
                    .header("host", host)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(v.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = r.status();
        let bytes = to_bytes(r.into_body(), 65536).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    async fn queued(&self, tool: &str) -> Job {
        let j = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: self.ws.device_id.clone(),
            owner: "owner".into(),
            tool: tool.into(),
            arguments: json!({"workspace_id":self.ws.id}),
        };
        sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'queued',?)")
            .bind(&j.id)
            .bind(&j.device_id)
            .bind(random())
            .bind(hash("x"))
            .bind(serde_json::to_string(&j).unwrap())
            .bind(now())
            .execute(&self.g.store.pool)
            .await
            .unwrap();
        j
    }
}
#[tokio::test]
async fn drain_filters_new_work_but_keeps_read_control_and_lease_identity() {
    let f = Fixture::new().await;
    let lease = hash("one");
    let r = json!({"action":"acquire","lease_id":lease,"ttl_seconds":30});
    assert_eq!(
        f.request("/device/maintenance", r.clone(), "bad").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.request("/device/maintenance", r, &f.token).await.0,
        StatusCode::OK
    );
    let write = f.queued("code_apply_edits").await;
    let read = f.queued("code_read").await;
    let poll = json!({"version":"0.5.0","session":random(),"roots":[],"allow_write":true,"allow_exec":true,"poll_wait_ms":100});
    let first = f.request("/device/poll", poll.clone(), &f.token).await;
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(first.1["job"]["id"], read.id);
    assert!(f.request("/device/poll", poll.clone(), &f.token).await.1["job"].is_null());
    assert_eq!(
        f.request(
            "/device/maintenance",
            json!({"action":"release","lease_id":hash("other")}),
            &f.token
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        f.request(
            "/device/maintenance",
            json!({"action":"release","lease_id":lease}),
            &f.token
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        f.request("/device/poll", poll, &f.token).await.1["job"]["id"],
        write.id
    );
}
#[tokio::test]
async fn upgrade_reports_are_scoped_and_do_not_expose_raw_errors() {
    let f = Fixture::new().await;
    let lease = hash("owner");
    f.request(
        "/device/maintenance",
        json!({"action":"acquire","lease_id":lease}),
        &f.token,
    )
    .await;
    let report = json!({"action":"report","lease_id":lease,"receipt":{"version":"0.5.0","state":"failed","service_changed":false,"error":"active work password=secret","token":"bad"}});
    assert_eq!(
        f.request("/device/maintenance", report, &f.token).await.0,
        StatusCode::OK
    );
    let v =
        f.g.dispatch(&f.p(), "devices_list", json!({}))
            .await
            .unwrap();
    assert_eq!(
        v["devices"][0]["upgrade"]["receipt"]["error_code"],
        "device_busy"
    );
    assert!(!v.to_string().contains("secret"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operation_get_observes_terminal_exit_without_new_read_jobs() {
    let mut f = Fixture::new().await;
    let listener = f.listen.take().unwrap();
    let router = f.g.router().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let a = f.a.clone();
    let agent = tokio::spawn(async move { a.run().await });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let v =
                f.g.dispatch(&f.p(), "devices_list", json!({}))
                    .await
                    .unwrap();
            if v["devices"][0]["online"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let result=f.g.dispatch(&f.p(),"terminal_exec",json!({"workspace_id":f.ws.id,"idempotency_key":"one","command":"sleep 0.15; printf ready; exit 7","timeout_seconds":5})).await.unwrap();
    let id = result["operation_id"].as_str().unwrap().to_owned();
    let observed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let v =
                f.g.dispatch(&f.p(), "operation_get", json!({"operation_id":id}))
                    .await
                    .unwrap();
            if v["terminal_observation"]["terminal"]["exit_code"] == 7
                && v["terminal_observation"]["terminal"]["output_complete"] == true
            {
                break v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed["terminal_observation"]["stale"], false);
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    agent.abort();
    server.abort();
}
#[tokio::test]
async fn edit_journal_tracks_each_published_file_and_final_state() {
    let f = Fixture::new().await;
    let j = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: f.ws.device_id.clone(),
        owner: "owner".into(),
        tool: "code_apply_edits".into(),
        arguments: json!({"workspace_id":f.ws.id,"idempotency_key":"edits","files":[{"path":"a","action":"create","expected_version":"absent","content":"a"},{"path":"b","action":"create","expected_version":"absent","content":"b"}]}),
    };
    let result = f.a.execute(&j).await.unwrap();
    assert!(result.get("error").is_none(), "{result}");
    let receipt: Value = serde_json::from_slice(
        &std::fs::read(
            f.a.config
                .state_dir
                .join("edits")
                .join(format!("{}.json", j.id)),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["status"], "completed");
    assert!(
        receipt["files"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["status"] == "applied")
    );
    let after =
        f.a.execute(&Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: f.ws.device_id.clone(),
            owner: "owner".into(),
            tool: "workspace_context".into(),
            arguments: json!({"workspace_id":f.ws.id}),
        })
        .await
        .unwrap();
    assert!(
        after["events"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["entity_id"] == j.id && e["state"] == "done")
    );
}

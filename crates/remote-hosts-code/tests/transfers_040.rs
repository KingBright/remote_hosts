//! 0.4 end-to-end protocol regression tests. Temporary state and synthetic keys only.
include!("support/transfer_040_cases.rs");
include!("support/gateway_process_040.rs");
include!("support/recovery_041.rs");
include!("support/collaboration_050.rs");
include!("support/scale_070.rs");
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request as AxumRequest, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::Principal,
    files::Workspace,
    gateway::{Gateway, Job},
    hash, now, random, tools,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;
const CHUNK: usize = 4 * 1024 * 1024;
struct Fixture {
    _dir: tempfile::TempDir,
    g: Gateway,
    token: String,
    agent: Agent,
    ws: Workspace,
    listen: Option<tokio::net::TcpListener>,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let token = random();
        let device = uuid::Uuid::new_v4().to_string();
        let root = dir.path().join("project");
        std::fs::create_dir(&root).unwrap();
        let g = Gateway::new(GatewayConfig {
            allowed_origins: remote_hosts_code::default_mcp_client_origins(),
            public_url: format!("https://{address}"),
            bind: address.to_string(),
            state_dir: dir.path().join("gateway"),
            owner: "fixture".into(),
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
        g.store.put("online", &device, &json!({"hello":{"version":"0.4.0","session":random(),"roots":[root],"allow_write":true,"allow_exec":true},"last_seen":now(),"runtime_features":{"protocol":1,"names":["durable_transfers_v2","workspace_context_v1"]}}), i64::MAX).await.unwrap();
        let config = AgentConfig {
            gateway_url: format!("https://{address}"),
            device_id: device.clone(),
            device_token: token.clone(),
            state_dir: dir.path().join("agent"),
            roots: vec![root.canonicalize().unwrap()],
            allow_write: true,
            allow_exec: true,
            shell: "/bin/sh".into(),
        };
        let mut agent = Agent::new(config).await.unwrap();
        Arc::make_mut(&mut agent.config).gateway_url = format!("http://{address}");
        let ws = Workspace {
            id: format!("{device}:{}", uuid::Uuid::new_v4()),
            device_id: device,
            root: root.canonicalize().unwrap(),
        };
        agent
            .store
            .put("workspace", &ws.id, &ws, i64::MAX)
            .await
            .unwrap();
        Self {
            _dir: dir,
            g,
            token,
            agent,
            ws,
            listen: Some(listener),
        }
    }
    fn principal(&self) -> Principal {
        Principal {
            owner: "fixture".into(),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        }
    }
    async fn job(&self, name: &str, extra: Value) -> Job {
        let mut args =
            json!({"workspace_id":self.ws.id,"path":"data.bin","idempotency_key":random()});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let j = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: self.ws.device_id.clone(),
            owner: "fixture".into(),
            tool: name.into(),
            arguments: args,
        };
        sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'dispatched',?)")
            .bind(&j.id)
            .bind(&j.device_id)
            .bind(random())
            .bind(hash("test"))
            .bind(serde_json::to_string(&j).unwrap())
            .bind(now())
            .execute(&self.g.store.pool)
            .await
            .unwrap();
        j
    }
    async fn request(
        &self,
        g: &Gateway,
        path: &str,
        method: &str,
        body: Vec<u8>,
        headers: &[(&str, String)],
    ) -> Response {
        let host = reqwest::Url::parse(&g.config.public_url)
            .unwrap()
            .authority()
            .to_owned();
        let mut r = Request::builder()
            .uri(path)
            .method(method)
            .header("host", host)
            .header("authorization", format!("Bearer {}", self.token));
        for (k, v) in headers {
            r = r.header(*k, v);
        }
        g.router()
            .unwrap()
            .oneshot(r.body(Body::from(body)).unwrap())
            .await
            .unwrap()
    }
    async fn json(&self, path: &str, body: Value) -> Response {
        self.request(
            &self.g,
            path,
            "POST",
            serde_json::to_vec(&body).unwrap(),
            &[("content-type", "application/json".into())],
        )
        .await
    }
    fn serve(&mut self, router: Option<Router>) -> tokio::task::JoinHandle<()> {
        let listener = self.listen.take().unwrap();
        let router = router.unwrap_or_else(|| self.g.router().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        })
    }
}
async fn value(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}
#[test]
fn catalog_exposes_three_scoped_workflow_tools_and_refresh_file_parameter() {
    assert_eq!(
        remote_hosts_code::release_manifest()["tool_count"],
        tools::catalog().len()
    );
    assert!(tools::catalog().iter().any(|t| t.name == "task_context"));
    let catalog = tools::catalog();
    let refresh = catalog
        .iter()
        .find(|t| t.name == "transfer_resume")
        .unwrap();
    let v = serde_json::to_value(refresh).unwrap();
    assert_eq!(v["_meta"]["openai/fileParams"], json!(["file"]));
    assert_eq!(v["annotations"]["readOnlyHint"], false);
    tools::validate(
        "transfer_cancel",
        &json!({"operation_id":"id","idempotency_key":"k"}),
    )
    .unwrap();
    assert!(
        tools::validate(
            "transfer_cancel",
            &json!({"operation_id":"id","idempotency_key":"k","command":"no"})
        )
        .is_err()
    );
}
#[tokio::test]
async fn gateway_chunks_survive_reopen_and_reject_corruption_and_wrong_offsets() {
    let f = Fixture::new().await;
    let bytes: Vec<u8> = (0..CHUNK + 193).map(|i| (i % 251) as u8).collect();
    let j = f
        .job("file_download", json!({"expected_version":hash(&bytes)}))
        .await;
    let base = format!("/device/transfers/{}", j.id);
    assert_eq!(
        f.json(&base, json!({"size":bytes.len(),"sha256":hash(&bytes)}))
            .await
            .status(),
        StatusCode::OK
    );
    // Explicit calls avoid borrowing temporary route strings across futures.
    let route = format!("{base}/chunk");
    let bad = f
        .request(
            &f.g,
            &route,
            "POST",
            bytes[..CHUNK].to_vec(),
            &[
                ("x-transfer-offset", "0".into()),
                ("x-chunk-sha256", hash("bad")),
            ],
        )
        .await;
    assert_eq!(bad.status(), StatusCode::CONFLICT);
    let first = f
        .request(
            &f.g,
            &route,
            "POST",
            bytes[..CHUNK].to_vec(),
            &[
                ("x-transfer-offset", "0".into()),
                ("x-chunk-sha256", hash(&bytes[..CHUNK])),
            ],
        )
        .await;
    assert_eq!(value(first).await["confirmed_bytes"], CHUNK);
    let duplicate = f
        .request(
            &f.g,
            &route,
            "POST",
            bytes[..CHUNK].to_vec(),
            &[
                ("x-transfer-offset", "0".into()),
                ("x-chunk-sha256", hash(&bytes[..CHUNK])),
            ],
        )
        .await;
    assert_eq!(value(duplicate).await["confirmed_bytes"], CHUNK);
    let wrong = f
        .request(
            &f.g,
            &route,
            "POST",
            vec![7; 30],
            &[
                ("x-transfer-offset", (CHUNK + 1).to_string()),
                ("x-chunk-sha256", hash(vec![7; 30])),
            ],
        )
        .await;
    assert_eq!(wrong.status(), StatusCode::CONFLICT);
    let restarted = Gateway::new((*f.g.config).clone()).await.unwrap();
    let status = f.request(&restarted, &base, "GET", vec![], &[]).await;
    assert_eq!(value(status).await["confirmed_bytes"], CHUNK);
    let last = f
        .request(
            &restarted,
            &route,
            "POST",
            bytes[CHUNK..].to_vec(),
            &[
                ("x-transfer-offset", CHUNK.to_string()),
                ("x-chunk-sha256", hash(&bytes[CHUNK..])),
            ],
        )
        .await;
    assert_eq!(value(last).await["confirmed_bytes"], bytes.len());
    let end = f
        .request(&restarted, &format!("{base}/complete"), "POST", vec![], &[])
        .await;
    assert_eq!(value(end).await["completed"], true);
    assert_eq!(
        std::fs::read(
            f.g.config
                .state_dir
                .join("file-objects")
                .join(format!("{}.blob", j.id))
        )
        .unwrap(),
        bytes
    );
    let again = f
        .request(&restarted, &format!("{base}/complete"), "POST", vec![], &[])
        .await;
    assert_eq!(value(again).await["completed"], true);
}
#[tokio::test]
async fn empty_snapshot_and_duplicate_finalize_are_valid() {
    let f = Fixture::new().await;
    let j = f.job("file_download", json!({})).await;
    let base = format!("/device/transfers/{}", j.id);
    let init = f.json(&base, json!({"size":0,"sha256":hash([])})).await;
    assert_eq!(value(init).await["confirmed_bytes"], 0);
    for _ in 0..2 {
        assert_eq!(
            value(
                f.request(&f.g, &format!("{base}/complete"), "POST", vec![], &[])
                    .await
            )
            .await["completed"],
            true
        );
    }
}
#[tokio::test]
async fn resume_keeps_job_identity_and_rejects_wrong_owner_or_changed_file() {
    let f = Fixture::new().await;
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}}),
        )
        .await;
    sqlx::query("UPDATE jobs SET state='awaiting_source' WHERE id=?")
        .bind(&j.id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    let args = json!({"operation_id":j.id,"idempotency_key":"resume","file":{"file_id":"different","download_url":"https://files.oaiusercontent.com/f?sig=synthetic"}});
    assert!(
        f.g.dispatch(&f.principal(), "transfer_resume", args.clone())
            .await
            .is_err()
    );
    let mut args = args;
    args["file"]["file_id"] = json!("same");
    let mut other = f.principal();
    other.owner = "other".into();
    assert!(
        f.g.dispatch(&other, "transfer_resume", args.clone())
            .await
            .is_err()
    );
    let out =
        f.g.dispatch(&f.principal(), "transfer_resume", args.clone())
            .await
            .unwrap();
    assert_eq!(out["operation_id"], j.id);
    assert_eq!(out["transfer_revision"], 1);
    assert_eq!(
        f.g.dispatch(&f.principal(), "transfer_resume", args)
            .await
            .unwrap(),
        out
    );
    let (saved,): (String,) = sqlx::query_as("SELECT request FROM jobs WHERE id=?")
        .bind(&j.id)
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&saved).unwrap(),
        serde_json::to_value(&j).unwrap()
    );
}
#[tokio::test]
async fn paused_source_is_not_cached_as_permanent_done() {
    let mut f = Fixture::new().await;
    let server = f.serve(None);
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}}),
        )
        .await;
    let out = f.agent.execute(&j).await.unwrap();
    assert_eq!(out["state"], "awaiting_source");
    assert_eq!(out["resumable"], true);
    let record: Value = f
        .agent
        .store
        .get("local_operation", &j.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record["resumable"], true);
    assert!(record["result"].is_null());
    let (state,): (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id=?")
        .bind(&j.id)
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    assert_eq!(state, "awaiting_source");
    assert_eq!(
        f.agent.execute(&j).await.unwrap()["diagnostic"]["code"],
        "explicit_resume_required"
    );
    server.abort();
}
#[tokio::test]
async fn cancelling_a_paused_import_cleans_staging_without_touching_target() {
    let mut f = Fixture::new().await;
    let server = f.serve(None);
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}}),
        )
        .await;
    assert_eq!(
        f.agent.execute(&j).await.unwrap()["state"],
        "awaiting_source"
    );
    let data = f
        .agent
        .config
        .state_dir
        .join("transfers-v2")
        .join(format!("{}.data", j.id));
    std::fs::write(&data, b"partial").unwrap();
    let cancel =
        f.g.dispatch(
            &f.principal(),
            "transfer_cancel",
            json!({"operation_id":j.id,"idempotency_key":"cancel"}),
        )
        .await
        .unwrap();
    assert_eq!(cancel["state"], "cancellation_requested");
    assert_eq!(cancel["cleanup_complete"], false);
    let result = f.agent.execute(&j).await.unwrap();
    assert_eq!(result["state"], "cancelled");
    assert_eq!(result["cleanup_complete"], true);
    assert!(!data.exists());
    assert!(!f.ws.root.join("data.bin").exists());
    server.abort();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_export_resumes_saved_snapshot_not_the_changed_source() {
    let mut f = Fixture::new().await;
    let bytes: Vec<u8> = (0..CHUNK + 111).map(|i| (i % 251) as u8).collect();
    std::fs::write(f.ws.root.join("data.bin"), &bytes).unwrap();
    let blocking = Arc::new(AtomicBool::new(true));
    let checkpoint_reached = Arc::new(tokio::sync::Notify::new());
    let flag = blocking.clone();
    let router = f.g.router().unwrap().layer(middleware::from_fn_with_state(
        (flag, checkpoint_reached.clone()),
        |State((flag, reached)): State<(Arc<AtomicBool>, Arc<tokio::sync::Notify>)>,
         r: AxumRequest,
         next: Next| async move {
            if r.uri().path().ends_with("/chunk")
                && r.headers()
                    .get("x-transfer-offset")
                    .and_then(|v| v.to_str().ok())
                    == Some("4194304")
                && flag.load(Ordering::SeqCst)
            {
                // The next request proves the first durable chunk was acknowledged.
                reached.notify_one();
                std::future::pending::<()>().await;
            }
            next.run(r).await
        },
    ));
    let server = f.serve(Some(router));
    let j = f
        .job("file_download", json!({"expected_version":hash(&bytes)}))
        .await;
    let a = f.agent.clone();
    let work = j.clone();
    let task = tokio::spawn(async move { a.execute(&work).await });
    // Synchronize fault injection with the protocol boundary instead of polling
    // SQLite at 100 Hz while the first 4 MiB snapshot is being fsynced.
    let reached =
        tokio::time::timeout(Duration::from_secs(30), checkpoint_reached.notified()).await;
    if reached.is_err() {
        task.abort();
        server.abort();
    }
    reached.expect("sender did not reach the second-chunk fault-injection barrier");
    let receiver =
        f.g.store
            .get::<Value>("transfer_receiver", &j.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        receiver["offset"], CHUNK,
        "interrupt only after a durable acknowledged checkpoint"
    );
    task.abort();
    let _ = task.await;
    std::fs::write(
        f.ws.root.join("data.bin"),
        b"source edited after captured snapshot",
    )
    .unwrap();
    blocking.store(false, Ordering::SeqCst);
    let mut cfg = (*f.agent.config).clone();
    let origin = cfg.gateway_url.clone();
    cfg.gateway_url = origin.replacen("http:", "https:", 1);
    let mut restarted = Agent::new(cfg).await.unwrap();
    Arc::make_mut(&mut restarted.config).gateway_url = origin;
    let result = tokio::time::timeout(Duration::from_secs(15), restarted.execute(&j))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["state"], "completed");
    assert_eq!(result["sha256"], hash(&bytes));
    assert_eq!(result["size"], bytes.len());
    assert_eq!(
        std::fs::read(
            f.g.config
                .state_dir
                .join("file-objects")
                .join(format!("{}.blob", j.id))
        )
        .unwrap(),
        bytes
    );
    assert_eq!(
        std::fs::read(f.ws.root.join("data.bin")).unwrap(),
        b"source edited after captured snapshot"
    );
    server.abort();
}
#[tokio::test]
async fn workspace_context_is_scoped_and_has_a_stable_cursor() {
    let f = Fixture::new().await;
    let other = format!("{}:{}", f.ws.device_id, uuid::Uuid::new_v4());
    f.agent
        .store
        .put(
            "terminal",
            "one",
            &json!({"id":"one","workspace_id":f.ws.id,"created_at":1,"state":"exited"}),
            i64::MAX,
        )
        .await
        .unwrap();
    f.agent
        .store
        .put(
            "terminal",
            "private",
            &json!({"id":"private","workspace_id":other,"created_at":2,"state":"running"}),
            i64::MAX,
        )
        .await
        .unwrap();
    let make = |args| Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: f.ws.device_id.clone(),
        owner: "fixture".into(),
        tool: "workspace_context".into(),
        arguments: args,
    };
    let first = f
        .agent
        .execute(&make(json!({"workspace_id":f.ws.id})))
        .await
        .unwrap();
    assert_eq!(first["terminals"].as_array().unwrap().len(), 1);
    assert_eq!(first["terminals"][0]["id"], "one");
    assert_eq!(first["next_action"], "no_active_work");
    assert_eq!(
        first["freshness"]["heartbeat_is_not_business_progress"],
        true
    );
    assert!(
        first["freshness"]["workspace_observed_at"]
            .as_i64()
            .is_some()
    );
    let again = f
        .agent
        .execute(&make(
            json!({"workspace_id":f.ws.id,"cursor":first["cursor"]}),
        ))
        .await
        .unwrap();
    assert_eq!(again["changed"], false);
}

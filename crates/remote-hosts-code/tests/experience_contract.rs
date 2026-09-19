//! Deterministic fault injection against the real Gateway store/router. Never deploys.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig,
    auth::{Principal, oauth_state_kind},
    contract,
    gateway::{Gateway, Job},
    hash, now, random, receipts,
};
use serde_json::{Value, json};
use tower::ServiceExt;

struct Fixture {
    dir: tempfile::TempDir,
    g: Gateway,
    p: Principal,
    tokens: Vec<String>,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let tokens = vec![random(), random()];
        let scopes = vec![
            "code:read".to_owned(),
            "code:write".to_owned(),
            "terminal:exec".to_owned(),
        ];
        let g = Gateway::new(GatewayConfig {
            public_url: "https://fixture.example".into(),
            bind: "127.0.0.1:0".into(),
            state_dir: dir.path().join("state"),
            owner: "owner".into(),
            password_hash: "unused".into(),
            redirect_uris: vec![],
            allowed_origins: remote_hosts_code::default_mcp_client_origins(),
            devices: tokens
                .iter()
                .enumerate()
                .map(|(i, t)| DeviceRegistration {
                    id: uuid::Uuid::new_v4().to_string(),
                    name: format!("device-{i}"),
                    token_hash: hash(t),
                    scopes: scopes.clone(),
                })
                .collect(),
        })
        .await
        .unwrap();
        for d in &g.config.devices {
            g.store.put("online",&d.id,&json!({"hello":{"version":"0.10.4","session":"s".repeat(64),"roots":[],"allow_write":true,"allow_exec":true},"last_seen":now()}),i64::MAX).await.unwrap();
        }
        Self {
            dir,
            g,
            p: Principal {
                owner: "owner".into(),
                scopes,
            },
            tokens,
        }
    }
    async fn job(&self, device: usize, state: &str, terminal: Option<Value>) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let ws = format!(
            "{}:{}",
            self.g.config.devices[device].id,
            uuid::Uuid::new_v4()
        );
        let job = Job {
            id: id.clone(),
            device_id: self.g.config.devices[device].id.clone(),
            owner: self.p.owner.clone(),
            tool: "terminal_exec".into(),
            arguments: json!({"workspace_id":ws}),
        };
        sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?)")
            .bind(&id)
            .bind(&job.device_id)
            .bind(random())
            .bind(random())
            .bind(serde_json::to_string(&job).unwrap())
            .bind(if state == "done" {
                Some(json!({"terminal_id":id}).to_string())
            } else {
                None
            })
            .bind(state)
            .bind(now())
            .execute(&self.g.store.pool)
            .await
            .unwrap();
        if let Some(mut terminal) = terminal {
            terminal["id"] = json!(id);
            terminal["workspace_id"] = json!(ws);
            self.g.store.put("terminal_observation",&id,&json!({"terminal":terminal,"session":"s".repeat(64),"reported_at":now(),"last_progress_at":now(),"output_preview":"hello","output_cursor_start":0,"output_cursor_end":5,"output_truncated_before":false}),now()+100).await.unwrap();
        }
        id
    }
    async fn link(&self, task: &str, id: &str) {
        self.g
            .store
            .put(
                "task_operation",
                id,
                &json!({"owner":self.p.owner,"task_id":task,"operation_id":id}),
                now() + 100,
            )
            .await
            .unwrap();
    }
    async fn count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
            .fetch_one(&self.g.store.pool)
            .await
            .unwrap()
    }
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Value,
        token: &str,
        headers: &[(&str, String)],
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "fixture.example")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let response = self
            .g
            .router()
            .unwrap()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes))),
        )
    }
    async fn bearer(&self) -> String {
        let token = random();
        self.g.store.put(&oauth_state_kind("access",&token),&hash(&token),&json!({
            "principal":self.p,"client_id":"test-client","resource":"https://fixture.example/mcp","family":random()}),now()+100).await.unwrap();
        token
    }
}
fn terminal(state: &str, updated: i64) -> Value {
    json!({"state":state,"created_at":updated,"updated_at":updated,
    "exit_code":if state=="exited"{json!(0)}else{Value::Null},"output_complete":state=="exited","output_truncated":false,
    "log_format":1,"process_id":1234,"working_directory":"/synthetic/source"})
}
fn request_id() -> String {
    format!("req_{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn receipt_storage_failure_prevents_job_creation() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_receipts BEFORE INSERT ON kv WHEN NEW.kind='request_receipt' BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;")
        .execute(&f.g.store.pool).await.unwrap();
    let id = request_id();
    let value = receipts::invoke(
        &f.g,
        &f.p,
        "workspace_open",
        json!({"device_id":f.g.config.devices[0].id,"root":"/fixture","idempotency_key":"one"}),
        &id,
    )
    .await
    .unwrap();
    assert_eq!(value["error_code"], "receipt_storage_failed");
    assert_eq!(value["execution_state"], "not_started");
    assert_eq!(value["receipt"]["durable"], false);
    assert_eq!(f.count().await, 0);
}

#[tokio::test]
async fn failure_after_enqueue_retains_atomic_request_binding_and_does_not_replay() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_timing BEFORE INSERT ON operation_timing BEGIN SELECT RAISE(ABORT,'injected after job commit'); END;")
        .execute(&f.g.store.pool).await.unwrap();
    let id = request_id();
    let args = json!({"device_id":f.g.config.devices[0].id,"root":"/fixture","idempotency_key":"original","task_id":"release.fixture"});
    let value = receipts::invoke(&f.g, &f.p, "workspace_open", args.clone(), &id)
        .await
        .unwrap();
    assert!(value["operation_id"].as_str().is_some());
    assert_eq!(value["execution_state"], "unknown");
    assert_eq!(f.count().await, 1);
    let observed =
        f.g.dispatch(&f.p, "operation_get", json!({"request_id":id,"wait_ms":0}))
            .await
            .unwrap();
    assert_eq!(observed["operation_id"], value["operation_id"]);
    let repeated = receipts::invoke(&f.g, &f.p, "workspace_open", args.clone(), &id)
        .await
        .unwrap();
    assert_eq!(repeated["operation_id"], value["operation_id"]);
    assert_eq!(f.count().await, 1);
    let mut conflict = args;
    conflict["root"] = json!("/different");
    assert!(
        receipts::invoke(&f.g, &f.p, "workspace_open", conflict, &id)
            .await
            .is_err()
    );
    let context =
        f.g.dispatch(&f.p, "task_context", json!({"task_id":"release.fixture"}))
            .await
            .unwrap();
    assert_eq!(context["summary"]["linked_operations"], 1);
    assert_eq!(context["automatic_replay"], false);
}

#[tokio::test]
async fn querying_a_pending_request_keeps_the_original_handle_separate_from_the_query_receipt() {
    let f = Fixture::new().await;
    let original = request_id();
    let query = request_id();
    f.g.store.put("request_receipt",&original,&json!({"request_id":original,"owner":"owner","tool":"terminal_exec",
        "state":"gateway_received","updated_at":now(),"operation_id":null,"execution_state":"not_started_or_unknown"}),i64::MAX).await.unwrap();
    let value = receipts::invoke(
        &f.g,
        &f.p,
        "operation_get",
        json!({"request_id":original}),
        &query,
    )
    .await
    .unwrap();
    assert_eq!(value["request_id"], query);
    assert_eq!(value["observed_request_id"], original);
    assert_eq!(value["request_receipt"]["request_id"], original);
    assert_eq!(
        value["receipt"]["execution_state"],
        "not_started_or_unknown"
    );
    assert_eq!(value["receipt"]["evidence_complete"], false);
    assert_eq!(f.count().await, 0);
}

#[tokio::test]
async fn final_receipt_save_failure_is_visible_and_keeps_the_original_record() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_final_receipt BEFORE UPDATE ON kv WHEN NEW.kind='request_receipt' AND json_extract(NEW.value,'$.receipt') IS NOT NULL BEGIN SELECT RAISE(ABORT,'injected final receipt failure'); END;")
        .execute(&f.g.store.pool).await.unwrap();
    let id = request_id();
    let value = receipts::invoke(&f.g, &f.p, "devices_list", json!({}), &id)
        .await
        .unwrap();
    assert_eq!(value["receipt"]["durable"], false);
    assert_eq!(value["receipt"]["evidence_complete"], false);
    assert_eq!(
        value["receipt"]["persistence_error"],
        "final_receipt_save_failed"
    );
    let row: Value =
        f.g.store
            .get("request_receipt", &id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(row["state"], "gateway_received");
    let expiry: i64 =
        sqlx::query_scalar("SELECT expires FROM kv WHERE kind='request_receipt' AND key=?")
            .bind(&id)
            .fetch_one(&f.g.store.pool)
            .await
            .unwrap();
    assert_eq!(expiry, i64::MAX);
    f.g.store.prune().await.unwrap();
    assert!(
        f.g.store
            .get::<Value>("request_receipt", &id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn missing_request_is_unknown_not_a_safety_rejection() {
    let f = Fixture::new().await;
    let value =
        f.g.dispatch(&f.p, "operation_get", json!({"request_id":request_id()}))
            .await
            .unwrap();
    assert_eq!(value["state"], "not_observed_or_expired");
    assert_eq!(value["execution_state"], "unknown");
    assert_eq!(value["failure_boundary"], "unknown");
    assert_eq!(f.count().await, 0);
}

#[tokio::test]
async fn task_survives_gateway_restart_and_never_reexecutes() {
    let f = Fixture::new().await;
    let a = f.job(0, "done", Some(terminal("exited", now()))).await;
    let b = f.job(1, "done", Some(terminal("running", now()))).await;
    f.link("multi-device", &a).await;
    f.link("multi-device", &b).await;
    let g = Gateway::new((*f.g.config).clone()).await.unwrap();
    let first = g
        .dispatch(&f.p, "task_context", json!({"task_id":"multi-device"}))
        .await
        .unwrap();
    assert_eq!(first["devices"].as_array().unwrap().len(), 2);
    assert_eq!(first["summary"]["active_in_page"], 1);
    assert_eq!(first["next_operation_id"], b);
    let second = g
        .dispatch(
            &f.p,
            "task_context",
            json!({"task_id":"multi-device","cursor":first["cursor"]}),
        )
        .await
        .unwrap();
    assert_eq!(second["changed"], false);
    assert!(second["operations"].as_array().unwrap().is_empty());
    assert_eq!(f.count().await, 2);
    assert!(f.dir.path().join("state/state.sqlite").is_file());
    let mut limited = f.p.clone();
    limited.scopes = vec!["code:read".into()];
    assert!(
        g.dispatch(&limited, "task_context", json!({"task_id":"multi-device"}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn status_counts_live_terminal_after_submission_completed() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("running", now()))).await;
    let token = f.bearer().await;
    let (status, value) = f
        .request("GET", "/admin/status", json!({}), &token, &[])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["summary"]["active_operations"], 1);
    assert_eq!(value["operations"][0]["operation_id"], id);
    assert_eq!(value["operations"][0]["state"], "process_running");
    assert_eq!(value["operations"][0]["process"]["pid"], 1234);
    assert_eq!(
        value["operations"][0]["receipt"]["business_state"],
        "not_evaluated"
    );
}

#[tokio::test]
async fn completed_observation_replaces_stale_submission_summary_without_rewriting_receipt() {
    let f = Fixture::new().await;
    for exit_code in [0, 7] {
        let mut ended = terminal("exited", now());
        ended["exit_code"] = json!(exit_code);
        let id = f.job(0, "done", Some(ended)).await;
        let original = json!({"terminal_id":id,"state":"running","cursor":0,"output":"",
            "next_action":"terminal_read","terminal":{"id":id,"state":"running","exit_code":null,"output_complete":false}});
        sqlx::query("UPDATE jobs SET result=? WHERE id=?")
            .bind(original.to_string())
            .bind(&id)
            .execute(&f.g.store.pool)
            .await
            .unwrap();
        let value =
            f.g.dispatch(
                &f.p,
                "operation_get",
                json!({"operation_id":id,"wait_ms":0}),
            )
            .await
            .unwrap();
        assert_eq!(value["state"], "exited");
        assert_eq!(value["terminal"]["state"], "exited");
        assert_eq!(value["terminal"]["exit_code"], exit_code);
        assert_eq!(value["receipt"]["process_exit_code"], exit_code);
        assert_eq!(value["output"], "hello");
        assert_eq!(value["cursor"], 5);
        assert_eq!(
            value["next_action"],
            if exit_code == 0 {
                Value::Null
            } else {
                json!("inspect_original_receipt")
            }
        );
        let saved: String = sqlx::query_scalar("SELECT result FROM jobs WHERE id=?")
            .bind(&id)
            .fetch_one(&f.g.store.pool)
            .await
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&saved).unwrap(), original);
    }
}

#[tokio::test]
async fn authenticated_late_running_snapshot_cannot_replace_terminal_outcome() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("exited", now()))).await;
    let raw: String = sqlx::query_scalar("SELECT request FROM jobs WHERE id=?")
        .bind(&id)
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    let job: Job = serde_json::from_str(&raw).unwrap();
    let mut old = terminal("running", now() - 1);
    old["id"] = json!(id);
    old["workspace_id"] = job.arguments["workspace_id"].clone();
    let (status,_)=f.request("POST","/device/heartbeat",json!({"version":"0.10.4","session":"s".repeat(64),"roots":[],"allow_write":true,"allow_exec":true,"terminal_updates":[old]}),&f.tokens[0],&[]).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let value =
        f.g.dispatch(
            &f.p,
            "operation_get",
            json!({"operation_id":id,"wait_ms":0}),
        )
        .await
        .unwrap();
    assert_eq!(value["terminal_observation"]["terminal"]["state"], "exited");
    assert_eq!(value["terminal_observation"]["output"], "hello");
}

#[tokio::test]
async fn output_only_change_moves_cursor_and_unchanged_payload_is_not_repeated() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("running", now()))).await;
    let first =
        f.g.dispatch(
            &f.p,
            "operation_get",
            json!({"operation_id":id,"wait_ms":0}),
        )
        .await
        .unwrap();
    let mut observation: Value =
        f.g.store
            .get("terminal_observation", &id)
            .await
            .unwrap()
            .unwrap();
    observation["output_preview"] = json!("hello!");
    observation["output_cursor_end"] = json!(6);
    f.g.store
        .put("terminal_observation", &id, &observation, now() + 100)
        .await
        .unwrap();
    let changed =
        f.g.dispatch(
            &f.p,
            "operation_get",
            json!({"operation_id":id,"wait_ms":0,"cursor":first["observation"]["cursor"]}),
        )
        .await
        .unwrap();
    assert_eq!(changed["observation"]["changed"], true);
    let unchanged =
        f.g.dispatch(
            &f.p,
            "operation_get",
            json!({"operation_id":id,"wait_ms":0,"cursor":changed["observation"]["cursor"]}),
        )
        .await
        .unwrap();
    assert_eq!(unchanged["observation"]["changed"], false);
    assert_eq!(unchanged["unchanged_payload_omitted"], true);
    assert!(unchanged["receipt"].is_object());
    assert_eq!(f.count().await, 1);
}

#[tokio::test]
async fn real_mcp_call_reports_adapter_and_host_as_independent_layers() {
    let f = Fixture::new().await;
    let token = f.bearer().await;
    let mut headers = contract::adapter_headers(&f.p)
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap().to_owned()))
        .collect::<Vec<_>>();
    headers.push(("MCP-Protocol-Version".into(), "2025-06-18".into()));
    let headers: Vec<(&str, String)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    let (status,value)=f.request("POST","/mcp",json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"devices_list","arguments":{}}}),&token,&headers).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let value = &value["result"]["structuredContent"];
    assert_eq!(
        value["capability_layers"]["connector_catalog"]["status"], "reported_match",
        "{value}"
    );
    assert_eq!(
        value["capability_layers"]["current_session_exposure"]["status"],
        "unknown_not_reported"
    );
    assert!(value["request_id"].as_str().is_some());
}

#[test]
fn checked_adapter_artifact_matches_this_compiled_catalog_and_skill() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("adapter-contract.json");
    let value: Value = serde_json::from_slice(&std::fs::read(path).expect(
        "generate adapter-contract.json using the compiled binary before running this gate",
    ))
    .unwrap();
    contract::verify(&value).unwrap();
}

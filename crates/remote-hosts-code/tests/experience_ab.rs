//! A/B of legacy and preferred workflows on the SAME candidate.
//! Real child processes, authenticated HTTP routers and durable stores; not a live fleet benchmark.
#![cfg(unix)]
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::{Principal, oauth_state_kind},
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;

async fn http(router: &Router, path: &str, token: &str, body: Value) -> Value {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("host", "ab.example")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("MCP-Protocol-Version", "2025-06-18")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    assert!(
        status.is_success(),
        "{path}: {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if status == StatusCode::NO_CONTENT || bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}
struct Harness {
    _dir: tempfile::TempDir,
    g: Gateway,
    router: Router,
    token: String,
    ws: String,
    stop: Arc<AtomicBool>,
    worker: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}
impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("source");
        std::fs::create_dir(&root).unwrap();
        let device = uuid::Uuid::new_v4().to_string();
        let device_token = random();
        let session = random();
        let a = Arc::new(
            Agent::new(AgentConfig {
                gateway_url: "https://ab.example".into(),
                device_id: device.clone(),
                device_token: device_token.clone(),
                state_dir: dir.path().join("agent"),
                roots: vec![root.clone()],
                allow_write: true,
                allow_exec: true,
                shell: "/bin/sh".into(),
            })
            .await
            .unwrap(),
        );
        let open = a
            .execute(&Job {
                id: uuid::Uuid::new_v4().to_string(),
                device_id: device.clone(),
                owner: "owner".into(),
                tool: "workspace_open".into(),
                arguments: json!({"device_id":device,"root":root,"idempotency_key":"open"}),
            })
            .await
            .unwrap();
        let ws = open["workspace"]["id"].as_str().unwrap().to_owned();
        let p = Principal {
            owner: "owner".into(),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        };
        let g = Gateway::new(GatewayConfig {
            public_url: "https://ab.example".into(),
            bind: "127.0.0.1:0".into(),
            state_dir: dir.path().join("gateway"),
            owner: "owner".into(),
            password_hash: "unused".into(),
            redirect_uris: vec![],
            allowed_origins: remote_hosts_code::default_mcp_client_origins(),
            devices: vec![DeviceRegistration {
                id: device.clone(),
                name: "local-process-fixture".into(),
                token_hash: hash(&device_token),
                scopes: p.scopes.clone(),
            }],
        })
        .await
        .unwrap();
        let hello = json!({"version":env!("CARGO_PKG_VERSION"),"wire_protocol":2,"session":session,"roots":[root],"allow_write":true,"allow_exec":true});
        g.store
            .put(
                "online",
                &device,
                &json!({"hello":hello,"last_seen":now()}),
                i64::MAX,
            )
            .await
            .unwrap();
        let token = random();
        g.store.put(&oauth_state_kind("access",&token),&hash(&token),&json!({"principal":p,"client_id":"ab-fixture","resource":"https://ab.example/mcp","family":random()}),now()+300).await.unwrap();
        let router = g.router().unwrap();
        let worker_router = router.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let worker = tokio::spawn(async move {
            while !stopping.load(Ordering::Relaxed) {
                let rows: Vec<String> = sqlx::query_scalar(
                    "SELECT value FROM kv WHERE kind='terminal' ORDER BY key LIMIT 24",
                )
                .fetch_all(&a.store.pool)
                .await
                .unwrap();
                let statuses: Vec<Value> = rows
                    .iter()
                    .map(|s| serde_json::from_str(s).unwrap())
                    .collect();
                let previews:Vec<Value>=statuses.iter().filter_map(|s|{
                    let id=s["id"].as_str()?;let path=a.config.state_dir.join("terminals").join(format!("{id}.log"));
                    let text=std::fs::read_to_string(path).ok()?;
                    // Fixed fixtures emit less than 2 KiB. No synthetic output is substituted.
                    assert!(text.len()<=2048);
                    Some(json!({"operation_id":id,"cursor_start":0,"cursor_end":text.len(),"output":text,"truncated_before":false}))
                }).collect();
                let mut poll = hello.clone();
                poll["poll_wait_ms"] = json!(100);
                poll["terminal_updates"] = json!(statuses);
                poll["terminal_previews"] = json!(previews);
                let value = http(&worker_router, "/device/poll", &device_token, poll).await;
                if let Some(job) = value.get("job").filter(|j| !j.is_null()) {
                    let job: Job = serde_json::from_value(job.clone()).unwrap();
                    let result = a.execute(&job).await.unwrap();
                    http(
                        &worker_router,
                        "/device/result",
                        &device_token,
                        json!({"operation_id":job.id,"result":result}),
                    )
                    .await;
                } else {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
            }
        });
        Self {
            _dir: dir,
            g,
            router,
            token,
            ws,
            stop,
            worker: Some(worker),
        }
    }
}
#[derive(Default)]
struct Meter {
    calls: u64,
    model_bytes: u64,
    observations: u64,
    read_jobs: u64,
    empty_output_calls: u64,
}
impl Meter {
    async fn call(&mut self, h: &Harness, tool: &str, args: Value) -> Value {
        self.calls += 1;
        self.observations += u64::from(tool == "operation_get");
        self.read_jobs += u64::from(tool == "terminal_read");
        let response=http(&h.router,"/mcp",&h.token,json!({"jsonrpc":"2.0","id":self.calls,"method":"tools/call","params":{"name":tool,"arguments":args}})).await;
        assert!(response.get("error").is_none(), "{response}");
        let result = &response["result"];
        self.model_bytes += serde_json::to_vec(result).unwrap().len() as u64;
        let value = result["structuredContent"].clone();
        assert!(value.get("error").is_none(), "{value}");
        if value["output"].as_str().unwrap_or_default().is_empty()
            && value["terminal_observation"]["output"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        {
            self.empty_output_calls += 1;
        }
        value
    }
}
fn done(value: &Value) -> bool {
    let t = if value["terminal_observation"]["terminal"].is_object() {
        &value["terminal_observation"]["terminal"]
    } else {
        &value["terminal"]
    };
    t["exit_code"].is_i64() && t["output_complete"] == true
}
async fn case(command: &str, expected: &str, exit: i64, preferred: bool) -> Value {
    let mut h = Harness::new().await;
    let mut meter = Meter::default();
    let started = Instant::now();
    let mut args = json!({"workspace_id":h.ws,"command":command,"timeout_seconds":10,"idempotency_key":"once","task_id":"ab.fixture"});
    if !preferred {
        args["wait_ms"] = json!(0);
        args["response_mode"] = json!("full");
    }
    let first = meter.call(&h, "terminal_exec", args).await;
    let id = first["operation_id"].as_str().unwrap().to_owned();
    let mut output = first["output"].as_str().unwrap_or_default().to_owned();
    let mut cursor = first["cursor"].as_u64().unwrap_or(output.len() as u64);
    let mut last = first;
    if !preferred {
        // Exercise the compatibility workflow that first resolves the submitted operation.
        meter
            .call(
                &h,
                "operation_get",
                json!({"operation_id":id,"response_mode":"full"}),
            )
            .await;
    }
    for _ in 0..100 {
        if done(&last) {
            break;
        }
        last = if preferred {
            meter
                .call(
                    &h,
                    "operation_get",
                    json!({"operation_id":id,"terminal_cursor":cursor,"wait_ms":5000}),
                )
                .await
        } else {
            meter.call(&h,"terminal_read",json!({"workspace_id":h.ws,"terminal_id":id,"cursor":cursor,"response_mode":"full","output_mode":"full"})).await
        };
        if let Some(text) = last["terminal_observation"]["output"].as_str() {
            assert_ne!(last["terminal_observation"]["output_gap"], true);
            output.push_str(text);
            cursor = last["terminal_observation"]["output_cursor_end"]
                .as_u64()
                .unwrap();
        } else if let Some(text) = last["output"].as_str() {
            output.push_str(text);
            cursor = last["cursor"].as_u64().unwrap();
        }
        if !done(&last) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    assert!(done(&last), "{last}");
    assert_eq!(output, expected);
    let terminal = if last["terminal_observation"]["terminal"].is_object() {
        &last["terminal_observation"]["terminal"]
    } else {
        &last["terminal"]
    };
    assert_eq!(terminal["exit_code"], exit);
    assert_ne!(terminal["output_truncated"], true);
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&h.g.store.pool)
        .await
        .unwrap();
    h.stop.store(true, Ordering::Relaxed);
    h.worker.take().unwrap().await.unwrap();
    json!({"mode":if preferred{"preferred_bounded_observe"}else{"legacy_explicit_read_full"},
        "tool_calls":meter.calls,"operation_observations":meter.observations,"terminal_read_jobs":meter.read_jobs,
        "empty_output_calls":meter.empty_output_calls,"model_visible_utf8_bytes":meter.model_bytes,"gateway_jobs":jobs,
        "elapsed_ms":started.elapsed().as_millis(),"output_sha256":hash(output),"exit_code":exit,"output_complete":true})
}
#[tokio::test]
async fn measured_legacy_and_preferred_workflows_preserve_real_process_results() {
    let mut cases = Vec::new();
    for (name, command, expected, exit) in [
        (
            "short_success",
            "printf 'short-result\n'",
            "short-result\n",
            0,
        ),
        (
            "incremental_output",
            "printf 'begin\n'; sleep 1.2; printf 'end\n'",
            "begin\nend\n",
            0,
        ),
        (
            "command_failure",
            "printf 'failure-evidence\n'; exit 7",
            "failure-evidence\n",
            7,
        ),
    ] {
        let legacy = tokio::time::timeout(
            Duration::from_secs(30),
            case(command, expected, exit, false),
        )
        .await
        .unwrap();
        let preferred =
            tokio::time::timeout(Duration::from_secs(30), case(command, expected, exit, true))
                .await
                .unwrap();
        assert_eq!(legacy["output_sha256"], preferred["output_sha256"]);
        assert_eq!(preferred["terminal_read_jobs"], 0);
        cases.push(json!({"case":name,"command":command,"legacy":legacy,"preferred":preferred}));
    }
    let report = json!({"protocol":1,"observed_at":now(),"version":env!("CARGO_PKG_VERSION"),
        "method":"same candidate, real /bin/sh processes and authenticated in-process HTTP; compatibility workflow versus preferred workflow",
        "scope":"not a measurement of an external host, live fleet, previous binary or model tokenizer",
        "token_count":null,"byte_metric":"serialized model-visible content plus structuredContent, UTF-8 bytes",
        "contract":remote_hosts_code::release_manifest(),"cases":cases,"deployment_performed":false});
    if let Some(path) = std::env::var_os("RH_EXPERIENCE_AB_REPORT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        file.write_all(&serde_json::to_vec_pretty(&report).unwrap())
            .unwrap();
        file.sync_all().unwrap();
    }
    println!("EXPERIENCE_AB={report}");
}

//! Native child plus loopback HTTP tests. No production credentials or state.
use super::*;
use axum::{Json, Router, http::StatusCode, routing::post};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

struct Fixture {
    agent: Agent,
    ws: Workspace,
    seen: mpsc::UnboundedReceiver<Value>,
    release: Arc<Notify>,
    heartbeat: tokio::task::JoinHandle<Result<()>>,
    server: tokio::task::JoinHandle<()>,
    _root: tempfile::TempDir,
    _state: tempfile::TempDir,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.release.notify_one();
        self.heartbeat.abort();
        self.server.abort();
    }
}
async fn fixture(hold_first: bool, fail_first_exit: bool, period: Duration) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let (send, seen) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let requests = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    let router = Router::new().route(
        "/device/heartbeat",
        post(move |Json(body): Json<Value>| {
            let (send, gate, requests, exits) =
                (send.clone(), gate.clone(), requests.clone(), exits.clone());
            async move {
                let first = requests.fetch_add(1, Ordering::SeqCst) == 0;
                let completed = body["terminal_updates"]
                    .as_array()
                    .is_some_and(|rows| rows.iter().any(|row| row["output_complete"] == true));
                let fail =
                    completed && exits.fetch_add(1, Ordering::SeqCst) == 0 && fail_first_exit;
                send.send(body).unwrap();
                if first && hold_first {
                    gate.notified().await;
                }
                if fail {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::NO_CONTENT
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut agent = Agent::new(AgentConfig {
        gateway_url: "https://fixture.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: crate::random(),
        state_dir: state.path().into(),
        roots: vec![root.path().into()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    // Test-only override after the normal HTTPS-validating constructor.
    Arc::make_mut(&mut agent.config).gateway_url = format!("http://{address}");
    let ws = Workspace {
        id: format!("{}:{}", agent.config.device_id, uuid::Uuid::new_v4()),
        device_id: agent.config.device_id.clone(),
        root: root.path().canonicalize().unwrap(),
    };
    agent
        .store
        .put("workspace", &ws.id, &ws, i64::MAX)
        .await
        .unwrap();
    let hello: DeviceHello = serde_json::from_value(json!({
        "version":env!("CARGO_PKG_VERSION"), "session":crate::random(),
        "roots":[ws.root], "allow_exec":true, "allow_write":true,
    }))
    .unwrap();
    let running = agent.clone();
    let heartbeat = tokio::spawn(async move {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        running.heartbeat_with_period(&client, &hello, period).await
    });
    Fixture {
        agent,
        ws,
        seen,
        release,
        heartbeat,
        server,
        _root: root,
        _state: state,
    }
}
async fn next(f: &mut Fixture) -> Value {
    tokio::time::timeout(Duration::from_secs(5), f.seen.recv())
        .await
        .expect("heartbeat failed to react within bound")
        .expect("heartbeat stream closed")
}
async fn command(f: &Fixture) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let result = f
        .agent
        .execute(&Job {
            id: id.clone(),
            device_id: f.agent.config.device_id.clone(),
            owner: "fixture".into(),
            tool: "terminal_exec".into(),
            arguments: json!({
                "workspace_id":f.ws.id, "command":"printf 'completion-event\\n'", "wait_ms":1500,
                "timeout_seconds":5, "idempotency_key":id,
            }),
        })
        .await
        .unwrap();
    assert_eq!(result["terminal"]["exit_code"], 0);
    assert_eq!(result["terminal"]["output_complete"], true);
    id
}
fn assert_exit(body: &Value, id: &str) {
    let row = body["terminal_updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == id)
        .unwrap();
    assert_eq!(row["state"], "exited");
    assert_eq!(row["exit_code"], 0);
    assert_eq!(row["output_complete"], true);
    let preview = body["terminal_previews"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["operation_id"] == id)
        .unwrap();
    assert_eq!(preview["output"], "completion-event\n");
}

#[tokio::test]
async fn committed_exit_wakes_heartbeat_before_sixty_second_periodic_tick() {
    let mut f = fixture(false, false, Duration::from_secs(60)).await;
    let initial = next(&mut f).await;
    assert!(initial["terminal_updates"].as_array().unwrap().is_empty());
    let id = command(&f).await;
    assert_exit(&next(&mut f).await, &id);
}

#[tokio::test]
async fn exit_during_inflight_heartbeat_is_not_lost_or_sent_concurrently() {
    let mut f = fixture(true, false, Duration::from_secs(60)).await;
    next(&mut f).await;
    let id = command(&f).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        f.seen.try_recv().is_err(),
        "do not open a concurrent heartbeat"
    );
    f.release.notify_one();
    assert_exit(&next(&mut f).await, &id);
}

#[tokio::test]
async fn rejected_heartbeat_retries_same_durable_exit_without_execution_replay() {
    let mut f = fixture(false, true, Duration::from_millis(250)).await;
    next(&mut f).await;
    let id = command(&f).await;
    for _ in 0..2 {
        let mut delivered = false;
        for _ in 0..16 {
            let body = next(&mut f).await;
            if body["terminal_updates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["id"] == id && v["output_complete"] == true)
            {
                assert_exit(&body, &id);
                delivered = true;
                break;
            }
        }
        assert!(
            delivered,
            "terminal exit did not arrive in the bounded observation budget"
        );
    }
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='terminal'")
        .fetch_one(&f.agent.store.pool)
        .await
        .unwrap();
    assert_eq!(
        count.0, 1,
        "observation retry must not create another terminal"
    );
}

#[tokio::test]
async fn failed_final_state_write_does_not_emit_a_completion_hint() {
    let mut f = fixture(false, false, Duration::from_secs(60)).await;
    next(&mut f).await;
    let changed = f.agent.terminals.subscribe_changes();
    sqlx::query("CREATE TRIGGER reject_terminal_end BEFORE UPDATE ON kv WHEN NEW.kind='terminal' AND json_extract(NEW.value,'$.output_complete')=1 BEGIN SELECT RAISE(ABORT,'fixture final write failed'); END")
        .execute(&f.agent.store.pool).await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let result = f
        .agent
        .execute(&Job {
            id: id.clone(),
            device_id: f.agent.config.device_id.clone(),
            owner: "fixture".into(),
            tool: "terminal_exec".into(),
            arguments: json!({"workspace_id":f.ws.id,
            "command":"printf 'not-a-durable-completion\\n'", "wait_ms":1500,
            "timeout_seconds":5, "idempotency_key":id}),
        })
        .await
        .unwrap();
    assert_ne!(result["terminal"]["output_complete"], true);
    assert!(
        !changed.has_changed().unwrap(),
        "failed write cannot publish a completion hint"
    );
    let durable: Value = f.agent.store.get("terminal", &id).await.unwrap().unwrap();
    assert_eq!(durable["state"], "running");
    assert_eq!(durable["output_complete"], false);
}

#[tokio::test]
async fn accepted_cancellation_wakes_heartbeat_and_is_not_projected_as_success() {
    let mut f = fixture(false, false, Duration::from_secs(60)).await;
    next(&mut f).await;
    let id = uuid::Uuid::new_v4().to_string();
    f.agent
        .execute(&Job {
            id: id.clone(),
            device_id: f.agent.config.device_id.clone(),
            owner: "fixture".into(),
            tool: "terminal_exec".into(),
            arguments: json!({
                "workspace_id":f.ws.id, "command":"sleep 30", "wait_ms":0,
                "timeout_seconds":5, "idempotency_key":id,
            }),
        })
        .await
        .unwrap();
    f.agent
        .terminals
        .cancel(&f.ws, &json!({"terminal_id":id}))
        .await
        .unwrap();
    let body = next(&mut f).await;
    let terminal = body["terminal_updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == id)
        .unwrap();
    assert_eq!(terminal["state"], "cancelled");
}

#[tokio::test]
async fn final_notification_is_retained_for_a_busy_subscriber() {
    let mut f = fixture(false, false, Duration::from_secs(60)).await;
    next(&mut f).await;
    let mut changed = f.agent.terminals.subscribe_changes();
    let id = command(&f).await;
    assert!(changed.has_changed().unwrap());
    let _revision = *changed.borrow_and_update();
    assert!(!changed.has_changed().unwrap());
    let row: Value = f.agent.store.get("terminal", &id).await.unwrap().unwrap();
    assert_eq!(row["state"], "exited");
    assert_eq!(row["output_complete"], true);
}

//! Real durable Gateway observations; synthetic identities only.
use super::*;
use crate::{DeviceRegistration, GatewayConfig, gateway::Job};

async fn fixture(state: &str) -> (tempfile::TempDir, Gateway, Principal, String) {
    let dir = tempfile::tempdir().unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: crate::default_mcp_client_origins(),
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().join("gateway"),
        owner: "fixture".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: device.clone(),
            name: "fixture".into(),
            token_hash: hash("synthetic"),
            scopes: vec!["terminal:exec".into()],
        }],
    })
    .await
    .unwrap();
    let p = Principal {
        owner: "fixture".into(),
        scopes: vec!["terminal:exec".into()],
    };
    let id = uuid::Uuid::new_v4().to_string();
    let job = Job {
        id: id.clone(),
        device_id: device.clone(),
        owner: p.owner.clone(),
        tool: "terminal_exec".into(),
        arguments: json!({"workspace_id":format!("{device}:{}",uuid::Uuid::new_v4())}),
    };
    let result = json!({"state":state,"terminal":terminal(&id,state),"terminal_id":id,"output":"","cursor":0,"has_more":false});
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,?,'done',?)")
        .bind(&id)
        .bind(&device)
        .bind(&id)
        .bind(&id)
        .bind(serde_json::to_string(&job).unwrap())
        .bind(result.to_string())
        .bind(now())
        .execute(&g.store.pool)
        .await
        .unwrap();
    (dir, g, p, id)
}
fn terminal(id: &str, state: &str) -> Value {
    json!({"id":id,"state":state,"exit_code":if state=="exited" {json!(0)} else {Value::Null},
        "output_complete":state=="exited","output_truncated":false,"created_at":now(),"updated_at":now(),"pty":false})
}
async fn snapshot(g: &Gateway, id: &str, state: &str, reported_at: i64) {
    let output = if state == "exited" { "done" } else { "" };
    g.store
        .put(
            "terminal_observation",
            id,
            &json!({"terminal":terminal(id,state),"reported_at":reported_at,
        "last_progress_at":reported_at,"output_preview":output,"output_cursor_start":0,
        "output_cursor_end":output.len(),"output_truncated_before":false}),
            i64::MAX,
        )
        .await
        .unwrap();
    g.observation_changed.notify_waiters();
}
#[tokio::test]
async fn no_cursor_waits_past_transport_completion_and_intermediate_snapshot() {
    let (_dir, g, p, id) = fixture("starting").await;
    let gg = g.clone();
    let pp = p.clone();
    let ii = id.clone();
    let waiter = tokio::spawn(async move {
        observe(&gg, &pp, &json!({"operation_id":ii,"wait_ms":1000}))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    snapshot(&g, &id, "running", now()).await;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert!(
        !waiter.is_finished(),
        "a submission or running transition is not the process result"
    );
    snapshot(&g, &id, "exited", now()).await;
    let out = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out["state"], "exited");
    assert_eq!(out["output"], "done");
    let (jobs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(jobs, 1, "observation must not enqueue another query");
}
#[tokio::test]
async fn legacy_single_call_waits_for_process_instead_of_completed_submission() {
    let (_dir, g, p, id) = fixture("running").await;
    let gg = g.clone();
    let pp = p.clone();
    let ii = id.clone();
    let waiter = tokio::spawn(async move {
        observe(&gg, &pp, &json!({"operation_id":ii}))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    snapshot(&g, &id, "exited", now()).await;
    let out = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out["state"], "exited");
    assert!(out.get("observation").is_none());
}
#[tokio::test]
async fn cursor_observation_still_returns_first_intermediate_change() {
    let (_dir, g, p, id) = fixture("starting").await;
    let initial = observe(&g, &p, &json!({"operation_id":id,"wait_ms":0}))
        .await
        .unwrap();
    let gg = g.clone();
    let pp = p.clone();
    let ii = id.clone();
    let waiter = tokio::spawn(async move {
        observe(
            &gg,
            &pp,
            &json!({"operation_id":ii,"wait_ms":1000,"cursor":initial["observation"]["cursor"]}),
        )
        .await
        .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    snapshot(&g, &id, "running", now()).await;
    let out = tokio::time::timeout(Duration::from_millis(700), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out["state"], "running");
    assert_eq!(out["observation"]["changed"], true);
}
#[tokio::test]
async fn running_submission_without_replication_obeys_wait_budget() {
    let (_dir, g, p, id) = fixture("running").await;
    let start = Instant::now();
    let out = observe(&g, &p, &json!({"operation_id":id,"wait_ms":80}))
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(60));
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(out["terminal"]["state"], "running");
}
#[tokio::test]
async fn stale_process_is_returned_immediately_as_unknown_not_waited_out() {
    let (_dir, g, p, id) = fixture("running").await;
    snapshot(&g, &id, "running", now() - 100).await;
    let start = Instant::now();
    let out = observe(&g, &p, &json!({"operation_id":id,"wait_ms":4000}))
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(out["state"], "outcome_unknown");
    assert_eq!(out["terminal_observation"]["stale"], true);
}
#[test]
fn output_failure_and_staleness_are_not_hidden_by_completion_wait() {
    assert!(!terminal_pending(
        &json!({"state":"exited","output_complete":false,"output_error":"failed"}),
        false
    ));
    assert!(!terminal_pending(
        &json!({"state":"exited","output_complete":false,"output_truncated":true}),
        false
    ));
    assert!(!awaiting_result(
        &json!({"pending":true,"receipt":{"stale":true}})
    ));
    assert!(!awaiting_result(
        &json!({"pending":true,"error":"unavailable"})
    ));
}

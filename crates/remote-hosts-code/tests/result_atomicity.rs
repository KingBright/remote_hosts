//! Isolated Gateway failure boundaries. No production database or real commands.
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig,
    auth::Principal,
    gateway::{Gateway, Job},
    hash, now, random, receipts,
};
use serde_json::{Value, json};

async fn fixture() -> (tempfile::TempDir, Gateway, Principal, String) {
    let dir = tempfile::tempdir().unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    let scopes = vec![
        "code:read".into(),
        "code:write".into(),
        "terminal:exec".into(),
    ];
    let g = Gateway::new(GatewayConfig {
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().into(),
        owner: "owner".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        allowed_origins: vec![],
        devices: vec![DeviceRegistration {
            id: device.clone(),
            name: "synthetic".into(),
            token_hash: hash(random()),
            scopes: scopes.clone(),
        }],
    })
    .await
    .unwrap();
    g.store.put("online",&device,&json!({"hello":{"version":"0.10.14","session":"s".repeat(64),"roots":[],"allow_write":true,"allow_exec":true},"last_seen":now()}),i64::MAX).await.unwrap();
    (
        dir,
        g,
        Principal {
            owner: "owner".into(),
            scopes,
        },
        device,
    )
}
async fn completed(g: &Gateway, p: &Principal, device: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let job = Job {
        id: id.clone(),
        device_id: device.into(),
        owner: p.owner.clone(),
        tool: "terminal_exec".into(),
        arguments: json!({"workspace_id":format!("{device}:workspace")}),
    };
    let result = json!({"state":"exited","terminal":{"id":id,"state":"exited","exit_code":7,"output_complete":true,"output_truncated":false},"output":"failed\n","cursor":7,"has_more":false,"output_view":"full"});
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,?,'done',?)")
        .bind(&id)
        .bind(device)
        .bind(random())
        .bind(random())
        .bind(serde_json::to_string(&job).unwrap())
        .bind(result.to_string())
        .bind(now())
        .execute(&g.store.pool)
        .await
        .unwrap();
    id
}
#[tokio::test]
async fn optional_timing_failure_does_not_erase_authorized_durable_failed_exit() {
    let (_dir, g, p, d) = fixture().await;
    let id = completed(&g, &p, &d).await;
    sqlx::query("DROP TABLE operation_timing")
        .execute(&g.store.pool)
        .await
        .unwrap();
    let request = format!("req_{}", uuid::Uuid::new_v4().simple());
    let v = receipts::invoke(
        &g,
        &p,
        "operation_get",
        json!({"operation_id":id,"response_mode":"full"}),
        &request,
    )
    .await
    .unwrap();
    assert_eq!(v["state"], "exited");
    assert_eq!(v["output"], "failed\n");
    assert_eq!(v["receipt"]["process_exit_code"], 7);
    assert_eq!(v["receipt"]["process_outcome"], "failed");
    assert_eq!(v["receipt"]["evidence_complete"], true);
    assert_eq!(v["receipt"]["durable"], true);
    assert_eq!(v["operation_lifecycle"]["available"], false);
    assert_eq!(
        v["operation_lifecycle"]["error_code"],
        "operation_lifecycle_unavailable"
    );
    assert!(v.get("error").is_none());
}
#[tokio::test]
async fn optional_metadata_failure_cannot_bypass_original_scope() {
    let (_dir, g, mut p, d) = fixture().await;
    let id = completed(&g, &p, &d).await;
    sqlx::query("DROP TABLE operation_timing")
        .execute(&g.store.pool)
        .await
        .unwrap();
    p.scopes = vec!["code:read".into()];
    let request = format!("req_{}", uuid::Uuid::new_v4().simple());
    let v = receipts::invoke(
        &g,
        &p,
        "operation_get",
        json!({"operation_id":id}),
        &request,
    )
    .await
    .unwrap();
    assert!(v.get("error").is_some());
    assert!(v.get("output").is_none());
}
#[tokio::test]
async fn enqueue_timing_failure_leaves_no_job_guard_or_task_link() {
    let (_dir, g, p, d) = fixture().await;
    sqlx::query("CREATE TRIGGER fail_timing BEFORE INSERT ON operation_timing BEGIN SELECT RAISE(ABORT,'synthetic metadata fault'); END").execute(&g.store.pool).await.unwrap();
    let request = format!("req_{}", uuid::Uuid::new_v4().simple());
    let v=receipts::invoke(&g,&p,"terminal_exec",json!({"workspace_id":format!("{d}:workspace"),"command":"not executed","idempotency_key":"original","task_id":"atomicity-fixture"}),&request).await.unwrap();
    assert!(v.get("error").is_some());
    assert!(v["operation_id"].is_null());
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    let guards: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM semantic_guards")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    let links: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM kv WHERE kind IN ('task_operation','operation_semantic')",
    )
    .fetch_one(&g.store.pool)
    .await
    .unwrap();
    assert_eq!((jobs, guards, links), (0, 0, 0));
    let retained: Value = g
        .store
        .get("request_receipt", &request)
        .await
        .unwrap()
        .unwrap();
    assert!(retained["operation_id"].is_null());
    assert_eq!(retained["state"], "gateway_rejected");
}

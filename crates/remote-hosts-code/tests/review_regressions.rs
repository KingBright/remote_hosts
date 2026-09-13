//! Regressions found during the September 2026 live code-gateway review.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig,
    auth::Principal,
    files::{self, Workspace},
    gateway::{Gateway, Job},
    hash, now, random,
};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn gateway() -> (tempfile::TempDir, Gateway, String) {
    let dir = tempfile::tempdir().unwrap();
    let credential = random();
    let gateway = Gateway::new(GatewayConfig {
        public_url: "https://review.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().into(),
        owner: "review".into(),
        // This fixture exercises device routing, never the OAuth login endpoint.
        password_hash: "unused-in-device-tests".into(),
        devices: vec![DeviceRegistration {
            id: uuid::Uuid::new_v4().to_string(),
            name: "review-device".into(),
            token_hash: hash(&credential),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        }],
        redirect_uris: vec!["https://chatgpt.com/connector_platform_oauth_redirect".into()],
    })
    .await
    .unwrap();
    (dir, gateway, credential)
}

async fn post(
    router: &Router,
    path: &str,
    credential: &str,
    body: Value,
) -> axum::response::Response {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("host", "review.example")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn queued(gateway: &Gateway, state: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let job = Job {
        id: id.clone(),
        device_id: gateway.config.devices[0].id.clone(),
        owner: "review".into(),
        tool: "code_list".into(),
        arguments: json!({"workspace_id":"fixture"}),
    };
    sqlx::query("INSERT INTO jobs (id,device,idem,fingerprint,request,result,state,updated) VALUES (?,?,?,?,?,NULL,?,?)")
        .bind(&id).bind(&job.device_id).bind(random()).bind(hash(b"fixture"))
        .bind(serde_json::to_string(&job).unwrap()).bind(state).bind(now())
        .execute(&gateway.store.pool).await.unwrap();
    id
}

#[tokio::test]
async fn receipt_rejects_unknown_operations() {
    let (_dir, g, credential) = gateway().await;
    let response = post(
        &g.router().unwrap(),
        "/device/result",
        &credential,
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"result":{"ok":true}}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn receipt_rejects_undispatched_operations() {
    let (_dir, g, credential) = gateway().await;
    let id = queued(&g, "queued").await;
    let response = post(
        &g.router().unwrap(),
        "/device/result",
        &credential,
        json!({"operation_id":id,"result":{"ok":true}}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn receipt_accepts_exact_replay_but_rejects_conflicting_result() {
    let (_dir, g, credential) = gateway().await;
    let id = queued(&g, "dispatched").await;
    let router = g.router().unwrap();
    let receipt = json!({"operation_id":id,"result":{"ok":true}});
    assert_eq!(
        post(&router, "/device/result", &credential, receipt.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        post(&router, "/device/result", &credential, receipt)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        post(
            &router,
            "/device/result",
            &credential,
            json!({"operation_id":id,"result":{"ok":false}})
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let saved: (String,) = sqlx::query_as("SELECT result FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&saved.0).unwrap(),
        json!({"ok":true})
    );
}

#[tokio::test]
async fn invalid_arguments_are_rejected_before_device_lookup_or_queueing() {
    let (_dir, g, _credential) = gateway().await;
    let principal = Principal {
        owner: "review".into(),
        scopes: vec!["code:read".into()],
    };
    let error = g
        .dispatch(
            &principal,
            "code_list",
            json!({
                "workspace_id": format!("{}:fixture", g.config.devices[0].id),
                "include_ignored": "false"
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("invalid_arguments"), "{error}");
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

fn workspace() -> (tempfile::TempDir, Workspace) {
    let dir = tempfile::tempdir().unwrap();
    let ws = Workspace {
        id: "review".into(),
        device_id: "fixture".into(),
        root: dir.path().canonicalize().unwrap(),
    };
    (dir, ws)
}

#[test]
fn search_keeps_the_matching_line_when_context_exceeds_budget() {
    let (dir, ws) = workspace();
    std::fs::write(
        dir.path().join("sample.txt"),
        format!("{}\nNEEDLE\nafter\n", "x".repeat(12000)),
    )
    .unwrap();
    let result = files::search(
        &ws,
        &json!({"query":"NEEDLE","glob":"*.txt","context_lines":1,"max_bytes":1024}),
    )
    .unwrap();
    let hit = &result["matches"][0];
    assert!(hit["text"].as_str().unwrap().contains("NEEDLE"), "{hit}");
    assert_eq!(hit["line"], 2);
    assert_eq!(hit["start_line"], 2);
}

#[test]
fn search_keeps_a_late_match_inside_an_oversized_line() {
    let (dir, ws) = workspace();
    std::fs::write(
        dir.path().join("sample.txt"),
        format!("{}NEEDLE{}\n", "x".repeat(12000), "y".repeat(12000)),
    )
    .unwrap();
    let result = files::search(
        &ws,
        &json!({"query":"NEEDLE","glob":"*.txt","max_bytes":1024}),
    )
    .unwrap();
    assert!(
        result["matches"][0]["text"]
            .as_str()
            .unwrap()
            .contains("NEEDLE")
    );
    assert_eq!(result["matches"][0]["truncated"], true);
}

#[test]
fn edits_reject_nul_bytes_before_writing_any_file() {
    let (dir, ws) = workspace();
    let journal = tempfile::tempdir().unwrap();
    let result = files::apply(
        &ws,
        &json!({"files":[
            {"path":"first.txt","action":"create","expected_version":"absent","content":"valid text"},
            {"path":"second.txt","action":"create","expected_version":"absent","content":"bad\u{0000}text"}
        ]}),
        &journal.path().join("operation.json"),
    );
    assert!(
        result.is_err(),
        "text-edit tools must not create files they cannot read"
    );
    assert!(!dir.path().join("first.txt").exists());
    assert!(!dir.path().join("second.txt").exists());
}

#[tokio::test]
async fn simultaneous_device_sessions_have_only_one_winner() {
    let (_dir, g, credential) = gateway().await;
    queued(&g, "queued").await;
    queued(&g, "queued").await;
    let router = g.router().unwrap();
    let hello =
        || json!({"session":random(),"roots":["/tmp"],"allow_write":true,"allow_exec":true});
    let (a, b) = tokio::join!(
        post(&router, "/device/poll", &credential, hello()),
        post(&router, "/device/poll", &credential, hello())
    );
    let statuses = [a.status(), b.status()];
    assert_eq!(
        statuses.iter().filter(|s| **s == StatusCode::OK).count(),
        1,
        "{statuses:?}"
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1,
        "{statuses:?}"
    );
}

#[test]
fn catalog_schema_rejects_nested_unknown_fields_types_and_bounds() {
    use remote_hosts_code::tools::{catalog, validate};
    assert_eq!(catalog().len(), 23); // Includes durable change-set recovery and explicit workspace GC.
    let valid = json!({"workspace_id":"w","idempotency_key":"edit","files":[{
        "path":"a.txt","expected_version":"absent","action":"create","content":"hello"
    }]});
    validate("code_apply_edits", &valid).unwrap();
    let mut bad = valid.clone();
    bad["files"][0]["action"] = json!("overwrite_everything");
    assert!(validate("code_apply_edits", &bad).is_err());
    let mut bad = valid.clone();
    bad["files"][0]["unexpected"] = json!(true);
    assert!(validate("code_apply_edits", &bad).is_err());
    let mut bad = valid;
    bad["files"][0]
        .as_object_mut()
        .unwrap()
        .remove("expected_version");
    assert!(validate("code_apply_edits", &bad).is_err());
    for limit in [json!(0), json!(501), json!("10"), json!(1.5), Value::Null] {
        assert!(validate("code_list", &json!({"workspace_id":"w","limit":limit})).is_err());
    }
    assert!(validate("code_read", &json!({"workspace_id":"w","requests":[]})).is_err());
    assert!(validate("devices_list", &json!({"unexpected":true})).is_err());
    validate("change_resume", &json!({"workspace_id":"w","idempotency_key":"r","change_set_id":"00000000-0000-0000-0000-000000000001"})).unwrap();
    validate("workspace_gc", &json!({"workspace_id":"w","idempotency_key":"g","action":"preview","older_than_seconds":3600,"max_items":100})).unwrap();
    assert!(validate("workspace_gc", &json!({"workspace_id":"w","idempotency_key":"g","action":"apply","older_than_seconds":60,"max_items":100,"preview_id":"x"})).is_err());
}

#[test]
fn search_window_preserves_utf8_and_reports_byte_offset() {
    let (dir, ws) = workspace();
    std::fs::write(
        dir.path().join("sample.txt"),
        format!("{}目标42{}\n", "前".repeat(5000), "后".repeat(5000)),
    )
    .unwrap();
    let result = files::search(
        &ws,
        &json!({"query":"目标[0-9]+","regex":true,"glob":"*.txt","max_bytes":1024}),
    )
    .unwrap();
    let hit = &result["matches"][0];
    assert!(hit["text"].as_str().unwrap().contains("目标42"));
    assert!(hit["text"].as_str().unwrap().len() <= 1024);
    assert!(hit["line_byte_offset"].as_u64().unwrap() > 0);
    assert_eq!(hit["match_truncated"], false);
}

#[test]
fn listing_rejects_overflowing_cursor_without_panicking() {
    let (dir, ws) = workspace();
    std::fs::write(dir.path().join("a.txt"), "a").unwrap();
    std::fs::write(dir.path().join("b.txt"), "b").unwrap();
    let mut query = json!({"glob":"*.txt","limit":1});
    let first = files::list(&ws, &query).unwrap();
    let fingerprint = first["next_cursor"]
        .as_str()
        .unwrap()
        .split_once(':')
        .unwrap()
        .0;
    query["cursor"] = json!(format!("{fingerprint}:{}", usize::MAX));
    assert!(files::list(&ws, &query).is_err());
}

#[tokio::test]
async fn receipt_cannot_complete_another_devices_job() {
    let (_dir, g, credential) = gateway().await;
    let id = queued(&g, "dispatched").await;
    sqlx::query("UPDATE jobs SET device=? WHERE id=?")
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&id)
        .execute(&g.store.pool)
        .await
        .unwrap();
    let response = post(
        &g.router().unwrap(),
        "/device/result",
        &credential,
        json!({"operation_id":id,"result":{"ok":true}}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let result: (Option<String>,) = sqlx::query_as("SELECT result FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert!(result.0.is_none());
}

#[tokio::test]
async fn expired_device_lease_can_be_replaced() {
    let (_dir, g, credential) = gateway().await;
    g.store
        .put(
            "online",
            &g.config.devices[0].id,
            &json!({
                "hello":{"session":random(),"roots":[],"allow_write":false,"allow_exec":false},
                "last_seen":now()-46
            }),
            i64::MAX,
        )
        .await
        .unwrap();
    queued(&g, "queued").await;
    let response = post(
        &g.router().unwrap(),
        "/device/poll",
        &credential,
        json!({"session":random(),"roots":[],"allow_write":false,"allow_exec":false}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn receipt_requires_an_object_result() {
    let (_dir, g, credential) = gateway().await;
    let id = queued(&g, "dispatched").await;
    let response = post(
        &g.router().unwrap(),
        "/device/result",
        &credential,
        json!({"operation_id":id,"result":true}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

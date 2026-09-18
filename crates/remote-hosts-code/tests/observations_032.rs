//! In-process authenticated gateway tests. No real devices, credentials or deployment.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig,
    auth::Principal,
    gateway::{Gateway, Job},
    hash, now, random, tools,
};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Gateway, Principal, String) {
    let d = tempfile::tempdir().unwrap();
    let token = random();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: remote_hosts_code::default_mcp_client_origins(),
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: d.path().join("gateway"),
        owner: "owner".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: uuid::Uuid::new_v4().to_string(),
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
    let p = Principal {
        owner: "owner".into(),
        scopes: vec![
            "code:read".into(),
            "code:write".into(),
            "terminal:exec".into(),
        ],
    };
    (d, g, p, token)
}
async fn insert(g: &Gateway, tool: &str, result: Option<Value>) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let j = Job {
        id: id.clone(),
        device_id: g.config.devices[0].id.clone(),
        owner: g.config.owner.clone(),
        tool: tool.into(),
        arguments: json!({}),
    };
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?)")
        .bind(&id)
        .bind(&j.device_id)
        .bind(random())
        .bind(random())
        .bind(serde_json::to_string(&j).unwrap())
        .bind(result.as_ref().map(Value::to_string))
        .bind(if result.is_some() {
            "done"
        } else {
            "dispatched"
        })
        .bind(now())
        .execute(&g.store.pool)
        .await
        .unwrap();
    id
}
async fn call(g: &Gateway, p: &Principal, args: Value) -> Value {
    g.dispatch(p, "operation_get", args).await.unwrap()
}
async fn post(g: &Gateway, token: &str, path: &str, args: Value) -> axum::response::Response {
    g.router()
        .unwrap()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("host", "fixture.example")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(args.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn count(g: &Gateway) -> i64 {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    n
}

#[tokio::test]
async fn legacy_single_shape_and_batch_order_are_preserved_without_dispatch() {
    let (_d, g, p, _) = fixture().await;
    let a = insert(&g, "code_read", Some(json!({"answer":1}))).await;
    let b = insert(&g, "terminal_exec", None).await;
    let old = call(&g, &p, json!({"operation_id":a})).await;
    assert_eq!(old["answer"], 1);
    assert!(old.get("observation").is_none());
    let batch = call(&g, &p, json!({"operation_ids":[b,a]})).await;
    assert_eq!(batch["operations"][0]["operation_id"], b);
    assert_eq!(
        batch["operations"][1], old,
        "small legacy batch preserves exact per-operation shape"
    );
    assert_eq!(batch["observation"]["observation_protocol"], 2);
    assert_eq!(batch["pending_count"], 1);
    assert_eq!(count(&g).await, 2);
}
#[tokio::test]
async fn legacy_single_observation_waits_for_result_without_recursive_query() {
    let (_d, g, p, token) = fixture().await;
    let id = insert(&g, "code_read", None).await;
    let gg = g.clone();
    let pp = p.clone();
    let operation = id.clone();
    let waiter =
        tokio::spawn(async move { call(&gg, &pp, json!({"operation_id":operation})).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        post(
            &g,
            &token,
            "/device/result",
            json!({"operation_id":id,"result":{"answer":"one-observation"}})
        )
        .await
        .status(),
        StatusCode::OK
    );
    let out = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out["answer"], "one-observation");
    assert_eq!(out["operation_id"], id);
    assert_ne!(out.get("pending"), Some(&json!(true)));
    assert_eq!(
        count(&g).await,
        1,
        "observation must never create a second job"
    );
}

#[tokio::test]
async fn terminal_preview_is_returned_and_continued_without_terminal_read_job() {
    let (_d, g, p, _) = fixture().await;
    let id = insert(&g, "terminal_exec", None).await;
    g.store
        .put(
            "terminal_observation",
            &id,
            &json!({
                "terminal":{"id":id,"workspace_id":"fixture-workspace","state":"running","exit_code":null,
                    "output_truncated":false,"created_at":now(),"updated_at":now(),"pty":false,
                    "log_format":1,"output_complete":false},
                "reported_at":now(),"session":"fixture-session",
                "output_preview":"abcdef","output_cursor_start":10,"output_cursor_end":16,
                "output_truncated_before":true
            }),
            now() + 100,
        )
        .await
        .unwrap();
    let full = call(&g, &p, json!({"operation_id":id,"wait_ms":0})).await;
    assert_eq!(full["terminal_observation"]["output"], "abcdef");
    assert_eq!(full["terminal_observation"]["output_cursor_start"], 10);
    assert_eq!(full["terminal_observation"]["output_cursor_end"], 16);
    assert_eq!(full["terminal_observation"]["output_gap"], true);

    let continued = call(
        &g,
        &p,
        json!({"operation_id":id,"wait_ms":0,"terminal_cursor":13}),
    )
    .await;
    assert_eq!(continued["terminal_observation"]["output"], "def");
    assert_eq!(continued["terminal_observation"]["output_cursor_start"], 13);
    assert_eq!(continued["terminal_observation"]["output_cursor_end"], 16);
    assert_eq!(continued["terminal_observation"]["output_gap"], false);
    assert_eq!(
        count(&g).await,
        1,
        "observe must not create a terminal_read job"
    );
}

#[tokio::test]
async fn request_receipt_is_observable_before_or_after_remote_operation_creation() {
    let (_d, g, p, _) = fixture().await;
    let rejected = format!("req_{}", "a".repeat(32));
    g.store
        .put(
            "request_receipt",
            &rejected,
            &json!({
                "protocol":1,"request_id":rejected,"owner":"owner","tool":"terminal_exec",
                "state":"gateway_rejected","received_at":now(),"updated_at":now(),
                "operation_id":null,"last_confirmed_stage":"gateway_received_request",
                "execution_state":"not_started","error_code":"invalid_arguments",
                "retry_policy":"retry_only_after_correcting_arguments","user_action":"correct_arguments"
            }),
            now() + 100,
        )
        .await
        .unwrap();
    let rejected_view = call(&g, &p, json!({"request_id":rejected})).await;
    assert_eq!(rejected_view["state"], "gateway_rejected");
    assert!(rejected_view["operation_id"].is_null());
    assert!(rejected_view.get("owner").is_none());
    assert!(
        rejected_view["next_action"]
            .as_str()
            .unwrap()
            .contains("no remote operation exists")
    );
    assert_eq!(count(&g).await, 0);

    let operation = insert(&g, "code_read", Some(json!({"answer":42}))).await;
    let accepted = format!("req_{}", "b".repeat(32));
    g.store
        .put(
            "request_receipt",
            &accepted,
            &json!({
                "protocol":1,"request_id":accepted,"owner":"owner","tool":"code_read",
                "state":"operation_created","received_at":now(),"updated_at":now(),
                "operation_id":operation,"last_confirmed_stage":"operation_created",
                "execution_state":"remote_operation_observable"
            }),
            now() + 100,
        )
        .await
        .unwrap();
    let accepted_view = call(&g, &p, json!({"request_id":accepted})).await;
    assert_eq!(accepted_view["answer"], 42);
    assert_eq!(accepted_view["operation_id"], operation);
    assert_eq!(accepted_view["request_receipt"]["request_id"], accepted);
    assert!(accepted_view["request_receipt"].get("owner").is_none());
    assert_eq!(
        count(&g).await,
        1,
        "request observation must not create a new job"
    );
}

#[tokio::test]
async fn receipt_wakes_existing_batch_wait_and_does_not_reexecute() {
    let (_d, g, p, token) = fixture().await;
    let id = insert(&g, "code_read", None).await;
    let first = call(&g, &p, json!({"operation_ids":[id]})).await;
    let gg = g.clone();
    let pp = p.clone();
    let request =
        json!({"operation_ids":[id],"cursor":first["observation"]["cursor"],"wait_ms":2000});
    let waiter = tokio::spawn(async move { call(&gg, &pp, request).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        post(
            &g,
            &token,
            "/device/result",
            json!({"operation_id":id,"result":{"answer":"ready"}})
        )
        .await
        .status(),
        StatusCode::OK
    );
    let out = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out["pending_count"], 0);
    assert_eq!(out["operations"][0]["answer"], "ready");
    assert_eq!(out["observation"]["changed"], true);
    assert_eq!(count(&g).await, 1);
}
#[tokio::test]
async fn unchanged_wait_is_bounded_and_does_not_renew_job() {
    let (_d, g, p, _) = fixture().await;
    let id = insert(&g, "code_read", None).await;
    sqlx::query("UPDATE jobs SET updated=123 WHERE id=?")
        .bind(&id)
        .execute(&g.store.pool)
        .await
        .unwrap();
    let first = call(&g, &p, json!({"operation_ids":[id]})).await;
    let out = call(
        &g,
        &p,
        json!({"operation_ids":[id],"cursor":first["observation"]["cursor"],"wait_ms":60}),
    )
    .await;
    assert_eq!(out["observation"]["changed"], false);
    assert!(out["observation"]["waited_ms"].as_u64().unwrap() >= 50);
    let (state, updated): (String, i64) =
        sqlx::query_as("SELECT state,updated FROM jobs WHERE id=?")
            .bind(&id)
            .fetch_one(&g.store.pool)
            .await
            .unwrap();
    assert_eq!(state, "dispatched");
    assert_eq!(updated, 123);
}
#[tokio::test]
async fn permission_checks_cover_every_id_before_batch_output() {
    let (_d, g, mut p, _) = fixture().await;
    let read = insert(&g, "code_read", Some(json!({"private":"data"}))).await;
    let write = insert(&g, "code_apply_edits", None).await;
    p.scopes = vec!["code:read".into()];
    assert!(
        g.dispatch(&p, "operation_get", json!({"operation_ids":[read,write]}))
            .await
            .is_err()
    );
    assert!(
        g.dispatch(
            &p,
            "operation_get",
            json!({"operation_ids":[read,uuid::Uuid::new_v4().to_string()]})
        )
        .await
        .is_err()
    );
    assert_eq!(count(&g).await, 2);
}
#[tokio::test]
async fn large_batch_results_are_explicitly_omitted_not_truncated() {
    let (_d, g, p, _) = fixture().await;
    let id = insert(&g, "code_read", Some(json!({"text":"x".repeat(20000)}))).await;
    let out = call(&g, &p, json!({"operation_ids":[id],"max_bytes":4096})).await;
    assert_eq!(out["operations"][0]["result_omitted"], true);
    assert!(serde_json::to_vec(&out).unwrap().len() <= 4096);
    assert_eq!(
        call(&g, &p, json!({"operation_id":id})).await["text"]
            .as_str()
            .unwrap()
            .len(),
        20000
    );
}
#[tokio::test]
async fn small_results_that_fit_the_budget_are_not_all_omitted() {
    let (_d, g, p, _) = fixture().await;
    let mut ids = vec![];
    for n in 0..20 {
        ids.push(insert(&g, "code_read", Some(json!({"n":n}))).await);
    }
    let out = call(&g, &p, json!({"operation_ids":ids,"max_bytes":4096})).await;
    assert!(serde_json::to_vec(&out).unwrap().len() <= 4096);
    assert!(
        out["operations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|x| x.get("n").is_some()),
        "20 tiny results fit but were unnecessarily omitted"
    );
    assert_eq!(out["observation"]["observation_protocol"], 2);
    assert!(out["operations"].as_array().unwrap().iter().all(|x| {
        x["operation_lifecycle"].get("available").is_some()
            || x["operation_lifecycle"].get("queue_ms").is_some()
    }));
}
#[tokio::test]
async fn expired_artifact_does_not_hide_other_authorized_results() {
    let (_d, g, p, _) = fixture().await;
    let saved =
        json!({"state":"completed","artifact_id":"expired","sha256":"a".repeat(64),"size":3});
    let gone = insert(&g, "file_download", Some(saved.clone())).await;
    let good = insert(&g, "code_read", Some(json!({"answer":42}))).await;
    let out = call(&g, &p, json!({"operation_ids":[gone,good]})).await;
    assert_eq!(out["operations"][0]["state"], "completed");
    assert_eq!(out["operations"][0]["sha256"], saved["sha256"]);
    assert_eq!(out["operations"][0]["size"], saved["size"]);
    assert_eq!(out["operations"][0]["download_available"], false);
    assert!(out["operations"][0].get("observation_error").is_none());
    assert!(out["operations"][0].get("download_url").is_none());
    assert_eq!(out["operations"][1]["answer"], 42);
    assert_eq!(count(&g).await, 2);
}
#[tokio::test]
async fn explicit_single_query_budget_is_honored_without_changing_legacy_shape() {
    let (_d, g, p, _) = fixture().await;
    let id = insert(&g, "code_read", Some(json!({"text":"x".repeat(12000)}))).await;
    let out = call(&g, &p, json!({"operation_id":id,"max_bytes":4096})).await;
    assert!(serde_json::to_vec(&out).unwrap().len() <= 4096);
    assert_eq!(out["result_omitted"], true);
    assert_eq!(
        call(&g, &p, json!({"operation_id":id})).await["text"]
            .as_str()
            .unwrap()
            .len(),
        12000
    );
}
#[tokio::test]
async fn only_meaningful_progress_changes_the_cursor() {
    let (_d, g, p, _) = fixture().await;
    let id = insert(&g, "file_upload", None).await;
    let progress = |bytes, elapsed| json!({"snapshot":{"phase":"transferring","bytes_done":bytes,"total_bytes":100,"elapsed_ms":elapsed,"retry_count":0},"reported_at":now(),"origin":"agent"});
    g.store
        .put("operation_progress", &id, &progress(10, 1), now() + 100)
        .await
        .unwrap();
    let a = call(&g, &p, json!({"operation_ids":[id]})).await;
    g.store
        .put("operation_progress", &id, &progress(10, 20), now() + 100)
        .await
        .unwrap();
    let b = call(
        &g,
        &p,
        json!({"operation_ids":[id],"cursor":a["observation"]["cursor"]}),
    )
    .await;
    assert_eq!(b["observation"]["changed"], false);
    g.store
        .put("operation_progress", &id, &progress(20, 30), now() + 100)
        .await
        .unwrap();
    let c = call(
        &g,
        &p,
        json!({"operation_ids":[id],"cursor":b["observation"]["cursor"]}),
    )
    .await;
    assert_eq!(c["observation"]["changed"], true);
}
#[tokio::test]
async fn malformed_mixed_or_duplicate_requests_never_create_jobs() {
    let (_d, g, p, _) = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    for args in [
        json!({}),
        json!({"operation_id":id,"operation_ids":[id]}),
        json!({"request_id":format!("req_{}", "c".repeat(32)),"operation_id":id}),
        json!({"operation_ids":[id,id]}),
        json!({"operation_ids":[]}),
        json!({"operation_id":id,"wait_ms":5001}),
    ] {
        assert!(g.dispatch(&p, "operation_get", args).await.is_err());
    }
    assert_eq!(count(&g).await, 0);
}
#[tokio::test]
async fn device_manifest_detects_schema_mismatch_without_guessing_old_agent_features() {
    let (_d, g, p, _) = fixture().await;
    let a = g.dispatch(&p, "devices_list", json!({})).await.unwrap();
    let expected = hash(serde_json::to_vec(&tools::catalog()).unwrap());
    assert_eq!(a["gateway"]["tools_sha256"], expected);
    assert_eq!(a["devices"][0]["runtime_features_status"], "not_reported");
    assert_eq!(
        a["capability_layers"]["current_session_exposure"]["status"],
        "unknown_not_reported"
    );
    assert_eq!(
        a["capability_layers"]["current_session_exposure"]["host_catalog_hash_supplied"],
        false
    );
    assert_eq!(
        a["capability_layers"]["account_authorization"]["status"],
        "known"
    );
    assert_eq!(
        a["capability_layers"]["server_catalog"]["tool_schema_revision"],
        expected
    );
    let good = g
        .dispatch(&p, "devices_list", json!({"known_tools_sha256":expected}))
        .await
        .unwrap();
    assert_eq!(good["gateway"]["refresh_required"], false);
    assert_eq!(
        good["capability_layers"]["current_session_exposure"]["status"],
        "unknown_not_reported"
    );
    let bad = g
        .dispatch(
            &p,
            "devices_list",
            json!({"known_tools_sha256":"0".repeat(64)}),
        )
        .await
        .unwrap();
    assert_eq!(bad["gateway"]["refresh_required"], true);
    assert_eq!(
        bad["capability_layers"]["current_session_exposure"]["status"],
        "unknown_not_reported"
    );
    assert!(
        g.dispatch(
            &p,
            "devices_list",
            json!({"known_tools_sha256":"not-a-hash"})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn features_are_saved_only_from_authenticated_device_polls() {
    let (_d, g, p, token) = fixture().await;
    let id = insert(&g, "code_read", None).await;
    sqlx::query("UPDATE jobs SET state='queued' WHERE id=?")
        .bind(&id)
        .execute(&g.store.pool)
        .await
        .unwrap();
    let hello = json!({"version":"0.3.2","session":random(),"roots":[],"allow_write":true,"allow_exec":true,"lanes":["read"],"runtime_features":{"protocol":1,"names":["terminal_wait_v1"]}});
    assert_eq!(
        post(&g, "bad", "/device/poll", hello.clone())
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&g, &token, "/device/poll", hello.clone())
            .await
            .status(),
        StatusCode::OK
    );
    let out = g.dispatch(&p, "devices_list", json!({})).await.unwrap();
    assert_eq!(
        out["devices"][0]["runtime_features"]["names"],
        json!(["terminal_wait_v1"])
    );
    let mut bad = hello;
    bad["runtime_features"]["names"] = json!(["x".repeat(65)]);
    let response = post(&g, &token, "/device/poll", bad).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let _ = to_bytes(response.into_body(), 1024).await.unwrap();
}

//! Cost budgets use SQL triggers only in an isolated fixture, never production.
use super::*;
use std::time::Duration;

async fn counters(f: &Fixture) {
    sqlx::query("CREATE TABLE test_writes(n INTEGER NOT NULL)")
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO test_writes VALUES(0)")
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    for statement in [
        "CREATE TRIGGER count_kv_insert AFTER INSERT ON kv BEGIN UPDATE test_writes SET n=n+1; END",
        "CREATE TRIGGER count_kv_update AFTER UPDATE ON kv BEGIN UPDATE test_writes SET n=n+1; END",
        "CREATE TRIGGER count_kv_delete AFTER DELETE ON kv BEGIN UPDATE test_writes SET n=n+1; END",
        "CREATE TRIGGER count_jobs_insert AFTER INSERT ON jobs BEGIN UPDATE test_writes SET n=n+1; END",
        "CREATE TRIGGER count_jobs_update AFTER UPDATE ON jobs BEGIN UPDATE test_writes SET n=n+1; END",
        "CREATE TRIGGER count_jobs_delete AFTER DELETE ON jobs BEGIN UPDATE test_writes SET n=n+1; END",
    ] {
        sqlx::query(statement)
            .execute(&f.g.store.pool)
            .await
            .unwrap();
    }
}
async fn writes(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT n FROM test_writes")
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap()
}
async fn request_rows(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM kv WHERE kind='request_receipt'")
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn one_hundred_observations_per_tool_have_zero_writes_and_no_extra_jobs() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("exited", now()))).await;
    f.link("budget", &id).await;
    counters(&f).await;
    for _ in 0..100 {
        for (tool, args) in [
            ("devices_list", json!({})),
            ("fleet_status", json!({})),
            ("task_context", json!({"task_id":"budget"})),
            ("operation_get", json!({"operation_id":id,"wait_ms":0})),
        ] {
            let value = receipts::invoke(&f.g, &f.p, tool, args, &request_id())
                .await
                .unwrap();
            assert!(value.get("error").is_none(), "{tool}: {value}");
            assert_eq!(value["receipt"]["durable"], false);
            assert_eq!(value["receipt"]["request_record_persisted"], false);
            assert_eq!(value["receipt"]["evidence_durable"], true);
            assert_eq!(value["receipt"]["protocol"], 2);
        }
    }
    assert_eq!(writes(&f).await, 0);
    assert_eq!(request_rows(&f).await, 0);
    assert_eq!(f.count().await, 1);
}

#[tokio::test]
async fn pure_observation_does_not_wait_for_an_external_sqlite_writer() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("exited", now()))).await;
    let tx = f.g.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(800),
        receipts::invoke(
            &f.g,
            &f.p,
            "operation_get",
            json!({"operation_id":id,"wait_ms":0}),
            &request_id(),
        ),
    )
    .await
    .expect("read must not wait for a writer")
    .unwrap();
    assert_eq!(result["receipt"]["evidence_complete"], true);
    assert_eq!(result["receipt"]["request_record_persisted"], false);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn explicit_full_audit_retains_start_and_final_record() {
    let mut f = Fixture::new().await;
    f.g.audit_observations = true;
    counters(&f).await;
    let id = request_id();
    let value = receipts::invoke(&f.g, &f.p, "devices_list", json!({}), &id)
        .await
        .unwrap();
    assert_eq!(value["receipt"]["durable"], true);
    assert_eq!(request_rows(&f).await, 1);
    assert_eq!(writes(&f).await, 2);
}

#[tokio::test]
async fn new_observation_failure_is_audited_once_but_saved_failed_exit_is_not() {
    let f = Fixture::new().await;
    let mut status = terminal("exited", now());
    status["exit_code"] = json!(7);
    let original = f.job(0, "done", Some(status)).await;
    counters(&f).await;
    for _ in 0..5 {
        let value = receipts::invoke(
            &f.g,
            &f.p,
            "operation_get",
            json!({"operation_id":original,"wait_ms":0}),
            &request_id(),
        )
        .await
        .unwrap();
        assert_eq!(value["receipt"]["process_exit_code"], 7);
        assert_eq!(value["receipt"]["next_action"], "inspect_original_receipt");
    }
    assert_eq!(writes(&f).await, 0);
    let query = request_id();
    let value = receipts::invoke(
        &f.g,
        &f.p,
        "operation_get",
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"wait_ms":0}),
        &query,
    )
    .await
    .unwrap();
    assert!(value.get("error").is_some());
    assert_eq!(value["receipt"]["request_record_persisted"], true);
    assert_eq!(value["receipt"]["evidence_complete"], false);
    assert_eq!(writes(&f).await, 1);
    assert!(
        f.g.store
            .get::<Value>("request_receipt", &query)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn observation_rechecks_scope_and_device_permission() {
    let mut f = Fixture::new().await;
    let id = f.job(0, "done", Some(terminal("exited", now()))).await;
    let mut limited = f.p.clone();
    limited.scopes = vec!["code:read".into()];
    let value = receipts::invoke(
        &f.g,
        &limited,
        "operation_get",
        json!({"operation_id":id,"wait_ms":0}),
        &request_id(),
    )
    .await
    .unwrap();
    assert!(value.get("error").is_some());
    assert!(value.get("output").is_none());
    std::sync::Arc::make_mut(&mut f.g.config).devices[0].scopes = vec!["code:read".into()];
    let value = receipts::invoke(
        &f.g,
        &f.p,
        "operation_get",
        json!({"operation_ids":[id],"wait_ms":0}),
        &request_id(),
    )
    .await
    .unwrap();
    assert!(value.get("error").is_some());
    assert!(value.get("operations").is_none());
    assert_eq!(f.count().await, 1);
}

#[tokio::test]
async fn observation_never_shadows_a_durable_execution_request_id() {
    let f = Fixture::new().await;
    let id = request_id();
    let entry = json!({"owner":"owner","fingerprint":"different","operation_id":"original"});
    f.g.store
        .put("request_receipt", &id, &entry, i64::MAX)
        .await
        .unwrap();
    assert!(
        receipts::invoke(&f.g, &f.p, "devices_list", json!({}), &id)
            .await
            .is_err()
    );
    assert_eq!(
        f.g.store
            .get::<Value>("request_receipt", &id)
            .await
            .unwrap()
            .unwrap(),
        entry
    );
}

#[tokio::test]
async fn download_capability_and_request_handle_recovery_keep_full_audit() {
    let f = Fixture::new().await;
    let id = f.job(0, "done", None).await;
    sqlx::query(
        "UPDATE jobs SET request=json_set(request,'$.tool','file_download'),result=? WHERE id=?",
    )
    .bind(json!({"state":"completed","error":"synthetic_export_failure"}).to_string())
    .bind(&id)
    .execute(&f.g.store.pool)
    .await
    .unwrap();
    for args in [
        json!({"operation_id":id,"wait_ms":0}),
        json!({"operation_ids":[id],"wait_ms":0}),
    ] {
        let query = request_id();
        let value = receipts::invoke(&f.g, &f.p, "operation_get", args, &query)
            .await
            .unwrap();
        assert_eq!(value["receipt"]["durable"], true);
        assert!(
            f.g.store
                .get::<Value>("request_receipt", &query)
                .await
                .unwrap()
                .is_some()
        );
    }
    let trace = request_id();
    receipts::invoke(&f.g, &f.p, "devices_list", json!({}), &trace)
        .await
        .unwrap();
    let missing =
        f.g.dispatch(&f.p, "operation_get", json!({"request_id":trace}))
            .await
            .unwrap();
    assert_eq!(missing["state"], "not_observed_or_expired");
    assert_eq!(missing["unretained_observation_possible"], true);
    assert_eq!(missing["execution_state"], "unknown");
}

#[tokio::test]
async fn failing_error_audit_does_not_turn_denial_into_success() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_observer_error BEFORE INSERT ON kv WHEN NEW.kind='request_receipt' BEGIN SELECT RAISE(ABORT,'fixture'); END")
        .execute(&f.g.store.pool).await.unwrap();
    let okay = receipts::invoke(&f.g, &f.p, "devices_list", json!({}), &request_id())
        .await
        .unwrap();
    assert_eq!(okay["receipt"]["evidence_complete"], true);
    let mut limited = f.p.clone();
    limited.scopes.clear();
    let value = receipts::invoke(&f.g, &limited, "devices_list", json!({}), &request_id())
        .await
        .unwrap();
    assert!(value.get("error").is_some());
    assert_eq!(value["receipt"]["durable"], false);
    assert_eq!(
        value["receipt"]["persistence_error"],
        "observation_error_record_not_saved"
    );
    assert_eq!(value["receipt"]["evidence_complete"], false);
}

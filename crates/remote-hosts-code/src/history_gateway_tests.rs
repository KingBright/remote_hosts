use super::*;
use crate::job_receipts::{self, Outcome};

#[tokio::test]
async fn malformed_results_are_preserved_without_stopping_other_history_cleanup() {
    let (_dir, store) = fixture().await;
    let mut protected = Vec::new();
    for malformed in ["[", "[]", "null"] {
        let row = job(&store, "code_read", 40, &json!({"state":"completed"})).await;
        sqlx::query("UPDATE jobs SET result=? WHERE id=?")
            .bind(malformed)
            .bind(&row.0)
            .execute(&store.pool)
            .await
            .unwrap();
        protected.push((row.0, malformed));
    }
    let valid = job(&store, "code_read", 40, &json!({"output":"expired"})).await;
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 1);
    assert_eq!(report["item_errors"], 3);
    assert_eq!(report["protected_bodies"], 3);
    assert_eq!(result(&store, &valid.0).await["state"], "history_expired");
    for (id, expected) in protected {
        let stored: String = sqlx::query_scalar("SELECT result FROM jobs WHERE id=?")
            .bind(id)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(stored, expected);
    }
}

async fn fixture() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    store.install_gateway_schema().await.unwrap();
    (dir, store)
}
async fn job(store: &Store, tool: &str, days: i64, result: &Value) -> Row {
    let id = uuid::Uuid::new_v4().to_string();
    let request = json!({"id":id,"device_id":"device","owner":"owner","tool":tool,"arguments":{"workspace_id":"device:workspace","command":"private body"}});
    let at = crate::now() - days * 86400;
    sqlx::query("INSERT INTO jobs(id,device,idem,fingerprint,request,result,state,updated) VALUES(?,'device',?,'original-fingerprint',?,?,'done',?)")
        .bind(&id).bind(format!("idem-{id}")).bind(request.to_string()).bind(result.to_string()).bind(at).execute(&store.pool).await.unwrap();
    (
        id,
        "device".into(),
        request.to_string(),
        result.to_string(),
        at,
        None,
    )
}
async fn result(store: &Store, id: &str) -> Value {
    let s: String = sqlx::query_scalar("SELECT result FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    serde_json::from_str(&s).unwrap()
}
#[tokio::test]
async fn old_bodies_expire_but_authorization_and_idempotency_identity_survive() {
    let (_dir, store) = fixture().await;
    let original = json!({"output":"bulk-result","state":"completed"});
    let row = job(&store, "code_read", 8, &original).await;
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 1);
    let saved = result(&store, &row.0).await;
    assert_eq!(saved["error"], "history_expired");
    assert!(saved["output"].is_null());
    let (device, idem, fp, request): (String, String, String, String) =
        sqlx::query_as("SELECT device,idem,fingerprint,request FROM jobs WHERE id=?")
            .bind(&row.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(device, "device");
    assert_eq!(idem, format!("idem-{}", row.0));
    assert_eq!(fp, "original-fingerprint");
    let request: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(request["owner"], "owner");
    assert_eq!(request["arguments"]["workspace_id"], "device:workspace");
    assert!(request["arguments"]["command"].is_null());
}
#[tokio::test]
async fn lost_ack_retries_still_match_original_digest_after_expiry() {
    let (_dir, store) = fixture().await;
    let original = json!({"output":"original","state":"completed"});
    let row = job(&store, "code_read", 8, &original).await;
    sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(
        job_receipts::commit(&store, "device", &row.0, &original)
            .await
            .unwrap(),
        Outcome::Accepted { duplicate: true }
    );
    assert_eq!(
        job_receipts::commit(
            &store,
            "device",
            &row.0,
            &json!({"output":"different","state":"completed"})
        )
        .await
        .unwrap(),
        Outcome::Conflict
    );
    assert_eq!(
        job_receipts::commit(&store, "foreign", &row.0, &original)
            .await
            .unwrap(),
        Outcome::NotFound
    );
    assert_eq!(result(&store, &row.0).await["error"], "history_expired");
}
#[tokio::test]
async fn original_failure_window_unknown_and_partial_results_are_preserved() {
    let (_dir, store) = fixture().await;
    let failed = job(&store, "code_read", 8, &json!({"error":"file_not_found"})).await;
    let old_failed = job(&store, "code_read", 31, &json!({"error":"file_not_found"})).await;
    let unknown = job(
        &store,
        "terminal_exec",
        40,
        &json!({"error":"outcome_unknown"}),
    )
    .await;
    let partial = job(
        &store,
        "code_apply_edits",
        40,
        &json!({"state":"partial","change_set":{"state":"partial"}}),
    )
    .await;
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 1);
    assert_eq!(
        result(&store, &old_failed.0).await["error"],
        "history_expired"
    );
    assert_eq!(result(&store, &failed.0).await["error"], "file_not_found");
    assert_eq!(result(&store, &unknown.0).await["error"], "outcome_unknown");
    assert_eq!(result(&store, &partial.0).await["state"], "partial");
}
#[tokio::test]
async fn semantic_guard_prevents_body_retirement() {
    let (_dir, store) = fixture().await;
    let row = job(
        &store,
        "code_apply_edits",
        40,
        &json!({"state":"completed"}),
    )
    .await;
    sqlx::query("INSERT INTO semantic_guards VALUES('scope',?,'outcome_unknown',0)")
        .bind(&row.0)
        .execute(&store.pool)
        .await
        .unwrap();
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 0);
    assert_eq!(result(&store, &row.0).await["state"], "completed");
}
#[tokio::test]
async fn terminal_ttl_starts_at_final_exit_not_old_submission() {
    let (_dir, store) = fixture().await;
    let mut row = job(
        &store,
        "terminal_exec",
        40,
        &json!({"state":"running","terminal":{"state":"running","output_complete":false}}),
    )
    .await;
    let observed = json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true,"updated_at":crate::now()},"output_preview":"private output"});
    store
        .put("terminal_observation", &row.0, &observed, i64::MAX)
        .await
        .unwrap();
    row.5 = Some(observed.to_string());
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 0);
    let report = sweep(&store, &Policy::default(), crate::now() + 8 * 86400)
        .await
        .unwrap();
    assert_eq!(report["expired_bodies"], 1);
    let saved = result(&store, &row.0).await;
    assert_eq!(saved["terminal"]["exit_code"], 0);
    assert!(!saved.to_string().contains("private output"));
    assert!(
        store
            .get::<Value>("terminal_observation", &row.0)
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn changed_observation_cannot_be_retired_from_stale_selection() {
    let (_dir, store) = fixture().await;
    let row = job(&store, "code_read", 8, &json!({"state":"completed"})).await;
    let (brief, _, _) = summary(&row, crate::now()).unwrap().unwrap();
    store
        .put(
            "terminal_observation",
            &row.0,
            &json!({"terminal":{"state":"running"}}),
            i64::MAX,
        )
        .await
        .unwrap();
    assert!(!retire(&store, &row, &brief).await.unwrap());
}
#[tokio::test]
async fn failed_compaction_transaction_keeps_original_payload_and_no_digest() {
    let (_dir, store) = fixture().await;
    let row = job(&store, "code_read", 8, &json!({"output":"keep"})).await;
    sqlx::query("CREATE TRIGGER reject_archive BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&store.pool).await.unwrap();
    assert!(
        sweep(&store, &Policy::default(), crate::now())
            .await
            .is_err()
    );
    assert_eq!(result(&store, &row.0).await["output"], "keep");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_result_digests")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
#[tokio::test]
async fn count_pressure_and_repeated_sweeps_do_not_grow_duplicate_records() {
    let (_dir, store) = fixture().await;
    let old = job(&store, "code_read", 5, &json!({"output":"old"})).await;
    let new = job(&store, "code_read", 2, &json!({"output":"new"})).await;
    let policy = Policy {
        max_items: 1,
        ..Policy::default()
    };
    let report = sweep(&store, &policy, crate::now()).await.unwrap();
    assert_eq!(report["expired_bodies"], 1);
    assert_eq!(result(&store, &old.0).await["error"], "history_expired");
    assert_eq!(result(&store, &new.0).await["output"], "new");
    let second = sweep(&store, &policy, crate::now()).await.unwrap();
    assert_eq!(second["expired_bodies"], 0);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_result_digests")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn finite_expired_entries_are_cleaned_without_an_incoming_transfer() {
    let (_dir, store) = fixture().await;
    store
        .put(
            "access",
            "old",
            &json!({"token":"expired"}),
            crate::now() - 1,
        )
        .await
        .unwrap();
    store
        .put("online", "device", &json!({"last_seen":1}), i64::MAX)
        .await
        .unwrap();
    let report = sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    assert_eq!(report["expired_cache_rows"], 1);
    assert!(
        store
            .get::<Value>("online", "device")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn expired_history_receipt_is_completed_not_an_unknown_execution() {
    let (_dir, store) = fixture().await;
    let row = job(&store, "code_read", 8, &json!({"output":"expired output"})).await;
    sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    let archived = result(&store, &row.0).await;
    let receipt = crate::receipts::decision(&archived, None, Some(&row.0), crate::now());
    assert_eq!(receipt["execution_state"], "completed");
    assert_eq!(receipt["evidence_complete"], false);
    assert_eq!(receipt["error_code"], "history_expired");
    assert_eq!(receipt["failure_boundary"], "history_retention");
    assert_eq!(receipt["next_action"], "history_expired_do_not_replay");
    assert_eq!(receipt["retry_policy"], "do_not_replay_completed_operation");
}

#[tokio::test]
async fn expired_terminal_receipt_preserves_exit_without_requesting_deleted_output() {
    let (_dir, store) = fixture().await;
    let output = json!({"terminal":{"state":"exited","exit_code":3,"output_complete":true,
        "updated_at":crate::now()-31*86400},"output":"failed private output"});
    let row = job(&store, "terminal_exec", 31, &output).await;
    sweep(&store, &Policy::default(), crate::now())
        .await
        .unwrap();
    let archived = result(&store, &row.0).await;
    let receipt = crate::receipts::decision(&archived, None, Some(&row.0), crate::now());
    assert_eq!(receipt["execution_state"], "exited");
    assert_eq!(receipt["process_exit_code"], 3);
    assert_eq!(receipt["process_outcome"], "failed");
    assert_eq!(receipt["evidence_complete"], false);
    assert_eq!(receipt["next_action"], "history_expired_do_not_replay");
}

#[tokio::test]
async fn unconfirmed_tool_reply_is_not_disposable_history() {
    let (_dir, store) = fixture().await;
    let variants = [
        json!({"state":"unknown"}),
        json!({"state":"running"}),
        json!({"pending":true}),
        json!({"execution_state":"unknown","error":"execution_failed"}),
        json!({"execution_state":"not_started_or_partial"}),
    ];
    for body in variants {
        job(&store, "code_apply_edits", 40, &body).await;
    }
    let report = sweep(
        &store,
        &Policy {
            max_items: 0,
            max_bytes: 0,
            ..Policy::default()
        },
        crate::now(),
    )
    .await
    .unwrap();
    assert_eq!(report["expired_bodies"], 0);
    assert_eq!(report["protected_bodies"], 5);
}

#[tokio::test]
async fn legacy_terminal_clock_preserves_original_receipt_and_expires_eventually() {
    let (_dir, store) = fixture().await;
    let original = json!({"state":"exited","output":"legacy output",
        "terminal":{"state":"exited","exit_code":0,"output_complete":true}});
    let row = job(&store, "terminal_exec", 60, &original).await;
    let at = crate::now();
    let first = sweep(&store, &Policy::default(), at).await.unwrap();
    assert_eq!(first["legacy_clocks_initialized"], 1);
    assert_eq!(first["expired_bodies"], 0);
    assert_eq!(result(&store, &row.0).await, original);
    assert_eq!(
        job_receipts::commit(&store, "device", &row.0, &original)
            .await
            .unwrap(),
        Outcome::Accepted { duplicate: true }
    );
    let next = sweep(&store, &Policy::default(), at + 3 * 86400)
        .await
        .unwrap();
    assert_eq!(next["legacy_clocks_initialized"], 0);
    assert_eq!(next["expired_bodies"], 0);
    let clock: Value = store
        .get("history_retention_clock", &row.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clock["confirmed_at"], at);
    let expired = sweep(&store, &Policy::default(), at + 8 * 86400)
        .await
        .unwrap();
    assert_eq!(expired["expired_bodies"], 1);
    assert!(
        store
            .get::<Value>("history_retention_clock", &row.0)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(result(&store, &row.0).await["error"], "history_expired");
    assert_eq!(
        job_receipts::commit(&store, "device", &row.0, &original)
            .await
            .unwrap(),
        Outcome::Accepted { duplicate: true }
    );
}

#[tokio::test]
async fn legacy_gateway_capacity_keeps_first_day_and_unknown_results_get_no_clock() {
    let (_dir, store) = fixture().await;
    let complete = job(
        &store,
        "terminal_exec",
        60,
        &json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true}}),
    )
    .await;
    let unknown = job(
        &store,
        "terminal_exec",
        60,
        &json!({"terminal":{"state":"runtime_lost","output_complete":false}}),
    )
    .await;
    let policy = Policy {
        max_items: 0,
        max_bytes: 0,
        ..Policy::default()
    };
    let at = crate::now();
    let first = sweep(&store, &policy, at).await.unwrap();
    assert_eq!(first["legacy_clocks_initialized"], 1);
    assert_eq!(first["expired_bodies"], 0);
    assert!(
        store
            .get::<Value>("history_retention_clock", &unknown.0)
            .await
            .unwrap()
            .is_none()
    );
    let hour = sweep(&store, &policy, at + 3600).await.unwrap();
    assert_eq!(hour["expired_bodies"], 0);
    let day = sweep(&store, &policy, at + 86401).await.unwrap();
    assert_eq!(day["expired_bodies"], 1);
    assert_eq!(
        result(&store, &complete.0).await["error"],
        "history_expired"
    );
    assert_eq!(
        result(&store, &unknown.0).await["terminal"]["state"],
        "runtime_lost"
    );
}

#[tokio::test]
async fn changed_observation_prevents_initializing_a_stale_legacy_clock() {
    let (_dir, store) = fixture().await;
    let row = job(
        &store,
        "terminal_exec",
        60,
        &json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true}}),
    )
    .await;
    store
        .put(
            "terminal_observation",
            &row.0,
            &json!({"terminal":{"state":"running"}}),
            i64::MAX,
        )
        .await
        .unwrap();
    assert!(
        legacy_clock(&store, &row, crate::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get::<Value>("history_retention_clock", &row.0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn bounded_ticks_preserve_progress_across_restart() {
    let (_dir, store) = fixture().await;
    for _ in 0..21 {
        job(
            &store,
            "code_read",
            40,
            &json!({"output":"private history body"}),
        )
        .await;
    }
    let at = crate::now();
    let policy = Policy::default();
    let mut scan = scan::Scan::default();
    let mut first = scan::Tick::default();
    scan::step(&store, &policy, at, &mut scan, &mut first)
        .await
        .unwrap();
    assert!(first.expired_bodies > 0 && first.expired_bodies <= 8);
    assert!(first.writes <= 8 && first.scanned <= 256);
    assert!(!scan.complete);
    let encoded = serde_json::to_value(&scan).unwrap();
    assert!(!encoded.to_string().contains("private history body"));
    assert!(encoded.to_string().len() < 4096);
    let mut scan = scan::Scan::restore(&encoded, at);
    let mut retired = first.expired_bodies;
    for _ in 0..30 {
        let mut tick = scan::Tick::default();
        scan::step(&store, &policy, at, &mut scan, &mut tick)
            .await
            .unwrap();
        assert!(tick.writes <= 8 && tick.scanned <= 256);
        retired += tick.expired_bodies;
        if scan.complete {
            break;
        }
    }
    assert!(scan.complete);
    assert_eq!(retired, 21);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_result_digests")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(n, 21);
}

#[tokio::test]
async fn read_budget_carries_partial_inventory_instead_of_restarting_from_head() {
    let (_dir, store) = fixture().await;
    for _ in 0..300 {
        job(&store, "code_read", 0, &json!({"output":"fresh"})).await;
    }
    let at = crate::now();
    let policy = Policy::default();
    let mut scan = scan::Scan::default();
    let mut seen = 0;
    for _ in 0..20 {
        let mut tick = scan::Tick::default();
        scan::step(&store, &policy, at, &mut scan, &mut tick)
            .await
            .unwrap();
        assert!(tick.scanned <= 256);
        assert_eq!(tick.writes, 0);
        seen += tick.scanned;
        if scan.complete {
            break;
        }
        assert!(!scan.after.is_empty());
        assert!(tick.budget_yield);
        scan = scan::Scan::restore(&serde_json::to_value(&scan).unwrap(), at);
    }
    assert!(scan.complete);
    assert_eq!(seen, 300);
    assert_eq!(scan.retained, 300);
}

#[tokio::test]
async fn failed_write_keeps_the_last_successful_cursor_and_original_uncommitted_body() {
    let (_dir, store) = fixture().await;
    let mut rows = Vec::new();
    for _ in 0..4 {
        rows.push(
            job(
                &store,
                "code_read",
                40,
                &json!({"output":"keep until committed"}),
            )
            .await,
        );
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    sqlx::query("CREATE TABLE fixture_reject(id TEXT PRIMARY KEY)")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO fixture_reject VALUES(?)")
        .bind(&rows[1].0)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fixture_history_failure BEFORE UPDATE ON jobs WHEN EXISTS(SELECT 1 FROM fixture_reject WHERE id=OLD.id) BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&store.pool).await.unwrap();
    let policy = Policy::default();
    let at = crate::now();
    let mut scan = scan::Scan::default();
    let mut retired = 0;
    for _ in 0..5 {
        let mut tick = scan::Tick::default();
        let result = scan::step(&store, &policy, at, &mut scan, &mut tick).await;
        retired += tick.expired_bodies;
        if result.is_err() {
            break;
        }
    }
    assert_eq!(retired, 1);
    assert_eq!(scan.after, rows[0].0);
    assert_eq!(
        result(&store, &rows[1].0).await["output"],
        "keep until committed"
    );
    sqlx::query("DROP TRIGGER fixture_history_failure")
        .execute(&store.pool)
        .await
        .unwrap();
    for _ in 0..10 {
        let mut tick = scan::Tick::default();
        scan::step(&store, &policy, at, &mut scan, &mut tick)
            .await
            .unwrap();
        retired += tick.expired_bodies;
        if scan.complete {
            break;
        }
    }
    assert!(scan.complete);
    assert_eq!(retired, 4);
}

#[tokio::test]
async fn contended_writer_preserves_cursor_and_recovers_without_replaying_a_job() {
    let (dir, store) = fixture().await;
    let row = job(&store, "code_read", 40, &json!({"output":"original"})).await;
    let low = crate::history_retention::maintenance_store(dir.path())
        .await
        .unwrap();
    let tx = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let at = crate::now();
    let mut scan = scan::Scan::default();
    let mut tick = scan::Tick::default();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        scan::step(&low, &Policy::default(), at, &mut scan, &mut tick),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        error_category(&error),
        "storage_busy" | "writer_budget" | "connection_budget"
    ));
    assert!(scan.after.is_empty());
    assert_eq!(tick.expired_bodies, 0);
    tx.rollback().await.unwrap();
    let mut tick = scan::Tick::default();
    scan::step(&low, &Policy::default(), at, &mut scan, &mut tick)
        .await
        .unwrap();
    assert_eq!(tick.expired_bodies, 1);
    assert_eq!(result(&store, &row.0).await["error"], "history_expired");
}

#[test]
fn restored_cursor_is_bounded_and_failed_rounds_back_off_without_hiding_progress() {
    for value in [
        json!({"at":1,"after":"not-a-uuid"}),
        json!({"at":i64::MAX}),
        json!({"at":1,"oldest":[{"id":"../x","at":1,"bytes":0}]}),
        json!("bad"),
    ] {
        assert_eq!(scan::Scan::restore(&value, crate::now()).at, 0);
    }
    assert_eq!(next_delay(&json!({"has_more":true})), 5);
    assert_eq!(
        next_delay(&json!({"has_more":true,"deferred":true,"expired_bodies":3})),
        60
    );
    assert_eq!(
        next_delay(&json!({"has_more":false,"pressure_remaining":false})),
        300
    );
    assert_eq!(
        error_category(&anyhow::anyhow!("secret body must not be logged")),
        "maintenance_error"
    );
}

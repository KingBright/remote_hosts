use super::*;
use crate::{delivery::Delivery, files::Workspace};

struct Fixture {
    _root: tempfile::TempDir,
    state: tempfile::TempDir,
    config: AgentConfig,
    store: Store,
    ws: Workspace,
    access: Arc<tokio::sync::RwLock<()>>,
}
async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = AgentConfig {
        gateway_url: "https://fixture.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: crate::random(),
        state_dir: state.path().into(),
        roots: vec![root.path().into()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    };
    let store = Store::open(state.path()).await.unwrap();
    store.install_agent_schema().await.unwrap();
    Delivery::new(store.clone(), Arc::new(config.clone()))
        .await
        .unwrap();
    let ws = Workspace {
        id: format!("{}:{}", config.device_id, uuid::Uuid::new_v4()),
        device_id: config.device_id.clone(),
        root: root.path().into(),
    };
    store.put("workspace", &ws.id, &ws, i64::MAX).await.unwrap();
    std::fs::create_dir_all(state.path().join("terminals")).unwrap();
    std::fs::create_dir_all(state.path().join("edits")).unwrap();
    Fixture {
        _root: root,
        state,
        config,
        store,
        ws,
        access: Arc::default(),
    }
}
async fn terminal(f: &Fixture, age_days: i64, code: i64, bytes: usize) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let at = crate::now() - age_days * 86400;
    f.store
        .put(
            "terminal",
            &id,
            &json!({"id":id,"workspace_id":f.ws.id,"state":"exited","exit_code":code,
        "output_complete":true,"created_at":at,"updated_at":at}),
            i64::MAX,
        )
        .await
        .unwrap();
    std::fs::write(
        f.state.path().join("terminals").join(format!("{id}.log")),
        vec![b'x'; bytes],
    )
    .unwrap();
    id
}
async fn run(f: &Fixture, policy: &Policy) -> Report {
    sweep(&f.store, &f.config, policy, crate::now(), &f.access)
        .await
        .unwrap()
}
async fn exists(f: &Fixture, kind: &str, id: &str) -> bool {
    f.store.get::<Value>(kind, id).await.unwrap().is_some()
}
async fn get_candidate(f: &Fixture, kind: &str, id: &str) -> Candidate {
    let text: Value = f.store.get(kind, id).await.unwrap().unwrap();
    let op: Option<Value> = f.store.get("local_operation", id).await.unwrap();
    let row = (
        kind.to_owned(),
        id.to_owned(),
        text.to_string(),
        op.map(|v| v.to_string()),
        0,
    );
    let root =
        cap_std::fs::Dir::open_ambient_dir(f.state.path(), cap_std::ambient_authority()).unwrap();
    candidate(&row, &f.config.device_id, &root)
        .unwrap()
        .unwrap()
}
#[test]
fn defaults_bound_bulk_history_without_configuration() {
    let p = Policy::default();
    assert_eq!(p.successful_seconds, 7 * 86400);
    assert_eq!(p.failed_seconds, 30 * 86400);
    assert_eq!(p.minimum_seconds, 86400);
    assert_eq!(p.max_items, 2000);
    assert_eq!(p.max_bytes, 512 * 1024 * 1024);
}
#[tokio::test]
async fn success_and_failure_have_different_retention_windows() {
    let f = fixture().await;
    let old = terminal(&f, 8, 0, 200).await;
    let recent = terminal(&f, 6, 0, 200).await;
    let failure = terminal(&f, 8, 1, 200).await;
    let old_failure = terminal(&f, 31, 1, 200).await;
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 2);
    assert_eq!(r.deleted_files, 2);
    assert_eq!(r.unlinked_bytes, 400);
    assert!(!exists(&f, "terminal", &old).await);
    assert!(!exists(&f, "terminal", &old_failure).await);
    assert!(exists(&f, "terminal", &recent).await);
    assert!(exists(&f, "terminal", &failure).await);
}
#[tokio::test]
async fn capacity_removes_oldest_but_keeps_first_day() {
    let f = fixture().await;
    let old = terminal(&f, 5, 0, 100).await;
    let newer = terminal(&f, 2, 0, 100).await;
    let fresh = terminal(&f, 0, 0, 100).await;
    let p = Policy {
        max_items: 1,
        ..Policy::default()
    };
    let r = run(&f, &p).await;
    assert_eq!(r.retired_items, 2);
    assert!(!exists(&f, "terminal", &old).await);
    assert!(!exists(&f, "terminal", &newer).await);
    assert!(exists(&f, "terminal", &fresh).await);
    assert!(!r.pressure_remaining);
}
#[tokio::test]
async fn byte_pressure_is_independent_from_record_count() {
    let f = fixture().await;
    let old = terminal(&f, 4, 0, 3000).await;
    let newer = terminal(&f, 2, 0, 100).await;
    let p = Policy {
        max_bytes: 1000,
        ..Policy::default()
    };
    let r = run(&f, &p).await;
    assert_eq!(r.retired_items, 1);
    assert!(!exists(&f, "terminal", &old).await);
    assert!(exists(&f, "terminal", &newer).await);
    assert!(r.retained_bytes < 1000);
}
#[tokio::test]
async fn active_unknown_unacknowledged_and_missing_timestamps_are_protected() {
    let f = fixture().await;
    let d = Delivery::new(f.store.clone(), Arc::new(f.config.clone()))
        .await
        .unwrap();
    for patch in [
        json!({"state":"running"}),
        json!({"state":"starting"}),
        json!({"state":"runtime_lost"}),
        json!({"output_complete":false}),
        json!({"created_at":0,"updated_at":0}),
        json!({"history_hold":true}),
        json!({"workspace_id":"another-device:workspace"}),
        json!({"updated_at":crate::now()+86400}),
    ] {
        let id = terminal(&f, 40, 0, 10).await;
        let mut v: Value = f.store.get("terminal", &id).await.unwrap().unwrap();
        for (k, value) in patch.as_object().unwrap() {
            v[k] = value.clone();
        }
        f.store.put("terminal", &id, &v, i64::MAX).await.unwrap();
    }
    for state in ["done", "unknown", "running"] {
        let id = terminal(&f, 40, 0, 10).await;
        f.store
            .put(
                "local_operation",
                &id,
                &json!({"tool":"terminal_exec","state":state,"gateway_accepted":false}),
                i64::MAX,
            )
            .await
            .unwrap();
    }
    let pending = terminal(&f, 40, 0, 10).await;
    d.enqueue(&pending, &json!({"result":"not-confirmed"}))
        .await
        .unwrap();
    let p = Policy {
        max_items: 0,
        max_bytes: 0,
        ..Policy::default()
    };
    let r = run(&f, &p).await;
    assert_eq!(r.retired_items, 0);
    assert_eq!(r.deleted_files, 0);
}
#[tokio::test]
async fn keyset_scan_does_not_skip_rows_deleted_in_previous_page() {
    let f = fixture().await;
    for _ in 0..130 {
        terminal(&f, 8, 0, 1).await;
    }
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 130);
    assert_eq!(r.cleanup_pending, 0);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv WHERE kind='terminal'")
        .fetch_one(&f.store.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
}
#[tokio::test]
async fn crash_after_retirement_retries_unlink_and_preserves_idempotency() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    f.store.put("local_operation",&id,&json!({"fingerprint":"original","tool":"terminal_exec","state":"done","gateway_accepted":true}),i64::MAX).await.unwrap();
    let c = get_candidate(&f, "terminal", &id).await;
    assert!(retire(&f.store, &c, &f.access).await.unwrap());
    let path = f.state.path().join("terminals").join(format!("{id}.log"));
    assert!(path.exists());
    assert!(!exists(&f, "terminal", &id).await);
    let reopened = Store::open(f.state.path()).await.unwrap();
    let mut r = Report::default();
    drain_files(&reopened, f.state.path(), &mut r, &f.access)
        .await
        .unwrap();
    assert!(!path.exists());
    assert_eq!(r.cleanup_pending, 0);
    let marker: Value = f.store.get("local_operation", &id).await.unwrap().unwrap();
    assert_eq!(marker["fingerprint"], "original");
    assert_eq!(marker["gateway_accepted"], true);
    drain_files(&reopened, f.state.path(), &mut r, &f.access)
        .await
        .unwrap();
    assert_eq!(r.deleted_files, 1);
}
#[tokio::test]
async fn transaction_failure_never_deletes_the_file_or_leaves_unlink_intent() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let c = get_candidate(&f, "terminal", &id).await;
    sqlx::query("CREATE TRIGGER reject_history_delete BEFORE DELETE ON kv WHEN OLD.kind='terminal' BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&f.store.pool).await.unwrap();
    assert!(retire(&f.store, &c, &f.access).await.is_err());
    assert!(exists(&f, "terminal", &id).await);
    assert!(
        f.state
            .path()
            .join("terminals")
            .join(format!("{id}.log"))
            .exists()
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_file_cleanup")
        .fetch_one(&f.store.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
}
#[tokio::test]
async fn concurrent_outbox_creation_invalidates_old_cleanup_candidate() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let c = get_candidate(&f, "terminal", &id).await;
    let d = Delivery::new(f.store.clone(), Arc::new(f.config.clone()))
        .await
        .unwrap();
    d.enqueue(&id, &json!({"accepted":false})).await.unwrap();
    assert!(!retire(&f.store, &c, &f.access).await.unwrap());
    assert!(exists(&f, "terminal", &id).await);
}
#[tokio::test]
async fn concurrent_lifecycle_change_invalidates_old_cleanup_candidate() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let c = get_candidate(&f, "terminal", &id).await;
    sqlx::query(
        "UPDATE kv SET value=json_set(value,'$.state','running') WHERE kind='terminal' AND key=?",
    )
    .bind(&id)
    .execute(&f.store.pool)
    .await
    .unwrap();
    assert!(!retire(&f.store, &c, &f.access).await.unwrap());
}
#[tokio::test]
async fn history_reader_makes_cleanup_yield_instead_of_waiting() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let c = get_candidate(&f, "terminal", &id).await;
    let guard = f.access.read().await;
    assert!(
        !tokio::time::timeout(Duration::from_millis(200), retire(&f.store, &c, &f.access))
            .await
            .unwrap()
            .unwrap()
    );
    drop(guard);
    assert!(retire(&f.store, &c, &f.access).await.unwrap());
}
async fn edit(f: &Fixture, status: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    f.store.put("local_operation",&id,&json!({"workspace_id":f.ws.id,"tool":"code_apply_edits","state":"done","gateway_accepted":true,
        "updated_at":crate::now()-8*86400,"fingerprint":"original","result":{"change_set":{"state":"completed"}}}),i64::MAX).await.unwrap();
    let journal = json!({"change_set_id":id,"status":status,"workspace":f.ws,"files":[]});
    std::fs::write(
        f.state.path().join("edits").join(format!("{id}.json")),
        journal.to_string(),
    )
    .unwrap();
    id
}
#[tokio::test]
async fn completed_edit_is_compacted_but_partial_on_disk_is_never_deleted() {
    let f = fixture().await;
    let complete = edit(&f, "completed").await;
    let partial = edit(&f, "partial").await;
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 1);
    let marker: Value = f
        .store
        .get("local_operation", &complete)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(marker["fingerprint"], "original");
    assert_eq!(marker["history_expired"], true);
    assert!(marker["result"].is_null());
    assert!(
        f.state
            .path()
            .join("edits")
            .join(format!("{partial}.json"))
            .exists()
    );
}
#[cfg(unix)]
#[tokio::test]
async fn symlinked_history_directory_cannot_delete_outside_state() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let outside = tempfile::tempdir().unwrap();
    let filename = format!("{id}.log");
    std::fs::write(outside.path().join(&filename), "keep").unwrap();
    std::fs::remove_dir_all(f.state.path().join("terminals")).unwrap();
    std::os::unix::fs::symlink(outside.path(), f.state.path().join("terminals")).unwrap();
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 0);
    assert!(r.item_errors > 0);
    assert_eq!(
        std::fs::read_to_string(outside.path().join(filename)).unwrap(),
        "keep"
    );
}
#[tokio::test]
async fn missing_file_metadata_retires_and_unknown_files_are_left_alone() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 10).await;
    std::fs::remove_file(f.state.path().join("terminals").join(format!("{id}.log"))).unwrap();
    let unknown = f.state.path().join("terminals/notes.txt");
    std::fs::write(&unknown, "unowned").unwrap();
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 1);
    assert_eq!(r.unlinked_bytes, 0);
    assert!(unknown.exists());
}
#[tokio::test]
async fn dedicated_maintenance_connection_yields_quickly_to_writer() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 10).await;
    let c = get_candidate(&f, "terminal", &id).await;
    let low = maintenance_store(f.state.path()).await.unwrap();
    let busy_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&low.pool)
        .await
        .unwrap();
    assert_eq!(busy_ms, 50);
    let tx = f.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), retire(&low, &c, &f.access))
            .await
            .unwrap()
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(exists(&f, "terminal", &id).await);
    assert!(retire(&low, &c, &f.access).await.unwrap());
}
#[tokio::test]
async fn lifecycle_runs_automatically_and_does_not_spawn_terminal_jobs() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 100).await;
    let config = Arc::new(f.config.clone());
    let access = f.access.clone();
    let worker = tokio::spawn(async move {
        agent_loop_with_timing(config, access, Duration::ZERO, Duration::from_millis(100)).await
    });
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(report) = f
                .store
                .get::<Value>("runtime", "automatic_history_cleanup")
                .await
                .unwrap()
            {
                break report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.abort();
    let _ = worker.await;
    let report = result.unwrap();
    assert_eq!(report["automatic"], true);
    assert!(!exists(&f, "terminal", &id).await);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv WHERE kind='local_operation'")
        .fetch_one(&f.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn unconfirmed_terminal_completion_is_not_deleted() {
    let f = fixture().await;
    for (state, pid) in [
        ("cancelled", None),
        ("timed_out", None),
        ("failed", Some(1234)),
    ] {
        let id = terminal(&f, 40, 1, 20).await;
        let mut value: Value = f.store.get("terminal", &id).await.unwrap().unwrap();
        value["state"] = json!(state);
        value["exit_code"] = Value::Null;
        value["process_id"] = json!(pid);
        f.store
            .put("terminal", &id, &value, i64::MAX)
            .await
            .unwrap();
    }
    let report = run(
        &f,
        &Policy {
            max_items: 0,
            max_bytes: 0,
            ..Policy::default()
        },
    )
    .await;
    assert_eq!(report.retired_items, 0);
    assert_eq!(report.deleted_files, 0);
}

#[tokio::test]
async fn resumed_journal_changed_after_selection_is_not_deleted_by_stale_candidate() {
    let f = fixture().await;
    let id = edit(&f, "completed").await;
    let selected = get_candidate(&f, "local_operation", &id).await;
    let path = f.state.path().join("edits").join(format!("{id}.json"));
    {
        let _reader = f.access.read().await;
        std::fs::write(
            &path,
            json!({"change_set_id":id,"status":"partial","workspace":f.ws,"files":[]}).to_string(),
        )
        .unwrap();
    }
    assert!(!retire(&f.store, &selected, &f.access).await.unwrap());
    assert!(path.exists());
    let op: Value = f.store.get("local_operation", &id).await.unwrap().unwrap();
    assert!(op["result"].is_object());
    assert_ne!(op["history_expired"], true);
    let r = run(&f, &Policy::default()).await;
    assert_eq!(r.retired_items, 0);
}

#[tokio::test]
async fn journal_created_after_missing_file_selection_is_not_deleted() {
    let f = fixture().await;
    let id = edit(&f, "completed").await;
    let path = f.state.path().join("edits").join(format!("{id}.json"));
    std::fs::remove_file(&path).unwrap();
    let selected = get_candidate(&f, "local_operation", &id).await;
    std::fs::write(
        &path,
        json!({"change_set_id":id,"status":"partial","workspace":f.ws,"files":[]}).to_string(),
    )
    .unwrap();
    assert!(!retire(&f.store, &selected, &f.access).await.unwrap());
    assert!(path.exists());
}

#[tokio::test]
async fn cancelled_writer_admission_cannot_leave_a_late_transaction_locked() {
    let f = fixture().await;
    let id = terminal(&f, 8, 0, 20).await;
    let low = maintenance_store(f.state.path()).await.unwrap();
    // Deliberately make the driver's lock wait longer than our caller budget.
    // This exercises an unacknowledged late BEGIN, not a fast SQLITE_BUSY only.
    sqlx::query("PRAGMA busy_timeout=5000")
        .execute(&low.pool)
        .await
        .unwrap();
    let held = f.store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let started = tokio::time::Instant::now();
    let blocked = tokio::time::timeout(Duration::from_secs(1), begin_maintenance(&low))
        .await
        .expect("maintenance admission exceeded its own bound");
    assert!(blocked.is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    held.rollback().await.unwrap();
    // Observe the same connection after the late worker has finished rolling
    // back the abandoned BEGIN. A fresh transaction must still work normally.
    tokio::time::timeout(Duration::from_secs(2), async {
        let tx = low.pool.begin_with("BEGIN IMMEDIATE").await?;
        tx.rollback().await
    })
    .await
    .expect("cancelled BEGIN retained the writer")
    .unwrap();
    assert!(exists(&f, "terminal", &id).await);
    let tickets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_file_cleanup")
        .fetch_one(&f.store.pool)
        .await
        .unwrap();
    assert_eq!(tickets, 0);
    assert!(
        f.state
            .path()
            .join("terminals")
            .join(format!("{id}.log"))
            .exists()
    );
}

#[tokio::test]
async fn legacy_completion_starts_once_at_confirmation_and_eventually_expires() {
    let f = fixture().await;
    let success = terminal(&f, 60, 0, 40).await;
    let failure = terminal(&f, 60, 1, 40).await;
    for id in [&success, &failure] {
        sqlx::query(
            "UPDATE kv SET value=json_remove(value,'$.updated_at') WHERE kind='terminal' AND key=?",
        )
        .bind(id)
        .execute(&f.store.pool)
        .await
        .unwrap();
    }
    let at = crate::now();
    let policy = Policy::default();
    let first = sweep(&f.store, &f.config, &policy, at, &f.access)
        .await
        .unwrap();
    assert_eq!(first.legacy_clocks_initialized, 2);
    assert_eq!(first.retired_items, 0);
    let repeat = sweep(&f.store, &f.config, &policy, at + 3 * 86400, &f.access)
        .await
        .unwrap();
    assert_eq!(repeat.legacy_clocks_initialized, 0);
    assert_eq!(repeat.retired_items, 0);
    let saved: Value = f.store.get("terminal", &success).await.unwrap().unwrap();
    assert_eq!(saved["history_retention_confirmed_at"], at);
    assert!(
        saved.get("updated_at").is_none(),
        "retention must not invent a business completion time"
    );
    let week = sweep(&f.store, &f.config, &policy, at + 8 * 86400, &f.access)
        .await
        .unwrap();
    assert_eq!(week.retired_items, 1);
    assert!(!exists(&f, "terminal", &success).await);
    assert!(exists(&f, "terminal", &failure).await);
    let month = sweep(&f.store, &f.config, &policy, at + 31 * 86400, &f.access)
        .await
        .unwrap();
    assert_eq!(month.retired_items, 1);
    assert!(!exists(&f, "terminal", &failure).await);
}

#[tokio::test]
async fn legacy_pressure_keeps_first_day_then_reclaims_without_manual_intervention() {
    let f = fixture().await;
    let id = terminal(&f, 60, 0, 40).await;
    sqlx::query(
        "UPDATE kv SET value=json_remove(value,'$.updated_at') WHERE kind='terminal' AND key=?",
    )
    .bind(&id)
    .execute(&f.store.pool)
    .await
    .unwrap();
    let policy = Policy {
        max_items: 0,
        max_bytes: 0,
        ..Policy::default()
    };
    let at = crate::now();
    let first = sweep(&f.store, &f.config, &policy, at, &f.access)
        .await
        .unwrap();
    assert_eq!(first.legacy_clocks_initialized, 1);
    assert_eq!(first.retired_items, 0);
    assert!(first.pressure_remaining);
    let hour = sweep(&f.store, &f.config, &policy, at + 3600, &f.access)
        .await
        .unwrap();
    assert_eq!(hour.retired_items, 0);
    let day = sweep(&f.store, &f.config, &policy, at + 86401, &f.access)
        .await
        .unwrap();
    assert_eq!(day.retired_items, 1);
    assert!(!exists(&f, "terminal", &id).await);
}

#[tokio::test]
async fn legacy_clock_never_marks_uncertain_or_undelivered_work_disposable() {
    let f = fixture().await;
    let delivery = Delivery::new(f.store.clone(), Arc::new(f.config.clone()))
        .await
        .unwrap();
    let mut ids = Vec::new();
    for state in ["running", "runtime_lost", "exited"] {
        let id = terminal(&f, 60, 0, 40).await;
        sqlx::query("UPDATE kv SET value=json_set(json_remove(value,'$.updated_at'),'$.state',?) WHERE kind='terminal' AND key=?")
            .bind(state).bind(&id).execute(&f.store.pool).await.unwrap();
        if state == "exited" {
            delivery
                .enqueue(&id, &json!({"still_pending":true}))
                .await
                .unwrap();
        }
        ids.push(id);
    }
    let report = run(&f, &Policy::default()).await;
    assert_eq!(report.legacy_clocks_initialized, 0);
    assert_eq!(report.retired_items, 0);
    for id in ids {
        let saved: Value = f.store.get("terminal", &id).await.unwrap().unwrap();
        assert!(saved.get("history_retention_confirmed_at").is_none());
    }
}

#[tokio::test]
async fn legacy_clock_rechecks_outbox_and_a_new_authoritative_timestamp() {
    let f = fixture().await;
    let delivery = Delivery::new(f.store.clone(), Arc::new(f.config.clone()))
        .await
        .unwrap();
    for pending in [true, false] {
        let id = terminal(&f, 60, 0, 40).await;
        sqlx::query(
            "UPDATE kv SET value=json_remove(value,'$.updated_at') WHERE kind='terminal' AND key=?",
        )
        .bind(&id)
        .execute(&f.store.pool)
        .await
        .unwrap();
        let selected = get_candidate(&f, "terminal", &id).await;
        assert_eq!(selected.at, 0);
        if pending {
            delivery
                .enqueue(&id, &json!({"still_pending":true}))
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE kv SET value=json_set(value,'$.updated_at',?) WHERE kind='terminal' AND key=?")
                .bind(crate::now()).bind(&id).execute(&f.store.pool).await.unwrap();
        }
        assert!(
            !initialize_legacy_clock(&f.store, &selected, crate::now())
                .await
                .unwrap()
        );
        let saved: Value = f.store.get("terminal", &id).await.unwrap().unwrap();
        assert!(saved.get("history_retention_confirmed_at").is_none());
    }
}

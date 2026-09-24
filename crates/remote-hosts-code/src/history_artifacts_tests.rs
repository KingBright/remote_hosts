use super::*;
use crate::{DeviceRegistration, GatewayConfig, history_retention::Policy};

async fn fixture() -> (tempfile::TempDir, Gateway) {
    let dir = tempfile::tempdir().unwrap();
    let g = Gateway::new(GatewayConfig {
        public_url: "https://fixture.invalid".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().join("gateway"),
        owner: "test-owner".into(),
        password_hash: "unused".into(),
        allowed_origins: crate::default_mcp_client_origins(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: uuid::Uuid::new_v4().to_string(),
            name: "device".into(),
            token_hash: crate::hash("fixture"),
            scopes: vec!["code:read".into(), "code:write".into()],
        }],
    })
    .await
    .unwrap();
    std::fs::create_dir_all(g.config.state_dir.join("file-objects")).unwrap();
    (dir, g)
}
async fn blob(g: &Gateway, expires: i64) -> (String, std::path::PathBuf) {
    let id = uuid::Uuid::new_v4().to_string();
    let value = Blob {
        operation: id.clone(),
        device: g.config.devices[0].id.clone(),
        owner: g.config.owner.clone(),
        name: "cache.bin".into(),
        size: 3,
        sha256: crate::hash("abc"),
        expires,
    };
    g.store
        .put("file_blob", &id, &value, i64::MAX)
        .await
        .unwrap();
    let path = g
        .config
        .state_dir
        .join("file-objects")
        .join(format!("{id}.blob"));
    std::fs::write(&path, b"abc").unwrap();
    (id, path)
}
async fn session(g: &Gateway, expires: i64) -> (String, std::path::PathBuf) {
    let id = uuid::Uuid::new_v4().to_string();
    let value = Session {
        id: id.clone(),
        device: g.config.devices[0].id.clone(),
        owner: g.config.owner.clone(),
        size: 3,
        offset: 2,
        sha256: crate::hash("abc"),
        expires,
        completed: false,
    };
    g.store
        .put("transfer_receiver", &id, &value, i64::MAX)
        .await
        .unwrap();
    let path = g
        .config
        .state_dir
        .join("file-objects")
        .join(format!("{id}.part040"));
    std::fs::write(&path, b"ab").unwrap();
    (id, path)
}
#[tokio::test]
async fn automatic_tick_reclaims_expired_bytes_without_an_incoming_transfer() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (old, old_path) = blob(&g, at - 1).await;
    let (_, fresh) = blob(&g, at + 3600).await;
    let (expired, part) = session(&g, at - 1).await;
    let (_, active) = session(&g, at + 86400).await;
    let immutable = json!({"fingerprint":"keep","outcome":"done"});
    g.store
        .put("request_receipt", &old, &immutable, i64::MAX)
        .await
        .unwrap();
    let report = crate::history_gateway::maintenance_tick(&g, &g.store, &Policy::default(), at)
        .await
        .unwrap();
    assert_eq!(report["transfer_cache"]["retired"], 2);
    assert_eq!(report["transfer_cache"]["unlinked_bytes"], 5);
    assert!(!old_path.exists());
    assert!(!part.exists());
    assert!(fresh.exists());
    assert!(active.exists());
    assert!(
        g.store
            .get::<Value>("transfer_receiver", &expired)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        g.store
            .get::<Value>("request_receipt", &old)
            .await
            .unwrap()
            .unwrap(),
        immutable
    );
}
#[tokio::test]
async fn active_operation_and_allocation_locks_make_cleanup_yield() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (id, path) = blob(&g, at - 1).await;
    let permit = g
        .transfer_limits
        .try_acquire(&g.config.devices[0].id, &id)
        .unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(1), sweep(&g, &g.store, at))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.deferred, 1);
    assert!(path.exists());
    drop(permit);
    let allocation = g.transfer_limits.allocation.lock().await;
    assert_eq!(sweep(&g, &g.store, at).await.unwrap().deferred, 1);
    assert!(path.exists());
    drop(allocation);
    assert_eq!(sweep(&g, &g.store, at).await.unwrap().retired, 1);
    assert!(!path.exists());
}
#[tokio::test]
async fn malformed_metadata_and_unlink_failure_do_not_starve_other_items() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (bad, path) = blob(&g, at - 1).await;
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let broken = uuid::Uuid::new_v4().to_string();
    g.store
        .put("file_blob", &broken, &json!("not metadata"), i64::MAX)
        .await
        .unwrap();
    let (good, good_path) = blob(&g, at - 1).await;
    let report = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(report.retired, 1);
    assert_eq!(report.item_errors, 2);
    assert!(path.is_dir());
    assert!(!good_path.exists());
    assert!(
        g.store
            .get::<Value>("file_blob", &bad)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        g.store
            .get::<Value>("file_blob", &good)
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn missing_cache_payload_is_idempotent_but_unowned_files_are_untouched() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (id, path) = blob(&g, at - 1).await;
    std::fs::remove_file(path).unwrap();
    let unowned = g.config.state_dir.join("file-objects/notes.txt");
    std::fs::write(&unowned, "keep").unwrap();
    let first = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(first.retired, 1);
    assert_eq!(first.unlinked_bytes, 0);
    assert_eq!(sweep(&g, &g.store, at).await.unwrap().retired, 0);
    assert!(unowned.exists());
    assert!(
        g.store
            .get::<Value>("file_blob", &id)
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn bounded_cursor_reaches_the_tail_across_restart() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    for _ in 0..300 {
        blob(&g, at - 1).await;
    }
    let first = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(first.scanned, 256);
    assert_eq!(first.retired, 256);
    assert!(first.has_more);
    let reopened = Store::open(&g.config.state_dir).await.unwrap();
    let second = sweep(&g, &reopened, at).await.unwrap();
    assert_eq!(second.retired, 44);
    assert!(!second.has_more);
    assert_eq!(sweep(&g, &reopened, at).await.unwrap().scanned, 0);
}
#[tokio::test]
async fn database_failure_after_unlink_retries_expiry_not_the_transfer() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (id, path) = blob(&g, at - 1).await;
    sqlx::query("CREATE TRIGGER reject_cache_delete BEFORE DELETE ON kv WHEN OLD.kind='file_blob' BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&g.store.pool).await.unwrap();
    assert!(sweep(&g, &g.store, at).await.is_err());
    assert!(!path.exists());
    assert!(
        g.store
            .get::<Value>("file_blob", &id)
            .await
            .unwrap()
            .is_some()
    );
    sqlx::query("DROP TRIGGER reject_cache_delete")
        .execute(&g.store.pool)
        .await
        .unwrap();
    let retried = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(retried.retired, 1);
    assert_eq!(retried.deleted_files, 0);
}
#[tokio::test]
async fn idle_or_unexpired_cache_pass_does_not_write() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (_, path) = blob(&g, at + 3600).await;
    for sql in [
        "CREATE TRIGGER deny_insert BEFORE INSERT ON kv BEGIN SELECT RAISE(ABORT,'unexpected write'); END",
        "CREATE TRIGGER deny_update BEFORE UPDATE ON kv BEGIN SELECT RAISE(ABORT,'unexpected write'); END",
        "CREATE TRIGGER deny_delete BEFORE DELETE ON kv BEGIN SELECT RAISE(ABORT,'unexpected write'); END",
    ] {
        sqlx::query(sql).execute(&g.store.pool).await.unwrap();
    }
    let report = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(report.retired, 0);
    assert!(path.exists());
}
#[test]
fn identity_rejects_path_escape_mismatch_and_missing_expiry() {
    let id = uuid::Uuid::new_v4().to_string();
    let mut b = json!({"operation":id,"device":"device","owner":"owner","name":"name","size":0,"sha256":"hash","expires":1});
    assert!(identity("file_blob", "../escape", &b.to_string(), 2).is_err());
    b["operation"] = json!(uuid::Uuid::new_v4().to_string());
    assert!(identity("file_blob", &id, &b.to_string(), 2).is_err());
    b["operation"] = json!(id);
    b["expires"] = json!(0);
    assert!(
        identity("file_blob", &id, &b.to_string(), 2)
            .unwrap()
            .is_none()
    );
}
#[cfg(unix)]
#[tokio::test]
async fn symlinked_cache_directory_cannot_unlink_outside_state() {
    let (_dir, g) = fixture().await;
    let at = crate::now();
    let (_, path) = blob(&g, at - 1).await;
    let outside = tempfile::tempdir().unwrap();
    let other = outside.path().join(path.file_name().unwrap());
    std::fs::write(&other, "keep").unwrap();
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(g.config.state_dir.join("file-objects")).unwrap();
    std::os::unix::fs::symlink(outside.path(), g.config.state_dir.join("file-objects")).unwrap();
    let report = sweep(&g, &g.store, at).await.unwrap();
    assert_eq!(report.retired, 0);
    assert_eq!(report.item_errors, 1);
    assert_eq!(std::fs::read_to_string(other).unwrap(), "keep");
}

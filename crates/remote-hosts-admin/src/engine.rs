//! Serial, host-bound, replay-safe plan/apply state machine, independent of OS execution.
use crate::{filesystem::Store, protocol::*};
use anyhow::{Result, ensure};
use serde_json::{Value, json};

#[allow(async_fn_in_trait)]
pub trait Backend {
    async fn inspect(&self, policy: &Policy) -> Result<Snapshot>;
    async fn execute(
        &self,
        policy: &Policy,
        store: &Store,
        record: &mut Record,
    ) -> Result<Snapshot>;
}
pub struct Engine<B> {
    pub store: Store,
    pub backend: B,
}
pub fn unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn response(record: &Record) -> Result<Value> {
    let mut v = serde_json::to_value(record)?;
    if record.state == State::Running {
        v["recovery_required"] = json!(true);
        v["next_action"] =
            json!("Inspect actual targets; never replay an interrupted plan automatically.");
    }
    Ok(v)
}
impl<B: Backend> Engine<B> {
    pub async fn handle(
        &self,
        policy: &Policy,
        uid: u32,
        request: Request,
        now: u64,
    ) -> Result<Value> {
        policy.authorize(uid)?;
        request.validate()?;
        let revoked = self.store.revoked()?;
        match request {
            Request::Status => Ok(
                json!({"available":true,"protocol":PROTOCOL,"version":env!("CARGO_PKG_VERSION"),"device_id":policy.device_id,"enabled":policy.enabled && !revoked,"allowed_action":ACTION,"caller_uid":uid,"plan_ttl_seconds":PLAN_TTL,"transport":"local_unix_socket","arbitrary_commands":false}),
            ),
            Request::Revoke => {
                self.store.revoke(uid, now)?;
                Ok(json!({"enabled":false,"revoked":true,"system_services_changed":false}))
            }
            Request::Receipt { request_id } => {
                let r = self
                    .store
                    .load(&request_id)?
                    .ok_or_else(|| anyhow::anyhow!("receipt_not_found"))?;
                ensure!(
                    r.plan.caller_uid == uid && r.plan.device_id == policy.device_id,
                    "receipt_binding_mismatch"
                );
                response(&r)
            }
            Request::Plan { request_id } => {
                ensure!(policy.enabled && !revoked, "grant_disabled_or_revoked");
                let policy_sha256 = digest(policy)?;
                if let Some(existing) = self.store.load(&request_id)? {
                    ensure!(
                        existing.plan.caller_uid == uid
                            && existing.plan.device_id == policy.device_id
                            && existing.plan.policy_sha256 == policy_sha256,
                        "idempotency_binding_conflict"
                    );
                    return response(&existing);
                }
                self.store.capacity()?;
                let snapshot = self.backend.inspect(policy).await?;
                ensure!(
                    snapshot.current_service_active,
                    "protected_current_remoteplay_not_active"
                );
                let plan = Plan {
                    protocol: PROTOCOL,
                    action: ACTION.into(),
                    request_id,
                    device_id: policy.device_id.clone(),
                    caller_uid: uid,
                    policy_sha256,
                    created_at: now,
                    expires_at: now
                        .checked_add(PLAN_TTL)
                        .ok_or_else(|| anyhow::anyhow!("clock_overflow"))?,
                    snapshot,
                };
                let record = Record {
                    plan_sha256: digest(&plan)?,
                    plan,
                    state: State::Prepared,
                    steps: vec![],
                    error: None,
                    after: None,
                    updated_at: now,
                };
                self.store.save(&record)?;
                response(&record)
            }
            Request::Apply {
                request_id,
                plan_sha256,
            } => {
                ensure!(policy.enabled && !revoked, "grant_disabled_or_revoked");
                let mut record = self
                    .store
                    .load(&request_id)?
                    .ok_or_else(|| anyhow::anyhow!("plan_not_found"))?;
                ensure!(
                    record.plan.caller_uid == uid
                        && record.plan.device_id == policy.device_id
                        && record.plan.policy_sha256 == digest(policy)?,
                    "plan_binding_mismatch"
                );
                ensure!(
                    record.plan_sha256 == plan_sha256 && digest(&record.plan)? == plan_sha256,
                    "plan_digest_mismatch"
                );
                if record.state != State::Prepared {
                    return response(&record);
                }
                ensure!(
                    now >= record.plan.created_at && now < record.plan.expires_at,
                    "plan_expired_or_clock_moved_backwards"
                );
                let fresh = self.backend.inspect(policy).await?;
                ensure!(
                    fresh.current_service_active,
                    "protected_current_remoteplay_not_active"
                );
                ensure!(
                    fresh == record.plan.snapshot,
                    "targets_changed_create_new_plan"
                );
                // Persist uncertainty BEFORE any mutation; a crash can never leave a replayable plan.
                record.state = State::Running;
                record.updated_at = now;
                self.store.save(&record)?;
                match self.backend.execute(policy, &self.store, &mut record).await {
                    Ok(after) => {
                        record.after = Some(after);
                        record.state = State::Succeeded;
                    }
                    Err(error) => {
                        record.error = Some(format!("{error:#}"));
                        record.state = State::Failed;
                    }
                }
                record.updated_at = unix_time();
                self.store.save(&record)?;
                response(&record)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        os::unix::fs::PermissionsExt,
    };
    struct Mock {
        calls: Cell<usize>,
        before: RefCell<Snapshot>,
        fail: Cell<bool>,
    }
    impl Backend for Mock {
        async fn inspect(&self, _: &Policy) -> Result<Snapshot> {
            Ok(self.before.borrow().clone())
        }
        async fn execute(&self, _: &Policy, _: &Store, _: &mut Record) -> Result<Snapshot> {
            self.calls.set(self.calls.get() + 1);
            ensure!(!self.fail.get(), "simulated_failure");
            Ok(self.before.borrow().clone())
        }
    }
    fn policy() -> Policy {
        Policy {
            protocol: PROTOCOL,
            device_id: uuid::Uuid::new_v4().to_string(),
            grant_id: uuid::Uuid::new_v4().to_string(),
            allowed_uid: 501,
            allowed_gid: 20,
            home: "/Users/test".into(),
            platform: "macos".into(),
            enabled: true,
        }
    }
    fn fixture() -> (tempfile::TempDir, Engine<Mock>, Policy) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let engine = Engine {
            store: Store {
                dir: dir.path().to_owned(),
                owner: nix::unistd::geteuid().as_raw(),
            },
            backend: Mock {
                calls: Cell::new(0),
                before: RefCell::new(Snapshot {
                    current_service_active: true,
                    ..Default::default()
                }),
                fail: Cell::new(false),
            },
        };
        (dir, engine, policy())
    }
    async fn plan(e: &Engine<Mock>, p: &Policy, id: &str) -> Value {
        e.handle(
            p,
            501,
            Request::Plan {
                request_id: id.into(),
            },
            1000,
        )
        .await
        .unwrap()
    }
    fn apply(id: &str, v: &Value) -> Request {
        Request::Apply {
            request_id: id.into(),
            plan_sha256: v["plan_sha256"].as_str().unwrap().into(),
        }
    }
    #[tokio::test]
    async fn duplicate_apply_executes_once() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        for _ in 0..2 {
            assert_eq!(
                e.handle(&p, 501, apply(&id, &v), 1001).await.unwrap()["state"],
                "succeeded"
            );
        }
        assert_eq!(e.backend.calls.get(), 1);
    }
    #[tokio::test]
    async fn duplicate_plan_returns_same_digest() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let a = plan(&e, &p, &id).await;
        let b = e
            .handle(&p, 501, Request::Plan { request_id: id }, 1100)
            .await
            .unwrap();
        assert_eq!(a, b);
    }
    #[tokio::test]
    async fn expired_plan_rejected() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        assert!(e.handle(&p, 501, apply(&id, &v), 1300).await.is_err());
        assert_eq!(e.backend.calls.get(), 0);
    }
    #[tokio::test]
    async fn backwards_clock_rejected() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        assert!(e.handle(&p, 501, apply(&id, &v), 999).await.is_err());
    }
    #[tokio::test]
    async fn wrong_uid_rejected() {
        let (_d, e, p) = fixture();
        assert!(e.handle(&p, 502, Request::Status, 1000).await.is_err());
    }
    #[tokio::test]
    async fn forged_digest_rejected() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let _ = plan(&e, &p, &id).await;
        assert!(
            e.handle(
                &p,
                501,
                Request::Apply {
                    request_id: id,
                    plan_sha256: "0".repeat(64)
                },
                1001
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn changed_target_rejected() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        e.backend.before.borrow_mut().files.push(None);
        assert!(e.handle(&p, 501, apply(&id, &v), 1001).await.is_err());
        assert_eq!(e.backend.calls.get(), 0);
    }
    #[tokio::test]
    async fn protected_service_required() {
        let (_d, e, p) = fixture();
        e.backend.before.borrow_mut().current_service_active = false;
        assert!(
            e.handle(
                &p,
                501,
                Request::Plan {
                    request_id: uuid::Uuid::new_v4().to_string()
                },
                1000
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn changed_policy_invalidates_plan() {
        let (_d, e, mut p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        p.grant_id = uuid::Uuid::new_v4().to_string();
        assert!(e.handle(&p, 501, apply(&id, &v), 1001).await.is_err());
    }
    #[tokio::test]
    async fn revoke_blocks_apply_but_keeps_receipt() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        e.handle(&p, 501, Request::Revoke, 1001).await.unwrap();
        assert!(e.handle(&p, 501, apply(&id, &v), 1002).await.is_err());
        assert!(
            e.handle(&p, 501, Request::Receipt { request_id: id }, 1002)
                .await
                .is_ok()
        );
        assert_eq!(e.backend.calls.get(), 0);
    }
    #[tokio::test]
    async fn failure_is_durable_not_replayed() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        e.backend.fail.set(true);
        for _ in 0..2 {
            assert_eq!(
                e.handle(&p, 501, apply(&id, &v), 1001).await.unwrap()["state"],
                "failed"
            );
        }
        assert_eq!(e.backend.calls.get(), 1);
    }
    #[tokio::test]
    async fn interrupted_record_is_never_replayed() {
        let (_d, e, p) = fixture();
        let id = uuid::Uuid::new_v4().to_string();
        let v = plan(&e, &p, &id).await;
        let mut r = e.store.load(&id).unwrap().unwrap();
        r.state = State::Running;
        e.store.save(&r).unwrap();
        let result = e.handle(&p, 501, apply(&id, &v), 1001).await.unwrap();
        assert_eq!(result["recovery_required"], true);
        assert_eq!(e.backend.calls.get(), 0);
    }
    #[tokio::test]
    async fn disabled_grant_rejects_plan() {
        let (_d, e, mut p) = fixture();
        p.enabled = false;
        assert!(
            e.handle(
                &p,
                501,
                Request::Plan {
                    request_id: uuid::Uuid::new_v4().to_string()
                },
                1000
            )
            .await
            .is_err()
        );
    }
    #[test]
    fn request_does_not_accept_arbitrary_command() {
        assert!(serde_json::from_value::<Request>(json!({"op":"apply","request_id":uuid::Uuid::new_v4(),"plan_sha256":"0".repeat(64),"command":"sh -c anything"})).is_err());
    }
    #[test]
    fn unknown_operation_rejected() {
        assert!(serde_json::from_value::<Request>(json!({"op":"exec","command":"id"})).is_err());
    }
    #[test]
    fn traversal_request_id_rejected() {
        assert!(valid_id("../../etc/sudoers").is_err());
    }
    #[test]
    fn root_and_unsafe_home_grants_rejected() {
        let mut p = policy();
        p.allowed_uid = 0;
        assert!(p.validate().is_err());
        p.allowed_uid = 501;
        p.home = "/Users/test/../root".into();
        assert!(p.validate().is_err());
    }
    #[test]
    fn whitelist_excludes_current_and_uu() {
        let p = policy();
        assert!(
            p.targets()
                .iter()
                .all(|s| s != &p.current_mac_binary() && !s.contains("UURemote"))
        );
        let mut linux = p;
        linux.platform = "linux".into();
        assert!(linux.targets().iter().all(|s| !s.contains(CURRENT_UNIT)));
    }
}

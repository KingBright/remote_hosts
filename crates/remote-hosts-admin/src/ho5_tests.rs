use super::*;
use crate::ho5_bus::{Query, ReadBus, ReadOnlyBus, fixed_argv};
use serde_json::{Value, json};
use std::{collections::VecDeque, os::unix::fs::PermissionsExt};

fn hash(c: char) -> String {
    c.to_string().repeat(64)
}
fn tx() -> Transaction {
    Transaction {
        owner: ":1.42".into(),
        identity_sha256: hash('c'),
    }
}
fn observation() -> Observation {
    Observation {
        boot_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
        booted: Deployment {
            checksum: hash('a'),
            packages: vec![],
        },
        staged: None,
        transaction: None,
        completion: None,
        packages_authorized: true,
        reboot_authorized: true,
        reboot_inhibited: false,
        service_uid: 0,
        no_new_privs: true,
        verified_booted_packages: vec![],
    }
}
fn staged(o: &mut Observation) {
    o.staged = Some(Deployment {
        checksum: hash('b'),
        packages: PACKAGES.iter().map(|p| (*p).into()).collect(),
    });
}
fn boot_target(o: &mut Observation) {
    o.boot_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".into();
    o.booted = o.staged.take().unwrap();
    o.verified_booted_packages = PACKAGES.iter().map(|p| (*p).into()).collect();
    o.completion = None;
}
#[derive(Clone)]
enum Mode {
    Accepted,
    Reboot,
    Unknown,
    Pending,
    Denied,
    CrashAfterAcceptance,
}
struct Fake {
    o: Observation,
    mode: Mode,
    calls: Vec<FixedCall>,
    path: PathBuf,
    inspect_failed: bool,
    inspect_pending: bool,
}
impl Backend for Fake {
    async fn inspect(&mut self) -> Result<Observation> {
        ensure!(!self.inspect_failed, "fake_transport_offline");
        if self.inspect_pending {
            std::future::pending::<()>().await;
        }
        Ok(self.o.clone())
    }
    fn mutation_enabled(&self) -> bool {
        true
    }
    async fn dispatch(&mut self, call: FixedCall) -> Dispatch {
        let data: Record =
            serde_json::from_slice(&std::fs::read(self.path.join("current.json")).unwrap())
                .unwrap();
        assert_eq!(data.dispatch_count, 1);
        assert!(matches!(
            data.state,
            State::DispatchIntent | State::RebootIntent
        ));
        self.calls.push(call);
        match self.mode {
            Mode::Accepted => Dispatch::Accepted(tx()),
            Mode::Reboot => Dispatch::RebootAccepted,
            Mode::Unknown => Dispatch::Unknown,
            Mode::Pending => std::future::pending::<Dispatch>().await,
            Mode::Denied => Dispatch::NotStarted("os_authorization_rejected"),
            Mode::CrashAfterAcceptance => {
                std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o500))
                    .unwrap();
                Dispatch::Accepted(tx())
            }
        }
    }
}
fn fixture(action: Action) -> (tempfile::TempDir, Coordinator, Fake, Intent) {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().canonicalize().unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let c = Coordinator {
        state_dir: path.clone(),
        caller_uid: nix::unistd::geteuid().as_raw(),
        timeout: Duration::from_millis(30),
    };
    let mut o = observation();
    if action == Action::RebootStaged {
        staged(&mut o);
    }
    let f = Fake {
        o,
        mode: if action == Action::SystemDeps {
            Mode::Accepted
        } else {
            Mode::Reboot
        },
        calls: vec![],
        path,
        inspect_failed: false,
        inspect_pending: false,
    };
    let i = Intent {
        task_id: uuid::Uuid::new_v4().to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        action,
        target_checksum: (action == Action::RebootStaged).then(|| hash('b')),
    };
    (d, c, f, i)
}
// The fake checks that the actual request journal was fsynced before dispatch.
fn point_fake(f: &Fake, id: &str) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(format!("{id}.json"), f.path.join("current.json")).unwrap();
}
async fn prepared(c: &Coordinator, f: &mut Fake, i: &Intent) -> Record {
    let r = c.prepare(f, i.clone(), 1000).await.unwrap();
    point_fake(f, &i.request_id);
    r
}
#[tokio::test]
async fn duplicate_apply_dispatches_once_and_preserves_fixed_parameters() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    for _ in 0..3 {
        assert_eq!(
            c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
                .await
                .unwrap()
                .state,
            State::Running
        );
    }
    assert_eq!(
        f.calls,
        vec![FixedCall::InstallProfile {
            packages: PACKAGES,
            remove: [],
            no_pull_base: true
        }]
    );
}
#[tokio::test]
async fn repeated_prepare_is_identical_without_fresh_probe() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = c.prepare(&mut f, i.clone(), 1000).await.unwrap();
    f.inspect_failed = true;
    let next = c.prepare(&mut f, i, 1005).await.unwrap();
    assert_eq!(
        serde_json::to_value(p).unwrap(),
        serde_json::to_value(next).unwrap()
    );
}
#[tokio::test]
async fn same_request_changed_task_conflicts() {
    let (_d, c, mut f, mut i) = fixture(Action::SystemDeps);
    c.prepare(&mut f, i.clone(), 1000).await.unwrap();
    i.task_id = uuid::Uuid::new_v4().to_string();
    assert!(c.prepare(&mut f, i, 1001).await.is_err());
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn new_request_cannot_bypass_unknown_semantic_intent() {
    let (_d, c, mut f, mut i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Unknown;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    i.request_id = uuid::Uuid::new_v4().to_string();
    assert!(
        c.prepare(&mut f, i, 1002)
            .await
            .unwrap_err()
            .to_string()
            .contains("existing_intent")
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn dispatch_timeout_stays_unknown_and_never_replays() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Pending;
    let r = c
        .advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    assert_eq!(r.state, State::OutcomeUnknown);
    assert_eq!(r.next_action(), "observe_original_do_not_replay");
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1002)
        .await
        .unwrap();
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn dispatch_error_unknown_stays_unknown_even_with_matching_external_stage() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Unknown;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    staged(&mut f.o);
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::OutcomeUnknown
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn existing_transaction_is_not_cancelled_or_adopted() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    f.o.transaction = Some(tx());
    let p = prepared(&c, &mut f, &i).await;
    assert_eq!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap()
            .state,
        State::ForeignTransactionBusy
    );
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn externally_staged_packages_are_not_claimed_or_installed() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    staged(&mut f.o);
    let p = prepared(&c, &mut f, &i).await;
    assert_eq!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap()
            .state,
        State::ExternalEffectObserved
    );
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn permission_expiry_stops_before_dispatch() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.o.packages_authorized = false;
    let r = c
        .advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    assert_eq!(r.state, State::AwaitingPlatformAuthorization);
    assert_eq!(r.dispatch_count, 0);
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn untrusted_service_or_changed_nnp_cannot_dispatch() {
    for service in [true, false] {
        let (_d, c, mut f, i) = fixture(Action::SystemDeps);
        let p = prepared(&c, &mut f, &i).await;
        if service {
            f.o.service_uid = 65534;
        } else {
            f.o.no_new_privs = false;
        }
        assert_eq!(
            c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
                .await
                .unwrap()
                .state,
            State::AwaitingPlatformAuthorization
        );
        assert!(f.calls.is_empty());
    }
}
#[tokio::test]
async fn rejected_call_is_known_not_started_and_not_retried() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Denied;
    assert_eq!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap()
            .state,
        State::Failed
    );
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1002)
        .await
        .unwrap();
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn crash_between_os_acceptance_and_receipt_cannot_redispatch() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::CrashAfterAcceptance;
    assert!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .is_err()
    );
    std::fs::set_permissions(&f.path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let r = c.receipt(&i.request_id).unwrap().unwrap();
    assert_eq!(r.state, State::DispatchIntent);
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1002)
        .await
        .unwrap();
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn own_completion_stages_then_boot_and_actual_packages_finish() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    staged(&mut f.o);
    f.o.completion = Some(Completion {
        transaction: tx(),
        success: true,
        target_checksum: hash('b'),
    });
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::StagedPendingReboot
    );
    boot_target(&mut f.o);
    f.o.packages_authorized = false;
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1003)
            .await
            .unwrap()
            .state,
        State::Succeeded
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn wrong_transaction_completion_cannot_claim_success() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    staged(&mut f.o);
    let mut foreign = tx();
    foreign.owner = ":1.99".into();
    f.o.completion = Some(Completion {
        transaction: foreign,
        success: true,
        target_checksum: hash('b'),
    });
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::OutcomeUnknown
    );
}
#[tokio::test]
async fn known_running_transaction_is_observed_without_redispatch() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    f.o.transaction = Some(tx());
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::Running
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn reboot_is_normal_once_and_same_boot_waits() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let p = prepared(&c, &mut f, &i).await;
    assert_eq!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap()
            .state,
        State::AwaitingReconnect
    );
    c.reconcile(&mut f, &i.request_id, 1002).await.unwrap();
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1003)
        .await
        .unwrap();
    assert_eq!(
        f.calls,
        vec![FixedCall::RebootNormal {
            target_checksum: hash('b'),
            interactive: false,
            ignore_inhibitors: false
        }]
    );
}
#[tokio::test]
async fn reboot_response_timeout_does_not_repeat_after_disconnect() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Pending;
    assert_eq!(
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap()
            .state,
        State::OutcomeUnknown
    );
    f.inspect_failed = true;
    let offline = c.reconcile(&mut f, &i.request_id, 1002).await.unwrap();
    assert_eq!(offline.state, State::OutcomeUnknown);
    assert_eq!(
        offline.error_code.as_deref(),
        Some("observation_unavailable")
    );
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1003)
        .await
        .unwrap();
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn new_boot_target_and_packages_verify_without_reboot_permission() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    boot_target(&mut f.o);
    f.o.reboot_authorized = false;
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::Succeeded
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn wrong_boot_target_or_missing_actual_package_fails_without_retry() {
    for wrong_target in [true, false] {
        let (_d, c, mut f, i) = fixture(Action::RebootStaged);
        let p = prepared(&c, &mut f, &i).await;
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap();
        boot_target(&mut f.o);
        if wrong_target {
            f.o.booted.checksum = hash('d');
        } else {
            f.o.verified_booted_packages.pop();
        }
        assert_eq!(
            c.reconcile(&mut f, &i.request_id, 1002)
                .await
                .unwrap()
                .state,
            State::Failed
        );
        c.advance(&mut f, &i.request_id, &p.plan_sha256, 1003)
            .await
            .unwrap();
        assert_eq!(f.calls.len(), 1);
    }
}
#[tokio::test]
async fn inhibitors_and_changed_staged_target_block_reboot() {
    for inhibited in [true, false] {
        let (_d, c, mut f, i) = fixture(Action::RebootStaged);
        let p = prepared(&c, &mut f, &i).await;
        if inhibited {
            f.o.reboot_inhibited = true;
        } else {
            f.o.staged.as_mut().unwrap().checksum = hash('d');
        }
        let r = c
            .advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
            .await
            .unwrap();
        assert!(matches!(
            r.state,
            State::AwaitingPlatformAuthorization | State::Failed
        ));
        assert!(f.calls.is_empty());
    }
}
#[tokio::test]
async fn expiry_digest_and_baseline_prevent_dispatch() {
    for mode in 0..3 {
        let (_d, c, mut f, i) = fixture(Action::SystemDeps);
        let p = prepared(&c, &mut f, &i).await;
        if mode == 0 {
            assert!(
                c.advance(&mut f, &i.request_id, &p.plan_sha256, 1300)
                    .await
                    .is_err()
            );
        }
        if mode == 1 {
            assert!(
                c.advance(&mut f, &i.request_id, &hash('f'), 1001)
                    .await
                    .is_err()
            );
        }
        if mode == 2 {
            f.o.booted.checksum = hash('d');
            assert_eq!(
                c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
                    .await
                    .unwrap()
                    .state,
                State::Failed
            );
        }
        assert!(f.calls.is_empty());
    }
}
#[tokio::test]
async fn query_timeout_does_not_create_dispatch_or_receipt() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    f.inspect_pending = true;
    assert!(c.prepare(&mut f, i.clone(), 1000).await.is_err());
    assert!(c.receipt(&i.request_id).unwrap().is_none());
    assert!(f.calls.is_empty());
}
#[test]
fn journal_lock_and_symlink_rejected() {
    let (_d, c, mut _f, i) = fixture(Action::SystemDeps);
    let (_s, _lock) = UserStore::locked(&c.state_dir, c.caller_uid).unwrap();
    assert!(UserStore::locked(&c.state_dir, c.caller_uid).is_err());
    let dir = tempfile::tempdir().unwrap();
    let link = dir.path().canonicalize().unwrap().join("link");
    std::os::unix::fs::symlink(&c.state_dir, &link).unwrap();
    let other = Coordinator {
        state_dir: link,
        ..c
    };
    assert!(other.receipt(&i.request_id).is_err());
}
#[test]
fn missing_receipt_creates_no_journal_or_lock() {
    let (_d, mut c, _f, i) = fixture(Action::SystemDeps);
    c.state_dir = c.state_dir.join("absent");
    assert!(c.receipt(&i.request_id).unwrap().is_none());
    assert!(!c.state_dir.exists());
}
#[test]
fn closed_requests_reject_shell_dns_and_noncanonical_uuid() {
    let (_d, _c, _f, i) = fixture(Action::SystemDeps);
    let mut v = serde_json::to_value(&i).unwrap();
    for key in ["command", "argv", "env", "packages", "path", "dns_rule"] {
        v[key] = json!("unsafe");
        assert!(serde_json::from_value::<Intent>(v.clone()).is_err());
        v.as_object_mut().unwrap().remove(key);
    }
    v["action"] = json!("dns");
    assert!(serde_json::from_value::<Intent>(v).is_err());
    let mut other = i;
    other.request_id = "../escape".into();
    assert!(other.validate().is_err());
}
#[tokio::test]
async fn corrupted_plan_digest_is_rejected() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let mut p = prepared(&c, &mut f, &i).await;
    p.plan.device_id = uuid::Uuid::new_v4().to_string();
    let (s, _lock) = UserStore::locked(&c.state_dir, c.caller_uid).unwrap();
    s.save_json(&i.request_id, &p).unwrap();
    drop(_lock);
    assert!(c.receipt(&i.request_id).is_err());
    assert!(f.calls.is_empty());
}
struct FakeBus {
    replies: VecDeque<(Query, Value)>,
}
impl ReadBus for FakeBus {
    async fn query(&mut self, q: Query) -> Result<Value> {
        let (expected, value) = self
            .replies
            .pop_front()
            .context("unexpected_fake_dbus_query")?;
        ensure!(q == expected, "wrong_fake_dbus_query_order");
        Ok(value)
    }
}
fn bus_fixture() -> ReadOnlyBus<FakeBus> {
    ReadOnlyBus(FakeBus {
        replies: VecDeque::from([
            (Query::BootId, json!("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")),
            (Query::Security, json!(true)),
            (
                Query::OstreeStatus,
                json!({"transaction":null,"deployments":[{"booted":true,"staged":false,"checksum":hash('a'),"packages":[]}]}),
            ),
            (Query::OwnerUid, json!({"type":"u","data":[0]})),
            (Query::CanReboot, json!({"type":"s","data":["yes"]})),
            (Query::PackagesAuthorization, json!(true)),
            (Query::RebootAuthorization, json!(true)),
            (Query::VerifiedPackages, json!([])),
        ]),
    })
}
#[tokio::test]
async fn fake_dbus_roundtrip_and_real_backend_mutation_gate() {
    let (_d, c, _f, i) = fixture(Action::SystemDeps);
    let mut bus = bus_fixture();
    let p = c.prepare(&mut bus, i.clone(), 1000).await.unwrap();
    assert!(bus.0.replies.is_empty());
    bus = bus_fixture();
    let r = c
        .advance(&mut bus, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    assert_eq!(r.state, State::Prepared);
    assert_eq!(r.dispatch_count, 0);
    assert_eq!(r.error_code.as_deref(), Some("execution_disabled"));
    assert!(!bus.mutation_enabled());
}
#[tokio::test]
async fn malformed_dbus_or_ambiguous_deployments_fail_closed() {
    for corrupt_owner in [true, false] {
        let mut bus = bus_fixture();
        if corrupt_owner {
            bus.0.replies[3].1 = json!({"data":["0"]});
        } else {
            let mut ds = bus.0.replies[2].1["deployments"]
                .as_array()
                .unwrap()
                .clone();
            ds.push(ds[0].clone());
            bus.0.replies[2].1["deployments"] = json!(ds);
        }
        assert!(bus.inspect().await.is_err());
    }
}
#[test]
fn read_query_argv_has_no_mutation_or_interactive_auth() {
    for q in [
        Query::OstreeStatus,
        Query::OwnerUid,
        Query::CanReboot,
        Query::PackagesAuthorization,
        Query::RebootAuthorization,
    ] {
        let (program, args) = fixed_argv(q, "123,456,1000").unwrap();
        assert!(program.starts_with("/usr/bin/"));
        assert!(!args.iter().any(|a| {
            [
                "install",
                "reboot",
                "restart",
                "enable",
                "--allow-user-interaction",
            ]
            .contains(&a.as_str())
        }));
    }
    assert!(fixed_argv(Query::PackagesAuthorization, "123;sh,456,1000").is_err());
}

fn restarted(c: &Coordinator) -> Coordinator {
    Coordinator {
        state_dir: c.state_dir.clone(),
        caller_uid: c.caller_uid,
        timeout: c.timeout,
    }
}
#[tokio::test]
async fn cold_start_same_boot_then_new_boot_never_reboots_again() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    drop(c);
    let cold = coordinator(&f.path);
    let mut fresh = Fake {
        o: f.o.clone(),
        mode: Mode::Reboot,
        calls: vec![],
        path: f.path.clone(),
        inspect_failed: true,
        inspect_pending: false,
    };
    let offline = cold
        .reconcile(&mut fresh, &i.request_id, 1002)
        .await
        .unwrap();
    assert_eq!(offline.state, State::AwaitingReconnect);
    assert_eq!(offline.dispatch_count, 1);
    fresh.inspect_failed = false;
    assert_eq!(
        cold.reconcile(&mut fresh, &i.request_id, 1003)
            .await
            .unwrap()
            .state,
        State::AwaitingReconnect
    );
    boot_target(&mut fresh.o);
    fresh.o.reboot_authorized = false;
    assert_eq!(
        cold.reconcile(&mut fresh, &i.request_id, 2000)
            .await
            .unwrap()
            .state,
        State::Succeeded
    );
    cold.advance(&mut fresh, &i.request_id, &p.plan_sha256, 2001)
        .await
        .unwrap();
    assert!(fresh.calls.is_empty());
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn cold_start_staged_provenance_survives_stale_finished_signal() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    staged(&mut f.o);
    let completion = Completion {
        transaction: tx(),
        success: true,
        target_checksum: hash('b'),
    };
    f.o.completion = Some(completion.clone());
    assert_eq!(
        c.reconcile(&mut f, &i.request_id, 1002)
            .await
            .unwrap()
            .state,
        State::StagedPendingReboot
    );
    let cold = restarted(&c);
    boot_target(&mut f.o);
    f.o.completion = Some(completion);
    f.o.packages_authorized = false;
    assert_eq!(
        cold.reconcile(&mut f, &i.request_id, 2000)
            .await
            .unwrap()
            .state,
        State::Succeeded
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn cold_start_timeout_does_not_adopt_matching_external_package_stage() {
    let (_d, c, mut f, mut i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    f.mode = Mode::Pending;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    let cold = restarted(&c);
    staged(&mut f.o);
    f.o.completion = Some(Completion {
        transaction: tx(),
        success: true,
        target_checksum: hash('b'),
    });
    let r = cold.reconcile(&mut f, &i.request_id, 2000).await.unwrap();
    assert_eq!(r.state, State::OutcomeUnknown);
    assert_eq!(r.dispatch_count, 1);
    i.request_id = uuid::Uuid::new_v4().to_string();
    i.task_id = uuid::Uuid::new_v4().to_string();
    assert!(cold.prepare(&mut f, i, 2001).await.is_err());
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn cold_start_reboot_dispatch_intent_only_observes_original() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let mut p = prepared(&c, &mut f, &i).await;
    p.state = State::RebootIntent;
    p.dispatch_count = 1;
    let (s, lock) = UserStore::locked(&c.state_dir, c.caller_uid).unwrap();
    s.save_json(&i.request_id, &p).unwrap();
    drop(lock);
    let cold = restarted(&c);
    assert_eq!(
        cold.advance(&mut f, &i.request_id, &p.plan_sha256, 2000)
            .await
            .unwrap()
            .state,
        State::RebootIntent
    );
    assert_eq!(
        cold.reconcile(&mut f, &i.request_id, 2001)
            .await
            .unwrap()
            .state,
        State::AwaitingReconnect
    );
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn reconcile_probe_timeout_preserves_durable_original_state() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    f.inspect_pending = true;
    let cold = restarted(&c);
    let r = cold.reconcile(&mut f, &i.request_id, 2000).await.unwrap();
    assert_eq!(r.state, State::Running);
    assert_eq!(r.transaction, Some(tx()));
    assert_eq!(r.dispatch_count, 1);
    assert_eq!(r.error_code.as_deref(), Some("observation_timeout"));
    assert_eq!(
        cold.receipt(&i.request_id).unwrap().unwrap().error_code,
        r.error_code
    );
    assert_eq!(f.calls.len(), 1);
}
#[tokio::test]
async fn completed_receipt_is_available_after_cold_start_without_os_connection() {
    let (_d, c, mut f, i) = fixture(Action::RebootStaged);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    boot_target(&mut f.o);
    let done = c.reconcile(&mut f, &i.request_id, 2000).await.unwrap();
    f.inspect_failed = true;
    let next = restarted(&c)
        .reconcile(&mut f, &i.request_id, 2001)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(done).unwrap(),
        serde_json::to_value(next).unwrap()
    );
}
#[tokio::test]
async fn copied_receipt_cannot_move_to_another_private_store() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    c.prepare(&mut f, i.clone(), 1000).await.unwrap();
    let d = tempfile::tempdir().unwrap();
    let path = d.path().canonicalize().unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::copy(
        c.state_dir.join(format!("{}.json", i.request_id)),
        path.join(format!("{}.json", i.request_id)),
    )
    .unwrap();
    let other = Coordinator {
        state_dir: path,
        ..restarted(&c)
    };
    assert!(other.receipt(&i.request_id).is_err());
}
#[tokio::test]
async fn uid_mapping_identity_mismatch_fails_even_with_recomputed_plan_digest() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let mut p = c.prepare(&mut f, i.clone(), 1000).await.unwrap();
    p.plan.caller_identity_sha256 = hash('f');
    p.plan_sha256 = digest(&p.plan).unwrap();
    let (s, lock) = UserStore::locked(&c.state_dir, c.caller_uid).unwrap();
    s.save_json(&i.request_id, &p).unwrap();
    drop(lock);
    assert!(restarted(&c).receipt(&i.request_id).is_err());
    assert!(f.calls.is_empty());
}
#[tokio::test]
async fn same_boot_changed_staged_target_is_uncertain_and_never_reapplied() {
    let (_d, c, mut f, i) = fixture(Action::SystemDeps);
    let p = prepared(&c, &mut f, &i).await;
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1001)
        .await
        .unwrap();
    staged(&mut f.o);
    f.o.completion = Some(Completion {
        transaction: tx(),
        success: true,
        target_checksum: hash('b'),
    });
    c.reconcile(&mut f, &i.request_id, 1002).await.unwrap();
    f.o.completion = None;
    f.o.staged.as_mut().unwrap().checksum = hash('d');
    assert_eq!(
        restarted(&c)
            .reconcile(&mut f, &i.request_id, 1003)
            .await
            .unwrap()
            .state,
        State::OutcomeUnknown
    );
    c.advance(&mut f, &i.request_id, &p.plan_sha256, 1004)
        .await
        .unwrap();
    assert_eq!(f.calls.len(), 1);
}

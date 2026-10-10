//! Ordinary-user HO5 plans and reconciliation. The shipped backend cannot mutate the OS.
use crate::{
    ho5_store::{CallerIdentity, StoreIdentity, UserStore},
    protocol::{PLAN_TTL, digest, valid_id},
};
use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub const DEVICE: &str = "02f29fa0-48c1-4e31-a345-90aa88467323";
pub const PROFILE: &str = "ho5-system-deps-v1";
pub const PACKAGES: [&str; 8] = [
    "alsa-lib-devel",
    "clang",
    "clang-libs",
    "cmake",
    "fontconfig-devel",
    "freetype-devel",
    "libxkbcommon-x11-devel",
    "pipewire-devel",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    SystemDeps,
    RebootStaged,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub task_id: String,
    pub request_id: String,
    pub action: Action,
    pub target_checksum: Option<String>,
}
fn checksum(s: &str) -> Result<()> {
    ensure!(
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid_deployment_checksum"
    );
    Ok(())
}
impl Intent {
    fn validate(&self) -> Result<()> {
        valid_id(&self.task_id)?;
        valid_id(&self.request_id)?;
        match self.action {
            Action::SystemDeps => ensure!(
                self.target_checksum.is_none(),
                "package_target_is_os_generated"
            ),
            Action::RebootStaged => checksum(
                self.target_checksum
                    .as_deref()
                    .context("reboot_target_required")?,
            )?,
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub checksum: String,
    pub packages: Vec<String>,
}
impl Deployment {
    fn validate(&self) -> Result<()> {
        checksum(&self.checksum)?;
        ensure!(
            self.packages.len() <= 512
                && self.packages.iter().all(|s| !s.is_empty()
                    && s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))),
            "invalid_package_metadata"
        );
        Ok(())
    }
    fn has_profile(&self) -> bool {
        PACKAGES
            .iter()
            .all(|p| self.packages.iter().any(|s| s == p))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    pub owner: String,
    pub identity_sha256: String,
}
impl Transaction {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.owner.starts_with(':')
                && self.owner.len() < 80
                && self.owner[1..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b == b'.'),
            "invalid_transaction_owner"
        );
        checksum(&self.identity_sha256)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub transaction: Transaction,
    pub success: bool,
    pub target_checksum: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub boot_id: String,
    pub booted: Deployment,
    pub staged: Option<Deployment>,
    pub transaction: Option<Transaction>,
    pub completion: Option<Completion>,
    pub packages_authorized: bool,
    pub reboot_authorized: bool,
    pub reboot_inhibited: bool,
    pub service_uid: u32,
    pub no_new_privs: bool,
    pub verified_booted_packages: Vec<String>,
}
impl Observation {
    pub fn validate(&self) -> Result<()> {
        valid_id(&self.boot_id)?;
        self.booted.validate()?;
        if let Some(d) = &self.staged {
            d.validate()?;
        }
        if let Some(t) = &self.transaction {
            t.validate()?;
        }
        if let Some(c) = &self.completion {
            c.transaction.validate()?;
            checksum(&c.target_checksum)?;
        }
        ensure!(
            self.verified_booted_packages
                .iter()
                .all(|p| PACKAGES.contains(&p.as_str())),
            "invalid_verified_package"
        );
        Ok(())
    }
    fn allowed(&self, a: Action) -> bool {
        self.service_uid == 0
            && self.no_new_privs
            && match a {
                Action::SystemDeps => self.packages_authorized,
                Action::RebootStaged => self.reboot_authorized && !self.reboot_inhibited,
            }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FixedCall {
    InstallProfile {
        packages: [&'static str; 8],
        remove: [String; 0],
        no_pull_base: bool,
    },
    RebootNormal {
        target_checksum: String,
        interactive: bool,
        ignore_inhibitors: bool,
    },
}
#[derive(Clone, Debug)]
pub enum Dispatch {
    Accepted(Transaction),
    RebootAccepted,
    NotStarted(&'static str),
    Unknown,
}
/// Only a fake backend implements dispatch in this candidate. Production queries are read-only.
#[allow(async_fn_in_trait)]
pub trait Backend {
    async fn inspect(&mut self) -> Result<Observation>;
    fn mutation_enabled(&self) -> bool {
        false
    }
    async fn dispatch(&mut self, _call: FixedCall) -> Dispatch {
        Dispatch::NotStarted("execution_disabled")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Prepared,
    AwaitingPlatformAuthorization,
    ForeignTransactionBusy,
    ExternalEffectObserved,
    DispatchIntent,
    Running,
    StagedPendingReboot,
    RebootIntent,
    AwaitingReconnect,
    Succeeded,
    Failed,
    OutcomeUnknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub protocol: u32,
    pub intent: Intent,
    pub device_id: String,
    pub caller_uid: u32,
    pub caller_identity_sha256: String,
    pub store_identity: StoreIdentity,
    pub profile_sha256: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub before: Observation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub plan: Plan,
    pub plan_sha256: String,
    pub state: State,
    pub dispatch_count: u8,
    pub transaction: Option<Transaction>,
    pub target_checksum: Option<String>,
    pub after: Option<Observation>,
    pub error_code: Option<String>,
    pub updated_at: u64,
}
impl Record {
    pub fn next_action(&self) -> &'static str {
        match self.state {
            State::DispatchIntent
            | State::RebootIntent
            | State::Running
            | State::OutcomeUnknown => "observe_original_do_not_replay",
            State::AwaitingReconnect => "wait_for_fresh_boot_observation_do_not_reboot_again",
            State::AwaitingPlatformAuthorization => "owner_platform_authorization_required",
            State::ForeignTransactionBusy => "observe_external_transaction_do_not_cancel",
            State::ExternalEffectObserved => "external_provenance_no_automatic_adoption",
            State::StagedPendingReboot => "verify_target_then_separate_explicit_reboot_plan",
            State::Prepared => "candidate_execution_disabled",
            State::Succeeded | State::Failed => "read_original_receipt",
        }
    }
}
pub struct Coordinator {
    pub state_dir: PathBuf,
    pub caller_uid: u32,
    pub timeout: Duration,
}
impl Coordinator {
    fn validate(&self, store: &UserStore, r: &Record, id: &str) -> Result<()> {
        ensure!(self.caller_uid != 0, "ordinary_non_root_caller_required");
        r.plan.intent.validate()?;
        r.plan.before.validate()?;
        ensure!(
            r.plan.protocol == 2
                && r.plan.caller_identity_sha256 == CallerIdentity::current()?.sha256()?
                && r.plan.store_identity == store.identity()?
                && r.plan.device_id == DEVICE
                && r.plan.caller_uid == self.caller_uid
                && r.plan.intent.request_id == id
                && r.plan.profile_sha256 == digest(&(PROFILE, PACKAGES))?
                && digest(&r.plan)? == r.plan_sha256
                && r.dispatch_count <= 1
                && r.plan.expires_at
                    == r.plan
                        .created_at
                        .checked_add(PLAN_TTL)
                        .context("plan_clock_overflow")?,
            "receipt_binding_or_digest_mismatch"
        );
        if let Some(t) = &r.transaction {
            t.validate()?;
        }
        if let Some(t) = &r.target_checksum {
            checksum(t)?;
        }
        if let Some(o) = &r.after {
            o.validate()?;
        }
        if r.plan.intent.action == Action::RebootStaged {
            ensure!(
                r.target_checksum == r.plan.intent.target_checksum,
                "reboot_target_binding_mismatch"
            );
        }
        Ok(())
    }
    fn load(&self, store: &UserStore, id: &str) -> Result<Record> {
        valid_id(id)?;
        let r = store
            .load_json::<Record>(id)?
            .context("ho5_receipt_not_found")?;
        self.validate(store, &r, id)?;
        Ok(r)
    }
    pub fn receipt(&self, id: &str) -> Result<Option<Record>> {
        valid_id(id)?;
        let Some(s) = UserStore::existing(&self.state_dir, self.caller_uid)? else {
            return Ok(None);
        };
        let Some(r) = s.load_json::<Record>(id)? else {
            return Ok(None);
        };
        self.validate(&s, &r, id)?;
        Ok(Some(r))
    }
    pub async fn prepare<B: Backend>(
        &self,
        backend: &mut B,
        intent: Intent,
        now: u64,
    ) -> Result<Record> {
        intent.validate()?;
        ensure!(self.caller_uid != 0, "ordinary_non_root_caller_required");
        let (store, _lock) = UserStore::locked(&self.state_dir, self.caller_uid)?;
        if let Some(r) = store.load_json::<Record>(&intent.request_id)? {
            self.validate(&store, &r, &intent.request_id)?;
            ensure!(r.plan.intent == intent, "idempotency_binding_conflict");
            return Ok(r);
        }
        store.capacity()?;
        // A new UUID must not bypass an accepted/unknown intent for this task and action.
        for id in store.ids()? {
            let previous = self.load(&store, &id)?;
            ensure!(
                previous.plan.intent.action != intent.action
                    || (previous.plan.intent.task_id != intent.task_id
                        && (previous.dispatch_count == 0
                            || matches!(previous.state, State::Succeeded | State::Failed))),
                "existing_intent_use_original_request"
            );
        }
        let before = tokio::time::timeout(self.timeout, backend.inspect())
            .await
            .context("ho5_probe_timeout")??;
        before.validate()?;
        let plan = Plan {
            protocol: 2,
            device_id: DEVICE.into(),
            caller_uid: self.caller_uid,
            caller_identity_sha256: CallerIdentity::current()?.sha256()?,
            store_identity: store.identity()?,
            profile_sha256: digest(&(PROFILE, PACKAGES))?,
            created_at: now,
            expires_at: now.checked_add(PLAN_TTL).context("plan_clock_overflow")?,
            before,
            intent,
        };
        let r = Record {
            target_checksum: plan.intent.target_checksum.clone(),
            plan_sha256: digest(&plan)?,
            plan,
            state: State::Prepared,
            dispatch_count: 0,
            transaction: None,
            after: None,
            error_code: None,
            updated_at: now,
        };
        store.save_json(&r.plan.intent.request_id, &r)?;
        Ok(r)
    }
    fn save(&self, store: &UserStore, r: &mut Record, now: u64) -> Result<()> {
        r.updated_at = now.max(r.updated_at);
        self.validate(store, r, &r.plan.intent.request_id)?;
        store.save_json(&r.plan.intent.request_id, r)
    }
    /// Fixture executor seam. ReadOnlyBus has mutation_enabled=false; no production side effects.
    pub async fn advance<B: Backend>(
        &self,
        backend: &mut B,
        id: &str,
        hash: &str,
        now: u64,
    ) -> Result<Record> {
        let (store, _lock) = UserStore::locked(&self.state_dir, self.caller_uid)?;
        let mut r = self.load(&store, id)?;
        ensure!(r.plan_sha256 == hash, "plan_digest_mismatch");
        if r.dispatch_count != 0
            || !matches!(
                r.state,
                State::Prepared
                    | State::AwaitingPlatformAuthorization
                    | State::ForeignTransactionBusy
            )
        {
            return Ok(r);
        }
        ensure!(
            now >= r.plan.created_at && now < r.plan.expires_at,
            "plan_expired_or_clock_moved_backwards"
        );
        let o = tokio::time::timeout(self.timeout, backend.inspect())
            .await
            .context("ho5_probe_timeout")??;
        o.validate()?;
        r.error_code = None;
        if !o.allowed(r.plan.intent.action) {
            r.state = State::AwaitingPlatformAuthorization;
        } else if o.transaction.is_some() {
            r.state = State::ForeignTransactionBusy;
        } else if o.boot_id != r.plan.before.boot_id || o.booted != r.plan.before.booted {
            r.state = State::Failed;
            r.error_code = Some("baseline_changed".into());
        } else if r.plan.intent.action == Action::SystemDeps && o.staged.is_some() {
            r.state = State::ExternalEffectObserved;
        } else if r.plan.intent.action == Action::RebootStaged
            && !o
                .staged
                .as_ref()
                .is_some_and(|d| Some(&d.checksum) == r.target_checksum.as_ref() && d.has_profile())
        {
            r.state = State::Failed;
            r.error_code = Some("reboot_target_changed".into());
        } else if !backend.mutation_enabled() {
            r.error_code = Some("execution_disabled".into());
        } else {
            let call = match r.plan.intent.action {
                Action::SystemDeps => FixedCall::InstallProfile {
                    packages: PACKAGES,
                    remove: [],
                    no_pull_base: true,
                },
                Action::RebootStaged => FixedCall::RebootNormal {
                    target_checksum: r
                        .target_checksum
                        .clone()
                        .context("reboot_target_required")?,
                    interactive: false,
                    ignore_inhibitors: false,
                },
            };
            r.state = if r.plan.intent.action == Action::SystemDeps {
                State::DispatchIntent
            } else {
                State::RebootIntent
            };
            r.dispatch_count = 1;
            r.after = Some(o);
            self.save(&store, &mut r, now)?; // Durable uncertainty before the sole dispatch.
            let result = tokio::time::timeout(self.timeout, backend.dispatch(call)).await;
            match result {
                Ok(Dispatch::Accepted(t)) if r.plan.intent.action == Action::SystemDeps => {
                    if t.validate().is_ok() {
                        r.transaction = Some(t);
                        r.state = State::Running;
                    } else {
                        r.state = State::OutcomeUnknown;
                    }
                }
                Ok(Dispatch::RebootAccepted) if r.plan.intent.action == Action::RebootStaged => {
                    r.state = State::AwaitingReconnect
                }
                Ok(Dispatch::NotStarted(code)) => {
                    r.state = State::Failed;
                    r.error_code = Some(code.into());
                }
                _ => {
                    r.state = State::OutcomeUnknown;
                    r.error_code = Some("dispatch_outcome_unknown".into());
                }
            }
            self.save(&store, &mut r, now)?;
            return Ok(r);
        }
        r.after = Some(o);
        self.save(&store, &mut r, now)?;
        Ok(r)
    }
    /// Only observes an accepted request. Even a matching external deployment cannot prove dispatch.
    pub async fn reconcile<B: Backend>(
        &self,
        backend: &mut B,
        id: &str,
        now: u64,
    ) -> Result<Record> {
        let (store, _lock) = UserStore::locked(&self.state_dir, self.caller_uid)?;
        let mut r = self.load(&store, id)?;
        if matches!(r.state, State::Succeeded | State::Failed) {
            return Ok(r);
        }
        let probe = tokio::time::timeout(self.timeout, backend.inspect()).await;
        let o = match probe {
            Ok(Ok(o)) if o.validate().is_ok() => o,
            failed => {
                r.error_code = Some(
                    match failed {
                        Err(_) => "observation_timeout",
                        Ok(Err(_)) => "observation_unavailable",
                        _ => "observation_invalid",
                    }
                    .into(),
                );
                self.save(&store, &mut r, now)?;
                return Ok(r);
            }
        };
        r.error_code = None;
        match r.plan.intent.action {
            Action::SystemDeps if r.dispatch_count == 1 => {
                // Durable staged provenance wins over stale or foreign Finished signals.
                if r.state == State::StagedPendingReboot {
                    if o.boot_id != r.plan.before.boot_id {
                        if Some(&o.booted.checksum) == r.target_checksum.as_ref()
                            && PACKAGES
                                .iter()
                                .all(|p| o.verified_booted_packages.iter().any(|s| s == p))
                        {
                            r.state = State::Succeeded;
                        } else {
                            r.state = State::Failed;
                            r.error_code = Some("boot_target_or_packages_mismatch".into());
                        }
                    } else if !o.staged.as_ref().is_some_and(|d| {
                        Some(&d.checksum) == r.target_checksum.as_ref() && d.has_profile()
                    }) {
                        r.state = State::OutcomeUnknown;
                        r.error_code = Some("staged_target_changed_observe_original".into());
                    }
                } else if let Some(c) = &o.completion {
                    if r.transaction.as_ref() == Some(&c.transaction) {
                        if c.success
                            && o.staged
                                .as_ref()
                                .is_some_and(|d| d.checksum == c.target_checksum && d.has_profile())
                        {
                            r.state = State::StagedPendingReboot;
                            r.target_checksum = Some(c.target_checksum.clone());
                        } else {
                            r.state = State::Failed;
                            r.error_code = Some("transaction_or_target_failed".into());
                        }
                    } else {
                        r.state = State::OutcomeUnknown;
                    }
                } else if r.transaction.is_some() && r.transaction == o.transaction {
                    r.state = State::Running;
                } else {
                    r.state = State::OutcomeUnknown;
                }
            }
            Action::RebootStaged if r.dispatch_count == 1 => {
                if o.boot_id != r.plan.before.boot_id {
                    if Some(&o.booted.checksum) == r.target_checksum.as_ref()
                        && PACKAGES
                            .iter()
                            .all(|p| o.verified_booted_packages.iter().any(|s| s == p))
                    {
                        r.state = State::Succeeded;
                    } else {
                        r.state = State::Failed;
                        r.error_code = Some("boot_target_or_packages_mismatch".into());
                    }
                } else {
                    r.state = State::AwaitingReconnect;
                }
            }
            _ => (),
        }
        r.after = Some(o);
        self.save(&store, &mut r, now)?;
        Ok(r)
    }
}
pub fn coordinator(path: &Path) -> Coordinator {
    Coordinator {
        state_dir: path.into(),
        caller_uid: nix::unistd::geteuid().as_raw(),
        timeout: Duration::from_secs(90),
    }
}
#[cfg(test)]
#[path = "ho5_tests.rs"]
mod tests;

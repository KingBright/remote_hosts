//! Ordinary-user task journal. Metadata only: no helper connection, shell, contents or elevation.
use crate::{
    engine::unix_time,
    filesystem::{Store, parent_fd},
    protocol::{PLAN_TTL, digest, valid_id},
};
use anyhow::{Context, Result, ensure};
use clap::{Subcommand, ValueEnum};
use nix::{
    fcntl::AtFlags,
    sys::stat::fstatat,
    unistd::{User, getegid, geteuid},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
};

pub const TASK_PROTOCOL: u32 = 1;
pub const SERVICE: &str = "system/com.remoteplay.mesh";
const PLIST: &str = "/Library/LaunchDaemons/com.remoteplay.mesh.plist";
const APPROVAL_TTL: u64 = 120;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    InspectRemoteplayMesh,
    RepairRemoteplayMeshOwnership,
}
#[derive(Subcommand)]
pub enum Command {
    /// Closed capability description; no state or target access.
    Describe {
        #[arg(long, value_enum)]
        action: Action,
    },
    /// Persist host/user-bound intent. Device identity is caller-declared, not authenticated here.
    Prepare {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        device_id: String,
        #[arg(long, value_enum)]
        action: Action,
    },
    /// Execute the saved metadata check once; privileged action remains blocked.
    Run {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        plan_sha256: String,
    },
    /// Record one explicit, short-lived gateway approval; no privileged action is launched.
    Approve {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        plan_sha256: String,
        #[arg(long)]
        approval_operation_id: String,
    },
    /// Cancel before execution; a running task stays on its original recovery handle.
    Cancel {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        plan_sha256: String,
    },
    Status {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
    },
    /// Observe fresh metadata and compare identity; never resolves uncertainty or applies repairs.
    Verify {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        request_id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub uid: u32,
    pub gid: u32,
    pub home: String,
    pub platform: String,
}
impl Identity {
    pub fn current() -> Result<Self> {
        let uid = geteuid();
        ensure!(!uid.is_root(), "ordinary_task_requires_non_root_caller");
        let user = User::from_uid(uid)?.context("caller_account_not_found")?;
        Ok(Self {
            uid: uid.as_raw(),
            gid: getegid().as_raw(),
            home: user.dir.to_str().context("home_not_utf8")?.into(),
            platform: std::env::consts::OS.into(),
        })
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.uid != 0 && self.platform == "macos",
            "task_platform_or_caller_unsupported"
        );
        ensure!(
            self.home.starts_with('/')
                && self.home.len() < 256
                && !self.home.chars().any(char::is_control)
                && self
                    .home
                    .split('/')
                    .skip(1)
                    .all(|s| !s.is_empty() && s != "." && s != ".."),
            "invalid_task_home"
        );
        Ok(())
    }
}
pub fn describe(action: Action) -> Value {
    json!({"protocol":TASK_PROTOCOL,"action":action,"execution_authority":"local_user",
        "device_identity_evidence":"caller_declared_not_gateway_authenticated",
        "allowed_service":SERVICE,"allowed_profile_files":["<account-home>/Library/Application Support/RemotePlay/NativeMesh/mesh.conf","<account-home>/Library/Application Support/RemotePlay/NativeMesh/mesh.secret"],
        "metadata_only":true,"reads_file_contents":false,"arbitrary_commands":false,"helper_contacted":false,"supported_platforms":["macos"],
        "authorization":{"ordinary_metadata_check":"current_local_user","administrator":"not_checked_not_granted","persistent_access_extension":"not_granted","platform_confirmation_required":action == Action::RepairRemoteplayMeshOwnership},
        "supported_execution":action == Action::InspectRemoteplayMesh,
        "privileged_executor_implemented":false,
        "privileged_scope":{"service":"disable/unload exact system/com.remoteplay.mesh only after separate authorization","files":"repair root owner to authenticated account UID; preserve group/mode; revalidate device/inode/link count/owner before each change"},
        "cua_boundary":"This interface does not open com.apple.Terminal and cannot authorize a denied action through another channel.",
        "verification_limit":"File identity metadata is observable. Service executable, loaded state, administrator authorization and repair success are not established by this task."})
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub path: String,
    pub device: u64,
    pub inode: u64,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub links: u64,
    pub size: u64,
    pub modified_seconds: i64,
    pub modified_nanos: i64,
    pub changed_seconds: i64,
    pub changed_nanos: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub service_label: String,
    pub service_plist: Option<Metadata>,
    pub profile_files: Vec<Option<Metadata>>,
    pub service_runtime_evidence: String,
}
// fstatat observes an unreadable 0600 file without opening it or hashing secret contents.
fn metadata(path: &Path, uid: u32) -> Result<Option<Metadata>> {
    let result: Result<Metadata> = (|| {
        let (dir, name) = parent_fd(path)?;
        let s = fstatat(&dir, name.as_str(), AtFlags::AT_SYMLINK_NOFOLLOW)?;
        ensure!(
            s.st_mode & nix::libc::S_IFMT == nix::libc::S_IFREG && s.st_nlink == 1,
            "target_not_single_link_regular_file"
        );
        ensure!(
            s.st_uid == 0 || s.st_uid == uid,
            "target_owned_by_other_user"
        );
        let (mn, cn) = nanoseconds(&s);
        Ok(Metadata {
            path: path.to_str().context("target_path_not_utf8")?.into(),
            device: s.st_dev as u64,
            inode: s.st_ino as u64,
            uid: s.st_uid,
            gid: s.st_gid,
            mode: s.st_mode as u32 & 0o7777,
            links: s.st_nlink as u64,
            size: s.st_size as u64,
            modified_seconds: s.st_mtime as i64,
            modified_nanos: mn,
            changed_seconds: s.st_ctime as i64,
            changed_nanos: cn,
        })
    })();
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.downcast_ref::<nix::errno::Errno>() == Some(&nix::errno::Errno::ENOENT) => {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}
#[cfg(target_os = "macos")]
fn nanoseconds(s: &nix::libc::stat) -> (i64, i64) {
    (s.st_mtime_nsec, s.st_ctime_nsec)
}
#[cfg(not(target_os = "macos"))]
fn nanoseconds(s: &nix::libc::stat) -> (i64, i64) {
    (s.st_mtime_nsec as i64, s.st_ctime_nsec as i64)
}
fn inspect(identity: &Identity) -> Result<Snapshot> {
    let profile =
        Path::new(&identity.home).join("Library/Application Support/RemotePlay/NativeMesh");
    Ok(Snapshot {
        service_label: SERVICE.into(),
        service_plist: metadata(Path::new(PLIST), identity.uid)?,
        profile_files: ["mesh.conf", "mesh.secret"]
            .iter()
            .map(|n| metadata(&profile.join(n), identity.uid))
            .collect::<Result<_>>()?,
        service_runtime_evidence:
            "not_observed; plist identity alone is not service executable or loaded-state proof"
                .into(),
    })
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub protocol: u32,
    pub request_id: String,
    pub device_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub context_sha256: String,
    pub identity: Identity,
    pub action: Action,
    pub created_at: u64,
    pub expires_at: u64,
    pub snapshot: Snapshot,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Prepared,
    AwaitingApproval,
    Approved,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    AwaitingPlatformAuthorization,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    pub approval_operation_id: String,
    pub plan_sha256: String,
    pub context_sha256: String,
    pub action: Action,
    pub uid: u32,
    pub approved_at: u64,
    pub expires_at: u64,
    pub consumed_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub plan: Plan,
    pub plan_sha256: String,
    pub state: State,
    #[serde(default)]
    pub approval: Option<Approval>,
    #[serde(default)]
    pub cancelled_at: Option<u64>,
    pub result: Option<Snapshot>,
    pub error_code: Option<String>,
    pub events: Vec<String>,
    pub updated_at: u64,
}
fn response(record: &Record) -> Result<Value> {
    let mut v = serde_json::to_value(record)?;
    v["system_changes_confirmed"] = json!(false);
    v["administrator_authorization"] = json!("not_checked_not_granted");
    v["device_identity_evidence"] = json!("caller_declared_not_gateway_authenticated");
    v["recovery_required"] = json!(record.state == State::Running);
    if record.state == State::Running {
        v["next_action"] = json!(
            "Observe and verify the original request; no automatic replay or new request ID."
        );
    }
    if record.state == State::AwaitingApproval {
        v["next_action"] = json!(
            "Review this fixed action and plan digest, then issue one explicit approval bound to this request, owner, workspace and device."
        );
    }
    if record.state == State::Approved {
        v["next_action"] = json!(
            "Run once before the approval expires; this records the approval and still does not execute privileged work."
        );
    }
    if record.state == State::Cancelled {
        v["next_action"] = json!("This request is terminally cancelled and will not be replayed.");
    }
    if record.state == State::AwaitingPlatformAuthorization {
        v["next_action"] = json!(
            "Separate platform installation/access confirmation and authenticated privileged executor are required; no privileged execution was attempted."
        );
    }
    Ok(v)
}
fn valid_sha256(value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid_sha256"
    );
    Ok(())
}
#[cfg(test)]
fn load(store: &Store, id: &str, identity: &Identity) -> Result<Record> {
    load_bound(store, id, identity, None)
}
fn load_bound(
    store: &Store,
    id: &str,
    identity: &Identity,
    context_sha256: Option<&str>,
) -> Result<Record> {
    let r: Record = store.load_json(id)?.context("task_not_found")?;
    validate_record_bound(r, id, identity, context_sha256)
}
fn validate_record_bound(
    r: Record,
    id: &str,
    identity: &Identity,
    context_sha256: Option<&str>,
) -> Result<Record> {
    r.plan.identity.validate()?;
    valid_id(&r.plan.request_id)?;
    valid_id(&r.plan.device_id)?;
    if let Some(context) = context_sha256 {
        valid_sha256(context)?;
        ensure!(
            r.plan.context_sha256.is_empty() || r.plan.context_sha256 == context,
            "task_context_binding_mismatch"
        );
    } else {
        ensure!(
            r.plan.context_sha256.is_empty(),
            "task_context_binding_required"
        );
    }
    ensure!(
        r.plan.protocol == TASK_PROTOCOL
            && r.plan.request_id == id
            && r.plan.identity == *identity
            && digest(&r.plan)? == r.plan_sha256,
        "task_binding_or_digest_mismatch"
    );
    if let Some(approval) = &r.approval {
        valid_id(&approval.approval_operation_id)?;
        valid_sha256(&approval.plan_sha256)?;
        valid_sha256(&approval.context_sha256)?;
        ensure!(
            context_sha256 == Some(approval.context_sha256.as_str())
                && approval.plan_sha256 == r.plan_sha256
                && approval.action == r.plan.action
                && approval.action == Action::RepairRemoteplayMeshOwnership
                && approval.uid == identity.uid
                && approval.approved_at >= r.plan.created_at
                && approval.expires_at > approval.approved_at
                && approval.expires_at <= r.plan.expires_at
                && approval
                    .consumed_at
                    .is_none_or(|at| at >= approval.approved_at),
            "task_approval_binding_mismatch"
        );
    }
    ensure!(
        r.state != State::Approved || r.approval.is_some(),
        "task_approval_missing"
    );
    Ok(r)
}
#[cfg(test)]
fn prepare(
    store: &Store,
    identity: &Identity,
    id: &str,
    device: &str,
    action: Action,
    now: u64,
) -> Result<Value> {
    prepare_bound(store, identity, id, device, action, None, now)
}
fn prepare_bound(
    store: &Store,
    identity: &Identity,
    id: &str,
    device: &str,
    action: Action,
    context_sha256: Option<&str>,
    now: u64,
) -> Result<Value> {
    identity.validate()?;
    valid_id(id)?;
    valid_id(device)?;
    if let Some(old) = store.load_json::<Record>(id)? {
        let r = load_bound(store, id, identity, context_sha256)?;
        ensure!(
            old.plan.device_id == device && old.plan.action == action,
            "task_idempotency_binding_conflict"
        );
        return response(&r);
    }
    store.capacity()?;
    // A crash leaves an original handle, not permission to bypass it with a fresh UUID.
    for entry in fs::read_dir(&store.dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|s| s == "json")
            && let Some(name) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .filter(|s| valid_id(s).is_ok())
        {
            let r: Record = store.load_json(name)?.context("task_record_disappeared")?;
            ensure!(
                !(r.state == State::Running && r.plan.identity == *identity),
                "original_running_task_requires_recovery"
            );
        }
    }
    if let Some(context) = context_sha256 {
        valid_sha256(context)?;
    }
    let plan = Plan {
        protocol: TASK_PROTOCOL,
        request_id: id.into(),
        device_id: device.into(),
        context_sha256: context_sha256.unwrap_or_default().into(),
        identity: identity.clone(),
        action,
        created_at: now,
        expires_at: now.checked_add(PLAN_TTL).context("clock_overflow")?,
        snapshot: inspect(identity)?,
    };
    let r = Record {
        plan_sha256: digest(&plan)?,
        plan,
        state: State::Prepared,
        approval: None,
        cancelled_at: None,
        result: None,
        error_code: None,
        events: vec!["intent_prepared".into()],
        updated_at: now,
    };
    store.save_json(id, &r)?;
    response(&r)
}
#[cfg(test)]
fn execute(store: &Store, identity: &Identity, id: &str, hash: &str, now: u64) -> Result<Value> {
    execute_bound(store, identity, id, hash, None, now)
}
fn execute_bound(
    store: &Store,
    identity: &Identity,
    id: &str,
    hash: &str,
    context_sha256: Option<&str>,
    now: u64,
) -> Result<Value> {
    let mut r = load_bound(store, id, identity, context_sha256)?;
    ensure!(r.plan_sha256 == hash, "task_plan_digest_mismatch");
    if !matches!(r.state, State::Prepared | State::Approved) {
        return response(&r);
    }
    ensure!(
        now >= r.plan.created_at && now < r.plan.expires_at,
        "task_expired_or_clock_moved_backwards"
    );
    if r.plan.action == Action::RepairRemoteplayMeshOwnership {
        if r.state == State::Prepared {
            r.state = State::AwaitingApproval;
            r.error_code = Some("explicit_single_approval_required".into());
            r.events.push("awaiting_explicit_approval".into());
        } else {
            let context = context_sha256.context("gateway_bound_approval_required")?;
            let approval = r.approval.as_mut().context("task_approval_missing")?;
            ensure!(
                approval.context_sha256 == context
                    && approval.plan_sha256 == r.plan_sha256
                    && approval.action == r.plan.action
                    && approval.uid == identity.uid
                    && approval.consumed_at.is_none(),
                "task_approval_binding_mismatch_or_consumed"
            );
            ensure!(
                now >= approval.approved_at && now < approval.expires_at,
                "task_approval_expired_or_clock_moved_backwards"
            );
            approval.consumed_at = Some(now);
            r.state = State::AwaitingPlatformAuthorization;
            r.error_code = Some("platform_confirmation_and_privileged_executor_required".into());
            r.events.push("explicit_approval_consumed".into());
            r.events.push("privileged_execution_not_attempted".into());
        }
        r.updated_at = now;
        store.save_json(id, &r)?;
        return response(&r);
    }
    ensure!(
        r.plan.action == Action::InspectRemoteplayMesh,
        "task_action_unavailable"
    );
    r.state = State::Running;
    r.events.push("metadata_check_started".into());
    r.updated_at = now;
    store.save_json(id, &r)?;
    match inspect(identity) {
        Ok(snapshot) if snapshot == r.plan.snapshot => {
            r.result = Some(snapshot);
            r.state = State::Succeeded;
            r.events.push("metadata_check_completed".into());
        }
        Ok(_) => {
            r.state = State::Failed;
            r.error_code = Some("targets_changed_since_plan".into());
            r.events.push("metadata_check_rejected".into());
        }
        Err(_) => {
            r.state = State::Failed;
            r.error_code = Some("metadata_check_rejected_or_unavailable".into());
            r.events.push("metadata_check_failed".into());
        }
    }
    r.updated_at = unix_time();
    store.save_json(id, &r)?;
    response(&r)
}
fn approve(
    store: &Store,
    identity: &Identity,
    id: &str,
    hash: &str,
    approval_operation_id: &str,
    context_sha256: Option<&str>,
    now: u64,
) -> Result<Value> {
    valid_id(id)?;
    valid_id(approval_operation_id)?;
    valid_sha256(hash)?;
    let context = context_sha256.context("gateway_bound_approval_required")?;
    valid_sha256(context)?;
    let mut r = load_bound(store, id, identity, Some(context))?;
    ensure!(r.plan_sha256 == hash, "task_plan_digest_mismatch");
    ensure!(
        r.plan.action == Action::RepairRemoteplayMeshOwnership,
        "approval_not_required_for_action"
    );
    if let Some(existing) = &r.approval {
        ensure!(
            existing.approval_operation_id == approval_operation_id
                && existing.plan_sha256 == hash
                && existing.context_sha256 == context
                && existing.action == r.plan.action
                && existing.uid == identity.uid,
            "task_approval_already_recorded"
        );
        return response(&r);
    }
    ensure!(
        matches!(r.state, State::Prepared | State::AwaitingApproval),
        "task_not_awaiting_approval"
    );
    ensure!(
        now >= r.plan.created_at && now < r.plan.expires_at,
        "task_expired_or_clock_moved_backwards"
    );
    let expires_at = now
        .checked_add(APPROVAL_TTL)
        .context("clock_overflow")?
        .min(r.plan.expires_at);
    ensure!(expires_at > now, "task_expired_or_clock_moved_backwards");
    r.approval = Some(Approval {
        approval_operation_id: approval_operation_id.into(),
        plan_sha256: hash.into(),
        context_sha256: context.into(),
        action: r.plan.action,
        uid: identity.uid,
        approved_at: now,
        expires_at,
        consumed_at: None,
    });
    r.state = State::Approved;
    r.error_code = None;
    r.events.push("explicit_approval_recorded".into());
    r.updated_at = now;
    store.save_json(id, &r)?;
    response(&r)
}
fn cancel(
    store: &Store,
    identity: &Identity,
    id: &str,
    hash: &str,
    context_sha256: Option<&str>,
    now: u64,
) -> Result<Value> {
    valid_sha256(hash)?;
    let mut r = load_bound(store, id, identity, context_sha256)?;
    ensure!(r.plan_sha256 == hash, "task_plan_digest_mismatch");
    let result = match r.state {
        State::Prepared
        | State::AwaitingApproval
        | State::Approved
        | State::AwaitingPlatformAuthorization => {
            r.state = State::Cancelled;
            r.cancelled_at = Some(now);
            r.error_code = None;
            r.events
                .push("request_cancelled_before_privileged_execution".into());
            r.updated_at = now;
            store.save_json(id, &r)?;
            "cancelled"
        }
        State::Cancelled => "already_cancelled",
        State::Running => "original_running_no_replay",
        State::Succeeded | State::Failed => "already_terminal",
    };
    let mut value = response(&r)?;
    value["cancel_result"] = json!(result);
    Ok(value)
}
#[cfg(test)]
fn verify(store: &Store, identity: &Identity, id: &str) -> Result<Value> {
    verify_bound(store, identity, id, None)
}
fn verify_bound(
    store: &Store,
    identity: &Identity,
    id: &str,
    context_sha256: Option<&str>,
) -> Result<Value> {
    let r = load_bound(store, id, identity, context_sha256)?;
    let fresh = inspect(identity)?;
    let comparison = match &r.result {
        Some(old) if old == &fresh => "unchanged",
        Some(_) => "targets_changed",
        None => "no_completed_baseline",
    };
    Ok(
        json!({"request_id":id,"plan_sha256":r.plan_sha256,"record_state":r.state,"comparison":comparison,"fresh":fresh,
        "recovery_required":r.state == State::Running,"record_state_changed":false,"administrator_repair_verified":false,
        "acceptance_scope":"fixed file metadata only; no service execution/signature/loaded-state or administrator authorization proof"}),
    )
}
// Validation opens no journal, creates no directory and acquires no lock.
pub(crate) fn existing_store(path: &Path, uid: u32) -> Result<Option<Store>> {
    ensure!(path.is_absolute(), "task_store_requires_absolute_path");
    ensure!(
        path.components()
            .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "unsafe_task_store_component"
    );
    let mut part = PathBuf::from("/");
    for c in path.components() {
        if let Component::Normal(n) = c {
            part.push(n);
        }
        let m = match fs::symlink_metadata(&part) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let sticky_root = m.uid() == 0 && m.mode() & 0o1000 != 0;
        ensure!(
            m.is_dir()
                && !m.file_type().is_symlink()
                && (m.uid() == 0 || m.uid() == uid)
                && (m.mode() & 0o022 == 0 || sticky_root),
            "untrusted_task_store_ancestor"
        );
    }
    let m = fs::symlink_metadata(path)?;
    ensure!(
        m.uid() == uid && m.mode() & 0o077 == 0,
        "task_store_must_be_caller_owned_private_directory"
    );
    Ok(Some(Store {
        dir: path.into(),
        owner: uid,
    }))
}
/// Only execution may create a private journal directory, under an existing private parent.
/// Existing permissions are validated, never changed.
pub fn create_private_store(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let identity = Identity::current()?;
    identity.validate()?;
    if existing_store(path, identity.uid)?.is_some() {
        return Ok(());
    }
    let parent = path.parent().context("task_store_parent_missing")?;
    ensure!(
        existing_store(parent, identity.uid)?.is_some(),
        "task_store_parent_missing"
    );
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    ensure!(
        existing_store(path, identity.uid)?.is_some(),
        "task_store_missing"
    );
    Ok(())
}
pub(crate) fn locked_store(path: &Path, uid: u32) -> Result<(Store, File)> {
    let store = existing_store(path, uid)?.context("task_store_missing")?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path.join("task.lock"))?;
    let m = lock.metadata()?;
    ensure!(
        m.is_file() && m.uid() == uid && m.nlink() == 1 && m.mode() & 0o077 == 0,
        "untrusted_task_lock"
    );
    fs2::FileExt::try_lock_exclusive(&lock).context("task_store_busy_observe_original")?;
    Ok((store, lock))
}
/// A missing journal/receipt is a read result. No directory, record or task.lock is created.
pub fn read_status(path: &Path, request_id: &str, device_id: &str) -> Result<Option<Value>> {
    read_status_inner(path, request_id, Some(device_id), &Identity::current()?)
}
pub fn read_status_bound(
    path: &Path,
    request_id: &str,
    device_id: &str,
    context_sha256: &str,
) -> Result<Option<Value>> {
    valid_sha256(context_sha256)?;
    read_status_scoped(
        path,
        request_id,
        Some(device_id),
        Some(context_sha256),
        &Identity::current()?,
    )
}
fn read_status_inner(
    path: &Path,
    id: &str,
    device: Option<&str>,
    identity: &Identity,
) -> Result<Option<Value>> {
    read_status_scoped(path, id, device, None, identity)
}
fn read_status_scoped(
    path: &Path,
    id: &str,
    device: Option<&str>,
    context_sha256: Option<&str>,
    identity: &Identity,
) -> Result<Option<Value>> {
    identity.validate()?;
    valid_id(id)?;
    if let Some(d) = device {
        valid_id(d)?;
    }
    let Some(store) = existing_store(path, identity.uid)? else {
        return Ok(None);
    };
    let Some(record) = store.load_json::<Record>(id)? else {
        return Ok(None);
    };
    let record = validate_record_bound(record, id, identity, context_sha256)?;
    ensure!(
        device.is_none_or(|d| record.plan.device_id == d),
        "task_device_binding_mismatch"
    );
    Ok(Some(response(&record)?))
}
pub fn ordinary_tasks_supported() -> bool {
    Identity::current().is_ok_and(|i| i.validate().is_ok())
}
pub fn run_cli(command: Command) -> Result<Value> {
    run_impl(command, None, None)
}
/// Device and owner/workspace context come from the authenticated Agent, never MCP fields.
pub fn run_bound_cli(command: Command, device: &str, context_sha256: &str) -> Result<Value> {
    valid_id(device)?;
    valid_sha256(context_sha256)?;
    run_impl(command, Some(device), Some(context_sha256))
}
fn run_impl(command: Command, device: Option<&str>, context_sha256: Option<&str>) -> Result<Value> {
    if let Command::Describe { action } = command {
        return Ok(describe(action));
    }
    let identity = Identity::current()?;
    identity.validate()?;
    if let Command::Status {
        state_dir,
        request_id,
    } = &command
    {
        return read_status_scoped(state_dir, request_id, device, context_sha256, &identity)?
            .context("task_not_found");
    }
    let path = match &command {
        Command::Prepare { state_dir, .. }
        | Command::Run { state_dir, .. }
        | Command::Approve { state_dir, .. }
        | Command::Cancel { state_dir, .. }
        | Command::Verify { state_dir, .. } => state_dir,
        _ => unreachable!(),
    };
    let (store, _lock) = locked_store(path, identity.uid)?;
    if let Some(device) = device {
        match &command {
            Command::Prepare { device_id, .. } => {
                ensure!(device_id == device, "task_device_binding_mismatch")
            }
            Command::Run { request_id, .. }
            | Command::Approve { request_id, .. }
            | Command::Cancel { request_id, .. }
            | Command::Verify { request_id, .. } => {
                ensure!(
                    load_bound(&store, request_id, &identity, context_sha256)?
                        .plan
                        .device_id
                        == device,
                    "task_device_binding_mismatch"
                );
            }
            _ => unreachable!(),
        }
    }
    match command {
        Command::Prepare {
            request_id,
            device_id,
            action,
            ..
        } => prepare_bound(
            &store,
            &identity,
            &request_id,
            &device_id,
            action,
            context_sha256,
            unix_time(),
        ),
        Command::Run {
            request_id,
            plan_sha256,
            ..
        } => execute_bound(
            &store,
            &identity,
            &request_id,
            &plan_sha256,
            context_sha256,
            unix_time(),
        ),
        Command::Approve {
            request_id,
            plan_sha256,
            approval_operation_id,
            ..
        } => approve(
            &store,
            &identity,
            &request_id,
            &plan_sha256,
            &approval_operation_id,
            context_sha256,
            unix_time(),
        ),
        Command::Cancel {
            request_id,
            plan_sha256,
            ..
        } => cancel(
            &store,
            &identity,
            &request_id,
            &plan_sha256,
            context_sha256,
            unix_time(),
        ),
        Command::Verify { request_id, .. } => {
            verify_bound(&store, &identity, &request_id, context_sha256)
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn fixture() -> (tempfile::TempDir, Store, Identity, String, String) {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let profile = home.join("Library/Application Support/RemotePlay/NativeMesh");
        fs::create_dir_all(&profile).unwrap();
        for name in ["mesh.conf", "mesh.secret"] {
            fs::write(
                profile.join(name),
                b"fixture contents must never appear in metadata",
            )
            .unwrap();
        }
        let dir = home.join("receipts");
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let identity = Identity {
            uid: geteuid().as_raw(),
            gid: getegid().as_raw(),
            home: home.to_str().unwrap().into(),
            platform: "macos".into(),
        };
        (
            temp,
            Store {
                dir,
                owner: identity.uid,
            },
            identity,
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
        )
    }
    fn intent(s: &Store, i: &Identity, id: &str, d: &str, a: Action) -> Value {
        prepare(s, i, id, d, a, 1000).unwrap()
    }
    #[test]
    fn closed_description_has_no_privileged_executor_or_authorization() {
        let v = describe(Action::RepairRemoteplayMeshOwnership);
        assert_eq!(v["supported_execution"], false);
        assert_eq!(v["privileged_executor_implemented"], false);
        assert_eq!(
            v["authorization"]["administrator"],
            "not_checked_not_granted"
        );
        assert!(serde_json::from_str::<Action>("\"arbitrary_shell\"").is_err());
    }
    #[test]
    fn duplicate_request_and_run_retain_original_evidence() {
        let (_t, s, i, id, d) = fixture();
        let a = intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        assert_eq!(
            a,
            prepare(&s, &i, &id, &d, Action::InspectRemoteplayMesh, 1050).unwrap()
        );
        let hash = a["plan_sha256"].as_str().unwrap();
        let first = execute(&s, &i, &id, hash, 1001).unwrap();
        assert_eq!(first["state"], "succeeded");
        // A replay after a target changes returns the saved receipt; it does not inspect again.
        fs::remove_file(
            Path::new(&i.home).join("Library/Application Support/RemotePlay/NativeMesh/mesh.conf"),
        )
        .unwrap();
        assert_eq!(first, execute(&s, &i, &id, hash, 1002).unwrap());
        assert_eq!(
            verify(&s, &i, &id).unwrap()["comparison"],
            "targets_changed"
        );
    }
    #[test]
    fn privileged_request_is_persistently_blocked_without_target_changes() {
        let (_t, s, i, id, d) = fixture();
        let a = intent(&s, &i, &id, &d, Action::RepairRemoteplayMeshOwnership);
        let before = inspect(&i).unwrap();
        let hash = a["plan_sha256"].as_str().unwrap();
        let r = execute(&s, &i, &id, hash, 1001).unwrap();
        assert_eq!(r["state"], "awaiting_approval");
        assert_eq!(r["system_changes_confirmed"], false);
        assert_eq!(before, inspect(&i).unwrap());
        assert_eq!(r, execute(&s, &i, &id, hash, 1002).unwrap());
    }
    #[test]
    fn approval_is_single_use_and_bound_to_request_plan_context_and_caller() {
        let (_t, s, i, id, d) = fixture();
        let context = "a".repeat(64);
        let planned = prepare_bound(
            &s,
            &i,
            &id,
            &d,
            Action::RepairRemoteplayMeshOwnership,
            Some(&context),
            1000,
        )
        .unwrap();
        let hash = planned["plan_sha256"].as_str().unwrap();
        let waiting = execute_bound(&s, &i, &id, hash, Some(&context), 1001).unwrap();
        assert_eq!(waiting["state"], "awaiting_approval");
        let approval_operation_id = uuid::Uuid::new_v4().to_string();
        let approved = approve(
            &s,
            &i,
            &id,
            hash,
            &approval_operation_id,
            Some(&context),
            1002,
        )
        .unwrap();
        assert_eq!(approved["state"], "approved");
        assert_eq!(
            approved["administrator_authorization"],
            "not_checked_not_granted"
        );
        assert_eq!(approved["system_changes_confirmed"], false);
        assert_eq!(
            approved["device_identity_evidence"],
            "caller_declared_not_gateway_authenticated"
        );
        assert_eq!(
            approved,
            approve(
                &s,
                &i,
                &id,
                hash,
                &approval_operation_id,
                Some(&context),
                1003,
            )
            .unwrap()
        );
        assert!(
            approve(
                &s,
                &i,
                &id,
                hash,
                &uuid::Uuid::new_v4().to_string(),
                Some(&context),
                1003,
            )
            .is_err()
        );

        let result = execute_bound(&s, &i, &id, hash, Some(&context), 1004).unwrap();
        assert_eq!(result["state"], "awaiting_platform_authorization");
        assert_eq!(
            result["administrator_authorization"],
            "not_checked_not_granted"
        );
        assert_eq!(result["system_changes_confirmed"], false);
        assert_eq!(
            result["device_identity_evidence"],
            "caller_declared_not_gateway_authenticated"
        );
        assert_eq!(
            result["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| *event == "privileged_execution_not_attempted")
                .count(),
            1
        );
        assert_eq!(result["approval"]["consumed_at"], 1004);
        assert_eq!(
            result,
            execute_bound(&s, &i, &id, hash, Some(&context), 1005).unwrap()
        );
    }
    #[test]
    fn approval_rejects_context_digest_mismatch_and_expired_single_use_evidence() {
        let (_t, s, i, id, d) = fixture();
        let context = "c".repeat(64);
        let wrong_context = "d".repeat(64);
        let planned = prepare_bound(
            &s,
            &i,
            &id,
            &d,
            Action::RepairRemoteplayMeshOwnership,
            Some(&context),
            1000,
        )
        .unwrap();
        let hash = planned["plan_sha256"].as_str().unwrap();
        let approval_operation_id = uuid::Uuid::new_v4().to_string();
        assert!(
            approve(
                &s,
                &i,
                &id,
                hash,
                &approval_operation_id,
                Some(&wrong_context),
                1001,
            )
            .is_err()
        );
        assert!(
            approve(
                &s,
                &i,
                &id,
                &"0".repeat(64),
                &approval_operation_id,
                Some(&context),
                1001,
            )
            .is_err()
        );
        let approved = approve(
            &s,
            &i,
            &id,
            hash,
            &approval_operation_id,
            Some(&context),
            1001,
        )
        .unwrap();
        let expires_at = approved["approval"]["expires_at"].as_u64().unwrap();
        assert!(execute_bound(&s, &i, &id, hash, Some(&context), expires_at).is_err());
        assert_eq!(
            load_bound(&s, &id, &i, Some(&context)).unwrap().state,
            State::Approved
        );
    }
    #[test]
    fn cancel_is_durable_idempotent_and_never_changes_a_running_record() {
        let (_t, s, i, id, d) = fixture();
        let context = "e".repeat(64);
        let planned = prepare_bound(
            &s,
            &i,
            &id,
            &d,
            Action::RepairRemoteplayMeshOwnership,
            Some(&context),
            1000,
        )
        .unwrap();
        let hash = planned["plan_sha256"].as_str().unwrap();
        let cancelled = cancel(&s, &i, &id, hash, Some(&context), 1001).unwrap();
        assert_eq!(cancelled["state"], "cancelled");
        assert_eq!(cancelled["cancel_result"], "cancelled");
        assert_eq!(cancelled["cancelled_at"], 1001);
        assert_eq!(
            cancel(&s, &i, &id, hash, Some(&context), 1002).unwrap()["cancel_result"],
            "already_cancelled"
        );
        assert_eq!(
            execute_bound(&s, &i, &id, hash, Some(&context), 1003).unwrap()["state"],
            "cancelled"
        );

        let running_id = uuid::Uuid::new_v4().to_string();
        let running = prepare_bound(
            &s,
            &i,
            &running_id,
            &d,
            Action::InspectRemoteplayMesh,
            Some(&context),
            1000,
        )
        .unwrap();
        let running_hash = running["plan_sha256"].as_str().unwrap();
        let mut record: Record = s.load_json(&running_id).unwrap().unwrap();
        record.state = State::Running;
        s.save_json(&running_id, &record).unwrap();
        let before = fs::read(s.dir.join(format!("{running_id}.json"))).unwrap();
        let observed = cancel(&s, &i, &running_id, running_hash, Some(&context), 1004).unwrap();
        assert_eq!(observed["state"], "running");
        assert_eq!(observed["cancel_result"], "original_running_no_replay");
        assert_eq!(
            fs::read(s.dir.join(format!("{running_id}.json"))).unwrap(),
            before
        );
    }
    #[test]
    fn legacy_record_fields_default_without_changing_plan_digest() {
        let (_t, s, i, id, d) = fixture();
        intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        let record: Record = s.load_json(&id).unwrap().unwrap();
        let mut legacy = serde_json::to_value(record).unwrap();
        legacy.as_object_mut().unwrap().remove("approval");
        legacy.as_object_mut().unwrap().remove("cancelled_at");
        legacy["plan"]
            .as_object_mut()
            .unwrap()
            .remove("context_sha256");
        s.save_json(&id, &legacy).unwrap();
        let restored = load(&s, &id, &i).unwrap();
        assert_eq!(restored.state, State::Prepared);
        assert_eq!(restored.approval, None);
        assert_eq!(restored.cancelled_at, None);
    }
    #[test]
    fn changed_inode_rejects_saved_plan() {
        let (_t, s, i, id, d) = fixture();
        let a = intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        let path =
            Path::new(&i.home).join("Library/Application Support/RemotePlay/NativeMesh/mesh.conf");
        fs::rename(&path, path.with_extension("old")).unwrap();
        fs::write(&path, b"replacement").unwrap();
        let v = execute(&s, &i, &id, a["plan_sha256"].as_str().unwrap(), 1001).unwrap();
        assert_eq!(v["state"], "failed");
        assert_eq!(v["error_code"], "targets_changed_since_plan");
    }
    #[test]
    fn private_contents_are_never_opened_or_returned() {
        let (_t, s, i, id, d) = fixture();
        let path = Path::new(&i.home)
            .join("Library/Application Support/RemotePlay/NativeMesh/mesh.secret");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o0)).unwrap();
        let a = intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        assert!(!a.to_string().contains("fixture contents"));
        assert_eq!(
            execute(&s, &i, &id, a["plan_sha256"].as_str().unwrap(), 1001).unwrap()["state"],
            "succeeded"
        );
    }
    #[test]
    fn final_symlink_hardlink_and_parent_symlink_are_rejected() {
        let (_t, _s, i, _id, _d) = fixture();
        let base = Path::new(&i.home);
        let original = base.join("Library/Application Support/RemotePlay/NativeMesh/mesh.conf");
        let linked = base.join("linked");
        symlink(&original, &linked).unwrap();
        assert!(metadata(&linked, i.uid).is_err());
        fs::remove_file(&linked).unwrap();
        fs::hard_link(&original, &linked).unwrap();
        assert!(metadata(&linked, i.uid).is_err());
        fs::remove_file(&linked).unwrap();
        symlink(original.parent().unwrap(), &linked).unwrap();
        assert!(metadata(&linked.join("mesh.conf"), i.uid).is_err());
    }
    #[test]
    fn interrupted_record_requires_original_handle_even_with_new_device_id() {
        let (_t, s, i, id, d) = fixture();
        intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        let mut r: Record = s.load_json(&id).unwrap().unwrap();
        r.state = State::Running;
        s.save_json(&id, &r).unwrap();
        assert_eq!(
            execute(&s, &i, &id, &r.plan_sha256, 1001).unwrap()["recovery_required"],
            true
        );
        let new_id = uuid::Uuid::new_v4().to_string();
        let new_device = uuid::Uuid::new_v4().to_string();
        assert!(
            prepare(
                &s,
                &i,
                &new_id,
                &new_device,
                Action::InspectRemoteplayMesh,
                1002
            )
            .unwrap_err()
            .to_string()
            .contains("requires_recovery")
        );
        let v = verify(&s, &i, &id).unwrap();
        assert_eq!(v["record_state_changed"], false);
        assert_eq!(v["recovery_required"], true);
        assert_eq!(load(&s, &id, &i).unwrap().state, State::Running);
    }
    #[test]
    fn idempotency_conflict_wrong_caller_digest_and_expiry_stop_before_execution() {
        let (_t, s, i, id, d) = fixture();
        let a = intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        assert!(prepare(&s, &i, &id, &d, Action::RepairRemoteplayMeshOwnership, 1001).is_err());
        assert!(execute(&s, &i, &id, &"0".repeat(64), 1001).is_err());
        let mut other = i.clone();
        other.uid += 1;
        assert!(load(&s, &id, &other).is_err());
        let hash = a["plan_sha256"].as_str().unwrap();
        assert!(execute(&s, &i, &id, hash, 999).is_err());
        assert!(execute(&s, &i, &id, hash, 1300).is_err());
        assert_eq!(load(&s, &id, &i).unwrap().state, State::Prepared);
    }
    #[test]
    fn journal_lock_and_private_directory_are_enforced() {
        let (_t, s, i, _id, _d) = fixture();
        let (_a, lock) = locked_store(&s.dir, i.uid).unwrap();
        assert!(locked_store(&s.dir, i.uid).is_err());
        drop(lock);
        fs::set_permissions(&s.dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(locked_store(&s.dir, i.uid).is_err());
    }
    #[test]
    fn tampered_record_and_noncanonical_identifiers_are_rejected() {
        let (_t, s, i, id, d) = fixture();
        intent(&s, &i, &id, &d, Action::InspectRemoteplayMesh);
        let mut r: Record = s.load_json(&id).unwrap().unwrap();
        r.plan.identity.gid += 1;
        s.save_json(&id, &r).unwrap();
        assert!(load(&s, &id, &i).is_err());
        assert!(
            prepare(
                &s,
                &i,
                "../invalid",
                &d,
                Action::InspectRemoteplayMesh,
                1000
            )
            .is_err()
        );
    }
    #[test]
    fn status_missing_store_or_record_has_zero_creations() {
        let (_t, store, identity, id, device) = fixture();
        let missing = store.dir.join("missing").join("nested");
        assert!(
            read_status_inner(&missing, &id, Some(&device), &identity)
                .unwrap()
                .is_none()
        );
        assert!(!store.dir.join("missing").exists());
        assert!(
            read_status_inner(&store.dir, &id, Some(&device), &identity)
                .unwrap()
                .is_none()
        );
        assert_eq!(fs::read_dir(&store.dir).unwrap().count(), 0);
    }
    #[test]
    fn status_reads_without_a_lock_and_does_not_touch_targets_or_records() {
        let (_t, store, identity, id, device) = fixture();
        let planned = intent(
            &store,
            &identity,
            &id,
            &device,
            Action::InspectRemoteplayMesh,
        );
        let before = fs::read(store.dir.join(format!("{id}.json"))).unwrap();
        // Even with targets gone, Status reads only the saved receipt.
        fs::remove_dir_all(Path::new(&identity.home).join("Library")).unwrap();
        assert_eq!(
            read_status_inner(&store.dir, &id, Some(&device), &identity).unwrap(),
            Some(planned.clone())
        );
        assert!(!store.dir.join("task.lock").exists());
        let (_s, lock) = locked_store(&store.dir, identity.uid).unwrap();
        assert_eq!(
            read_status_inner(&store.dir, &id, Some(&device), &identity).unwrap(),
            Some(planned)
        );
        assert_eq!(
            fs::read(store.dir.join(format!("{id}.json"))).unwrap(),
            before
        );
        drop(lock);
    }
    #[test]
    fn status_rejects_wrong_device_tampering_and_unsupported_identity() {
        let (_t, store, identity, id, device) = fixture();
        intent(
            &store,
            &identity,
            &id,
            &device,
            Action::InspectRemoteplayMesh,
        );
        assert!(
            read_status_inner(
                &store.dir,
                &id,
                Some(&uuid::Uuid::new_v4().to_string()),
                &identity
            )
            .is_err()
        );
        let mut unsupported = identity.clone();
        unsupported.uid = 0;
        assert!(read_status_inner(&store.dir, &id, Some(&device), &unsupported).is_err());
        unsupported = identity.clone();
        unsupported.platform = "linux".into();
        assert!(read_status_inner(&store.dir, &id, Some(&device), &unsupported).is_err());
        let mut record: Record = store.load_json(&id).unwrap().unwrap();
        record.plan_sha256 = "0".repeat(64);
        store.save_json(&id, &record).unwrap();
        assert!(read_status_inner(&store.dir, &id, Some(&device), &identity).is_err());
        assert!(!store.dir.join("task.lock").exists());
    }
    #[test]
    fn status_rejects_a_symlink_journal_without_creating_a_lock() {
        let (_t, store, identity, id, device) = fixture();
        let alias = store.dir.parent().unwrap().join("journal-alias");
        symlink(&store.dir, &alias).unwrap();
        assert!(read_status_inner(&alias, &id, Some(&device), &identity).is_err());
        assert!(!store.dir.join("task.lock").exists());
    }
}

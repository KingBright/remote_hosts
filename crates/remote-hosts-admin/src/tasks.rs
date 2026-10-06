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
    Running,
    Succeeded,
    Failed,
    AwaitingPlatformAuthorization,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub plan: Plan,
    pub plan_sha256: String,
    pub state: State,
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
    if record.state == State::AwaitingPlatformAuthorization {
        v["next_action"] = json!(
            "Separate platform installation/access confirmation and authenticated privileged executor are required; do not retry authentication through this CLI."
        );
    }
    Ok(v)
}
fn load(store: &Store, id: &str, identity: &Identity) -> Result<Record> {
    let r: Record = store.load_json(id)?.context("task_not_found")?;
    r.plan.identity.validate()?;
    valid_id(&r.plan.request_id)?;
    ensure!(
        r.plan.protocol == TASK_PROTOCOL
            && r.plan.request_id == id
            && r.plan.identity == *identity
            && digest(&r.plan)? == r.plan_sha256,
        "task_binding_or_digest_mismatch"
    );
    Ok(r)
}
fn prepare(
    store: &Store,
    identity: &Identity,
    id: &str,
    device: &str,
    action: Action,
    now: u64,
) -> Result<Value> {
    identity.validate()?;
    valid_id(id)?;
    valid_id(device)?;
    if let Some(old) = store.load_json::<Record>(id)? {
        let r = load(store, id, identity)?;
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
    let plan = Plan {
        protocol: TASK_PROTOCOL,
        request_id: id.into(),
        device_id: device.into(),
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
        result: None,
        error_code: None,
        events: vec!["intent_prepared".into()],
        updated_at: now,
    };
    store.save_json(id, &r)?;
    response(&r)
}
fn execute(store: &Store, identity: &Identity, id: &str, hash: &str, now: u64) -> Result<Value> {
    let mut r = load(store, id, identity)?;
    ensure!(r.plan_sha256 == hash, "task_plan_digest_mismatch");
    if r.state != State::Prepared {
        return response(&r);
    }
    if r.plan.action == Action::RepairRemoteplayMeshOwnership {
        r.state = State::AwaitingPlatformAuthorization;
        r.error_code = Some("platform_confirmation_and_privileged_executor_required".into());
        r.events.push("privileged_execution_not_attempted".into());
        r.updated_at = now;
        store.save_json(id, &r)?;
        return response(&r);
    }
    ensure!(
        now >= r.plan.created_at && now < r.plan.expires_at,
        "task_expired_or_clock_moved_backwards"
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
fn verify(store: &Store, identity: &Identity, id: &str) -> Result<Value> {
    let r = load(store, id, identity)?;
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
// Reuse Store's private, atomic, fsynced records. The separate user-owned directory is not a root grant.
fn locked_store(path: &Path, uid: u32) -> Result<(Store, File)> {
    ensure!(path.is_absolute(), "task_store_requires_absolute_path");
    let mut part = PathBuf::from("/");
    for c in path.components() {
        match c {
            Component::RootDir => (),
            Component::Normal(n) => part.push(n),
            _ => anyhow::bail!("unsafe_task_store_component"),
        }
        let m = fs::symlink_metadata(&part)?;
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
    Ok((
        Store {
            dir: path.into(),
            owner: uid,
        },
        lock,
    ))
}
pub fn run_cli(command: Command) -> Result<Value> {
    if let Command::Describe { action } = command {
        return Ok(describe(action));
    }
    let identity = Identity::current()?;
    let path = match &command {
        Command::Prepare { state_dir, .. }
        | Command::Run { state_dir, .. }
        | Command::Status { state_dir, .. }
        | Command::Verify { state_dir, .. } => state_dir,
        _ => unreachable!(),
    };
    let (store, _lock) = locked_store(path, identity.uid)?;
    match command {
        Command::Prepare {
            request_id,
            device_id,
            action,
            ..
        } => prepare(
            &store,
            &identity,
            &request_id,
            &device_id,
            action,
            unix_time(),
        ),
        Command::Run {
            request_id,
            plan_sha256,
            ..
        } => execute(&store, &identity, &request_id, &plan_sha256, unix_time()),
        Command::Status { request_id, .. } => response(&load(&store, &request_id, &identity)?),
        Command::Verify { request_id, .. } => verify(&store, &identity, &request_id),
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
        assert_eq!(r["state"], "awaiting_platform_authorization");
        assert_eq!(r["system_changes_confirmed"], false);
        assert_eq!(before, inspect(&i).unwrap());
        assert_eq!(r, execute(&s, &i, &id, hash, 1002).unwrap());
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
}

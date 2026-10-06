//! Optional maintenance adapter for existing tools. No shell, root helper or separate operation store.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(target_os = "macos")]
use serde_json::json;
use std::path::{Path, PathBuf};

pub const PROTOCOL: u32 = 1;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    pub protocol: u32,
    pub describe: bool,
    pub receipt: bool,
    pub ordinary_execution: bool,
    pub privileged_executor: bool,
}
impl Capability {
    pub fn valid(&self) -> bool {
        self.protocol == PROTOCOL && !self.privileged_executor
    }
}
pub(crate) fn current(allow_exec: bool) -> Option<Capability> {
    #[cfg(target_os = "macos")]
    if remote_hosts_admin::tasks::ordinary_tasks_supported() {
        return Some(Capability {
            protocol: PROTOCOL,
            describe: true,
            receipt: true,
            ordinary_execution: allow_exec,
            privileged_executor: false,
        });
    }
    let _ = allow_exec;
    None
}
pub fn requested(tool: &str, v: &Value) -> bool {
    match tool {
        "workspace_context" => matches!(
            v["action"].as_str(),
            Some("maintenance_describe" | "maintenance_receipt")
        ),
        "terminal_exec" => v["action"] == "maintenance_task",
        _ => false,
    }
}
fn canonical_uuid(v: &str) -> Result<()> {
    ensure!(
        uuid::Uuid::parse_str(v).is_ok_and(|u| u.to_string() == v),
        "maintenance_requires_canonical_uuid"
    );
    Ok(())
}
fn action(v: &Value) -> Result<&str> {
    let a = v["maintenance_action"]
        .as_str()
        .context("maintenance_action_required")?;
    ensure!(
        matches!(
            a,
            "inspect_remoteplay_mesh" | "repair_remoteplay_mesh_ownership"
        ),
        "maintenance_action_unavailable"
    );
    Ok(a)
}
fn request_id(v: &Value) -> Result<&str> {
    let id = v["maintenance_request_id"]
        .as_str()
        .context("maintenance_request_id_required")?;
    canonical_uuid(id)?;
    Ok(id)
}
fn step(v: &Value) -> Result<&str> {
    let s = v["maintenance_step"]
        .as_str()
        .context("maintenance_step_required")?;
    ensure!(
        matches!(s, "prepare" | "run" | "verify"),
        "maintenance_step_unavailable"
    );
    Ok(s)
}
/// Cross-field checks complement the closed JSON schema at both trust boundaries.
pub fn validate(tool: &str, v: &Value) -> Result<()> {
    let reject = |keys: &[&str]| -> Result<()> {
        for key in keys {
            ensure!(
                v.get(*key).is_none(),
                "invalid_arguments: incompatible field {key}"
            );
        }
        Ok(())
    };
    let maintenance_fields = [
        "maintenance_action",
        "maintenance_request_id",
        "maintenance_step",
        "plan_sha256",
    ];
    match tool {
        "terminal_exec" if v["action"] == "maintenance_task" => {
            reject(&["command", "pty", "rows", "cols"])?;
            request_id(v)?;
            match step(v)? {
                "prepare" => {
                    action(v)?;
                    reject(&["plan_sha256"])?;
                }
                "run" => {
                    reject(&["maintenance_action"])?;
                    let digest = v["plan_sha256"].as_str().context("plan_sha256_required")?;
                    ensure!(
                        digest.len() == 64
                            && digest
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                        "invalid_plan_sha256"
                    );
                }
                "verify" => reject(&["maintenance_action", "plan_sha256"])?,
                _ => unreachable!(),
            }
        }
        "terminal_exec" => {
            ensure!(
                v["command"].is_string(),
                "invalid_arguments: $.command is required"
            );
            reject(&maintenance_fields)?;
        }
        "workspace_context" if requested(tool, v) => {
            reject(&[
                "cursor",
                "limit",
                "transfer_after",
                "terminal_cursor",
                "active_only",
                "after_event",
            ])?;
            if v["action"] == "maintenance_describe" {
                action(v)?;
                reject(&["maintenance_request_id"])?;
            } else {
                request_id(v)?;
                reject(&["maintenance_action"])?;
            }
        }
        "workspace_context" => reject(&maintenance_fields)?,
        _ => (),
    }
    Ok(())
}
/// Called before queueing a new job; exact retries still observe the original operation.
pub fn require_capability(
    tool: &str,
    v: &Value,
    platform: Option<&str>,
    cap: Option<&Capability>,
    allow_exec: bool,
) -> Result<()> {
    if !requested(tool, v) {
        return Ok(());
    }
    let cap = cap.filter(|c| c.valid()).context(
        "maintenance_feature_unavailable: selected agent has not negotiated maintenance tasks",
    )?;
    ensure!(
        platform == Some("macos"),
        "maintenance_platform_unavailable"
    );
    let supported = if tool == "terminal_exec" {
        allow_exec && cap.ordinary_execution
    } else if v["action"] == "maintenance_describe" {
        cap.describe
    } else {
        cap.receipt
    };
    ensure!(
        supported,
        "maintenance_feature_unavailable: requested maintenance action is disabled"
    );
    Ok(())
}
/// Authenticated owner/device/workspace are inputs from the Job and Agent, never MCP fields.
pub fn journal_dir(state: &Path, owner: &str, workspace: &str, device: &str) -> Result<PathBuf> {
    canonical_uuid(device)?;
    let (ws_device, ws_id) = workspace
        .split_once(':')
        .context("maintenance_workspace_binding_invalid")?;
    ensure!(ws_device == device, "maintenance_workspace_device_mismatch");
    canonical_uuid(ws_id)?;
    ensure!(
        !owner.is_empty() && owner.len() <= 256 && !owner.chars().any(char::is_control),
        "maintenance_owner_invalid"
    );
    ensure!(state.is_absolute(), "maintenance_state_root_invalid");
    let namespace = crate::hash(serde_json::to_vec(&[owner, workspace, device])?);
    Ok(state.join("maintenance_tasks").join(namespace))
}
#[cfg(target_os = "macos")]
fn core_action(v: &Value) -> Result<remote_hosts_admin::tasks::Action> {
    Ok(serde_json::from_value(json!(action(v)?))?)
}
#[cfg(target_os = "macos")]
fn bound(mut value: Value) -> Value {
    value["device_identity_evidence"] = json!("gateway_authenticated_agent_bound");
    value
}
pub(crate) fn read(
    config: &crate::AgentConfig,
    ws: &crate::files::Workspace,
    owner: &str,
    v: &Value,
) -> Result<Value> {
    validate("workspace_context", v)?;
    require_capability(
        "workspace_context",
        v,
        Some(std::env::consts::OS),
        current(config.allow_exec).as_ref(),
        config.allow_exec,
    )?;
    let path = journal_dir(&config.state_dir, owner, &ws.id, &config.device_id)?;
    ensure!(
        ws.device_id == config.device_id,
        "maintenance_workspace_device_mismatch"
    );
    #[cfg(target_os = "macos")]
    {
        if v["action"] == "maintenance_describe" {
            return Ok(
                json!({"maintenance_protocol":PROTOCOL,"journal_read_only":true,
                "descriptor":remote_hosts_admin::tasks::describe(core_action(v)?)}),
            );
        }
        let receipt =
            remote_hosts_admin::tasks::read_status(&path, request_id(v)?, &config.device_id)?
                .map(bound);
        Ok(
            json!({"maintenance_protocol":PROTOCOL,"journal_read_only":true,"found":receipt.is_some(),
            "maintenance_request_id":request_id(v)?,"receipt":receipt}),
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        anyhow::bail!("maintenance_platform_unavailable")
    }
}
pub(crate) struct FixedLaunch {
    pub executable: PathBuf,
    pub args: Vec<std::ffi::OsString>,
}
pub(crate) fn launch(
    config: &crate::AgentConfig,
    ws: &crate::files::Workspace,
    owner: &str,
    v: &Value,
) -> Result<FixedLaunch> {
    validate("terminal_exec", v)?;
    ensure!(requested("terminal_exec", v), "maintenance_action_required");
    require_capability(
        "terminal_exec",
        v,
        Some(std::env::consts::OS),
        current(config.allow_exec).as_ref(),
        config.allow_exec,
    )?;
    journal_dir(&config.state_dir, owner, &ws.id, &config.device_id)?;
    ensure!(
        ws.device_id == config.device_id,
        "maintenance_workspace_device_mismatch"
    );
    let mut args: Vec<std::ffi::OsString> = vec![
        "maintenance-task".into(),
        "--state-root".into(),
        config.state_dir.clone().into_os_string(),
        format!("--owner={owner}").into(),
        "--workspace-id".into(),
        ws.id.clone().into(),
        "--device-id".into(),
        config.device_id.clone().into(),
        "--step".into(),
        step(v)?.into(),
        "--request-id".into(),
        request_id(v)?.into(),
    ];
    if step(v)? == "prepare" {
        args.extend(["--maintenance-action".into(), action(v)?.into()]);
    }
    if step(v)? == "run" {
        args.extend([
            "--plan-sha256".into(),
            v["plan_sha256"]
                .as_str()
                .context("plan_sha256_required")?
                .into(),
        ]);
    }
    Ok(FixedLaunch {
        executable: std::env::current_exe()?,
        args,
    })
}
/// Hidden ordinary-user child entry; every argument is generated by launch(), never a shell.
#[cfg(target_os = "macos")]
#[derive(clap::Args)]
pub struct ChildArgs {
    #[arg(long)]
    pub state_root: PathBuf,
    #[arg(long)]
    pub owner: String,
    #[arg(long)]
    pub workspace_id: String,
    #[arg(long)]
    pub device_id: String,
    #[arg(long)]
    pub step: String,
    #[arg(long)]
    pub request_id: String,
    #[arg(long)]
    pub maintenance_action: Option<String>,
    #[arg(long)]
    pub plan_sha256: Option<String>,
}
#[cfg(target_os = "macos")]
pub fn run_child(args: ChildArgs) -> Result<Value> {
    use remote_hosts_admin::tasks::{self, Command};
    ensure!(
        tasks::ordinary_tasks_supported(),
        "maintenance_platform_or_caller_unsupported"
    );
    let mut v = json!({"action":"maintenance_task", "maintenance_step":args.step, "maintenance_request_id":args.request_id});
    if let Some(a) = &args.maintenance_action {
        v["maintenance_action"] = json!(a);
    }
    if let Some(p) = &args.plan_sha256 {
        v["plan_sha256"] = json!(p);
    }
    validate("terminal_exec", &v)?;
    let state_dir = journal_dir(
        &args.state_root,
        &args.owner,
        &args.workspace_id,
        &args.device_id,
    )?;
    let command = match step(&v)? {
        "prepare" => {
            tasks::create_private_store(
                state_dir
                    .parent()
                    .context("maintenance_store_parent_missing")?,
            )?;
            tasks::create_private_store(&state_dir)?;
            Command::Prepare {
                state_dir,
                request_id: args.request_id,
                device_id: args.device_id.clone(),
                action: core_action(&v)?,
            }
        }
        "run" => Command::Run {
            state_dir,
            request_id: args.request_id,
            plan_sha256: args.plan_sha256.context("plan_sha256_required")?,
        },
        "verify" => Command::Verify {
            state_dir,
            request_id: args.request_id,
        },
        _ => unreachable!(),
    };
    let mut value = tasks::run_bound_cli(command, &args.device_id)?;
    value["device_identity_evidence"] = json!("fixed_child_device_bound");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn args() -> Value {
        json!({"workspace_id":"fixture","idempotency_key":"fixture","action":"maintenance_task",
            "maintenance_step":"prepare","maintenance_request_id":uuid::Uuid::new_v4().to_string(),
            "maintenance_action":"inspect_remoteplay_mesh"})
    }
    #[test]
    fn legacy_defaults_and_typed_arguments_are_closed() {
        crate::tools::validate(
            "terminal_exec",
            &json!({"workspace_id":"fixture","idempotency_key":"legacy","command":"true"}),
        )
        .unwrap();
        crate::tools::validate(
            "workspace_context",
            &json!({"workspace_id":"fixture","active_only":true}),
        )
        .unwrap();
        assert!(
            crate::tools::validate(
                "terminal_exec",
                &json!({"workspace_id":"fixture","idempotency_key":"missing"})
            )
            .is_err()
        );
        let v = args();
        crate::tools::validate("terminal_exec", &v).unwrap();
        for key in [
            "command",
            "pty",
            "rows",
            "cols",
            "state_dir",
            "home",
            "owner",
            "device_id",
            "executable",
        ] {
            let mut bad = v.clone();
            bad[key] = match key {
                "pty" => json!(true),
                "rows" => json!(40),
                "cols" => json!(120),
                _ => json!("untrusted"),
            };
            assert!(
                crate::tools::validate("terminal_exec", &bad).is_err(),
                "{key}"
            );
        }
        let mut bad = v.clone();
        bad["maintenance_action"] = json!("arbitrary_root_command");
        assert!(crate::tools::validate("terminal_exec", &bad).is_err());
        let mut run = v;
        run["maintenance_step"] = json!("run");
        run.as_object_mut().unwrap().remove("maintenance_action");
        assert!(crate::tools::validate("terminal_exec", &run).is_err());
        run["plan_sha256"] = json!("a".repeat(64));
        crate::tools::validate("terminal_exec", &run).unwrap();
        run["plan_sha256"] = json!("A".repeat(64));
        assert!(crate::tools::validate("terminal_exec", &run).is_err());
    }
    #[test]
    fn read_actions_reject_mixed_snapshot_and_execution_fields() {
        let describe = json!({"workspace_id":"fixture","action":"maintenance_describe","maintenance_action":"inspect_remoteplay_mesh"});
        crate::tools::validate("workspace_context", &describe).unwrap();
        for key in [
            "cursor",
            "active_only",
            "maintenance_request_id",
            "maintenance_step",
            "state_dir",
        ] {
            let mut bad = describe.clone();
            bad[key] = if key == "active_only" {
                json!(true)
            } else {
                json!("untrusted")
            };
            assert!(crate::tools::validate("workspace_context", &bad).is_err());
        }
        let receipt = json!({"workspace_id":"fixture","action":"maintenance_receipt","maintenance_request_id":uuid::Uuid::new_v4().to_string()});
        crate::tools::validate("workspace_context", &receipt).unwrap();
        let mut bad = receipt;
        bad["maintenance_action"] = json!("inspect_remoteplay_mesh");
        assert!(crate::tools::validate("workspace_context", &bad).is_err());
    }
    #[test]
    fn optional_capability_is_explicit_and_never_privileged() {
        let v = args();
        let cap = Capability {
            protocol: 1,
            describe: true,
            receipt: true,
            ordinary_execution: true,
            privileged_executor: false,
        };
        require_capability("terminal_exec", &v, Some("macos"), Some(&cap), true).unwrap();
        assert!(require_capability("terminal_exec", &v, Some("macos"), None, true).is_err());
        assert!(require_capability("terminal_exec", &v, Some("linux"), Some(&cap), true).is_err());
        assert!(require_capability("terminal_exec", &v, Some("macos"), Some(&cap), false).is_err());
        let invalid = Capability {
            privileged_executor: true,
            ..cap
        };
        assert!(!invalid.valid());
        require_capability(
            "terminal_exec",
            &json!({"command":"true"}),
            None,
            None,
            false,
        )
        .unwrap();
        let features = crate::capabilities::RuntimeFeatures::current();
        assert_eq!(features.names.len(), 32);
        assert!(features.valid());
        assert_eq!(crate::tools::catalog().len(), 24);
        let hello: crate::gateway::DeviceHello = serde_json::from_value(
            json!({"session":"legacy","roots":[],"allow_write":false,"allow_exec":false}),
        )
        .unwrap();
        assert!(hello.maintenance_tasks.is_none());
    }
    #[test]
    fn journal_namespaces_separate_owner_workspace_and_device_without_io() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path();
        let device = uuid::Uuid::new_v4().to_string();
        let workspace = format!("{device}:{}", uuid::Uuid::new_v4());
        let path = journal_dir(state, "owner", &workspace, &device).unwrap();
        assert_eq!(
            path,
            journal_dir(state, "owner", &workspace, &device).unwrap()
        );
        assert_ne!(
            path,
            journal_dir(state, "other", &workspace, &device).unwrap()
        );
        assert_ne!(
            path,
            journal_dir(
                state,
                "owner",
                &format!("{device}:{}", uuid::Uuid::new_v4()),
                &device
            )
            .unwrap()
        );
        let other = uuid::Uuid::new_v4().to_string();
        assert_ne!(
            path,
            journal_dir(
                state,
                "owner",
                &format!("{other}:{}", uuid::Uuid::new_v4()),
                &other
            )
            .unwrap()
        );
        assert!(journal_dir(state, "owner", &workspace, &other).is_err());
        assert_eq!(std::fs::read_dir(state).unwrap().count(), 0);
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn generated_argv_has_no_shell_or_caller_selected_executable() {
        let temp = tempfile::tempdir().unwrap();
        let device = uuid::Uuid::new_v4().to_string();
        let ws = crate::files::Workspace {
            id: format!("{device}:{}", uuid::Uuid::new_v4()),
            device_id: device.clone(),
            root: temp.path().to_owned(),
        };
        let config = crate::AgentConfig {
            gateway_url: "https://fixture.invalid".into(),
            device_id: device,
            device_token: "unused".into(),
            state_dir: temp.path().to_owned(),
            roots: vec![ws.root.clone()],
            allow_write: false,
            allow_exec: true,
            shell: "/nonexistent-shell".into(),
        };
        let fixed = launch(&config, &ws, "owner; unexecuted", &args()).unwrap();
        assert_eq!(fixed.executable, std::env::current_exe().unwrap());
        assert_eq!(fixed.args[0], "maintenance-task");
        assert!(fixed.args.iter().any(|a| a == "--owner=owner; unexecuted"));
        assert!(!fixed.args.iter().any(|a| a == "-c" || a == "sudo"));
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}

//! Synthetic gateway/device tests; no production credentials, root executor or target-file access.
use remote_hosts_code::{
    DeviceRegistration, GatewayConfig, auth::Principal, gateway::Gateway, hash,
    maintenance_tasks::journal_dir, now, random,
};
use serde_json::{Value, json};

async fn gateway() -> (tempfile::TempDir, Gateway, Principal, String) {
    let temp = tempfile::tempdir().unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: remote_hosts_code::default_mcp_client_origins(),
        public_url: "https://maintenance.fixture".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: temp.path().join("gateway"),
        owner: "fixture-owner".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: device.clone(),
            name: "fixture".into(),
            token_hash: hash(random()),
            scopes: vec!["code:read".into(), "terminal:exec".into()],
        }],
    })
    .await
    .unwrap();
    let p = Principal {
        owner: "fixture-owner".into(),
        scopes: vec!["code:read".into(), "terminal:exec".into()],
    };
    (temp, g, p, format!("{device}:{}", uuid::Uuid::new_v4()))
}
fn execution(workspace: &str) -> Value {
    json!({"workspace_id":workspace,"idempotency_key":"maintenance-fixture","action":"maintenance_task",
        "maintenance_step":"prepare","maintenance_request_id":uuid::Uuid::new_v4().to_string(),
        "maintenance_action":"inspect_remoteplay_mesh"})
}
async fn advertise(g: &Gateway, platform: &str, cap: Option<Value>, allow_exec: bool) {
    let mut hello = json!({"version":"fixture","session":random(),"platform":platform,"roots":[],"allow_write":false,"allow_exec":allow_exec});
    if let Some(cap) = cap {
        hello["maintenance_tasks"] = cap;
    }
    g.store
        .put(
            "online",
            &g.config.devices[0].id,
            &json!({"hello":hello,"last_seen":now()}),
            i64::MAX,
        )
        .await
        .unwrap();
}
fn capability() -> Value {
    json!({"protocol":1,"describe":true,"receipt":true,"ordinary_execution":true,"privileged_executor":false})
}
async fn jobs(g: &Gateway) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap()
}
#[tokio::test]
async fn unavailable_agents_and_permissions_reject_before_queue() {
    let (_temp, g, p, ws) = gateway().await;
    let v = execution(&ws);
    for (platform, cap, allow_exec) in [
        ("macos", None, true),
        ("windows", Some(capability()), true),
        ("linux", Some(capability()), true),
        ("macos", Some(capability()), false),
        (
            "macos",
            Some(
                json!({"protocol":1,"describe":true,"receipt":true,"ordinary_execution":true,"privileged_executor":true}),
            ),
            true,
        ),
    ] {
        advertise(&g, platform, cap, allow_exec).await;
        assert!(g.dispatch(&p, "terminal_exec", v.clone()).await.is_err());
        assert_eq!(jobs(&g).await, 0);
    }
    advertise(&g, "macos", Some(capability()), true).await;
    let readonly = Principal {
        owner: p.owner.clone(),
        scopes: vec!["code:read".into()],
    };
    assert!(g.dispatch(&readonly, "terminal_exec", v).await.is_err());
    assert_eq!(jobs(&g).await, 0);
}
#[tokio::test]
async fn exact_retry_keeps_original_operation_when_capability_disappears() {
    let (_temp, g, p, ws) = gateway().await;
    let v = execution(&ws);
    advertise(&g, "macos", Some(capability()), true).await;
    g.dispatch(&p, "terminal_exec", v.clone()).await.unwrap();
    let first: String = sqlx::query_scalar("SELECT id FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    // Stored exact retries must bypass the new-job gate and observe this original handle.
    g.store
        .take::<Value>("online", &g.config.devices[0].id)
        .await
        .unwrap();
    g.dispatch(&p, "terminal_exec", v.clone()).await.unwrap();
    assert_eq!(jobs(&g).await, 1);
    let original: String = sqlx::query_scalar("SELECT id FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(original, first);
    let mut changed = v.clone();
    changed["maintenance_action"] = json!("repair_remoteplay_mesh_ownership");
    assert!(
        g.dispatch(&p, "terminal_exec", changed)
            .await
            .unwrap_err()
            .to_string()
            .contains("idempotency_conflict")
    );
    let mut new_key = v;
    new_key["idempotency_key"] = json!("different-key");
    assert!(g.dispatch(&p, "terminal_exec", new_key).await.is_err());
    assert_eq!(jobs(&g).await, 1);
}
#[test]
fn journal_selection_never_creates_an_owner_or_workspace_directory() {
    let temp = tempfile::tempdir().unwrap();
    let device = uuid::Uuid::new_v4().to_string();
    let ws = format!("{device}:{}", uuid::Uuid::new_v4());
    assert_ne!(
        journal_dir(temp.path(), "first", &ws, &device).unwrap(),
        journal_dir(temp.path(), "second", &ws, &device).unwrap()
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn agent_maintenance_queries_are_readonly_and_owner_scoped() {
    use remote_hosts_code::{AgentConfig, agent::Agent, gateway::Job};
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("project");
    std::fs::create_dir(&root).unwrap();
    let agent = Agent::new(AgentConfig {
        gateway_url: "https://fixture.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: random(),
        state_dir: base.join("state"),
        roots: vec![root.clone()],
        allow_write: false,
        allow_exec: false,
        shell: "/nonexistent-shell".into(),
    })
    .await
    .unwrap();
    let job = |owner: &str, tool: &str, arguments: Value| Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: agent.config.device_id.clone(),
        owner: owner.into(),
        tool: tool.into(),
        arguments,
    };
    let opened = agent
        .execute(&job(
            "first",
            "workspace_open",
            json!({"device_id":agent.config.device_id,"root":root,"idempotency_key":"open"}),
        ))
        .await
        .unwrap();
    let ws = opened["workspace"]["id"].as_str().unwrap();
    let request = uuid::Uuid::new_v4().to_string();
    let described = agent.execute(&job("first", "workspace_context", json!({"workspace_id":ws,"action":"maintenance_describe","maintenance_action":"repair_remoteplay_mesh_ownership"}))).await.unwrap();
    assert_eq!(described["descriptor"]["supported_execution"], false);
    let query =
        json!({"workspace_id":ws,"action":"maintenance_receipt","maintenance_request_id":request});
    let absent = agent
        .execute(&job("first", "workspace_context", query.clone()))
        .await
        .unwrap();
    assert_eq!(absent["found"], false);
    assert!(!agent.config.state_dir.join("maintenance_tasks").exists());
    // Seed only a synthetic receipt; Status never examines a production target.
    let path = journal_dir(
        &agent.config.state_dir,
        "first",
        ws,
        &agent.config.device_id,
    )
    .unwrap();
    let saved = seed_receipt(
        &agent.config.state_dir,
        &path,
        &request,
        &agent.config.device_id,
        false,
    );
    let bytes = std::fs::read(path.join(format!("{request}.json"))).unwrap();
    let present = agent
        .execute(&job("first", "workspace_context", query.clone()))
        .await
        .unwrap();
    assert_eq!(present["found"], true);
    assert_eq!(present["receipt"]["plan_sha256"], saved);
    assert!(!path.join("task.lock").exists());
    assert_eq!(
        std::fs::read(path.join(format!("{request}.json"))).unwrap(),
        bytes
    );
    let other = agent
        .execute(&job("second", "workspace_context", query))
        .await
        .unwrap();
    assert_eq!(other["found"], false);
    assert!(
        !journal_dir(
            &agent.config.state_dir,
            "second",
            ws,
            &agent.config.device_id
        )
        .unwrap()
        .exists()
    );
}
#[cfg(target_os = "macos")]
fn seed_receipt(
    state: &std::path::Path,
    path: &std::path::Path,
    request: &str,
    device: &str,
    repair: bool,
) -> String {
    use remote_hosts_admin::{
        filesystem::Store,
        protocol::digest,
        tasks::{self, Action, Identity, Plan, Record, Snapshot, State},
    };
    use std::os::unix::fs::PermissionsExt;
    if !state.exists() {
        std::fs::create_dir(state).unwrap();
        std::fs::set_permissions(state, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    tasks::create_private_store(path.parent().unwrap()).unwrap();
    tasks::create_private_store(path).unwrap();
    let identity = Identity::current().unwrap();
    let plan = Plan {
        protocol: 1,
        request_id: request.into(),
        device_id: device.into(),
        identity: identity.clone(),
        action: if repair {
            Action::RepairRemoteplayMeshOwnership
        } else {
            Action::InspectRemoteplayMesh
        },
        created_at: 1,
        expires_at: 2,
        snapshot: Snapshot {
            service_label: "synthetic-no-target-access".into(),
            service_plist: None,
            profile_files: vec![],
            service_runtime_evidence: "fixture".into(),
        },
    };
    let hash = digest(&plan).unwrap();
    let record = Record {
        plan,
        plan_sha256: hash.clone(),
        state: if repair {
            State::Prepared
        } else {
            State::Succeeded
        },
        result: None,
        error_code: None,
        events: vec!["synthetic".into()],
        updated_at: 1,
    };
    Store {
        dir: path.into(),
        owner: identity.uid,
    }
    .save_json(request, &record)
    .unwrap();
    hash
}
#[cfg(target_os = "macos")]
#[tokio::test]
async fn hidden_child_replays_receipt_or_blocks_privilege_with_real_exit_codes() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let state = base.join("state");
    let device = uuid::Uuid::new_v4().to_string();
    let ws = format!("{device}:{}", uuid::Uuid::new_v4());
    let path = journal_dir(&state, "fixture", &ws, &device).unwrap();
    for repair in [false, true] {
        let request = uuid::Uuid::new_v4().to_string();
        let plan_hash = seed_receipt(&state, &path, &request, &device, repair);
        let run = || {
            let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_remote-hosts-code"));
            command
                .args(["maintenance-task", "--state-root"])
                .arg(&state)
                .args([
                    "--owner",
                    "fixture",
                    "--workspace-id",
                    &ws,
                    "--device-id",
                    &device,
                    "--step",
                    "run",
                    "--request-id",
                    &request,
                    "--plan-sha256",
                    &plan_hash,
                ])
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true);
            async move {
                tokio::time::timeout(std::time::Duration::from_secs(5), command.output())
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let first = run().await;
        let second = run().await;
        assert_eq!(first.status.code(), Some(if repair { 2 } else { 0 }));
        assert_eq!(second.status.code(), first.status.code());
        let a: Value = serde_json::from_slice(&first.stdout).unwrap();
        let b: Value = serde_json::from_slice(&second.stdout).unwrap();
        assert_eq!(a, b);
        assert_eq!(a["system_changes_confirmed"], false);
        if repair {
            assert_eq!(a["state"], "awaiting_platform_authorization");
            assert_eq!(
                a["error_code"],
                "platform_confirmation_and_privileged_executor_required"
            );
        }
    }
}

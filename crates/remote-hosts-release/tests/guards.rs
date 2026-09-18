//! Guard failures must not spawn a command or manufacture success.
use remote_hosts_release::{
    evidence, executor,
    pipeline::{self, Gate, Plan, Receipt},
    source,
    toolchain::Toolchain,
};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};
fn fixture() -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().canonicalize().unwrap();
    let source = root.join("source");
    fs::create_dir(&source).unwrap();
    fs::create_dir(source.join("src")).unwrap();
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname=\"fixture\"\nversion=\"0.1.0\"\n",
    )
    .unwrap();
    fs::write(source.join("Cargo.lock"), "version=4\n").unwrap();
    fs::write(source.join("src/lib.rs"), "").unwrap();
    let inputs = source::inventory(&source).unwrap();
    let source_id = evidence::identity(&inputs).unwrap();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_rh-release"))
        .canonicalize()
        .unwrap();
    let plan = Plan {
        schema: 1,
        source_id: source_id.clone(),
        inputs,
        package: "fixture".into(),
        toolchain: Toolchain {
            requested: "1.94.1".into(),
            host: "fixture".into(),
            rustc_version: "fixture".into(),
            cargo_version: "fixture".into(),
            programs: BTreeMap::from([("cargo".into(), binary.clone())]),
            fingerprints: BTreeMap::from([("cargo".into(), evidence::hash_file(&binary).unwrap())]),
            env: executor::environment(),
        },
        gate_budgets: [30; 4],
        gates: vec![Gate::Format, Gate::Clippy, Gate::Tests, Gate::Build],
    };
    let state = Receipt {
        schema: 1,
        plan_id: evidence::identity(&plan).unwrap(),
        source_id,
        state: "prepared".into(),
        phase: None,
        updated_ms: 0,
        attempts: BTreeMap::new(),
        last_error: None,
        deployed: false,
    };
    evidence::atomic(&root.join("plan.json"), &plan).unwrap();
    evidence::atomic(&root.join("state.json"), &state).unwrap();
    (d, root)
}
#[cfg(unix)]
#[test]
fn existing_workspace_permissions_are_preserved() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let root = d.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
    evidence::private_dir(&root).unwrap();
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o750
    );
    let child = root.join("private");
    evidence::private_new_dir(&child).unwrap();
    assert_eq!(
        fs::metadata(&child).unwrap().permissions().mode() & 0o777,
        0o700
    );
}
#[cfg(unix)]
#[tokio::test]
async fn passed_label_without_all_gate_evidence_is_rejected() {
    let (_d, root) = fixture();
    let (_, mut state) = pipeline::load_run(&root).unwrap();
    state.state = "passed".into();
    evidence::atomic(&root.join("state.json"), &state).unwrap();
    assert!(
        pipeline::resume(&root, &Arc::new(AtomicBool::new(false)))
            .await
            .is_err()
    );
    let (_, state) = pipeline::load_run(&root).unwrap();
    assert_eq!(state.state, "blocked");
    assert!(state.attempts.is_empty());
}
#[cfg(unix)]
#[tokio::test]
async fn cancellation_submitted_before_execution_is_not_lost() {
    let (_d, root) = fixture();
    pipeline::request_cancel(&root).unwrap();
    let state = pipeline::resume(&root, &Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    assert_eq!(state.state, "cancelled");
    assert!(state.attempts.is_empty());
}
#[test]
fn plan_changes_cannot_reuse_existing_receipt() {
    let (_d, root) = fixture();
    let (mut plan, _) = pipeline::load_run(&root).unwrap();
    plan.package = "changed".into();
    evidence::atomic(&root.join("plan.json"), &plan).unwrap();
    assert!(pipeline::load_run(&root).is_err());
}
#[cfg(unix)]
#[tokio::test]
async fn snapshot_inside_its_input_tree_is_rejected_before_toolchain_probe() {
    let (_d, root) = fixture();
    fs::create_dir(root.join("source/crates")).unwrap();
    let result = pipeline::prepare(
        &root.join("source"),
        &root.join("source/crates/run"),
        "fixture",
        "missing-toolchain",
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("run_inside_source_inventory")
    );
}

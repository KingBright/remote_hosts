//! Real native processes and a disposable Cargo project. No Python or shell fixtures.
use remote_hosts_release::{
    evidence::{self, Lease},
    executor::{self, CommandSpec},
    pipeline, source,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn temp() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().canonicalize().unwrap();
    (dir, path)
}
fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rh-release"))
        .canonicalize()
        .unwrap()
}
fn spec(cwd: &Path, args: &[&str], timeout: u64) -> CommandSpec {
    CommandSpec {
        program: binary(),
        args: args.iter().map(|s| (*s).into()).collect(),
        cwd: cwd.into(),
        env: executor::environment(),
        timeout_seconds: timeout,
    }
}
fn project(root: &Path) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(root.join("Cargo.toml"), "[package]\nname = \"native-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n").unwrap();
    fs::write(
        root.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"native-fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();
}
#[test]
fn relative_paths_reject_escapes() {
    for bad in ["", "/etc/passwd", "../x", "a/../../x", "./x"] {
        assert!(evidence::relative(Path::new(bad)).is_err(), "{bad}");
    }
    assert!(evidence::relative(Path::new("a/b.rs")).is_ok());
}
#[test]
fn file_hash_is_content_bound() {
    let (_d, root) = temp();
    let file = root.join("sample");
    fs::write(&file, b"abc").unwrap();
    let blob = evidence::hash_file(&file).unwrap();
    assert_eq!(
        blob.sha256,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(blob.size, 3);
}
#[test]
fn evidence_claim_is_exclusive_and_released() {
    let (_d, root) = temp();
    let file = root.join("run.lock");
    let lease = Lease::acquire(&file).unwrap();
    assert!(Lease::acquire(&file).is_err());
    drop(lease);
    assert!(Lease::acquire(&file).is_ok());
}
#[test]
fn atomic_receipts_replace_without_partial_json() {
    let (_d, root) = temp();
    let file = root.join("state.json");
    evidence::atomic(&file, &vec![1, 2]).unwrap();
    evidence::atomic(&file, &vec![3]).unwrap();
    assert_eq!(evidence::load::<Vec<i32>>(&file).unwrap(), vec![3]);
}
#[test]
fn source_snapshot_is_independent_of_editor_changes() {
    let (_d, root) = temp();
    let original = root.join("original");
    project(&original);
    let saved = source::capture(&original, &root.join("snapshot")).unwrap();
    fs::write(original.join("src/main.rs"), "fn main() { panic!(); }\n").unwrap();
    assert!(source::verify(&root.join("snapshot"), &saved).is_ok());
    assert!(source::verify(&original, &saved).is_err());
}
#[test]
fn source_membership_is_part_of_identity() {
    let (_d, root) = temp();
    project(&root);
    let before = source::inventory(&root).unwrap();
    fs::write(root.join("src/new.rs"), "// added").unwrap();
    assert!(source::verify(&root, &before).is_err());
    fs::remove_file(root.join("src/new.rs")).unwrap();
    assert!(source::verify(&root, &before).is_ok());
    fs::remove_file(root.join("src/main.rs")).unwrap();
    assert!(source::verify(&root, &before).is_err());
}
#[test]
fn build_outputs_and_private_runtime_are_not_inputs() {
    let (_d, root) = temp();
    project(&root);
    let before = source::inventory(&root).unwrap();
    fs::create_dir(root.join("target")).unwrap();
    fs::write(root.join("target/cache"), "ignored").unwrap();
    fs::create_dir(root.join("ops")).unwrap();
    fs::write(root.join("ops/secret"), "not a source input").unwrap();
    assert_eq!(source::inventory(&root).unwrap(), before);
}
#[test]
fn locked_source_is_mandatory() {
    let (_d, root) = temp();
    project(&root);
    fs::remove_file(root.join("Cargo.lock")).unwrap();
    assert!(source::inventory(&root).is_err());
}
#[cfg(unix)]
#[test]
fn snapshot_rejects_symlink_and_tracks_executable_mode() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (_d, root) = temp();
    project(&root);
    let saved = source::inventory(&root).unwrap();
    fs::set_permissions(root.join("src/main.rs"), fs::Permissions::from_mode(0o700)).unwrap();
    assert!(source::verify(&root, &saved).is_err());
    symlink("main.rs", root.join("src/link.rs")).unwrap();
    assert!(source::inventory(&root).is_err());
}
#[cfg(unix)]
#[test]
fn receipt_does_not_follow_symlink() {
    use std::os::unix::fs::symlink;
    let (_d, root) = temp();
    fs::write(root.join("real"), "keep").unwrap();
    symlink("real", root.join("link")).unwrap();
    assert!(evidence::atomic(&root.join("link"), &1).is_err());
    assert_eq!(fs::read_to_string(root.join("real")).unwrap(), "keep");
}
#[cfg(unix)]
#[tokio::test]
async fn execution_uses_exit_status_not_success_words() {
    let (_d, root) = temp();
    let cancelled = Arc::new(AtomicBool::new(false));
    let out = executor::execute(
        &spec(
            &root,
            &["probe", "echo", "--text", "failed, but exit is zero"],
            20,
        ),
        &root.join("ok.out"),
        &root.join("ok.err"),
        None,
        &cancelled,
        |_| Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(out.state, "passed");
    assert_eq!(out.exit_code, Some(0));
    assert!(out.cleanup_confirmed);
    let out = executor::execute(
        &spec(
            &root,
            &["probe", "hash", "--path", "passed-but-missing"],
            20,
        ),
        &root.join("bad.out"),
        &root.join("bad.err"),
        None,
        &cancelled,
        |_| Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(out.state, "failed");
    assert_eq!(out.exit_code, Some(1));
}
#[cfg(unix)]
#[tokio::test]
async fn timeout_kills_owned_process_and_preserves_output() {
    let (_d, root) = temp();
    let out = executor::execute(
        &spec(
            &root,
            &[
                "probe",
                "sleep",
                "--ready",
                "ready",
                "--finished",
                "must-not-exist",
            ],
            1,
        ),
        &root.join("stdout"),
        &root.join("stderr"),
        None,
        &Arc::new(AtomicBool::new(false)),
        |_| Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(out.state, "timed_out");
    assert!(out.cleanup_confirmed);
    assert!(!root.join("must-not-exist").exists());
}
#[cfg(unix)]
#[tokio::test]
async fn cancellation_kills_the_owned_child_tree() {
    let (_d, root) = temp();
    let flag = Arc::new(AtomicBool::new(false));
    let setter = flag.clone();
    let ready = root.join("ready");
    let signal = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(20), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        setter.store(true, Ordering::Relaxed);
    });
    let out = executor::execute(
        &spec(
            &root,
            &[
                "probe",
                "parent",
                "--ready",
                "ready",
                "--finished",
                "must-not-exist",
            ],
            30,
        ),
        &root.join("stdout"),
        &root.join("stderr"),
        None,
        &flag,
        |_| Ok(()),
    )
    .await
    .unwrap();
    signal.await.unwrap();
    assert_eq!(out.state, "cancelled");
    assert!(out.cleanup_confirmed);
    assert!(!root.join("must-not-exist").exists());
}
#[cfg(unix)]
#[tokio::test]
async fn log_files_cannot_be_overwritten_by_replay() {
    let (_d, root) = temp();
    fs::write(root.join("stdout"), "evidence").unwrap();
    assert!(
        executor::execute(
            &spec(&root, &["probe", "echo"], 20),
            &root.join("stdout"),
            &root.join("stderr"),
            None,
            &Arc::new(AtomicBool::new(false)),
            |_| Ok(())
        )
        .await
        .is_err()
    );
    assert_eq!(fs::read_to_string(root.join("stdout")).unwrap(), "evidence");
}
#[test]
fn portable_probe_is_binary_exact_and_no_clobber() {
    let (_d, root) = temp();
    let path = root.join("payload");
    assert!(
        std::process::Command::new(binary())
            .args(["probe", "pattern", "--bytes", "65536", "--path"])
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 65536);
    assert!(data.iter().enumerate().all(|(n, v)| *v == (n % 256) as u8));
    assert!(
        !std::process::Command::new(binary())
            .args(["probe", "pattern", "--path"])
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(fs::read(&path).unwrap(), data);
}
fn legacy_package(root: &Path) {
    fs::write(root.join("candidate"), "binary fixture").unwrap();
    let mut checks = serde_json::Map::new();
    for name in ["fmt", "clippy", "rust_tests", "python_tests", "workspace"] {
        checks.insert(
            name.into(),
            serde_json::json!({"state":"finished","exit_code":0}),
        );
    }
    let proof = serde_json::json!({"state":"passed","version":"0.10.4","snapshot_id":"fixture","source_inputs":{},"source_inputs_unchanged":true,"functional_tests":{"passed":1,"failed":0,"test_gates_completed_successfully":true},"checks":checks});
    evidence::atomic(&root.join("source-verification.json"), &proof).unwrap();
    evidence::atomic(&root.join("manifest.json"), &serde_json::json!({"version":"0.10.4","snapshot_id":"fixture","source_inputs":{},"source_verification_sha256":evidence::hash_file(&root.join("source-verification.json")).unwrap().sha256,"artifacts":{"candidate":evidence::hash_file(&root.join("candidate")).unwrap()}})).unwrap();
}
#[test]
fn prior_package_is_verified_without_executing_helpers() {
    let (_d, root) = temp();
    legacy_package(&root);
    let result = pipeline::verify_package(&root).unwrap();
    assert_eq!(result["state"], "verified");
    assert_eq!(result["executed_scripts"], false);
    assert_eq!(result["deployed"], false);
}
#[test]
fn legacy_package_with_no_executed_tests_is_rejected() {
    let (_d, root) = temp();
    legacy_package(&root);
    let mut proof: serde_json::Value =
        evidence::load(&root.join("source-verification.json")).unwrap();
    proof["functional_tests"]["passed"] = serde_json::json!(0);
    evidence::atomic(&root.join("source-verification.json"), &proof).unwrap();
    let mut manifest: serde_json::Value = evidence::load(&root.join("manifest.json")).unwrap();
    manifest["source_verification_sha256"] = serde_json::json!(
        evidence::hash_file(&root.join("source-verification.json"))
            .unwrap()
            .sha256
    );
    evidence::atomic(&root.join("manifest.json"), &manifest).unwrap();
    assert!(pipeline::verify_package(&root).is_err());
}

#[test]
fn corrupt_package_is_rejected() {
    let (_d, root) = temp();
    legacy_package(&root);
    fs::write(root.join("candidate"), "changed").unwrap();
    assert!(pipeline::verify_package(&root).is_err());
}
#[test]
fn incomplete_legacy_evidence_is_rejected_even_with_a_matching_hash() {
    let (_d, root) = temp();
    legacy_package(&root);
    let mut proof: serde_json::Value =
        evidence::load(&root.join("source-verification.json")).unwrap();
    proof["checks"]["rust_tests"]["exit_code"] = serde_json::json!(101);
    evidence::atomic(&root.join("source-verification.json"), &proof).unwrap();
    let mut manifest: serde_json::Value = evidence::load(&root.join("manifest.json")).unwrap();
    manifest["source_verification_sha256"] = serde_json::json!(
        evidence::hash_file(&root.join("source-verification.json"))
            .unwrap()
            .sha256
    );
    evidence::atomic(&root.join("manifest.json"), &manifest).unwrap();
    assert!(pipeline::verify_package(&root).is_err());
}
#[cfg(unix)]
#[tokio::test]
async fn real_cargo_failure_resume_reuses_passed_gates_and_binds_artifact() {
    let (_d, root) = temp();
    let original = root.join("project");
    project(&original);
    fs::write(
        original.join("src/main.rs"),
        r#"fn main() {
    println!("native-release-ok");
}

#[cfg(test)]
mod tests {
    #[test]
    fn external_fixture_gate() {
        let run = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        assert!(
            run.join("allow-test").exists(),
            "intentional fixture failure"
        );
    }
}
"#,
    )
    .unwrap();
    let run = root.join("run");
    let cancel = Arc::new(AtomicBool::new(false));
    let pinned = std::env::var("RH_TEST_TOOLCHAIN").unwrap_or_else(|_| "1.94.1".into());
    let prepared = pipeline::prepare(&original, &run, "native-fixture", &pinned)
        .await
        .unwrap();
    let failed = pipeline::resume(&run, &cancel).await.unwrap();
    assert_eq!(
        failed.state,
        "failed",
        "{}",
        serde_json::to_string(&failed).unwrap()
    );
    let diagnostics = failed
        .attempts
        .values()
        .flat_map(|attempts| attempts.iter())
        .map(|attempt| {
            format!(
                "{}\n{}",
                fs::read_to_string(run.join(&attempt.stdout)).unwrap_or_default(),
                fs::read_to_string(run.join(&attempt.stderr)).unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(failed.phase.as_deref(), Some("tests"), "{diagnostics}");
    assert_eq!(failed.attempts["format"].len(), 1);
    assert_eq!(failed.attempts["clippy"].len(), 1);
    assert!(!failed.attempts.contains_key("build"));
    fs::write(run.join("allow-test"), "fixture environment repaired").unwrap();
    // Editor changes do not contaminate this already frozen run.
    fs::write(original.join("src/main.rs"), "not valid Rust").unwrap();
    let passed = pipeline::resume(&run, &cancel).await.unwrap();
    assert_eq!(
        passed.state,
        "passed",
        "{}",
        serde_json::to_string(&passed).unwrap()
    );
    assert_eq!(passed.plan_id, prepared.plan_id);
    assert_eq!(passed.attempts["format"].len(), 1);
    assert_eq!(passed.attempts["clippy"].len(), 1);
    assert_eq!(passed.attempts["tests"].len(), 2);
    assert_eq!(passed.attempts["tests"][0].state, "failed");
    let artifact = &passed.attempts["build"][0].artifacts[0];
    let output = std::process::Command::new(run.join(&artifact.path))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "native-release-ok"
    );
    let replay = pipeline::resume(&run, &cancel).await.unwrap();
    assert_eq!(replay.attempts["tests"].len(), 2);
    fs::write(run.join(&artifact.path), "tampered").unwrap();
    assert!(pipeline::resume(&run, &cancel).await.is_err());
    let (_, blocked) = pipeline::load_run(&run).unwrap();
    assert_eq!(blocked.state, "blocked");
    assert_eq!(blocked.attempts["build"].len(), 1);
}

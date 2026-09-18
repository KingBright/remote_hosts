//! One immutable plan and one authoritative receipt per run. Only native build operations.
use crate::{
    evidence::{self, Blob, Lease},
    executor::{self, Cancel, CommandSpec, Outcome},
    source::{self, Inventory},
    toolchain::{self, Toolchain},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Gate {
    Format,
    Clippy,
    Tests,
    Build,
}
impl Gate {
    fn name(&self) -> &'static str {
        match self {
            Self::Format => "format",
            Self::Clippy => "clippy",
            Self::Tests => "tests",
            Self::Build => "build",
        }
    }
    fn command(&self, package: &str) -> Vec<String> {
        let mut args: Vec<String> = match self {
            Self::Format => vec!["fmt", "-p", package, "--", "--check"],
            Self::Clippy => vec![
                "clippy",
                "-p",
                package,
                "--all-targets",
                "--frozen",
                "--",
                "-D",
                "warnings",
            ],
            Self::Tests => vec![
                "test",
                "-p",
                package,
                "--frozen",
                "--no-fail-fast",
                "--",
                "--test-threads=1",
                "--color",
                "never",
            ],
            Self::Build => vec![
                "build",
                "-p",
                package,
                "--release",
                "--frozen",
                "--message-format=json",
            ],
        }
        .into_iter()
        .map(str::to_owned)
        .collect();
        args.shrink_to_fit();
        args
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: u32,
    pub source_id: String,
    pub inputs: Inventory,
    pub package: String,
    pub toolchain: Toolchain,
    pub gate_budgets: [u64; 4],
    pub gates: Vec<Gate>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub path: String,
    pub blob: Blob,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub number: u32,
    pub state: String,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub process_group: Option<u32>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_blob: Option<Blob>,
    pub stderr_blob: Option<Blob>,
    pub outcome: Option<Outcome>,
    #[serde(default)]
    pub command: Option<CommandSpec>,
    #[serde(default)]
    pub test_evidence: Option<crate::test_evidence::Tests>,
    pub artifacts: Vec<Artifact>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub schema: u32,
    pub plan_id: String,
    pub source_id: String,
    pub state: String,
    pub phase: Option<String>,
    pub updated_ms: u64,
    pub attempts: BTreeMap<String, Vec<Attempt>>,
    pub last_error: Option<String>,
    pub deployed: bool,
}
pub fn load_run(run: &Path) -> Result<(Plan, Receipt)> {
    let plan: Plan = evidence::load(&run.join("plan.json"))?;
    let receipt: Receipt = evidence::load(&run.join("state.json"))?;
    ensure!(
        plan.schema == 1 && receipt.schema == 1 && !receipt.deployed,
        "invalid_build_protocol"
    );
    ensure!(
        evidence::identity(&plan)? == receipt.plan_id
            && evidence::identity(&plan.inputs)? == plan.source_id
            && receipt.source_id == plan.source_id,
        "plan_identity_changed"
    );
    ensure!(
        plan.gates == vec![Gate::Format, Gate::Clippy, Gate::Tests, Gate::Build],
        "invalid_gate_contract"
    );
    ensure!(
        plan.gate_budgets.iter().all(|n| (1..=7200).contains(n)),
        "invalid_gate_budget"
    );
    Ok((plan, receipt))
}
pub async fn prepare(source: &Path, run: &Path, package: &str, toolchain: &str) -> Result<Receipt> {
    ensure!(
        !package.is_empty()
            && package
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
        "invalid_package_name"
    );
    let source = source.canonicalize()?;
    ensure!(
        !run.exists(),
        "run_already_exists: use resume, not a new execution"
    );
    let parent = run.parent().context("run_parent_missing")?;
    evidence::private_dir(parent)?;
    let canonical_run = parent
        .canonicalize()?
        .join(run.file_name().context("run_name_missing")?);
    ensure!(
        !source.starts_with(&canonical_run),
        "run_must_not_contain_source"
    );
    ensure!(
        !canonical_run.starts_with(&source) || canonical_run.starts_with(source.join("target")),
        "run_inside_source_inventory: use target/ or a directory outside the source"
    );
    // Environment failures happen before snapshot copying or expensive Cargo work.
    let tools = toolchain::doctor(toolchain, &parent.canonicalize()?).await?;
    evidence::private_new_dir(&canonical_run)?;
    let inputs = source::capture(&source, &canonical_run.join("source"))?;
    let source_id = evidence::identity(&inputs)?;
    let plan = Plan {
        schema: 1,
        source_id: source_id.clone(),
        inputs,
        package: package.into(),
        toolchain: tools,
        gate_budgets: [300, 1800, 3600, 7200],
        gates: vec![Gate::Format, Gate::Clippy, Gate::Tests, Gate::Build],
    };
    evidence::atomic(&canonical_run.join("plan.json"), &plan)?;
    let receipt = Receipt {
        schema: 1,
        plan_id: evidence::identity(&plan)?,
        source_id,
        state: "prepared".into(),
        phase: None,
        updated_ms: evidence::now_ms(),
        attempts: BTreeMap::new(),
        last_error: None,
        deployed: false,
    };
    evidence::atomic(&canonical_run.join("state.json"), &receipt)?;
    Ok(receipt)
}
fn update(run: &Path, receipt: &mut Receipt) -> Result<()> {
    receipt.updated_ms = evidence::now_ms();
    evidence::atomic(&run.join("state.json"), receipt)
}
fn verify_attempt(run: &Path, attempt: &Attempt) -> Result<()> {
    for (name, expected) in [
        (&attempt.stdout, &attempt.stdout_blob),
        (&attempt.stderr, &attempt.stderr_blob),
    ] {
        evidence::relative(Path::new(name))?;
        evidence::no_symlinks(&run.join(name))?;
        ensure!(
            Some(evidence::hash_file(&run.join(name))?) == *expected,
            "completed_log_evidence_changed: {name}"
        );
    }
    for artifact in &attempt.artifacts {
        evidence::relative(Path::new(&artifact.path))?;
        evidence::no_symlinks(&run.join(&artifact.path))?;
        ensure!(
            evidence::hash_file(&run.join(&artifact.path))? == artifact.blob,
            "completed_artifact_changed: {}",
            artifact.path
        );
    }
    ensure!(
        attempt
            .outcome
            .as_ref()
            .is_some_and(|o| o.state == "passed" && o.exit_code == Some(0) && o.cleanup_confirmed),
        "incomplete_step_evidence"
    );
    Ok(())
}
fn archive_artifacts(run: &Path, out: &Path, attempt: u32) -> Result<Vec<Artifact>> {
    let cache = run.join("cargo-target").canonicalize()?;
    let mut produced = Vec::new();
    let mut build_finished = false;
    for line in BufReader::new(fs::File::open(out)?).lines() {
        let line = line?;
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
            if json["reason"] == "build-finished" {
                build_finished = json["success"] == true;
            }
            if json["reason"] == "compiler-artifact"
                && json["target"]["kind"]
                    .as_array()
                    .is_some_and(|k| k.iter().any(|v| v == "bin"))
                && let Some(path) = json["executable"].as_str()
            {
                produced.push(PathBuf::from(path));
            }
        }
    }
    ensure!(build_finished, "cargo_build_finished_evidence_missing");
    produced.sort();
    produced.dedup();
    let mut artifacts = Vec::new();
    for path in produced {
        evidence::no_symlinks(&path)?;
        let path = path.canonicalize()?;
        ensure!(
            path.starts_with(&cache),
            "cargo_artifact_outside_owned_target"
        );
        let name = path
            .file_name()
            .context("artifact_name_missing")?
            .to_str()
            .context("artifact_non_utf8")?;
        let relative = format!("artifacts/build-{attempt}/{name}");
        let destination = run.join(&relative);
        evidence::private_dir(destination.parent().unwrap())?;
        let expected = evidence::hash_file(&path)?;
        let mut original = fs::File::open(&path)?;
        let mut output = evidence::log_file(&destination)?;
        std::io::copy(&mut original, &mut output)?;
        output.sync_all()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&destination, fs::Permissions::from_mode(0o700))?;
        }
        ensure!(
            evidence::hash_file(&destination)? == expected,
            "artifact_changed_during_archive"
        );
        artifacts.push(Artifact {
            path: relative,
            blob: expected,
        });
    }
    Ok(artifacts)
}
pub fn request_cancel(run: &Path) -> Result<()> {
    let (_, state) = load_run(run)?;
    ensure!(state.state != "passed", "completed_run_is_not_cancellable");
    // Cancellation is cooperative, scoped to this run. Never kill a PID taken from an old receipt.
    if !run.join("cancel.request").exists() {
        evidence::atomic(
            &run.join("cancel.request"),
            &serde_json::json!({"plan_id":state.plan_id}),
        )?;
    }
    Ok(())
}
pub async fn resume(run: &Path, cancel: &Cancel) -> Result<Receipt> {
    let run = run.canonicalize()?;
    evidence::no_symlinks(&run)?;
    let _lease = Lease::acquire(&run.join("run.lock"))?;
    let (plan, mut receipt) = load_run(&run)?;
    let result = execute_plan(&run, &plan, &mut receipt, cancel).await;
    if let Err(error) = &result {
        receipt.state = "blocked".into();
        receipt.last_error = Some(format!("{error:#}").chars().take(1000).collect());
        update(&run, &mut receipt)?;
    }
    result?;
    Ok(receipt)
}
async fn execute_plan(
    run: &Path,
    plan: &Plan,
    receipt: &mut Receipt,
    cancel: &Cancel,
) -> Result<()> {
    source::verify(&run.join("source"), &plan.inputs)?;
    plan.toolchain.check_files()?;
    for (name, attempts) in &receipt.attempts {
        if let Some(last) = attempts.last() {
            if last.state == "passed" {
                verify_attempt(run, last)?;
                let gate = plan
                    .gates
                    .iter()
                    .find(|gate| gate.name() == name)
                    .context("unknown_recorded_gate")?;
                let command = last
                    .command
                    .as_ref()
                    .context("legacy_step_command_not_recorded")?;
                let mut expected_env = plan.toolchain.env.clone();
                expected_env.insert(
                    "CARGO_TARGET_DIR".into(),
                    run.join("cargo-target").display().to_string(),
                );
                expected_env.insert("CARGO_BUILD_JOBS".into(), "2".into());
                ensure!(
                    command.env == expected_env
                        && command.program == plan.toolchain.programs["cargo"]
                        && command.args == gate.command(&plan.package)
                        && command.cwd == run.join("source")
                        && command.timeout_seconds
                            == plan.gate_budgets
                                [plan.gates.iter().position(|g| g == gate).unwrap()],
                    "step_command_identity_changed"
                );
                if *gate == Gate::Tests {
                    let evidence = crate::test_evidence::from_logs(
                        &run.join(&last.stdout),
                        &run.join(&last.stderr),
                        true,
                        true,
                    )?;
                    ensure!(
                        evidence.evidence_complete
                            && last.test_evidence.as_ref() == Some(&evidence),
                        "completed_test_evidence_changed"
                    );
                }
            }
            if last.state == "running" || last.state == "cleanup_unconfirmed" {
                let pgid = last.process_group.context("interrupted_spawn_outcome_unknown: inspect the original attempt; never guess ownership")?;
                ensure!(
                    !executor::group_alive(pgid)?,
                    "previous_child_group_active: {pgid}; observe original work; no replay or PID kill"
                );
            }
        }
    }
    if receipt.state == "passed" {
        ensure!(
            plan.gates.iter().all(|gate| receipt
                .attempts
                .get(gate.name())
                .and_then(|a| a.last())
                .is_some_and(|a| a.state == "passed")),
            "incomplete_success_receipt"
        );
        return Ok(());
    }
    executor::raise_file_limit()?;
    if run.join("cancel.request").exists() {
        if receipt.state == "cancelled" {
            fs::remove_file(run.join("cancel.request"))?;
        } else {
            receipt.state = "cancelled".into();
            update(run, receipt)?;
            return Ok(());
        }
    }
    receipt.last_error = None;
    evidence::private_dir(&run.join("logs"))?;
    evidence::private_dir(&run.join("cargo-target"))?;
    for (index, gate) in plan.gates.iter().enumerate() {
        let name = gate.name();
        if receipt
            .attempts
            .get(name)
            .and_then(|a| a.last())
            .is_some_and(|a| a.state == "passed")
        {
            continue;
        }
        if cancel.load(Ordering::Relaxed) {
            receipt.state = "cancelled".into();
            update(run, receipt)?;
            return Ok(());
        }
        source::verify(&run.join("source"), &plan.inputs)?;
        let attempts = receipt.attempts.entry(name.into()).or_default();
        if let Some(last) = attempts.last_mut()
            && last.state == "running"
        {
            last.state = "interrupted".into();
            last.finished_ms = Some(evidence::now_ms());
        }
        ensure!(attempts.len() < 100, "attempt_budget_exceeded");
        let number = attempts.len() as u32 + 1;
        let stdout = format!("logs/{name}-{number}.stdout");
        let stderr = format!("logs/{name}-{number}.stderr");
        attempts.push(Attempt {
            number,
            state: "running".into(),
            started_ms: evidence::now_ms(),
            finished_ms: None,
            process_group: None,
            stdout: stdout.clone(),
            stderr: stderr.clone(),
            stdout_blob: None,
            stderr_blob: None,
            outcome: None,
            command: None,
            test_evidence: None,
            artifacts: Vec::new(),
        });
        receipt.state = "running".into();
        receipt.phase = Some(name.into());
        update(run, receipt)?;
        let mut env = plan.toolchain.env.clone();
        env.insert(
            "CARGO_TARGET_DIR".into(),
            run.join("cargo-target").display().to_string(),
        );
        env.insert("CARGO_BUILD_JOBS".into(), "2".into());
        let spec = CommandSpec {
            program: plan.toolchain.programs["cargo"].clone(),
            args: gate.command(&plan.package),
            cwd: run.join("source"),
            env,
            timeout_seconds: plan.gate_budgets[index],
        };
        receipt
            .attempts
            .get_mut(name)
            .unwrap()
            .last_mut()
            .unwrap()
            .command = Some(spec.clone());
        update(run, receipt)?;
        println!(
            "{}",
            serde_json::json!({"schema":1,"event":"step_started","step":name,"attempt":number,"run":run})
        );
        let outcome = executor::execute(
            &spec,
            &run.join(&stdout),
            &run.join(&stderr),
            Some(&run.join("cancel.request")),
            cancel,
            |pid| {
                receipt
                    .attempts
                    .get_mut(name)
                    .unwrap()
                    .last_mut()
                    .unwrap()
                    .process_group = Some(pid);
                update(run, receipt)
            },
        )
        .await;
        let attempt = receipt.attempts.get_mut(name).unwrap().last_mut().unwrap();
        attempt.finished_ms = Some(evidence::now_ms());
        attempt.stdout_blob = evidence::hash_file(&run.join(&stdout)).ok();
        attempt.stderr_blob = evidence::hash_file(&run.join(&stderr)).ok();
        match outcome {
            Ok(outcome) => {
                attempt.state = outcome.state.clone();
                attempt.outcome = Some(outcome);
                receipt.state = attempt.state.clone();
            }
            Err(error) => {
                // An unrecorded spawn remains uncertain; no automatic replay after a write failure.
                attempt.state = if attempt.process_group.is_none() {
                    "spawn_failed"
                } else {
                    "cleanup_unconfirmed"
                }
                .into();
                receipt.state = "failed".into();
                receipt.last_error = Some(error.to_string());
            }
        }
        if let Err(error) = source::verify(&run.join("source"), &plan.inputs) {
            attempt.state = "invalid_source".into();
            receipt.state = "blocked".into();
            receipt.last_error = Some(error.to_string());
        }
        if *gate == Gate::Tests {
            let complete = attempt.stdout_blob.is_some()
                && attempt.stderr_blob.is_some()
                && attempt
                    .outcome
                    .as_ref()
                    .is_some_and(|o| o.cleanup_confirmed);
            match crate::test_evidence::from_logs(
                &run.join(&stdout),
                &run.join(&stderr),
                complete,
                attempt.state == "passed",
            ) {
                Ok(evidence) => {
                    let valid = evidence.evidence_complete;
                    attempt.test_evidence = Some(evidence);
                    if attempt.state == "passed" && !valid {
                        attempt.state = "incomplete_test_evidence".into();
                        receipt.state = "failed".into();
                        receipt.last_error = Some(
                            "No executed passing test evidence, failed cases, or incomplete output"
                                .into(),
                        );
                    }
                }
                Err(error) => {
                    attempt.state = "test_evidence_error".into();
                    receipt.state = "failed".into();
                    receipt.last_error = Some(error.to_string());
                }
            }
        }
        if attempt.state == "passed" && *gate == Gate::Build {
            match archive_artifacts(run, &run.join(&stdout), number) {
                Ok(artifacts) => attempt.artifacts = artifacts,
                Err(error) => {
                    attempt.state = "artifact_error".into();
                    receipt.state = "failed".into();
                    receipt.last_error = Some(error.to_string());
                }
            }
        }
        let passed = attempt.state == "passed";
        // A run is not successful merely because the most recent non-final gate passed.
        if passed {
            receipt.state = "running".into();
        }
        update(run, receipt)?;
        println!(
            "{}",
            serde_json::json!({"schema":1,"event":"step_finished","step":name,"attempt":number,"passed":passed})
        );
        if !passed {
            return Ok(());
        }
    }
    plan.toolchain.check_files()?;
    source::verify(&run.join("source"), &plan.inputs)?;
    receipt.state = "passed".into();
    receipt.phase = None;
    update(run, receipt)?;
    Ok(())
}
/// Verify a previously built release without importing or executing its helper scripts.
pub fn verify_package(package: &Path) -> Result<serde_json::Value> {
    let root = package.canonicalize()?;
    evidence::no_symlinks(&root)?;
    let manifest: serde_json::Value = evidence::load(&root.join("manifest.json"))?;
    let files = manifest["artifacts"]
        .as_object()
        .context("release_artifacts_missing")?;
    ensure!(
        !files.is_empty() && files.len() <= 256,
        "release_artifact_count_invalid"
    );
    for (name, value) in files {
        evidence::relative(Path::new(name))?;
        ensure!(
            Path::new(name).components().count() == 1,
            "release_name_must_be_flat"
        );
        let blob = evidence::hash_file(&root.join(name))?;
        ensure!(
            value["sha256"].as_str() == Some(&blob.sha256)
                && value["size"].as_u64() == Some(blob.size),
            "release_artifact_mismatch: {name}"
        );
    }
    let proof_path = root.join("source-verification.json");
    let proof: serde_json::Value = evidence::load(&proof_path)?;
    ensure!(
        manifest["source_verification_sha256"].as_str()
            == Some(&evidence::hash_file(&proof_path)?.sha256),
        "release_verification_digest_mismatch"
    );
    ensure!(
        proof["state"] == "passed"
            && proof["source_inputs_unchanged"] == true
            && proof["functional_tests"]["failed"] == 0
            && proof["functional_tests"]["test_gates_completed_successfully"] == true
            && proof["functional_tests"]["passed"]
                .as_u64()
                .is_some_and(|n| n > 0),
        "release_verification_incomplete"
    );
    ensure!(
        proof["version"] == manifest["version"]
            && proof["snapshot_id"] == manifest["snapshot_id"]
            && proof["source_inputs"] == manifest["source_inputs"],
        "release_provenance_mismatch"
    );
    for gate in ["fmt", "clippy", "rust_tests", "python_tests", "workspace"] {
        if proof["checks"][gate]["state"] != "finished" || proof["checks"][gate]["exit_code"] != 0 {
            bail!("release_gate_incomplete: {gate}");
        }
    }
    Ok(
        serde_json::json!({"schema":1,"state":"verified","version":manifest["version"],"snapshot_id":manifest["snapshot_id"],"manifest":evidence::hash_file(&root.join("manifest.json"))?,"artifacts":files.len(),"source_tests":proof["functional_tests"],"executed_scripts":false,"deployed":false,"trust":"integrity check only; publisher signature/authentication is separate"}),
    )
}

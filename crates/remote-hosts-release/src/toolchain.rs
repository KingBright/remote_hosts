//! Resolve and pin actual tools, exercise the linker, and check native environment readiness.
use crate::{
    evidence::{self, Blob},
    executor::{self, CommandSpec},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Toolchain {
    pub requested: String,
    pub host: String,
    pub rustc_version: String,
    pub cargo_version: String,
    pub programs: BTreeMap<String, PathBuf>,
    pub fingerprints: BTreeMap<String, Blob>,
    pub env: BTreeMap<String, String>,
}
impl Toolchain {
    pub fn check_files(&self) -> Result<()> {
        for (name, expected) in &self.fingerprints {
            let path = self
                .programs
                .get(name)
                .context("toolchain_program_missing")?;
            ensure!(
                &evidence::hash_file(path)? == expected,
                "toolchain_changed: {name}"
            );
        }
        Ok(())
    }
}
pub async fn doctor(requested: &str, root: &Path) -> Result<Toolchain> {
    ensure!(
        cfg!(unix),
        "native_build_requires_unix; verify-package and probe remain portable"
    );
    ensure!(
        !requested.is_empty()
            && requested
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-')),
        "invalid_toolchain"
    );
    ensure!(
        requested.as_bytes().first().is_some_and(u8::is_ascii_digit),
        "pin_an_exact_toolchain_not_stable_or_nightly"
    );
    let (_, limit) = executor::raise_file_limit()?;
    ensure!(limit >= 2048, "insufficient_file_limit");
    ensure!(
        fs2::available_space(root)? >= 512 * 1024 * 1024,
        "insufficient_build_disk_space"
    );
    let mut env = executor::environment();
    env.insert("RUSTUP_TOOLCHAIN".into(), requested.into());
    let cargo_home = env.get("CARGO_HOME").map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from(env.get("HOME").cloned().unwrap_or_default()).join(".cargo")
    });
    let rustup = cargo_home
        .join("bin/rustup")
        .canonicalize()
        .context("rustup_missing: prepare the build host first")?;
    let mut programs = BTreeMap::new();
    let mut fingerprints = BTreeMap::new();
    for name in ["cargo", "rustc", "rustfmt", "cargo-clippy"] {
        let resolved =
            executor::probe_output(&rustup, &["which", "--toolchain", requested, name], &env)
                .await?;
        let path = PathBuf::from(resolved)
            .canonicalize()
            .with_context(|| format!("missing_toolchain_component: {name}"))?;
        fingerprints.insert(name.into(), evidence::hash_file(&path)?);
        programs.insert(name.into(), path);
    }
    let rustc_version = executor::probe_output(&programs["rustc"], &["-vV"], &env).await?;
    let cargo_version = executor::probe_output(&programs["cargo"], &["-V"], &env).await?;
    ensure!(
        rustc_version
            .lines()
            .any(|line| line == format!("release: {requested}")),
        "effective_toolchain_mismatch"
    );
    let host = rustc_version
        .lines()
        .find_map(|s| s.strip_prefix("host: "))
        .context("toolchain_host_missing")?
        .to_string();
    // Direct cargo invocation must use these resolved tools, not unrelated PATH overrides.
    env.insert("RUSTC".into(), programs["rustc"].display().to_string());
    env.insert("RUSTFMT".into(), programs["rustfmt"].display().to_string());
    let tool_dir = programs["cargo"].parent().unwrap();
    let inherited = env.get("PATH").cloned().unwrap_or_default();
    env.insert("PATH".into(), format!("{}:{inherited}", tool_dir.display()));
    let temp = tempfile::tempdir_in(root)?;
    let cwd = temp.path().canonicalize()?;
    fs::write(
        cwd.join("smoke.rs"),
        "fn main() { println!(\"rh-native-link-ok\"); }\n",
    )?;
    let binary = cwd.join("smoke");
    let result = executor::execute(
        &CommandSpec {
            program: programs["rustc"].clone(),
            args: vec![
                "smoke.rs".into(),
                "--edition=2024".into(),
                "-o".into(),
                binary.display().to_string(),
            ],
            cwd: cwd.clone(),
            env: env.clone(),
            timeout_seconds: 60,
        },
        &cwd.join("compile.out"),
        &cwd.join("compile.err"),
        None,
        &Arc::new(AtomicBool::new(false)),
        |_| Ok(()),
    )
    .await?;
    ensure!(
        result.state == "passed",
        "native_linker_probe_failed: {}",
        fs::read_to_string(cwd.join("compile.err"))?
            .chars()
            .take(500)
            .collect::<String>()
    );
    ensure!(
        executor::probe_output(&binary, &[], &env).await? == "rh-native-link-ok",
        "native_binary_probe_failed"
    );
    Ok(Toolchain {
        requested: requested.into(),
        host,
        rustc_version,
        cargo_version,
        programs,
        fingerprints,
        env,
    })
}

//! Interpreter-free one-shot installation core. Service launch and remote readiness stay outside this narrow primitive.
use crate::evidence;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallRequest {
    pub schema: u32,
    pub version: String,
    pub candidate: PathBuf,
    pub candidate_sha256: String,
    pub target: PathBuf,
    pub backup_dir: PathBuf,
    pub result: PathBuf,
    #[serde(default)]
    pub restart: Option<RestartSpec>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallReceipt {
    pub schema: u32,
    pub state: String,
    pub version: String,
    pub candidate_sha256: String,
    pub previous_sha256: Option<String>,
    pub installed_sha256: Option<String>,
    pub backup: Option<PathBuf>,
    pub service_changed: bool,
    pub rollback: Option<String>,
    pub error_code: Option<String>,
}
fn valid_version(value: &str) -> bool {
    let parts: Vec<_> = value.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn copy_atomic(source: &Path, target: &Path) -> Result<()> {
    let parent = target.parent().context("install_target_parent_missing")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    let mut input = fs::File::open(source)?;
    std::io::copy(&mut input, &mut temp)?;
    temp.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o755))?;
    }
    temp.persist(target).map_err(|e| e.error)?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}
fn write_receipt(path: &Path, value: &InstallReceipt) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    evidence::atomic(path, value)
}
pub fn install(request: &InstallRequest) -> Result<InstallReceipt> {
    ensure!(
        request.schema == 1
            && valid_version(&request.version)
            && valid_hash(&request.candidate_sha256),
        "invalid_install_request"
    );
    ensure!(
        request.candidate.is_absolute()
            && request.target.is_absolute()
            && request.backup_dir.is_absolute()
            && request.result.is_absolute(),
        "absolute_install_paths_required"
    );
    ensure!(
        request.candidate != request.target,
        "candidate_target_alias"
    );
    let actual = evidence::hash_file(&request.candidate)?;
    ensure!(
        actual.sha256 == request.candidate_sha256,
        "candidate_checksum_mismatch"
    );
    let version = Command::new(&request.candidate)
        .arg("--version")
        .output()
        .context("candidate_version_probe_failed")?;
    ensure!(
        version.status.success()
            && String::from_utf8_lossy(&version.stdout).trim()
                == format!("remote-hosts-code {}", request.version),
        "candidate_version_mismatch"
    );
    if request.result.exists() {
        let old: InstallReceipt = evidence::load(&request.result)?;
        ensure!(
            old.version == request.version && old.candidate_sha256 == request.candidate_sha256,
            "install_receipt_identity_conflict"
        );
        return Ok(old);
    }
    fs::create_dir_all(&request.backup_dir)?;
    let previous = request
        .target
        .is_file()
        .then(|| evidence::hash_file(&request.target))
        .transpose()?;
    let backup = previous
        .as_ref()
        .map(|_| request.backup_dir.join("remote-hosts-code.previous"));
    if let Some(path) = &backup {
        ensure!(!path.exists(), "backup_already_exists");
        copy_atomic(&request.target, path)?;
    }
    let mut receipt = InstallReceipt {
        schema: 1,
        state: "installing".into(),
        version: request.version.clone(),
        candidate_sha256: request.candidate_sha256.clone(),
        previous_sha256: previous.as_ref().map(|v| v.sha256.clone()),
        installed_sha256: None,
        backup: backup.clone(),
        service_changed: false,
        rollback: None,
        error_code: None,
    };
    let outcome = (|| -> Result<()> {
        copy_atomic(&request.candidate, &request.target)?;
        receipt.service_changed = true;
        let installed = evidence::hash_file(&request.target)?;
        ensure!(
            installed.sha256 == request.candidate_sha256,
            "installed_checksum_mismatch"
        );
        receipt.installed_sha256 = Some(installed.sha256);
        if let Some(restart) = &request.restart {
            ensure!(
                restart.program.is_absolute(),
                "absolute_restart_program_required"
            );
            let status = Command::new(&restart.program)
                .args(&restart.args)
                .status()
                .context("service_restart_failed")?;
            ensure!(status.success(), "service_restart_failed");
            receipt.state = "restarted_needs_readiness".into();
        } else {
            receipt.state = "installed_needs_restart".into();
        }
        Ok(())
    })();
    if let Err(error) = outcome {
        receipt.state = "failed".into();
        receipt.error_code = Some(
            error
                .to_string()
                .split(':')
                .next()
                .unwrap_or("install_failed")
                .to_string(),
        );
        if receipt.service_changed
            && let Some(path) = &backup
        {
            match copy_atomic(path, &request.target) {
                Ok(()) => receipt.rollback = Some("restored_previous_binary".into()),
                Err(_) => receipt.rollback = Some("rollback_failed".into()),
            }
        }
        write_receipt(&request.result, &receipt)?;
        anyhow::bail!(
            "native_install_failed: {}",
            receipt.error_code.as_deref().unwrap_or("unknown")
        );
    }
    write_receipt(&request.result, &receipt)?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    #[test]
    fn invalid_candidate_is_rejected_without_touching_target() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let root = d.path().canonicalize().unwrap();
        let candidate = root.join("candidate");
        fs::write(&candidate, "not executable\n").unwrap();
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o755)).unwrap();
        let target = root.join("target");
        fs::write(&target, "old").unwrap();
        let req = InstallRequest {
            schema: 1,
            version: "0.10.4".into(),
            candidate: candidate.clone(),
            candidate_sha256: evidence::hash_file(&candidate).unwrap().sha256,
            target: target.clone(),
            backup_dir: root.join("backup"),
            result: root.join("result.json"),
            restart: None,
        };
        assert!(install(&req).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"old");
        assert!(!req.result.exists());
    }
}

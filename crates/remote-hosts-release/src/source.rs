//! Frozen build inputs, including executable modes. Deliberately excludes deployment secrets.
use crate::evidence::{self, Blob};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io::Write, path::Path};
const MAX_FILES: usize = 20000;
const MAX_SOURCE_BYTES: u64 = 512 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub blob: Blob,
    pub executable: bool,
}
pub type Inventory = BTreeMap<String, Input>;
fn executable(meta: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        false
    }
}
fn visit(root: &Path, path: &Path, found: &mut Inventory) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        !meta.file_type().is_symlink(),
        "source_symlink: {}",
        path.display()
    );
    if meta.is_dir() {
        let mut children = fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        children.sort_by_key(|e| e.file_name());
        for child in children {
            if matches!(
                child.file_name().to_str(),
                Some("target" | ".git" | "node_modules" | "__pycache__")
            ) {
                continue;
            }
            visit(root, &child.path(), found)?;
        }
    } else {
        ensure!(meta.is_file(), "source_special_file: {}", path.display());
        ensure!(
            found.len() < MAX_FILES && meta.len() <= MAX_SOURCE_BYTES,
            "source_budget_exceeded"
        );
        let relative = path
            .strip_prefix(root)?
            .to_str()
            .context("non_utf8_source_path")?
            .replace('\\', "/");
        evidence::relative(Path::new(&relative))?;
        found.insert(
            relative,
            Input {
                blob: evidence::hash_file(path)?,
                executable: executable(&meta),
            },
        );
    }
    Ok(())
}
pub fn inventory(root: &Path) -> Result<Inventory> {
    let mut found = Inventory::new();
    // These are repository inputs, not an implicit copy of HOME or a live deployment directory.
    for name in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain",
        "rust-toolchain.toml",
        "build.rs",
        "rustfmt.toml",
        ".rustfmt.toml",
        "clippy.toml",
        ".clippy.toml",
        "crates",
        "src",
        "tests",
        "examples",
        "benches",
        ".cargo",
        "migrations",
        "fixtures",
        "assets",
        "skills",
        "scripts",
        "README.md",
        "README_EN.md",
        ".sqlx",
    ] {
        let path = root.join(name);
        if fs::symlink_metadata(&path).is_ok() {
            visit(root, &path, &mut found)?;
        }
    }
    ensure!(
        found.contains_key("Cargo.toml") && found.contains_key("Cargo.lock"),
        "locked_cargo_source_required"
    );
    ensure!(
        found.values().map(|v| v.blob.size).sum::<u64>() <= MAX_SOURCE_BYTES,
        "source_budget_exceeded"
    );
    Ok(found)
}
pub fn capture(root: &Path, destination: &Path) -> Result<Inventory> {
    ensure!(!destination.exists(), "snapshot_already_exists");
    let expected = inventory(root)?;
    evidence::private_dir(destination)?;
    for (name, input) in &expected {
        let dest = destination.join(name);
        fs::create_dir_all(dest.parent().unwrap())?;
        let mut output = evidence::log_file(&dest)?;
        let mut source = fs::File::open(root.join(name))?;
        std::io::copy(&mut source, &mut output)?;
        output.flush()?;
        output.sync_all()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                &dest,
                fs::Permissions::from_mode(if input.executable { 0o700 } else { 0o600 }),
            )?;
        }
    }
    ensure!(
        inventory(root)? == expected && inventory(destination)? == expected,
        "source_changed_during_capture"
    );
    Ok(expected)
}
pub fn verify(root: &Path, expected: &Inventory) -> Result<()> {
    ensure!(
        &inventory(root)? == expected,
        "snapshot_changed: do not reuse verification evidence"
    );
    Ok(())
}

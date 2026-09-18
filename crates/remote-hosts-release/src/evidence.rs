//! Bounded hashing, private atomic receipts, and nonblocking ownership locks.
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path},
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_JSON: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Blob {
    pub sha256: String,
    pub size: u64,
}
pub fn hash_file(path: &Path) -> Result<Blob> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink(),
        "not_regular_file: {}",
        path.display()
    );
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut size = 0;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        sha.update(&buffer[..n]);
        size += n as u64;
    }
    ensure!(
        size == meta.len(),
        "file_changed_during_hash: {}",
        path.display()
    );
    Ok(Blob {
        sha256: format!("{:x}", sha.finalize()),
        size,
    })
}
pub fn identity<T: Serialize>(value: &T) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn relative(path: &Path) -> Result<()> {
    ensure!(
        !path.as_os_str().is_empty()
            && path.components().all(|c| matches!(c, Component::Normal(_))),
        "invalid_relative_path: {}",
        path.display()
    );
    Ok(())
}
pub fn no_symlinks(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(m) => ensure!(
                !m.file_type().is_symlink(),
                "symlink_path: {}",
                ancestor.display()
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub fn private_dir(path: &Path) -> Result<()> {
    no_symlinks(path)?;
    // Do not chmod an existing user workspace just to create a run below it.
    if !path.exists() {
        fs::create_dir_all(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
    }
    ensure!(path.is_dir(), "expected_directory");
    Ok(())
}
pub fn private_new_dir(path: &Path) -> Result<()> {
    no_symlinks(path)?;
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    Ok(())
}
pub fn load<T: DeserializeOwned>(path: &Path) -> Result<T> {
    no_symlinks(path)?;
    ensure!(
        fs::metadata(path)?.len() <= MAX_JSON,
        "receipt_budget_exceeded"
    );
    Ok(serde_json::from_reader(File::open(path)?)?)
}
pub fn atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    no_symlinks(path)?;
    let parent = path.parent().context("receipt_parent_missing")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, value)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub fn log_file(path: &Path) -> Result<File> {
    no_symlinks(path)?;
    let mut opts = OpenOptions::new();
    opts.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    Ok(opts.open(path)?)
}
pub struct Lease(File);
impl Lease {
    pub fn acquire(path: &Path) -> Result<Self> {
        no_symlinks(path)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        file.try_lock_exclusive()
            .context("run_busy: observe the existing run, do not duplicate it")?;
        Ok(Self(file))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

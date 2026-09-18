//! Root-store validation and fd-relative file operations; reject symlinks and hardlinks.
use crate::protocol::{FileState, Record, valid_id};
use anyhow::{Context, Result, ensure};
use nix::{
    fcntl::{OFlag, open, openat},
    sys::stat::Mode,
    unistd::{UnlinkatFlags, unlinkat},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
};

const MAX_ARTIFACT: u64 = 256 * 1024 * 1024;
const MAX_RECORD: u64 = 256 * 1024;

/// All ancestors must already exist, be directories, root-owned and not writable by others.
pub fn trusted_dir(path: &Path, owner: u32) -> Result<()> {
    ensure!(path.is_absolute(), "trusted_path_not_absolute");
    let mut part = PathBuf::from("/");
    for c in path.components() {
        match c {
            Component::RootDir => (),
            Component::Normal(name) => part.push(name),
            _ => anyhow::bail!("unsafe_path_component"),
        }
        let meta = fs::symlink_metadata(&part)?;
        ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "untrusted_directory_type: {}",
            part.display()
        );
        ensure!(
            meta.uid() == owner && meta.mode() & 0o022 == 0,
            "untrusted_directory_permissions: {}",
            part.display()
        );
    }
    Ok(())
}
pub fn private_read(path: &Path, owner: u32) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    let m = file.metadata()?;
    ensure!(
        m.is_file() && m.nlink() == 1 && m.uid() == owner && m.mode() & 0o077 == 0,
        "untrusted_private_file"
    );
    ensure!(m.len() <= MAX_RECORD, "private_file_too_large");
    let mut data = Vec::new();
    file.take(MAX_RECORD + 1).read_to_end(&mut data)?;
    ensure!(
        data.len() as u64 <= MAX_RECORD,
        "private_file_grew_too_large"
    );
    Ok(data)
}
pub fn atomic_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("missing_parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    serde_json::to_writer(&mut tmp, value)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[derive(Clone)]
pub struct Store {
    pub dir: PathBuf,
    pub owner: u32,
}
impl Store {
    pub fn path(&self, id: &str) -> Result<PathBuf> {
        valid_id(id)?;
        Ok(self.dir.join(format!("{id}.json")))
    }
    pub fn load(&self, id: &str) -> Result<Option<Record>> {
        let path = self.path(id)?;
        match private_read(&path, self.owner) {
            Ok(data) => Ok(Some(serde_json::from_slice(&data)?)),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
    pub fn save(&self, record: &Record) -> Result<()> {
        atomic_json(&self.path(&record.plan.request_id)?, record)
    }
    pub fn revoked(&self) -> Result<bool> {
        match private_read(&self.dir.join("revoked.json"), self.owner) {
            Ok(_) => Ok(true),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
    pub fn revoke(&self, uid: u32, now: u64) -> Result<()> {
        atomic_json(
            &self.dir.join("revoked.json"),
            &serde_json::json!({"uid":uid,"revoked_at":now}),
        )
    }
    pub fn capacity(&self) -> Result<()> {
        ensure!(
            fs::read_dir(&self.dir)?.take(2049).count() < 2048,
            "receipt_store_full_archive_with_admin_authorization"
        );
        Ok(())
    }
}

/// Pin each directory with O_NOFOLLOW; /etc and home symlinks are never silently followed.
fn parent_fd(path: &Path) -> Result<(File, String)> {
    ensure!(path.is_absolute(), "artifact_path_not_absolute");
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .context("bad_artifact_name")?
        .to_owned();
    let mut dir = File::from(open(
        "/",
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    for c in path.parent().context("missing_parent")?.components() {
        match c {
            Component::RootDir => (),
            Component::Normal(name) => {
                dir = File::from(openat(
                    &dir,
                    name,
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                    Mode::empty(),
                )?);
            }
            _ => anyhow::bail!("unsafe_artifact_component"),
        }
    }
    Ok((dir, filename))
}
fn opened(path: &Path) -> Result<(File, String, File)> {
    let (dir, name) = parent_fd(path)?;
    let file = File::from(openat(
        &dir,
        name.as_str(),
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    Ok((dir, name, file))
}
fn state(path: &str, file: &mut File, allowed_uid: u32) -> Result<FileState> {
    let before = file.metadata()?;
    ensure!(
        before.is_file() && before.nlink() == 1,
        "artifact_not_single_link_regular_file"
    );
    ensure!(
        before.uid() == 0 || before.uid() == allowed_uid,
        "artifact_owned_by_another_user"
    );
    ensure!(before.len() <= MAX_ARTIFACT, "artifact_too_large");
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        ensure!(total <= MAX_ARTIFACT, "artifact_grew_too_large");
        hash.update(&buf[..n]);
    }
    let after = file.metadata()?;
    ensure!(
        total == before.len()
            && before.len() == after.len()
            && before.mtime() == after.mtime()
            && before.mtime_nsec() == after.mtime_nsec()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec(),
        "artifact_changed_while_hashing"
    );
    Ok(FileState {
        path: path.into(),
        sha256: format!("{:x}", hash.finalize()),
        size: total,
        device: before.dev(),
        inode: before.ino(),
        uid: before.uid(),
        mode: before.mode(),
    })
}
pub fn inspect_file(path: &str, uid: u32) -> Result<Option<FileState>> {
    match opened(Path::new(path)) {
        Ok((_, _, mut file)) => Ok(Some(state(path, &mut file, uid)?)),
        Err(e) if e.downcast_ref::<nix::errno::Errno>() == Some(&nix::errno::Errno::ENOENT) => {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}
/// Preserve exact bytes in the private root store, then unlink only the pinned basename.
/// No recursive deletion, shell, wildcard or caller-controlled destination is used.
pub fn quarantine(expected: &FileState, uid: u32, backup: &Path) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    let (dir, name, mut file) = opened(Path::new(&expected.path))?;
    ensure!(
        state(&expected.path, &mut file, uid)? == *expected,
        "artifact_changed_since_plan"
    );
    file.seek(SeekFrom::Start(0))?;
    let mut copy = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(backup)?;
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        count += n as u64;
        ensure!(count <= MAX_ARTIFACT, "artifact_grew_during_backup");
        copy.write_all(&buf[..n])?;
        hash.update(&buf[..n]);
    }
    copy.sync_all()?;
    File::open(backup.parent().context("missing_backup_parent")?)?.sync_all()?;
    ensure!(
        count == expected.size && format!("{:x}", hash.finalize()) == expected.sha256,
        "backup_hash_mismatch_source_not_removed"
    );
    let current = File::from(openat(
        &dir,
        name.as_str(),
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let meta = current.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.dev() == expected.device
            && meta.ino() == expected.inode,
        "artifact_replaced_before_unlink"
    );
    unlinkat(&dir, name.as_str(), UnlinkatFlags::NoRemoveDir)?;
    dir.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uid() -> u32 {
        nix::unistd::geteuid().as_raw()
    }
    // Canonicalize only the test fixture (macOS TMPDIR contains /var -> /private/var).
    fn temp() -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().canonicalize().unwrap();
        (d, p)
    }
    #[test]
    fn quarantine_keeps_exact_bytes() {
        let (_d, p) = temp();
        let f = p.join("legacy");
        let b = p.join("backup");
        fs::write(&f, b"exact bytes\x00\xff").unwrap();
        let s = inspect_file(f.to_str().unwrap(), uid()).unwrap().unwrap();
        quarantine(&s, uid(), &b).unwrap();
        assert!(!f.exists());
        assert_eq!(fs::read(b).unwrap(), b"exact bytes\x00\xff");
    }
    #[test]
    fn symlink_file_is_rejected() {
        let (_d, p) = temp();
        let victim = p.join("victim");
        let link = p.join("link");
        fs::write(&victim, b"keep").unwrap();
        std::os::unix::fs::symlink(&victim, &link).unwrap();
        assert!(inspect_file(link.to_str().unwrap(), uid()).is_err());
        assert_eq!(fs::read(victim).unwrap(), b"keep");
    }
    #[test]
    fn symlink_parent_is_rejected() {
        let (_d, p) = temp();
        let real = p.join("real");
        fs::create_dir(&real).unwrap();
        fs::write(real.join("file"), b"keep").unwrap();
        std::os::unix::fs::symlink(&real, p.join("alias")).unwrap();
        assert!(inspect_file(p.join("alias/file").to_str().unwrap(), uid()).is_err());
    }
    #[test]
    fn hardlink_is_rejected() {
        let (_d, p) = temp();
        let f = p.join("file");
        fs::write(&f, b"keep").unwrap();
        fs::hard_link(&f, p.join("hard")).unwrap();
        assert!(inspect_file(f.to_str().unwrap(), uid()).is_err());
    }
    #[test]
    fn replaced_artifact_is_not_removed() {
        let (_d, p) = temp();
        let f = p.join("file");
        fs::write(&f, b"first").unwrap();
        let s = inspect_file(f.to_str().unwrap(), uid()).unwrap().unwrap();
        fs::write(&f, b"new-content").unwrap();
        assert!(quarantine(&s, uid(), &p.join("backup")).is_err());
        assert_eq!(fs::read(f).unwrap(), b"new-content");
    }
    #[test]
    fn missing_artifact_is_noop() {
        let (_d, p) = temp();
        assert!(
            inspect_file(p.join("absent/file").to_str().unwrap(), uid())
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn backup_refuses_overwrite() {
        let (_d, p) = temp();
        let f = p.join("file");
        let b = p.join("backup");
        fs::write(&f, b"source").unwrap();
        fs::write(&b, b"old").unwrap();
        let s = inspect_file(f.to_str().unwrap(), uid()).unwrap().unwrap();
        assert!(quarantine(&s, uid(), &b).is_err());
        assert!(f.exists());
        assert_eq!(fs::read(b).unwrap(), b"old");
    }
    #[test]
    fn private_read_rejects_world_readable() {
        let (_d, p) = temp();
        let f = p.join("policy");
        fs::write(&f, b"{}").unwrap();
        fs::set_permissions(&f, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(private_read(&f, uid()).is_err());
    }
    #[test]
    fn store_path_rejects_traversal() {
        let (_d, p) = temp();
        let store = Store {
            dir: p,
            owner: uid(),
        };
        assert!(store.path("../other").is_err());
    }
    #[test]
    fn trusted_directory_rejects_writable_ancestor() {
        let (_d, p) = temp();
        assert!(trusted_dir(&p, uid()).is_err());
    }
}

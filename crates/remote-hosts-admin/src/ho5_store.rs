//! Ordinary-user journal anchored by an owned private directory, never by overflow UID.
use crate::protocol::{digest, valid_id};
use anyhow::{Context, Result, ensure};
use nix::{
    dir::Dir,
    fcntl::{OFlag, open, openat, renameat},
    sys::stat::Mode,
    unistd::{UnlinkatFlags, unlinkat},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

const MAX_RECORD: u64 = 256 * 1024;
const MAX_ENTRIES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallerIdentity {
    pub platform: String,
    pub uid: u32,
    pub gid: u32,
    /// Mapping is relative to the namespace parent; it is not authenticated host identity.
    pub parent_uid: u32,
    pub parent_gid: u32,
    pub mapping_sha256: String,
}
#[cfg(any(target_os = "linux", test))]
type MappingRows = Vec<(u32, u32, u32)>;
#[cfg(any(target_os = "linux", test))]
fn mapping(text: &str, id: u32) -> Result<(u32, MappingRows)> {
    ensure!(text.len() <= 65536, "namespace_mapping_too_large");
    let mut rows = vec![];
    let mut mapped = None;
    for line in text.lines() {
        let values = line
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            values.len() == 3 && values[2] != 0 && rows.len() < 340,
            "invalid_namespace_mapping"
        );
        let (inside, outside, count) = (values[0], values[1], values[2]);
        let end = u64::from(inside) + u64::from(count);
        let parent_end = u64::from(outside) + u64::from(count);
        ensure!(
            end <= 1u64 << 32 && parent_end <= 1u64 << 32,
            "namespace_mapping_overflow"
        );
        for &(a, b, n) in &rows {
            ensure!(
                (end <= u64::from(a) || u64::from(inside) >= u64::from(a) + u64::from(n))
                    && (parent_end <= u64::from(b)
                        || u64::from(outside) >= u64::from(b) + u64::from(n)),
                "overlapping_namespace_mapping"
            );
        }
        if id >= inside && u64::from(id) < end {
            mapped = Some(outside + (id - inside));
        }
        rows.push((inside, outside, count));
    }
    Ok((mapped.context("caller_not_mapped")?, rows))
}
impl CallerIdentity {
    pub fn current() -> Result<Self> {
        let uid = nix::unistd::geteuid().as_raw();
        let gid = nix::unistd::getegid().as_raw();
        ensure!(uid != 0 && uid != 65534, "ordinary_mapped_caller_required");
        #[cfg(target_os = "linux")]
        let (parent_uid, parent_gid, mapping_sha256) = {
            let (u, um) = mapping(&std::fs::read_to_string("/proc/self/uid_map")?, uid)?;
            let (g, gm) = mapping(&std::fs::read_to_string("/proc/self/gid_map")?, gid)?;
            ensure!(u != 0 && u != 65534, "ordinary_parent_caller_required");
            (u, g, digest(&(um, gm))?)
        };
        #[cfg(not(target_os = "linux"))]
        let (parent_uid, parent_gid, mapping_sha256) = (uid, gid, digest(&("native", uid, gid))?);
        Ok(Self {
            platform: std::env::consts::OS.into(),
            uid,
            gid,
            parent_uid,
            parent_gid,
            mapping_sha256,
        })
    }
    pub fn sha256(&self) -> Result<String> {
        digest(self)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreIdentity {
    pub device: u64,
    pub inode: u64,
}
/// Ancestors are traversed without symlinks but confer no identity or OS authority.
/// Once a private caller-owned anchor is reached, every descendant must remain
/// caller-owned and non-writable by others. All journal operations use the pinned fd.
pub struct UserStore {
    dir: File,
    owner: u32,
}
impl UserStore {
    pub fn existing(path: &Path, owner: u32) -> Result<Option<Self>> {
        ensure!(
            owner == CallerIdentity::current()?.uid,
            "journal_caller_mismatch"
        );
        ensure!(path.is_absolute(), "task_store_requires_absolute_path");
        let mut dir = File::from(open(
            "/",
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        let mut anchored = false;
        for part in path.components() {
            match part {
                Component::RootDir => continue,
                Component::Normal(name) => {
                    dir = match openat(
                        &dir,
                        name,
                        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                        Mode::empty(),
                    ) {
                        Ok(fd) => File::from(fd),
                        Err(nix::errno::Errno::ENOENT) => return Ok(None),
                        Err(e) => return Err(e.into()),
                    };
                }
                _ => anyhow::bail!("unsafe_task_store_component"),
            }
            let m = dir.metadata()?;
            if anchored {
                ensure!(
                    m.uid() == owner && m.mode() & 0o022 == 0,
                    "untrusted_private_anchor_descendant"
                );
            }
            anchored |= m.uid() == owner && m.mode() & 0o077 == 0;
        }
        let m = dir.metadata()?;
        ensure!(
            anchored && m.uid() == owner && m.mode() & 0o077 == 0,
            "task_store_must_be_caller_owned_private_directory"
        );
        Ok(Some(Self { dir, owner }))
    }
    pub fn locked(path: &Path, owner: u32) -> Result<(Self, File)> {
        let store = Self::existing(path, owner)?.context("task_store_missing")?;
        let lock = File::from(openat(
            &store.dir,
            "task.lock",
            OFlag::O_RDWR
                | OFlag::O_CREAT
                | OFlag::O_NOFOLLOW
                | OFlag::O_NONBLOCK
                | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )?);
        store.private_file(&lock)?;
        fs2::FileExt::try_lock_exclusive(&lock).context("task_store_busy_observe_original")?;
        Ok((store, lock))
    }
    pub fn identity(&self) -> Result<StoreIdentity> {
        let m = self.dir.metadata()?;
        Ok(StoreIdentity {
            device: m.dev(),
            inode: m.ino(),
        })
    }
    fn private_file(&self, f: &File) -> Result<()> {
        let m = f.metadata()?;
        ensure!(
            m.is_file() && m.uid() == self.owner && m.nlink() == 1 && m.mode() & 0o077 == 0,
            "untrusted_private_file"
        );
        ensure!(m.len() <= MAX_RECORD, "private_file_too_large");
        Ok(())
    }
    fn bytes(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let f = match openat(
            &self.dir,
            name,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => File::from(fd),
            Err(nix::errno::Errno::ENOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        self.private_file(&f)?;
        let mut bytes = vec![];
        f.take(MAX_RECORD + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_RECORD,
            "private_file_grew_too_large"
        );
        Ok(Some(bytes))
    }
    pub fn load_json<T: serde::de::DeserializeOwned>(&self, id: &str) -> Result<Option<T>> {
        valid_id(id)?;
        self.bytes(&format!("{id}.json"))?
            .map(|b| serde_json::from_slice(&b).map_err(Into::into))
            .transpose()
    }
    pub fn save_json<T: serde::Serialize>(&self, id: &str, value: &T) -> Result<()> {
        valid_id(id)?;
        let name = format!("{id}.json");
        self.bytes(&name)?; // Validate any existing entry before replacing it.
        let bytes = serde_json::to_vec(value)?;
        ensure!(bytes.len() as u64 <= MAX_RECORD, "receipt_too_large");
        let temporary = format!(".receipt-{}.tmp", uuid::Uuid::new_v4());
        let mut file = File::from(openat(
            &self.dir,
            temporary.as_str(),
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )?);
        let result = (|| {
            file.write_all(&bytes)?;
            file.sync_all()?;
            renameat(&self.dir, temporary.as_str(), &self.dir, name.as_str())?;
            self.dir.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = unlinkat(&self.dir, temporary.as_str(), UnlinkatFlags::NoRemoveDir);
        }
        result
    }
    fn entries(&self) -> Result<Vec<String>> {
        let mut dir = Dir::from_fd(self.dir.try_clone()?.into())?;
        let mut names = vec![];
        for entry in dir.iter() {
            let entry = entry?;
            let bytes = entry.file_name().to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            ensure!(
                names.len() < MAX_ENTRIES,
                "receipt_store_full_archive_with_admin_authorization"
            );
            if let Ok(name) = std::str::from_utf8(bytes) {
                names.push(name.to_owned());
            } else {
                anyhow::bail!("invalid_journal_entry_name");
            }
        }
        Ok(names)
    }
    pub fn capacity(&self) -> Result<()> {
        ensure!(
            self.entries()?.len() < MAX_ENTRIES,
            "receipt_store_full_archive_with_admin_authorization"
        );
        Ok(())
    }
    pub fn ids(&self) -> Result<Vec<String>> {
        Ok(self
            .entries()?
            .into_iter()
            .filter_map(|n| {
                n.strip_suffix(".json")
                    .filter(|id| valid_id(id).is_ok())
                    .map(String::from)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn private() -> (tempfile::TempDir, std::path::PathBuf, u32) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().canonicalize().unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        (dir, path, nix::unistd::geteuid().as_raw())
    }
    #[test]
    fn namespace_mapping_is_exact_and_overlaps_fail() {
        assert_eq!(mapping("1000 1000 1\n", 1000).unwrap().0, 1000);
        assert_eq!(mapping("0 1000 1\n1 100000 65536\n", 42).unwrap().0, 100041);
        for bad in [
            "1000 1000 0",
            "1000 1000 1\n1000 2000 1",
            "0 0 10\n20 5 10",
            "4294967295 0 2",
            "1000 1",
        ] {
            assert!(mapping(bad, 1000).is_err());
        }
        assert!(mapping("1000 1000 1", 65534).is_err());
    }
    #[test]
    fn private_leaf_and_lock_are_enforced() {
        let (_d, path, uid) = private();
        let (_s, lock) = UserStore::locked(&path, uid).unwrap();
        assert!(UserStore::locked(&path, uid).is_err());
        drop(lock);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(UserStore::existing(&path, uid).is_err());
    }
    #[test]
    fn pinned_directory_is_not_redirected_by_path_replacement() {
        let (_d, path, uid) = private();
        let original = path.join("original");
        std::fs::create_dir(&original).unwrap();
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (s, _lock) = UserStore::locked(&original, uid).unwrap();
        let moved = path.join("moved");
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o700)).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        s.save_json(&id, &serde_json::json!({"original":true}))
            .unwrap();
        assert!(moved.join(format!("{id}.json")).is_file());
        assert!(!original.join(format!("{id}.json")).exists());
        assert_eq!(s.ids().unwrap(), vec![id]);
    }
    #[test]
    fn symlink_and_hardlink_journals_cannot_redirect_io() {
        let (_d, path, uid) = private();
        let s = UserStore::existing(&path, uid).unwrap().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let target = path.join("target");
        std::fs::write(&target, b"{}").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let record = path.join(format!("{id}.json"));
        std::os::unix::fs::symlink(&target, &record).unwrap();
        assert!(s.load_json::<serde_json::Value>(&id).is_err());
        assert!(s.save_json(&id, &serde_json::json!({})).is_err());
        std::fs::remove_file(&record).unwrap();
        std::fs::hard_link(&target, &record).unwrap();
        assert!(s.load_json::<serde_json::Value>(&id).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"{}");
    }
    #[test]
    fn symlink_ancestor_is_rejected_without_creating_state() {
        let (_d, path, uid) = private();
        std::os::unix::fs::symlink(&path, path.join("alias")).unwrap();
        assert!(UserStore::existing(&path.join("alias"), uid).is_err());
        assert!(
            UserStore::existing(&path.join("missing"), uid)
                .unwrap()
                .is_none()
        );
        assert!(!path.join("task.lock").exists());
    }
}

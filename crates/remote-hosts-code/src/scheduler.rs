//! Bounded execution lanes and resource-scoped coordination. No filesystem I/O.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Lane {
    Read,
    Write,
    Transfer,
    Terminal,
    Control,
}
impl Lane {
    pub const ALL: [Self; 5] = [
        Self::Read,
        Self::Write,
        Self::Transfer,
        Self::Terminal,
        Self::Control,
    ];
    pub fn for_tool(tool: &str) -> Self {
        match tool {
            "file_upload" | "file_download" => Self::Transfer,
            "terminal_exec" | "terminal_input" => Self::Terminal,
            "terminal_cancel" | "terminal_read" => Self::Control,
            "code_apply_edits" | "workspace_open" | "files_sync" => Self::Write,
            _ => Self::Read,
        }
    }
    pub fn limit(self) -> usize {
        match self {
            Self::Read => 8,
            Self::Write => 2,
            Self::Transfer => 2,
            Self::Terminal => 4,
            Self::Control => 2,
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Transfer => 2,
            Self::Terminal => 3,
            Self::Control => 4,
        }
    }
}

/// Weak entries are pruned so millions of completed operation IDs do not leak locks.
#[derive(Default)]
pub(crate) struct KeyedLocks(Mutex<HashMap<String, Weak<AsyncMutex<()>>>>);
impl KeyedLocks {
    pub fn get(&self, key: &str) -> Result<Arc<AsyncMutex<()>>> {
        let mut locks = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("resource lock poisoned"))?;
        locks.retain(|_, v| v.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key.to_owned(), Arc::downgrade(&lock));
        Ok(lock)
    }
    pub async fn lock(&self, key: &str) -> Result<OwnedMutexGuard<()>> {
        Ok(self.get(key)?.lock_owned().await)
    }
}

/// Different workspace IDs can point to the same or nested directories.
/// Serialize overlapping write roots, not just their logical workspace IDs.
#[derive(Default)]
pub(crate) struct WriteScopes {
    held: Mutex<HashMap<String, PathBuf>>,
    changed: Notify,
}
pub(crate) struct WriteGuard {
    scopes: Arc<WriteScopes>,
    id: String,
}
impl WriteScopes {
    pub fn roots(&self) -> Result<Vec<PathBuf>> {
        Ok(self
            .held
            .lock()
            .map_err(|_| anyhow::anyhow!("write scope lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }
    pub async fn acquire(self: &Arc<Self>, root: &Path) -> Result<WriteGuard> {
        let id = crate::random();
        loop {
            let notified = self.changed.notified();
            {
                let mut held = self
                    .held
                    .lock()
                    .map_err(|_| anyhow::anyhow!("write scope lock poisoned"))?;
                if !held
                    .values()
                    .any(|p| p.starts_with(root) || root.starts_with(p))
                {
                    held.insert(id.clone(), root.to_owned());
                    return Ok(WriteGuard {
                        scopes: self.clone(),
                        id,
                    });
                }
            }
            notified.await;
        }
    }
}
impl Drop for WriteGuard {
    fn drop(&mut self) {
        if let Ok(mut held) = self.scopes.held.lock() {
            held.remove(&self.id);
        }
        self.scopes.changed.notify_waiters();
    }
}

pub(crate) struct Scheduler {
    permits: [Arc<Semaphore>; 5],
    pub operations: KeyedLocks,
    pub terminals: KeyedLocks,
    pub writes: Arc<WriteScopes>,
}
impl Default for Scheduler {
    fn default() -> Self {
        Self {
            permits: Lane::ALL.map(|lane| Arc::new(Semaphore::new(lane.limit()))),
            operations: KeyedLocks::default(),
            terminals: KeyedLocks::default(),
            writes: Arc::default(),
        }
    }
}
impl Scheduler {
    pub async fn acquire(&self, lane: Lane) -> Result<OwnedSemaphorePermit> {
        self.permits[lane.index()]
            .clone()
            .acquire_owned()
            .await
            .context("execution lane closed")
    }
}

#[derive(Clone, Default)]
pub(crate) struct ActiveResource {
    pub write_root: Option<PathBuf>,
    pub terminal_input: Option<String>,
}

/// Advisory queue admission only. Actual authorization and resource locks still
/// apply at execution. Unknown/old clients send no filter and keep old behavior.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ResourceFilter {
    pub write_workspaces: Vec<String>,
    pub terminal_inputs: Vec<String>,
    pub all_writes: bool,
}
impl ResourceFilter {
    pub fn valid(&self) -> bool {
        self.write_workspaces.len() <= 1024
            && self.terminal_inputs.len() <= 32
            && self.write_workspaces.iter().all(|s| {
                s.len() <= 128
                    && s.split_once(':').is_some_and(|(d, w)| {
                        uuid::Uuid::parse_str(d).is_ok() && uuid::Uuid::parse_str(w).is_ok()
                    })
            })
            && self
                .terminal_inputs
                .iter()
                .all(|s| uuid::Uuid::parse_str(s).is_ok())
    }
    pub fn is_empty(&self) -> bool {
        !self.all_writes && self.write_workspaces.is_empty() && self.terminal_inputs.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct ActiveJobs(Mutex<HashMap<String, ActiveResource>>);
pub(crate) struct ActiveGuard {
    jobs: Arc<ActiveJobs>,
    id: String,
}
impl ActiveJobs {
    pub fn list(&self) -> Result<Vec<String>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("active job lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }
    pub fn resources(&self) -> Result<Vec<ActiveResource>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("active job lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }
    #[cfg(test)]
    pub fn enter(self: &Arc<Self>, id: &str) -> Result<Option<ActiveGuard>> {
        self.enter_scoped(id, ActiveResource::default())
    }
    pub fn enter_scoped(
        self: &Arc<Self>,
        id: &str,
        resource: ActiveResource,
    ) -> Result<Option<ActiveGuard>> {
        let mut active = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("active job lock poisoned"))?;
        if active.contains_key(id) {
            return Ok(None);
        }
        active.insert(id.to_owned(), resource);
        Ok(Some(ActiveGuard {
            jobs: self.clone(),
            id: id.to_owned(),
        }))
    }
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.jobs.0.lock() {
            active.remove(&self.id);
        }
    }
}

/// Stream permits are held for the body lifetime. Allocation holds a separate
/// short lock, so quota checks stay atomic without serializing file streaming.
pub(crate) struct TransferLimits {
    global: Arc<Semaphore>,
    devices: HashMap<String, Arc<Semaphore>>,
    operations: KeyedLocks,
    pub allocation: AsyncMutex<()>,
}
pub(crate) struct TransferPermit {
    _global: OwnedSemaphorePermit,
    _device: OwnedSemaphorePermit,
    _operation: OwnedMutexGuard<()>,
}
impl TransferLimits {
    pub fn new<'a>(devices: impl Iterator<Item = &'a str>) -> Self {
        Self {
            global: Arc::new(Semaphore::new(4)),
            devices: devices
                .map(|id| (id.to_owned(), Arc::new(Semaphore::new(2))))
                .collect(),
            operations: KeyedLocks::default(),
            allocation: AsyncMutex::new(()),
        }
    }
    pub fn try_acquire(&self, device: &str, operation: &str) -> Result<TransferPermit> {
        let op = self
            .operations
            .get(operation)?
            .try_lock_owned()
            .context("transfer operation busy")?;
        // Take device capacity first so one device cannot reserve global slots while waiting.
        let device = self
            .devices
            .get(device)
            .context("unknown transfer device")?
            .clone()
            .try_acquire_owned()
            .context("device transfer capacity reached")?;
        let global = self
            .global
            .clone()
            .try_acquire_owned()
            .context("gateway transfer capacity reached")?;
        Ok(TransferPermit {
            _global: global,
            _device: device,
            _operation: op,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn transfer_capacity_does_not_consume_read_or_terminal_slots() {
        let scheduler = Scheduler::default();
        let a = scheduler.acquire(Lane::Transfer).await.unwrap();
        let b = scheduler.acquire(Lane::Transfer).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), scheduler.acquire(Lane::Transfer))
                .await
                .is_err()
        );
        let _read = scheduler.acquire(Lane::Read).await.unwrap();
        let _terminal = scheduler.acquire(Lane::Terminal).await.unwrap();
        drop(a);
        drop(b);
        let _permit = scheduler.acquire(Lane::Transfer).await.unwrap();
    }
    #[tokio::test]
    async fn overlapping_roots_serialize_but_sibling_roots_do_not() {
        let scopes = Arc::new(WriteScopes::default());
        let held = scopes.acquire(Path::new("/project/a")).await.unwrap();
        let _sibling = scopes.acquire(Path::new("/project/b")).await.unwrap();
        for root in ["/project", "/project/a", "/project/a/nested"] {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), scopes.acquire(Path::new(root)))
                    .await
                    .is_err()
            );
        }
        drop(held);
        scopes
            .acquire(Path::new("/project/a/nested"))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn keyed_locks_serialize_only_the_same_resource_and_are_pruned() {
        let locks = KeyedLocks::default();
        let a = locks.lock("terminal-a").await.unwrap();
        let _b = locks.lock("terminal-b").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), locks.lock("terminal-a"))
                .await
                .is_err()
        );
        drop(a);
        locks.lock("terminal-a").await.unwrap();
        for n in 0..100 {
            drop(locks.lock(&n.to_string()).await.unwrap());
        }
        assert!(locks.0.lock().unwrap().len() <= 2);
    }
    #[test]
    fn gateway_transfer_limits_are_global_and_per_device() {
        let limits = TransferLimits::new(["a", "b", "c"].into_iter());
        let a1 = limits.try_acquire("a", "a1").unwrap();
        let a2 = limits.try_acquire("a", "a2").unwrap();
        assert!(limits.try_acquire("a", "a3").is_err());
        assert!(limits.try_acquire("b", "a1").is_err());
        let _b1 = limits.try_acquire("b", "b1").unwrap();
        let _b2 = limits.try_acquire("b", "b2").unwrap();
        assert!(limits.try_acquire("c", "c1").is_err());
        drop(a1);
        drop(a2);
        assert!(limits.try_acquire("c", "c1").is_ok());
    }
    #[test]
    fn active_jobs_are_unique_and_removed_on_drop() {
        let jobs = Arc::new(ActiveJobs::default());
        let held = jobs.enter("operation").unwrap().unwrap();
        assert!(jobs.enter("operation").unwrap().is_none());
        assert_eq!(jobs.list().unwrap().len(), 1);
        drop(held);
        assert!(jobs.list().unwrap().is_empty());
    }
}

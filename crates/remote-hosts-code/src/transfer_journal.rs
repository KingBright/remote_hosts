//! Device-local transfer metadata. No signed URL or credential is persisted here.
use crate::{AgentConfig, files::Workspace, gateway::Job, hash, now, store::Store};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Journal {
    pub operation_id: String,
    pub device_id: String,
    pub workspace_id: String,
    pub root: PathBuf,
    pub origin: String,
    pub fingerprint: String,
    pub direction: String,
    pub path: String,
    pub phase: String,
    pub revision: u64,
    pub offset: u64,
    pub total: Option<u64>,
    pub etag: Option<String>,
    pub prefix_sha256: String,
    pub content_sha256: Option<String>,
    pub reserved_bytes: usize,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: i64,
    pub result: Option<Value>,
    pub publication_temp: Option<String>,
    #[serde(default)]
    pub publication_identity: Option<(u64, u64)>,
    #[serde(default)]
    pub data_cleaned: bool,
}
impl Journal {
    pub fn data_path(&self, config: &AgentConfig) -> PathBuf {
        config
            .state_dir
            .join("transfers-v2")
            .join(format!("{}.data", self.operation_id))
    }
    pub async fn save(&mut self, store: &Store) -> Result<()> {
        self.updated_at = now();
        store
            .put("transfer_local", &self.operation_id, self, i64::MAX)
            .await
    }
    pub fn view(&self) -> Value {
        json!({"operation_id":self.operation_id,"workspace_id":self.workspace_id,"direction":self.direction,
            "path":self.path,"state":self.phase,"confirmed_bytes":self.offset,"total_bytes":self.total,
            "transfer_revision":self.revision,"expires_at":self.expires_at,"updated_at":self.updated_at,
            "checkpoint_bytes":crate::transfer_receiver::CHUNK,"retained":!matches!(self.phase.as_str(),"completed"|"cancelled"|"failed")})
    }
    pub async fn load_or_create(
        config: &AgentConfig,
        ws: &Workspace,
        job: &Job,
        store: &Store,
        scopes: Arc<crate::scheduler::WriteScopes>,
    ) -> Result<Self> {
        uuid::Uuid::parse_str(&job.id)?;
        let fingerprint = hash(serde_json::to_vec(job)?);
        let path = crate::files::text(&job.arguments, "path")?;
        crate::files::relative(path)?;
        if let Some(record) = store.get::<Self>("transfer_local", &job.id).await? {
            ensure!(
                record.fingerprint == fingerprint
                    && record.device_id == config.device_id
                    && record.origin == config.gateway_url
                    && record.root == ws.root
                    && record.workspace_id == ws.id,
                "transfer_identity_conflict"
            );
            return Ok(record);
        }
        let directory = config.state_dir.join("transfers-v2");
        let _guard = scopes.acquire(&directory).await?;
        tokio::fs::create_dir_all(&directory).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).await?;
        }
        // Expired entries cannot be resumed. Preserve their receipt but release
        // quota only after deleting their private partial data successfully.
        let rows:Vec<(String,String)>=sqlx::query_as("SELECT key,value FROM kv WHERE kind='transfer_local' AND json_extract(value,'$.expires_at')<=? AND json_extract(value,'$.phase')<>'publishing' AND COALESCE(json_extract(value,'$.data_cleaned'),0)=0 LIMIT 128")
            .bind(now()).fetch_all(&store.pool).await?;
        for (id, text) in rows {
            if uuid::Uuid::parse_str(&id).is_err() {
                continue;
            }
            let mut old: Self = serde_json::from_str(&text)?;
            let target = old.data_path(config);
            match tokio::fs::remove_file(target).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => continue,
            }
            if old.result.is_none() {
                old.phase = "expired".into();
            }
            old.data_cleaned = true;
            old.save(store).await?;
        }
        let (used,count):(i64,i64)=sqlx::query_as("SELECT COALESCE(SUM(json_extract(value,'$.reserved_bytes')),0),COUNT(*) FROM kv WHERE kind='transfer_local' AND json_extract(value,'$.phase') NOT IN ('completed','cancelled','failed','expired')")
            .fetch_one(&store.pool).await?;
        let max = crate::files::number(
            &job.arguments,
            "max_bytes",
            crate::transfers::MAX_BYTES,
            1,
            crate::transfers::MAX_BYTES,
        )?;
        // Account for failed cleanup as well as active reservations.
        let mut leftover_bytes = 0u64;
        let mut disk_entries = 0usize;
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            disk_entries += 1;
            ensure!(
                disk_entries <= 512,
                "transfer_storage_limit: too many retained files"
            );
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let meta = entry.metadata()?;
            if !meta.is_file() {
                continue;
            }
            let saved = store.get::<Self>("transfer_local", id).await?;
            if saved.as_ref().is_none_or(|s| {
                matches!(
                    s.phase.as_str(),
                    "completed" | "cancelled" | "failed" | "expired"
                )
            }) {
                leftover_bytes = leftover_bytes.saturating_add(meta.len());
            }
        }
        ensure!(
            used >= 0
                && (used as u64)
                    .saturating_add(leftover_bytes)
                    .saturating_add(max as u64)
                    <= 512 * 1024 * 1024
                && count < 128,
            "transfer_storage_limit: cancel or expire retained transfers"
        );
        let mut j = Self {
            operation_id: job.id.clone(),
            device_id: config.device_id.clone(),
            workspace_id: ws.id.clone(),
            root: ws.root.clone(),
            origin: config.gateway_url.clone(),
            fingerprint,
            direction: job.tool.clone(),
            path: path.into(),
            phase: "preparing".into(),
            revision: 0,
            offset: 0,
            total: None,
            etag: None,
            prefix_sha256: hash([]),
            content_sha256: None,
            reserved_bytes: max,
            created_at: now(),
            updated_at: now(),
            expires_at: now() + crate::transfer_receiver::RETENTION,
            result: None,
            publication_temp: None,
            publication_identity: None,
            data_cleaned: false,
        };
        j.save(store).await?;
        Ok(j)
    }
}

pub(crate) fn open_data(path: &std::path::Path, create: bool) -> Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let file = opts.open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "transfer data must be regular file"
    );
    Ok(file)
}
/// Discard only bytes not included in the last durable checkpoint. Verify the
/// retained prefix, including after a process restart, before appending any data.
pub(crate) fn restore_prefix(
    mut file: std::fs::File,
    offset: u64,
    expected: &str,
) -> Result<(std::fs::File, Sha256)> {
    ensure!(file.metadata()?.len() >= offset, "checkpoint_data_missing");
    file.set_len(offset)?;
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut left = offset;
    let mut buf = [0u8; 65536];
    while left > 0 {
        let n = file.read(&mut buf[..left.min(65536) as usize])?;
        ensure!(n > 0, "checkpoint_data_missing");
        digest.update(&buf[..n]);
        left -= n as u64;
    }
    ensure!(
        format!("{:x}", digest.clone().finalize()) == expected,
        "checkpoint_checksum_mismatch"
    );
    file.seek(SeekFrom::Start(offset))
        .context("checkpoint seek failed")?;
    Ok((file, digest))
}

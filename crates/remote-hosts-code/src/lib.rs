//! Authenticated personal MCP gateway and outbound local code agent.
pub mod agent;
pub mod auth;
mod capabilities;
mod change_review;
mod delivery;
mod diagnostics;
mod durable_transfer;
pub mod files;
mod files_sync;
pub mod gateway;
mod maintenance;
mod observations;
mod progress;
mod reads;
mod resumable;
mod scheduler;
pub mod store;
pub mod terminal;
mod terminal_output;
mod terminal_sync;
pub mod tools;
mod transfer_control;
mod transfer_journal;
mod transfer_receiver;
mod transfer_watch;
pub mod transfers;
mod work_events;
mod workspace_context;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const SCOPES: &str = "code:read code:write terminal:exec";
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
pub fn random() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}
pub fn hash(value: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(value.as_ref()))
}
pub fn secret_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(hash(a).as_bytes().ct_eq(hash(b).as_bytes()))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceRegistration {
    pub id: String,
    pub name: String,
    pub token_hash: String,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    pub public_url: String,
    pub bind: String,
    pub state_dir: PathBuf,
    pub owner: String,
    pub password_hash: String,
    pub devices: Vec<DeviceRegistration>,
    pub redirect_uris: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub gateway_url: String,
    pub device_id: String,
    pub device_token: String,
    pub state_dir: PathBuf,
    pub roots: Vec<PathBuf>,
    pub allow_write: bool,
    pub allow_exec: bool,
    pub shell: PathBuf,
}
pub fn read_config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "config must have mode 0600"
        );
    }
    serde_json::from_slice(&std::fs::read(path)?).context("invalid config")
}
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let parent = path.parent().context("file needs parent")?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}
pub fn validate_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value)?;
    ensure!(
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "gateway URL must be an HTTPS origin, including public port if required"
    );
    ensure!(!value.ends_with('/'), "omit trailing slash from public URL");
    Ok(url)
}

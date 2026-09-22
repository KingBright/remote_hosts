//! Authenticated personal MCP gateway and outbound local code agent.
mod activity;
pub mod agent;
pub mod agent_log;
pub mod auth;
mod capabilities;
mod poll_health;
mod status_view;
pub use capabilities::release_manifest;
pub mod adapter;
mod change_review;
pub mod contract;
mod delivery;
mod diagnostics;
mod durable_transfer;
pub mod files;
mod files_sync;
pub mod gateway;
mod job_dispatch;
mod job_receipts;
mod maintenance;
mod observations;
mod progress;
mod readiness;
mod reads;
pub mod receipts;
mod resumable;
mod scheduler;
mod storage_gc;
pub mod store;
pub mod terminal;
#[cfg(unix)]
mod terminal_io;
mod terminal_output;
mod terminal_sync;
mod token_output;
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
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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
    /// Exact browser origins. OAuth callback URIs are a separate allowlist.
    #[serde(default = "default_mcp_client_origins")]
    pub allowed_origins: Vec<String>,
}
/// Preserve the browser policy of existing configurations on upgrade.
pub fn default_mcp_client_origins() -> Vec<String> {
    vec![
        "https://chatgpt.com".into(),
        "https://gemini.google.com".into(),
    ]
}
impl GatewayConfig {
    /// Check client-facing policy without starting a service or opening its database.
    pub fn validate_oauth_policy(&self) -> Result<()> {
        validate_url(&self.public_url)?;
        ensure!(self.allowed_origins.len() <= 32, "too many browser origins");
        ensure!(self.redirect_uris.len() <= 64, "too many OAuth callbacks");
        for origin in &self.allowed_origins {
            validate_url(origin).context("browser origin must be an exact HTTPS origin")?;
            ensure!(!origin.contains('*'), "wildcard origins are not allowed");
        }
        for uri in &self.redirect_uris {
            let u = reqwest::Url::parse(uri)?;
            ensure!(
                uri.len() <= 4096
                    && !uri.contains('*')
                    && !uri
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                    && (u.scheme() == "https" || native_oauth_callback(uri))
                    && u.host_str().is_some()
                    && u.username().is_empty()
                    && u.password().is_none()
                    && u.fragment().is_none(),
                "OAuth callback must be exact HTTPS or a native loopback callback without userinfo or fragment"
            );
        }
        Ok(())
    }
    pub(crate) fn browser_origins(&self) -> Vec<String> {
        let mut origins = self.allowed_origins.clone();
        origins.push(self.public_url.clone());
        origins.sort();
        origins.dedup();
        origins
    }
    pub(crate) fn authorization_csp(&self) -> String {
        self.authorization_csp_for_callback(None)
    }
    pub(crate) fn authorization_csp_for_callback(&self, callback: Option<&str>) -> String {
        // Only configured callbacks contribute redirect destinations. Browser
        // request origins must never implicitly authorize an OAuth callback.
        let mut origins: Vec<_> = self
            .redirect_uris
            .iter()
            .filter_map(|uri| reqwest::Url::parse(uri).ok())
            .map(|uri| uri.origin().ascii_serialization())
            .collect();
        // A validated grant contributes only its concrete callback origin,
        // never a wildcard host/port or an unrelated browser origin.
        if let Some(callback) =
            callback.filter(|uri| native_oauth_callback(uri) || google_oauth_callback(uri))
            && let Ok(uri) = reqwest::Url::parse(callback)
        {
            origins.push(uri.origin().ascii_serialization());
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| origin == "https://gemini.google.com")
        {
            origins.push("https://oauth-redirect.googleusercontent.com".into());
        }
        origins.sort();
        origins.dedup();
        format!(
            "default-src 'none'; form-action 'self' {}; frame-ancestors 'none'; base-uri 'none'",
            origins.join(" ")
        )
    }
}

/// The six callback variants registered together by the real Spark client.
/// This is an exact host/path family, never a googleusercontent wildcard.
pub(crate) fn google_oauth_callback(candidate: &str) -> bool {
    let Ok(uri) = reqwest::Url::parse(candidate) else {
        return false;
    };
    let Some(suffix) = uri
        .path()
        .strip_prefix("/r/user_bound_custom-mcp-")
        .or_else(|| uri.path().strip_prefix("/a/user_bound_custom-mcp-"))
    else {
        return false;
    };
    uri.as_str() == candidate
        && uri.scheme() == "https"
        && matches!(
            uri.host_str(),
            Some(
                "oauth-redirect.googleusercontent.com"
                    | "oauth-redirect-sandbox.googleusercontent.com"
                    | "oauth-redirect-test.googleusercontent.com"
            )
        )
        && uri.port().is_none()
        && uri.username().is_empty()
        && uri.password().is_none()
        && uri.query().is_none()
        && uri.fragment().is_none()
        && !suffix.is_empty()
        && suffix.len() <= 512
        && suffix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.~".contains(&b))
}

/// Bounded native OAuth redirects. The concrete URI is still bound to the
/// registered client and every authorization/code exchange; PKCE is mandatory.
pub(crate) fn native_oauth_callback(candidate: &str) -> bool {
    let Ok(uri) = reqwest::Url::parse(candidate) else {
        return false;
    };
    let path_allowed = uri.path() == "/callback"
        || uri.path().strip_prefix("/callback/").is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix.len() <= 128
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        });
    candidate.len() <= 512
        && uri.as_str() == candidate
        && uri.scheme() == "http"
        && matches!(uri.host_str(), Some("127.0.0.1" | "[::1]"))
        && uri.port().is_some_and(|port| port != 0)
        && uri.username().is_empty()
        && uri.password().is_none()
        && uri.query().is_none()
        && uri.fragment().is_none()
        && path_allowed
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

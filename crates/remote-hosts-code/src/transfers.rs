//! Binary data plane. Tool JSON carries identifiers, never file bytes.
//! Inbound URL credentials are short-lived and kept out of operation journals.
use crate::{
    AgentConfig,
    files::{self, Workspace},
    gateway::{Gateway, Job},
    hash, now, random,
};
use anyhow::{Context, Result, anyhow, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as HttpPath, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cap_std::fs::Dir;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const DISK_CAP: u64 = 1024 * 1024 * 1024;
pub(crate) const STORAGE_RESERVE_BYTES: u64 = 256 * 1024 * 1024;
const BLOB_TTL: i64 = 3600;
pub(crate) const SOURCE_TTL: i64 = 900;
const LINK_TTL: i64 = 900;
const IDLE: Duration = crate::resumable::IDLE;

pub(crate) fn limit(v: &Value) -> Result<usize> {
    files::number(v, "max_bytes", DEFAULT_MAX_BYTES, 1, MAX_BYTES)
}
pub(crate) fn ensure_storage_capacity(path: &Path, requested: u64) -> Result<u64> {
    let available = fs2::available_space(path)?;
    ensure!(
        available >= requested.saturating_add(STORAGE_RESERVE_BYTES),
        "storage_capacity_insufficient: keep reserve space or request a smaller transfer"
    );
    Ok(available)
}
pub(crate) async fn source_authorization_status(g: &Gateway, id: &str) -> Result<Value> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT expires FROM kv WHERE kind='file_source' AND key=?")
            .bind(id)
            .fetch_optional(&g.store.pool)
            .await?;
    let control = crate::transfer_control::control(g, id).await?;
    let at = now();
    let (state, expires_at) = match row {
        Some((expires,)) if expires > at => ("available", Some(expires)),
        Some((expires,)) => ("expired", Some(expires)),
        None => ("required", None),
    };
    Ok(json!({"protocol":1,"state":state,"expires_at":expires_at,
        "error_code":if state=="available" {Value::Null} else {json!("source_authorization_required")},
        "refresh_supported":true,"transfer_revision":control.revision,
        "next_action":if state=="available" {"observe_original_operation"} else {"transfer_resume_with_refreshed_file_authorization"}}))
}
pub(crate) fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn verify_hash(expected: Option<&str>, actual: &str) -> Result<()> {
    if let Some(expected) = expected {
        ensure!(
            valid_hash(expected),
            "SHA-256 must be 64 lowercase hexadecimal characters"
        );
        ensure!(
            expected == actual,
            "sha256_mismatch: destination was not changed"
        );
    }
    Ok(())
}
fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        // Full uploads are watched using sender demand and receiver progress.
        // read_timeout would also time out waiting for response headers.
        .build()?)
}
/// Only host-supplied file storage or this personal gateway may supply bytes.
/// DNS is additionally checked and pinned below; no redirects or forwarded bearer.
pub fn source_url(value: &str, gateway: &str) -> Result<reqwest::Url> {
    ensure!(value.len() <= 16384, "file URL too long");
    let u = reqwest::Url::parse(value).map_err(|_| anyhow!("invalid file URL"))?;
    let own = reqwest::Url::parse(gateway)?;
    ensure!(
        u.scheme() == "https"
            && u.username().is_empty()
            && u.password().is_none()
            && u.fragment().is_none(),
        "file source requires HTTPS without userinfo or fragment"
    );
    let host = u.host_str().context("file source needs host")?;
    let own_file = u.host_str() == own.host_str()
        && [Some(443), own.port_or_known_default()].contains(&u.port_or_known_default())
        && u.path().starts_with("/files/");
    let hosted = (host == "oaiusercontent.com"
        || host.ends_with(".oaiusercontent.com")
        || host.ends_with(".blob.core.windows.net"))
        && u.port_or_known_default() == Some(443);
    ensure!(
        own_file || hosted,
        "file source host not authorized; use a ChatGPT file parameter or a gateway download link"
    );
    Ok(u)
}
fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let o = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_documentation()
                || o[0] == 0
                || o[0] >= 224
                || (o[0] == 100 && (64..=127).contains(&o[1]))
                || (o[0] == 198 && (o[1] == 18 || o[1] == 19))
                || (o[0] == 192 && o[1] == 0 && o[2] == 0))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            s[0] & 0xe000 == 0x2000 && !(s[0] == 0x2001 && (s[1] == 0x0db8 || s[1] < 0x0200))
        }
    }
}
pub(crate) async fn source_client(u: &reqwest::Url) -> Result<reqwest::Client> {
    let host = u.host_str().context("missing source host")?;
    let addresses: Vec<SocketAddr> = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::net::lookup_host((host, u.port_or_known_default().unwrap_or(443))),
    )
    .await
    .map_err(|_| anyhow!("file source DNS lookup timed out"))?
    .map_err(|_| anyhow!("file source DNS lookup failed"))?
    .collect();
    ensure!(
        !addresses.is_empty() && addresses.iter().all(|a| public_ip(a.ip())),
        "file source resolves to a non-public address"
    );
    Ok(reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(IDLE)
        .resolve_to_addrs(host, &addresses)
        .build()?)
}
pub(crate) fn root(ws: &Workspace) -> Result<Dir> {
    Ok(Dir::open_ambient_dir(
        &ws.root,
        cap_std::ambient_authority(),
    )?)
}
pub(crate) fn digest_reader(
    mut file: impl Read,
    max: usize,
    mut output: Option<&mut std::fs::File>,
) -> Result<(usize, String)> {
    let mut digest = Sha256::new();
    let mut size = 0usize;
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size = size.checked_add(n).context("file size overflow")?;
        ensure!(size <= max, "file exceeds max_bytes");
        digest.update(&buf[..n]);
        if let Some(output) = output.as_mut() {
            output.write_all(&buf[..n])?;
        }
    }
    Ok((size, format!("{:x}", digest.finalize())))
}
pub(crate) fn existing(dir: &Dir, name: &Path) -> Result<String> {
    match dir.symlink_metadata(name) {
        Ok(m) => {
            ensure!(
                m.is_file() && !m.is_symlink(),
                "destination must be a regular non-symlink file"
            );
            ensure!(
                m.len() <= MAX_BYTES as u64,
                "existing destination exceeds transfer limit"
            );
            Ok(digest_reader(dir.open(name)?, MAX_BYTES, None)?.1)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok("absent".into()),
        Err(e) => Err(e.into()),
    }
}
/// Capability-relative staging; pins the destination parent directory.
pub(crate) struct Staged {
    dir: Dir,
    name: PathBuf,
    pub(crate) temporary: String,
    expected: String,
}
impl Drop for Staged {
    fn drop(&mut self) {
        let _ = self.dir.remove_file(&self.temporary);
    }
}
impl Staged {
    pub(crate) fn new(ws: &Workspace, v: &Value) -> Result<(Self, std::fs::File)> {
        let path = files::relative(files::text(v, "path")?)?;
        let root = root(ws)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        root.create_dir_all(parent)?;
        let dir = root.open_dir(parent)?;
        let name = PathBuf::from(path.file_name().context("missing file name")?);
        let expected = v
            .get("expected_version")
            .and_then(Value::as_str)
            .unwrap_or("absent")
            .to_owned();
        ensure!(
            expected == "absent" || valid_hash(&expected),
            "expected_version must be absent or a SHA-256"
        );
        ensure!(
            existing(&dir, &name)? == expected,
            "version_conflict: destination was not changed"
        );
        let temporary = format!(".remote-hosts-transfer-{}.tmp", random());
        let mut opts = cap_std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        let file = dir.open_with(&temporary, &opts)?.into_std();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        Ok((
            Self {
                dir,
                name,
                temporary,
                expected,
            },
            file,
        ))
    }
    pub(crate) fn publish(self) -> Result<()> {
        ensure!(
            existing(&self.dir, &self.name)? == self.expected,
            "version_conflict_during_transfer: destination was not changed"
        );
        if self.expected == "absent" {
            // Atomic create-only publication, including a concurrent creator race.
            self.dir.hard_link(&self.temporary, &self.dir, &self.name)?;
            self.dir.remove_file(&self.temporary)?;
        } else {
            let permissions = self.dir.metadata(&self.name)?.permissions();
            self.dir.set_permissions(&self.temporary, permissions)?;
            self.dir.rename(&self.temporary, &self.dir, &self.name)?;
        }
        #[cfg(unix)]
        self.dir.try_clone()?.into_std_file().sync_all()?;
        Ok(())
    }
}

pub async fn upload(
    config: &AgentConfig,
    ws: &Workspace,
    v: &Value,
    operation: &str,
) -> Result<Value> {
    upload_managed(
        config,
        ws,
        v,
        operation,
        &crate::progress::Progress::new(operation),
        None,
    )
    .await
}
pub(crate) async fn upload_managed(
    config: &AgentConfig,
    ws: &Workspace,
    v: &Value,
    operation: &str,
    progress: &crate::progress::Progress,
    scopes: Option<std::sync::Arc<crate::scheduler::WriteScopes>>,
) -> Result<Value> {
    let max = limit(v)?;
    let expected_sha = v.get("sha256").and_then(Value::as_str);
    if let Some(s) = expected_sha {
        ensure!(valid_hash(s), "invalid incoming sha256");
    }
    progress.phase("connecting");
    let http_control = client()?;
    let mut source = None;
    for attempt in 0..3 {
        let response = http_control
            .get(format!(
                "{}/device/file-source/{operation}",
                config.gateway_url
            ))
            .bearer_auth(&config.device_token)
            .timeout(Duration::from_secs(15))
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                source = Some(
                    response
                        .json::<Value>()
                        .await
                        .map_err(|_| anyhow!("invalid file source response"))?,
                );
                break;
            }
            Ok(response) if response.status().as_u16() == 410 => anyhow::bail!(
                "source_authorization_required: refresh the original file authorization with transfer_resume; destination unchanged"
            ),
            Ok(response) if response.status().is_client_error() => anyhow::bail!(
                "file_source_rejected: authorization or operation identity rejected; destination unchanged"
            ),
            _ if attempt < 2 => {
                progress.retry(0);
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
            _ => anyhow::bail!(
                "file source lookup failed after three attempts; destination unchanged"
            ),
        }
    }
    let source = source.context("file source lookup failed")?;
    let u = source_url(files::text(&source, "download_url")?, &config.gateway_url)?;
    let http = source_client(&u).await?;
    let (stage, raw) = Staged::new(ws, v)?;
    let mut output = tokio::fs::File::from_std(raw);
    let result = crate::resumable::fetch(
        &http,
        &u,
        &mut output,
        max,
        expected_sha,
        progress,
        crate::resumable::Policy::default(),
    )
    .await?;
    drop(output);
    progress.phase("waiting_resource");
    let guard = match scopes {
        Some(scopes) => Some(scopes.acquire(&ws.root).await?),
        None => None,
    };
    // Network I/O does not hold a workspace write lock. The actual publication
    // retains its guard through the blocking call and rechecks the target version.
    progress.phase("publishing");
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        stage.publish()
    })
    .await??;
    Ok(
        json!({"path":v["path"],"size":result.0,"sha256":result.1,"version":result.1,"state":"completed","atomic":true,"source_file_id":v["file"]["file_id"]}),
    )
}

pub async fn download(
    config: &AgentConfig,
    ws: &Workspace,
    v: &Value,
    operation: &str,
) -> Result<Value> {
    download_managed(
        config,
        ws,
        v,
        operation,
        &crate::progress::Progress::new(operation),
    )
    .await
}
pub(crate) async fn download_managed(
    config: &AgentConfig,
    ws: &Workspace,
    v: &Value,
    operation: &str,
    progress: &crate::progress::Progress,
) -> Result<Value> {
    progress.phase("snapshot");
    let ws = ws.clone();
    let args = v.clone();
    let dir = config.state_dir.join("transfer-staging");
    std::fs::create_dir_all(&dir)?;
    let (snapshot, size, sha, name) = tokio::task::spawn_blocking(move || -> Result<_> {
        let path = files::relative(files::text(&args, "path")?)?;
        let directory = root(&ws)?;
        let meta = directory.symlink_metadata(path)?;
        ensure!(
            meta.is_file() && !meta.is_symlink(),
            "source must be a regular non-symlink file"
        );
        let file = directory.open(path)?;
        ensure!(file.metadata()?.is_file(), "source is not a regular file");
        ensure!(meta.len() <= limit(&args)? as u64, "file exceeds max_bytes");
        let mut snapshot = tempfile::NamedTempFile::new_in(dir)?;
        let (size, sha) = digest_reader(file, limit(&args)?, Some(snapshot.as_file_mut()))?;
        verify_hash(args.get("expected_version").and_then(Value::as_str), &sha)?;
        snapshot.as_file().sync_all()?;
        let name = path
            .file_name()
            .context("missing source filename")?
            .to_string_lossy()
            .into_owned();
        Ok((snapshot, size, sha, name))
    })
    .await??;
    let http = client()?;
    progress.advance(0, Some(size as u64));
    let mut accepted = false;
    for attempt in 0..3 {
        progress.phase("transferring");
        let mut input = tokio::fs::File::from_std(snapshot.reopen()?);
        input.seek(std::io::SeekFrom::Start(0)).await?;
        let consumed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = consumed.clone();
        let stream = ReaderStream::new(input).map(move |chunk| {
            if let Ok(bytes) = &chunk {
                counter.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            chunk
        });
        let request = http
            .post(format!("{}/device/files/{operation}", config.gateway_url))
            .bearer_auth(&config.device_token)
            .header("x-file-size", size)
            .header("x-file-sha256", &sha)
            .header(header::CONTENT_LENGTH, size)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(reqwest::Body::wrap_stream(stream));
        let receiver_progress = http
            .get(format!(
                "{}/device/file-progress/{operation}",
                config.gateway_url
            ))
            .bearer_auth(&config.device_token);
        let response =
            crate::transfer_watch::send(request, receiver_progress, consumed, IDLE).await;
        match response {
            Ok(r) if r.status().is_success() => {
                let manifest: Value = tokio::time::timeout(IDLE, r.json())
                    .await
                    .map_err(|_| anyhow!("gateway transfer receipt timed out"))?
                    .map_err(|_| anyhow!("invalid gateway transfer receipt"))?;
                ensure!(
                    manifest["sha256"] == sha && manifest["size"] == size,
                    "gateway transfer receipt mismatch"
                );
                accepted = true;
                break;
            }
            Ok(r) if r.status().is_client_error() && r.status().as_u16() != 429 => {
                anyhow::bail!("gateway rejected file transfer ({})", r.status().as_u16());
            }
            _ if attempt < 2 => {
                progress.retry(0);
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
            _ => {}
        }
    }
    ensure!(accepted, "file upload to gateway failed; source unchanged");
    progress.advance(size as u64, Some(size as u64));
    Ok(
        json!({"artifact_id":operation,"path":v["path"],"file_name":name,"size":size,"sha256":sha,"state":"completed"}),
    )
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Blob {
    pub(crate) operation: String,
    pub(crate) device: String,
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) size: usize,
    pub(crate) sha256: String,
    pub(crate) expires: i64,
}
#[derive(Serialize, Deserialize)]
struct Link {
    operation: String,
    device: String,
    owner: String,
}
pub(crate) fn blob_path(g: &Gateway, id: &str) -> PathBuf {
    g.config
        .state_dir
        .join("file-objects")
        .join(format!("{id}.blob"))
}

pub fn routes(g: Gateway) -> Router {
    Router::new()
        .route("/device/file-source/{id}", get(get_source))
        .route("/device/files/{id}", post(receive_file))
        .route("/device/file-progress/{id}", get(get_received_progress))
        .route("/files/{key}/{name}", get(serve_file))
        .with_state(g)
}
async fn authorized_job(g: &Gateway, h: &HeaderMap, id: &str, tool: &str) -> Result<Job> {
    uuid::Uuid::parse_str(id).context("invalid operation id")?;
    let device = g.device(h).await?;
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT request,state FROM jobs WHERE id=? AND device=?")
            .bind(id)
            .bind(&device)
            .fetch_optional(&g.store.pool)
            .await?;
    let (request, state) = row.context("transfer operation not found")?;
    let job: Job = serde_json::from_str(&request)?;
    ensure!(
        job.tool == tool
            && job.owner == g.config.owner
            && (state == "dispatched" || state == "done"),
        "transfer operation is not authorized"
    );
    let scope = crate::tools::scope(tool).context("unknown transfer scope")?;
    ensure!(
        g.config
            .devices
            .iter()
            .any(|d| d.id == device && d.scopes.iter().any(|s| s == scope)),
        "transfer scope revoked"
    );
    Ok(job)
}
async fn get_source(
    State(g): State<Gateway>,
    HttpPath(id): HttpPath<String>,
    headers: HeaderMap,
) -> Response {
    if authorized_job(&g, &headers, &id, "file_upload")
        .await
        .is_err()
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    match g.store.get::<Value>("file_source", &id).await {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => match source_authorization_status(&g, &id).await {
            Ok(status) => (StatusCode::GONE, Json(status)).into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn get_received_progress(
    State(g): State<Gateway>,
    HttpPath(id): HttpPath<String>,
    headers: HeaderMap,
) -> Response {
    if authorized_job(&g, &headers, &id, "file_download")
        .await
        .is_err()
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    match g.store.get::<Value>("receive_progress", &id).await {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => Json(json!({"snapshot":{"bytes_done":0,"phase":"queued"}})).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
pub(crate) async fn cleanup(g: &Gateway) -> Result<u64> {
    for b in g.store.list::<Blob>("file_blob").await? {
        if b.expires <= now() {
            let _ = std::fs::remove_file(blob_path(g, &b.operation));
            sqlx::query("DELETE FROM kv WHERE kind='file_blob' AND key=?")
                .bind(&b.operation)
                .execute(&g.store.pool)
                .await?;
        }
    }
    g.store.prune().await?;
    let dir = g.config.state_dir.join("file-objects");
    std::fs::create_dir_all(&dir)?;
    let mut used = 0u64;
    let mut count = 0usize;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            // A failed/cancelled stream may drop its temporary file concurrently.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !meta.is_file() {
            continue;
        }
        if meta.modified()?.elapsed().unwrap_or_default()
            > Duration::from_secs(BLOB_TTL as u64 + 300)
            && entry.path().extension().is_none_or(|e| e != "part040")
        {
            std::fs::remove_file(entry.path())?;
        } else {
            used = used.saturating_add(meta.len());
            count += 1;
        }
    }
    ensure!(count < 512, "gateway artifact count limit reached");
    Ok(used)
}
async fn receive_file(
    State(g): State<Gateway>,
    HttpPath(id): HttpPath<String>,
    request: Request,
) -> Response {
    let headers = request.headers();
    let job = match authorized_job(&g, headers, &id, "file_download").await {
        Ok(j) => j,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    let size = headers
        .get("x-file-size")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok());
    let sha = headers
        .get("x-file-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let Some(size) = size.filter(|n| *n <= limit(&job.arguments).unwrap_or(0)) else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    if !valid_hash(&sha) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(_permit) = g.transfer_limits.try_acquire(&job.device_id, &id) else {
        return (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "2")]).into_response();
    };
    let saved = match g.store.get::<Blob>("file_blob", &id).await {
        Ok(saved) => saved,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if let Some(saved) = saved {
        if saved.sha256 != sha || saved.size != size || saved.expires <= now() {
            return StatusCode::CONFLICT.into_response();
        }
        // Drain and verify a repeated streaming request before acknowledging it.
        // Returning early can reset a sender that still has body bytes in flight.
        let verified = async {
            let mut stream = request.into_body().into_data_stream();
            let mut count = 0usize;
            let mut digest = Sha256::new();
            while let Some(bytes) = tokio::time::timeout(IDLE, stream.next()).await? {
                let bytes = bytes.context("duplicate stream failed")?;
                count = count
                    .checked_add(bytes.len())
                    .context("file size overflow")?;
                ensure!(count <= size, "duplicate exceeds declared size");
                digest.update(&bytes);
            }
            ensure!(
                count == size && format!("{:x}", digest.finalize()) == sha,
                "duplicate checksum mismatch"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        return match verified {
            Ok(()) => Json(json!({"size":size,"sha256":sha,"duplicate":true})).into_response(),
            Err(error) if error.is::<tokio::time::error::Elapsed>() => {
                StatusCode::REQUEST_TIMEOUT.into_response()
            }
            Err(_) => StatusCode::BAD_REQUEST.into_response(),
        };
    }
    let temporary = {
        let _allocation = g.transfer_limits.allocation.lock().await;
        let used = match cleanup(&g).await {
            Ok(n) => n,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        if used.saturating_add(size as u64) > DISK_CAP
            || ensure_storage_capacity(&g.config.state_dir, size as u64).is_err()
        {
            return StatusCode::INSUFFICIENT_STORAGE.into_response();
        }
        let temporary =
            match tempfile::NamedTempFile::new_in(g.config.state_dir.join("file-objects")) {
                Ok(file) => file,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
        // Reserve the full logical size before releasing quota coordination. Other
        // concurrent streams count this reservation, not its current partial length.
        if temporary.as_file().set_len(size as u64).is_err() {
            return StatusCode::INSUFFICIENT_STORAGE.into_response();
        }
        temporary
    };
    let progress = crate::progress::Progress::new(&id);
    progress.phase("transferring");
    progress.advance(0, Some(size as u64));
    record_received_progress(&g, &progress).await;
    let result = async {
        let mut file = tokio::fs::File::from_std(temporary.reopen()?);
        let mut stream = request.into_body().into_data_stream();
        let mut count = 0usize;
        let mut digest = Sha256::new();
        let mut last_report = std::time::Instant::now();
        while let Some(bytes) = tokio::time::timeout(IDLE, stream.next()).await? {
            let bytes = bytes.context("incoming file stream failed")?;
            count = count
                .checked_add(bytes.len())
                .context("file size overflow")?;
            ensure!(count <= size, "incoming file exceeds declared size");
            digest.update(&bytes);
            file.write_all(&bytes).await?;
            progress.advance(count as u64, Some(size as u64));
            if count == bytes.len() || last_report.elapsed() >= Duration::from_millis(500) {
                record_received_progress(&g, &progress).await;
                last_report = std::time::Instant::now();
            }
        }
        progress.phase("verifying");
        record_received_progress(&g, &progress).await;
        ensure!(
            count == size && format!("{:x}", digest.finalize()) == sha,
            "incoming file checksum or size mismatch"
        );
        file.sync_all().await?;
        drop(file);
        let name = files::relative(files::text(&job.arguments, "path")?)?
            .file_name()
            .context("missing file name")?
            .to_string_lossy()
            .into_owned();
        ensure!(name.len() <= 255, "file name too long");
        let blob = Blob {
            operation: id.clone(),
            device: job.device_id,
            owner: job.owner,
            name,
            size,
            sha256: sha.clone(),
            expires: now() + BLOB_TTL,
        };
        // Coordinate only publication metadata, not the network/body lifetime.
        let _allocation = g.transfer_limits.allocation.lock().await;
        temporary.persist(blob_path(&g, &id))?;
        g.store.put("file_blob", &id, &blob, i64::MAX).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    progress.phase(if result.is_ok() {
        "completed"
    } else {
        "failed"
    });
    record_received_progress(&g, &progress).await;
    match result {
        Ok(()) => Json(json!({"size":size,"sha256":sha})).into_response(),
        Err(error) if error.is::<tokio::time::error::Elapsed>() => {
            StatusCode::REQUEST_TIMEOUT.into_response()
        }
        Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}
async fn record_received_progress(g: &Gateway, progress: &crate::progress::Progress) {
    if let Some(snapshot) = progress.snapshot() {
        let _ = g
            .store
            .put(
                "receive_progress",
                &snapshot.operation_id,
                &json!({"snapshot":snapshot,"reported_at":now(),"origin":"gateway_receiver"}),
                now() + 86400,
            )
            .await;
    }
}
/// Called only after the original operation and principal have been authorized.
pub async fn decorate(g: &Gateway, job: &Job, result: &mut Value) -> Result<()> {
    if job.tool != "file_download"
        || result.get("error").is_some()
        || result.get("state").is_some_and(|s| s != "completed")
    {
        return Ok(());
    }
    let blob: Blob = g
        .store
        .get("file_blob", &job.id)
        .await?
        .context("download artifact unavailable")?;
    ensure!(
        blob.device == job.device_id && blob.owner == job.owner && blob.expires > now(),
        "download artifact expired; source unchanged"
    );
    ensure!(
        result["sha256"] == blob.sha256 && result["size"] == blob.size,
        "download artifact receipt mismatch"
    );
    let key = random();
    let expires = (now() + LINK_TTL).min(blob.expires);
    g.store
        .put(
            "file_link",
            &hash(&key),
            &Link {
                operation: job.id.clone(),
                device: blob.device.clone(),
                owner: blob.owner.clone(),
            },
            expires,
        )
        .await?;
    let name: String = url::form_urlencoded::byte_serialize(blob.name.as_bytes()).collect();
    result["download_url"] = json!(format!("{}/files/{key}/{name}", g.config.public_url));
    result["expires_at"] = json!(expires);
    result["artifact_expires_at"] = json!(blob.expires);
    result["mime_type"] = json!("application/octet-stream");
    Ok(())
}
async fn serve_file(
    State(g): State<Gateway>,
    HttpPath((key, _name)): HttpPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if key.len() != 64 || !key.bytes().all(|c| c.is_ascii_hexdigit()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let link = match g.store.get::<Link>("file_link", &hash(&key)).await {
        Ok(Some(link)) => link,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    if link.owner != g.config.owner
        || !g
            .config
            .devices
            .iter()
            .any(|d| d.id == link.device && d.scopes.iter().any(|s| s == "code:read"))
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let blob = match g.store.get::<Blob>("file_blob", &link.operation).await {
        Ok(Some(b)) if b.expires > now() && b.device == link.device && b.owner == link.owner => b,
        _ => return StatusCode::GONE.into_response(),
    };
    let mut file = match tokio::fs::File::open(blob_path(&g, &blob.operation)).await {
        Ok(f) => f,
        Err(_) => return StatusCode::GONE.into_response(),
    };
    if !matches!(file.metadata().await, Ok(meta) if meta.is_file() && meta.len() == blob.size as u64)
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let mut start = 0usize;
    let mut end = blob.size;
    let mut status = StatusCode::OK;
    // RFC 9110: mismatched/weak/date If-Range uses the full immutable body.
    let etag = format!("\"{}\"", blob.sha256);
    if let Some(range) = headers.get(header::RANGE)
        && headers
            .get(header::IF_RANGE)
            .is_none_or(|value| value == etag.as_str())
    {
        let parsed = (|| -> Option<(usize, usize)> {
            let (a, b) = range
                .to_str()
                .ok()?
                .strip_prefix("bytes=")?
                .split_once('-')?;
            if a.is_empty() {
                let suffix = b.parse::<usize>().ok()?;
                return (suffix > 0 && blob.size > 0)
                    .then_some((blob.size.saturating_sub(suffix), blob.size));
            }
            let a = a.parse::<usize>().ok()?;
            let b = if b.is_empty() {
                blob.size
            } else {
                b.parse::<usize>().ok()?.checked_add(1)?.min(blob.size)
            };
            (a < b && b <= blob.size).then_some((a, b))
        })();
        let Some((a, b)) = parsed else {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", blob.size))],
            )
                .into_response();
        };
        start = a;
        end = b;
        status = StatusCode::PARTIAL_CONTENT;
    }
    if file
        .seek(std::io::SeekFrom::Start(start as u64))
        .await
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let name: String = url::form_urlencoded::byte_serialize(blob.name.as_bytes()).collect();
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, end - start)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"download.bin\"; filename*=UTF-8''{name}"),
        )
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, format!("\"{}\"", blob.sha256));
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{}", end - 1, blob.size),
        );
    }
    builder
        .body(Body::from_stream(ReaderStream::new(
            file.take((end - start) as u64),
        )))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_allowlist_and_private_addresses() {
        let own = "https://mcp.example:8443";
        for bad in [
            "http://files.oaiusercontent.com/f",
            "https://files.oaiusercontent.com.evil.test/f",
            "https://127.0.0.1/f",
            "https://files.oaiusercontent.com@evil.test/f",
            "https://files.oaiusercontent.com:22/f",
            "https://mcp.example/oauth/token",
        ] {
            assert!(source_url(bad, own).is_err(), "{bad}");
        }
        for good in [
            "https://files.oaiusercontent.com/f?signature=synthetic",
            "https://account.blob.core.windows.net/files/a",
            "https://mcp.example/files/a/b",
            "https://mcp.example:8443/files/a/b",
        ] {
            assert!(source_url(good, own).is_ok(), "{good}");
        }
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "100.64.0.1",
            "198.18.0.1",
            "::1",
            "fc00::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        assert!(public_ip("1.1.1.1".parse().unwrap()));
        assert!(public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
    #[test]
    fn binary_publish_default_no_clobber_and_hash_conflicts() {
        let d = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: random(),
            device_id: random(),
            root: d.path().canonicalize().unwrap(),
        };
        let bytes = b"\0\xff\xfe\x01binary\0";
        let (stage, mut file) = Staged::new(&ws, &json!({"path":"assets/a.bin"})).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        stage.publish().unwrap();
        assert_eq!(std::fs::read(ws.root.join("assets/a.bin")).unwrap(), bytes);
        assert!(Staged::new(&ws, &json!({"path":"assets/a.bin"})).is_err());
        assert!(Staged::new(&ws, &json!({"path":"../outside"})).is_err());
        let (stage, mut file) = Staged::new(&ws, &json!({"path":"new.bin"})).unwrap();
        file.write_all(bytes).unwrap();
        drop(file);
        std::fs::write(ws.root.join("new.bin"), b"concurrent").unwrap();
        assert!(stage.publish().is_err());
        assert_eq!(
            std::fs::read(ws.root.join("new.bin")).unwrap(),
            b"concurrent"
        );
        assert!(verify_hash(Some(&hash(b"wrong")), &hash(bytes)).is_err());
    }
    #[test]
    fn bounded_snapshot_and_failed_stage_cleanup() {
        assert!(digest_reader(&b"0123456789"[..], 3, None).is_err());
        let d = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: random(),
            device_id: random(),
            root: d.path().canonicalize().unwrap(),
        };
        let (stage, file) = Staged::new(&ws, &json!({"path":"a.bin"})).unwrap();
        drop(file);
        drop(stage);
        assert_eq!(std::fs::read_dir(&ws.root).unwrap().count(), 0);
    }
    #[cfg(unix)]
    #[test]
    fn capability_transfer_rejects_symlink_escape() {
        let d = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let ws = Workspace {
            id: random(),
            device_id: random(),
            root: d.path().canonicalize().unwrap(),
        };
        std::os::unix::fs::symlink(outside.path(), ws.root.join("escape")).unwrap();
        assert!(Staged::new(&ws, &json!({"path":"escape/a.bin"})).is_err());
        std::fs::write(outside.path().join("target"), b"untouched").unwrap();
        std::os::unix::fs::symlink(outside.path().join("target"), ws.root.join("link")).unwrap();
        assert!(
            Staged::new(
                &ws,
                &json!({"path":"link","expected_version":hash(b"untouched")})
            )
            .is_err()
        );
    }
}

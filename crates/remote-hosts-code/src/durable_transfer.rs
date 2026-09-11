#[cfg(test)]
#[path = "durable_transfer_tests.rs"]
mod tests;
// Device transfer state machine. Restart recovery is restricted to file jobs with
// durable checkpoints; arbitrary shell/mutation outcomes are never re-executed.
use crate::{
    AgentConfig,
    files::{self, Workspace},
    gateway::Job,
    progress::Progress,
    store::Store,
    transfer_control::Control,
    transfer_journal::{self, Journal},
    transfer_receiver::CHUNK,
    transfers,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io::{Seek, SeekFrom},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct Fault {
    pub state: &'static str,
    pub code: &'static str,
}
impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code)
    }
}
impl std::error::Error for Fault {}
fn paused(code: &'static str) -> anyhow::Error {
    Fault {
        state: "paused",
        code,
    }
    .into()
}
fn needs_source() -> anyhow::Error {
    Fault {
        state: "awaiting_source",
        code: "source_authorization_required",
    }
    .into()
}
fn cancelled() -> anyhow::Error {
    Fault {
        state: "cancelled",
        code: "cancel_requested",
    }
    .into()
}
fn http() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .build()?)
}
async fn small_json(mut response: reqwest::Response) -> Result<Value> {
    let mut body = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), response.chunk()).await {
            Ok(Ok(Some(part))) if body.len() + part.len() <= 16384 => body.extend_from_slice(&part),
            Ok(Ok(None)) => break,
            _ => return Err(paused("control_response_unavailable")),
        }
    }
    serde_json::from_slice(&body).map_err(|_| paused("invalid_control_response"))
}
async fn request_json(request: reqwest::RequestBuilder) -> Result<Value> {
    let response = request
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| paused("gateway_connection"))?;
    let status = response.status().as_u16();
    if status == 200 {
        return small_json(response).await;
    }
    if matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504) {
        return Err(paused("gateway_retry_required"));
    }
    Err(anyhow::anyhow!("gateway_transfer_rejected: HTTP {status}"))
}
struct Guard<'a> {
    client: reqwest::Client,
    config: &'a AgentConfig,
    id: &'a str,
    revision: u64,
    next_check: tokio::time::Instant,
}
impl<'a> Guard<'a> {
    async fn probe(&self) -> Result<Control> {
        let value = request_json(
            self.client
                .get(format!(
                    "{}/device/transfer-control/{}",
                    self.config.gateway_url, self.id
                ))
                .bearer_auth(&self.config.device_token),
        )
        .await?;
        ensure!(value["protocol"] == 2, "transfer_protocol_2_required");
        Ok(Control {
            revision: value["revision"]
                .as_u64()
                .context("invalid transfer revision")?,
            cancel_requested: value["cancel_requested"] == true,
        })
    }
    async fn check(&mut self) -> Result<()> {
        let c = self.probe().await?;
        self.next_check = tokio::time::Instant::now() + Duration::from_secs(1);
        if c.cancel_requested {
            return Err(cancelled());
        }
        if c.revision != self.revision {
            return Err(paused("transfer_attempt_superseded"));
        }
        Ok(())
    }
    async fn wait<T>(&mut self, future: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result=&mut future=>return result,
                _=tokio::time::sleep_until(self.next_check)=>self.check().await?,
            }
        }
    }
}

pub(crate) fn is_file(tool: &str) -> bool {
    matches!(tool, "file_upload" | "file_download")
}
pub(crate) fn is_suspended(v: &Value) -> bool {
    matches!(v["state"].as_str(), Some("paused" | "awaiting_source")) && v["resumable"] == true
}

pub(crate) async fn run(
    config: &AgentConfig,
    ws: &Workspace,
    job: &Job,
    p: &Progress,
    store: &Store,
    scopes: Arc<crate::scheduler::WriteScopes>,
) -> Result<Value> {
    let mut j = Journal::load_or_create(config, ws, job, store, scopes.clone()).await?;
    if let Some(result) = j.result.clone() {
        return Ok(result);
    }
    let mut guard = Guard {
        client: http()?,
        config,
        id: &job.id,
        revision: j.revision,
        next_check: tokio::time::Instant::now(),
    };
    let outcome = async {
        let c = guard.probe().await?;
        if j.expires_at <= crate::now() {
            anyhow::bail!("transfer_expired: retained checkpoint is no longer available");
        }
        if !c.cancel_requested
            && matches!(j.phase.as_str(), "paused" | "awaiting_source")
            && c.revision == j.revision
        {
            return Err(Fault {
                state: if j.phase == "awaiting_source" {
                    "awaiting_source"
                } else {
                    "paused"
                },
                code: "explicit_resume_required",
            }
            .into());
        }
        j.revision = c.revision;
        guard.revision = c.revision;
        // An incomplete publication has its own intent and content hash. Verify
        // target or original precondition before doing anything with a new URL.
        if j.phase == "publishing" {
            return publish(
                config,
                ws,
                job,
                p,
                store,
                scopes.clone(),
                &mut j,
                &mut guard,
            )
            .await;
        }
        if c.cancel_requested {
            return Err(cancelled());
        }
        j.phase = "transferring".into();
        j.save(store).await?;
        if job.tool == "file_upload" {
            inbound(config, job, p, store, &mut j, &mut guard).await?;
            publish(config, ws, job, p, store, scopes, &mut j, &mut guard).await
        } else {
            outbound(config, ws, job, p, store, &mut j, &mut guard).await
        }
    }
    .await;
    // A concurrent cancellation can cause the data endpoint to reject before
    // the next monitor tick. Resolve the saved control intent rather than
    // misreporting that acknowledgement as a permanent transport failure.
    let outcome = if outcome.is_err()
        && j.phase != "publishing"
        && guard.probe().await.is_ok_and(|c| c.cancel_requested)
    {
        Err(cancelled())
    } else {
        outcome
    };
    let mut result = match outcome {
        Ok(result) => result,
        Err(error)
            if error
                .downcast_ref::<Fault>()
                .is_some_and(|f| f.state == "cancelled") =>
        {
            p.phase("cancelling");
            // Drop of the network future does not mean the disk writer finished.
            // Each writer is flushed at its cancellation boundary below.
            if let Err(e) = cleanup(config, job, &j, &guard.client, true).await {
                return suspended(
                    config,
                    store,
                    &mut j,
                    p,
                    "paused",
                    "cancel_cleanup_pending",
                    Some(e.to_string()),
                )
                .await;
            }
            json!({"operation_id":job.id,"state":"cancelled","cancelled":true,"cleanup_complete":true,
                "destination_changed":false,"source_unchanged":true,"transfer_revision":j.revision})
        }
        Err(error) if error.downcast_ref::<Fault>().is_some() => {
            let f = error.downcast_ref::<Fault>().unwrap();
            return suspended(config, store, &mut j, p, f.state, f.code, None).await;
        }
        Err(error) => {
            // Keep publication intent for recovery rather than describing a
            // potentially committed rename as a clean no-op.
            if j.phase == "publishing" {
                return suspended(
                    config,
                    store,
                    &mut j,
                    p,
                    "paused",
                    "publication_recovery_required",
                    None,
                )
                .await;
            }
            let _ = cleanup(config, job, &j, &guard.client, false).await;
            json!({"operation_id":job.id,"state":"failed","error":"transfer_failed","message":error.to_string(),
                "resumable":false,"transfer_revision":j.revision,"destination_changed":false})
        }
    };
    result["transfer_revision"] = json!(j.revision);
    j.phase = result["state"].as_str().unwrap_or("failed").into();
    j.result = Some(result.clone());
    j.save(store).await?;
    if result["state"] == "completed" {
        let _ = tokio::fs::remove_file(j.data_path(config)).await;
    }
    Ok(result)
}
async fn suspended(
    config: &AgentConfig,
    store: &Store,
    j: &mut Journal,
    p: &Progress,
    state: &str,
    code: &str,
    _detail: Option<String>,
) -> Result<Value> {
    // Preserve a publishing marker across pause; it enables crash reconciliation.
    let publication_pending = j.phase == "publishing";
    j.phase = if publication_pending {
        "publishing".into()
    } else {
        state.into()
    };
    j.save(store).await?;
    p.phase(if state == "awaiting_source" {
        "awaiting_source"
    } else {
        "paused"
    });
    let value = json!({"operation_id":j.operation_id,"state":state,"resumable":true,"pending":false,
        "diagnostic":{"code":code},"next_action":"transfer_resume","transfer_revision":j.revision,
        "confirmed_bytes":j.offset,"total_bytes":j.total,"expires_at":j.expires_at,"cleanup_complete":false,
        "publication_pending":publication_pending,"progress":p.snapshot()});
    // If this bounded status transmission is interrupted, the gateway's expired
    // dispatch lease retrieves the same paused journal; no file work is redone.
    let client = http()?;
    for _ in 0..3 {
        if request_json(
            client
                .post(format!(
                    "{}/device/transfer-status/{}",
                    config.gateway_url, j.operation_id
                ))
                .bearer_auth(&config.device_token)
                .json(&value),
        )
        .await
        .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(value)
}
fn clean_publication(config: &AgentConfig, j: &Journal) -> Result<()> {
    let Some(name) = &j.publication_temp else {
        return Ok(());
    };
    ensure!(
        std::path::Path::new(name)
            .file_name()
            .and_then(|v| v.to_str())
            == Some(name)
            && name.starts_with(".remote-hosts-transfer-")
            && name.ends_with(".tmp"),
        "invalid publication cleanup path"
    );
    let canonical = j.root.canonicalize()?;
    ensure!(
        canonical == j.root && config.roots.iter().any(|r| canonical.starts_with(r)),
        "workspace authorization changed"
    );
    let ws = Workspace {
        id: j.workspace_id.clone(),
        device_id: j.device_id.clone(),
        root: j.root.clone(),
    };
    let relative = files::relative(&j.path)?;
    let parent = relative
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let dir = transfers::root(&ws)?.open_dir(parent)?;
    let meta = match dir.symlink_metadata(name) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        meta.is_file() && !meta.is_symlink(),
        "publication cleanup identity conflict"
    );
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt;
        ensure!(
            j.publication_identity == Some((meta.dev(), meta.ino())),
            "publication cleanup identity conflict"
        );
    }
    #[cfg(not(unix))]
    anyhow::bail!("publication cleanup needs platform file identity");
    dir.remove_file(name)?;
    Ok(())
}
async fn cleanup(
    config: &AgentConfig,
    job: &Job,
    j: &Journal,
    client: &reqwest::Client,
    cancel: bool,
) -> Result<()> {
    if cancel && job.tool == "file_download" {
        let result = request_json(
            client
                .post(format!(
                    "{}/device/transfers/{}/abort",
                    config.gateway_url, job.id
                ))
                .bearer_auth(&config.device_token),
        )
        .await?;
        ensure!(
            result["cleanup_complete"] == true,
            "remote cleanup not confirmed"
        );
    }
    clean_publication(config, j)?;
    match tokio::fs::remove_file(j.data_path(config)).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

async fn inbound(
    config: &AgentConfig,
    job: &Job,
    p: &Progress,
    store: &Store,
    j: &mut Journal,
    guard: &mut Guard<'_>,
) -> Result<()> {
    if j.total == Some(j.offset) {
        let path = j.data_path(config);
        let offset = j.offset;
        let prefix = j.prefix_sha256.clone();
        let expected = job.arguments["sha256"].as_str().map(str::to_owned);
        let (_, digest) = tokio::task::spawn_blocking(move || {
            transfer_journal::restore_prefix(
                transfer_journal::open_data(&path, false)?,
                offset,
                &prefix,
            )
        })
        .await??;
        let sha = format!("{:x}", digest.finalize());
        ensure!(expected.is_none_or(|e| e == sha), "sha256_mismatch");
        j.content_sha256 = Some(sha);
        j.save(store).await?;
        p.restore(j.offset, j.total, j.offset);
        return Ok(());
    }
    p.phase("connecting");
    let response = guard
        .client
        .get(format!(
            "{}/device/file-source/{}",
            config.gateway_url, job.id
        ))
        .bearer_auth(&config.device_token)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| paused("source_lookup_network"))?;
    if matches!(response.status().as_u16(), 401 | 403 | 404 | 410) {
        return Err(needs_source());
    }
    if !response.status().is_success() {
        return Err(paused("source_lookup_unavailable"));
    }
    let source = small_json(response).await?;
    let u = transfers::source_url(files::text(&source, "download_url")?, &config.gateway_url)?;
    let client = transfers::source_client(&u)
        .await
        .map_err(|_| paused("source_dns_or_connection"))?;
    let path = j.data_path(config);
    let offset = j.offset;
    let prefix = j.prefix_sha256.clone();
    let (raw, digest) = tokio::task::spawn_blocking(move || {
        let file = transfer_journal::open_data(&path, true)?;
        transfer_journal::restore_prefix(file, offset, &prefix)
    })
    .await??;
    let mut output = tokio::fs::File::from_std(raw);
    p.restore(j.offset, j.total, j.offset);
    let result = guard
        .wait(fetch(&client, &u, &mut output, job, p, store, j, digest))
        .await;
    // Tokio file writes may use a blocking worker. Drain it before cancelling,
    // deleting or handing the same path to a later attempt.
    output.flush().await?;
    drop(output);
    result
}
fn strong_etag(h: &reqwest::header::HeaderMap) -> Option<String> {
    h.get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.starts_with('"') && v.ends_with('"') && v.len() <= 1024)
        .map(str::to_owned)
}
fn content_range(h: &reqwest::header::HeaderMap) -> Result<(u64, u64, u64)> {
    let value = h
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .context("missing Content-Range")?;
    let (span, total) = value
        .strip_prefix("bytes ")
        .context("invalid Content-Range unit")?
        .split_once('/')
        .context("invalid Content-Range")?;
    let (start, end) = span.split_once('-').context("invalid Content-Range")?;
    let (a, b, n) = (
        start.parse::<u64>()?,
        end.parse::<u64>()?,
        total.parse::<u64>()?,
    );
    ensure!(a <= b && b < n, "invalid Content-Range bounds");
    Ok((a, b + 1, n))
}
async fn checkpoint(
    output: &mut tokio::fs::File,
    store: &Store,
    j: &mut Journal,
    p: &Progress,
    digest: &Sha256,
    offset: u64,
) -> Result<()> {
    output.flush().await?;
    output.sync_all().await?;
    j.offset = offset;
    j.prefix_sha256 = format!("{:x}", digest.clone().finalize());
    j.save(store).await?;
    p.confirm(offset);
    Ok(())
}
// The transport, durable state and live progress remain explicit at this boundary.
#[allow(clippy::too_many_arguments)]
async fn fetch(
    client: &reqwest::Client,
    url: &reqwest::Url,
    output: &mut tokio::fs::File,
    job: &Job,
    p: &Progress,
    store: &Store,
    j: &mut Journal,
    mut digest: Sha256,
) -> Result<()> {
    let expected = job.arguments["sha256"].as_str();
    let mut offset = j.offset;
    let mut retries = 0u32;
    loop {
        if j.total == Some(offset) {
            break;
        }
        if offset > 0 && j.etag.is_none() && expected.is_none() {
            output.set_len(0).await?;
            output.seek(SeekFrom::Start(0)).await?;
            offset = 0;
            digest = Sha256::new();
            j.total = None;
            checkpoint(output, store, j, p, &digest, 0).await?;
            p.restore(0, None, 0);
        }
        p.phase("connecting");
        let mut request = client
            .get(url.clone())
            .header(reqwest::header::ACCEPT_ENCODING, "identity");
        if offset > 0 {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
            if let Some(etag) = &j.etag {
                request = request.header(reqwest::header::IF_RANGE, etag);
            }
        }
        let response = tokio::time::timeout(Duration::from_secs(60), request.send()).await;
        let mut response = match response {
            Ok(Ok(r)) => r,
            _ => {
                retry(p, offset, &mut retries).await?;
                continue;
            }
        };
        let status = response.status().as_u16();
        if matches!(status, 401 | 403 | 404 | 410) {
            return Err(needs_source());
        }
        if status >= 500 || matches!(status, 408 | 429) {
            retry(p, offset, &mut retries).await?;
            continue;
        }
        ensure!(
            response
                .headers()
                .get(reqwest::header::CONTENT_ENCODING)
                .is_none_or(|v| v == "identity"),
            "encoded transfer cannot be resumed"
        );
        let etag = strong_etag(response.headers());
        let end = if status == 206 {
            let (start, end, total) = content_range(response.headers())?;
            ensure!(
                start == offset && total <= j.reserved_bytes as u64,
                "range_offset_or_size_mismatch"
            );
            ensure!(
                j.total.is_none_or(|t| t == total),
                "source_changed: total size differs"
            );
            if let (Some(old), Some(new)) = (&j.etag, &etag) {
                ensure!(old == new, "source_changed: ETag differs");
            }
            ensure!(
                j.etag.is_none() || etag.is_some() || expected.is_some(),
                "resume_missing_strong_validator"
            );
            ensure!(
                response.content_length().is_none_or(|n| n == end - start),
                "range_body_length_mismatch"
            );
            j.total = Some(total);
            Some(end)
        } else if status == 200 {
            if offset > 0 {
                // Server ignored Range or the source changed. A full body replaces
                // staging; it is NEVER appended to the previous representation.
                output.set_len(0).await?;
                output.seek(SeekFrom::Start(0)).await?;
                offset = 0;
                digest = Sha256::new();
            }
            j.total = response.content_length();
            ensure!(
                j.total.is_none_or(|n| n <= j.reserved_bytes as u64),
                "file exceeds max_bytes"
            );
            checkpoint(output, store, j, p, &digest, offset).await?;
            p.restore(offset, j.total, offset);
            j.total
        } else {
            anyhow::bail!("source_http_rejected: {status}");
        };
        if etag.is_some() || offset == 0 {
            j.etag = etag;
        }
        ensure!(
            !(status == 206
                && end.zip(j.total).is_some_and(|(e, t)| e < t)
                && j.etag.is_none()
                && expected.is_none()),
            "partial source lacks identity"
        );
        j.save(store).await?;
        p.phase("transferring");
        p.advance(offset, j.total);
        let mut interrupted = false;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(60), response.chunk()).await;
            match next {
                Ok(Ok(Some(bytes))) => {
                    let mut data = bytes.as_ref();
                    while !data.is_empty() {
                        let boundary = (offset / CHUNK as u64 + 1) * CHUNK as u64;
                        let take = data.len().min((boundary - offset) as usize);
                        let next = offset.checked_add(take as u64).context("size overflow")?;
                        ensure!(
                            next <= j.reserved_bytes as u64 && end.is_none_or(|e| next <= e),
                            "response exceeds size limit"
                        );
                        output.write_all(&data[..take]).await?;
                        digest.update(&data[..take]);
                        offset = next;
                        data = &data[take..];
                        p.advance(offset, j.total);
                        if offset == boundary {
                            checkpoint(output, store, j, p, &digest, offset).await?;
                        }
                    }
                }
                Ok(Ok(None)) => break,
                _ => {
                    interrupted = true;
                    break;
                }
            }
        }
        checkpoint(output, store, j, p, &digest, offset).await?;
        if interrupted || end.is_some_and(|e| e != offset) {
            retry(p, offset, &mut retries).await?;
            continue;
        }
        if j.total.is_some_and(|t| offset < t) {
            continue;
        }
        break;
    }
    p.phase("verifying");
    let sha = format!("{:x}", digest.finalize());
    ensure!(expected.is_none_or(|s| s == sha), "sha256_mismatch");
    j.content_sha256 = Some(sha);
    j.total = Some(offset);
    j.offset = offset;
    j.save(store).await?;
    p.confirm(offset);
    Ok(())
}
async fn retry(p: &Progress, offset: u64, count: &mut u32) -> Result<()> {
    *count += 1;
    if *count > 6 {
        return Err(paused("network_retry_exhausted"));
    }
    p.retry(offset);
    tokio::time::sleep(Duration::from_millis(
        250u64 * (1u64 << (*count - 1).min(4)),
    ))
    .await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publish(
    config: &AgentConfig,
    ws: &Workspace,
    job: &Job,
    p: &Progress,
    store: &Store,
    scopes: Arc<crate::scheduler::WriteScopes>,
    j: &mut Journal,
    guard: &mut Guard<'_>,
) -> Result<Value> {
    let sha = j
        .content_sha256
        .clone()
        .context("missing verified content")?;
    // Detect already committed rename after process restart before consulting
    // cancellation: an irreversible completed publication wins that race.
    if j.phase == "publishing" {
        let copy = ws.clone();
        let path = j.path.clone();
        let expected = sha.clone();
        let matches = tokio::task::spawn_blocking(move || -> Result<bool> {
            let root = transfers::root(&copy)?;
            Ok(transfers::existing(&root, std::path::Path::new(&path))? == expected)
        })
        .await??;
        if matches {
            clean_publication(config, j)?;
            return Ok(completed_upload(job, j, &sha, true));
        }
    }
    guard.check().await?;
    p.phase("waiting_resource");
    let write = guard.wait(scopes.acquire(&ws.root)).await?;
    guard.check().await?;
    let canonical = ws.root.canonicalize()?;
    ensure!(
        canonical == ws.root && config.roots.iter().any(|r| canonical.starts_with(r)),
        "workspace authorization changed"
    );
    clean_publication(config, j)?;
    let (stage, mut output) = transfers::Staged::new(ws, &job.arguments)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = output.metadata()?;
        j.publication_identity = Some((meta.dev(), meta.ino()));
    }
    j.publication_temp = Some(stage.temporary.clone());
    j.phase = "publishing".into();
    j.save(store).await?;
    p.phase("publishing");
    let path = j.data_path(config);
    let expected = sha.clone();
    let max = j.reserved_bytes;
    // Never drop this critical section on cancellation. It is a bounded local
    // copy + atomic publication, and retains the workspace lock to completion.
    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = write;
        let input = transfer_journal::open_data(&path, false)?;
        let (_, actual) = transfers::digest_reader(input, max, Some(&mut output))?;
        ensure!(
            actual == expected,
            "checkpoint content changed before publication"
        );
        output.sync_all()?;
        drop(output);
        stage.publish()
    })
    .await??;
    Ok(completed_upload(job, j, &sha, false))
}
fn completed_upload(job: &Job, j: &Journal, sha: &str, recovered: bool) -> Value {
    json!({"operation_id":job.id,"path":j.path,"size":j.total,"sha256":sha,"version":sha,
        "state":"completed","atomic":true,"publication_recovered":recovered,
        "transfer_revision":j.revision,"checkpoint_bytes":CHUNK,"source_file_id":job.arguments["file"]["file_id"]})
}

async fn outbound(
    config: &AgentConfig,
    ws: &Workspace,
    job: &Job,
    p: &Progress,
    store: &Store,
    j: &mut Journal,
    guard: &mut Guard<'_>,
) -> Result<Value> {
    if j.content_sha256.is_none() {
        p.phase("snapshot");
        let root_ws = ws.clone();
        let args = job.arguments.clone();
        let path = j.data_path(config);
        let max = j.reserved_bytes;
        let (size, sha) = tokio::task::spawn_blocking(move || -> Result<_> {
            let relative = files::relative(files::text(&args, "path")?)?;
            let root = transfers::root(&root_ws)?;
            let meta = root.symlink_metadata(relative)?;
            ensure!(
                meta.is_file() && !meta.is_symlink() && meta.len() <= max as u64,
                "invalid source file"
            );
            let mut output = transfer_journal::open_data(&path, true)?;
            output.set_len(0)?;
            output.seek(SeekFrom::Start(0))?;
            let result = transfers::digest_reader(root.open(relative)?, max, Some(&mut output))?;
            if let Some(expected) = args["expected_version"].as_str() {
                ensure!(expected == result.1, "source_version_conflict");
            }
            output.sync_all()?;
            Ok(result)
        })
        .await??;
        j.total = Some(size as u64);
        j.content_sha256 = Some(sha);
        j.save(store).await?;
    }
    let sha = j.content_sha256.clone().unwrap();
    let size = j.total.context("missing snapshot size")?;
    let path = j.data_path(config);
    let check_path = path.clone();
    let expected = sha.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let actual = transfers::digest_reader(
            transfer_journal::open_data(&check_path, false)?,
            size as usize,
            None,
        )?;
        ensure!(
            actual.0 as u64 == size && actual.1 == expected,
            "snapshot_integrity_mismatch"
        );
        Ok(())
    })
    .await??;
    let endpoint = format!("{}/device/transfers/{}", config.gateway_url, job.id);
    let mut status = guard
        .wait(request_json(
            guard
                .client
                .post(&endpoint)
                .bearer_auth(&config.device_token)
                .json(&json!({"size":size,"sha256":sha})),
        ))
        .await?;
    let mut retries = 0u32;
    let mut file = tokio::fs::File::open(path).await?;
    p.phase("transferring");
    let mut first = true;
    loop {
        ensure!(
            status["size"] == size && status["sha256"] == sha && status["protocol"] == 2,
            "gateway snapshot identity mismatch"
        );
        let offset = status["confirmed_bytes"]
            .as_u64()
            .context("missing gateway offset")?;
        ensure!(offset <= size, "gateway offset beyond source");
        j.offset = offset;
        j.save(store).await?;
        if first {
            p.restore(offset, Some(size), offset);
            first = false;
        } else {
            p.advance(offset, Some(size));
            p.confirm(offset);
        }
        if offset == size {
            break;
        }
        guard.check().await?;
        let len = (size - offset).min(CHUNK as u64) as usize;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes).await?;
        let digest = crate::hash(&bytes);
        let consumed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = consumed.clone();
        let parts: Vec<Vec<u8>> = bytes.chunks(16384).map(<[u8]>::to_vec).collect();
        let stream = futures_util::stream::iter(parts.into_iter().map(move |part| {
            counter.fetch_add(part.len() as u64, std::sync::atomic::Ordering::Relaxed);
            Ok::<_, std::io::Error>(part)
        }));
        let request = guard
            .client
            .post(format!("{endpoint}/chunk"))
            .bearer_auth(&config.device_token)
            .header("x-transfer-offset", offset)
            .header("x-chunk-sha256", digest)
            .header(reqwest::header::CONTENT_LENGTH, len)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(reqwest::Body::wrap_stream(stream));
        let receiver = guard
            .client
            .get(format!(
                "{}/device/file-progress/{}",
                config.gateway_url, job.id
            ))
            .bearer_auth(&config.device_token);
        // Watch sender demand AND receiver activity. A slow but advancing chunk
        // has no fixed overall deadline; only loss of progress times out.
        let sending = async {
            let response =
                crate::transfer_watch::send(request, receiver, consumed, Duration::from_secs(60))
                    .await
                    .map_err(|_| paused("gateway_chunk_stalled"))?;
            if response.status().is_success() {
                small_json(response).await
            } else if matches!(
                response.status().as_u16(),
                408 | 425 | 429 | 500 | 502 | 503 | 504
            ) {
                Err(paused("gateway_chunk_retry"))
            } else {
                Err(anyhow::anyhow!(
                    "chunk_rejected: HTTP {}",
                    response.status().as_u16()
                ))
            }
        };
        match guard.wait(sending).await {
            Ok(v) => {
                status = v;
                p.advance(
                    status["confirmed_bytes"].as_u64().unwrap_or(offset),
                    Some(size),
                );
                p.confirm(status["confirmed_bytes"].as_u64().unwrap_or(offset));
            }
            Err(e)
                if e.downcast_ref::<Fault>()
                    .is_some_and(|f| f.state == "paused") =>
            {
                retry(p, offset, &mut retries).await?;
                status = guard
                    .wait(request_json(
                        guard
                            .client
                            .get(&endpoint)
                            .bearer_auth(&config.device_token),
                    ))
                    .await?;
            }
            Err(e) => return Err(e),
        }
    }
    guard.check().await?;
    p.phase("verifying");
    let done = guard
        .wait(request_json(
            guard
                .client
                .post(format!("{endpoint}/complete"))
                .bearer_auth(&config.device_token),
        ))
        .await?;
    ensure!(
        done["completed"] == true && done["sha256"] == sha && done["size"] == size,
        "gateway final receipt mismatch"
    );
    j.offset = size;
    j.save(store).await?;
    let name = std::path::Path::new(&j.path)
        .file_name()
        .context("missing file name")?
        .to_string_lossy();
    Ok(
        json!({"artifact_id":job.id,"path":j.path,"file_name":name,"size":size,"sha256":sha,
        "state":"completed","transfer_revision":j.revision,"confirmed_bytes":size,"checkpoint_bytes":CHUNK}),
    )
}

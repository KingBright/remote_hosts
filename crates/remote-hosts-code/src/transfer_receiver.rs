//! Gateway-side sequential chunk protocol. Bytes are synced before offset commit.
//! Interrupted bodies never advance the durable offset; retries compare exact data.
use crate::{
    files,
    gateway::{Gateway, Job},
    hash, now, transfer_control, transfers,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub(crate) const CHUNK: usize = 4 * 1024 * 1024;
pub(crate) const RETENTION: i64 = 86400;
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    pub id: String,
    pub device: String,
    pub owner: String,
    pub size: usize,
    pub sha256: String,
    pub offset: usize,
    pub expires: i64,
    pub completed: bool,
}
fn part(g: &Gateway, id: &str) -> PathBuf {
    g.config
        .state_dir
        .join("file-objects")
        .join(format!("{id}.part040"))
}
fn summary(s: &Session) -> Value {
    json!({"protocol":2,"operation_id":s.id,"size":s.size,"sha256":s.sha256,
    "confirmed_bytes":s.offset,"chunk_bytes":CHUNK,"completed":s.completed,"expires_at":s.expires})
}
async fn authorized(g: &Gateway, h: &HeaderMap, id: &str) -> Result<Job> {
    let job = transfer_control::device_job(g, h, id).await?;
    ensure!(job.tool == "file_download", "export required");
    ensure!(
        !transfer_control::control(g, id).await?.cancel_requested,
        "transfer_cancelled"
    );
    Ok(job)
}
async fn record_progress(g: &Gateway, s: &Session, phase: &str) -> Result<()> {
    g.store.put("receive_progress",&s.id,&json!({"snapshot":{"operation_id":s.id,"phase":phase,
        "bytes_done":s.offset,"confirmed_bytes":s.offset,"total_bytes":s.size},"origin":"durable_gateway_receiver","reported_at":now()}),now()+RETENTION).await?;
    g.observation_changed.notify_waiters();
    Ok(())
}
pub(crate) fn routes(g: Gateway) -> Router {
    Router::new()
        .route("/device/transfers/{id}", get(status).post(initialize))
        .route("/device/transfers/{id}/chunk", post(chunk))
        .route("/device/transfers/{id}/complete", post(complete))
        .route("/device/transfers/{id}/abort", post(abort))
        .with_state(g)
}
async fn status(State(g): State<Gateway>, Path(id): Path<String>, h: HeaderMap) -> Response {
    if authorized(&g, &h, &id).await.is_err() {
        return StatusCode::FORBIDDEN.into_response();
    }
    match g.store.get::<Session>("transfer_receiver", &id).await {
        Ok(Some(s)) if s.expires > now() => Json(summary(&s)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => StatusCode::GONE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn initialize(
    State(g): State<Gateway>,
    Path(id): Path<String>,
    h: HeaderMap,
    Json(v): Json<Value>,
) -> Response {
    let job = match authorized(&g, &h, &id).await {
        Ok(j) => j,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    let size = v["size"].as_u64();
    let sha = v["sha256"].as_str().unwrap_or("");
    let max = files::number(
        &job.arguments,
        "max_bytes",
        transfers::MAX_BYTES,
        1,
        transfers::MAX_BYTES,
    )
    .unwrap_or(0);
    let Some(size) = size.filter(|n| *n <= max as u64).map(|n| n as usize) else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    if !transfers::valid_hash(sha)
        || job.arguments["expected_version"]
            .as_str()
            .is_some_and(|s| s != sha)
    {
        return StatusCode::CONFLICT.into_response();
    }
    let Ok(_permit) = g.transfer_limits.try_acquire(&job.device_id, &id) else {
        return (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "2")]).into_response();
    };
    let result = async {
        if let Some(s) = g.store.get::<Session>("transfer_receiver", &id).await? {
            ensure!(
                s.size == size
                    && s.sha256 == sha
                    && s.device == job.device_id
                    && s.owner == job.owner,
                "session_identity_conflict"
            );
            ensure!(s.expires > now(), "session_expired");
            return Ok::<_, anyhow::Error>(s);
        }
        let _allocation = g.transfer_limits.allocation.lock().await;
        cleanup_expired(&g).await?;
        let used = transfers::cleanup(&g).await?;
        let uncommitted = match tokio::fs::symlink_metadata(part(&g, &id)).await {
            Ok(meta) if meta.is_file() && !meta.is_symlink() => meta.len(),
            Ok(_) => anyhow::bail!("invalid orphan staging type"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        ensure!(
            used.saturating_sub(uncommitted).saturating_add(size as u64) <= 512 * 1024 * 1024,
            "gateway_storage_limit"
        );
        let path = part(&g, &id);
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(nix::libc::O_NOFOLLOW);
        }
        // This private, UUID-named path belongs exclusively to this authenticated
        // operation. Bytes without committed metadata were never acknowledged.
        let file = match opts.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let file = crate::transfer_journal::open_data(&path, false)?;
                ensure!(
                    file.metadata()?.len() <= transfers::MAX_BYTES as u64,
                    "orphan_size_limit"
                );
                file.set_len(0)?;
                file
            }
            Err(error) => return Err(error.into()),
        };
        file.set_len(size as u64)?;
        file.sync_all()?;
        let s = Session {
            id: id.clone(),
            device: job.device_id,
            owner: job.owner,
            size,
            sha256: sha.into(),
            offset: 0,
            expires: now() + RETENTION,
            completed: false,
        };
        g.store.put("transfer_receiver", &id, &s, i64::MAX).await?;
        record_progress(&g, &s, "transferring").await?;
        Ok(s)
    }
    .await;
    match result {
        Ok(s) => Json(summary(&s)).into_response(),
        Err(e) if e.to_string().contains("storage_limit") => {
            StatusCode::INSUFFICIENT_STORAGE.into_response()
        }
        Err(e) if e.to_string().contains("identity_conflict") => {
            StatusCode::CONFLICT.into_response()
        }
        Err(e) if e.to_string().contains("expired") => StatusCode::GONE.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn chunk(State(g): State<Gateway>, Path(id): Path<String>, request: Request) -> Response {
    let h = request.headers();
    let job = match authorized(&g, h, &id).await {
        Ok(j) => j,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    let offset = h
        .get("x-transfer-offset")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    let sha = h
        .get("x-chunk-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let Some(offset) = offset.filter(|n| *n <= transfers::MAX_BYTES) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !transfers::valid_hash(&sha) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(_permit) = g.transfer_limits.try_acquire(&job.device_id, &id) else {
        return (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "2")]).into_response();
    };
    let result=async {
        let mut s:Session=g.store.get("transfer_receiver",&id).await?.context("session_missing")?;
        ensure!(s.expires>now(),"session_expired");
        let mut stream=request.into_body().into_data_stream();let mut bytes=Vec::with_capacity(CHUNK);
        let mut reported=std::time::Instant::now()-Duration::from_secs(1);
        while let Some(next)=tokio::time::timeout(Duration::from_secs(60),stream.next()).await? {
            let next=next.context("chunk_stream_error")?;
            ensure!(bytes.len().saturating_add(next.len())<=CHUNK,"chunk_too_large");
            ensure!(offset.saturating_add(bytes.len()).saturating_add(next.len())<=s.size,"offset_conflict");
            bytes.extend_from_slice(&next);
            if reported.elapsed()>=Duration::from_millis(500) {
                ensure!(!transfer_control::control(&g,&id).await?.cancel_requested,"transfer_cancelled");
                g.store.put("receive_progress",&id,&json!({"snapshot":{"operation_id":id,"phase":"transferring",
                    "bytes_done":offset+bytes.len(),"confirmed_bytes":s.offset,"total_bytes":s.size},
                    "origin":"durable_gateway_receiver","reported_at":now()}),now()+RETENTION).await?;
                reported=std::time::Instant::now();
            }
        }
        ensure!(!bytes.is_empty() && hash(&bytes)==sha,"chunk_checksum_mismatch");
        let end=offset.checked_add(bytes.len()).context("offset_overflow")?;
        ensure!(end<=s.size && offset<=s.offset,"offset_conflict");
        ensure!(!transfer_control::control(&g,&id).await?.cancel_requested,"transfer_cancelled");
        let path=if s.completed {transfers::blob_path(&g,&id)} else {part(&g,&id)};
        let mut f=tokio::fs::OpenOptions::new().read(true).write(!s.completed).open(path).await?;
        ensure!(f.metadata().await?.len()==s.size as u64,"staging_size_mismatch");
        f.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        if offset<s.offset {
            ensure!(end<=s.offset,"overlapping_chunk_conflict");let mut old=vec![0;bytes.len()];f.read_exact(&mut old).await?;
            ensure!(old==bytes,"duplicate_chunk_conflict");return Ok::<_,anyhow::Error>(summary(&s));
        }
        ensure!(!s.completed,"already_completed");
        f.write_all(&bytes).await?;f.flush().await?;f.sync_data().await?;
        s.offset=end;g.store.put("transfer_receiver",&id,&s,i64::MAX).await?;
        record_progress(&g,&s,"transferring").await?;Ok(summary(&s))
    }.await;
    match result {
        Ok(v) => Json(v).into_response(),
        Err(e) if e.is::<tokio::time::error::Elapsed>() => {
            StatusCode::REQUEST_TIMEOUT.into_response()
        }
        Err(e) if e.to_string().contains("too_large") => {
            StatusCode::PAYLOAD_TOO_LARGE.into_response()
        }
        Err(e) if e.to_string().contains("expired") => StatusCode::GONE.into_response(),
        Err(e) if e.to_string().contains("missing") => StatusCode::NOT_FOUND.into_response(),
        Err(e) if e.to_string().contains("cancelled") => StatusCode::FORBIDDEN.into_response(),
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}
async fn complete(State(g): State<Gateway>, Path(id): Path<String>, h: HeaderMap) -> Response {
    let job = match authorized(&g, &h, &id).await {
        Ok(j) => j,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    let Ok(_permit) = g.transfer_limits.try_acquire(&job.device_id, &id) else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let result = async {
        let mut s: Session = g
            .store
            .get("transfer_receiver", &id)
            .await?
            .context("session_missing")?;
        ensure!(
            s.expires > now() && s.offset == s.size,
            "session_not_complete"
        );
        let target = transfers::blob_path(&g, &id);
        if let Some(blob) = g.store.get::<transfers::Blob>("file_blob", &id).await? {
            ensure!(
                blob.device == s.device
                    && blob.owner == s.owner
                    && blob.sha256 == s.sha256
                    && blob.size == s.size
                    && blob.expires > now(),
                "completed_artifact_expired_or_changed"
            );
            ensure!(
                tokio::fs::metadata(&target).await?.len() == s.size as u64,
                "completed_data_missing"
            );
            if !s.completed {
                s.completed = true;
                g.store.put("transfer_receiver", &id, &s, i64::MAX).await?;
            }
            return Ok(summary(&s));
        }
        ensure!(!s.completed, "completed_artifact_expired");
        let staged = part(&g, &id);
        let source = if staged.exists() {
            staged.clone()
        } else {
            target.clone()
        };
        let max = s.size;
        let actual = tokio::task::spawn_blocking(move || -> Result<_> {
            let file = std::fs::File::open(source)?;
            transfers::digest_reader(file, max, None)
        })
        .await??;
        ensure!(
            actual.0 == s.size && actual.1 == s.sha256,
            "snapshot_checksum_mismatch"
        );
        ensure!(
            !transfer_control::control(&g, &id).await?.cancel_requested,
            "transfer_cancelled"
        );
        let name = files::relative(files::text(&job.arguments, "path")?)?
            .file_name()
            .context("missing_filename")?
            .to_string_lossy()
            .into_owned();
        ensure!(name.len() <= 255, "filename_too_long");
        if staged.exists() {
            tokio::fs::rename(&staged, &target).await?;
        }
        // A restart after rename simply rechecks the same immutable blob and
        // commits metadata. Never needs a second upload of the original bytes.
        let blob = transfers::Blob {
            operation: id.clone(),
            device: job.device_id,
            owner: job.owner,
            name,
            size: s.size,
            sha256: s.sha256.clone(),
            expires: now() + 3600,
        };
        g.store.put("file_blob", &id, &blob, i64::MAX).await?;
        s.completed = true;
        g.store.put("transfer_receiver", &id, &s, i64::MAX).await?;
        record_progress(&g, &s, "completed").await?;
        Ok::<_, anyhow::Error>(summary(&s))
    }
    .await;
    match result {
        Ok(v) => Json(v).into_response(),
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}
async fn abort(State(g): State<Gateway>, Path(id): Path<String>, h: HeaderMap) -> Response {
    let job = match transfer_control::device_job(&g, &h, &id).await {
        Ok(j) => j,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    if job.tool != "file_download" || !control_cancelled(&g, &id).await {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(_permit) = g.transfer_limits.try_acquire(&job.device_id, &id) else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let result=async {
        let (state,):(String,)=sqlx::query_as("SELECT state FROM jobs WHERE id=?").bind(&id).fetch_one(&g.store.pool).await?;
        ensure!(state!="done","already_finished");
        for path in [part(&g,&id),transfers::blob_path(&g,&id)] {
            match tokio::fs::remove_file(path).await {Ok(())=>{},Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{},Err(e)=>return Err(e.into())}
        }
        sqlx::query("DELETE FROM kv WHERE kind IN ('transfer_receiver','file_blob','receive_progress') AND key=?")
            .bind(&id).execute(&g.store.pool).await?;Ok::<_,anyhow::Error>(())
    }.await;
    match result {
        Ok(()) => Json(json!({"cleanup_complete":true})).into_response(),
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}
async fn control_cancelled(g: &Gateway, id: &str) -> bool {
    transfer_control::control(g, id)
        .await
        .is_ok_and(|c| c.cancel_requested)
}
/// Called under the short allocation mutex; never removes a current session.
async fn cleanup_expired(g: &Gateway) -> Result<()> {
    let rows:Vec<(String,String)>=sqlx::query_as("SELECT key,value FROM kv WHERE kind='transfer_receiver' AND json_extract(value,'$.expires')<=?")
        .bind(now()).fetch_all(&g.store.pool).await?;
    for (id, text) in rows {
        let s: Session = serde_json::from_str(&text)?;
        let Ok(_permit) = g.transfer_limits.try_acquire(&s.device, &id) else {
            continue;
        };
        if !s.completed {
            match tokio::fs::remove_file(part(g, &id)).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => continue,
            }
        }
        sqlx::query("DELETE FROM kv WHERE kind='transfer_receiver' AND key=?")
            .bind(id)
            .execute(&g.store.pool)
            .await?;
    }
    Ok(())
}

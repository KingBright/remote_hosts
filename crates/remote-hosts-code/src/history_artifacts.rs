//! Autonomous collection of expired, metadata-owned gateway transfer caches.
//! Expiry ends cache authority, not the job's execution or receipt identity.
use crate::{gateway::Gateway, store::Store, transfer_receiver::Session, transfers::Blob};
use anyhow::{Result, ensure};
use serde::Serialize;
use serde_json::{Value, json};
use std::path::Path;

const PAGE: usize = 64;
const SCAN_BUDGET: usize = 256;
#[derive(Default, Debug, Serialize)]
pub(crate) struct Report {
    pub scanned: usize,
    pub retired: usize,
    pub deleted_files: usize,
    pub unlinked_bytes: u64,
    pub deferred: usize,
    pub item_errors: usize,
    pub has_more: bool,
}
fn identity(kind: &str, id: &str, text: &str, at: i64) -> Result<Option<(String, String)>> {
    uuid::Uuid::parse_str(id)?;
    let (saved_id, device, owner, expires, suffix) = match kind {
        "file_blob" => {
            let b: Blob = serde_json::from_str(text)?;
            (b.operation, b.device, b.owner, b.expires, "blob")
        }
        "transfer_receiver" => {
            let s: Session = serde_json::from_str(text)?;
            (s.id, s.device, s.owner, s.expires, "part040")
        }
        _ => anyhow::bail!("invalid_cache_kind"),
    };
    ensure!(
        saved_id == id && !device.is_empty() && !owner.is_empty(),
        "invalid_cache_identity"
    );
    if expires <= 0 || expires > at {
        return Ok(None);
    }
    Ok(Some((device, format!("{id}.{suffix}"))))
}
fn unlink_owned(dir: &Path, name: &str) -> Result<Option<u64>> {
    let root = cap_std::fs::Dir::open_ambient_dir(dir, cap_std::ambient_authority())?;
    let meta = match root.symlink_metadata("file-objects") {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        meta.is_dir() && !meta.file_type().is_symlink(),
        "invalid_cache_directory"
    );
    let files = root.open_dir("file-objects")?;
    let meta = match files.symlink_metadata(name) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink(),
        "invalid_cache_file"
    );
    match files.remove_file(name) {
        Ok(()) => Ok(Some(meta.len())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Fixed scan budget with a persistent keyset cursor, including over protected
/// or malformed rows. One poison entry cannot starve the tail on every tick.
/// Only already-expired cache bytes are discarded. Job results, commands,
/// semantic guards and Agent receipt queues are never changed by this collector.
pub(crate) async fn sweep(g: &Gateway, store: &Store, at: i64) -> Result<Report> {
    let saved = store
        .get::<Value>("runtime", "artifact_retention_cursor")
        .await?;
    let mut after = saved
        .as_ref()
        .and_then(|v| {
            Some((
                v["kind"].as_str()?.to_owned(),
                v["key"].as_str()?.to_owned(),
            ))
        })
        .unwrap_or_default();
    let initial = after.clone();
    let mut report = Report::default();
    while report.scanned < SCAN_BUDGET {
        let rows: Vec<(String,String,String)> = sqlx::query_as("SELECT kind,key,value FROM kv WHERE kind IN ('file_blob','transfer_receiver') AND (kind,key)>(?,?) ORDER BY kind,key LIMIT 64")
            .bind(&after.0).bind(&after.1).fetch_all(&store.pool).await?;
        let page_complete = rows.len() < PAGE;
        for (kind, id, text) in rows {
            after = (kind.clone(), id.clone());
            report.scanned += 1;
            let (device, name) = match identity(&kind, &id, &text, at) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(_) => {
                    report.item_errors += 1;
                    continue;
                }
            };
            // Same order as writers: operation permit, then allocation. Neither
            // acquisition waits behind an upload, publisher or another sweep.
            let Ok(_permit) = g.transfer_limits.try_acquire(&device, &id) else {
                report.deferred += 1;
                continue;
            };
            let Ok(_allocation) = g.transfer_limits.allocation.try_lock() else {
                report.deferred += 1;
                continue;
            };
            let unchanged: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM kv WHERE kind=? AND key=? AND value=?)",
            )
            .bind(&kind)
            .bind(&id)
            .bind(&text)
            .fetch_one(&store.pool)
            .await?;
            if !unchanged {
                report.deferred += 1;
                continue;
            }
            // A failed unlink retains the metadata for retry. If the database
            // delete fails after unlink, the same expired record retries safely
            // with a missing payload, never by restarting the original transfer.
            match unlink_owned(&g.config.state_dir, &name) {
                Ok(bytes) => {
                    let deleted = sqlx::query("DELETE FROM kv WHERE kind=? AND key=? AND value=?")
                        .bind(&kind)
                        .bind(&id)
                        .bind(&text)
                        .execute(&store.pool)
                        .await?;
                    report.retired += deleted.rows_affected() as usize;
                    if let Some(bytes) = bytes {
                        report.deleted_files += 1;
                        report.unlinked_bytes = report.unlinked_bytes.saturating_add(bytes);
                    }
                }
                Err(_) => report.item_errors += 1,
            }
        }
        if page_complete {
            after = (String::new(), String::new());
            break;
        }
        if report.scanned == SCAN_BUDGET {
            report.has_more = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    // No write for an idle pass or a complete pass that starts/ends at origin.
    if after != initial {
        store
            .put(
                "runtime",
                "artifact_retention_cursor",
                &json!({"kind":after.0,"key":after.1}),
                i64::MAX,
            )
            .await?;
    }
    Ok(report)
}

#[cfg(test)]
#[path = "history_artifacts_tests.rs"]
mod tests;

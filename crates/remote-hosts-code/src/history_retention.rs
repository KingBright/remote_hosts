//! Default-on, device-wide retention. Recovery truth is never a cache entry.
//! Scan bounded keyset pages on one low-priority connection; unlink only after
//! retirement intent commits. A crash retries file cleanup, never command work.
use crate::{AgentConfig, store::Store};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

const PAGE: usize = 64;
const FILE_QUEUE_LIMIT: i64 = 128;
const INTERVAL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub successful_seconds: i64,
    pub failed_seconds: i64,
    pub minimum_seconds: i64,
    pub max_items: usize,
    pub max_bytes: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            successful_seconds: 7 * 86400,
            failed_seconds: 30 * 86400,
            minimum_seconds: 86400,
            max_items: 2000,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Default, Debug, Serialize)]
pub(crate) struct Report {
    pub scanned: u64,
    pub retained_items: usize,
    pub retained_bytes: u64,
    pub protected_items: u64,
    pub retired_items: u64,
    pub legacy_clocks_initialized: u64,
    pub deleted_files: u64,
    pub unlinked_bytes: u64,
    pub cleanup_pending: i64,
    pub item_errors: u64,
    pub pressure_remaining: bool,
}
#[derive(Clone)]
struct Candidate {
    kind: String,
    id: String,
    value: String,
    at: i64,
    failed: bool,
    bytes: u64,
    journal_guard: Option<JournalGuard>,
}
#[derive(Clone)]
struct JournalGuard {
    root: Arc<cap_std::fs::Dir>,
    version: Option<String>,
}
type Row = (String, String, String, Option<String>, i64);
const SCAN: &str = "SELECT v.kind,v.key,v.value,o.value,EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=v.key) FROM kv v LEFT JOIN kv o ON o.kind='local_operation' AND o.key=v.key WHERE v.kind IN ('local_operation','terminal','transfer_local') AND (v.kind,v.key)>(?,?) ORDER BY v.kind,v.key LIMIT 64";

pub(crate) async fn install(store: &Store) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS history_file_cleanup(kind TEXT NOT NULL,id TEXT NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(kind,id))")
        .execute(&store.pool).await?;
    Ok(())
}

/// A separate pool cannot consume the four command/receipt connections. FULL
/// durability remains enabled; contention yields instead of increasing its wait.
pub(crate) async fn maintenance_store(dir: &Path) -> Result<Store> {
    let options = SqliteConnectOptions::new()
        .filename(dir.join("state.sqlite"))
        .create_if_missing(false)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
        .busy_timeout(Duration::from_millis(50));
    Ok(Store {
        pool: SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(250))
            .connect_with(options)
            .await?,
    })
}

/// Bound admission only, not a commit with an ambiguous durable outcome.
/// SQLx rolls back a BEGIN whose worker acknowledgement was cancelled before
/// a Transaction could be returned. No cleanup intent exists at this boundary.
pub(crate) async fn begin_maintenance(
    store: &Store,
) -> Result<sqlx::Transaction<'_, sqlx::Sqlite>> {
    Ok(tokio::time::timeout(
        Duration::from_millis(250),
        store.pool.begin_with("BEGIN IMMEDIATE"),
    )
    .await
    .map_err(|_| anyhow::anyhow!("maintenance_writer_wait_budget"))??)
}

fn file_name(kind: &str, id: &str) -> Result<PathBuf> {
    uuid::Uuid::parse_str(id)?;
    let (directory, extension) = match kind {
        "terminal" => ("terminals", "log"),
        "transfer_local" => ("transfers-v2", "data"),
        "local_operation" => ("edits", "json"),
        _ => anyhow::bail!("invalid_history_file_kind"),
    };
    Ok(Path::new(directory).join(format!("{id}.{extension}")))
}

/// Capability-relative paths protect against parent-directory symlink escapes.
/// Unknown/special files are not guessed disposable; a bad item cannot starve
/// unrelated cleanup. Missing private payloads still permit metadata retirement.
fn file_size(root: &cap_std::fs::Dir, kind: &str, id: &str) -> Result<u64> {
    let path = file_name(kind, id)?;
    let parent = path.parent().expect("fixed relative parent");
    match root.symlink_metadata(parent) {
        Ok(meta) => ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "history_directory_not_regular"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    }
    match root.symlink_metadata(&path) {
        Ok(meta) => {
            ensure!(
                meta.is_file() && !meta.file_type().is_symlink(),
                "history_file_not_regular"
            );
            Ok(meta.len())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

fn read_journal(root: &cap_std::fs::Dir, id: &str) -> Result<Option<(Value, String)>> {
    const LIMIT: u64 = 64 * 1024 * 1024;
    let bytes = file_size(root, "local_operation", id)?;
    if bytes == 0 {
        return Ok(None);
    }
    ensure!(bytes <= LIMIT, "history_journal_too_large");
    let mut content = Vec::new();
    root.open(file_name("local_operation", id)?)?
        .take(LIMIT + 1)
        .read_to_end(&mut content)?;
    ensure!(content.len() as u64 <= LIMIT, "history_journal_too_large");
    Ok(Some((
        serde_json::from_slice(&content)?,
        crate::hash(&content),
    )))
}

fn candidate(row: &Row, device: &str, root: &cap_std::fs::Dir) -> Result<Option<Candidate>> {
    let (kind, id, text, operation, pending) = row;
    let value: Value = serde_json::from_str(text)?;
    if kind == "local_operation"
        && (value["tool"] != "code_apply_edits" || value["history_expired"] == true)
    {
        return Ok(None);
    }
    if *pending != 0 || value["history_hold"] == true {
        return Ok(None);
    }
    let workspace = value["workspace_id"].as_str().unwrap_or_default();
    if workspace.split_once(':').is_none_or(|(d, _)| d != device) {
        return Ok(None);
    }
    if let Some(text) = operation {
        let op: Value = serde_json::from_str(text)?;
        if op["state"] != "done" || op["gateway_accepted"] != true || op["history_hold"] == true {
            return Ok(None);
        }
    }
    let (finished, failed) = match kind.as_str() {
        "terminal" => (
            value["output_complete"] == true
                && matches!(
                    value["state"].as_str(),
                    Some("exited" | "cancelled" | "timed_out" | "failed")
                ),
            value["state"] != "exited"
                || value["exit_code"] != 0
                || value["output_error"].is_string(),
        ),
        "transfer_local" => (
            matches!(
                value["phase"].as_str(),
                Some("completed" | "cancelled" | "failed" | "expired")
            ) && value["device_id"] == device
                && value["operation_id"] == *id,
            value["phase"] != "completed",
        ),
        "local_operation" => (
            value["state"] == "done"
                && value["gateway_accepted"] == true
                && value["result"]["change_set"]["state"] == "completed",
            false,
        ),
        _ => (false, false),
    };
    if !finished {
        return Ok(None);
    }
    if kind == "terminal" {
        let execution =
            crate::receipts::decision(&json!({"terminal":&value}), None, None, crate::now());
        if !matches!(
            execution["execution_state"].as_str(),
            Some("exited" | "not_started")
        ) {
            return Ok(None);
        }
    }
    // Unknown/runtime-lost, incomplete output, active/paused transfers and
    // partial edits stay protected, even under disk pressure.
    // A legacy completed record can lack a final timestamp. Start its clock
    // when completion is first confirmed, never at an old creation/file time.
    let at = value["updated_at"]
        .as_i64()
        .filter(|v| *v > 0)
        .or_else(|| {
            value["history_retention_confirmed_at"]
                .as_i64()
                .filter(|v| *v > 0)
        })
        .unwrap_or(0);
    let bytes = file_size(root, kind, id)?;
    let journal = if kind == "local_operation" {
        read_journal(root, id)?
    } else {
        None
    };
    if let Some((journal, _)) = &journal
        && (journal["status"] != "completed"
            || journal["change_set_id"] != *id
            || journal["workspace"]["id"] != workspace
            || journal["workspace"]["device_id"] != device)
    {
        return Ok(None);
    }
    Ok(Some(Candidate {
        kind: kind.clone(),
        id: id.clone(),
        value: text.clone(),
        at,
        failed,
        bytes: bytes.saturating_add(text.len() as u64),
        journal_guard: if kind == "local_operation" {
            Some(JournalGuard {
                root: Arc::new(root.try_clone()?),
                version: journal.map(|(_, version)| version),
            })
        } else {
            None
        },
    }))
}

async fn initialize_legacy_clock(store: &Store, c: &Candidate, at: i64) -> Result<bool> {
    ensure!(c.at == 0 && at > 0, "invalid_legacy_retention_clock");
    let mut tx = begin_maintenance(store).await?;
    // Only a still-confirmed candidate may receive a retention-only timestamp.
    // This never changes its business completion time, result or output file.
    let updated = sqlx::query("UPDATE kv AS v SET value=json_set(v.value,'$.history_retention_confirmed_at',?) WHERE v.kind=? AND v.key=? AND v.value=? AND NOT EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=v.key) AND NOT EXISTS(SELECT 1 FROM kv o WHERE o.kind='local_operation' AND o.key=v.key AND (COALESCE(json_extract(o.value,'$.state'),'unknown')<>'done' OR COALESCE(json_extract(o.value,'$.gateway_accepted'),0)<>1 OR COALESCE(json_extract(o.value,'$.history_hold'),0)=1))")
        .bind(at).bind(&c.kind).bind(&c.id).bind(&c.value).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(updated.rows_affected() == 1)
}

async fn retire(store: &Store, c: &Candidate, access: &tokio::sync::RwLock<()>) -> Result<bool> {
    let Ok(_access) = access.try_write() else {
        return Ok(false);
    };
    if let Some(guard) = c.journal_guard.clone() {
        let id = c.id.clone();
        // A resume may finish after selection but before this exclusive access.
        // Database CAS alone does not detect a changed on-disk recovery journal.
        // Recheck bytes, including a journal that appeared after being absent.
        let recheck = tokio::task::spawn_blocking(move || -> Result<bool> {
            Ok(read_journal(&guard.root, &id)?.map(|(_, version)| version) == guard.version)
        });
        if !matches!(
            tokio::time::timeout(Duration::from_millis(500), recheck).await,
            Ok(Ok(Ok(true)))
        ) {
            return Ok(false);
        }
    }
    // The read-side candidate is only a hint. Recheck lifecycle and delivery
    // under the writer lock before persisting unlink intent. No filesystem work
    // is done in this transaction, so commit failure cannot destroy a log.
    let mut tx = begin_maintenance(store).await?;
    let queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM history_file_cleanup")
        .fetch_one(&mut *tx)
        .await?;
    if queued >= FILE_QUEUE_LIMIT {
        tx.rollback().await?;
        return Ok(false);
    }
    let safe: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kv v WHERE v.kind=? AND v.key=? AND v.value=? AND NOT EXISTS(SELECT 1 FROM receipt_outbox r WHERE r.id=v.key) AND NOT EXISTS(SELECT 1 FROM kv o WHERE o.kind='local_operation' AND o.key=v.key AND (COALESCE(json_extract(o.value,'$.state'),'unknown')<>'done' OR COALESCE(json_extract(o.value,'$.gateway_accepted'),0)<>1 OR COALESCE(json_extract(o.value,'$.history_hold'),0)=1)))")
        .bind(&c.kind).bind(&c.id).bind(&c.value).fetch_one(&mut *tx).await?;
    if !safe {
        tx.rollback().await?;
        return Ok(false);
    }
    sqlx::query("INSERT OR IGNORE INTO history_file_cleanup(kind,id,bytes) VALUES(?,?,?)")
        .bind(&c.kind)
        .bind(&c.id)
        .bind(c.bytes as i64)
        .execute(&mut *tx)
        .await?;
    if c.kind == "local_operation" {
        // Keep the existing fingerprint and acknowledged outcome. Forgetting
        // this deduplication anchor could turn a delayed poll into a new edit.
        sqlx::query("UPDATE kv SET value=json_set(json_remove(value,'$.result'),'$.history_expired',json('true')) WHERE kind='local_operation' AND key=? AND value=?")
            .bind(&c.id).bind(&c.value).execute(&mut *tx).await?;
    } else {
        sqlx::query("DELETE FROM kv WHERE kind=? AND key=? AND value=?")
            .bind(&c.kind)
            .bind(&c.id)
            .bind(&c.value)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub(crate) async fn drain_files(
    store: &Store,
    dir: &Path,
    report: &mut Report,
    access: &tokio::sync::RwLock<()>,
) -> Result<()> {
    let root = cap_std::fs::Dir::open_ambient_dir(dir, cap_std::ambient_authority())?;
    // One bounded pass attempts every ticket, so a permissions error at the
    // beginning cannot prevent deletion of later files indefinitely.
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT kind,id FROM history_file_cleanup ORDER BY kind,id LIMIT 128")
            .fetch_all(&store.pool)
            .await?;
    for (kind, id) in rows {
        let Ok(_access) = access.try_write() else {
            break;
        };
        let cleanup = || -> Result<u64> {
            let bytes = file_size(&root, &kind, &id)?;
            match root.remove_file(file_name(&kind, &id)?) {
                Ok(()) => Ok(bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
                Err(e) => Err(e.into()),
            }
        };
        match cleanup() {
            Ok(bytes) => {
                sqlx::query("DELETE FROM history_file_cleanup WHERE kind=? AND id=?")
                    .bind(&kind)
                    .bind(&id)
                    .execute(&store.pool)
                    .await?;
                report.deleted_files += u64::from(bytes > 0);
                report.unlinked_bytes = report.unlinked_bytes.saturating_add(bytes);
            }
            Err(_) => report.item_errors += 1,
        }
        tokio::task::yield_now().await;
    }
    report.cleanup_pending = sqlx::query_scalar("SELECT COUNT(*) FROM history_file_cleanup")
        .fetch_one(&store.pool)
        .await?;
    Ok(())
}

pub(crate) async fn sweep(
    store: &Store,
    config: &AgentConfig,
    policy: &Policy,
    at: i64,
    access: &tokio::sync::RwLock<()>,
) -> Result<Report> {
    let root = Arc::new(cap_std::fs::Dir::open_ambient_dir(
        &config.state_dir,
        cap_std::ambient_authority(),
    )?);
    let mut report = Report::default();
    drain_files(store, &config.state_dir, &mut report, access).await?;
    let mut after = (String::new(), String::new());
    let mut oldest = Vec::<Candidate>::new();
    loop {
        let rows: Vec<Row> = sqlx::query_as(SCAN)
            .bind(&after.0)
            .bind(&after.1)
            .fetch_all(&store.pool)
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            after = (row.0.clone(), row.1.clone());
            report.scanned += 1;
            let row_kind = row.0.clone();
            let file_root = root.clone();
            let device = config.device_id.clone();
            let checked =
                tokio::task::spawn_blocking(move || candidate(&row, &device, &file_root)).await?;
            let c = match checked {
                Ok(Some(c)) => c,
                Ok(None) => {
                    if row_kind != "local_operation" {
                        report.protected_items += 1;
                    }
                    continue;
                }
                Err(_) => {
                    report.item_errors += 1;
                    report.protected_items += 1;
                    continue;
                }
            };
            if c.at == 0 {
                if initialize_legacy_clock(store, &c, at).await? {
                    report.legacy_clocks_initialized += 1;
                    report.retained_items += 1;
                    report.retained_bytes = report.retained_bytes.saturating_add(c.bytes);
                } else {
                    report.protected_items += 1;
                }
                // Never delete from the old pre-migration snapshot or bypass
                // the minimum retention window on the first observation.
                continue;
            }
            let age = at.saturating_sub(c.at);
            let retention = if c.failed {
                policy.failed_seconds
            } else {
                policy.successful_seconds
            };
            if age >= retention && retire(store, &c, access).await? {
                report.retired_items += 1;
                continue;
            }
            report.retained_items += 1;
            report.retained_bytes = report.retained_bytes.saturating_add(c.bytes);
            if age >= policy.minimum_seconds {
                oldest.push(c);
                oldest.sort_unstable_by(|a, b| (a.at, &a.kind, &a.id).cmp(&(b.at, &b.kind, &b.id)));
                oldest.truncate(PAGE);
            }
        }
        drain_files(store, &config.state_dir, &mut report, access).await?;
        // Bounded pages with breathing room. This is not on any poll lane and
        // not a single long writer transaction over the entire retained history.
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for c in oldest {
        if report.retained_items <= policy.max_items && report.retained_bytes <= policy.max_bytes {
            break;
        }
        if retire(store, &c, access).await? {
            report.retired_items += 1;
            report.retained_items = report.retained_items.saturating_sub(1);
            report.retained_bytes = report.retained_bytes.saturating_sub(c.bytes);
        }
    }
    drain_files(store, &config.state_dir, &mut report, access).await?;
    report.pressure_remaining =
        report.retained_items > policy.max_items || report.retained_bytes > policy.max_bytes;
    Ok(report)
}

pub(crate) async fn agent_loop(
    config: Arc<AgentConfig>,
    access: Arc<tokio::sync::RwLock<()>>,
) -> Result<()> {
    agent_loop_with_timing(config, access, Duration::from_secs(30), INTERVAL).await
}

async fn agent_loop_with_timing(
    config: Arc<AgentConfig>,
    access: Arc<tokio::sync::RwLock<()>>,
    startup: Duration,
    interval: Duration,
) -> Result<()> {
    // Startup recovery and receipt-queue installation run first. A network
    // outage does not disable this independent lifecycle task.
    tokio::time::sleep(startup).await;
    let policy = Policy::default();
    loop {
        let run = async {
            let store = maintenance_store(&config.state_dir).await?;
            let started = Instant::now();
            let report = sweep(&store,&config,&policy,crate::now(),&access).await?;
            store.prune_batch().await?;
            // One latest summary, never a new job/terminal/audit entry per tick.
            store.put("runtime","automatic_history_cleanup",&json!({"protocol":1,
                "automatic":true,"scope":"device-wide completed history, not execution authority",
                "observed_at":crate::now(),"elapsed_ms":started.elapsed().as_millis(),
                "policy":policy,"last_run":report,
                "quota_semantics":"soft limits; active, uncertain, held and undelivered data are protected",
                "database_reclamation":"deleted pages reusable; no blocking full VACUUM"}),i64::MAX).await?;
            // No truncation or waiting for readers; freed SQLite pages are
            // reusable even if the main database file does not immediately shrink.
            let _ = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)").execute(&store.pool).await;
            store.pool.close().await;
            Ok::<_,anyhow::Error>(report.pressure_remaining && report.retired_items > 0)
        }.await;
        let delay = match run {
            Ok(true) => Duration::from_secs(5),
            Ok(false) => interval,
            Err(_) => {
                tracing::warn!(
                    "automatic history cleanup deferred; original data and recovery intent retained"
                );
                Duration::from_secs(60)
            }
        };
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
#[path = "history_retention_tests.rs"]
mod tests;

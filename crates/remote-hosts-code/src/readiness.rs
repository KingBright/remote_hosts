//! Optional readiness telemetry, derived only from authenticated poll replies.
//! No command execution depends on this cache or on a successful telemetry write.
use crate::store::Store;
use anyhow::{Result, ensure};
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

const INTERVAL: Duration = Duration::from_secs(10);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const WRITE_BUDGET: Duration = Duration::from_millis(250);

#[derive(Default)]
struct Pending {
    session: String,
    lanes: BTreeMap<String, i64>,
    revision: i64,
    saved_revision: i64,
    saved_lane_count: usize,
    last_attempt: Option<Instant>,
    last_commit: Option<Instant>,
}

#[derive(Default)]
pub(crate) struct Readiness {
    pending: Mutex<Pending>,
    flush_lock: tokio::sync::Mutex<()>,
}

impl Readiness {
    pub fn start(&self, session: &str) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("readiness_cache_unavailable"))?;
        *pending = Pending {
            session: session.to_owned(),
            ..Pending::default()
        };
        Ok(())
    }

    /// Runs on a poll lane without acquiring a database connection or writer.
    pub fn record(&self, session: &str, lane: &str, at: i64) -> Result<()> {
        ensure!(
            matches!(lane, "read" | "write" | "transfer" | "terminal" | "control") && at >= 0,
            "invalid_readiness_observation"
        );
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("readiness_cache_unavailable"))?;
        ensure!(pending.session == session, "readiness_session_changed");
        pending.lanes.insert(lane.to_owned(), at);
        pending.revision = pending
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("readiness_revision_exhausted"))?;
        Ok(())
    }

    /// Reuse the existing heartbeat loop, rather than add another reporting task.
    /// A bounded write cannot hold up heartbeat output for SQLite's busy timeout.
    pub async fn flush(&self, store: &Store, session: &str) -> Result<bool> {
        self.flush_at(store, session, Instant::now()).await
    }

    async fn flush_at(&self, store: &Store, session: &str, clock: Instant) -> Result<bool> {
        let _serial = self.flush_lock.lock().await;
        let (revision, lanes) = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("readiness_cache_unavailable"))?;
            ensure!(pending.session == session, "readiness_session_changed");
            if pending.revision == pending.saved_revision
                || pending
                    .last_attempt
                    .is_some_and(|at| clock.saturating_duration_since(at) < RETRY_INTERVAL)
                || (pending.saved_lane_count == pending.lanes.len()
                    && pending
                        .last_commit
                        .is_some_and(|at| clock.saturating_duration_since(at) < INTERVAL))
            {
                return Ok(false);
            }
            pending.last_attempt = Some(clock);
            (pending.revision, pending.lanes.clone())
        };
        let at = lanes.values().copied().max().unwrap_or(0);
        let update = async {
            let result = sqlx::query("UPDATE kv SET value=json_set(value,'$.phase','polling','$.lanes',json(?),'$.updated_at',?,'$.publication_revision',?) WHERE kind='runtime' AND key='readiness' AND json_extract(value,'$.session')=? AND json_extract(value,'$.pid')=? AND COALESCE(json_extract(value,'$.publication_revision'),0)<?")
                .bind(serde_json::to_string(&lanes)?).bind(at).bind(revision).bind(session)
                .bind(i64::from(std::process::id())).bind(revision).execute(&store.pool).await?;
            if result.rows_affected() == 0 {
                // A timed-out write can still commit. Confirm the existing
                // publication instead of overwriting a newer snapshot.
                let saved: Option<i64> = sqlx::query_scalar("SELECT json_extract(value,'$.publication_revision') FROM kv WHERE kind='runtime' AND key='readiness' AND json_extract(value,'$.session')=? AND json_extract(value,'$.pid')=?")
                    .bind(session).bind(i64::from(std::process::id())).fetch_optional(&store.pool).await?;
                ensure!(
                    saved.is_some_and(|v| v >= revision),
                    "readiness_session_changed"
                );
            }
            Ok::<(), anyhow::Error>(())
        };
        tokio::time::timeout(WRITE_BUDGET, update)
            .await
            .map_err(|_| anyhow::anyhow!("readiness_write_unconfirmed"))??;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("readiness_cache_unavailable"))?;
        // New acknowledgements recorded during the write remain dirty. An old
        // session can never reset a new session's in-memory publication cursor.
        ensure!(pending.session == session, "readiness_session_changed");
        pending.saved_revision = revision;
        pending.saved_lane_count = lanes.len();
        pending.last_commit = Some(clock);
        Ok(true)
    }
}

#[cfg(test)]
#[path = "readiness_tests.rs"]
mod tests;

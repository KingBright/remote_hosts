//! Durable receipt delivery. A retry sends saved bytes, never executes a job.
//! Device and gateway bindings survive restart; a different destination is not a retry.
use crate::{AgentConfig, now, store::Store};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Status {
    pub protocol: u8,
    pub pending: u64,
    pub blocked: u64,
    pub sending: u64,
    pub oldest_created_at: Option<i64>,
    pub reported_at: i64,
}
impl Status {
    pub fn valid(&self) -> bool {
        self.protocol == 1
            && self.pending <= 1_000_000
            && self.blocked <= 1_000_000
            && self.sending <= self.pending
            && self.reported_at >= 0
            && self.reported_at <= now() + 90
            && self
                .oldest_created_at
                .is_none_or(|t| t >= 0 && t <= self.reported_at)
    }
}
#[derive(Clone)]
pub(crate) struct Delivery {
    store: Store,
    config: Arc<AgentConfig>,
    changed: Arc<Notify>,
}
struct Claim {
    id: String,
    payload: String,
    attempt: i64,
    lease: String,
}
#[derive(Debug, PartialEq)]
enum Outcome {
    Accepted,
    Retry(Option<u16>),
    Blocked(u16),
}
impl Delivery {
    pub async fn new(store: Store, config: Arc<AgentConfig>) -> Result<Self> {
        sqlx::query("CREATE TABLE IF NOT EXISTS receipt_outbox (id TEXT PRIMARY KEY,device TEXT NOT NULL,origin TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,next_attempt INTEGER NOT NULL,lease TEXT,last_http INTEGER,created INTEGER NOT NULL)")
            .execute(&store.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS receipt_outbox_due ON receipt_outbox(device,origin,state,next_attempt,id)")
            .execute(&store.pool).await?;
        // Startup discards receipts owned by the Gateway: no poll response from
        // the previous process can still arrive. During this process, keep a
        // compact fingerprint receipt for late responses, never a duplicate body.
        // code_apply_edits remains the original change_resume recovery anchor.
        sqlx::query("DELETE FROM kv WHERE kind='local_operation' AND json_extract(value,'$.state')='done' AND json_extract(value,'$.tool') IS NOT NULL AND (json_extract(value,'$.tool')<>'code_apply_edits' OR (json_extract(value,'$.gateway_accepted')=1 AND json_extract(value,'$.result') IS NULL)) AND NOT EXISTS (SELECT 1 FROM receipt_outbox r WHERE r.id=kv.key)")
            .execute(&store.pool).await?;
        // Do not recover another device's receipts or redirect private results.
        // Expired in-flight leases are reclaimed by claim(); a restart need not
        // guess whether a previous sender is still finishing its bounded request.
        Ok(Self {
            store,
            config,
            changed: Arc::default(),
        })
    }
    async fn enqueue_tx(
        &self,
        connection: &mut SqliteConnection,
        id: &str,
        value: &Value,
    ) -> Result<()> {
        uuid::Uuid::parse_str(id).context("invalid receipt operation id")?;
        ensure!(value.is_object(), "receipt must be an object");
        let payload = json!({"operation_id":id,"result":value}).to_string();
        ensure!(payload.len() <= 256 * 1024, "receipt budget exceeded");
        sqlx::query(
            "INSERT OR IGNORE INTO receipt_outbox VALUES(?,?,?,?,'pending',0,?,NULL,NULL,?)",
        )
        .bind(id)
        .bind(&self.config.device_id)
        .bind(&self.config.gateway_url)
        .bind(&payload)
        .bind(now())
        .bind(now())
        .execute(&mut *connection)
        .await?;
        let (device, origin, saved): (String, String, String) =
            sqlx::query_as("SELECT device,origin,payload FROM receipt_outbox WHERE id=?")
                .bind(id)
                .fetch_one(&mut *connection)
                .await?;
        ensure!(
            device == self.config.device_id
                && origin == self.config.gateway_url
                && saved == payload,
            "receipt_identity_conflict: retain original result and destination"
        );
        Ok(())
    }
    pub async fn enqueue(&self, id: &str, value: &Value) -> Result<()> {
        let mut tx = self.store.pool.begin().await?;
        self.enqueue_tx(&mut tx, id, value).await?;
        tx.commit().await?;
        self.changed.notify_waiters();
        Ok(())
    }
    /// Persist final local result and its delivery intent in one transaction.
    pub async fn complete(
        &self,
        id: &str,
        local_record: &impl Serialize,
        value: &Value,
    ) -> Result<()> {
        let mut tx = self.store.pool.begin().await?;
        sqlx::query("INSERT INTO kv VALUES('local_operation',?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
            .bind(id).bind(serde_json::to_string(local_record)?).bind(i64::MAX)
            .execute(&mut *tx).await?;
        self.enqueue_tx(&mut tx, id, value).await?;
        tx.commit().await?;
        self.changed.notify_waiters();
        Ok(())
    }
    async fn claim(&self) -> Result<Option<Claim>> {
        // An empty UPDATE still acquires SQLite's writer. Idle senders run four
        // times per second, so inspect the indexed queue before attempting a claim.
        // The UPDATE below repeats every predicate and remains the authority.
        let due: Option<String> = sqlx::query_scalar("SELECT id FROM receipt_outbox WHERE device=? AND origin=? AND state IN ('pending','sending') AND next_attempt<=? ORDER BY next_attempt,id LIMIT 1")
            .bind(&self.config.device_id).bind(&self.config.gateway_url).bind(now())
            .fetch_optional(&self.store.pool).await?;
        if due.is_none() {
            return Ok(None);
        }
        let lease = crate::random();
        let row = sqlx::query("UPDATE receipt_outbox SET state='sending',attempts=attempts+1,next_attempt=?,lease=? WHERE id=(SELECT id FROM receipt_outbox WHERE device=? AND origin=? AND state IN ('pending','sending') AND next_attempt<=? ORDER BY next_attempt,id LIMIT 1) RETURNING id,payload,attempts")
            .bind(now()+15).bind(&lease).bind(&self.config.device_id).bind(&self.config.gateway_url)
            .bind(now()).fetch_optional(&self.store.pool).await?;
        row.map(|row| {
            Ok(Claim {
                id: row.try_get("id")?,
                payload: row.try_get("payload")?,
                attempt: row.try_get("attempts")?,
                lease,
            })
        })
        .transpose()
    }
    async fn send(&self, client: &reqwest::Client, claim: &Claim) -> Outcome {
        // Total timeout is appropriate for this bounded 256 KiB control message,
        // not for long file transfers. It covers the response body as well.
        let response = client
            .post(format!("{}/device/result", self.config.gateway_url))
            .bearer_auth(&self.config.device_token)
            .header("content-type", "application/json")
            .body(claim.payload.clone())
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        let Ok(mut response) = response else {
            return Outcome::Retry(None);
        };
        let status = response.status().as_u16();
        if status != 200 {
            return if matches!(status, 400 | 401 | 403 | 404 | 405 | 409 | 410 | 413 | 422) {
                Outcome::Blocked(status)
            } else {
                Outcome::Retry(Some(status))
            };
        }
        let mut bytes = Vec::with_capacity(128);
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len().saturating_add(chunk.len()) <= 1024 => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                _ => return Outcome::Retry(Some(status)),
            }
        }
        // A proxy's HTTP 200 or a lost response is not proof of receipt acceptance.
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) if value["accepted"] == true => Outcome::Accepted,
            _ => Outcome::Retry(Some(status)),
        }
    }
    async fn deliver(&self, client: &reqwest::Client, claim: Claim) -> Result<()> {
        let outcome = self.send(client, &claim).await;
        match outcome {
            Outcome::Accepted => {
                let mut tx = self.store.pool.begin().await?;
                let accepted = sqlx::query(
                    "DELETE FROM receipt_outbox WHERE id=? AND device=? AND origin=? AND lease=?",
                )
                .bind(&claim.id)
                .bind(&self.config.device_id)
                .bind(&self.config.gateway_url)
                .bind(&claim.lease)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                // A late acknowledgement must not remove a newer sender's recovery anchor.
                if accepted == 1 {
                    // Keep only the current-runtime deduplication receipt. A
                    // delayed poll can outlive result delivery; deleting the
                    // fingerprint here would permit repeating a mutation.
                    sqlx::query("UPDATE kv SET value=json_set(json_remove(value,'$.result'),'$.gateway_accepted',json('true')) WHERE kind='local_operation' AND key=? AND json_extract(value,'$.state')='done' AND json_extract(value,'$.tool') IS NOT NULL AND json_extract(value,'$.tool')<>'code_apply_edits'")
                        .bind(&claim.id).execute(&mut *tx).await?;
                }
                if accepted == 1 {
                    // Acknowledgement is also recorded for edits, without erasing
                    // their recovery journal/result until automatic retention.
                    sqlx::query("UPDATE kv SET value=json_set(value,'$.gateway_accepted',json('true')) WHERE kind='local_operation' AND key=? AND json_extract(value,'$.state')='done' AND json_extract(value,'$.tool')='code_apply_edits'")
                        .bind(&claim.id).execute(&mut *tx).await?;
                }
                tx.commit().await?;
            }
            Outcome::Retry(status) => {
                let backoff = 1i64 << claim.attempt.saturating_sub(1).clamp(0, 6);
                sqlx::query("UPDATE receipt_outbox SET state='pending',next_attempt=?,last_http=?,lease=NULL WHERE id=? AND lease=?")
                    .bind(now()+backoff).bind(status.map(i64::from)).bind(&claim.id).bind(&claim.lease)
                    .execute(&self.store.pool).await?;
            }
            Outcome::Blocked(status) => {
                sqlx::query("UPDATE receipt_outbox SET state='blocked',last_http=?,lease=NULL WHERE id=? AND lease=?")
                    .bind(i64::from(status)).bind(&claim.id).bind(&claim.lease).execute(&self.store.pool).await?;
                tracing::warn!(operation_id=%claim.id,http_status=status,"receipt delivery needs operator attention; result retained, no task replay");
            }
        }
        self.changed.notify_waiters();
        Ok(())
    }
    /// Read authoritative queue facts when constructing an outbound snapshot.
    /// Never persist a second copy of telemetry or reuse a previous process's
    /// health timestamp. A read failure is unavailable, not an empty queue.
    pub(crate) async fn status(store: &Store, config: &AgentConfig) -> Result<Status> {
        let row = sqlx::query("SELECT COUNT(*) AS total,COALESCE(SUM(state='blocked'),0) AS blocked,COALESCE(SUM(state='sending'),0) AS sending,MIN(created) AS oldest FROM receipt_outbox WHERE device=? AND origin=?")
            .bind(&config.device_id).bind(&config.gateway_url).fetch_one(&store.pool).await?;
        let total: i64 = row.try_get("total")?;
        let blocked: i64 = row.try_get("blocked")?;
        Ok(Status {
            protocol: 1,
            pending: u64::try_from(total - blocked)?,
            blocked: u64::try_from(blocked)?,
            sending: u64::try_from(row.try_get::<i64, _>("sending")?)?,
            oldest_created_at: row.try_get("oldest")?,
            reported_at: now(),
        })
    }
    async fn next_wake_delay(&self) -> Result<Duration> {
        let next: Option<i64> = sqlx::query_scalar("SELECT MIN(next_attempt) FROM receipt_outbox WHERE device=? AND origin=? AND state IN ('pending','sending')")
            .bind(&self.config.device_id).bind(&self.config.gateway_url)
            .fetch_one(&self.store.pool).await?;
        Ok(wake_delay(next, now()))
    }
    pub async fn run(&self, client: &reqwest::Client) -> Result<()> {
        let mut workers = tokio::task::JoinSet::new();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            while let Some(outcome) = workers.try_join_next() {
                if !matches!(outcome, Ok(Ok(()))) {
                    tracing::warn!("receipt sender unavailable; lease and saved result retained");
                }
            }
            while workers.len() < 2 {
                match self.claim().await {
                    Ok(Some(claim)) => {
                        let sender = self.clone();
                        let client = client.clone();
                        workers.spawn(async move { sender.deliver(&client, claim).await });
                    }
                    Ok(None) => break,
                    Err(_) => {
                        tracing::warn!(
                            "receipt queue unavailable; execution workers remain independent"
                        );
                        break;
                    }
                }
            }
            // Subscribe above, then read the durable queue: enqueue/complete
            // wakes immediately even when it races with this deadline lookup.
            // Timers are only for due retries, old leases and lost notifications.
            let delay = if workers.len() >= 2 {
                Duration::from_secs(5)
            } else {
                self.next_wake_delay()
                    .await
                    .unwrap_or(Duration::from_secs(1))
            };
            tokio::select! {
                _=notified=>{},
                _=tokio::time::sleep(delay)=>{},
                Some(outcome)=workers.join_next(),if !workers.is_empty()=>{
                    if !matches!(outcome,Ok(Ok(()))) {tracing::warn!("receipt delivery interrupted; lease retained");}
                }
            }
        }
    }
}

fn wake_delay(next: Option<i64>, at: i64) -> Duration {
    Duration::from_secs(next.map_or(5, |due| due.saturating_sub(at).clamp(1, 5) as u64))
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;

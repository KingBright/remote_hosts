//! Independent durable state; never shares the operator/connector database.
use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::{path::Path, time::Duration};

#[derive(Clone)]
pub struct Store {
    pub pool: SqlitePool,
}
/// Keyset page, ordered by key. Concurrent mutations are not a global snapshot.
pub struct StatePage<T> {
    pub entries: Vec<(String, T)>,
    pub next_key: Option<String>,
}
impl Store {
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(dir.join("state.sqlite"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(10));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS kv (kind TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, expires INTEGER NOT NULL, PRIMARY KEY(kind,key))").execute(&pool).await?;
        // One shared state index serves startup recovery, drain checks and terminal reconciliation.
        // Do not grow a family of per-feature JSON indexes on the generic store.
        sqlx::query("CREATE INDEX IF NOT EXISTS kv_kind_state_created ON kv(kind,json_extract(value,'$.state'),json_extract(value,'$.created_at'))")
            .execute(&pool)
            .await?;
        // Only finite-lived rows participate. Permanent execution evidence does
        // not pay an index-entry cost or get scanned by expiry maintenance.
        sqlx::query("CREATE INDEX IF NOT EXISTS kv_expiring ON kv(expires,kind,key) WHERE expires<9223372036854775807")
            .execute(&pool).await?;
        Ok(Self { pool })
    }
    pub async fn install_agent_schema(&self) -> Result<()> {
        // One agent-only partial index matches the bounded replication order.
        // Without it LIMIT 24 still sorts/parses every historical terminal row.
        // Other KV kinds and the Gateway do not pay this write amplification.
        sqlx::query("CREATE INDEX IF NOT EXISTS terminal_sync_recent ON kv(kind,(json_extract(value,'$.state') IN ('running','starting')) DESC,COALESCE(json_extract(value,'$.updated_at'),json_extract(value,'$.created_at'),0) DESC,key) WHERE kind='terminal'")
            .execute(&self.pool).await?;
        crate::work_events::install(self).await
    }
    pub async fn install_gateway_schema(&self) -> Result<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, device TEXT NOT NULL, idem TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, request TEXT NOT NULL, result TEXT, state TEXT NOT NULL, updated INTEGER NOT NULL)").execute(&self.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS jobs_device_state ON jobs(device,state,updated)")
            .execute(&self.pool)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS semantic_guards (semantic TEXT PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL, updated INTEGER NOT NULL)")
            .execute(&self.pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS kv_task_owner_id ON kv(kind,json_extract(value,'$.owner'),json_extract(value,'$.task_id'),key) WHERE kind='task_operation'")
            .execute(&self.pool).await?;
        // The status page reads only recent unbound receipts. Retaining evidence
        // must not turn that bounded view into a sort of the entire request history.
        sqlx::query("CREATE INDEX IF NOT EXISTS kv_unbound_request_owner_time ON kv(json_extract(value,'$.owner'),json_extract(value,'$.received_at') DESC) WHERE kind='request_receipt' AND json_extract(value,'$.operation_id') IS NULL")
            .execute(&self.pool).await?;
        // Keep timing separate for one compatibility release. 0.10.3 rollback
        // still writes the legacy eight-column jobs row positionally.
        sqlx::query("CREATE TABLE IF NOT EXISTS operation_timing (id TEXT PRIMARY KEY,queued_ms INTEGER NOT NULL,dispatched_ms INTEGER,result_ms INTEGER)")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn put(
        &self,
        kind: &str,
        key: &str,
        value: &impl Serialize,
        expires: i64,
    ) -> Result<()> {
        sqlx::query("INSERT INTO kv VALUES(?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value, expires=excluded.expires").bind(kind).bind(key).bind(serde_json::to_string(value)?).bind(expires).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn get<T: DeserializeOwned>(&self, kind: &str, key: &str) -> Result<Option<T>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM kv WHERE kind=? AND key=? AND expires>?")
                .bind(kind)
                .bind(key)
                .bind(crate::now())
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(s,)| serde_json::from_str(&s).context("stored state malformed"))
            .transpose()
    }
    pub async fn take<T: DeserializeOwned>(&self, kind: &str, key: &str) -> Result<Option<T>> {
        let row: Option<(String,)> =
            sqlx::query_as("DELETE FROM kv WHERE kind=? AND key=? AND expires>? RETURNING value")
                .bind(kind)
                .bind(key)
                .bind(crate::now())
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(s,)| serde_json::from_str(&s).context("stored state malformed"))
            .transpose()
    }
    pub async fn list<T: DeserializeOwned>(&self, kind: &str) -> Result<Vec<T>> {
        let page = self.list_page(kind, None, 1000).await?;
        ensure!(
            page.next_key.is_none(),
            "state_list_truncated: more than 1000 entries; use list_page and next_key"
        );
        Ok(page.entries.into_iter().map(|(_, value)| value).collect())
    }
    pub async fn list_page<T: DeserializeOwned>(
        &self,
        kind: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<StatePage<T>> {
        ensure!(
            (1..=1000).contains(&limit),
            "state_page_limit must be 1..1000"
        );
        let mut rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key,value FROM kv WHERE kind=? AND expires>? AND (? IS NULL OR key>?) ORDER BY key LIMIT ?")
            .bind(kind).bind(crate::now()).bind(after).bind(after).bind((limit+1) as i64)
            .fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_key = if has_more {
            rows.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        let entries = rows
            .into_iter()
            .map(|(key, text)| {
                serde_json::from_str(&text)
                    .map(|value| (key, value))
                    .context("stored state malformed")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StatePage { entries, next_key })
    }
    pub async fn prune(&self) -> Result<()> {
        self.prune_batch().await.map(|_| ())
    }
    /// Reclamation is bounded; get/take still enforce expiry immediately.
    /// The read-before-write is a hint only. The DELETE rechecks eligibility
    /// atomically, so a concurrent renewal can never be deleted from a stale list.
    pub async fn prune_batch(&self) -> Result<u64> {
        let cutoff = crate::now();
        let due: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kv WHERE expires<9223372036854775807 AND expires<=? LIMIT 1)")
            .bind(cutoff).fetch_one(&self.pool).await?;
        if !due {
            return Ok(0);
        }
        let deleted = sqlx::query("DELETE FROM kv WHERE (kind,key) IN (SELECT kind,key FROM kv WHERE expires<9223372036854775807 AND expires<=? ORDER BY expires,kind,key LIMIT 256) AND expires<9223372036854775807 AND expires<=?")
            .bind(cutoff).bind(cutoff).execute(&self.pool).await?;
        Ok(deleted.rows_affected())
    }
}

#[cfg(test)]
#[path = "store_cost_tests.rs"]
mod cost_tests;

#[cfg(test)]
mod tests {
    use super::*;

    async fn table_exists(store: &Store, name: &str) -> bool {
        sqlx::query_as::<_, (i64,)>("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
            .bind(name)
            .fetch_optional(&store.pool)
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn base_store_installs_only_shared_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        assert!(table_exists(&store, "kv").await);
        assert!(!table_exists(&store, "jobs").await);
        assert!(!table_exists(&store, "work_events").await);
        assert!(!table_exists(&store, "operation_timing").await);
    }

    #[tokio::test]
    async fn role_schema_does_not_install_the_other_side() {
        let agent_dir = tempfile::tempdir().unwrap();
        let agent = Store::open(agent_dir.path()).await.unwrap();
        agent.install_agent_schema().await.unwrap();
        assert!(table_exists(&agent, "work_events").await);
        assert!(!table_exists(&agent, "jobs").await);

        let gateway_dir = tempfile::tempdir().unwrap();
        let gateway = Store::open(gateway_dir.path()).await.unwrap();
        gateway.install_gateway_schema().await.unwrap();
        assert!(table_exists(&gateway, "jobs").await);
        assert!(table_exists(&gateway, "semantic_guards").await);
        assert!(table_exists(&gateway, "operation_timing").await);
        assert!(!table_exists(&gateway, "work_events").await);
    }

    #[tokio::test]
    async fn gateway_jobs_schema_remains_0103_rollback_compatible() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.install_gateway_schema().await.unwrap();
        sqlx::query(
            "INSERT INTO jobs VALUES('id','device','idem','fingerprint','{}',NULL,'queued',0)",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE id='id'")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}

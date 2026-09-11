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
        sqlx::query("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, device TEXT NOT NULL, idem TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, request TEXT NOT NULL, result TEXT, state TEXT NOT NULL, updated INTEGER NOT NULL)").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS jobs_device_state ON jobs(device,state,updated)")
            .execute(&pool)
            .await?;
        let store = Self { pool };
        crate::work_events::install(&store).await?;
        Ok(store)
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
        sqlx::query("DELETE FROM kv WHERE expires<=?")
            .bind(crate::now())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

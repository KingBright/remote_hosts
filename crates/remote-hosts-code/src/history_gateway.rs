//! Gateway result retention preserves compact immutable replay evidence.
//! Old bodies expire, not operation identities or exactly-once protection.
use crate::{gateway::Gateway, history_retention::Policy, store::Store};
use anyhow::Result;
use serde_json::{Value, json};
use std::time::Duration;

type Row = (String, String, String, String, i64, Option<String>);
const SCAN: &str = "SELECT j.id,j.device,j.request,j.result,j.updated,o.value FROM jobs j LEFT JOIN kv o ON o.kind='terminal_observation' AND o.key=j.id WHERE j.id>? AND j.state='done' AND j.result IS NOT NULL AND NOT EXISTS(SELECT 1 FROM history_result_digests h WHERE h.id=j.id) AND NOT EXISTS(SELECT 1 FROM semantic_guards s WHERE s.operation_id=j.id) ORDER BY j.id LIMIT 64";
pub(crate) async fn install(store: &Store) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS history_result_digests(id TEXT PRIMARY KEY,digest TEXT NOT NULL)")
        .execute(&store.pool).await?;
    Ok(())
}
fn summary(row: &Row, at: i64) -> Result<Option<(Value, bool, i64)>> {
    let request: Value = serde_json::from_str(&row.2)?;
    let result: Value = serde_json::from_str(&row.3)?;
    if result["history_hold"] == true || at < row.4 {
        return Ok(None);
    }
    // A delivered tool reply can still describe an unconfirmed mutation.
    // Terminal submissions are resolved below using their final observation.
    if request["tool"] != "terminal_exec"
        && (result["pending"] == true
            || matches!(
                result["state"].as_str(),
                Some(
                    "unknown" | "runtime_lost" | "interrupted" | "running" | "starting" | "pending"
                )
            )
            || matches!(
                result["execution_state"].as_str(),
                Some("unknown" | "not_started_or_unknown" | "not_started_or_partial")
            ))
    {
        return Ok(None);
    }
    let error = result["error"].as_str().unwrap_or_default();
    if error.contains("unknown")
        || error.starts_with("partial")
        || matches!(
            result["state"].as_str(),
            Some("partial" | "paused" | "awaiting_source" | "publishing" | "outcome_unknown")
        )
        || result["change_set"]
            .as_object()
            .is_some_and(|_| result["change_set"]["state"] != "completed")
    {
        return Ok(None);
    }
    let mut brief = json!({"state":"history_expired","error":"history_expired","operation_id":row.0,
        "history":{"expired_at":at,"automatic":true,"original_state":result["state"],
            "body_available":false,"command_replayed":false},
        "next_action":"do_not_replay_completed_operation",
        "error_code":"history_expired","execution_state":"completed",
        "failure_boundary":"history_retention",
        "retry_policy":"do_not_replay_completed_operation"});
    let mut finished_at = row.4.max(0);
    let mut failed = !error.is_empty()
        || matches!(
            result["state"].as_str(),
            Some("failed" | "cancelled" | "timed_out")
        );
    if request["tool"] == "terminal_exec" {
        let observed: Option<Value> = row.5.as_deref().map(serde_json::from_str).transpose()?;
        let terminal = observed
            .as_ref()
            .map(|v| &v["terminal"])
            .filter(|v| v.is_object())
            .unwrap_or(&result["terminal"]);
        if terminal["output_complete"] != true
            || !matches!(
                terminal["state"].as_str(),
                Some("exited" | "cancelled" | "timed_out" | "failed")
            )
        {
            return Ok(None);
        }
        let terminal_finished = terminal["updated_at"].as_i64().filter(|v| *v > 0);
        let execution = crate::receipts::decision(&json!({"terminal":terminal}), None, None, at);
        if !matches!(
            execution["execution_state"].as_str(),
            Some("exited" | "not_started")
        ) {
            return Ok(None);
        }
        finished_at = terminal_finished.map_or(0, |finished| finished_at.max(finished));
        failed = terminal["state"] != "exited"
            || terminal["exit_code"] != 0
            || terminal["output_error"].is_string();
        // Final process facts are small; neither preview nor command/output body
        // is carried forward as though it were still available.
        brief["terminal"] = terminal.clone();
    }
    if let Some(revision) = result.get("transfer_revision") {
        brief["transfer_revision"] = revision.clone();
    }
    Ok(Some((brief, failed, finished_at)))
}
async fn legacy_clock(store: &Store, row: &Row, at: i64) -> Result<Option<(i64, bool)>> {
    if let Some(saved) = store
        .get::<Value>("history_retention_clock", &row.0)
        .await?
    {
        return Ok(saved["confirmed_at"]
            .as_i64()
            .filter(|v| *v > 0)
            .map(|v| (v, false)));
    }
    anyhow::ensure!(at > 0, "invalid_legacy_retention_clock");
    let mut tx = crate::history_retention::begin_maintenance(store).await?;
    // Do not alter the original result: its digest must still accept a lost-ACK
    // retry. This tiny legacy-only clock is removed with the eventual body.
    let inserted = sqlx::query("INSERT OR IGNORE INTO kv(kind,key,value,expires) SELECT 'history_retention_clock',j.id,?,? FROM jobs j WHERE j.id=? AND j.device=? AND j.request=? AND j.result=? AND j.state='done' AND NOT EXISTS(SELECT 1 FROM semantic_guards s WHERE s.operation_id=j.id) AND NOT EXISTS(SELECT 1 FROM history_result_digests h WHERE h.id=j.id) AND (SELECT o.value FROM kv o WHERE o.kind='terminal_observation' AND o.key=j.id) IS ?")
        .bind(json!({"confirmed_at":at}).to_string()).bind(i64::MAX)
        .bind(&row.0).bind(&row.1).bind(&row.2).bind(&row.3).bind(&row.5)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok((inserted.rows_affected() == 1).then_some((at, true)))
}

async fn retire(store: &Store, row: &Row, brief: &Value) -> Result<bool> {
    let mut request: Value = serde_json::from_str(&row.2)?;
    let original: Value = serde_json::from_str(&row.3)?;
    // Owner/device/tool/workspace are retained for all later authorization.
    request["arguments"] = json!({"workspace_id":request["arguments"]["workspace_id"]});
    let mut tx = crate::history_retention::begin_maintenance(store).await?;
    let safe:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=? AND device=? AND request=? AND result=? AND state='done' AND NOT EXISTS(SELECT 1 FROM semantic_guards s WHERE s.operation_id=jobs.id) AND NOT EXISTS(SELECT 1 FROM history_result_digests h WHERE h.id=jobs.id) AND (SELECT o.value FROM kv o WHERE o.kind='terminal_observation' AND o.key=jobs.id) IS ?)")
        .bind(&row.0).bind(&row.1).bind(&row.2).bind(&row.3).bind(&row.5).fetch_one(&mut *tx).await?;
    if !safe {
        tx.rollback().await?;
        return Ok(false);
    }
    sqlx::query("INSERT INTO history_result_digests(id,digest) VALUES(?,?)")
        .bind(&row.0)
        .bind(crate::hash(original.to_string()))
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE jobs SET request=?,result=? WHERE id=?")
        .bind(request.to_string())
        .bind(brief.to_string())
        .bind(&row.0)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM kv WHERE kind IN ('terminal_observation','operation_progress','receive_progress','history_retention_clock') AND key=?")
        .bind(&row.0).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub(crate) async fn sweep(store: &Store, policy: &Policy, at: i64) -> Result<Value> {
    let mut after = String::new();
    let mut kept = 0usize;
    let mut bytes = 0u64;
    let mut expired = 0u64;
    let mut protected = 0u64;
    let mut legacy_clocks_initialized = 0u64;
    let mut oldest = Vec::<(Row, Value)>::new();
    loop {
        let rows: Vec<Row> = sqlx::query_as(SCAN)
            .bind(&after)
            .fetch_all(&store.pool)
            .await?;
        if rows.is_empty() {
            break;
        }
        for mut row in rows {
            after = row.0.clone();
            let Some((brief, failed, finished_at)) = summary(&row, at)? else {
                protected += 1;
                continue;
            };
            row.4 = if finished_at == 0 {
                let Some((confirmed, initialized)) = legacy_clock(store, &row, at).await? else {
                    protected += 1;
                    continue;
                };
                legacy_clocks_initialized += u64::from(initialized);
                confirmed
            } else {
                finished_at
            };
            let age = at.saturating_sub(row.4);
            let ttl = if failed {
                policy.failed_seconds
            } else {
                policy.successful_seconds
            };
            if age >= ttl && retire(store, &row, &brief).await? {
                expired += 1;
                continue;
            }
            kept += 1;
            bytes = bytes.saturating_add((row.2.len() + row.3.len()) as u64);
            if age >= policy.minimum_seconds {
                oldest.push((row, brief));
                oldest.sort_unstable_by(|a, b| (a.0.4, &a.0.0).cmp(&(b.0.4, &b.0.0)));
                oldest.truncate(64);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for (row, brief) in oldest {
        if kept <= policy.max_items && bytes <= policy.max_bytes {
            break;
        }
        if retire(store, &row, &brief).await? {
            expired += 1;
            kept = kept.saturating_sub(1);
            bytes = bytes.saturating_sub((row.2.len() + row.3.len()) as u64);
        }
    }
    // Existing finite-lived auth/cache entries also get autonomous cleanup,
    // rather than depending on a later file-transfer request to trigger pruning.
    let expired_cache_rows = store.prune_batch().await?;
    Ok(
        json!({"expired_bodies":expired,"retained_bodies":kept,"retained_body_bytes":bytes,
        "protected_bodies":protected,"expired_cache_rows":expired_cache_rows,
        "legacy_clocks_initialized":legacy_clocks_initialized,
        "pressure_remaining":kept>policy.max_items || bytes>policy.max_bytes,
        "idempotency_records_preserved":true}),
    )
}
pub(crate) async fn run(gateway: &Gateway) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(30)).await;
    let policy = Policy::default();
    loop {
        let work=async {
            let store=crate::history_retention::maintenance_store(&gateway.config.state_dir).await?;
            let report=sweep(&store,&policy,crate::now()).await?;
            store.put("runtime","automatic_history_cleanup",&json!({"protocol":1,"automatic":true,
                "observed_at":crate::now(),"policy":policy,"last_run":report,
                "scope":"gateway result bodies; immutable idempotency and recovery anchors retained"}),i64::MAX).await?;
            let _=sqlx::query("PRAGMA wal_checkpoint(PASSIVE)").execute(&store.pool).await;
            store.pool.close().await;
            Ok::<_,anyhow::Error>(report["pressure_remaining"]==true && report["expired_bodies"].as_u64().unwrap_or(0)>0)
        }.await;
        let delay = match work {
            Ok(true) => 5,
            Ok(false) => 300,
            Err(_) => {
                tracing::warn!("gateway automatic retention deferred; original evidence retained");
                60
            }
        };
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}
#[cfg(test)]
#[path = "history_gateway_tests.rs"]
mod tests;

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
    anyhow::ensure!(
        request.is_object() && result.is_object(),
        "invalid_history_payload_shape"
    );
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

#[path = "history_gateway_scan.rs"]
mod scan;

// Test adapter drains exactly one cycle through the same production stepper.
#[cfg(test)]
pub(crate) async fn sweep(store: &Store, policy: &Policy, at: i64) -> Result<Value> {
    let mut scan = scan::Scan::default();
    let mut total = scan::Tick::default();
    for _ in 0..10000 {
        let mut tick = scan::Tick::default();
        scan::step(store, policy, at, &mut scan, &mut tick).await?;
        total.scanned += tick.scanned;
        total.writes += tick.writes;
        total.expired_bodies += tick.expired_bodies;
        total.legacy_clocks_initialized += tick.legacy_clocks_initialized;
        if scan.complete {
            let mut report = scan.report(&total, policy);
            report["expired_cache_rows"] = json!(store.prune_batch().await?);
            return Ok(report);
        }
    }
    anyhow::bail!("fixture_scan_budget_exhausted")
}

#[cfg(test)]
pub(crate) async fn maintenance_tick(
    gateway: &Gateway,
    store: &Store,
    policy: &Policy,
    at: i64,
) -> Result<Value> {
    let artifacts = crate::history_artifacts::sweep(gateway, store, at).await?;
    let mut report = sweep(store, policy, at).await?;
    report["transfer_cache"] = serde_json::to_value(artifacts)?;
    Ok(report)
}

/// Sanitized classifications only. Database bodies, SQL parameters and tokens
/// must not leak into warnings merely because a maintenance pass was deferred.
fn error_category(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<sqlx::Error>() {
        return match error {
            sqlx::Error::PoolTimedOut => "connection_budget",
            sqlx::Error::Database(db)
                if db
                    .code()
                    .and_then(|v| v.parse::<i32>().ok())
                    .is_some_and(|v| matches!(v & 255, 5 | 6)) =>
            {
                "storage_busy"
            }
            sqlx::Error::Database(_) => "storage_error",
            _ => "storage_unavailable",
        };
    }
    if error.to_string() == "maintenance_writer_wait_budget" {
        "writer_budget"
    } else {
        "maintenance_error"
    }
}

async fn bounded_tick(
    gateway: &Gateway,
    store: &Store,
    policy: &Policy,
    at: i64,
    scan: &mut scan::Scan,
) -> Value {
    let mut tick = scan::Tick::default();
    let result = scan::step(store, policy, at, scan, &mut tick).await;
    let mut report = scan.report(&tick, policy);
    if let Err(error) = result {
        report["deferred"] = json!(true);
        report["error_category"] = json!(error_category(&error));
    }
    // Independent stages: a busy body scan must not permanently prevent the
    // transfer cache and ordinary expiry queues from making their own progress.
    report["transfer_cache"] = match crate::history_artifacts::sweep(gateway, store, at).await {
        Ok(value) => serde_json::to_value(value).expect("bounded report serializes"),
        Err(error) => json!({"deferred":true,"error_category":error_category(&error)}),
    };
    match store.prune_batch().await {
        Ok(count) => report["expired_cache_rows"] = json!(count),
        Err(error) => report["cache_prune_error"] = json!(error_category(&error)),
    }
    report
}

fn next_delay(report: &Value) -> u64 {
    if report["deferred"] == true
        || report["transfer_cache"]["deferred"] == true
        || !report["cache_prune_error"].is_null()
    {
        60
    } else if report["has_more"] == true
        || report["transfer_cache"]["has_more"] == true
        || (report["pressure_remaining"] == true
            && report["expired_bodies"].as_u64().unwrap_or(0) > 0)
    {
        5
    } else {
        300
    }
}

pub(crate) async fn run(gateway: &Gateway) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(30)).await;
    let policy = Policy::default();
    let mut scan = scan::Scan::default();
    let mut restored = false;
    // Keep the private maintenance connection across ticks; repeated pool
    // initialization must not compete with all five live polling lanes.
    let mut connection = None;
    loop {
        if connection.is_none() {
            match crate::history_retention::maintenance_store(&gateway.config.state_dir).await {
                Ok(store) => connection = Some(store),
                Err(error) => {
                    tracing::warn!(
                        category = error_category(&error),
                        stage = "connect",
                        "gateway automatic retention deferred"
                    );
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
            }
        }
        let store = connection
            .as_ref()
            .expect("maintenance connection initialized");
        if !restored {
            match store
                .get::<Value>("runtime", "automatic_history_cleanup")
                .await
            {
                Ok(saved) => {
                    if let Some(saved) = saved {
                        scan = scan::Scan::restore(&saved["scan_checkpoint"], crate::now());
                    }
                    restored = true;
                }
                Err(error) => {
                    tracing::warn!(
                        category = error_category(&error),
                        stage = "restore",
                        "gateway automatic retention deferred"
                    );
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
            }
        }
        let started = std::time::Instant::now();
        let report = bounded_tick(gateway, store, &policy, crate::now(), &mut scan).await;
        let mut delay = next_delay(&report);
        let status = json!({"protocol":2,"automatic":true,"observed_at":crate::now(),
            "elapsed_ms":started.elapsed().as_millis(),"policy":policy,"last_run":report,
            "scan_checkpoint":scan,"scope":"gateway result bodies; immutable idempotency and recovery anchors retained",
            "budgets":{"body_rows_per_tick":scan::READ_BUDGET,"body_writes_per_tick":scan::WRITE_BUDGET,
                "cooperative_wall_ms":1000,"commits_cancelled":false}});
        // Publish even a partial/failed round. Failed publication keeps the
        // in-memory cursor. A crash resumes the last durable cursor safely.
        if let Err(error) = store
            .put("runtime", "automatic_history_cleanup", &status, i64::MAX)
            .await
        {
            tracing::warn!(
                category = error_category(&error),
                stage = "publish",
                scanned = report["scanned"].as_u64().unwrap_or(0),
                retired = report["expired_bodies"].as_u64().unwrap_or(0),
                "gateway retention progress retained in memory; status write deferred"
            );
            delay = 60;
        }
        if scan.complete && delay == 300 {
            let _ = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .execute(&store.pool)
                .await;
        }
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}
#[cfg(test)]
#[path = "history_gateway_tests.rs"]
mod tests;

//! Read-only projections for human status and cross-device task recovery.
//! Links live in the existing kv store; job state remains the only execution authority.
use crate::{
    auth::Principal,
    gateway::{Gateway, Job},
    hash, now, receipts, tools,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};

#[derive(sqlx::FromRow)]
struct ActivityRow {
    id: String,
    request: String,
    state: String,
    updated: i64,
    result: Option<String>,
    terminal: Option<String>,
    progress: Option<String>,
    online: Option<String>,
}

/// One bounded read supplies every operation projection. No N+1 queries, and
/// no result decoration that could issue new download credentials while reading.
async fn load(g: &Gateway, ids: &[String]) -> Result<HashMap<String, ActivityRow>> {
    ensure!(ids.len() <= 100, "activity_page_too_large");
    let rows: Vec<ActivityRow> = sqlx::query_as(
        "SELECT j.id,j.request,j.state,j.updated,j.result,t.value AS terminal,COALESCE(r.value,p.value) AS progress,o.value AS online FROM jobs j LEFT JOIN kv t ON t.kind='terminal_observation' AND t.key=j.id AND t.expires>? LEFT JOIN kv p ON p.kind='operation_progress' AND p.key=j.id AND p.expires>? LEFT JOIN kv r ON r.kind='receive_progress' AND r.key=j.id AND r.expires>? AND json_extract(j.request,'$.tool')='file_download' LEFT JOIN kv o ON o.kind='online' AND o.key=j.device AND o.expires>? WHERE j.id IN (SELECT value FROM json_each(?))")
        .bind(now()).bind(now()).bind(now()).bind(now()).bind(serde_json::to_string(ids)?)
        .fetch_all(&g.store.pool).await?;
    Ok(rows.into_iter().map(|row| (row.id.clone(), row)).collect())
}

fn operation(g: &Gateway, p: &Principal, row: &ActivityRow) -> Result<Value> {
    let id = &row.id;
    let job: Job = serde_json::from_str(&row.request)?;
    let scope = tools::scope(&job.tool).context("unknown original tool")?;
    ensure!(
        job.owner == p.owner
            && p.owner == g.config.owner
            && p.scopes.iter().any(|s| s == scope)
            && g.config
                .devices
                .iter()
                .any(|d| d.id == job.device_id && d.scopes.iter().any(|s| s == scope)),
        "operation_unavailable: original operation access is required"
    );
    // Do not decorate results with download URLs or refreshable credentials.
    // A status read must not mutate transfers or depend on an expired artifact.
    let mut value: Value = row
        .result
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or_else(|| json!({}));
    value["operation_id"] = json!(id);
    value["pending"] = json!(matches!(row.state.as_str(), "queued" | "dispatched"));
    if matches!(row.state.as_str(), "paused" | "awaiting_source") {
        value["state"] = json!(row.state);
    }
    if job.tool == "terminal_exec" {
        value["terminal_id"] = json!(id);
        if let Some(raw) = row.terminal.as_deref() {
            let observed =
                crate::terminal_sync::observation_value(&serde_json::from_str(raw)?, now());
            crate::terminal_sync::project_result(&mut value, observed);
        }
    }
    if let Some(raw) = row.progress.as_deref() {
        let progress: Value = serde_json::from_str(raw)?;
        value["progress"] = progress["snapshot"].clone();
        value["progress_stale"] = json!(
            value["pending"] == true
                && !(0..=45).contains(&(now() - progress["reported_at"].as_i64().unwrap_or(0)))
        );
    }
    let receipt = receipts::decision(&value, None, Some(id), row.updated);
    let terminal = &value["terminal_observation"]["terminal"];
    let terminal = if terminal.is_object() {
        terminal
    } else {
        &value["terminal"]
    };
    let online: Option<Value> = row
        .online
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;
    let device_seen = online.as_ref().and_then(|v| v["last_seen"].as_i64());
    let online = device_seen.is_some_and(|at| (0..45).contains(&(now() - at)));
    let executing = matches!(receipt["execution_state"].as_str(), Some("running"));
    let unresolved = receipt["execution_state"] == "unknown" || receipt["stale"] == true;
    let display = if matches!(row.state.as_str(), "queued" | "dispatched") && !online {
        "waiting_device"
    } else if unresolved {
        "stale_or_unknown"
    } else if receipt["execution_state"] == "paused" {
        if value["state"] == "awaiting_source" {
            "awaiting_source"
        } else {
            "paused"
        }
    } else if executing {
        "process_running"
    } else if row.state == "queued" {
        "queued"
    } else if row.state == "dispatched" {
        "awaiting_device_result"
    } else if matches!(
        receipt["process_outcome"].as_str(),
        Some("cancelled" | "timed_out")
    ) {
        if receipt["process_outcome"] == "cancelled" {
            "process_cancelled"
        } else {
            "process_timed_out"
        }
    } else if value.get("error").is_some() || receipt["execution_state"] == "not_started" {
        "operation_failed"
    } else if terminal["exit_code"].as_i64().is_some_and(|exit| exit != 0) {
        "process_failed"
    } else if terminal.is_object() && receipt["evidence_complete"] != true {
        "output_incomplete"
    } else {
        "completed"
    };
    let device_name = g
        .config
        .devices
        .iter()
        .find(|d| d.id == job.device_id)
        .map(|d| d.name.as_str());
    Ok(
        json!({"operation_id":id,"device_id":job.device_id,"device_name":device_name,
        "workspace_id":job.arguments["workspace_id"],"tool":job.tool,
        "working_directory":terminal.get("working_directory").or_else(||job.arguments.get("root")),
        "transport_state":row.state,"state":display,"updated_at":row.updated,
        "active":executing || matches!(row.state.as_str(),"queued"|"dispatched"),
        "uncertain":unresolved,"stale":receipt["stale"],
        "process":{"pid":terminal["process_id"],"state":terminal["state"],"exit_code":terminal["exit_code"],"evidence_source":"agent_terminal_snapshot"},
        "last_confirmed_event":{"stage":receipt["last_confirmed_stage"],"at":value["terminal_observation"].get("reported_at").cloned().unwrap_or(json!(row.updated))},
        "last_progress_at":value["terminal_observation"]["last_progress_at"],
        "progress":value["progress"],"heartbeat_at":device_seen,
        "blocked_reason":receipt["error_code"],"needs_user_action":receipt["user_action"].as_str().is_some_and(|a|a!="none"&&!a.starts_with("none_")),
        "input_fingerprint":hash(serde_json::to_vec(&job.arguments)?),
        "verification":value.get("verification").cloned().unwrap_or(json!({"state":"not_attached","business_acceptance":"not_implied"})),
        "receipt":receipt}),
    )
}

pub(crate) async fn task(g: &Gateway, p: &Principal, args: &Value) -> Result<Value> {
    let task = args["task_id"].as_str().context("task_id required")?;
    ensure!(receipts::valid_task_id(task), "invalid_task_id");
    let limit = crate::files::number(args, "limit", 50, 1, 100)?;
    let after = args.get("after_operation").and_then(Value::as_str);
    if let Some(id) = after {
        uuid::Uuid::parse_str(id).context("invalid task page cursor")?;
    }
    let count: i64=sqlx::query_scalar("SELECT COUNT(*) FROM kv WHERE kind='task_operation' AND json_extract(value,'$.owner')=? AND json_extract(value,'$.task_id')=? AND expires>?")
        .bind(&p.owner).bind(task).bind(now()).fetch_one(&g.store.pool).await?;
    let mut ids: Vec<String>=sqlx::query_scalar("SELECT key FROM kv WHERE kind='task_operation' AND json_extract(value,'$.owner')=? AND json_extract(value,'$.task_id')=? AND expires>? AND (? IS NULL OR key>?) ORDER BY key LIMIT ?")
        .bind(&p.owner).bind(task).bind(now()).bind(after).bind(after).bind((limit+1) as i64).fetch_all(&g.store.pool).await?;
    let more = ids.len() > limit;
    ids.truncate(limit);
    let rows = load(g, &ids).await?;
    let mut items = Vec::with_capacity(ids.len());
    // Reauthorize every row before returning the batch. A task label never grants access.
    for id in &ids {
        items.push(operation(
            g,
            p,
            rows.get(id).context("operation_unavailable")?,
        )?);
    }
    let mut devices = BTreeSet::new();
    let mut workspaces = BTreeSet::new();
    for item in &items {
        if let Some(id) = item["device_id"].as_str() {
            devices.insert(id.to_owned());
        }
        if let Some(id) = item["workspace_id"].as_str() {
            workspaces.insert(id.to_owned());
        }
    }
    let active = items.iter().filter(|v| v["active"] == true).count();
    let uncertain = items.iter().filter(|v| v["uncertain"] == true).count();
    let next = items
        .iter()
        .find(|v| {
            v["uncertain"] == true || v["active"] == true || !v["receipt"]["next_action"].is_null()
        })
        .map(|v| v["operation_id"].clone());
    let identity = hash(serde_json::to_vec(
        &json!({"task":task,"owner":p.owner,"page":after,"items":items.iter().map(|v|
        json!({"id":v["operation_id"],"state":v["state"],"event":v["last_confirmed_event"]["stage"],"progress":v["last_progress_at"],"stale":v["stale"],"receipt":v["receipt"],"verification":v["verification"]})).collect::<Vec<_>>()}),
    )?);
    let changed = args.get("cursor").and_then(Value::as_str) != Some(identity.as_str());
    Ok(
        json!({"protocol":1,"task_id":task,"scope":"authorized operations linked to this owner and task",
        "observed_at":now(),"changed":changed,"cursor":identity,
        "operations":if changed{items}else{Vec::<Value>::new()},"devices":devices,"workspaces":workspaces,
        "summary":{"linked_operations":count,"page_operations":ids.len(),"active_in_page":active,"uncertain_in_page":uncertain},
        "has_more":more,"scope_complete":!more&&after.is_none(),
        "next_page":if more{ids.last().cloned()}else{None},
        "next_action":if more{"continue_task_page"}else if next.is_some(){"observe_existing_operation"}else if count==0{"not_observed_or_expired"}else{"no_active_work_in_page"},
        "next_operation_id":next,"automatic_replay":false,
        "last_verified_source":null,"verification_note":"Only attached verification receipts prove source/test identity; terminal exit alone does not.",
        "retention_seconds":receipts::RETENTION,"retention_policy":"until_explicit_owner_cleanup"}),
    )
}

pub(crate) async fn status(g: &Gateway, p: &Principal) -> Result<Value> {
    let fleet = g.dispatch(p, "fleet_status", json!({})).await?;
    let ids:Vec<String>=sqlx::query_scalar("SELECT j.id FROM jobs j LEFT JOIN kv t ON t.kind='terminal_observation' AND t.key=j.id AND t.expires>? WHERE json_extract(j.request,'$.owner')=? ORDER BY (j.state IN ('queued','dispatched') OR json_extract(t.value,'$.terminal.state') IN ('running','starting')) DESC,j.updated DESC,j.id LIMIT 101")
        .bind(now()).bind(&p.owner).fetch_all(&g.store.pool).await?;
    let truncated = ids.len() > 100;
    let rows = load(g, &ids.iter().take(100).cloned().collect::<Vec<_>>()).await?;
    let mut items = Vec::new();
    let mut unavailable = 0usize;
    for id in ids.iter().take(100) {
        // Filtering or an observation failure cannot turn an incomplete page
        // into evidence that there is no active work.
        match rows
            .get(id)
            .context("operation_unavailable")
            .and_then(|row| operation(g, p, row))
        {
            Ok(item) => items.push(item),
            Err(_) => unavailable += 1,
        }
    }
    let active = items.iter().filter(|v| v["active"] == true).count();
    let uncertain = items.iter().filter(|v| v["uncertain"] == true).count();
    let requests:Vec<String>=sqlx::query_scalar("SELECT value FROM kv WHERE kind='request_receipt' AND json_extract(value,'$.owner')=? AND json_extract(value,'$.operation_id') IS NULL AND expires>? ORDER BY json_extract(value,'$.received_at') DESC LIMIT 20")
        .bind(&p.owner).bind(now()).fetch_all(&g.store.pool).await?;
    let mut request_views = Vec::new();
    for raw in requests {
        let mut entry: Value = serde_json::from_str(&raw)?;
        let permitted = entry["scope"]
            .as_str()
            .is_some_and(|scope| p.scopes.iter().any(|s| s == scope));
        if !permitted {
            continue;
        }
        let object = entry.as_object_mut().context("invalid receipt")?;
        object.remove("owner");
        object.remove("fingerprint");
        entry["stale"] = json!(
            entry["state"] == "gateway_received"
                && now() - entry["updated_at"].as_i64().unwrap_or(0) > 45
        );
        request_views.push(entry);
    }
    let unconfirmed_requests = request_views
        .iter()
        .filter(|v| {
            matches!(
                v["state"].as_str(),
                Some("gateway_received" | "outcome_unknown")
            )
        })
        .count();
    Ok(
        json!({"protocol":2,"observed_at":now(),"status_source":"gateway durable jobs + agent progress; no second task database",
        "summary":{"recent_operations":items.len(),"active_operations":active,"uncertain_operations":uncertain,
            "unconfirmed_requests":unconfirmed_requests,"unavailable_or_filtered_operations":unavailable,
            "scope_complete":!truncated&&unavailable==0,"state":if active>0{"active_operations"}else if uncertain>0||unconfirmed_requests>0{"outcomes_require_observation"}else if unavailable>0{"observation_incomplete"}else if truncated{"history_page_limited"}else{"no_active_work"}},
        "history_truncated":truncated,"heartbeat_is_not_business_progress":true,
        "fleet":fleet,"operations":items,"requests_without_operation":request_views}),
    )
}

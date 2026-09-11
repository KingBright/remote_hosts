//! Bounded observation of durable operations. No dispatch, lease renewal or replay.
//! Cursor means a latest-state fingerprint, not a replayable event-log position.
use crate::{auth::Principal, files, gateway::Gateway, hash, now, tools};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio::time::Instant;

struct View {
    cursor: String,
    pending: usize,
}
fn omitted(id: &str) -> Value {
    json!({"operation_id":id,"result_omitted":true,"reason":"response_budget",
        "next_action":"query_this_operation_individually"})
}
fn compact_batch_result(mut result: Value) -> Value {
    if let Some(lifecycle) = result.get_mut("operation_lifecycle") {
        let gateway = &lifecycle["gateway"];
        *lifecycle = if gateway["available"] == false {
            json!({"available":false})
        } else {
            json!({"queue_ms":gateway["queue_ms"],"dispatch_to_result_ms":gateway["dispatch_to_result_ms"]})
        };
    }
    result
}
async fn view(g: &Gateway, p: &Principal, ids: &[String]) -> Result<View> {
    let rows = sqlx::query("SELECT j.id,j.device,j.state,json_extract(j.request,'$.owner') AS owner,json_extract(j.request,'$.tool') AS tool,COALESCE(r.value,a.value) AS progress FROM jobs j LEFT JOIN kv a ON a.kind='operation_progress' AND a.key=j.id AND a.expires>? LEFT JOIN kv r ON r.kind='receive_progress' AND r.key=j.id AND r.expires>? AND json_extract(j.request,'$.tool')='file_download' WHERE j.id IN (SELECT value FROM json_each(?))")
        .bind(now()).bind(now()).bind(serde_json::to_string(ids)?).fetch_all(&g.store.pool).await?;
    ensure!(
        rows.len() == ids.len(),
        "operation_unavailable: check the original operation identifiers"
    );
    let rows: HashMap<String, _> = rows.into_iter().map(|r| (r.get("id"), r)).collect();
    let mut stamps = Vec::with_capacity(ids.len());
    let mut pending = 0;
    for id in ids {
        let row = rows.get(id).context("operation unavailable")?;
        let owner: String = row.try_get("owner")?;
        let device: String = row.try_get("device")?;
        let tool: String = row.try_get("tool")?;
        let scope = tools::scope(&tool).context("unknown original operation tool")?;
        ensure!(
            owner == p.owner
                && p.owner == g.config.owner
                && p.scopes.iter().any(|s| s == scope)
                && g.config
                    .devices
                    .iter()
                    .any(|d| d.id == device && d.scopes.iter().any(|s| s == scope)),
            "operation_unavailable: original operation access is required"
        );
        let state: String = row.try_get("state")?;
        let progress: Option<String> = row.try_get("progress")?;
        let mut stamp = json!({"id":id,"state":state});
        if tool == "terminal_exec"
            && let Some(observed) = crate::terminal_sync::observed(g, id).await?
        {
            let t = &observed["terminal"];
            stamp["terminal"] = json!({"state":t["state"],"exit_code":t["exit_code"],"output_complete":t["output_complete"],"stale":observed["stale"]});
            if state == "done"
                && observed["stale"] == false
                && matches!(t["state"].as_str(), Some("running" | "starting"))
            {
                pending += 1;
            }
        }
        if matches!(tool.as_str(), "file_upload" | "file_download") {
            let control = crate::transfer_control::control(g, id).await?;
            stamp["transfer_revision"] = json!(control.revision);
            stamp["cancel_requested"] = json!(control.cancel_requested);
        }
        if state != "done" {
            if matches!(state.as_str(), "queued" | "dispatched") {
                pending += 1;
            }
            if let Some(text) = progress {
                let value: Value = serde_json::from_str(&text)?;
                let snapshot = &value["snapshot"];
                for key in [
                    "phase",
                    "bytes_done",
                    "confirmed_bytes",
                    "total_bytes",
                    "retry_count",
                    "resumed_bytes",
                ] {
                    stamp[key] = snapshot[key].clone();
                }
                stamp["stale"] = json!(now() - value["reported_at"].as_i64().unwrap_or(0) > 15);
            }
        }
        stamps.push(stamp);
    }
    let mut scopes = p.scopes.clone();
    scopes.sort();
    Ok(View {
        cursor: hash(serde_json::to_vec(
            &json!({"owner":p.owner,"scopes":scopes,"states":stamps}),
        )?),
        pending,
    })
}
pub(crate) async fn observe(g: &Gateway, p: &Principal, args: &Value) -> Result<Value> {
    let single = args.get("operation_id").and_then(Value::as_str);
    ensure!(
        single.is_some() != args.get("operation_ids").is_some(),
        "invalid_arguments: supply exactly one of operation_id or operation_ids"
    );
    let ids: Vec<String> = if let Some(id) = single {
        vec![id.to_owned()]
    } else {
        serde_json::from_value(args["operation_ids"].clone())?
    };
    ensure!(
        !ids.is_empty() && ids.len() <= 20 && ids.iter().collect::<HashSet<_>>().len() == ids.len(),
        "invalid_arguments: use 1..20 unique operation IDs"
    );
    for id in &ids {
        uuid::Uuid::parse_str(id).context("invalid operation id")?;
    }
    if let Some(id) = single
        && args.as_object().is_some_and(|o| o.len() == 1)
    {
        return g.result(p, id).await;
    }
    let supplied = args.get("cursor").and_then(Value::as_str);
    if let Some(c) = supplied {
        ensure!(
            c.len() == 64
                && c.bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid observation cursor"
        );
    }
    let wait = files::number(args, "wait_ms", 0, 0, 5000)?;
    let budget = files::number(args, "max_bytes", 65536, 4096, 131072)?;
    let started = Instant::now();
    let deadline = started + Duration::from_millis(wait as u64);
    let mut baseline = supplied.map(str::to_owned);
    let final_view = loop {
        // Subscribe before durable read, so a committed result cannot be missed.
        let changed = g.observation_changed.notified();
        let current = view(g, p, &ids).await?;
        let previous = baseline.get_or_insert_with(|| current.cursor.clone());
        if current.cursor != *previous
            || wait == 0
            || current.pending == 0
            || Instant::now() >= deadline
        {
            break current;
        }
        tokio::select! { _=changed=>{}, _=tokio::time::sleep_until(deadline.min(Instant::now()+Duration::from_millis(500)))=>{} }
    };
    let metadata = json!({"cursor":final_view.cursor,"changed":supplied.is_none_or(|c|c!=final_view.cursor),
        "waited_ms":started.elapsed().as_millis(),"semantics":"latest_state_not_event_log","snapshot_consistency":"per_operation","observation_protocol":2});
    // The legacy no-options call returned above retains its exact shape and
    // budget. Explicit observation options honor their requested byte ceiling.
    if let Some(id) = single {
        let mut result = g.result(p, id).await?;
        result["observation"] = metadata.clone();
        if serde_json::to_vec(&result)?.len() > budget {
            result = omitted(id);
            result["observation"] = metadata;
        }
        return Ok(result);
    }
    // First preserve exact legacy per-operation shapes when the whole batch fits.
    // Only compact lifecycle metadata as a second representation when the
    // requested response budget actually requires it.
    let mut full = Vec::with_capacity(ids.len());
    for id in &ids {
        // view() authorizes the entire batch before returning any output. An
        // expired artifact/result-decoration failure is not a new execution and
        // must not discard independent authorized results. Omit error details
        // that could contain URLs or private state; retain the original ID.
        full.push(match g.result(p, id).await {
            Ok(result) => result,
            Err(_) => json!({"operation_id":id,"observation_error":{"code":"result_unavailable"},
                "next_action":"inspect_original_operation_without_reexecution"}),
        });
    }
    let output =
        json!({"operations":full,"pending_count":final_view.pending,"observation":metadata});
    if serde_json::to_vec(&output)?.len() <= budget {
        return Ok(output);
    }
    let compact: Vec<Value> = output["operations"]
        .as_array()
        .expect("batch operations array")
        .iter()
        .cloned()
        .map(compact_batch_result)
        .collect();
    let compact_output = json!({"operations":compact,"pending_count":final_view.pending,"observation":output["observation"]});
    if serde_json::to_vec(&compact_output)?.len() <= budget {
        return Ok(compact_output);
    }
    // Finally reserve actual serialized placeholders and admit compact results
    // individually. This keeps large siblings from hiding independent small ones.
    let placeholders: Vec<Value> = ids.iter().map(|id| omitted(id)).collect();
    let mut bounded = json!({"operations":placeholders,"pending_count":final_view.pending,"observation":compact_output["observation"]});
    ensure!(
        serde_json::to_vec(&bounded)?.len() <= budget,
        "observation_response_budget: query fewer operations"
    );
    for (index, result) in compact_output["operations"]
        .as_array()
        .expect("batch operations array")
        .iter()
        .cloned()
        .enumerate()
    {
        let previous = std::mem::replace(&mut bounded["operations"][index], result);
        if serde_json::to_vec(&bounded)?.len() > budget {
            bounded["operations"][index] = previous;
        }
    }
    Ok(bounded)
}

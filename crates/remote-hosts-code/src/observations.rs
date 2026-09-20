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

#[cfg(test)]
mod completion_tests;

struct View {
    cursor: String,
    pending: usize,
}
fn terminal_pending(terminal: &Value, stale: bool) -> bool {
    !stale
        && (matches!(terminal["state"].as_str(), Some("running" | "starting"))
            || (terminal["state"] == "exited"
                && terminal["output_complete"] == false
                && terminal["output_error"].is_null()
                && terminal["output_truncated"] != true))
}
fn awaiting_result(result: &Value) -> bool {
    if result.get("error").is_some()
        || result["receipt"]["stale"] == true
        || result["state"] == "outcome_unknown"
    {
        return false;
    }
    result["pending"] == true || terminal_pending(&result["terminal"], false)
}
fn omitted(id: &str, original: &Value) -> Value {
    let mut value = json!({"operation_id":id,"result_omitted":true,"reason":"response_budget",
        "next_action":"query_this_operation_individually","evidence_complete":false});
    for key in [
        "receipt",
        "error",
        "error_code",
        "outcome",
        "stale",
        "retry_policy",
        "user_action",
        "execution_state",
        "request_id",
    ] {
        if let Some(field) = original.get(key) {
            value[key] = field.clone();
        }
    }
    if value["receipt"].is_object() {
        value["receipt"]["evidence_complete"] = json!(false);
    }
    value
}
fn completed_receipt_defaults() -> Value {
    json!({"protocol":1,"failure_boundary":null,"last_confirmed_stage":"gateway_completed_request",
        "execution_state":"completed","business_state":"not_evaluated","evidence_complete":true,
        "retry_policy":"do_not_replay_completed_operation","next_action":null,"user_action":"none","stale":false})
}
fn compact_batch_result(mut result: Value) -> Value {
    if result["receipt"]["execution_state"] == "completed"
        && result["receipt"]["evidence_complete"] == true
        && result["receipt"]["stale"] == false
        && result["receipt"]["error_code"].is_null()
        && result["receipt"]["user_action"] == "none"
        && result["receipt"]["next_action"].is_null()
    {
        // Losslessly factor repeated no-action decisions into the batch defaults.
        // Uncertainty, errors and required actions always retain their own receipt.
        if let Some(object) = result.as_object_mut() {
            object.remove("receipt");
        }
    }
    if let Some(object) = result.as_object_mut() {
        object.remove("device_id");
    }
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
fn apply_terminal_cursor(result: &mut Value, args: &Value) -> Result<()> {
    let Some(requested) = args.get("terminal_cursor") else {
        return Ok(());
    };
    let requested = requested
        .as_u64()
        .context("invalid_arguments: terminal_cursor must be a non-negative integer")?;
    let Some(observation) = result.get_mut("terminal_observation") else {
        return Ok(());
    };
    let Some(start) = observation["output_cursor_start"].as_u64() else {
        return Ok(());
    };
    let Some(end) = observation["output_cursor_end"].as_u64() else {
        return Ok(());
    };
    ensure!(
        requested <= end,
        "invalid_arguments: terminal_cursor is ahead of observed output"
    );
    if requested < start {
        observation["output_gap"] = json!(true);
        observation["requested_output_cursor"] = json!(requested);
        // Do not leave a stale top-level prefix alongside the requested tail.
        let object = result.as_object_mut().context("invalid result object")?;
        object.remove("output");
        object.remove("compression");
        object.insert("result_omitted".into(), json!(true));
        object.insert("output_range_complete".into(), json!(false));
        object.insert("next_action".into(), json!("terminal_read"));
        return Ok(());
    }
    let output = observation["output"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let offset = usize::try_from(requested - start)?;
    ensure!(
        offset <= output.len() && output.is_char_boundary(offset),
        "invalid_arguments: terminal_cursor is not a UTF-8 output boundary"
    );
    let delta = &output[offset..];
    observation["output"] = json!(delta);
    observation["output_cursor_start"] = json!(requested);
    observation["output_gap"] = json!(false);
    result["output"] = json!(delta);
    result["raw_cursor_start"] = json!(requested);
    result["cursor"] = json!(end);
    result["has_more"] = json!(false);
    result["output_view"] = json!("delta");
    result["output_range_complete"] = json!(true);
    result["whole_log_returned"] =
        json!(requested == 0 && result["terminal"]["output_complete"] == true);
    result["compression"] = json!({"profile":"identity","raw_bytes":end-requested,"output_bytes":delta.len(),"saved_tokens":0,"full_output_available":true});
    result
        .as_object_mut()
        .context("invalid result object")?
        .remove("result_omitted");
    Ok(())
}
async fn view(g: &Gateway, p: &Principal, ids: &[String]) -> Result<View> {
    let rows = sqlx::query("SELECT j.id,j.device,j.state,json_extract(j.request,'$.owner') AS owner,json_extract(j.request,'$.tool') AS tool,json_extract(j.result,'$.terminal') AS saved_terminal,j.updated AS job_updated,COALESCE(r.value,a.value) AS progress FROM jobs j LEFT JOIN kv a ON a.kind='operation_progress' AND a.key=j.id AND a.expires>? LEFT JOIN kv r ON r.kind='receive_progress' AND r.key=j.id AND r.expires>? AND json_extract(j.request,'$.tool')='file_download' WHERE j.id IN (SELECT value FROM json_each(?))")
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
        let observed = if tool == "terminal_exec" {
            match crate::terminal_sync::observed(g, id).await? {
                Some(observed) => Some(observed),
                None => {
                    let saved: Option<String> = row.try_get("saved_terminal")?;
                    let updated: i64 = row.try_get("job_updated")?;
                    saved
                        .map(|raw| {
                            serde_json::from_str::<Value>(&raw).map(|terminal| {
                                json!({"terminal":terminal,
                            "stale":!(0..=45).contains(&(now()-updated))})
                            })
                        })
                        .transpose()?
                }
            }
        } else {
            None
        };
        if let Some(observed) = observed {
            let t = &observed["terminal"];
            stamp["terminal"] = json!({"state":t["state"],"exit_code":t["exit_code"],"output_complete":t["output_complete"],
                "output_truncated":t["output_truncated"],"output_error":t["output_error"],
                "output_cursor_end":observed["output_cursor_end"],"output_gap":observed["output_gap"],"stale":observed["stale"]});
            if state == "done" && terminal_pending(t, observed["stale"] != false) {
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
    let request_id = args.get("request_id").and_then(Value::as_str);
    let direct_single = args.get("operation_id").and_then(Value::as_str);
    let has_batch = args.get("operation_ids").is_some();
    ensure!(
        usize::from(request_id.is_some())
            + usize::from(direct_single.is_some())
            + usize::from(has_batch)
            == 1,
        "invalid_arguments: supply exactly one of request_id, operation_id or operation_ids"
    );
    let mut request_receipt = None;
    let resolved_request_operation = if let Some(request_id) = request_id {
        ensure!(
            request_id.len() == 36
                && request_id.starts_with("req_")
                && request_id[4..]
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid_arguments: invalid request_id"
        );
        let Some(mut receipt) = g.store.get::<Value>("request_receipt", request_id).await? else {
            return Ok(
                json!({"request_id":request_id,"state":"not_observed_or_expired","operation_id":null,
                "execution_state":"unknown","business_state":"not_evaluated","evidence_complete":false,
                "failure_boundary":"unknown","last_confirmed_stage":"receipt_lookup_only","stale":true,
                "retry_policy":"do_not_replay_until_reconciled","next_action":"check_original_transport_receipt",
                "user_action":"none","observed_at":now()}),
            );
        };
        ensure!(
            receipt["owner"].as_str() == Some(&p.owner) && p.owner == g.config.owner,
            "request_unavailable: request belongs to another owner"
        );
        let original_scope = receipt["tool"].as_str().and_then(tools::scope);
        ensure!(
            original_scope.map_or(receipt["state"] == "gateway_rejected", |scope| p
                .scopes
                .iter()
                .any(|s| s == scope)),
            "request_unavailable: original request scope is required"
        );
        let operation = receipt["operation_id"].as_str().map(str::to_owned);
        if let Some(object) = receipt.as_object_mut() {
            object.remove("owner");
            object.remove("fingerprint");
        }
        if operation.is_none() {
            receipt["pending"] = json!(receipt["state"] == "gateway_received");
            receipt["stale"] = json!(
                receipt["state"] == "gateway_received"
                    && now() - receipt["updated_at"].as_i64().unwrap_or(0) > 45
            );
            if receipt["state"] == "completed_at_gateway" {
                receipt["next_action"] = Value::Null;
                return Ok(receipt);
            }
            receipt["next_action"] = json!(if receipt["state"] == "gateway_rejected" {
                "follow_retry_policy; no remote operation exists"
            } else {
                "observe this request_id; no remote operation has been confirmed"
            });
            return Ok(receipt);
        }
        request_receipt = Some(receipt);
        operation
    } else {
        None
    };
    let single = resolved_request_operation.as_deref().or(direct_single);
    let ids: Vec<String> = if let Some(id) = single {
        vec![id.to_owned()]
    } else {
        serde_json::from_value(args["operation_ids"].clone())?
    };
    ensure!(
        args.get("terminal_cursor").is_none() || single.is_some(),
        "invalid_arguments: terminal_cursor is only valid for one operation_id"
    );
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
        // Backward-compatible shape with a short bounded wait. This reduces the
        // submit -> get -> get chain for older hosts that cannot send wait_ms.
        let deadline = Instant::now() + Duration::from_millis(1200);
        loop {
            let changed = g.observation_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut result = g.result(p, id).await?;
            if let Some(receipt) = &request_receipt {
                result["request_receipt"] = receipt.clone();
            }
            if !awaiting_result(&result) || Instant::now() >= deadline {
                return Ok(result);
            }
            tokio::select! {
                _ = changed => {},
                _ = tokio::time::sleep_until(deadline.min(Instant::now()+Duration::from_millis(250))) => {}
            }
        }
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
    let wait = files::number(args, "wait_ms", 1200, 0, 5000)?;
    let budget = files::number(args, "max_bytes", 65536, 4096, 131072)?;
    let started = Instant::now();
    let deadline = started + Duration::from_millis(wait as u64);
    let mut baseline = supplied.map(str::to_owned);
    let final_view = loop {
        // Subscribe before durable read, so a committed result cannot be missed.
        let changed = g.observation_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let current = view(g, p, &ids).await?;
        let previous = baseline.get_or_insert_with(|| current.cursor.clone());
        // With a cursor the caller requested the next change. Without one,
        // wait for the process result, not merely transport bookkeeping.
        if (supplied.is_some() && current.cursor != *previous)
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
        apply_terminal_cursor(&mut result, args)?;
        if args.get("terminal_cursor").is_some() {
            let at = result["receipt"]["observed_at"]
                .as_i64()
                .unwrap_or_else(now);
            result["receipt"] = crate::receipts::decision(&result, None, Some(id), at);
        }
        if let Some(receipt) = &request_receipt {
            result["request_receipt"] = receipt.clone();
        }
        result["observation"] = metadata.clone();
        if metadata["changed"] == false
            && args.get("terminal_cursor").is_none()
            && result.get("error").is_none()
        {
            // Preserve decisions and uncertainty while omitting unchanged payload.
            result = json!({"operation_id":id,"pending":result["pending"],"receipt":result["receipt"],
                "observation":metadata,"request_receipt":result["request_receipt"],
                "next_action":result["receipt"]["next_action"],"unchanged_payload_omitted":true});
        }
        if serde_json::to_vec(&result)?.len() > budget {
            result = omitted(id, &result);
            result["observation"] = metadata;
        }
        ensure!(
            serde_json::to_vec(&result)?.len() <= budget,
            "observation_response_budget: critical evidence exceeds requested budget; increase max_bytes"
        );
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
    let compact_output = json!({"operations":compact,"receipt_defaults":completed_receipt_defaults(),
        "receipt_semantics":"A row without receipt inherits receipt_defaults; operation_id remains its immutable lookup handle.",
        "pending_count":final_view.pending,"observation":output["observation"]});
    if serde_json::to_vec(&compact_output)?.len() <= budget {
        return Ok(compact_output);
    }
    // Finally reserve actual serialized placeholders and admit compact results
    // individually. This keeps large siblings from hiding independent small ones.
    let placeholders: Vec<Value> = ids
        .iter()
        .zip(
            compact_output["operations"]
                .as_array()
                .expect("batch results"),
        )
        .map(|(id, result)| omitted(id, result))
        .collect();
    let mut bounded = json!({"operations":placeholders,"receipt_defaults":completed_receipt_defaults(),
        "receipt_semantics":"Only complete rows without a receipt inherit defaults. result_omitted always means incomplete evidence.",
        "pending_count":final_view.pending,"observation":compact_output["observation"]});
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

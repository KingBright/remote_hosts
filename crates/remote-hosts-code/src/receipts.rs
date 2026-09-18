//! One decision receipt over the existing job/kv store. No second task database.
use crate::{
    auth::Principal,
    gateway::{Gateway, Job},
    hash, now, tools,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Sqlite, Transaction};

/// Durable evidence is not a seven-day cache. Removal requires explicit owner cleanup.
pub const RETAIN_UNTIL_EXPLICIT_CLEANUP: i64 = i64::MAX;
pub const RETENTION: Option<u64> = None;
pub fn valid_request_id(id: &str) -> bool {
    id.len() == 36
        && id.starts_with("req_")
        && id[4..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
pub fn valid_task_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 96
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
}

/// Process completion, output capture and business acceptance are separate facts.
/// Absence of evidence never becomes permission to replay an arbitrary command.
pub fn decision(
    value: &Value,
    request_id: Option<&str>,
    operation_id: Option<&str>,
    at: i64,
) -> Value {
    let observed = &value["terminal_observation"];
    let terminal = if observed["terminal"].is_object() {
        &observed["terminal"]
    } else {
        &value["terminal"]
    };
    let pending = value["pending"] == true;
    let error = value.get("error").is_some()
        || value.get("observation_error").is_some()
        || value["error_code"].is_string()
        || value["state"] == "gateway_rejected";
    let terminal_unobserved = value["terminal_id"].is_string() && !terminal.is_object();
    let stale =
        value["stale"] == true || value["progress_stale"] == true || observed["stale"] == true;
    let state = terminal["state"].as_str();
    let execution = match state {
        Some("running" | "starting") if stale => "unknown",
        Some("running" | "starting") => "running",
        Some("exited") => "exited",
        Some("cancelled" | "timed_out") if terminal["exit_code"].is_number() => "exited",
        Some("cancelled" | "timed_out" | "runtime_lost") => "unknown",
        Some("failed") if terminal["process_id"].is_number() => "unknown",
        Some("failed") => "not_started",
        _ if stale => "unknown",
        _ if error => value["execution_state"].as_str().unwrap_or("unknown"),
        _ if terminal_unobserved => "unknown",
        _ if pending => "not_started_or_unknown",
        _ => "completed",
    };
    let incomplete = value["result_omitted"] == true
        || value["output_truncated"] == true
        || value["truncated"] == true
        || terminal["output_truncated"] == true
        || terminal["output_error"].is_string()
        || observed["output_gap"] == true;
    let evidence_complete = !pending
        && !stale
        && !error
        && !incomplete
        && !terminal_unobserved
        && (!terminal.is_object()
            || (terminal["output_complete"] == true && terminal["exit_code"].is_number()));
    let next = if stale || execution == "unknown" {
        Some("observe_original_and_reconcile")
    } else if pending || matches!(execution, "running" | "not_started_or_unknown") {
        Some("observe_original")
    } else if incomplete || (terminal.is_object() && !evidence_complete) {
        Some("read_original_output")
    } else if error || terminal["exit_code"].as_i64().is_some_and(|n| n != 0) {
        Some("inspect_original_receipt")
    } else {
        None
    };
    let stage = if terminal["exit_code"].is_number() {
        "process_exited"
    } else if matches!(execution, "running") {
        "process_started"
    } else if value["progress_origin"] == "agent" {
        "device_reported_progress"
    } else if pending {
        if operation_id.is_some() {
            "gateway_has_operation"
        } else {
            "gateway_received_request"
        }
    } else {
        value["last_confirmed_stage"]
            .as_str()
            .unwrap_or("gateway_completed_request")
    };
    json!({"protocol":1,"request_id":request_id,"operation_id":operation_id,
    "failure_boundary":value.get("failure_boundary").cloned().unwrap_or_else(||if error{json!("unknown")}else{Value::Null}),"last_confirmed_stage":stage,
    "execution_state":execution,"business_state":"not_evaluated",
    "evidence_complete":evidence_complete,"stale":stale,"observed_at":at,
    "retry_policy":value["retry_policy"].as_str().unwrap_or(if pending || execution=="unknown" || execution=="running" {
        "do_not_replay_observe_original"
    } else {"do_not_replay_completed_operation"}),
    "next_action":next,"user_action":value["user_action"].as_str().unwrap_or("none"),
    "error_code":value["error_code"],"transport_complete":!pending,
    "process_exit_code":terminal["exit_code"],"output_complete":terminal["output_complete"],
    "process_outcome":match state {
        Some("cancelled")=>"cancelled",Some("timed_out")=>"timed_out",
        _=>match terminal["exit_code"].as_i64(){Some(0)=>"succeeded",Some(_)=>"failed",None=>"not_confirmed"}
    }})
}

/// Bind the request to its remote operation in the SAME transaction that creates
/// the job. A crash after commit can be recovered using request_id without replay.
pub(crate) async fn bind(
    tx: &mut Transaction<'_, Sqlite>,
    request_id: &str,
    job: &Job,
) -> Result<()> {
    let raw: String = sqlx::query_scalar(
        "SELECT value FROM kv WHERE kind='request_receipt' AND key=? AND expires>?",
    )
    .bind(request_id)
    .bind(now())
    .fetch_one(&mut **tx)
    .await?;
    let entry: Value = serde_json::from_str(&raw)?;
    ensure!(entry["owner"] == job.owner, "request_owner_conflict");
    ensure!(
        entry["operation_id"].is_null() || entry["operation_id"] == job.id,
        "request_operation_conflict"
    );
    sqlx::query("UPDATE kv SET value=json_set(value,'$.operation_id',?,'$.state','operation_created','$.last_confirmed_stage','operation_created','$.updated_at',?) WHERE kind='request_receipt' AND key=?")
        .bind(&job.id).bind(now()).bind(request_id).execute(&mut **tx).await?;
    if let Some(task) = entry["task_id"].as_str() {
        let old: Option<String> =
            sqlx::query_scalar("SELECT value FROM kv WHERE kind='task_operation' AND key=?")
                .bind(&job.id)
                .fetch_optional(&mut **tx)
                .await?;
        if let Some(old) = old {
            let old: Value = serde_json::from_str(&old)?;
            ensure!(
                old["owner"] == job.owner && old["task_id"] == task,
                "task_association_conflict"
            );
        } else {
            let link =
                json!({"owner":job.owner,"task_id":task,"operation_id":job.id,"created_at":now()});
            sqlx::query("INSERT INTO kv VALUES('task_operation',?,?,?)")
                .bind(&job.id)
                .bind(link.to_string())
                .bind(RETAIN_UNTIL_EXPLICIT_CLEANUP)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

fn storage_failure(request_id: &str, before_dispatch: bool) -> Value {
    let mut value = json!({"error":"receipt_storage_failed","error_code":"receipt_storage_failed",
        "request_id":request_id,"operation_id":null,"failure_boundary":"gateway_storage",
        "execution_state":if before_dispatch{"not_started"}else{"unknown"},
        "last_confirmed_stage":if before_dispatch{"gateway_received_request"}else{"dispatch_attempted"},
        "retry_policy":"do_not_replay_until_original_outcome_is_observed","user_action":"none"});
    value["receipt"] = decision(&value, Some(request_id), None, now());
    value["receipt"]["durable"] = json!(false);
    value["receipt"]["evidence_complete"] = json!(false);
    value
}

/// Entry point shared by MCP and authenticated in-process acceptance tests.
/// The transport, never the model's tool arguments, supplies request_id.
pub async fn invoke(
    g: &Gateway,
    p: &Principal,
    tool: &str,
    args: Value,
    request_id: &str,
) -> Result<Value> {
    ensure!(valid_request_id(request_id), "invalid_request_id");
    let task = if tool == "task_context" {
        None
    } else {
        args.get("task_id").and_then(Value::as_str)
    };
    ensure!(p.owner == g.config.owner, "unknown owner");
    let observed_request_id = if tool == "operation_get" {
        args.get("request_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    } else {
        None
    };
    let fingerprint = hash(serde_json::to_vec(&json!({"tool":tool,"arguments":args}))?);
    let entry = json!({"protocol":1,"request_id":request_id,"owner":p.owner,
        "tool":tool,"scope":tools::scope(tool),"task_id":task,"fingerprint":fingerprint,
        "state":"gateway_received","received_at":now(),"updated_at":now(),
        "operation_id":null,"last_confirmed_stage":"gateway_received_request","execution_state":"not_started_or_unknown"});
    let created = sqlx::query("INSERT OR IGNORE INTO kv VALUES('request_receipt',?,?,?)")
        .bind(request_id)
        .bind(entry.to_string())
        .bind(RETAIN_UNTIL_EXPLICIT_CLEANUP)
        .execute(&g.store.pool)
        .await;
    let Ok(created) = created else {
        return Ok(storage_failure(request_id, true));
    };
    if created.rows_affected() == 0 {
        let old: Value = g
            .store
            .get("request_receipt", request_id)
            .await?
            .context("request_unavailable")?;
        ensure!(
            old["owner"] == p.owner && old["fingerprint"] == fingerprint,
            "request_identity_conflict"
        );
        if let Some(id) = old["operation_id"].as_str() {
            let mut result = g.result(p, id).await?;
            result["request_id"] = json!(request_id);
            result["receipt"]["request_id"] = json!(request_id);
            return Ok(result);
        }
        let mut out = old;
        out.as_object_mut().unwrap().remove("owner");
        out.as_object_mut().unwrap().remove("fingerprint");
        out["next_action"] = if out["state"] == "completed_at_gateway" {
            Value::Null
        } else {
            json!("observe_original_request; remote start not confirmed; no replay")
        };
        return Ok(out);
    }
    let mut value = match if task.is_some_and(|id| !valid_task_id(id)) {
        Err(anyhow::anyhow!("invalid_arguments: task_id"))
    } else {
        g.dispatch_traced(p, tool, args, Some(request_id)).await
    } {
        Ok(value) => value,
        Err(error) => crate::diagnostics::error_with_request(
            tool,
            &error.to_string(),
            request_id,
            None,
            "gateway_dispatch",
        ),
    };
    if let Some(observed) = &observed_request_id {
        if !value["request_receipt"].is_object() {
            value["request_receipt"] = value.clone();
        }
        value["observed_request_id"] = json!(observed);
        if value["pending"] == true && value["operation_id"].is_null() {
            value["next_action"] = json!("observe observed_request_id; never resubmit execution");
        }
    }
    // If dispatch failed AFTER creating work, retain its atomic binding. Never
    // convert a result-collection or metadata error into a pre-execution rejection.
    let durable: Result<Option<Value>> = g.store.get("request_receipt", request_id).await;
    let mut saved = match durable {
        Ok(Some(saved)) => saved,
        // Never overwrite a committed binding with the pre-dispatch entry when
        // its read failed. The original row is the recovery authority.
        Ok(None) | Err(_) => {
            let operation = value["operation_id"].as_str().map(str::to_owned);
            value["request_id"] = json!(request_id);
            value["execution_state"] = json!("unknown");
            value["retry_policy"] = json!("do_not_replay_observe_original");
            let mut receipt = decision(&value, Some(request_id), operation.as_deref(), now());
            receipt["durable"] = json!(false);
            receipt["evidence_complete"] = json!(false);
            receipt["persistence_error"] = json!("request_binding_read_unconfirmed");
            receipt["next_action"] = json!("observe_original_request_id; do not replay");
            value["receipt"] = receipt;
            return Ok(value);
        }
    };
    let operation = saved["operation_id"]
        .as_str()
        .or_else(|| value["operation_id"].as_str())
        .map(str::to_owned);
    if value.get("error").is_some() && operation.is_some() {
        value["execution_state"] = json!("unknown");
        value["last_confirmed_stage"] = json!("operation_created");
        value["retry_policy"] = json!("do_not_replay_observe_original");
    }
    value["request_id"] = json!(request_id);
    if operation.is_some() {
        value["operation_id"] = json!(operation);
    }
    let receipt = decision(&value, Some(request_id), operation.as_deref(), now());
    saved["receipt"] = receipt.clone();
    if let Some(observed) = observed_request_id {
        saved["observed_request_id"] = json!(observed);
    }
    saved["operation_id"] = json!(operation);
    saved["updated_at"] = json!(now());
    saved["state"] = json!(if value.get("error").is_some() {
        if operation.is_some() {
            "outcome_unknown"
        } else {
            "gateway_rejected"
        }
    } else if operation.is_some() {
        "operation_observed"
    } else {
        "completed_at_gateway"
    });
    saved["execution_state"] = receipt["execution_state"].clone();
    saved["error_code"] = value["error_code"].clone();
    saved["retry_policy"] = receipt["retry_policy"].clone();
    saved["user_action"] = receipt["user_action"].clone();
    value["receipt"] = receipt;
    let persisted = g
        .store
        .put(
            "request_receipt",
            request_id,
            &saved,
            RETAIN_UNTIL_EXPLICIT_CLEANUP,
        )
        .await
        .is_ok();
    value["receipt"]["durable"] = json!(persisted);
    if !persisted {
        value["receipt"]["evidence_complete"] = json!(false);
        value["receipt"]["persistence_error"] = json!("final_receipt_save_failed");
        value["receipt"]["next_action"] = json!("observe_original_operation; do not replay");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exit_zero_with_incomplete_output_is_not_acceptance() {
        let v = json!({"terminal":{"state":"exited","exit_code":0,"output_complete":true,"output_truncated":true}});
        let r = decision(&v, None, Some("id"), 1);
        assert_eq!(r["execution_state"], "exited");
        assert_eq!(r["evidence_complete"], false);
        assert_eq!(r["business_state"], "not_evaluated");
        assert_eq!(r["next_action"], "read_original_output");
    }
    #[test]
    fn terminal_handle_without_observation_is_not_process_completion() {
        let r = decision(
            &json!({"terminal_id":"handle","state":"running"}),
            None,
            Some("handle"),
            1,
        );
        assert_eq!(r["execution_state"], "unknown");
        assert_eq!(r["evidence_complete"], false);
        assert_eq!(r["retry_policy"], "do_not_replay_observe_original");
    }
    #[test]
    fn stale_running_process_is_unknown_not_failed_or_replayable() {
        let v = json!({"terminal_observation":{"stale":true,"terminal":{"state":"running","exit_code":null}}});
        let r = decision(&v, None, Some("id"), 1);
        assert_eq!(r["execution_state"], "unknown");
        assert_eq!(r["stale"], true);
        assert_eq!(r["retry_policy"], "do_not_replay_observe_original");
    }
}

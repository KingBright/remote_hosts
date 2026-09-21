//! Pure observations do not create durable operations or copies of their facts.
//! No queue/cache/database is added. Errors keep a compact durable diagnostic;
//! full audit remains an operator policy. Authorization is checked on every read.
use super::*;

pub(super) async fn eligible(g: &Gateway, tool: &str, args: &Value) -> Result<bool> {
    if matches!(tool, "devices_list" | "fleet_status" | "task_context") {
        return Ok(true);
    }
    if tool != "operation_get" || args.get("request_id").is_some() {
        // A request can acquire an operation binding concurrently. Keep that
        // recovery path audited rather than guessing what it might resolve to.
        return Ok(false);
    }
    let ids: Vec<String> = match (args.get("operation_id"), args.get("operation_ids")) {
        (Some(Value::String(id)), None) => vec![id.clone()],
        (None, Some(Value::Array(ids))) if !ids.is_empty() && ids.len() <= 20 => ids
            .iter()
            .map(|id| {
                id.as_str()
                    .map(str::to_owned)
                    .context("invalid operation ID")
            })
            .collect::<Result<_>>()?,
        _ => return Ok(false),
    };
    if ids.iter().any(|id| uuid::Uuid::parse_str(id).is_err()) {
        return Ok(false);
    }
    // Job tool identity is immutable. Download observation can mint a bearer
    // link in transfers::decorate, so it is not a pure read (even in a batch).
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id IN (SELECT value FROM json_each(?)) AND json_extract(request,'$.tool')='file_download'")
        .bind(serde_json::to_string(&ids)?)
        .fetch_one(&g.store.pool).await?;
    Ok(writes == 0)
}

pub(super) async fn invoke(
    g: &Gateway,
    p: &Principal,
    tool: &str,
    args: Value,
    request_id: &str,
) -> Result<Value> {
    let fingerprint = hash(serde_json::to_vec(&json!({"tool":tool,"arguments":args}))?);
    // A lightweight trace must never shadow a previously reserved execution ID.
    if let Some(old) = g.store.get::<Value>("request_receipt", request_id).await? {
        ensure!(
            old["owner"] == p.owner && old["fingerprint"] == fingerprint,
            "request_identity_conflict"
        );
        let scope = tools::scope(tool).context("unknown tool")?;
        ensure!(p.scopes.iter().any(|s| s == scope), "insufficient_scope");
        return super::invoke_durable(g, p, tool, args, request_id).await;
    }
    let invalid_task = tool != "task_context"
        && args
            .get("task_id")
            .and_then(Value::as_str)
            .is_some_and(|id| !valid_task_id(id));
    let result = if invalid_task {
        Err(anyhow::anyhow!("invalid_arguments: task_id"))
    } else {
        // No execution request is created on these audited pure-read paths.
        // dispatch_traced still validates current owner, scope, device and input.
        g.dispatch_traced(p, tool, args, None).await
    };
    let mut value = match result {
        Ok(value) => value,
        Err(error) => return persist_failure(g, p, tool, request_id, &fingerprint, &error).await,
    };
    let operation = value["operation_id"].as_str().map(str::to_owned);
    let mut receipt = if tool == "operation_get" && value["receipt"].is_object() {
        // This projection already knows exit/output/stale facts, including in
        // an unchanged or byte-budget-limited response. Do not erase them.
        value["receipt"].clone()
    } else {
        decision(&value, Some(request_id), operation.as_deref(), now())
    };
    receipt["protocol"] = json!(2);
    receipt["request_id"] = json!(request_id);
    receipt["observed_at"] = json!(now());
    receipt["durable"] = json!(false);
    receipt["evidence_durable"] = json!(true);
    receipt["request_record_persisted"] = json!(false);
    receipt["durability_scope"] = json!("observed_facts_only_not_this_query");
    receipt["request_retention"] = json!("not_retained_use_original_operation");
    value["request_id"] = json!(request_id);
    value["receipt"] = receipt;
    Ok(value)
}

async fn persist_failure(
    g: &Gateway,
    p: &Principal,
    tool: &str,
    request_id: &str,
    fingerprint: &str,
    error: &anyhow::Error,
) -> Result<Value> {
    // A failed read is different from observing a saved nonzero process exit.
    // Keep the former once, not a start+finish pair; never overwrite a reservation.
    let mut value = crate::diagnostics::error_with_request(
        tool,
        &error.to_string(),
        request_id,
        None,
        "gateway_observation",
    );
    let entry = json!({"protocol":2,"request_id":request_id,"owner":p.owner,
        "tool":tool,"scope":tools::scope(tool),"fingerprint":fingerprint,
        "operation_id":null,"state":"gateway_rejected","received_at":now(),
        "updated_at":now(),"execution_state":"not_started",
        "receipt":value["receipt"],"error_code":value["error_code"],
        "observation_failure":true});
    let saved = sqlx::query("INSERT OR IGNORE INTO kv VALUES('request_receipt',?,?,?)")
        .bind(request_id)
        .bind(entry.to_string())
        .bind(RETAIN_UNTIL_EXPLICIT_CLEANUP)
        .execute(&g.store.pool)
        .await;
    let persisted = match saved {
        Ok(row) if row.rows_affected() == 1 => true,
        Ok(_) => {
            let old: Value = g
                .store
                .get("request_receipt", request_id)
                .await?
                .context("request_unavailable")?;
            ensure!(
                old["owner"] == p.owner && old["fingerprint"] == fingerprint,
                "request_identity_conflict"
            );
            true
        }
        Err(_) => false,
    };
    value["receipt"]["protocol"] = json!(2);
    value["receipt"]["durable"] = json!(persisted);
    value["receipt"]["evidence_durable"] = json!(false);
    value["receipt"]["request_record_persisted"] = json!(persisted);
    value["receipt"]["durability_scope"] = json!("observation_error_record_only");
    value["receipt"]["evidence_complete"] = json!(false);
    if !persisted {
        value["receipt"]["persistence_error"] = json!("observation_error_record_not_saved");
    }
    Ok(value)
}

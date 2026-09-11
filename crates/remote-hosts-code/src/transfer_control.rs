//! Persistent control intent for file jobs. Controls never replay arbitrary commands.
use crate::{
    auth::Principal,
    files,
    gateway::{Gateway, Job},
    hash, now, tools,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Control {
    pub revision: u64,
    pub cancel_requested: bool,
}

pub(crate) async fn device_job(g: &Gateway, headers: &HeaderMap, id: &str) -> Result<Job> {
    uuid::Uuid::parse_str(id)?;
    let device = g.device(headers).await?;
    let (request,): (String,) = sqlx::query_as("SELECT request FROM jobs WHERE id=? AND device=?")
        .bind(id)
        .bind(&device)
        .fetch_optional(&g.store.pool)
        .await?
        .context("transfer unavailable")?;
    let job: Job = serde_json::from_str(&request)?;
    ensure!(
        matches!(job.tool.as_str(), "file_upload" | "file_download") && job.owner == g.config.owner,
        "transfer unavailable"
    );
    let scope = tools::scope(&job.tool).context("unsupported transfer")?;
    ensure!(
        g.config
            .devices
            .iter()
            .any(|d| d.id == device && d.scopes.iter().any(|s| s == scope)),
        "transfer scope revoked"
    );
    Ok(job)
}

pub(crate) async fn control(g: &Gateway, id: &str) -> Result<Control> {
    Ok(g.store
        .get("transfer_control", id)
        .await?
        .unwrap_or_default())
}

pub(crate) fn routes(g: Gateway) -> Router {
    Router::new()
        .route("/device/transfer-control/{id}", get(read_control))
        .route("/device/transfer-status/{id}", post(report_paused))
        .with_state(g)
}
async fn read_control(
    State(g): State<Gateway>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if device_job(&g, &headers, &id).await.is_err() {
        return StatusCode::FORBIDDEN.into_response();
    }
    match control(&g, &id).await {
        Ok(c) => {
            Json(json!({"revision":c.revision,"cancel_requested":c.cancel_requested,"protocol":2}))
                .into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn report_paused(
    State(g): State<Gateway>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Response {
    if device_job(&g, &headers, &id).await.is_err() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let state = value["state"].as_str().unwrap_or("");
    let revision = value["transfer_revision"].as_u64();
    if !matches!(state, "paused" | "awaiting_source")
        || revision.is_none()
        || serde_json::to_vec(&value).map_or(true, |v| v.len() > 8192)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let result = async {
        let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
        let saved: Option<(String,)> = sqlx::query_as("SELECT value FROM kv WHERE kind='transfer_control' AND key=?")
            .bind(&id).fetch_optional(&mut *tx).await?;
        let c: Control = saved.map(|(s,)|serde_json::from_str(&s)).transpose()?.unwrap_or_default();
        if c.revision != revision.unwrap() {return Ok::<_,anyhow::Error>(false);}
        if c.cancel_requested {
            // Never overwrite an acknowledged cancellation with a late pause.
            sqlx::query("UPDATE jobs SET state='queued',result=NULL,updated=? WHERE id=? AND state<>'done'")
                .bind(now()).bind(&id).execute(&mut *tx).await?;
        } else {
            sqlx::query("UPDATE jobs SET state=?,result=?,updated=? WHERE id=? AND state IN ('dispatched','paused','awaiting_source')")
                .bind(state).bind(serde_json::to_string(&value)?).bind(now()).bind(&id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        g.observation_changed.notify_waiters();
        Ok(true)
    }.await;
    match result {
        Ok(accepted) => Json(json!({"accepted":accepted,"obsolete":!accepted})).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Both the caller and the registered device must retain access to the original job.
pub(crate) async fn apply(g: &Gateway, p: &Principal, name: &str, args: &Value) -> Result<Value> {
    let id = files::text(args, "operation_id")?;
    uuid::Uuid::parse_str(id)?;
    let key = files::text(args, "idempotency_key")?;
    ensure!(
        !key.is_empty() && key.len() <= 128,
        "invalid idempotency key"
    );
    let (text,): (String,) = sqlx::query_as("SELECT request FROM jobs WHERE id=?")
        .bind(id)
        .fetch_optional(&g.store.pool)
        .await?
        .context("transfer unavailable")?;
    let job: Job = serde_json::from_str(&text)?;
    let scope = tools::scope(&job.tool).context("transfer unavailable")?;
    ensure!(
        matches!(job.tool.as_str(), "file_upload" | "file_download")
            && job.owner == p.owner
            && p.owner == g.config.owner
            && p.scopes.iter().any(|s| s == scope)
            && g.config
                .devices
                .iter()
                .any(|d| d.id == job.device_id && d.scopes.iter().any(|s| s == scope)),
        "transfer unavailable"
    );
    let online = g.store.get::<Value>("online", &job.device_id).await?;
    ensure!(
        online
            .as_ref()
            .and_then(|v| v["runtime_features"]["names"].as_array())
            .is_some_and(|names| names.iter().any(|n| n == "durable_transfers_v2")),
        "device_feature_unavailable: upgrade the selected agent before controlling transfers"
    );
    let mut fingerprint_args = args.clone();
    let source = if let Some(file) = args.get("file") {
        ensure!(
            name == "transfer_resume" && job.tool == "file_upload",
            "only imports accept refreshed file authorization"
        );
        let url = files::text(file, "download_url")?;
        crate::transfers::source_url(url, &g.config.public_url)?;
        let file_id = files::text(file, "file_id")?;
        ensure!(
            !file_id.is_empty() && file_id.len() <= 1024,
            "invalid file identity"
        );
        ensure!(
            file["file_id"] == job.arguments["file"]["file_id"]
                || job.arguments["sha256"]
                    .as_str()
                    .is_some_and(crate::transfers::valid_hash),
            "refresh_identity_conflict: a different file reference requires an original expected sha256"
        );
        fingerprint_args["file"]["download_url"] = json!("refreshable_authorization");
        Some(json!({"download_url":url}))
    } else {
        None
    };
    let action_key = hash(format!("{}:{id}:{name}:{key}", p.owner));
    let fp = hash(serde_json::to_vec(&fingerprint_args)?);
    let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT value FROM kv WHERE kind='transfer_action' AND key=?")
            .bind(&action_key)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some((saved,)) = existing {
        let v: Value = serde_json::from_str(&saved)?;
        ensure!(v["fingerprint"] == fp, "idempotency_conflict");
        // Refresh only the original generation's authorization, not its effects.
        if let Some(source) = &source {
            let saved_revision = v["result"]["transfer_revision"].as_i64();
            sqlx::query("INSERT INTO kv(kind,key,value,expires) SELECT 'file_source',?,?,? WHERE EXISTS (SELECT 1 FROM jobs j JOIN kv c ON c.kind='transfer_control' AND c.key=j.id WHERE j.id=? AND j.state<>'done' AND json_extract(c.value,'$.revision')=? AND json_extract(c.value,'$.cancel_requested')=0) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
                .bind(id).bind(serde_json::to_string(source)?).bind(now()+crate::transfers::SOURCE_TTL).bind(id).bind(saved_revision)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        return Ok(v["result"].clone());
    }
    let (state,): (String,) = sqlx::query_as("SELECT state FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let old: Option<(String,)> =
        sqlx::query_as("SELECT value FROM kv WHERE kind='transfer_control' AND key=?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    let mut c: Control = old
        .map(|(s,)| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or_default();
    let output = if state == "done" {
        json!({"operation_id":id,"state":"already_finished","changed":false,"next_action":"operation_get"})
    } else {
        if name == "transfer_cancel" {
            c.cancel_requested = true;
            // Paused/queued work must reach the device to prove cleanup. Offline
            // devices remain cancellation_requested, never falsely cancelled.
            if matches!(state.as_str(), "paused" | "awaiting_source") {
                sqlx::query("UPDATE jobs SET state='queued',result=NULL,updated=? WHERE id=?")
                    .bind(now())
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        } else {
            ensure!(
                matches!(state.as_str(), "paused" | "awaiting_source"),
                "transfer_not_paused: observe original task before resuming"
            );
            ensure!(
                !c.cancel_requested,
                "cancellation_is_final; cannot resume a cancelled transfer"
            );
            c.revision = c.revision.checked_add(1).context("revision overflow")?;
            if let Some(source) = &source {
                sqlx::query("INSERT INTO kv VALUES('file_source',?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
                    .bind(id).bind(serde_json::to_string(source)?).bind(now()+crate::transfers::SOURCE_TTL).execute(&mut *tx).await?;
            }
            sqlx::query("UPDATE jobs SET state='queued',result=NULL,updated=? WHERE id=?")
                .bind(now())
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("INSERT INTO kv VALUES('transfer_control',?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,expires=excluded.expires")
            .bind(id).bind(serde_json::to_string(&c)?).bind(i64::MAX).execute(&mut *tx).await?;
        json!({"operation_id":id,"state":if c.cancel_requested {"cancellation_requested"} else {"resume_queued"},
            "transfer_revision":c.revision,"same_operation":true,"next_action":"operation_get","cleanup_complete":false})
    };
    sqlx::query("INSERT INTO kv VALUES('transfer_action',?,?,?)")
        .bind(action_key)
        .bind(json!({"fingerprint":fp,"result":output}).to_string())
        .bind(i64::MAX)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    g.wake_device(&job.device_id);
    g.observation_changed.notify_waiters();
    Ok(output)
}

/// Late results from a pre-resume attempt are acknowledged as obsolete, not
/// allowed to overwrite the current attempt. Old clients without revisions keep
/// their existing receipt behavior until explicitly controlled by a new client.
pub(crate) async fn obsolete(g: &Gateway, id: &str, value: &Value) -> Result<bool> {
    if let Some(revision) = value.get("transfer_revision").and_then(Value::as_u64) {
        return Ok(revision < control(g, id).await?.revision);
    }
    Ok(false)
}

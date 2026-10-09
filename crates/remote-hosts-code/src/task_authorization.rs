//! Persistent task grants add restrictions to existing authority; they never issue credentials.
use crate::{auth::Principal, files, gateway::Gateway, hash, now, receipts, tools};
use anyhow::{Context, Result, ensure};
use axum::{
    Form, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Sqlite, Transaction};

pub(crate) const PROTOCOL: u32 = 1;
const KIND: &str = "task_authorization";
const BINDING: &str = "operation_task_authorization";

#[derive(Clone, Serialize, Deserialize)]
struct Grant {
    owner: String,
    task_id: String,
    version: u64,
    enabled: bool,
    devices: Vec<String>,
    scopes: Vec<String>,
    updated_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub key: String,
    pub owner: String,
    pub task_id: String,
    pub version: u64,
    pub device_id: String,
    pub scope: String,
}
fn key(owner: &str, task: &str) -> String {
    hash(serde_json::to_vec(&(owner, task)).expect("string pair"))
}
fn check(grant: Option<&Grant>, binding: &Binding) -> Result<()> {
    let grant = grant.context("task_authorization_missing")?;
    ensure!(
        grant.owner == binding.owner && grant.task_id == binding.task_id,
        "task_authorization_owner_conflict"
    );
    ensure!(grant.enabled, "task_authorization_revoked");
    ensure!(
        grant.version == binding.version,
        "task_authorization_version_changed"
    );
    ensure!(
        grant.devices.contains(&binding.device_id),
        "task_device_denied"
    );
    ensure!(grant.scopes.contains(&binding.scope), "task_scope_denied");
    Ok(())
}
pub(crate) async fn requested(
    g: &Gateway,
    p: &Principal,
    tool: &str,
    args: &Value,
    device: &str,
) -> Result<Option<Binding>> {
    let task = args.get("task_id").and_then(Value::as_str);
    let version = args.get("authorization_version").and_then(Value::as_u64);
    let Some(task) = task else {
        ensure!(
            version.is_none(),
            "invalid_arguments: authorization_version requires task_id"
        );
        return Ok(None);
    };
    ensure!(receipts::valid_task_id(task), "invalid_arguments: task_id");
    let grant_key = key(&p.owner, task);
    let grant: Option<Grant> = g.store.get(KIND, &grant_key).await?;
    // Legacy task_id remains a correlation label. A version is never a self-grant.
    if grant.is_none() && version.is_none() {
        return Ok(None);
    }
    let version = version.context("task_authorization_version_required")?;
    let binding = Binding {
        key: grant_key,
        owner: p.owner.clone(),
        task_id: task.into(),
        version,
        device_id: device.into(),
        scope: tools::scope(tool).context("unknown tool")?.into(),
    };
    check(grant.as_ref(), &binding)?;
    Ok(Some(binding))
}
pub(crate) async fn check_in_transaction(
    tx: &mut Transaction<'_, Sqlite>,
    binding: &Binding,
) -> Result<()> {
    let raw: Option<String> =
        sqlx::query_scalar("SELECT value FROM kv WHERE kind=? AND key=? AND expires>?")
            .bind(KIND)
            .bind(&binding.key)
            .bind(now())
            .fetch_optional(&mut **tx)
            .await?;
    let grant = raw
        .as_deref()
        .map(serde_json::from_str::<Grant>)
        .transpose()?;
    check(grant.as_ref(), binding)
}
pub(crate) async fn bind(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    binding: &Binding,
) -> Result<()> {
    check_in_transaction(tx, binding).await?;
    sqlx::query("INSERT INTO kv VALUES(?,?,?,?)")
        .bind(BINDING)
        .bind(id)
        .bind(serde_json::to_string(binding)?)
        .bind(i64::MAX)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(crate) async fn view(g: &Gateway, p: &Principal, task: &str) -> Result<Value> {
    let grant: Option<Grant> = g.store.get(KIND, &key(&p.owner, task)).await?;
    Ok(grant.map_or(
        json!({"state":"correlation_only","grants_authority":false}),
        |v| {
            json!({"state":if v.enabled {"authorized"}else{"revoked"},"version":v.version,
            "task_id":v.task_id,"devices":v.devices,"scopes":v.scopes,"updated_at":v.updated_at,
            "authority":"intersection_with_current_account_device_and_local_policy",
            "terminal_authority":"local_user; not confined by code roots"})
        },
    ))
}
pub(crate) async fn blocked(g: &Gateway, p: &Principal, id: &str) -> Result<bool> {
    let Some(binding) = g.store.get::<Binding>(BINDING, id).await? else {
        return Ok(false);
    };
    ensure!(binding.owner == p.owner, "operation owner mismatch");
    let grant: Option<Grant> = g.store.get(KIND, &binding.key).await?;
    Ok(check(grant.as_ref(), &binding).is_err())
}
pub(crate) fn recoverable_tool(tool: &str) -> bool {
    matches!(
        tool,
        "workspace_open" | "code_apply_edits" | "files_sync" | "terminal_exec"
    )
}
pub(crate) fn authorization_rejection(code: &str) -> bool {
    matches!(
        code,
        "task_authorization_missing"
            | "task_authorization_version_required"
            | "task_authorization_version_changed"
            | "task_authorization_revoked"
            | "task_scope_denied"
            | "task_device_denied"
    )
}

pub(crate) async fn resume(g: &Gateway, p: &Principal, args: &Value) -> Result<Value> {
    ensure!(
        args.get("request_id").is_some() != args.get("operation_id").is_some(),
        "invalid_arguments: supply request_id or operation_id"
    );
    if args.get("operation_id").is_some() {
        resume_queued(g, p, args).await
    } else {
        crate::receipts::resume_authorized(g, p, args).await
    }
}

/// This affects only a queued job never handed to an Agent. Dispatched outcomes
/// remain observations, even when a grant is later revoked.
pub(crate) async fn resume_queued(g: &Gateway, p: &Principal, args: &Value) -> Result<Value> {
    let id = files::text(args, "operation_id")?;
    uuid::Uuid::parse_str(id).context("invalid_arguments: operation_id")?;
    let task = files::text(args, "task_id")?;
    let version = args["authorization_version"]
        .as_u64()
        .context("invalid_arguments: authorization_version")?;
    let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let (raw, state, result, dispatched, timing): (String, String, Option<String>, Option<i64>, Option<String>) =
        sqlx::query_as("SELECT j.request,j.state,j.result,t.dispatched_ms,t.id FROM jobs j LEFT JOIN operation_timing t ON t.id=j.id WHERE j.id=?")
            .bind(id).fetch_optional(&mut *tx).await?.context("operation_unavailable")?;
    let job: crate::gateway::Job = serde_json::from_str(&raw)?;
    ensure!(job.owner == p.owner, "operation owner mismatch");
    let raw: String = sqlx::query_scalar("SELECT value FROM kv WHERE kind=? AND key=?")
        .bind(BINDING)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .context("task_resume_not_eligible")?;
    let mut binding: Binding = serde_json::from_str(&raw)?;
    ensure!(
        binding.owner == p.owner && binding.task_id == task,
        "task_resume_owner_conflict"
    );
    // An identical completed resume is a read of the same operation.
    if binding.version == version {
        tx.rollback().await?;
        return g.result(p, id).await;
    }
    ensure!(
        state == "queued" && result.is_none() && dispatched.is_none() && timing.is_some(),
        "task_resume_not_eligible"
    );
    ensure!(
        version > binding.version,
        "task_authorization_change_required"
    );
    let scope = tools::scope(&job.tool).context("unknown tool")?;
    ensure!(p.scopes.iter().any(|v| v == scope), "insufficient_scope");
    let device = g
        .config
        .devices
        .iter()
        .find(|v| v.id == job.device_id)
        .context("unknown device")?;
    ensure!(
        device.scopes.iter().any(|v| v == scope),
        "device_scope_denied"
    );
    binding.version = version;
    check_in_transaction(&mut tx, &binding).await?;
    sqlx::query("UPDATE kv SET value=? WHERE kind=? AND key=?")
        .bind(serde_json::to_string(&binding)?)
        .bind(BINDING)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO kv VALUES('task_resume_audit',?,?,?) ON CONFLICT(kind,key) DO NOTHING",
    )
    .bind(hash(format!("{id}:{version}")))
    .bind(
        json!({"owner":p.owner,"task_id":task,"operation_id":id,"authorization_version":version,
            "not_dispatched":true,"resumed_at":now()})
        .to_string(),
    )
    .bind(i64::MAX)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    g.wake_device(&job.device_id);
    let mut value = g.result(p, id).await?;
    value["authorization_resume"] =
        json!({"operation_id":id,"version":version,"new_operation":false});
    Ok(value)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    task_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateForm {
    task_id: String,
    expected_version: u64,
    action: String,
    devices: String,
    scopes: String,
    csrf: String,
    password: String,
}
fn csrf(token: &str) -> String {
    hash(format!("task-authorization:{token}"))
}
pub(crate) fn routes(g: Gateway) -> Router {
    Router::new()
        .route("/status/task-authorization", get(page).post(update))
        .with_state(g)
}
async fn page(
    State(g): State<Gateway>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if !crate::gateway::status_session_valid(&g, &headers).await {
        return (
            StatusCode::UNAUTHORIZED,
            Html("<a href='/status'>Sign in as the Gateway owner</a>"),
        )
            .into_response();
    }
    let task = query.task_id.unwrap_or_default();
    if !task.is_empty() && !receipts::valid_task_id(&task) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let grant: Option<Grant> = match g.store.get(KIND, &key(&g.config.owner, &task)).await {
        Ok(v) => v,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let escape = crate::gateway::status_html_escape;
    let devices = grant
        .as_ref()
        .map(|v| v.devices.join(" "))
        .unwrap_or_default();
    let scopes = grant
        .as_ref()
        .map(|v| v.scopes.join(" "))
        .unwrap_or_else(|| "code:read code:write terminal:exec".into());
    let inventory = g
        .config
        .devices
        .iter()
        .map(|v| format!("<li>{}: {}</li>", escape(&v.name), escape(&v.id)))
        .collect::<String>();
    let token = crate::gateway::status_cookie(&headers).expect("validated session cookie");
    let html = format!(
        r#"<!doctype html><html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Task authorization</title><h1>Task authorization</h1>
<p>Approve a persistent task using the existing Gateway owner password. Each request still requires its current account and device scopes and local allow flags. Terminal access has local-user authority outside code roots. This does not approve platform prompts or administrator actions.</p>
<p>Updating or revoking increments the version. Already dispatched work may continue. Queued work requires an explicit resume using the new version. No operation runs from this form.</p>
<ul>{inventory}</ul><form method=post action=/status/task-authorization>
<input type=hidden name=csrf value="{csrf}"><input type=hidden name=expected_version value="{version}">
<label>Task ID <input name=task_id required value="{task}"></label><br>
<label>Device IDs, separated by spaces <input name=devices required value="{devices}"></label><br>
<label>Scopes, separated by spaces <input name=scopes required value="{scopes}"></label><br>
<label>Existing owner password <input type=password name=password required autocomplete=current-password></label><br>
<button name=action value=authorize>Authorize or update this task</button>
<button name=action value=revoke>Revoke this task</button></form></html>"#,
        csrf = csrf(token),
        version = grant.as_ref().map_or(0, |v| v.version),
        task = escape(&task),
        devices = escape(&devices),
        scopes = escape(&scopes)
    );
    let mut response = Html(html).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response
}
async fn update(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Form(form): Form<UpdateForm>,
) -> Response {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    let expected_origin = crate::validate_url(&g.config.public_url)
        .expect("validated Gateway URL")
        .origin()
        .ascii_serialization();
    if origin != Some(expected_origin.as_str())
        || !crate::gateway::status_session_valid(&g, &headers).await
        || !crate::gateway::status_cookie(&headers)
            .is_some_and(|token| crate::secret_eq(&csrf(token), &form.csrf))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !receipts::valid_task_id(&form.task_id)
        || form.devices.len() > 4096
        || form.scopes.len() > 256
        || form.password.is_empty()
        || form.password.len() > 1024
        || !matches!(form.action.as_str(), "authorize" | "revoke")
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !g.auth.allow_owner_password_attempt() {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    if !g.auth.owner_password_valid(form.password).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut devices: Vec<String> = form.devices.split_whitespace().map(str::to_owned).collect();
    let mut scopes: Vec<String> = form.scopes.split_whitespace().map(str::to_owned).collect();
    devices.sort();
    devices.dedup();
    scopes.sort();
    scopes.dedup();
    if form.action == "authorize"
        && (devices.is_empty()
            || devices.len() > 50
            || scopes.is_empty()
            || scopes
                .iter()
                .any(|s| !crate::SCOPES.split_whitespace().any(|v| v == s))
            || devices.iter().any(|id| {
                !g.config
                    .devices
                    .iter()
                    .any(|d| d.id == *id && scopes.iter().all(|s| d.scopes.contains(s)))
            }))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match save(
        &g,
        &form.task_id,
        form.expected_version,
        form.action == "authorize",
        devices,
        scopes,
    )
    .await
    {
        Ok(v) => axum::Json(
            json!({"task_id":v.task_id,"authorization_version":v.version,
            "enabled":v.enabled,"devices":v.devices,"scopes":v.scopes,"operations_started":false}),
        )
        .into_response(),
        Err(e) if e.to_string() == "task_authorization_missing" => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(e) if e.to_string() == "task_authorization_version_conflict" => {
            StatusCode::CONFLICT.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn save(
    g: &Gateway,
    task: &str,
    expected: u64,
    enabled: bool,
    devices: Vec<String>,
    scopes: Vec<String>,
) -> Result<Grant> {
    let k = key(&g.config.owner, task);
    let mut tx = g.store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let previous: Option<String> =
        sqlx::query_scalar("SELECT value FROM kv WHERE kind=? AND key=?")
            .bind(KIND)
            .bind(&k)
            .fetch_optional(&mut *tx)
            .await?;
    let previous = previous
        .as_deref()
        .map(serde_json::from_str::<Grant>)
        .transpose()?;
    ensure!(
        previous.as_ref().map_or(0, |v| v.version) == expected,
        "task_authorization_version_conflict"
    );
    // Revocation must remain possible after enrollment or scopes change. It
    // disables the existing grant, without accepting new devices or scopes.
    let (devices, scopes) = if enabled {
        (devices, scopes)
    } else {
        let old = previous.as_ref().context("task_authorization_missing")?;
        (old.devices.clone(), old.scopes.clone())
    };
    let grant = Grant {
        owner: g.config.owner.clone(),
        task_id: task.into(),
        version: expected
            .checked_add(1)
            .context("task_authorization_version_exhausted")?,
        enabled,
        devices,
        scopes,
        updated_at: now(),
    };
    sqlx::query(
        "INSERT INTO kv VALUES(?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value",
    )
    .bind(KIND)
    .bind(k)
    .bind(serde_json::to_string(&grant)?)
    .bind(i64::MAX)
    .execute(&mut *tx)
    .await?;
    // Retain prior versions so grant edits cannot erase the authorization history.
    sqlx::query("INSERT INTO kv VALUES('task_authorization_history',?,?,?)")
        .bind(hash(format!(
            "{}:{}",
            key(&grant.owner, task),
            grant.version
        )))
        .bind(serde_json::to_string(&grant)?)
        .bind(i64::MAX)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    g.observation_changed.notify_waiters();
    Ok(grant)
}

#[cfg(test)]
#[path = "task_authorization_tests.rs"]
mod tests;

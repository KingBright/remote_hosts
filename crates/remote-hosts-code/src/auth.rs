//! Single-owner OAuth authorization-code/PKCE service with hashed opaque tokens.
mod clients;
use crate::{GatewayConfig, SCOPES, hash, now, random, secret_eq, store::Store};
use axum::{
    Form, Json, Router,
    extract::{Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clients::{Client, redirect_uri_allowed};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type HttpResult<T> = Result<T, (StatusCode, Json<Value>)>;
/// Keep confidential-client state invisible to binaries that cannot authenticate it.
/// This is a namespace in the existing KV store, not a separate database.
pub fn oauth_state_kind(kind: &str, opaque: &str) -> String {
    if opaque.starts_with("rh2_") {
        format!("oauth2_{kind}")
    } else {
        kind.into()
    }
}
fn oauth_random(confidential: bool) -> String {
    if confidential {
        format!("rh2_{}", random())
    } else {
        random()
    }
}
fn invalid() -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":"invalid_request"})),
    )
}
fn internal(_: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"server_error"})),
    )
}
#[derive(Clone)]
pub struct Auth {
    pub config: Arc<GatewayConfig>,
    pub store: Store,
    rate: Arc<Mutex<HashMap<String, (i64, u32)>>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Principal {
    pub owner: String,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Grant {
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    resource: String,
    scope: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Access {
    principal: Principal,
    client_id: String,
    resource: String,
    family: String,
}
impl Auth {
    pub fn new(config: Arc<GatewayConfig>, store: Store) -> Self {
        Self {
            config,
            store,
            rate: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn limit(&self, key: &str, limit: u32) -> bool {
        let Ok(mut rate) = self.rate.lock() else {
            return false;
        };
        let entry = rate.entry(key.into()).or_insert((now(), 0));
        if now() - entry.0 >= 60 {
            *entry = (now(), 0)
        }
        entry.1 += 1;
        entry.1 <= limit
    }
    pub(crate) fn allow_owner_password_attempt(&self) -> bool {
        self.limit("password", 10)
    }
    pub(crate) async fn owner_password_valid(&self, password: String) -> bool {
        let encoded = self.config.password_hash.clone();
        tokio::task::spawn_blocking(move || {
            use argon2::{Argon2, PasswordHash, PasswordVerifier};
            PasswordHash::new(&encoded).ok().is_some_and(|h| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &h)
                    .is_ok()
            })
        })
        .await
        .unwrap_or(false)
    }
    pub fn routes(&self) -> Router {
        Router::new()
            .route(
                "/.well-known/oauth-protected-resource",
                get(resource_metadata),
            )
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(resource_metadata),
            )
            .route(
                "/.well-known/oauth-authorization-server",
                get(auth_metadata),
            )
            .route("/oauth/register", post(register))
            .route("/oauth/authorize", get(authorize))
            .route("/oauth/approve", post(approve))
            .route("/oauth/token", post(token))
            .route("/oauth/revoke", post(revoke))
            .with_state(self.clone())
    }
    pub async fn principal(&self, headers: &HeaderMap) -> Option<Principal> {
        let token = headers
            .get(header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?;
        let access: Access = self
            .store
            .get(&oauth_state_kind("access", token), &hash(token))
            .await
            .ok()??;
        if access.resource != format!("{}/mcp", self.config.public_url)
            || self
                .store
                .get::<bool>("revoked", &access.family)
                .await
                .ok()?
                .is_some()
        {
            return None;
        }
        if access.principal.owner != self.config.owner {
            return None;
        }
        Some(access.principal)
    }
    pub fn challenge(&self) -> Response {
        let mut r = (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        )
            .into_response();
        if let Ok(v) = format!(
            "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource\", scope=\"{}\"",
            self.config.public_url, SCOPES
        )
        .parse()
        {
            r.headers_mut().insert(header::WWW_AUTHENTICATE, v);
        }
        r
    }
}
pub async fn require_auth(State(auth): State<Auth>, mut req: Request, next: Next) -> Response {
    let Some(principal) = auth.principal(req.headers()).await else {
        return auth.challenge();
    };
    req.extensions_mut().insert(principal);
    next.run(req).await
}
async fn resource_metadata(State(a): State<Auth>) -> Json<Value> {
    Json(
        json!({"resource":format!("{}/mcp",a.config.public_url),"authorization_servers":[a.config.public_url],"scopes_supported":SCOPES.split_whitespace().collect::<Vec<_>>(),"bearer_methods_supported":["header"]}),
    )
}
async fn auth_metadata(State(a): State<Auth>) -> Json<Value> {
    let u = &a.config.public_url;
    Json(
        json!({"issuer":u,"authorization_endpoint":format!("{u}/oauth/authorize"),"token_endpoint":format!("{u}/oauth/token"),"registration_endpoint":format!("{u}/oauth/register"),"revocation_endpoint":format!("{u}/oauth/revoke"),"response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],"token_endpoint_auth_methods_supported":["none","client_secret_basic","client_secret_post"],"revocation_endpoint_auth_methods_supported":["none","client_secret_basic","client_secret_post"],"code_challenge_methods_supported":["S256"],"scopes_supported":SCOPES.split_whitespace().collect::<Vec<_>>(),"authorization_response_iss_parameter_supported":true}),
    )
}
async fn register(
    State(a): State<Auth>,
    Json(v): Json<Value>,
) -> HttpResult<(StatusCode, Json<Value>)> {
    if !a.limit("register", 20) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    let client = a.register_client(&v).await?;
    Ok((StatusCode::CREATED, Json(client)))
}
#[derive(Deserialize)]
struct Authorize {
    client_id: String,
    redirect_uri: String,
    response_type: String,
    code_challenge: String,
    code_challenge_method: String,
    state: String,
    resource: String,
    scope: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Pending {
    grant: Grant,
    state: String,
}
async fn authorize(State(a): State<Auth>, Query(q): Query<Authorize>) -> HttpResult<Response> {
    if !a.limit("authorize", 60) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    let c: Client = a
        .store
        .get(&oauth_state_kind("client", &q.client_id), &q.client_id)
        .await
        .map_err(internal)?
        .ok_or_else(invalid)?;
    let scope = q.scope.unwrap_or_else(|| SCOPES.into());
    if !c.redirect_uris.contains(&q.redirect_uri)
        || !redirect_uri_allowed(&a.config, &q.redirect_uri)
        || q.response_type != "code"
        || q.code_challenge_method != "S256"
        || q.code_challenge.len() != 43
        || !q
            .code_challenge
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
        || q.resource != format!("{}/mcp", a.config.public_url)
        || q.state.len() > 4096
        || scope.is_empty()
        || !scope
            .split_whitespace()
            .all(|s| SCOPES.split_whitespace().any(|x| x == s))
    {
        return Err(invalid());
    }
    let nonce = oauth_random(c.client_id.starts_with("rh2_"));
    let pending = Pending {
        grant: Grant {
            client_id: q.client_id,
            redirect_uri: q.redirect_uri,
            code_challenge: q.code_challenge,
            resource: q.resource,
            scope: scope.clone(),
        },
        state: q.state,
    };
    a.store
        .put(
            &oauth_state_kind("pending", &nonce),
            &hash(&nonce),
            &pending,
            now() + 600,
        )
        .await
        .map_err(internal)?;
    let html = format!(
        "<!doctype html><html lang=zh-CN><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Remote Hosts 授权</title><body><main><h1>连接你的 Remote Hosts</h1><p>客户端名称（由申请方提供，未经验证）：{}</p><p>客户端 ID：{}</p><p>授权回调：{}</p><p>允许该客户端访问已登记的个人电脑。终端权限可以在本机用户权限范围内执行任意命令，包括项目目录以外的操作。</p><p>本次权限：{}</p><form method=post action=/oauth/approve><input type=hidden name=nonce value='{}'><label>网关登录密码 <input type=password name=password required autocomplete=current-password></label><p><button type=submit>登录并授权</button></p></form></main></body></html>",
        html_escape(c.client_name.as_deref().unwrap_or("未提供")),
        html_escape(&c.client_id),
        html_escape(&pending.grant.redirect_uri),
        html_escape(&scope),
        nonce
    );
    let mut response = Html(html).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        format!("rh_oauth={nonce}; Secure; HttpOnly; SameSite=Lax; Path=/oauth; Max-Age=600")
            .parse()
            .map_err(internal)?,
    );
    Ok(response)
}
#[derive(Deserialize)]
struct Approval {
    nonce: String,
    password: String,
}
async fn approve(
    State(a): State<Auth>,
    headers: HeaderMap,
    Form(f): Form<Approval>,
) -> HttpResult<Response> {
    if !a.allow_owner_password_attempt() {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    if f.password.len() > 1024
        || !(f.nonce.len() == 64 || (f.nonce.starts_with("rh2_") && f.nonce.len() == 68))
    {
        return Err(invalid());
    }
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(';')
                .find_map(|s| s.trim().strip_prefix("rh_oauth="))
        });
    if cookie.is_none_or(|c| !secret_eq(c, &f.nonce)) {
        return Err(invalid());
    }
    let valid = a.owner_password_valid(f.password).await;
    if !valid {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid_login"})),
        ));
    }
    let pending: Pending = a
        .store
        .take(&oauth_state_kind("pending", &f.nonce), &hash(&f.nonce))
        .await
        .map_err(internal)?
        .ok_or_else(invalid)?;
    if !redirect_uri_allowed(&a.config, &pending.grant.redirect_uri) {
        return Err(invalid());
    }
    let code = oauth_random(pending.grant.client_id.starts_with("rh2_"));
    a.store
        .put(
            &oauth_state_kind("code", &code),
            &hash(&code),
            &pending.grant,
            now() + 120,
        )
        .await
        .map_err(internal)?;
    let mut redirect = reqwest::Url::parse(&pending.grant.redirect_uri).map_err(internal)?;
    redirect
        .query_pairs_mut()
        .append_pair("code", &code)
        .append_pair("state", &pending.state)
        .append_pair("iss", &a.config.public_url);
    let mut response = Redirect::to(redirect.as_str()).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        "rh_oauth=; Secure; HttpOnly; SameSite=Lax; Path=/oauth; Max-Age=0"
            .parse()
            .map_err(internal)?,
    );
    Ok(response)
}
#[derive(Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    code: Option<String>,
    code_verifier: Option<String>,
    redirect_uri: Option<String>,
    resource: String,
    refresh_token: Option<String>,
}
async fn token(State(a): State<Auth>, headers: HeaderMap, Form(f): Form<TokenRequest>) -> Response {
    client_response(token_inner(a, &headers, f).await)
}
fn client_response(result: HttpResult<Json<Value>>) -> Response {
    let mut response = result.into_response();
    if response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            axum::http::HeaderValue::from_static("Basic realm=\"remote-hosts-code\""),
        );
    }
    response
}
async fn token_inner(a: Auth, headers: &HeaderMap, f: TokenRequest) -> HttpResult<Json<Value>> {
    if !a.limit("token", 120) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    let client = a
        .authenticate_client(headers, f.client_id.as_deref(), f.client_secret.as_deref())
        .await?;
    let client_id = &client.client_id;
    if f.resource != format!("{}/mcp", a.config.public_url) {
        return Err(invalid());
    }
    let access = match f.grant_type.as_str() {
        "authorization_code" => {
            let code = f.code.ok_or_else(invalid)?;
            let verifier = f.code_verifier.ok_or_else(invalid)?;
            if !(43..=128).contains(&verifier.len())
                || !verifier
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
            {
                return Err(invalid());
            }
            let g: Grant = a
                .store
                .get(&oauth_state_kind("code", &code), &hash(&code))
                .await
                .map_err(internal)?
                .ok_or_else(invalid)?;
            if &g.client_id != client_id
                || Some(g.redirect_uri) != f.redirect_uri
                || g.resource != f.resource
                || !secret_eq(
                    &g.code_challenge,
                    &URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
                )
            {
                return Err(invalid());
            }
            // Invalid credentials, redirect or PKCE must not consume another
            // client's code. The final atomic take still enforces single use.
            a.store
                .take::<Grant>(&oauth_state_kind("code", &code), &hash(&code))
                .await
                .map_err(internal)?
                .ok_or_else(invalid)?;
            Access {
                principal: Principal {
                    owner: a.config.owner.clone(),
                    scopes: g.scope.split_whitespace().map(str::to_owned).collect(),
                },
                client_id: client_id.clone(),
                resource: f.resource,
                family: random(),
            }
        }
        "refresh_token" => {
            let refresh = f.refresh_token.ok_or_else(invalid)?;
            let key = hash(&refresh);
            let refresh_kind = oauth_state_kind("refresh", &refresh);
            let used_kind = oauth_state_kind("used_refresh", &refresh);
            let access: Option<Access> =
                a.store.get(&refresh_kind, &key).await.map_err(internal)?;
            if let Some(access) = &access
                && (&access.client_id != client_id
                    || access.resource != f.resource
                    || a.store
                        .get::<bool>("revoked", &access.family)
                        .await
                        .map_err(internal)?
                        .is_some())
            {
                return Err(invalid());
            }
            // Consume and record the replay marker in one transaction so two
            // concurrent refresh requests cannot miss each other's marker.
            let mut tx = a.store.pool.begin().await.map_err(internal)?;
            let row: Option<(String,)> = sqlx::query_as(
                "DELETE FROM kv WHERE kind=? AND key=? AND expires>? RETURNING value",
            )
            .bind(&refresh_kind)
            .bind(&key)
            .bind(now())
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal)?;
            let Some((encoded,)) = row else {
                tx.commit().await.map_err(internal)?;
                let used = match a
                    .store
                    .get::<UsedRefresh>("oauth_refresh_binding", &key)
                    .await
                    .map_err(internal)?
                {
                    Some(used) => Some(used),
                    None => a
                        .store
                        .get::<UsedRefresh>(&used_kind, &key)
                        .await
                        .map_err(internal)?,
                };
                if let Some(used) = used {
                    let family = match used {
                        UsedRefresh::Bound {
                            family,
                            client_id: owner,
                        } if &owner == client_id => Some(family),
                        // Records issued before multi-client authentication used a string.
                        UsedRefresh::Legacy(family)
                            if client.token_endpoint_auth_method == "none" =>
                        {
                            Some(family)
                        }
                        _ => None,
                    };
                    if let Some(family) = family {
                        a.store
                            .put("revoked", &family, &true, now() + 2592000)
                            .await
                            .map_err(internal)?;
                    }
                }
                return Err(invalid());
            };
            let access: Access = serde_json::from_str(&encoded).map_err(internal)?;
            if &access.client_id != client_id || access.resource != f.resource {
                tx.rollback().await.map_err(internal)?;
                return Err(invalid());
            }
            let used = UsedRefresh::Bound {
                family: access.family.clone(),
                client_id: client_id.clone(),
            };
            // Preserve the legacy string format for public-client rollback.
            // A separate binding row adds client ownership without changing it.
            for (kind, value) in [
                (
                    used_kind.as_str(),
                    serde_json::to_string(&access.family).map_err(internal)?,
                ),
                (
                    "oauth_refresh_binding",
                    serde_json::to_string(&used).map_err(internal)?,
                ),
            ] {
                sqlx::query("INSERT INTO kv (kind,key,value,expires) VALUES(?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value, expires=excluded.expires")
                    .bind(kind).bind(&key).bind(value).bind(now() + 2592000)
                    .execute(&mut *tx).await.map_err(internal)?;
            }
            tx.commit().await.map_err(internal)?;
            access
        }
        _ => return Err(invalid()),
    };
    let bearer = oauth_random(client_id.starts_with("rh2_"));
    let refresh = oauth_random(client_id.starts_with("rh2_"));
    a.store
        .put(
            &oauth_state_kind("access", &bearer),
            &hash(&bearer),
            &access,
            now() + 3600,
        )
        .await
        .map_err(internal)?;
    a.store
        .put(
            &oauth_state_kind("refresh", &refresh),
            &hash(&refresh),
            &access,
            now() + 2592000,
        )
        .await
        .map_err(internal)?;
    Ok(Json(
        json!({"access_token":bearer,"refresh_token":refresh,"token_type":"Bearer","expires_in":3600,"scope":access.principal.scopes.join(" "),"resource":access.resource}),
    ))
}
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum UsedRefresh {
    Bound { family: String, client_id: String },
    Legacy(String),
}
async fn revoke(
    State(a): State<Auth>,
    headers: HeaderMap,
    Form(v): Form<HashMap<String, String>>,
) -> Response {
    client_response(revoke_inner(a, &headers, v).await)
}
async fn revoke_inner(
    a: Auth,
    headers: &HeaderMap,
    v: HashMap<String, String>,
) -> HttpResult<Json<Value>> {
    if !a.limit("revoke", 120) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    let token = v.get("token").ok_or_else(invalid)?;
    let key = hash(token);
    let access: Option<Access> = match a
        .store
        .get(&oauth_state_kind("access", token), &key)
        .await
        .map_err(internal)?
    {
        Some(access) => Some(access),
        None => a
            .store
            .get(&oauth_state_kind("refresh", token), &key)
            .await
            .map_err(internal)?,
    };
    // Preserve token-possession revocation for legacy public clients. A
    // confidential client always has to authenticate with its registered method.
    let legacy_id =
        if !headers.contains_key(header::AUTHORIZATION) && !v.contains_key("client_secret") {
            access.as_ref().map(|access| access.client_id.as_str())
        } else {
            None
        };
    let id = v.get("client_id").map(String::as_str).or(legacy_id);
    if id.is_none()
        && !headers.contains_key(header::AUTHORIZATION)
        && !v.contains_key("client_secret")
    {
        return Ok(Json(json!({})));
    }
    let client = a
        .authenticate_client(headers, id, v.get("client_secret").map(String::as_str))
        .await?;
    if let Some(access) = access
        && access.client_id == client.client_id
    {
        a.store
            .put("revoked", &access.family, &true, now() + 2592000)
            .await
            .map_err(internal)?;
        for kind in ["access", "refresh"] {
            a.store
                .take::<Access>(&oauth_state_kind(kind, token), &key)
                .await
                .map_err(internal)?;
        }
    }
    Ok(Json(json!({})))
}
fn html_escape(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

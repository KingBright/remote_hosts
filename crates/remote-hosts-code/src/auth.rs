//! Single-owner OAuth authorization-code/PKCE service with hashed opaque tokens.
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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type HttpResult<T> = Result<T, (StatusCode, Json<Value>)>;
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
struct Client {
    client_id: String,
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: String,
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
        let access: Access = self.store.get("access", &hash(token)).await.ok()??;
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
        json!({"issuer":u,"authorization_endpoint":format!("{u}/oauth/authorize"),"token_endpoint":format!("{u}/oauth/token"),"registration_endpoint":format!("{u}/oauth/register"),"revocation_endpoint":format!("{u}/oauth/revoke"),"response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],"token_endpoint_auth_methods_supported":["none"],"code_challenge_methods_supported":["S256"],"scopes_supported":SCOPES.split_whitespace().collect::<Vec<_>>(),"authorization_response_iss_parameter_supported":true}),
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
    let uris: Vec<String> =
        serde_json::from_value(v.get("redirect_uris").cloned().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    if uris.is_empty()
        || uris.len() > 5
        || !uris.iter().all(|u| a.config.redirect_uris.contains(u))
        || v.get("token_endpoint_auth_method")
            .and_then(Value::as_str)
            .is_some_and(|s| s != "none")
    {
        return Err(invalid());
    }
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='client'")
        .fetch_one(&a.store.pool)
        .await
        .map_err(internal)?;
    if count.0 >= 1000 {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"client_capacity"})),
        ));
    }
    let c = Client {
        client_id: random(),
        redirect_uris: uris,
        token_endpoint_auth_method: "none".into(),
    };
    a.store
        .put("client", &c.client_id, &c, i64::MAX)
        .await
        .map_err(internal)?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"client_id":c.client_id,"redirect_uris":c.redirect_uris,"token_endpoint_auth_method":"none","grant_types":["authorization_code","refresh_token"],"response_types":["code"]}),
        ),
    ))
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
        .get("client", &q.client_id)
        .await
        .map_err(internal)?
        .ok_or_else(invalid)?;
    let scope = q.scope.unwrap_or_else(|| SCOPES.into());
    if !c.redirect_uris.contains(&q.redirect_uri)
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
    let nonce = random();
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
        .put("pending", &hash(&nonce), &pending, now() + 600)
        .await
        .map_err(internal)?;
    let html = format!(
        "<!doctype html><html lang=zh-CN><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Remote Hosts 授权</title><body><main><h1>连接你的 Remote Hosts</h1><p>授权 ChatGPT 访问已登记的个人电脑。终端权限可以在本机用户权限范围内执行任意命令，包括项目目录以外的操作。</p><p>本次权限：{}</p><form method=post action=/oauth/approve><input type=hidden name=nonce value='{}'><label>网关登录密码 <input type=password name=password required autocomplete=current-password></label><p><button type=submit>登录并授权</button></p></form></main></body></html>",
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
    if !a.limit("password", 10) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
    if f.password.len() > 1024 || f.nonce.len() != 64 {
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
    let encoded = a.config.password_hash.clone();
    let valid = tokio::task::spawn_blocking(move || {
        use argon2::{Argon2, PasswordHash, PasswordVerifier};
        PasswordHash::new(&encoded).ok().is_some_and(|h| {
            Argon2::default()
                .verify_password(f.password.as_bytes(), &h)
                .is_ok()
        })
    })
    .await
    .map_err(internal)?;
    if !valid {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid_login"})),
        ));
    }
    let pending: Pending = a
        .store
        .take("pending", &hash(&f.nonce))
        .await
        .map_err(internal)?
        .ok_or_else(invalid)?;
    let code = random();
    a.store
        .put("code", &hash(&code), &pending.grant, now() + 120)
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
    client_id: String,
    code: Option<String>,
    code_verifier: Option<String>,
    redirect_uri: Option<String>,
    resource: String,
    refresh_token: Option<String>,
}
async fn token(State(a): State<Auth>, Form(f): Form<TokenRequest>) -> HttpResult<Json<Value>> {
    if !a.limit("token", 120) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        ));
    }
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
                .take("code", &hash(code))
                .await
                .map_err(internal)?
                .ok_or_else(invalid)?;
            if g.client_id != f.client_id
                || Some(g.redirect_uri) != f.redirect_uri
                || g.resource != f.resource
                || !secret_eq(
                    &g.code_challenge,
                    &URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
                )
            {
                return Err(invalid());
            }
            Access {
                principal: Principal {
                    owner: a.config.owner.clone(),
                    scopes: g.scope.split_whitespace().map(str::to_owned).collect(),
                },
                client_id: f.client_id,
                resource: f.resource,
                family: random(),
            }
        }
        "refresh_token" => {
            let key = hash(f.refresh_token.ok_or_else(invalid)?);
            let Some(access) = a
                .store
                .take::<Access>("refresh", &key)
                .await
                .map_err(internal)?
            else {
                if let Some(family) = a
                    .store
                    .get::<String>("used_refresh", &key)
                    .await
                    .map_err(internal)?
                {
                    a.store
                        .put("revoked", &family, &true, now() + 2592000)
                        .await
                        .map_err(internal)?;
                }
                return Err(invalid());
            };
            if access.client_id != f.client_id
                || access.resource != f.resource
                || a.store
                    .get::<bool>("revoked", &access.family)
                    .await
                    .map_err(internal)?
                    .is_some()
            {
                return Err(invalid());
            }
            a.store
                .put("used_refresh", &key, &access.family, now() + 2592000)
                .await
                .map_err(internal)?;
            access
        }
        _ => return Err(invalid()),
    };
    let bearer = random();
    let refresh = random();
    a.store
        .put("access", &hash(&bearer), &access, now() + 3600)
        .await
        .map_err(internal)?;
    a.store
        .put("refresh", &hash(&refresh), &access, now() + 2592000)
        .await
        .map_err(internal)?;
    Ok(Json(
        json!({"access_token":bearer,"refresh_token":refresh,"token_type":"Bearer","expires_in":3600,"scope":access.principal.scopes.join(" "),"resource":access.resource}),
    ))
}
async fn revoke(
    State(a): State<Auth>,
    Form(v): Form<HashMap<String, String>>,
) -> HttpResult<Json<Value>> {
    if let Some(t) = v.get("token") {
        for kind in ["access", "refresh"] {
            if let Some(access) = a
                .store
                .take::<Access>(kind, &hash(t))
                .await
                .map_err(internal)?
            {
                a.store
                    .put("revoked", &access.family, &true, now() + 2592000)
                    .await
                    .map_err(internal)?;
            }
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

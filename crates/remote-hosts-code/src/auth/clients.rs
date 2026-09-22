//! OAuth client registration and authentication shared by DCR and the local CLI.
use super::{Auth, HttpResult, internal, invalid, oauth_random, oauth_state_kind};
use crate::{GatewayConfig, hash, now, random, secret_eq};
use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Client {
    pub(super) client_id: String,
    pub(super) redirect_uris: Vec<String>,
    pub(super) token_endpoint_auth_method: String,
    // Defaults preserve already registered public clients and existing tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) client_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) client_secret_hash: Option<String>,
}
#[derive(Deserialize)]
struct Registration {
    redirect_uris: Vec<String>,
    client_name: Option<String>,
    token_endpoint_auth_method: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
}
/// Store every concrete callback on registration and require it exactly thereafter.
/// Native clients use literal loopback listeners; Spark uses one bounded Google
/// Account Linking family. Neither rule permits wildcard hosts or redirects.
pub(super) fn redirect_uri_allowed(config: &GatewayConfig, candidate: &str) -> bool {
    if config.redirect_uris.iter().any(|uri| uri == candidate)
        || crate::native_oauth_callback(candidate)
    {
        return true;
    }
    let Ok(uri) = reqwest::Url::parse(candidate) else {
        return false;
    };
    let Some(suffix) = uri.path().strip_prefix("/r/user_bound_custom-mcp-") else {
        return false;
    };
    uri.scheme() == "https"
        && uri.host_str() == Some("oauth-redirect.googleusercontent.com")
        && uri.port().is_none()
        && uri.username().is_empty()
        && uri.password().is_none()
        && uri.query().is_none()
        && uri.fragment().is_none()
        && !suffix.is_empty()
        && suffix.len() <= 512
        && !suffix.contains('/')
        && suffix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.~".contains(&b))
}
fn invalid_client() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error":"invalid_client"})),
    )
}
impl Auth {
    /// Register one client. Returned secrets are disclosed once, never stored in plaintext.
    /// This does not grant access; owner login and S256 PKCE remain mandatory.
    pub async fn register_client(&self, value: &Value) -> HttpResult<Value> {
        let r: Registration = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        let method = r
            .token_endpoint_auth_method
            .as_deref()
            .unwrap_or("client_secret_basic");
        if r.redirect_uris.is_empty()
            || r.redirect_uris.len() > 5
            || !r
                .redirect_uris
                .iter()
                .all(|uri| redirect_uri_allowed(&self.config, uri))
            || !["none", "client_secret_basic", "client_secret_post"].contains(&method)
            || r.client_name
                .as_ref()
                .is_some_and(|n| n.is_empty() || n.len() > 200 || n.chars().any(char::is_control))
            || r.grant_types.as_ref().is_some_and(|g| {
                g.is_empty()
                    || g.len() > 2
                    || !g.iter().any(|v| v == "authorization_code")
                    || g.iter()
                        .any(|v| v != "authorization_code" && v != "refresh_token")
            })
            || r.response_types
                .as_ref()
                .is_some_and(|r| r.len() != 1 || r[0] != "code")
        {
            return Err(invalid());
        }
        let secret = (method != "none").then(random);
        let client = Client {
            client_id: oauth_random(method != "none"),
            redirect_uris: r.redirect_uris,
            token_endpoint_auth_method: method.into(),
            client_name: r.client_name,
            client_secret_hash: secret.as_ref().map(hash),
        };
        // The capacity check and insertion are one SQLite write statement.
        let inserted = sqlx::query("INSERT INTO kv (kind,key,value,expires) SELECT ?,?,?,? WHERE (SELECT COUNT(*) FROM kv WHERE kind IN ('client','oauth2_client')) < 1000")
            .bind(oauth_state_kind("client", &client.client_id))
            .bind(&client.client_id)
            .bind(serde_json::to_string(&client).map_err(internal)?)
            .bind(i64::MAX)
            .execute(&self.store.pool).await.map_err(internal)?;
        if inserted.rows_affected() != 1 {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error":"client_capacity"})),
            ));
        }
        let mut result = json!({
            "client_id":client.client_id,
            "client_id_issued_at":now(),
            "redirect_uris":client.redirect_uris,
            "token_endpoint_auth_method":client.token_endpoint_auth_method,
            "grant_types":["authorization_code","refresh_token"],
            "response_types":["code"]
        });
        if let Some(name) = client.client_name {
            result["client_name"] = json!(name);
        }
        if let Some(secret) = secret {
            result["client_secret"] = json!(secret);
            result["client_secret_expires_at"] = json!(0);
        }
        Ok(result)
    }
    pub(super) async fn authenticate_client(
        &self,
        headers: &HeaderMap,
        body_id: Option<&str>,
        body_secret: Option<&str>,
    ) -> HttpResult<Client> {
        if headers.get_all(header::AUTHORIZATION).iter().count() > 1
            || body_id.is_some_and(|v| v.is_empty() || v.len() > 512)
            || body_secret.is_some_and(|v| v.len() > 4096)
        {
            return Err(invalid_client());
        }
        let (id, secret, method) = if let Some(value) = headers.get(header::AUTHORIZATION) {
            let value = value.to_str().map_err(|_| invalid_client())?;
            let (scheme, encoded) = value.split_once(' ').ok_or_else(invalid_client)?;
            if !scheme.eq_ignore_ascii_case("Basic")
                || encoded.len() > 8192
                || body_secret.is_some()
            {
                return Err(invalid_client());
            }
            let decoded = STANDARD.decode(encoded).map_err(|_| invalid_client())?;
            let decoded = String::from_utf8(decoded).map_err(|_| invalid_client())?;
            let (id, secret) = decoded.split_once(':').ok_or_else(invalid_client)?;
            let id = decode_component(id).ok_or_else(invalid_client)?;
            let secret = decode_component(secret).ok_or_else(invalid_client)?;
            if body_id.is_some_and(|body| body != id) {
                return Err(invalid_client());
            }
            (id, Some(secret), "client_secret_basic")
        } else {
            let id = body_id.ok_or_else(invalid_client)?.to_owned();
            let method = if body_secret.is_some() {
                "client_secret_post"
            } else {
                "none"
            };
            (id, body_secret.map(str::to_owned), method)
        };
        if id.is_empty() || id.len() > 512 || secret.as_ref().is_some_and(|v| v.len() > 4096) {
            return Err(invalid_client());
        }
        let client: Client = self
            .store
            .get(&oauth_state_kind("client", &id), &id)
            .await
            .map_err(internal)?
            .ok_or_else(invalid_client)?;
        if client.client_id != id || client.token_endpoint_auth_method != method {
            return Err(invalid_client());
        }
        match method {
            "none" if client.client_secret_hash.is_none() && secret.is_none() => {}
            "client_secret_basic" | "client_secret_post" => {
                let expected = client
                    .client_secret_hash
                    .as_deref()
                    .ok_or_else(invalid_client)?;
                let secret = secret
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(invalid_client)?;
                if !secret_eq(expected, &hash(secret)) {
                    return Err(invalid_client());
                }
            }
            _ => return Err(invalid_client()),
        }
        Ok(client)
    }
}
// RFC 6749 section 2.3.1 form-encodes both components before HTTP Basic.
fn decode_component(value: &str) -> Option<String> {
    if value.contains('&')
        || value.as_bytes().iter().enumerate().any(|(i, b)| {
            *b == b'%'
                && (i + 2 >= value.len()
                    || !value.as_bytes()[i + 1].is_ascii_hexdigit()
                    || !value.as_bytes()[i + 2].is_ascii_hexdigit())
        })
    {
        return None;
    }
    let encoded = format!("v={value}");
    Some(
        url::form_urlencoded::parse(encoded.as_bytes())
            .next()?
            .1
            .into_owned(),
    )
}

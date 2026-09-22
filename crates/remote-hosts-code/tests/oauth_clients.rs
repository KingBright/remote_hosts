//! In-process HTTP conformance tests, NOT evidence of a live Gemini account connection.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use remote_hosts_code::{GatewayConfig, gateway::Gateway, hash, random, write_private};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const ORIGIN: &str = "https://mcp.example.com:8443";
const RESOURCE: &str = "https://mcp.example.com:8443/mcp";
const GPT: &str = "https://chatgpt.com/connector_platform_oauth_redirect";
// Synthetic, deliberately not a real user's Google callback.
const SPARK: &str =
    "https://oauth-redirect.googleusercontent.com/r/user_bound_custom-mcp-fixture-gemini";
struct Fixture {
    _dir: tempfile::TempDir,
    g: Gateway,
    router: Router,
    password: String,
}
impl Fixture {
    async fn new() -> Self {
        use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
        let dir = tempfile::tempdir().unwrap();
        let password = random();
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let g = Gateway::new(GatewayConfig {
            public_url: ORIGIN.into(),
            bind: "127.0.0.1:0".into(),
            state_dir: dir.path().join("state"),
            owner: "fixture-owner".into(),
            password_hash,
            devices: vec![],
            // Spark DCR callbacks are accepted through the bounded Google
            // Account Linking callback family, so they need not be prelisted.
            redirect_uris: vec![GPT.into()],
            allowed_origins: vec![
                "https://chatgpt.com".into(),
                "https://gemini.google.com".into(),
            ],
        })
        .await
        .unwrap();
        let router = g.router().unwrap();
        Self {
            _dir: dir,
            g,
            router,
            password,
        }
    }
    async fn request(
        &self,
        method: &str,
        path: &str,
        value: Value,
        form: bool,
        headers: &[(&str, String)],
    ) -> (StatusCode, HeaderMap, Value) {
        let body = if form {
            url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(
                    value
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str().unwrap())),
                )
                .finish()
        } else {
            value.to_string()
        };
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "mcp.example.com:8443")
            .header(
                "content-type",
                if form {
                    "application/x-www-form-urlencoded"
                } else {
                    "application/json"
                },
            )
            .header("accept", "application/json, text/event-stream");
        for (name, value) in headers {
            req = req.header(*name, value);
        }
        let response = self
            .router
            .clone()
            .oneshot(req.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
        (status, headers, value)
    }
    async fn register(&self, method: &str, callback: &str) -> Value {
        let (status, _, client) = self
            .request(
                "POST",
                "/oauth/register",
                json!({
                    "redirect_uris":[callback],"client_name":"Fixture client",
                    "token_endpoint_auth_method":method
                }),
                false,
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{client}");
        client
    }
    async fn code(&self, client: &Value) -> Value {
        let verifier = random();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let callback = client["redirect_uris"][0].as_str().unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("client_id", client["client_id"].as_str().unwrap()),
                ("redirect_uri", callback),
                ("response_type", "code"),
                ("code_challenge", challenge.as_str()),
                ("code_challenge_method", "S256"),
                ("resource", RESOURCE),
                ("state", "fixture-state"),
                ("scope", "code:read"),
            ])
            .finish();
        let (status, headers, _) = self
            .request(
                "GET",
                &format!("/oauth/authorize?{query}"),
                json!({}),
                false,
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        if callback.starts_with("http://") || callback.contains("googleusercontent.com/") {
            let origin = url::Url::parse(callback)
                .unwrap()
                .origin()
                .ascii_serialization();
            let csp = headers["content-security-policy"].to_str().unwrap();
            assert!(csp.contains(&origin));
            assert!(!csp.contains('*'));
        }
        let cookie = headers["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let nonce = cookie.strip_prefix("rh_oauth=").unwrap();
        let (status, headers, _) = self
            .request(
                "POST",
                "/oauth/approve",
                json!({"nonce":nonce,"password":self.password}),
                true,
                &[("cookie", cookie.into()), ("origin", ORIGIN.into())],
            )
            .await;
        assert_eq!(status, StatusCode::SEE_OTHER);
        if callback.starts_with("http://") || callback.contains("googleusercontent.com/") {
            let origin = url::Url::parse(callback)
                .unwrap()
                .origin()
                .ascii_serialization();
            assert!(
                headers["content-security-policy"]
                    .to_str()
                    .unwrap()
                    .contains(&origin)
            );
        }
        let location = url::Url::parse(headers["location"].to_str().unwrap()).unwrap();
        let query: std::collections::HashMap<_, _> = location.query_pairs().into_owned().collect();
        assert_eq!(query["iss"], ORIGIN);
        assert_eq!(query["state"], "fixture-state");
        json!({"grant_type":"authorization_code","client_id":client["client_id"],"code":query["code"],
            "code_verifier":verifier,"redirect_uri":callback,"resource":RESOURCE})
    }
    async fn authenticated(
        &self,
        client: &Value,
        path: &str,
        mut form: Value,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut headers = Vec::new();
        form["client_id"] = client["client_id"].clone();
        match client["token_endpoint_auth_method"].as_str().unwrap() {
            "client_secret_basic" => headers.push(("authorization", basic(client))),
            "client_secret_post" => form["client_secret"] = client["client_secret"].clone(),
            "none" => {}
            _ => panic!("invalid fixture method"),
        }
        self.request("POST", path, form, true, &headers).await
    }
    async fn login(&self, client: &Value) -> Value {
        let code = self.code(client).await;
        let (status, _, token) = self.authenticated(client, "/oauth/token", code).await;
        assert_eq!(status, StatusCode::OK, "{token}");
        token
    }
    async fn mcp(
        &self,
        token: &Value,
        method: &str,
        params: Value,
    ) -> (StatusCode, HeaderMap, Value) {
        self.request(
            "POST",
            "/mcp",
            json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
            false,
            &[
                (
                    "authorization",
                    format!("Bearer {}", token["access_token"].as_str().unwrap()),
                ),
                ("origin", "https://gemini.google.com".into()),
            ],
        )
        .await
    }
    async fn refresh(&self, client: &Value, token: &Value) -> (StatusCode, HeaderMap, Value) {
        self.authenticated(client,"/oauth/token",json!({"grant_type":"refresh_token","refresh_token":token["refresh_token"],"resource":RESOURCE})).await
    }
}
fn basic(client: &Value) -> String {
    format!(
        "Basic {}",
        STANDARD.encode(format!(
            "{}:{}",
            client["client_id"].as_str().unwrap(),
            client["client_secret"].as_str().unwrap()
        ))
    )
}
fn invalid_client(status: StatusCode, headers: &HeaderMap, body: &Value) {
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], "invalid_client");
    assert!(headers.contains_key("www-authenticate"));
}

#[tokio::test]
async fn status_page_uses_owner_password_cookie_and_gateway_truth() {
    let f = Fixture::new().await;
    let (status, headers, login) = f.request("GET", "/status", json!({}), false, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(login.as_str().unwrap().contains("Gateway password"));
    assert!(
        headers["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("style-src 'unsafe-inline'")
    );

    let (status, headers, _) = f
        .request(
            "POST",
            "/status/login",
            json!({"password":f.password}),
            true,
            &[("origin", ORIGIN.into())],
        )
        .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(headers["location"], "/status");
    let cookie = headers["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    assert!(cookie.starts_with("rh_status="));

    let (status, _, page) = f
        .request("GET", "/status", json!({}), false, &[("cookie", cookie)])
        .await;
    assert_eq!(status, StatusCode::OK);
    let page = page.as_str().unwrap();
    assert!(page.contains("Authoritative Gateway state"));
    assert!(page.contains("Heartbeat is not counted as business progress"));
    assert!(!page.contains("device_token"));
    assert!(!page.contains("command_preview"));
}

#[tokio::test]
async fn discovery_and_dcr_secret_defaults() {
    let f = Fixture::new().await;
    let (status, headers, _) = f
        .request(
            "POST",
            "/mcp",
            json!({}),
            false,
            &[("origin", "https://gemini.google.com".into())],
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        headers["www-authenticate"]
            .to_str()
            .unwrap()
            .contains("oauth-protected-resource")
    );
    let (_, _, metadata) = f
        .request(
            "GET",
            "/.well-known/oauth-authorization-server",
            json!({}),
            false,
            &[],
        )
        .await;
    assert_eq!(
        metadata["token_endpoint_auth_methods_supported"],
        json!(["none", "client_secret_basic", "client_secret_post"])
    );
    let (status, _, client) = f
        .request(
            "POST",
            "/oauth/register",
            json!({"redirect_uris":[SPARK]}),
            false,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(client["token_endpoint_auth_method"], "client_secret_basic");
    let secret = client["client_secret"].as_str().unwrap();
    let stored: Value =
        f.g.store
            .get("oauth2_client", client["client_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(stored["client_secret_hash"], hash(secret));
    assert!(!stored.to_string().contains(secret));
}
#[tokio::test]
async fn native_loopback_login_refresh_and_revoke_preserve_scope_and_pkce() {
    for callback in [
        "http://127.0.0.1:45231/callback",
        "http://[::1]:45232/callback/native-client",
    ] {
        let f = Fixture::new().await;
        let mut configured = (*f.g.config).clone();
        configured.redirect_uris = vec![callback.into()];
        configured.validate_oauth_policy().unwrap();
        let client = f.register("none", callback).await;
        let code = f.code(&client).await;
        for (field, value) in [
            ("redirect_uri", callback.replace("4523", "5523")),
            ("code_verifier", random()),
            ("resource", "https://untrusted.example/mcp".into()),
        ] {
            let mut bad = code.clone();
            bad[field] = value.into();
            assert_eq!(
                f.authenticated(&client, "/oauth/token", bad).await.0,
                StatusCode::BAD_REQUEST
            );
        }
        let (status, _, token) = f.authenticated(&client, "/oauth/token", code).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(token["scope"], "code:read");
        let (status, _, renewed) = f.refresh(&client, &token).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(renewed["scope"], "code:read");
        let (status, _, _) = f
            .authenticated(
                &client,
                "/oauth/revoke",
                json!({"token":renewed["refresh_token"]}),
            )
            .await;
        assert!(status.is_success());
        assert_ne!(
            f.mcp(&renewed, "tools/list", json!({})).await.0,
            StatusCode::OK
        );
    }
}
#[tokio::test]
async fn native_callbacks_reject_aliases_external_addresses_and_ambiguous_paths() {
    let f = Fixture::new().await;
    for callback in [
        "http://localhost:45231/callback",
        "http://127.0.0.2:45231/callback",
        "http://0.0.0.0:45231/callback",
        "http://192.168.1.1:45231/callback",
        "http://127.0.0.1.evil.test:45231/callback",
        "http://127.1:45231/callback",
        "http://2130706433:45231/callback",
        "http://[::ffff:127.0.0.1]:45231/callback",
        "http://user@127.0.0.1:45231/callback",
        "http://127.0.0.1/callback",
        "http://127.0.0.1:0/callback",
        "http://127.0.0.1:45231/callback?next=evil",
        "http://127.0.0.1:45231/callback#fragment",
        "http://127.0.0.1:45231/other",
        "http://127.0.0.1:45231/a/../callback",
        "http://127.0.0.1:45231/callback/../callback",
        "http://127.0.0.1:45231/callback/%61",
        "http://127.0.0.1:45231/callback/nested/path",
    ] {
        let (status, _, _) = f
            .request(
                "POST",
                "/oauth/register",
                json!({"redirect_uris":[callback],"token_endpoint_auth_method":"none"}),
                false,
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{callback}");
        let mut bad = (*f.g.config).clone();
        bad.redirect_uris = vec![callback.into()];
        assert!(bad.validate_oauth_policy().is_err(), "{callback}");
    }
    let (_, headers, _) = f
        .request(
            "GET",
            "/.well-known/oauth-authorization-server",
            json!({}),
            false,
            &[],
        )
        .await;
    assert!(
        !headers["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("127.0.0.1")
    );
}
#[tokio::test]
async fn confidential_state_is_invisible_to_pre_authentication_binaries() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let id = client["client_id"].as_str().unwrap();
    assert!(id.starts_with("rh2_"));
    assert!(
        f.g.store
            .get::<Value>("client", id)
            .await
            .unwrap()
            .is_none()
    );
    let code = f.code(&client).await;
    let key = hash(code["code"].as_str().unwrap());
    assert!(
        f.g.store
            .get::<Value>("code", &key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.g.store
            .get::<Value>("oauth2_code", &key)
            .await
            .unwrap()
            .is_some()
    );
    let (status, _, token) = f.authenticated(&client, "/oauth/token", code).await;
    assert_eq!(status, StatusCode::OK);
    for (kind, field) in [("access", "access_token"), ("refresh", "refresh_token")] {
        let key = hash(token[field].as_str().unwrap());
        assert!(f.g.store.get::<Value>(kind, &key).await.unwrap().is_none());
        assert!(
            f.g.store
                .get::<Value>(&format!("oauth2_{kind}"), &key)
                .await
                .unwrap()
                .is_some()
        );
    }
}
#[tokio::test]
async fn public_refresh_markers_preserve_legacy_string_and_client_binding() {
    let f = Fixture::new().await;
    let client = f.register("none", GPT).await;
    let other = f.register("none", SPARK).await;
    let token = f.login(&client).await;
    let (status, _, fresh) = f.refresh(&client, &token).await;
    assert_eq!(status, StatusCode::OK);
    let key = hash(token["refresh_token"].as_str().unwrap());
    let family: String = f.g.store.get("used_refresh", &key).await.unwrap().unwrap();
    let binding: Value =
        f.g.store
            .get("oauth_refresh_binding", &key)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(binding["family"], family);
    assert_eq!(binding["client_id"], client["client_id"]);
    assert_eq!(f.refresh(&other, &token).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn basic_mcp_refresh_and_authenticated_revoke() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let token = f.login(&client).await;
    let (status,_,init)=f.mcp(&token,"initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}})).await;
    assert_eq!(status, StatusCode::OK);
    assert!(init["result"]["capabilities"]["tools"].is_object());
    let (status, _, tools) = f.mcp(&token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let authorized = tools["result"]["tools"].as_array().unwrap();
    // This OAuth grant requested code:read only. Tool discovery must not
    // advertise terminal execution or code writes that the grant cannot call.
    assert!(authorized.iter().all(|t| remote_hosts_code::tools::scope(
        t["name"].as_str().unwrap()
    ) == Some("code:read")));
    assert!(
        !authorized
            .iter()
            .any(|t| t["name"] == "terminal_exec" || t["name"] == "code_apply_edits")
    );
    let read = authorized
        .iter()
        .find(|t| t["name"] == "code_read")
        .unwrap();
    assert_eq!(read["annotations"]["readOnlyHint"], true);
    let download = authorized
        .iter()
        .find(|t| t["name"] == "file_download")
        .unwrap();
    assert_eq!(download["annotations"]["readOnlyHint"], false);
    let (status, _, devices) = f
        .mcp(
            &token,
            "tools/call",
            json!({"name":"devices_list","arguments":{}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(devices["result"]["structuredContent"]["devices"].is_array());
    let mut wrong = client.clone();
    wrong["client_secret"] = json!("wrong");
    let (status, headers, body) = f.refresh(&wrong, &token).await;
    invalid_client(status, &headers, &body);
    let (status, _, fresh) = f.refresh(&client, &token).await;
    assert_eq!(status, StatusCode::OK);
    let revoke = json!({"token":fresh["refresh_token"]});
    let (status, headers, body) = f
        .authenticated(&wrong, "/oauth/revoke", revoke.clone())
        .await;
    invalid_client(status, &headers, &body);
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(
        f.authenticated(&client, "/oauth/revoke", revoke).await.0,
        StatusCode::OK
    );
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn post_auth_cannot_be_downgraded_to_basic_or_none() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_post", SPARK).await;
    let code = f.code(&client).await;
    for headers in [vec![], vec![("authorization", basic(&client))]] {
        let (status, h, b) = f
            .request("POST", "/oauth/token", code.clone(), true, &headers)
            .await;
        invalid_client(status, &h, &b);
    }
    assert_eq!(
        f.authenticated(&client, "/oauth/token", code).await.0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn invalid_secret_client_pkce_redirect_or_resource_preserves_code() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let other = f.register("client_secret_basic", GPT).await;
    let code = f.code(&client).await;
    let mut wrong = client.clone();
    wrong["client_secret"] = json!("wrong");
    let (s, h, b) = f.authenticated(&wrong, "/oauth/token", code.clone()).await;
    invalid_client(s, &h, &b);
    assert_eq!(
        f.authenticated(&other, "/oauth/token", code.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    for (key, value) in [
        ("code_verifier", random()),
        ("redirect_uri", GPT.into()),
        ("resource", "https://other.example/mcp".into()),
    ] {
        let mut bad = code.clone();
        bad[key] = json!(value);
        assert_eq!(
            f.authenticated(&client, "/oauth/token", bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        f.authenticated(&client, "/oauth/token", code.clone())
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        f.authenticated(&client, "/oauth/token", code).await.0,
        StatusCode::BAD_REQUEST
    );
}
#[tokio::test]
async fn malformed_mixed_or_duplicate_client_auth_is_rejected() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let code = f.code(&client).await;
    for value in [
        "Basic ???".to_owned(),
        "Bearer unexpected".to_owned(),
        format!("Basic {}", STANDARD.encode("no-colon")),
        format!("Basic {}", STANDARD.encode("bad%xx:secret")),
    ] {
        let (s, h, b) = f
            .request(
                "POST",
                "/oauth/token",
                code.clone(),
                true,
                &[("authorization", value)],
            )
            .await;
        invalid_client(s, &h, &b);
    }
    let mut mixed = code.clone();
    mixed["client_secret"] = client["client_secret"].clone();
    let (s, h, b) = f
        .request(
            "POST",
            "/oauth/token",
            mixed,
            true,
            &[("authorization", basic(&client))],
        )
        .await;
    invalid_client(s, &h, &b);
    let (s, h, b) = f
        .request(
            "POST",
            "/oauth/token",
            code.clone(),
            true,
            &[
                ("authorization", basic(&client)),
                ("authorization", basic(&client)),
            ],
        )
        .await;
    invalid_client(s, &h, &b);
    let mut basic_only = code;
    basic_only.as_object_mut().unwrap().remove("client_id");
    assert_eq!(
        f.request(
            "POST",
            "/oauth/token",
            basic_only,
            true,
            &[("authorization", basic(&client))]
        )
        .await
        .0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn refresh_tokens_and_replay_revocation_are_client_bound() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let other = f.register("client_secret_basic", GPT).await;
    let token = f.login(&client).await;
    assert_eq!(f.refresh(&other, &token).await.0, StatusCode::BAD_REQUEST);
    let (status, _, fresh) = f.refresh(&client, &token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f.refresh(&other, &token).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(f.refresh(&client, &token).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn concurrent_refresh_cannot_miss_replay_marker() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let token = f.login(&client).await;
    let (a, b) = tokio::join!(f.refresh(&client, &token), f.refresh(&client, &token));
    let (ok, bad) = if a.0 == StatusCode::OK {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(ok.0, StatusCode::OK);
    assert_eq!(bad.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        f.mcp(&ok.2, "tools/list", json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn legacy_public_client_records_and_token_only_revocation_still_work() {
    let f = Fixture::new().await;
    let client =
        json!({"client_id":random(),"redirect_uris":[GPT],"token_endpoint_auth_method":"none"});
    f.g.store
        .put(
            "client",
            client["client_id"].as_str().unwrap(),
            &client,
            i64::MAX,
        )
        .await
        .unwrap();
    let token = f.login(&client).await;
    let (status, _, fresh) = f.refresh(&client, &token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        f.request(
            "POST",
            "/oauth/revoke",
            json!({"token":fresh["refresh_token"]}),
            true,
            &[]
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        f.mcp(&fresh, "tools/list", json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn unknown_or_another_clients_revoke_does_not_revoke_the_owner() {
    let f = Fixture::new().await;
    let client = f.register("client_secret_basic", SPARK).await;
    let other = f.register("client_secret_basic", GPT).await;
    let token = f.login(&client).await;
    assert_eq!(
        f.authenticated(
            &other,
            "/oauth/revoke",
            json!({"token":token["refresh_token"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        f.mcp(&token, "tools/list", json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(
        f.request(
            "POST",
            "/oauth/revoke",
            json!({"token":token["refresh_token"]}),
            true,
            &[]
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.request(
            "POST",
            "/oauth/revoke",
            json!({"token":random()}),
            true,
            &[]
        )
        .await
        .0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn spark_six_callback_registration_and_exact_code_exchange() {
    let f = Fixture::new().await;
    let callbacks: Vec<_> = [
        "oauth-redirect-sandbox.googleusercontent.com",
        "oauth-redirect-test.googleusercontent.com",
        "oauth-redirect.googleusercontent.com",
    ]
    .into_iter()
    .flat_map(|host| {
        ["r", "a"].map(|kind| format!("https://{host}/{kind}/user_bound_custom-mcp-fixture-spark"))
    })
    .collect();
    let registration = json!({"redirect_uris":callbacks,"client_name":"Google",
        "token_endpoint_auth_method":"client_secret_post",
        "grant_types":["authorization_code","refresh_token"],"response_types":["code"]});
    let (status, _, client) = f
        .request("POST", "/oauth/register", registration.clone(), false, &[])
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(client["redirect_uris"], registration["redirect_uris"]);
    for callback in &callbacks {
        let mut selected = client.clone();
        selected["redirect_uris"] = json!([callback]);
        let code = f.code(&selected).await;
        let mut wrong = code.clone();
        // Even another registered callback cannot exchange this callback's code.
        wrong["redirect_uri"] = json!(callbacks.iter().find(|uri| *uri != callback).unwrap());
        assert_eq!(
            f.authenticated(&client, "/oauth/token", wrong).await.0,
            StatusCode::BAD_REQUEST
        );
        let (status, _, token) = f.authenticated(&client, "/oauth/token", code).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(f.refresh(&client, &token).await.0, StatusCode::OK);
    }
    let mut too_many = registration;
    too_many["redirect_uris"]
        .as_array_mut()
        .unwrap()
        .push(json!(GPT));
    assert_eq!(
        f.request("POST", "/oauth/register", too_many, false, &[])
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn origin_allowlist_and_csp_are_separate_from_exact_callbacks() {
    let f = Fixture::new().await;
    for origin in [
        "https://evil.example",
        "https://gemini.google.com.evil.example",
        "null",
    ] {
        assert_eq!(
            f.request(
                "GET",
                "/.well-known/oauth-authorization-server",
                json!({}),
                false,
                &[("origin", origin.into())]
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }
    let (s, h, _) = f
        .request(
            "GET",
            "/.well-known/oauth-authorization-server",
            json!({}),
            false,
            &[("origin", "https://gemini.google.com".into())],
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let csp = h["content-security-policy"].to_str().unwrap();
    assert!(csp.contains("https://chatgpt.com"));
    assert!(csp.contains("https://oauth-redirect.googleusercontent.com"));
    assert!(!csp.contains("https://gemini.google.com"));
    assert!(!csp.contains('*'));
    for callback in [
        "https://gemini.google.com/callback",
        "https://oauth-redirect.googleusercontent.com/r/not-registered",
        "https://oauth-redirect.googleusercontent.com/r/user_bound_custom-mcp-ok/extra",
        "https://oauth-redirect.googleusercontent.com/r/user_bound_custom-mcp-ok?next=evil",
        "https://oauth-redirect.googleusercontent.com.evil.test/r/user_bound_custom-mcp-ok",
        "https://oauth-redirect-sandbox.googleusercontent.com.evil.test/a/user_bound_custom-mcp-ok",
        "https://oauth-redirect-other.googleusercontent.com/a/user_bound_custom-mcp-ok",
        "https://oauth-redirect-test.googleusercontent.com/b/user_bound_custom-mcp-ok",
        "https://oauth-redirect-test.googleusercontent.com/a/user_bound_custom-mcp-ok#fragment",
        "https://oauth-redirect-test.googleusercontent.com/a/user_bound_custom-mcp-%2fescape",
        "http://oauth-redirect-test.googleusercontent.com/a/user_bound_custom-mcp-ok",
        "https://user@oauth-redirect-test.googleusercontent.com/a/user_bound_custom-mcp-ok",
        "https://oauth-redirect-test.googleusercontent.com:444/a/user_bound_custom-mcp-ok",
    ] {
        assert_eq!(
            f.request(
                "POST",
                "/oauth/register",
                json!({"redirect_uris":[callback]}),
                false,
                &[]
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
}
#[tokio::test]
async fn legacy_config_defaults_and_invalid_policy_fail_closed() {
    let f = Fixture::new().await;
    let mut old = serde_json::to_value(&*f.g.config).unwrap();
    old.as_object_mut().unwrap().remove("allowed_origins");
    let config: GatewayConfig = serde_json::from_value(old).unwrap();
    assert_eq!(
        config.allowed_origins,
        vec!["https://chatgpt.com", "https://gemini.google.com"]
    );
    for origin in [
        "https://*.google.com",
        "http://gemini.google.com",
        "https://gemini.google.com/path",
        "https://gemini.google.com/",
    ] {
        let mut bad = config.clone();
        bad.allowed_origins = vec![origin.into()];
        assert!(bad.validate_oauth_policy().is_err());
    }
    for uri in [
        "https://*.googleusercontent.com/r/x",
        "https://user:pass@example.com/r/x",
        "https://example.com/cb#fragment",
        "http://example.com/cb",
    ] {
        let mut bad = config.clone();
        bad.redirect_uris = vec![uri.into()];
        assert!(bad.validate_oauth_policy().is_err());
    }
}
#[tokio::test]
async fn registration_rejects_unsupported_auth_and_grants() {
    let f = Fixture::new().await;
    for extra in [
        json!({"token_endpoint_auth_method":"client_secret_jwt"}),
        json!({"grant_types":["client_credentials"]}),
        json!({"response_types":["token"]}),
        json!({"client_name":"bad\nname"}),
    ] {
        let mut value = json!({"redirect_uris":[SPARK]});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert_eq!(
            f.request("POST", "/oauth/register", value, false, &[])
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
}
#[tokio::test]
async fn authorization_page_escapes_unverified_client_name() {
    let f = Fixture::new().await;
    let (_,_,client)=f.request("POST","/oauth/register",json!({"redirect_uris":[SPARK],"client_name":"<script>alert('x')</script>","token_endpoint_auth_method":"none"}),false,&[]).await;
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(random().as_bytes()));
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs([
            ("client_id", client["client_id"].as_str().unwrap()),
            ("redirect_uri", SPARK),
            ("response_type", "code"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("resource", RESOURCE),
            ("state", "x"),
        ])
        .finish();
    let (s, _, html) = f
        .request(
            "GET",
            &format!("/oauth/authorize?{query}"),
            json!({}),
            false,
            &[],
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let html = html.as_str().unwrap();
    assert!(html.contains("&lt;script&gt;"));
    assert!(!html.contains("<script>"));
    assert!(html.contains("未经验证"));
    assert!(html.contains(SPARK));
}
#[tokio::test]
async fn static_cli_credentials_are_private_no_clobber_and_usable() {
    let f = Fixture::new().await;
    let config = f._dir.path().join("gateway.json");
    let output = f._dir.path().join("oauth-client.json");
    write_private(&config, &serde_json::to_vec_pretty(&*f.g.config).unwrap()).unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_remote-hosts-code"))
            .args(["register-oauth-client", "--gateway-config"])
            .arg(&config)
            .args([
                "--name",
                "Fixture Spark",
                "--redirect-uri",
                SPARK,
                "--output",
            ])
            .arg(&output)
            .output()
            .unwrap()
    };
    let first = run();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let bytes = std::fs::read(&output).unwrap();
    let client: Value = serde_json::from_slice(&bytes).unwrap();
    let secret = client["client_secret"].as_str().unwrap();
    assert!(!String::from_utf8_lossy(&first.stdout).contains(secret));
    assert!(!String::from_utf8_lossy(&first.stderr).contains(secret));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(!run().status.success());
    assert_eq!(std::fs::read(&output).unwrap(), bytes);
    let token = f.login(&client).await;
    assert_eq!(
        f.mcp(&token, "tools/list", json!({})).await.0,
        StatusCode::OK
    );
}

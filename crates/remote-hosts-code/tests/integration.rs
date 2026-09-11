use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    auth::Principal,
    gateway::{Gateway, Job},
    hash, random,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

async fn request(
    router: &Router,
    method: &str,
    path: &str,
    body: Value,
    bearer: Option<&str>,
    cookie: Option<&str>,
    form: bool,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let body = if form {
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                body.as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str().unwrap())),
            )
            .finish()
    } else {
        body.to_string()
    };
    let mut r = Request::builder()
        .method(method)
        .uri(path)
        .header("Host", "mcp.example.com:8443")
        .header(
            "content-type",
            if form {
                "application/x-www-form-urlencoded"
            } else {
                "application/json"
            },
        )
        .header("accept", "application/json, text/event-stream");
    if let Some(b) = bearer {
        r = r.header("authorization", format!("Bearer {b}"));
    }
    if let Some(c) = cookie {
        r = r.header("cookie", c);
    }
    let response = router
        .clone()
        .oneshot(r.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, headers, value)
}
async fn gateway(dir: &std::path::Path) -> (Gateway, String, String) {
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    let password = random();
    let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
    let password_hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string();
    let token = random();
    let config = GatewayConfig {
        public_url: "https://mcp.example.com:8443".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.into(),
        owner: "owner".into(),
        password_hash,
        devices: vec![DeviceRegistration {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Mac A".into(),
            token_hash: hash(&token),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        }],
        redirect_uris: vec!["https://chatgpt.com/connector_platform_oauth_redirect".into()],
    };
    (Gateway::new(config).await.unwrap(), password, token)
}
async fn login(router: &Router, password: &str) -> Value {
    use base64::Engine;
    use sha2::Digest;
    let(status,_,client)=request(router,"POST","/oauth/register",json!({"redirect_uris":["https://chatgpt.com/connector_platform_oauth_redirect"],"token_endpoint_auth_method":"none"}),None,None,false).await;
    assert_eq!(status, StatusCode::CREATED);
    let verifier = random();
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(sha2::Sha256::digest(verifier.as_bytes()));
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs([
            ("client_id", client["client_id"].as_str().unwrap()),
            (
                "redirect_uri",
                "https://chatgpt.com/connector_platform_oauth_redirect",
            ),
            ("response_type", "code"),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("state", "test-state"),
            ("resource", "https://mcp.example.com:8443/mcp"),
        ])
        .finish();
    let (status, headers, _) = request(
        router,
        "GET",
        &format!("/oauth/authorize?{query}"),
        json!({}),
        None,
        None,
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cookie = headers
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let nonce = cookie.strip_prefix("rh_oauth=").unwrap();
    let (status, _, _) = request(
        router,
        "POST",
        "/oauth/approve",
        json!({"nonce":nonce,"password":password}),
        None,
        None,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "CSRF cookie required");
    let (status, headers, _) = request(
        router,
        "POST",
        "/oauth/approve",
        json!({"nonce":nonce,"password":password}),
        None,
        Some(cookie),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let uri = url::Url::parse(headers.get("location").unwrap().to_str().unwrap()).unwrap();
    let query: std::collections::HashMap<_, _> = uri.query_pairs().into_owned().collect();
    assert_eq!(query["iss"], "https://mcp.example.com:8443");
    assert_eq!(query["state"], "test-state");
    let form = json!({"grant_type":"authorization_code","client_id":client["client_id"],"code":query["code"],"code_verifier":verifier,"redirect_uri":"https://chatgpt.com/connector_platform_oauth_redirect","resource":"https://mcp.example.com:8443/mcp"});
    let (status, _, mut token) = request(
        router,
        "POST",
        "/oauth/token",
        form.clone(),
        None,
        None,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = request(router, "POST", "/oauth/token", form, None, None, true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "code must be single-use");
    token["client_id"] = client["client_id"].clone();
    token
}
#[tokio::test]
async fn oauth_pkce_mcp_and_refresh_replay() {
    let d = tempfile::tempdir().unwrap();
    let (g, password, _) = gateway(d.path()).await;
    let router = g.router().unwrap();
    let (status, headers, _) = request(&router, "POST", "/mcp", json!({}), None, None, false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(headers.contains_key("www-authenticate"));
    let (status, _, _) = request(
        &router,
        "POST",
        "/oauth/register",
        json!({"redirect_uris":["https://evil.example/callback"]}),
        None,
        None,
        false,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let token = login(&router, &password).await;
    let bearer = token["access_token"].as_str().unwrap();
    let(status,_,result)=request(&router,"POST","/mcp",json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}),Some(bearer),None,false).await;
    assert_eq!(status, StatusCode::OK);
    assert!(result["result"]["capabilities"]["tools"].is_object());
    let(status,_,result)=request(&router,"POST","/mcp",json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"devices_list","arguments":{}}}),Some(bearer),None,false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        result["result"]["structuredContent"]["devices"][0]["name"],
        "Mac A"
    );
    let form = json!({"grant_type":"refresh_token","client_id":token["client_id"],"refresh_token":token["refresh_token"],"resource":"https://mcp.example.com:8443/mcp"});
    let (status, _, new) = request(
        &router,
        "POST",
        "/oauth/token",
        form.clone(),
        None,
        None,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = request(&router, "POST", "/oauth/token", form, None, None, true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _, _) = request(
        &router,
        "POST",
        "/mcp",
        json!({}),
        new["access_token"].as_str(),
        None,
        false,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "refresh replay revokes family"
    );
}
#[tokio::test]
async fn relay_edit_retry_and_isolation() {
    let gd = tempfile::tempdir().unwrap();
    let (g, _, device_token) = gateway(gd.path()).await;
    let root = tempfile::tempdir().unwrap();
    let ad = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("code.rs"), "fn answer() -> u32 { 41 }\n").unwrap();
    let agent = Arc::new(
        Agent::new(AgentConfig {
            gateway_url: g.config.public_url.clone(),
            device_id: g.config.devices[0].id.clone(),
            device_token: device_token.clone(),
            state_dir: ad.path().into(),
            roots: vec![root.path().into()],
            allow_write: true,
            allow_exec: true,
            shell: "/bin/sh".into(),
        })
        .await
        .unwrap(),
    );
    let router = g.router().unwrap();
    let worker_router = router.clone();
    let a = agent.clone();
    let session = random();
    let roots = root.path().to_string_lossy().to_string();
    let worker = tokio::spawn(async move {
        loop {
            let (status, _, v) = request(
                &worker_router,
                "POST",
                "/device/poll",
                json!({"session":session,"roots":[roots],"allow_write":true,"allow_exec":true}),
                Some(&device_token),
                None,
                false,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            if !v["job"].is_null() {
                let job: Job = serde_json::from_value(v["job"].clone()).unwrap();
                let result = a.execute(&job).await.unwrap();
                let (status, _, _) = request(
                    &worker_router,
                    "POST",
                    "/device/result",
                    json!({"operation_id":job.id,"result":result}),
                    Some(&device_token),
                    None,
                    false,
                )
                .await;
                assert_eq!(status, StatusCode::OK);
            }
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let p = Principal {
        owner: "owner".into(),
        scopes: vec![
            "code:read".into(),
            "code:write".into(),
            "terminal:exec".into(),
        ],
    };
    let open = g
        .dispatch(
            &p,
            "workspace_open",
            json!({"device_id":g.config.devices[0].id,"root":root.path(),"idempotency_key":"open"}),
        )
        .await
        .unwrap();
    let ws = open["workspace"]["id"].as_str().unwrap();
    let edit = json!({"workspace_id":ws,"idempotency_key":"edit","files":[{"path":"code.rs","expected_version":hash("fn answer() -> u32 { 41 }\n"),"edits":[{"old_text":"41","new_text":"42"}]}]});
    let first = g
        .dispatch(&p, "code_apply_edits", edit.clone())
        .await
        .unwrap();
    let second = g
        .dispatch(&p, "code_apply_edits", edit.clone())
        .await
        .unwrap();
    assert_eq!(first, second);
    assert!(
        std::fs::read_to_string(root.path().join("code.rs"))
            .unwrap()
            .contains("42")
    );
    let mut different = edit.clone();
    different["files"][0]["edits"][0]["new_text"] = json!("43");
    assert!(g.dispatch(&p, "code_apply_edits", different).await.is_err());
    let stranger = Principal {
        owner: "stranger".into(),
        scopes: p.scopes.clone(),
    };
    assert!(
        g.dispatch(
            &stranger,
            "operation_get",
            json!({"operation_id":first["operation_id"]})
        )
        .await
        .is_err()
    );
    let t=g.dispatch(&p,"terminal_exec",json!({"workspace_id":ws,"idempotency_key":"test","command":"printf 'test passed\\n'","timeout_seconds":10})).await.unwrap();
    let terminal = t["terminal_id"].as_str().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let out = g
        .dispatch(
            &p,
            "terminal_read",
            json!({"workspace_id":ws,"terminal_id":terminal}),
        )
        .await
        .unwrap();
    assert!(out["output"].as_str().unwrap().contains("test passed"));
    worker.abort();
}

#[tokio::test]
async fn device_binding_terminal_input_cancel_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let ad = tempfile::tempdir().unwrap();
    let bd = tempfile::tempdir().unwrap();
    let config = |state: &std::path::Path| AgentConfig {
        gateway_url: "https://mcp.example.com:8443".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: random(),
        state_dir: state.into(),
        roots: vec![root.path().into()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    };
    let ca = config(ad.path());
    let cb = config(bd.path());
    let a = Agent::new(ca.clone()).await.unwrap();
    let b = Agent::new(cb).await.unwrap();
    let job = |device: &str, tool: &str, args: Value| Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: device.into(),
        owner: "owner".into(),
        tool: tool.into(),
        arguments: args,
    };
    let wa = a
        .execute(&job(
            &ca.device_id,
            "workspace_open",
            json!({"device_id":ca.device_id,"root":root.path(),"idempotency_key":"open"}),
        ))
        .await
        .unwrap();
    let ws = wa["workspace"]["id"].as_str().unwrap();
    let cross = b
        .execute(&job(
            &b.config.device_id,
            "code_list",
            json!({"workspace_id":ws}),
        ))
        .await
        .unwrap();
    assert_eq!(cross["error"], "tool_failed");
    let t=a.execute(&job(&ca.device_id,"terminal_exec",json!({"workspace_id":ws,"command":"read line; printf 'received:%s\\n' \"$line\"; sleep 30","pty":true,"timeout_seconds":60,"idempotency_key":"terminal"}))).await.unwrap();
    let tid = t["terminal_id"].as_str().unwrap();
    let input = job(
        &ca.device_id,
        "terminal_input",
        json!({"workspace_id":ws,"terminal_id":tid,"text":"hello\n","idempotency_key":"input"}),
    );
    assert_eq!(
        a.execute(&input).await.unwrap(),
        a.execute(&input).await.unwrap()
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut cursor = 0u64;
    let mut observed = String::new();
    loop {
        let output = a
            .execute(&job(
                &ca.device_id,
                "terminal_read",
                json!({"workspace_id":ws,"terminal_id":tid,"cursor":cursor}),
            ))
            .await
            .unwrap();
        observed.push_str(output["output"].as_str().unwrap());
        cursor = output["cursor"].as_u64().unwrap();
        if observed.contains("received:hello") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "terminal input was accepted but expected output never became observable"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let cancelled = a
        .execute(&job(
            &ca.device_id,
            "terminal_cancel",
            json!({"workspace_id":ws,"terminal_id":tid,"idempotency_key":"cancel"}),
        ))
        .await
        .unwrap();
    assert_eq!(cancelled["terminal"]["state"], "cancelled");
    let id = uuid::Uuid::new_v4().to_string();
    let interrupted = Job {
        id: id.clone(),
        device_id: ca.device_id.clone(),
        owner: "owner".into(),
        tool: "code_apply_edits".into(),
        arguments: json!({"workspace_id":ws,"idempotency_key":"crash","files":[]}),
    };
    a.store.put("local_operation",&id,&json!({"fingerprint":hash(serde_json::to_vec(&interrupted).unwrap()),"state":"running","result":null}),i64::MAX).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let restarted = Agent::new(ca).await.unwrap();
    assert_eq!(
        restarted.execute(&interrupted).await.unwrap()["error"],
        "outcome_unknown"
    );
}

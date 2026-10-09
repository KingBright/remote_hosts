use super::*;
use crate::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    agent::Agent,
    gateway::{DeviceHello, Job},
};
use axum::{body::Body, http::Request};
use tower::ServiceExt;

struct Fixture {
    _dir: tempfile::TempDir,
    g: Gateway,
    p: Principal,
    agent: Agent,
    device: String,
    session: String,
    cookie: String,
    password: String,
}
async fn fixture() -> Fixture {
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    let dir = tempfile::tempdir().unwrap();
    let password = "synthetic-owner-password".to_owned();
    let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
    let password_hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string();
    let device = uuid::Uuid::new_v4().to_string();
    let session = crate::random();
    let token = crate::random();
    let scopes: Vec<String> = crate::SCOPES
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let g = Gateway::new(GatewayConfig {
        public_url: "https://fixture.example".into(),
        bind: "127.0.0.1:0".into(),
        state_dir: dir.path().join("gateway"),
        owner: "owner".into(),
        password_hash,
        devices: vec![DeviceRegistration {
            id: device.clone(),
            name: "Fixture".into(),
            token_hash: hash(&token),
            scopes: scopes.clone(),
        }],
        redirect_uris: vec![],
        allowed_origins: crate::default_mcp_client_origins(),
    })
    .await
    .unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let agent = Agent::new(AgentConfig {
        gateway_url: g.config.public_url.clone(),
        device_id: device.clone(),
        device_token: token,
        state_dir: dir.path().join("agent"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let hello: DeviceHello = serde_json::from_value(json!({
        "version":env!("CARGO_PKG_VERSION"),"session":session,"roots":[root],
        "platform":std::env::consts::OS,"allow_write":true,"allow_exec":true,
    }))
    .unwrap();
    g.store
        .put(
            "online",
            &device,
            &json!({"hello":hello,"last_seen":now()}),
            i64::MAX,
        )
        .await
        .unwrap();
    let cookie = crate::random();
    g.store
        .put("status_session", &hash(&cookie), &true, now() + 3600)
        .await
        .unwrap();
    Fixture {
        _dir: dir,
        g,
        p: Principal {
            owner: "owner".into(),
            scopes,
        },
        agent,
        device,
        session,
        cookie,
        password,
    }
}
fn request_id() -> String {
    format!("req_{}", uuid::Uuid::new_v4().simple())
}
async fn grant(f: &Fixture, expected: u64, enabled: bool, scopes: &[&str]) -> Grant {
    save(
        &f.g,
        "overnight",
        expected,
        enabled,
        vec![f.device.clone()],
        scopes.iter().map(|v| (*v).into()).collect(),
    )
    .await
    .unwrap()
}
async fn count(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap()
}
async fn call(f: &Fixture, tool: &str, args: Value) -> Value {
    receipts::invoke(&f.g, &f.p, tool, args, &request_id())
        .await
        .unwrap()
}
fn selection(f: &Fixture) -> crate::job_dispatch::Selection<'_> {
    crate::job_dispatch::Selection {
        device: &f.device,
        session: &f.session,
        active: "[]",
        scopes: r#"["code:read","code:write","terminal:exec"]"#,
        lanes: r#"["read","write","terminal","control","transfer"]"#,
        defer_writes: false,
        write_workspaces: "[]",
        terminal_inputs: "[]",
    }
}
async fn run(f: &Fixture, tool: &str, args: Value) -> Value {
    let pending = call(f, tool, args).await;
    let id = pending["operation_id"].as_str().expect("queued operation");
    let (claimed, raw) = crate::job_dispatch::claim(&f.g.store, &selection(f), now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed, id);
    let job: Job = serde_json::from_str(&raw).unwrap();
    assert!(job.arguments.get("authorization_version").is_none());
    assert!(job.arguments.get("task_id").is_none());
    let value = f.agent.execute(&job).await.unwrap();
    crate::job_receipts::commit(&f.g.store, &f.device, id, &value)
        .await
        .unwrap();
    f.g.result(&f.p, id).await.unwrap()
}
fn terminal(f: &Fixture, version: u64, key: &str) -> Value {
    json!({"workspace_id":format!("{}:{}",f.device,uuid::Uuid::new_v4()),
        "command":"printf immutable-command","idempotency_key":key,
        "task_id":"overnight","authorization_version":version})
}
async fn form(
    f: &Fixture,
    cookie: bool,
    origin: &str,
    token: &str,
    password: &str,
    expected: &str,
    scopes: &str,
) -> StatusCode {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs([
            ("task_id", "overnight"),
            ("expected_version", expected),
            ("action", "authorize"),
            ("devices", f.device.as_str()),
            ("scopes", scopes),
            ("csrf", token),
            ("password", password),
        ])
        .finish();
    let mut builder = Request::builder()
        .method("POST")
        .uri("/status/task-authorization")
        .header("host", "fixture.example")
        .header("origin", origin)
        .header("authorization", "Bearer synthetic-not-owner-authority")
        .header("content-type", "application/x-www-form-urlencoded");
    if cookie {
        builder = builder.header("cookie", format!("rh_status={}", f.cookie))
    }
    let response =
        f.g.router()
            .unwrap()
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
    response.status()
}

#[tokio::test]
async fn owner_can_revoke_after_device_enrollment_or_scopes_change() {
    for remove_device in [false, true] {
        let mut f = fixture().await;
        let previous = grant(&f, 0, true, &["code:read", "code:write", "terminal:exec"]).await;
        let config = std::sync::Arc::make_mut(&mut f.g.config);
        if remove_device {
            config.devices.clear();
        } else {
            config.devices[0].scopes = vec!["code:read".into()];
        }
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("task_id", "overnight"),
                ("expected_version", "1"),
                ("action", "revoke"),
                ("devices", "unregistered"),
                ("scopes", "root"),
                ("csrf", csrf(&f.cookie).as_str()),
                ("password", f.password.as_str()),
            ])
            .finish();
        let response =
            f.g.router()
                .unwrap()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/status/task-authorization")
                        .header("host", "fixture.example")
                        .header("origin", "https://fixture.example")
                        .header("cookie", format!("rh_status={}", f.cookie))
                        .header("content-type", "application/x-www-form-urlencoded")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let revoked: Grant =
            f.g.store
                .get(KIND, &key("owner", "overnight"))
                .await
                .unwrap()
                .unwrap();
        assert!(!revoked.enabled);
        assert_eq!(revoked.version, 2);
        assert_eq!(revoked.devices, previous.devices);
        assert_eq!(revoked.scopes, previous.scopes);
        assert_eq!(count(&f).await, 0);
    }
}

#[tokio::test]
async fn queued_resume_requires_existing_undispatched_timing_evidence() {
    let f = fixture().await;
    grant(&f, 0, true, &["code:read", "terminal:exec"]).await;
    let pending = call(&f, "terminal_exec", terminal(&f, 1, "missing-timing")).await;
    let id = pending["operation_id"].as_str().unwrap();
    grant(&f, 1, true, &["code:read", "terminal:exec"]).await;
    // Simulate a corrupted/missing receipt in this isolated fixture only.
    sqlx::query("DELETE FROM operation_timing WHERE id=?")
        .bind(id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    let denied = call(
        &f,
        "task_resume",
        json!({"operation_id":id,"task_id":"overnight","authorization_version":2}),
    )
    .await;
    assert!(denied.get("error").is_some());
    let binding: Binding = f.g.store.get(BINDING, id).await.unwrap().unwrap();
    assert_eq!(binding.version, 1);
    assert_eq!(count(&f).await, 1);
    assert!(
        crate::job_dispatch::claim(&f.g.store, &selection(&f), now())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn owner_route_requires_password_session_origin_csrf_and_cas() {
    let mut f = fixture().await;
    // Exercise the existing owner login, not a manually seeded grant/session.
    f.g.store
        .take::<bool>("status_session", &hash(&f.cookie))
        .await
        .unwrap();
    let response =
        f.g.router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .uri("/status/task-authorization?task_id=overnight")
                    .header("host", "fixture.example")
                    .header("authorization", "Bearer synthetic-not-owner-authority")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let login = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("password", &f.password)
        .finish();
    let response =
        f.g.router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/status/login")
                    .header("host", "fixture.example")
                    .header("origin", "https://fixture.example")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(login))
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cookie.contains("Secure; HttpOnly; SameSite=Strict; Path=/status"));
    f.cookie = cookie
        .strip_prefix("rh_status=")
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let token = csrf(&f.cookie);
    let response =
        f.g.router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .uri("/status/task-authorization?task_id=overnight")
                    .header("host", "fixture.example")
                    .header("cookie", format!("rh_status={}", f.cookie))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let html = String::from_utf8(
        axum::body::to_bytes(response.into_body(), 16384)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains(&format!("name=csrf value=\"{token}\"")));
    assert!(html.contains("name=expected_version value=\"0\""));
    assert!(
        !crate::tools::catalog()
            .iter()
            .any(|v| v.name.contains("authorize") || v.name.contains("grant"))
    );
    for (cookie, origin, nonce, pass, status) in [
        (
            false,
            "https://fixture.example",
            token.as_str(),
            f.password.as_str(),
            StatusCode::FORBIDDEN,
        ),
        (
            true,
            "https://attacker.example",
            token.as_str(),
            f.password.as_str(),
            StatusCode::FORBIDDEN,
        ),
        (
            true,
            "https://fixture.example",
            "wrong",
            f.password.as_str(),
            StatusCode::FORBIDDEN,
        ),
        (
            true,
            "https://fixture.example",
            token.as_str(),
            "wrong",
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        assert_eq!(
            form(&f, cookie, origin, nonce, pass, "0", crate::SCOPES).await,
            status
        );
    }
    assert_eq!(
        form(
            &f,
            true,
            "https://fixture.example",
            &token,
            &f.password,
            "0",
            "root"
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        form(
            &f,
            true,
            "https://fixture.example",
            &token,
            &f.password,
            "0",
            crate::SCOPES
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        form(
            &f,
            true,
            "https://fixture.example",
            &token,
            &f.password,
            "0",
            crate::SCOPES
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(count(&f).await, 0);
    let stored: Grant =
        f.g.store
            .get(KIND, &key("owner", "overnight"))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(stored.version, 1);
    assert_eq!(stored.owner, "owner");
    assert!(f.g.store.list::<Value>("access").await.unwrap().is_empty());
    assert!(f.g.store.list::<Value>("refresh").await.unwrap().is_empty());
    // Logging out does not erase the persisted task approval.
    let response =
        f.g.router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/status/logout")
                    .header("host", "fixture.example")
                    .header("origin", "https://fixture.example")
                    .header("cookie", format!("rh_status={}", f.cookie))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(
        f.g.store
            .get::<bool>("status_session", &hash(&f.cookie))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        requested(
            &f.g,
            &f.p,
            "terminal_exec",
            &terminal(&f, 1, "offline"),
            &f.device
        )
        .await
        .unwrap()
        .is_some()
    );
}
#[tokio::test]
async fn legacy_label_is_not_a_grant_and_intersections_remain_required() {
    let f = fixture().await;
    let mut args = terminal(&f, 1, "one");
    assert!(
        requested(&f.g, &f.p, "terminal_exec", &args, &f.device)
            .await
            .unwrap_err()
            .to_string()
            .contains("missing")
    );
    args.as_object_mut()
        .unwrap()
        .remove("authorization_version");
    assert!(
        requested(&f.g, &f.p, "terminal_exec", &args, &f.device)
            .await
            .unwrap()
            .is_none()
    );
    grant(&f, 0, true, &["code:read"]).await;
    assert_eq!(
        call(&f, "terminal_exec", terminal(&f, 1, "blocked")).await["error_code"],
        "task_scope_denied"
    );
    let limited = Principal {
        owner: f.p.owner.clone(),
        scopes: vec!["code:read".into()],
    };
    let denied = receipts::invoke(
        &f.g,
        &limited,
        "terminal_exec",
        terminal(&f, 1, "account"),
        &request_id(),
    )
    .await
    .unwrap();
    assert_eq!(denied["error_code"], "account_scope_denied");
    assert_eq!(count(&f).await, 0);
    let binding = Binding {
        key: key("owner", "overnight"),
        owner: "other".into(),
        task_id: "overnight".into(),
        version: 1,
        device_id: f.device.clone(),
        scope: "code:read".into(),
    };
    let stored: Grant =
        f.g.store
            .get(KIND, &key("owner", "overnight"))
            .await
            .unwrap()
            .unwrap();
    assert!(check(Some(&stored), &binding).is_err());
}
#[tokio::test]
async fn authorized_owner_offline_workspace_edit_test_commit_and_evidence() {
    let f = fixture().await;
    let nonce = csrf(&f.cookie);
    assert_eq!(
        form(
            &f,
            true,
            "https://fixture.example",
            &nonce,
            &f.password,
            "0",
            crate::SCOPES
        )
        .await,
        StatusCode::OK
    );
    f.g.store
        .take::<bool>("status_session", &hash(&f.cookie))
        .await
        .unwrap();
    let root = f._dir.path().join("project");
    let opened = run(
        &f,
        "workspace_open",
        json!({"device_id":f.device,"root":root,
        "idempotency_key":"open","task_id":"overnight","authorization_version":1}),
    )
    .await;
    let ws = opened["workspace"]["id"].as_str().unwrap();
    let edited=run(&f,"code_apply_edits",json!({"workspace_id":ws,"idempotency_key":"create",
        "task_id":"overnight","authorization_version":1,
        "files":[{"path":"sample.txt","action":"create","expected_version":"absent","content":"approved\n"}]})).await;
    assert_eq!(edited["state"], "completed");
    let tested=run(&f,"terminal_exec",json!({"workspace_id":ws,"idempotency_key":"test-and-commit",
        "task_id":"overnight","authorization_version":1,"wait_ms":2000,"timeout_seconds":15,
        "command":"test \"$(cat sample.txt)\" = approved && git init -q && git add sample.txt && git -c user.name='Task Fixture' -c user.email='fixture@example.invalid' commit -qm approved && git rev-parse --verify HEAD"})).await;
    let terminal_id = tested["terminal_id"].as_str().unwrap();
    let mut evidence = Value::Null;
    for _ in 0..30 {
        evidence = run(
            &f,
            "terminal_read",
            json!({"workspace_id":ws,"terminal_id":terminal_id,
            "max_bytes":4096,"task_id":"overnight","authorization_version":1}),
        )
        .await;
        if evidence["terminal"]["exit_code"].is_number() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(evidence["terminal"]["exit_code"], 0, "{}", evidence);
    assert!(
        evidence["output"]
            .as_str()
            .unwrap()
            .lines()
            .any(|v| v.len() == 40 && v.bytes().all(|b| b.is_ascii_hexdigit()))
    );
    let read = run(
        &f,
        "code_read",
        json!({"workspace_id":ws,"requests":[{"path":"sample.txt","start_line":1,"end_line":2}],
        "task_id":"overnight","authorization_version":1}),
    )
    .await;
    assert_eq!(read["ranges"][0]["text"], "approved\n");
}
#[tokio::test]
async fn revoked_queue_does_not_block_independent_work_and_resume_keeps_job() {
    let f = fixture().await;
    grant(&f, 0, true, &["code:read", "terminal:exec"]).await;
    let first = call(&f, "terminal_exec", terminal(&f, 1, "first")).await;
    let id = first["operation_id"].as_str().unwrap();
    grant(&f, 1, false, &["code:read", "terminal:exec"]).await;
    assert!(
        crate::job_dispatch::claim(&f.g.store, &selection(&f), now())
            .await
            .unwrap()
            .is_none()
    );
    let independent = call(
        &f,
        "code_read",
        json!({"workspace_id":format!("{}:{}",f.device,uuid::Uuid::new_v4()),
        "requests":[{"path":"independent","start_line":1,"end_line":1}]}),
    )
    .await;
    let picked = crate::job_dispatch::claim(&f.g.store, &selection(&f), now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(picked.0, independent["operation_id"]);
    let context = call(&f, "task_context", json!({"task_id":"overnight"})).await;
    assert_eq!(context["summary"]["authorization_blocked"], 1);
    grant(&f, 2, true, &["code:read", "terminal:exec"]).await;
    assert!(
        crate::job_dispatch::claim(&f.g.store, &selection(&f), now())
            .await
            .unwrap()
            .is_none()
    );
    let resumed = call(
        &f,
        "task_resume",
        json!({"operation_id":id,"task_id":"overnight","authorization_version":3}),
    )
    .await;
    assert_eq!(resumed["operation_id"], id);
    assert_eq!(count(&f).await, 2);
    let picked = crate::job_dispatch::claim(&f.g.store, &selection(&f), now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(picked.0, id);
    grant(&f, 3, true, &["code:read", "terminal:exec"]).await;
    let denied = call(
        &f,
        "task_resume",
        json!({"operation_id":id,"task_id":"overnight","authorization_version":4}),
    )
    .await;
    assert!(denied.get("error").is_some());
    assert_eq!(count(&f).await, 2);
}
#[tokio::test]
async fn changed_authorization_resumes_one_immutable_rejected_request_once() {
    let f = fixture().await;
    grant(&f, 0, true, &["code:read"]).await;
    let original = terminal(&f, 1, "immutable");
    let request = request_id();
    let denied = receipts::invoke(&f.g, &f.p, "terminal_exec", original.clone(), &request)
        .await
        .unwrap();
    assert_eq!(denied["execution_state"], "not_started");
    assert_eq!(denied["error_code"], "task_scope_denied");
    assert!(denied["operation_id"].is_null());
    assert_eq!(count(&f).await, 0);
    grant(&f, 1, true, &["code:read", "terminal:exec"]).await;
    let args = json!({"request_id":request,"task_id":"overnight","authorization_version":2});
    let (a, b) = tokio::join!(
        call(&f, "task_resume", args.clone()),
        call(&f, "task_resume", args.clone())
    );
    let successful = if a["operation_id"].is_string() { a } else { b };
    let id = successful["operation_id"].as_str().unwrap();
    assert_eq!(count(&f).await, 1);
    let duplicate = call(&f, "task_resume", args).await;
    assert_eq!(duplicate["operation_id"], id);
    assert_eq!(count(&f).await, 1);
    let raw: String = sqlx::query_scalar("SELECT request FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    let job: Job = serde_json::from_str(&raw).unwrap();
    assert_eq!(job.arguments["command"], original["command"]);
    assert_eq!(job.arguments["idempotency_key"], "immutable");
    let observed = call(
        &f,
        "operation_get",
        json!({"request_id":request,"wait_ms":0}),
    )
    .await;
    assert!(!observed.to_string().contains("immutable-command"));
}
#[tokio::test]
async fn unknown_platform_and_source_denials_are_never_resumed() {
    let f = fixture().await;
    grant(&f, 0, true, &["code:read"]).await;
    let request = request_id();
    receipts::invoke(
        &f.g,
        &f.p,
        "terminal_exec",
        terminal(&f, 1, "blocked"),
        &request,
    )
    .await
    .unwrap();
    grant(&f, 1, true, &["code:read", "terminal:exec"]).await;
    let original: Value =
        f.g.store
            .get("request_receipt", &request)
            .await
            .unwrap()
            .unwrap();
    for (state, code) in [
        ("unknown", "task_scope_denied"),
        ("not_started", "platform_authorization_required"),
        ("not_started", "source_address_policy_rejected"),
    ] {
        let mut altered = original.clone();
        altered["execution_state"] = json!(state);
        altered["error_code"] = json!(code);
        f.g.store
            .put("request_receipt", &request, &altered, i64::MAX)
            .await
            .unwrap();
        let out = call(
            &f,
            "task_resume",
            json!({"request_id":request,"task_id":"overnight","authorization_version":2}),
        )
        .await;
        assert!(out.get("error").is_some());
        assert_eq!(count(&f).await, 0);
    }
}
#[tokio::test]
async fn concurrent_grant_updates_cas_and_preserve_history() {
    let f = fixture().await;
    grant(&f, 0, true, &["code:read"]).await;
    let devices = vec![f.device.clone()];
    let (a, b) = tokio::join!(
        save(
            &f.g,
            "overnight",
            1,
            false,
            devices.clone(),
            vec!["code:read".into()]
        ),
        save(
            &f.g,
            "overnight",
            1,
            true,
            devices,
            vec!["code:read".into()]
        )
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        f.g.store
            .list::<Grant>("task_authorization_history")
            .await
            .unwrap()
            .len(),
        2
    );
}
#[tokio::test]
async fn local_exec_disable_and_device_scopes_are_not_enlarged() {
    let mut f = fixture().await;
    grant(&f, 0, true, &["code:read", "terminal:exec"]).await;
    let g = std::sync::Arc::make_mut(&mut f.g.config);
    g.devices[0].scopes = vec!["code:read".into()];
    let out = call(&f, "terminal_exec", terminal(&f, 1, "device")).await;
    assert_eq!(out["error_code"], "device_scope_denied");
    assert_eq!(count(&f).await, 0);
    let command = "touch should-not-exist";
    let root = f._dir.path().join("project");
    let other = Agent::new(AgentConfig {
        gateway_url: "https://fixture.example".into(),
        device_id: f.device.clone(),
        device_token: crate::random(),
        state_dir: f._dir.path().join("disabled-agent"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: false,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let job = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: f.device.clone(),
        owner: "owner".into(),
        tool: "terminal_exec".into(),
        arguments: json!({"workspace_id":"unused","idempotency_key":"disabled","command":command}),
    };
    let result = other.execute(&job).await.unwrap();
    assert_eq!(result["error_code"], "local_exec_disabled");
    assert!(!root.join("should-not-exist").exists());
}
#[test]
fn denial_classes_keep_platform_and_task_authority_separate() {
    for (message, code) in [
        ("insufficient_scope: terminal:exec", "account_scope_denied"),
        ("device_scope_denied: code:write", "device_scope_denied"),
        ("device file writes disabled", "local_write_disabled"),
        ("device terminal execution disabled", "local_exec_disabled"),
        ("authorization_required", "platform_authorization_required"),
        (
            "source_address_policy_rejected",
            "source_address_policy_rejected",
        ),
    ] {
        let v = crate::diagnostics::error("terminal_exec", message, None, "gateway_dispatch");
        assert_eq!(v["error_code"], code);
        assert!(!authorization_rejection(code));
    }
    let forged = crate::diagnostics::error(
        "code_apply_edits",
        "version_conflict: task_scope_denied",
        None,
        "agent_execute",
    );
    assert_eq!(forged["error_code"], "version_conflict");
}

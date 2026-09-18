//! Binary data-plane acceptance using temporary storage and synthetic credentials.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use remote_hosts_code::{
    AgentConfig, DeviceRegistration, GatewayConfig,
    auth::Principal,
    files::Workspace,
    gateway::{Gateway, Job},
    hash, now, random, tools, transfers,
};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn fixture() -> (tempfile::TempDir, Gateway, String, tokio::net::TcpListener) {
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let credential = random();
    let g = Gateway::new(GatewayConfig {
        allowed_origins: remote_hosts_code::default_mcp_client_origins(),
        public_url: format!("https://{}", listener.local_addr().unwrap()),
        bind: listener.local_addr().unwrap().to_string(),
        state_dir: dir.path().join("gateway"),
        owner: "test-owner".into(),
        password_hash: "unused-in-data-plane-tests".into(),
        devices: vec![DeviceRegistration {
            id: uuid::Uuid::new_v4().to_string(),
            name: "test-device".into(),
            token_hash: hash(&credential),
            scopes: vec!["code:read".into(), "code:write".into()],
        }],
        redirect_uris: vec![],
    })
    .await
    .unwrap();
    (dir, g, credential, listener)
}
async fn job(g: &Gateway, tool: &str, arguments: Value) -> Job {
    let job = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: g.config.devices[0].id.clone(),
        owner: g.config.owner.clone(),
        tool: tool.into(),
        arguments,
    };
    sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'dispatched',?)")
        .bind(&job.id)
        .bind(&job.device_id)
        .bind(random())
        .bind(hash(b"fixture"))
        .bind(serde_json::to_string(&job).unwrap())
        .bind(now())
        .execute(&g.store.pool)
        .await
        .unwrap();
    job
}
fn principal(g: &Gateway) -> Principal {
    Principal {
        owner: g.config.owner.clone(),
        scopes: vec!["code:read".into(), "code:write".into()],
    }
}
async fn get(g: &Gateway, path: &str, range: Option<&str>) -> axum::response::Response {
    let host = reqwest::Url::parse(&g.config.public_url)
        .unwrap()
        .authority()
        .to_owned();
    let mut r = Request::builder().uri(path).header("host", host);
    if let Some(range) = range {
        r = r.header("range", range);
    }
    g.router()
        .unwrap()
        .oneshot(r.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[test]
fn host_file_parameter_metadata_and_safety_annotations_survive_catalog() {
    assert_eq!(
        remote_hosts_code::release_manifest()["tool_count"],
        tools::catalog().len()
    );
    assert!(tools::catalog().iter().any(|t| t.name == "task_context"));
    let t = tools::catalog()
        .into_iter()
        .find(|t| t.name == "file_upload")
        .unwrap();
    let v = serde_json::to_value(t).unwrap();
    assert_eq!(v["_meta"]["openai/fileParams"], json!(["file"]));
    assert_eq!(v["annotations"]["readOnlyHint"], false);
    assert_eq!(v["annotations"]["destructiveHint"], true);
    let args = json!({"workspace_id":"d:w","idempotency_key":"import","path":"a.bin","file":{"file_id":"file-synthetic","download_url":"https://files.oaiusercontent.com/f"}});
    tools::validate("file_upload", &args).unwrap();
    let mut bad = args;
    bad["file"].as_object_mut().unwrap().remove("file_id");
    assert!(tools::validate("file_upload", &bad).is_err());
    tools::validate(
        "file_download",
        &json!({"workspace_id":"w","path":"f","idempotency_key":"k","max_bytes":67108865}),
    )
    .unwrap();
    assert!(
        tools::validate(
            "file_download",
            &json!({"workspace_id":"w","path":"f","idempotency_key":"k2","max_bytes":268435457})
        )
        .is_err()
    );
}

#[tokio::test]
async fn real_streaming_export_checksum_range_expiry_and_duplicate() {
    let (dir, g, credential, listener) = fixture().await;
    let project = dir.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let bytes: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(project.join("binary-test.bin"), &bytes).unwrap();
    let ws = Workspace {
        id: format!("{}:{}", g.config.devices[0].id, random()),
        device_id: g.config.devices[0].id.clone(),
        root: project.canonicalize().unwrap(),
    };
    let job=job(&g,"file_download",json!({"workspace_id":ws.id,"path":"binary-test.bin","idempotency_key":"export","expected_version":hash(&bytes)})).await;
    let config = AgentConfig {
        gateway_url: format!("http://{}", listener.local_addr().unwrap()),
        device_id: ws.device_id.clone(),
        device_token: credential.clone(),
        state_dir: dir.path().join("agent"),
        roots: vec![ws.root.clone()],
        allow_write: true,
        allow_exec: false,
        shell: "/bin/sh".into(),
    };
    let router = g.router().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let output = transfers::download(&config, &ws, &job.arguments, &job.id)
        .await
        .unwrap();
    assert_eq!(output["size"], bytes.len());
    assert_eq!(output["sha256"], hash(&bytes));
    assert_eq!(
        transfers::download(&config, &ws, &job.arguments, &job.id)
            .await
            .unwrap(),
        output
    );
    let response = reqwest::Client::new()
        .post(format!("{}/device/result", config.gateway_url))
        .bearer_auth(&credential)
        .json(&json!({"operation_id":job.id,"result":output}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result = g
        .dispatch(
            &principal(&g),
            "operation_get",
            json!({"operation_id":job.id}),
        )
        .await
        .unwrap();
    let url = reqwest::Url::parse(result["download_url"].as_str().unwrap()).unwrap();
    let full = get(&g, url.path(), None).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert!(
        full.headers()["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment;")
    );
    assert_eq!(full.headers()["referrer-policy"], "no-referrer");
    assert_eq!(
        to_bytes(full.into_body(), 3 * 1024 * 1024)
            .await
            .unwrap()
            .as_ref(),
        &bytes
    );
    let range = get(&g, url.path(), Some("bytes=11-999")).await;
    assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        to_bytes(range.into_body(), 1024).await.unwrap().as_ref(),
        &bytes[11..1000]
    );
    assert_eq!(
        get(&g, url.path(), Some("bytes=99999999-")).await.status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    sqlx::query("UPDATE kv SET expires=0 WHERE kind='file_link'")
        .execute(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(
        get(&g, url.path(), None).await.status(),
        StatusCode::NOT_FOUND
    );
    let refreshed = g
        .dispatch(
            &principal(&g),
            "operation_get",
            json!({"operation_id":job.id}),
        )
        .await
        .unwrap();
    assert_ne!(refreshed["download_url"], result["download_url"]);
    assert_eq!(refreshed["download_available"], true);
    let (durable_before,): (String,) = sqlx::query_as("SELECT result FROM jobs WHERE id=?")
        .bind(&job.id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    let (links_before,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='file_link'")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    // Both an expired cache entry and a collected artifact preserve the receipt.
    for statement in [
        "UPDATE kv SET expires=0 WHERE kind='file_blob' AND key=?",
        "DELETE FROM kv WHERE kind='file_blob' AND key=?",
    ] {
        sqlx::query(statement)
            .bind(&job.id)
            .execute(&g.store.pool)
            .await
            .unwrap();
        let unavailable = g
            .dispatch(
                &principal(&g),
                "operation_get",
                json!({"operation_id":job.id}),
            )
            .await
            .unwrap();
        assert!(unavailable.get("error").is_none(), "{unavailable}");
        assert_eq!(unavailable["sha256"], result["sha256"]);
        assert_eq!(unavailable["size"], result["size"]);
        assert_eq!(unavailable["operation_id"], job.id);
        assert_eq!(unavailable["download_available"], false);
        assert_eq!(
            unavailable["download_unavailable_reason"],
            "artifact_expired_or_removed"
        );
        assert!(unavailable.get("download_url").is_none());
        let (durable_after,): (String,) = sqlx::query_as("SELECT result FROM jobs WHERE id=?")
            .bind(&job.id)
            .fetch_one(&g.store.pool)
            .await
            .unwrap();
        assert_eq!(durable_after, durable_before);
    }
    let (links_after,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kv WHERE kind='file_link'")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(
        links_after, links_before,
        "unavailable artifacts must not mint links"
    );
    assert_eq!(
        std::fs::read(ws.root.join("binary-test.bin")).unwrap(),
        bytes
    );
    server.abort();
}

#[tokio::test]
async fn completed_export_remains_observable_without_blob_and_keeps_authorization() {
    let (_dir, g, _credential, _listener) = fixture().await;
    let export = job(&g, "file_download", json!({"path":"receipt.bin"})).await;
    let saved =
        json!({"state":"completed","artifact_id":export.id,"sha256":hash(b"receipt"),"size":7});
    sqlx::query("UPDATE jobs SET state='done',result=? WHERE id=?")
        .bind(saved.to_string())
        .bind(&export.id)
        .execute(&g.store.pool)
        .await
        .unwrap();
    let result = g
        .dispatch(
            &principal(&g),
            "operation_get",
            json!({"operation_id":export.id}),
        )
        .await
        .unwrap();
    assert_eq!(result["state"], "completed");
    assert_eq!(result["sha256"], saved["sha256"]);
    assert_eq!(result["download_available"], false);
    assert_eq!(
        result["recovery_action"],
        "export_again_with_expected_source_version"
    );
    assert!(result.get("download_url").is_none());
    let mut foreign = principal(&g);
    foreign.owner = "another-owner".into();
    assert!(
        g.dispatch(&foreign, "operation_get", json!({"operation_id":export.id}))
            .await
            .is_err()
    );
    let mut unprivileged = principal(&g);
    unprivileged.scopes.clear();
    assert!(
        g.dispatch(
            &unprivileged,
            "operation_get",
            json!({"operation_id":export.id})
        )
        .await
        .is_err()
    );
    let (jobs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert_eq!(
        jobs, 1,
        "observing a receipt must never re-export the source"
    );
}

#[tokio::test]
async fn gateway_rejects_wrong_device_bad_hash_and_oversized_upload() {
    let (_dir, g, credential, listener) = fixture().await;
    let job = job(&g, "file_download", json!({"path":"a.bin","max_bytes":10})).await;
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = g.router().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    let endpoint = format!("{origin}/device/files/{}", job.id);
    let send = |token: &str, size: usize, sha: &str| {
        client
            .post(&endpoint)
            .bearer_auth(token)
            .header("x-file-size", size)
            .header("x-file-sha256", sha)
            .body(b"abc".to_vec())
            .send()
    };
    assert_eq!(
        send("wrong", 3, &hash(b"abc")).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(&credential, 11, &hash(b"abc")).await.unwrap().status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        send(&credential, 3, &hash(b"bad")).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert!(
        !g.config
            .state_dir
            .join("file-objects")
            .join(format!("{}.blob", job.id))
            .exists()
    );
    assert_eq!(
        send(&credential, 3, &hash(b"abc")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&credential, 3, &hash(b"new")).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    server.abort();
}

#[tokio::test]
async fn source_url_is_not_in_durable_job_and_is_device_scoped() {
    let (_dir, g, credential, listener) = fixture().await;
    let device = &g.config.devices[0].id;
    g.store.put("online",device,&json!({"hello":{"session":random(),"version":"0.2.0","roots":[],"allow_write":true,"allow_exec":false},"last_seen":now()}),i64::MAX).await.unwrap();
    let args = json!({"workspace_id":format!("{device}:workspace"),"path":"a.bin","idempotency_key":"source","file":{"file_id":"file-synthetic","download_url":"https://files.oaiusercontent.com/f?signature=do-not-journal"}});
    let out = g
        .dispatch(&principal(&g), "file_upload", args.clone())
        .await
        .unwrap();
    let id = out["operation_id"].as_str().unwrap();
    let (stored,): (String,) = sqlx::query_as("SELECT request FROM jobs WHERE id=?")
        .bind(id)
        .fetch_one(&g.store.pool)
        .await
        .unwrap();
    assert!(!stored.contains("do-not-journal"));
    assert!(stored.contains("resolved_by_gateway"));
    sqlx::query("UPDATE jobs SET state='dispatched' WHERE id=?")
        .bind(id)
        .execute(&g.store.pool)
        .await
        .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = g.router().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    let path = format!("{origin}/device/file-source/{id}");
    assert_eq!(
        client
            .get(&path)
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let source: Value = client
        .get(&path)
        .bearer_auth(credential)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(source["download_url"], args["file"]["download_url"]);
    server.abort();
}

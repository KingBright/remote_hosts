#[tokio::test]
async fn r070_large_file_limit_requires_explicit_agent_negotiation() {
    let f = Fixture::new().await;
    let large = 65 * 1024 * 1024u64;
    remote_hosts_code::tools::validate(
        "file_download",
        &json!({"workspace_id":f.ws.id,"path":"data.bin","idempotency_key":"large-old","max_bytes":large}),
    )
    .unwrap();
    let error = f
        .g
        .dispatch(
            &f.principal(),
            "file_download",
            json!({"workspace_id":f.ws.id,"path":"data.bin","idempotency_key":"large-old","max_bytes":large}),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("device_feature_unavailable"));
    let mut online: Value =
        f.g.store
            .get("online", &f.ws.device_id)
            .await
            .unwrap()
            .unwrap();
    online["runtime_features"]["names"]
        .as_array_mut()
        .unwrap()
        .push(json!("large_file_transfer_v1"));
    online["hello"]["version"] = json!("0.7.0");
    online["hello"]["transfer_limits"] = json!({
        "protocol":1,
        "default_max_bytes":64 * 1024 * 1024u64,
        "hard_max_bytes":256 * 1024 * 1024u64,
        "checkpoint_bytes":CHUNK as u64
    });
    online["last_seen"] = json!(now());
    f.g.store
        .put("online", &f.ws.device_id, &online, i64::MAX)
        .await
        .unwrap();
    let accepted = f
        .g
        .dispatch(
            &f.principal(),
            "file_download",
            json!({"workspace_id":f.ws.id,"path":"data.bin","idempotency_key":"large-new","max_bytes":large}),
        )
        .await
        .unwrap();
    assert_eq!(accepted["pending"], true);
    assert!(accepted["operation_id"].as_str().is_some());
    assert!(remote_hosts_code::tools::validate(
        "file_download",
        &json!({"workspace_id":f.ws.id,"path":"data.bin","idempotency_key":"too-large","max_bytes":256 * 1024 * 1024u64 + 1}),
    )
    .is_err());
}

#[tokio::test]
async fn r070_source_authorization_state_is_observable_and_structured() {
    let f = Fixture::new().await;
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"stable-file","download_url":"resolved_by_gateway"}}),
        )
        .await;
    f.g.store
        .put(
            "file_source",
            &j.id,
            &json!({"download_url":"https://files.oaiusercontent.com/test"}),
            now() + 30,
        )
        .await
        .unwrap();
    let available =
        f.g.dispatch(
            &f.principal(),
            "operation_get",
            json!({"operation_id":j.id}),
        )
        .await
        .unwrap();
    assert_eq!(available["source_authorization"]["state"], "available");
    assert!(
        available["source_authorization"]["expires_at"]
            .as_i64()
            .is_some()
    );
    sqlx::query("UPDATE kv SET expires=? WHERE kind='file_source' AND key=?")
        .bind(now() - 1)
        .bind(&j.id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    let expired =
        f.g.dispatch(
            &f.principal(),
            "operation_get",
            json!({"operation_id":j.id}),
        )
        .await
        .unwrap();
    assert_eq!(expired["source_authorization"]["state"], "expired");
    assert_eq!(
        expired["source_authorization"]["error_code"],
        "source_authorization_required"
    );
    assert_eq!(
        expired["source_authorization"]["next_action"],
        "transfer_resume_with_refreshed_file_authorization"
    );
    let response = f
        .request(
            &f.g,
            &format!("/device/file-source/{}", j.id),
            "GET",
            vec![],
            &[],
        )
        .await;
    assert_eq!(response.status(), StatusCode::GONE);
    let status = value(response).await;
    assert_ne!(status["state"], "available");
    assert_eq!(status["error_code"], "source_authorization_required");
    assert_eq!(status["refresh_supported"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r071_transient_receiver_5xx_retries_the_same_durable_chunk() {
    let mut f = Fixture::new().await;
    let bytes: Vec<u8> = (0..CHUNK + 777).map(|i| (i % 241) as u8).collect();
    std::fs::write(f.ws.root.join("data.bin"), &bytes).unwrap();
    let j = f
        .job("file_download", json!({"expected_version":hash(&bytes)}))
        .await;
    #[derive(Default)]
    struct RetryProbe {
        first_requests: std::sync::atomic::AtomicUsize,
        final_requests: std::sync::atomic::AtomicUsize,
        injected: tokio::sync::Notify,
    }
    let probe = Arc::new(RetryProbe::default());
    async fn fail_final_chunk_once(
        State(probe): State<Arc<RetryProbe>>,
        request: AxumRequest,
        next: Next,
    ) -> Response {
        if request.uri().path().ends_with("/chunk") {
            let offset = request
                .headers()
                .get("x-transfer-offset")
                .and_then(|v| v.to_str().ok());
            if offset == Some("0") {
                probe.first_requests.fetch_add(1, Ordering::SeqCst);
            }
            if offset == Some("4194304") && probe.final_requests.fetch_add(1, Ordering::SeqCst) == 0
            {
                probe.injected.notify_one();
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
        next.run(request).await
    }
    let router = f.g.router().unwrap().layer(middleware::from_fn_with_state(
        probe.clone(),
        fail_final_chunk_once,
    ));
    let server = f.serve(Some(router));
    let agent = f.agent.clone();
    let work = j.clone();
    let mut sender = tokio::spawn(async move { agent.execute(&work).await });
    // Snapshotting and its first durable write are fixture preparation. Start
    // the existing retry watchdog only when the server actually injects 5xx.
    let injected = tokio::time::timeout(Duration::from_secs(30), probe.injected.notified()).await;
    if injected.is_err() {
        sender.abort();
        server.abort();
    }
    injected.expect("export did not reach the receiver fault-injection boundary");
    let retried = tokio::time::timeout(Duration::from_secs(30), &mut sender).await;
    if retried.is_err() {
        sender.abort();
    }
    server.abort();
    let result = retried
        .expect("sender retry timed out after injected 5xx")
        .expect("sender task failed")
        .expect("sender treated transient 5xx as permanent");
    assert_eq!(result["state"], "completed");
    assert_eq!(result["sha256"], hash(&bytes));
    assert_eq!(result["size"], bytes.len());
    assert_eq!(
        probe.first_requests.load(Ordering::SeqCst),
        1,
        "retry must not resend the acknowledged first chunk"
    );
    assert!(
        probe.final_requests.load(Ordering::SeqCst) >= 2,
        "the failed final chunk must really be retried"
    );
    assert_eq!(
        std::fs::read(
            f.g.config
                .state_dir
                .join("file-objects")
                .join(format!("{}.blob", j.id))
        )
        .unwrap(),
        bytes
    );
}

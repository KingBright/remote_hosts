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
    let mut online: Value = f
        .g
        .store
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
    let available = f
        .g
        .dispatch(&f.principal(), "operation_get", json!({"operation_id":j.id}))
        .await
        .unwrap();
    assert_eq!(available["source_authorization"]["state"], "available");
    assert!(available["source_authorization"]["expires_at"].as_i64().is_some());
    sqlx::query("UPDATE kv SET expires=? WHERE kind='file_source' AND key=?")
        .bind(now() - 1)
        .bind(&j.id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    let expired = f
        .g
        .dispatch(&f.principal(), "operation_get", json!({"operation_id":j.id}))
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

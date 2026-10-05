// Source initialization and observation use only temporary state/synthetic grants.
#[tokio::test]
async fn source_diagnostics_address_rejection_preserves_original_operation_and_target() {
    let mut f = Fixture::new().await;
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}}),
        )
        .await;
    let source = format!(
        "{}/files/private-source-path?signature=synthetic-secret",
        f.g.config.public_url
    );
    f.g.store
        .put(
            "file_source",
            &j.id,
            &json!({"download_url":source}),
            now() + 900,
        )
        .await
        .unwrap();
    let server = f.serve(None);
    let result = f.agent.execute(&j).await.unwrap();
    assert_eq!(result["state"], "paused");
    assert_eq!(result["confirmed_bytes"], 0);
    assert_eq!(
        result["diagnostic"]["code"],
        "source_address_policy_rejected"
    );
    assert_eq!(result["diagnostic"]["source_host"], "127.0.0.1");
    assert_eq!(result["diagnostic"]["http_request_started"], false);
    assert_eq!(result["next_action"], "diagnose_original_source");
    assert!(!f.ws.root.join("data.bin").exists());
    let observed =
        f.g.dispatch(
            &f.principal(),
            "operation_get",
            json!({"operation_id":j.id}),
        )
        .await
        .unwrap();
    assert_eq!(observed["operation_id"], j.id);
    assert_eq!(observed["source_authorization"]["state"], "available");
    assert_eq!(
        observed["receipt"]["next_action"],
        "diagnose_original_source"
    );
    assert_eq!(
        observed["receipt"]["failure_boundary"],
        "source_address_policy"
    );
    assert_eq!(
        observed["receipt"]["retry_policy"],
        "resume_original_only_after_connection_and_source_authorization_confirmed"
    );
    assert!(observed["receipt"]["evidence_complete"] == false);
    let text = observed.to_string();
    for secret in [
        "private-source-path",
        "signature",
        "synthetic-secret",
        &f.token,
    ] {
        assert!(!text.contains(secret));
    }
    let record: Value = f
        .agent
        .store
        .get("local_operation", &j.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record["resumable"], true);
    assert!(record["result"].is_null());
    server.abort();
}

#[tokio::test]
async fn source_diagnostics_paused_observation_tracks_source_expiry_without_replay() {
    let f = Fixture::new().await;
    let j = f
        .job(
            "file_upload",
            json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}}),
        )
        .await;
    let source = json!({"download_url":"https://files.oaiusercontent.com/private-path?signature=synthetic-secret"});
    let result = json!({"state":"paused","resumable":true,"pending":false,"confirmed_bytes":0,"next_action":"diagnose_original_source"});
    sqlx::query("UPDATE jobs SET state='paused',result=? WHERE id=?")
        .bind(result.to_string())
        .bind(&j.id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    for (expires, state) in [(now() + 900, "available"), (now() - 1, "expired")] {
        f.g.store
            .put("file_source", &j.id, &source, expires)
            .await
            .unwrap();
        let observed =
            f.g.dispatch(
                &f.principal(),
                "operation_get",
                json!({"operation_id":j.id}),
            )
            .await
            .unwrap();
        assert_eq!(observed["source_authorization"]["state"], state);
        assert_eq!(
            observed["source_authorization"]["source_endpoint"]["host"],
            "files.oaiusercontent.com"
        );
        assert_eq!(
            observed["source_authorization"]["source_endpoint"]["port"],
            443
        );
        assert_eq!(observed["operation_id"], j.id);
        assert!(!observed.to_string().contains("synthetic-secret"));
    }
    sqlx::query("DELETE FROM kv WHERE kind='file_source' AND key=?")
        .bind(&j.id)
        .execute(&f.g.store.pool)
        .await
        .unwrap();
    let observed =
        f.g.dispatch(
            &f.principal(),
            "operation_get",
            json!({"operation_id":j.id}),
        )
        .await
        .unwrap();
    assert_eq!(observed["source_authorization"]["state"], "required");
    assert!(observed["source_authorization"]["source_endpoint"].is_null());
    let mut other = f.principal();
    other.owner = "other".into();
    assert!(
        f.g.dispatch(&other, "operation_get", json!({"operation_id":j.id}))
            .await
            .is_err()
    );
    let mut revoked = f.principal();
    revoked.scopes.retain(|s| s != "code:write");
    assert!(
        f.g.dispatch(&revoked, "operation_get", json!({"operation_id":j.id}))
            .await
            .is_err()
    );
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM jobs")
        .fetch_one(&f.g.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(!f.ws.root.join("data.bin").exists());
}

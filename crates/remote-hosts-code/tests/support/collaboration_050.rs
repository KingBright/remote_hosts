// Isolated failure and integration coverage for the 0.5 collaboration release.
#[tokio::test]
async fn r050_drain_stops_new_writes_but_keeps_controls_and_existing_results() {
    let f=Fixture::new().await;
    let j=f.job("code_apply_edits",json!({"files":[]})).await;
    sqlx::query("UPDATE jobs SET state='queued' WHERE id=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    let lease=hash("lease");
    assert_eq!(f.json("/device/maintenance",json!({"action":"acquire","lease_id":lease})).await.status(),StatusCode::OK);
    assert!(f.g.dispatch(&f.principal(),"terminal_exec",json!({"workspace_id":f.ws.id,"command":"echo blocked","idempotency_key":"new"})).await.unwrap_err().to_string().contains("device_draining"));
    let session=f.g.store.get::<Value>("online",&f.ws.device_id).await.unwrap().unwrap()["hello"]["session"].clone();
    let hello=json!({"version":"0.5.0","session":session,"roots":[f.ws.root],"allow_write":true,"allow_exec":true,"lanes":["write"],"poll_wait_ms":100});
    let v=value(f.json("/device/poll",hello.clone()).await).await;assert!(v["job"].is_null());
    let read=f.job("terminal_read",json!({"terminal_id":uuid::Uuid::new_v4().to_string()})).await;
    sqlx::query("UPDATE jobs SET state='queued' WHERE id=?").bind(&read.id).execute(&f.g.store.pool).await.unwrap();
    let mut controls=hello.clone();controls["lanes"]=json!(["control"]);
    assert_eq!(value(f.json("/device/poll",controls).await).await["job"]["id"],read.id);
    assert_eq!(f.json("/device/maintenance",json!({"action":"release","lease_id":hash("wrong")})).await.status(),StatusCode::CONFLICT);
    assert_eq!(f.json("/device/maintenance",json!({"action":"release","lease_id":lease})).await.status(),StatusCode::OK);
    assert_eq!(value(f.json("/device/poll",hello).await).await["job"]["id"],j.id);
}
#[tokio::test]
async fn r050_terminal_completion_can_be_observed_without_read_job() {
    let f=Fixture::new().await;let j=f.job("terminal_exec",json!({"command":"true"})).await;
    let session=f.g.store.get::<Value>("online",&f.ws.device_id).await.unwrap().unwrap()["hello"]["session"].clone();
    let status=json!({"id":j.id,"workspace_id":f.ws.id,"state":"exited","exit_code":7,"output_truncated":false,"created_at":now(),"output_complete":true});
    let hello=json!({"version":"0.5.0","session":session,"roots":[f.ws.root],"allow_write":true,"allow_exec":true,"lanes":["control"],"poll_wait_ms":100,"terminal_updates":[status]});
    assert_eq!(f.json("/device/poll",hello.clone()).await.status(),StatusCode::OK);
    assert_eq!(f.json("/device/result",json!({"operation_id":j.id,"result":{"terminal_id":j.id,"state":"running"}})).await.status(),StatusCode::OK);
    let before:(i64,)=sqlx::query_as("SELECT COUNT(*) FROM jobs").fetch_one(&f.g.store.pool).await.unwrap();
    let v=f.g.dispatch(&f.principal(),"operation_get",json!({"operation_id":j.id})).await.unwrap();
    assert_eq!(v["terminal_observation"]["terminal"]["exit_code"],7);
    assert_eq!(v["terminal_observation"]["terminal"]["output_complete"],true);
    let after:(i64,)=sqlx::query_as("SELECT COUNT(*) FROM jobs").fetch_one(&f.g.store.pool).await.unwrap();assert_eq!(before,after);
    let mut wrong=hello;wrong["terminal_updates"][0]["workspace_id"]=json!("another");wrong["terminal_updates"][0]["exit_code"]=json!(0);
    f.json("/device/poll",wrong).await;
    let v=f.g.dispatch(&f.principal(),"operation_get",json!({"operation_id":j.id})).await.unwrap();assert_eq!(v["terminal_observation"]["terminal"]["exit_code"],7);
}
#[tokio::test]
async fn r050_workspace_events_are_bounded_and_do_not_record_observers() {
    let f=Fixture::new().await;
    let args=json!({"workspace_id":f.ws.id,"limit":50});
    let original=Job{id:uuid::Uuid::new_v4().to_string(),device_id:f.ws.device_id.clone(),owner:"fixture".into(),tool:"workspace_context".into(),arguments:args.clone()};
    let first=f.agent.execute(&original).await.unwrap();let cursor=first["events"]["cursor"].as_str().unwrap().to_owned();
    let n=20;
    for i in 0..n {f.agent.store.put("terminal",&format!("event-{i}"),&json!({"workspace_id":f.ws.id,"id":format!("event-{i}"),"state":"exited","created_at":now(),"exit_code":0}),i64::MAX).await.unwrap();}
    let mut q=original.clone();q.id=uuid::Uuid::new_v4().to_string();q.arguments["after_event"]=json!(cursor);
    let next=f.agent.execute(&q).await.unwrap();assert_eq!(next["events"]["items"].as_array().unwrap().len(),n);assert_eq!(next["events"]["has_more"],false);
    q.id=uuid::Uuid::new_v4().to_string();q.arguments["after_event"]=next["events"]["cursor"].clone();
    assert!(f.agent.execute(&q).await.unwrap()["events"]["items"].as_array().unwrap().is_empty());
    // Exercise the real retention triggers on one terminal instead of mutating
    // the removed floor table or retaining thousands of synthetic terminal rows.
    const RETENTION: i64 = 4096;
    for i in 0..RETENTION {
        f.agent.store.put(
            "terminal",
            "retention-probe",
            &json!({"workspace_id":f.ws.id,"id":"retention-probe","state":if i % 2 == 0 {"running"} else {"exited"},"created_at":now(),"exit_code":0}),
            i64::MAX,
        ).await.unwrap();
    }
    let (retained,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM work_events WHERE workspace=?")
        .bind(&f.ws.id).fetch_one(&f.agent.store.pool).await.unwrap();
    assert_eq!(retained, RETENTION);
    let (floor_tables,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='work_event_floor'")
        .fetch_one(&f.agent.store.pool).await.unwrap();
    assert_eq!(floor_tables, 0, "retention must not recreate the obsolete floor table");
    q.id=uuid::Uuid::new_v4().to_string();q.arguments["after_event"]=json!(cursor);
    assert_eq!(f.agent.execute(&q).await.unwrap()["error_code"],"invalid_or_expired_cursor");
    // A fresh snapshot recovers from expiration, and observing it adds no events.
    q.id = uuid::Uuid::new_v4().to_string();
    q.arguments.as_object_mut().unwrap().remove("after_event");
    let fresh = f.agent.execute(&q).await.unwrap();
    assert!(fresh.get("error").is_none(), "{fresh}");
    assert_eq!(fresh["events"]["items"].as_array().unwrap().len(), 50);
    q.id = uuid::Uuid::new_v4().to_string();
    q.arguments["after_event"] = fresh["events"]["cursor"].clone();
    assert!(f.agent.execute(&q).await.unwrap()["events"]["items"].as_array().unwrap().is_empty());
}
#[tokio::test]
async fn r050_multi_file_partial_edit_has_committed_and_pending_evidence() {
    let f=Fixture::new().await;let id=uuid::Uuid::new_v4().to_string();
    let args=json!({"workspace_id":f.ws.id,"idempotency_key":"partial","files":[{"path":"made-parent","action":"create","expected_version":"absent","content":"keep"},{"path":"made-parent/child","action":"create","expected_version":"absent","content":"not published"}]});
    let result=f.agent.execute(&Job{id:id.clone(),device_id:f.ws.device_id.clone(),owner:"fixture".into(),tool:"code_apply_edits".into(),arguments:args}).await.unwrap();
    assert_eq!(result["error"],"partial_edit");assert_eq!(result["changed"].as_array().unwrap().len(),1);assert_eq!(result["pending"],json!(["made-parent/child"]));assert_eq!(result["journal_id"],id);
    let journal:Value=serde_json::from_slice(&std::fs::read(f.agent.config.state_dir.join("edits").join(format!("{id}.json"))).unwrap()).unwrap();
    assert_eq!(journal["status"],"partial");assert_eq!(journal["files"][0]["status"],"applied");assert_eq!(std::fs::read(f.ws.root.join("made-parent")).unwrap(),b"keep");
}
#[tokio::test]
async fn r050_maintenance_receipts_strip_unrelated_payloads() {
    let f=Fixture::new().await;let lease=hash("receipt");
    assert_eq!(f.json("/device/maintenance",json!({"action":"acquire","lease_id":lease})).await.status(),StatusCode::OK);
    let r=json!({"action":"report","lease_id":lease,"receipt":{"version":"0.5.0","state":"failed","phase":"waiting_idle","error":"agent still has active work","password":"do-not-persist","command":"do-not-persist"}});
    assert_eq!(f.json("/device/maintenance",r).await.status(),StatusCode::OK);
    let v=f.g.store.get::<Value>("device_update",&f.ws.device_id).await.unwrap().unwrap();assert!(!v.to_string().contains("do-not-persist"));assert_eq!(v["receipt"]["error_code"],"device_busy");
}
#[tokio::test]
async fn r050_sync_manifest_cannot_escape_scope_or_alias_destinations() {
    let f=Fixture::new().await;
    for names in [["a//b","a/b"],["a/./b","a/b"],["a/../b","b"]] {
        let args=json!({"workspace_id":f.ws.id,"idempotency_key":random(),"mode":"plan","files":names.map(|p|json!({"path":p,"sha256":hash(b"x"),"size":1}))});
        let v=f.agent.execute(&Job{id:uuid::Uuid::new_v4().to_string(),device_id:f.ws.device_id.clone(),owner:"fixture".into(),tool:"files_sync".into(),arguments:args}).await.unwrap();assert!(v.get("error").is_some());
    }
    assert!(std::fs::read_dir(&f.ws.root).unwrap().next().is_none());
}

#[tokio::test]
async fn unacknowledged_gateway_orphan_is_recovered_from_offset_zero() {
    let f=Fixture::new().await;let j=f.job("file_download",json!({})).await;
    let directory=f.g.config.state_dir.join("file-objects");std::fs::create_dir_all(&directory).unwrap();
    let path=directory.join(format!("{}.part040",j.id));std::fs::write(&path,b"uncommitted").unwrap();
    let response=f.json(&format!("/device/transfers/{}",j.id),json!({"size":5,"sha256":hash(b"fresh")})).await;
    assert_eq!(response.status(),StatusCode::OK);assert_eq!(value(response).await["confirmed_bytes"],0);
    assert_eq!(std::fs::read(path).unwrap(),vec![0;5]);
}
#[tokio::test]
async fn duplicate_finalize_does_not_extend_artifact_authorization() {
    let f=Fixture::new().await;let j=f.job("file_download",json!({})).await;let base=format!("/device/transfers/{}",j.id);
    assert_eq!(f.json(&base,json!({"size":0,"sha256":hash([])})).await.status(),StatusCode::OK);
    assert_eq!(f.request(&f.g,&format!("{base}/complete"),"POST",vec![],&[]).await.status(),StatusCode::OK);
    let expires=now()+20;
    sqlx::query("UPDATE kv SET value=json_set(value,'$.expires',?) WHERE kind='file_blob' AND key=?").bind(expires).bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    assert_eq!(f.request(&f.g,&format!("{base}/complete"),"POST",vec![],&[]).await.status(),StatusCode::OK);
    let blob:Value=f.g.store.get("file_blob",&j.id).await.unwrap().unwrap();assert_eq!(blob["expires"],expires);
    sqlx::query("UPDATE kv SET value=json_set(value,'$.expires',0) WHERE kind='file_blob' AND key=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    assert_eq!(f.request(&f.g,&format!("{base}/complete"),"POST",vec![],&[]).await.status(),StatusCode::CONFLICT);
}
#[tokio::test]
async fn stale_result_cannot_overwrite_resumed_attempt_and_cancel_changes_cursor() {
    let f=Fixture::new().await;let j=f.job("file_upload",json!({"file":{"file_id":"same","download_url":"resolved_by_gateway"}})).await;
    sqlx::query("UPDATE jobs SET state='paused' WHERE id=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    let resumed=f.g.dispatch(&f.principal(),"transfer_resume",json!({"operation_id":j.id,"idempotency_key":"resume"})).await.unwrap();assert_eq!(resumed["transfer_revision"],1);
    let stale=f.json("/device/result",json!({"operation_id":j.id,"result":{"state":"completed","transfer_revision":0}})).await;
    assert_eq!(value(stale).await["obsolete"],true);
    let before=f.g.dispatch(&f.principal(),"operation_get",json!({"operation_ids":[j.id]})).await.unwrap();
    f.g.dispatch(&f.principal(),"transfer_cancel",json!({"operation_id":j.id,"idempotency_key":"cancel"})).await.unwrap();
    let after=f.g.dispatch(&f.principal(),"operation_get",json!({"operation_ids":[j.id],"cursor":before["observation"]["cursor"],"wait_ms":500})).await.unwrap();
    assert_eq!(after["observation"]["changed"],true);
    let accepted=f.json("/device/result",json!({"operation_id":j.id,"result":{"state":"cancelled","transfer_revision":1,"cleanup_complete":true}})).await;
    assert_eq!(value(accepted).await["accepted"],true);
    assert_eq!(f.g.dispatch(&f.principal(),"operation_get",json!({"operation_id":j.id})).await.unwrap()["state"],"cancelled");
}
#[tokio::test]
async fn controls_require_original_scope_and_do_not_cancel_shell_jobs() {
    let f=Fixture::new().await;let mut p=f.principal();p.scopes=vec!["code:read".into()];
    let j=f.job("file_download",json!({})).await;
    assert!(f.g.dispatch(&p,"transfer_cancel",json!({"operation_id":j.id,"idempotency_key":"x"})).await.is_err());
    let j=f.job("terminal_exec",json!({"command":"true"})).await;
    assert!(f.g.dispatch(&f.principal(),"transfer_cancel",json!({"operation_id":j.id,"idempotency_key":"x"})).await.is_err());
}
#[test]
fn outbound_os_child() {
    let Some(path)=std::env::var_os("RH040_EXPORT_FIXTURE") else{return;};
    let v:Value=serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async{
        let config:AgentConfig=serde_json::from_value(v["config"].clone()).unwrap();
        let mut agent=Agent::new(config).await.unwrap();Arc::make_mut(&mut agent.config).gateway_url=v["origin"].as_str().unwrap().into();
        let job:Job=serde_json::from_value(v["job"].clone()).unwrap();
        let result=agent.execute(&job).await.unwrap();assert_eq!(result["state"],"completed","{result}");
    });
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn export_recovers_after_killed_sender_and_gateway_restart() {
    let mut f=Fixture::new().await;let bytes:Vec<u8>=(0..CHUNK+777).map(|i|(i%241)as u8).collect();
    std::fs::write(f.ws.root.join("data.bin"),&bytes).unwrap();let j=f.job("file_download",json!({"expected_version":hash(&bytes)})).await;
    async fn hold_second(request:AxumRequest,next:Next)->Response {
        if request.uri().path().ends_with("/chunk") && request.headers().get("x-transfer-offset").and_then(|v|v.to_str().ok())==Some("4194304") { std::future::pending::<()>().await; }
        next.run(request).await
    }
    let server=f.serve(Some(f.g.router().unwrap().layer(middleware::from_fn(hold_second))));
    let spec=f._dir.path().join("export-child.json");let mut config=(*f.agent.config).clone();let origin=config.gateway_url.clone();config.gateway_url=config.gateway_url.replacen("http:","https:",1);
    std::fs::write(&spec,json!({"config":config,"origin":origin,"job":j}).to_string()).unwrap();
    let mut child=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","outbound_os_child","--nocapture"]).env("RH040_EXPORT_FIXTURE",&spec)
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
    let reached=tokio::time::timeout(Duration::from_secs(20),async{
        loop {if f.g.store.get::<Value>("transfer_receiver",&j.id).await.unwrap().is_some_and(|v|v["offset"]==CHUNK){break;}
            assert!(child.try_wait().unwrap().is_none(),"sender exited before checkpoint");tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await;
    let _=child.kill();let _=child.wait();assert!(reached.is_ok());server.abort();let _=server.await;
    let url=reqwest::Url::parse(&origin).unwrap();let listener=tokio::net::TcpListener::bind(format!("{}:{}",url.host_str().unwrap(),url.port().unwrap())).await.unwrap();
    let gateway=Gateway::new((*f.g.config).clone()).await.unwrap();
    let seen=Arc::new(std::sync::Mutex::new(Vec::<String>::new()));let observe=seen.clone();
    async fn record(State(seen):State<Arc<std::sync::Mutex<Vec<String>>>>,request:AxumRequest,next:Next)->Response {
        if request.uri().path().ends_with("/chunk") {seen.lock().unwrap().push(request.headers()["x-transfer-offset"].to_str().unwrap().into());}
        next.run(request).await
    }
    let router=gateway.router().unwrap().layer(middleware::from_fn_with_state(observe,record));
    let restarted=tokio::spawn(async move{axum::serve(listener,router).await.unwrap();});
    std::fs::write(f.ws.root.join("data.bin"),b"new user content after snapshot").unwrap();
    let status=tokio::task::spawn_blocking(move||std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","outbound_os_child","--nocapture"]).env("RH040_EXPORT_FIXTURE",&spec).output()).await.unwrap().unwrap();
    assert!(status.status.success(),"{}",String::from_utf8_lossy(&status.stderr));
    assert_eq!(seen.lock().unwrap().as_slice(),[CHUNK.to_string()]);
    assert_eq!(std::fs::read(f.g.config.state_dir.join("file-objects").join(format!("{}.blob",j.id))).unwrap(),bytes);
    assert_eq!(std::fs::read(f.ws.root.join("data.bin")).unwrap(),b"new user content after snapshot");
    restarted.abort();
}

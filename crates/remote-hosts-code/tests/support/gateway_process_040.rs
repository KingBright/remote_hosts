struct OwnedTestProcess(std::process::Child);
impl Drop for OwnedTestProcess { fn drop(&mut self) { let _=self.0.kill();let _=self.0.wait(); } }
#[test]
fn gateway_os_child() {
    let Some(path)=std::env::var_os("RH040_GATEWAY_FIXTURE") else {return;};
    let config:GatewayConfig=serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async{
        let listener=tokio::net::TcpListener::bind(&config.bind).await.unwrap();
        let g=Gateway::new(config).await.unwrap();axum::serve(listener,g.router().unwrap()).await.unwrap();
    });
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn gateway_durable_offset_survives_actual_server_process_kill() {
    let mut f=Fixture::new().await;let bytes:Vec<u8>=(0..CHUNK+71).map(|i|(i%239)as u8).collect();
    let j=f.job("file_download",json!({"expected_version":hash(&bytes)})).await;
    let origin=f.agent.config.gateway_url.clone();drop(f.listen.take());
    let spec=f._dir.path().join("gateway-child.json");std::fs::write(&spec,serde_json::to_vec(&*f.g.config).unwrap()).unwrap();
    let start=||OwnedTestProcess(std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","gateway_os_child","--nocapture"])
        .env("RH040_GATEWAY_FIXTURE",&spec).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap());
    let client=reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    async fn ready(c:&reqwest::Client,origin:&str) {
        tokio::time::timeout(Duration::from_secs(10),async{loop {
            if c.get(format!("{origin}/healthz")).send().await.is_ok_and(|r|r.status().is_success()){break;}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }}).await.unwrap();
    }
    let child=start();ready(&client,&origin).await;let endpoint=format!("{origin}/device/transfers/{}",j.id);
    let init=client.post(&endpoint).bearer_auth(&f.token).json(&json!({"size":bytes.len(),"sha256":hash(&bytes)})).send().await.unwrap();assert!(init.status().is_success());
    let first=client.post(format!("{endpoint}/chunk")).bearer_auth(&f.token).header("x-transfer-offset","0").header("x-chunk-sha256",hash(&bytes[..CHUNK])).body(bytes[..CHUNK].to_vec()).send().await.unwrap();
    assert_eq!(first.json::<Value>().await.unwrap()["confirmed_bytes"],CHUNK);
    drop(child);
    let _restarted=start();ready(&client,&origin).await;
    let checkpoint=client.get(&endpoint).bearer_auth(&f.token).send().await.unwrap().json::<Value>().await.unwrap();assert_eq!(checkpoint["confirmed_bytes"],CHUNK);
    let last=client.post(format!("{endpoint}/chunk")).bearer_auth(&f.token).header("x-transfer-offset",CHUNK.to_string()).header("x-chunk-sha256",hash(&bytes[CHUNK..])).body(bytes[CHUNK..].to_vec()).send().await.unwrap();assert!(last.status().is_success());
    let done=client.post(format!("{endpoint}/complete")).bearer_auth(&f.token).send().await.unwrap().json::<Value>().await.unwrap();assert_eq!(done["completed"],true);
    assert_eq!(std::fs::read(f.g.config.state_dir.join("file-objects").join(format!("{}.blob",j.id))).unwrap(),bytes);
}
#[tokio::test]
async fn old_agent_cannot_falsely_acknowledge_new_transfer_controls() {
    let f=Fixture::new().await;let j=f.job("file_download",json!({})).await;
    let mut online:Value=f.g.store.get("online",&f.ws.device_id).await.unwrap().unwrap();online.as_object_mut().unwrap().remove("runtime_features");f.g.store.put("online",&f.ws.device_id,&online,i64::MAX).await.unwrap();
    let err=f.g.dispatch(&f.principal(),"transfer_cancel",json!({"operation_id":j.id,"idempotency_key":"no-old-cancel"})).await.unwrap_err();
    assert!(err.to_string().contains("device_feature_unavailable"));assert!(f.g.store.get::<Value>("transfer_control",&j.id).await.unwrap().is_none());
}

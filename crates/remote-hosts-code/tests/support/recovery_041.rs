// Regression cases use only temporary storage and synthetic authorization.
#[tokio::test]
async fn r041_exact_resume_retry_refreshes_url_without_new_generation() {
    let f=Fixture::new().await;
    let j=f.job("file_upload",json!({"sha256":hash("same"),"file":{"file_id":"same","download_url":"resolved_by_gateway"}})).await;
    sqlx::query("UPDATE jobs SET state='awaiting_source' WHERE id=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    let mut args=json!({"operation_id":j.id,"idempotency_key":"resume-once","file":{"file_id":"same","download_url":"https://files.oaiusercontent.com/file?token=first"}});
    let first=f.g.dispatch(&f.principal(),"transfer_resume",args.clone()).await.unwrap();
    args["file"]["download_url"]=json!("https://files.oaiusercontent.com/file?token=refreshed");
    let second=f.g.dispatch(&f.principal(),"transfer_resume",args.clone()).await.unwrap();
    assert_eq!(first,second);
    let source:Value=f.g.store.get("file_source",&j.id).await.unwrap().unwrap();
    assert_eq!(source["download_url"],args["file"]["download_url"]);
    let c:Value=f.g.store.get("transfer_control",&j.id).await.unwrap().unwrap();assert_eq!(c["revision"],1);
    let action:Vec<Value>=f.g.store.list("transfer_action").await.unwrap();assert_eq!(action.len(),1);
    assert!(!serde_json::to_string(&action).unwrap().contains("token="));
}
#[tokio::test]
async fn r041_old_resume_retry_never_overwrites_newer_authorization() {
    let f=Fixture::new().await;
    let j=f.job("file_upload",json!({"sha256":hash("same"),"file":{"file_id":"same","download_url":"resolved_by_gateway"}})).await;
    let old=json!({"operation_id":j.id,"idempotency_key":"old","file":{"file_id":"same","download_url":"https://files.oaiusercontent.com/file?token=old"}});
    let mut new=old.clone();new["idempotency_key"]=json!("new");new["file"]["download_url"]=json!("https://files.oaiusercontent.com/file?token=new");
    for args in [&old,&new] {
        sqlx::query("UPDATE jobs SET state='awaiting_source' WHERE id=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
        f.g.dispatch(&f.principal(),"transfer_resume",args.clone()).await.unwrap();
    }
    f.g.dispatch(&f.principal(),"transfer_resume",old).await.unwrap();
    assert_eq!(f.g.store.get::<Value>("file_source",&j.id).await.unwrap().unwrap()["download_url"],new["file"]["download_url"]);
}
#[tokio::test]
async fn r041_future_receipt_is_rejected_not_acknowledged_obsolete() {
    let f=Fixture::new().await;let j=f.job("file_upload",json!({})).await;
    let response=f.json("/device/result",json!({"operation_id":j.id,"result":{"state":"completed","transfer_revision":99}})).await;
    assert_eq!(response.status(),StatusCode::CONFLICT);
    let (state,result):(String,Option<String>)=sqlx::query_as("SELECT state,result FROM jobs WHERE id=?").bind(&j.id).fetch_one(&f.g.store.pool).await.unwrap();
    assert_eq!(state,"dispatched");assert!(result.is_none());
}
#[tokio::test]
async fn r041_confirmation_alone_advances_observation_cursor() {
    let f=Fixture::new().await;let j=f.job("file_upload",json!({})).await;
    let mut p=json!({"snapshot":{"operation_id":j.id,"phase":"transferring","bytes_done":100,"confirmed_bytes":0,"total_bytes":200},"reported_at":now()});
    f.g.store.put("operation_progress",&j.id,&p,now()+60).await.unwrap();
    let args=json!({"operation_ids":[j.id]});let first=f.g.dispatch(&f.principal(),"operation_get",args.clone()).await.unwrap();
    p["snapshot"]["confirmed_bytes"]=json!(100);f.g.store.put("operation_progress",&j.id,&p,now()+60).await.unwrap();
    let next=f.g.dispatch(&f.principal(),"operation_get",args).await.unwrap();
    assert_ne!(first["observation"]["cursor"],next["observation"]["cursor"]);
}
#[tokio::test]
async fn r041_cancelled_task_never_reports_completed_progress() {
    let mut f=Fixture::new().await;let j=f.job("file_upload",json!({"file":{"file_id":"x","download_url":"resolved_by_gateway"}})).await;
    f.g.dispatch(&f.principal(),"transfer_cancel",json!({"operation_id":j.id,"idempotency_key":"cancel"})).await.unwrap();
    let server=f.serve(None);let result=f.agent.execute(&j).await.unwrap();server.abort();
    assert_eq!(result["state"],"cancelled");assert_eq!(result["progress"]["phase"],"cancelled");
    assert_eq!(result["cleanup_complete"],true);
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn r041_concurrent_duplicate_controls_have_one_durable_effect() {
    let f=Fixture::new().await;let j=f.job("file_download",json!({})).await;
    sqlx::query("UPDATE jobs SET state='paused' WHERE id=?").bind(&j.id).execute(&f.g.store.pool).await.unwrap();
    let mut tasks=tokio::task::JoinSet::new();let barrier=Arc::new(tokio::sync::Barrier::new(8));
    for _ in 0..8 {let g=f.g.clone();let p=f.principal();let id=j.id.clone();let b=barrier.clone();tasks.spawn(async move{
        b.wait().await;g.dispatch(&p,"transfer_resume",json!({"operation_id":id,"idempotency_key":"one"})).await
    });}
    let mut results=Vec::new();while let Some(v)=tasks.join_next().await{results.push(v.unwrap().expect("concurrent retry must not fail with SQLITE_BUSY"));}
    assert!(results.iter().all(|v|v==&results[0]));assert_eq!(results[0]["transfer_revision"],1);
}
#[tokio::test]
async fn r041_workspace_terminal_pages_and_scope_are_explicit() {
    let f=Fixture::new().await;
    for (id,created,state) in [("a",10,"running"),("b",30,"exited"),("c",20,"running"),("d",40,"exited"),("e",40,"exited")] {
        f.agent.store.put("terminal",id,&json!({"id":id,"workspace_id":f.ws.id,"state":state,"created_at":created,"output_complete":true}),i64::MAX).await.unwrap();
    }
    async fn context(f:&Fixture,args:Value)->Value {f.agent.execute(&Job{id:uuid::Uuid::new_v4().to_string(),device_id:f.ws.device_id.clone(),owner:"fixture".into(),tool:"workspace_context".into(),arguments:args}).await.unwrap()}
    let mut args=json!({"workspace_id":f.ws.id,"limit":2});let mut found=Vec::new();
    for _ in 0..4 {
        let v=context(&f,args.clone()).await;assert!(v.get("error").is_none(),"{v}");
        found.extend(v["terminals"].as_array().unwrap().iter().map(|t|t["id"].as_str().unwrap().to_owned()));
        if v["next_terminal_cursor"].is_null(){break;}
        args["terminal_cursor"]=v["next_terminal_cursor"].clone();
    }
    assert_eq!(found,["c","a","d","e","b"]);
    let active=context(&f,json!({"workspace_id":f.ws.id,"limit":50,"active_only":true})).await;
    assert_eq!(active["terminals"].as_array().unwrap().len(),2);assert_eq!(active["summary"]["active_terminals"],2);
    let first=context(&f,json!({"workspace_id":f.ws.id,"limit":2})).await;
    let mut wrong=json!({"workspace_id":f.ws.id,"limit":2,"active_only":true,"terminal_cursor":first["next_terminal_cursor"]});
    assert!(context(&f,wrong.clone()).await.get("error").is_some());
    wrong["active_only"]=json!(false);wrong["terminal_cursor"]=json!("invalid-cursor");assert!(context(&f,wrong).await.get("error").is_some());
}
#[tokio::test]
async fn r041_if_range_mismatch_returns_full_body_and_suffix_range_works() {
    let f=Fixture::new().await;let data=b"0123456789";let j=f.job("file_download",json!({})).await;
    let base=format!("/device/transfers/{}",j.id);
    assert_eq!(f.json(&base,json!({"size":10,"sha256":hash(data)})).await.status(),StatusCode::OK);
    assert_eq!(f.request(&f.g,&format!("{base}/chunk"),"POST",data.to_vec(),&[("x-transfer-offset","0".into()),("x-chunk-sha256",hash(data))]).await.status(),StatusCode::OK);
    assert_eq!(f.request(&f.g,&format!("{base}/complete"),"POST",vec![],&[]).await.status(),StatusCode::OK);
    assert_eq!(f.json("/device/result",json!({"operation_id":j.id,"result":{"state":"completed","sha256":hash(data),"size":10,"transfer_revision":0}})).await.status(),StatusCode::OK);
    let result=f.g.dispatch(&f.principal(),"operation_get",json!({"operation_id":j.id})).await.unwrap();let url=reqwest::Url::parse(result["download_url"].as_str().unwrap()).unwrap();
    let r=f.request(&f.g,url.path(),"GET",vec![],&[("range","bytes=3-".into()),("if-range","\"old\"".into())]).await;
    assert_eq!(r.status(),StatusCode::OK);assert_eq!(&to_bytes(r.into_body(),100).await.unwrap()[..],data);
    let r=f.request(&f.g,url.path(),"GET",vec![],&[("range","bytes=-4".into())]).await;
    assert_eq!(r.status(),StatusCode::PARTIAL_CONTENT);assert_eq!(&to_bytes(r.into_body(),100).await.unwrap()[..],b"6789");
}

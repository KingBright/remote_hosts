#[cfg(unix)]
#[tokio::test]
async fn cleanup_checks_publication_inode_and_preserves_replacement_file() {
    use std::os::unix::fs::MetadataExt;
    let (_d,c,ws,_job,_store,mut j)=fixture().await;
    let name=format!(".remote-hosts-transfer-{}.tmp",crate::random());let path=ws.root.join(&name);std::fs::write(&path,b"owned").unwrap();
    let meta=std::fs::metadata(&path).unwrap();j.publication_temp=Some(name);j.publication_identity=Some((meta.dev(),meta.ino()));
    // Keep original inode allocated to avoid inode-reuse ambiguity in the test.
    std::fs::rename(&path,ws.root.join("kept-original")).unwrap();std::fs::write(&path,b"user replacement").unwrap();
    assert!(clean_publication(&c,&j).is_err());assert_eq!(std::fs::read(&path).unwrap(),b"user replacement");
    let meta=std::fs::metadata(&path).unwrap();j.publication_identity=Some((meta.dev(),meta.ino()));clean_publication(&c,&j).unwrap();assert!(!path.exists());
}
#[tokio::test]
async fn completed_private_files_still_count_if_cleanup_did_not_finish() {
    let (_d,c,ws,job,store,mut j)=fixture().await;
    let file=transfer_journal::open_data(&j.data_path(&c),true).unwrap();file.set_len(crate::transfers::DISK_CAP).unwrap();
    j.phase="completed".into();j.result=Some(json!({"state":"completed"}));j.save(&store).await.unwrap();
    let mut next=job;next.id=uuid::Uuid::new_v4().to_string();assert!(Journal::load_or_create(&c,&ws,&next,&store,Arc::default()).await.is_err());
}

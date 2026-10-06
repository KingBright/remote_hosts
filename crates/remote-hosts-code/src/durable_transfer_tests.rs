use super::*;
include!("durable_transfer_fault_cases.rs");
use axum::{
    Router,
    body::{Body, Bytes},
    response::Response,
    routing::{get, post},
};
use futures_util::StreamExt;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

async fn fixture() -> (
    tempfile::TempDir,
    AgentConfig,
    Workspace,
    Job,
    Store,
    Journal,
) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let config = AgentConfig {
        gateway_url: "https://example.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: crate::random(),
        state_dir: d.path().join("agent"),
        roots: vec![root.canonicalize().unwrap()],
        allow_write: true,
        allow_exec: false,
        shell: "/bin/sh".into(),
    };
    let ws = Workspace {
        id: format!("{}:{}", config.device_id, uuid::Uuid::new_v4()),
        device_id: config.device_id.clone(),
        root: config.roots[0].clone(),
    };
    let job = Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: config.device_id.clone(),
        owner: "fixture".into(),
        tool: "file_upload".into(),
        arguments: json!({"workspace_id":ws.id,"path":"received.bin","file":{"file_id":"fixed","download_url":"resolved_by_gateway"},"expected_version":"absent","idempotency_key":"fixture"}),
    };
    let store = Store::open(&config.state_dir).await.unwrap();
    let j = Journal::load_or_create(&config, &ws, &job, &store, Arc::default())
        .await
        .unwrap();
    (d, config, ws, job, store, j)
}
fn prefix(config: &AgentConfig, j: &mut Journal, data: &[u8]) {
    std::fs::write(j.data_path(config), data).unwrap();
    j.offset = data.len() as u64;
    j.prefix_sha256 = crate::hash(data);
}
async fn static_source(bytes: Vec<u8>, mode: &str) -> (reqwest::Url, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/source", listener.local_addr().unwrap())).unwrap();
    let mode = mode.to_owned();
    let router = Router::new().route(
        "/source",
        get(move || {
            let data = bytes.clone();
            let mode = mode.clone();
            async move {
                let mut r = Response::builder().header("etag", "\"v1\"");
                r = match mode.as_str() {
                    "wrong_offset" => r.status(206).header(
                        "content-range",
                        format!("bytes 0-{}/{}", data.len() - 1, data.len()),
                    ),
                    "changed_etag" => r.status(206).header("etag", "\"v2\"").header(
                        "content-range",
                        format!("bytes 3-{}/{}", data.len() + 2, data.len() + 3),
                    ),
                    "expired" => r.status(403),
                    _ => r.status(200),
                };
                r.body(Body::from(data)).unwrap()
            }
        }),
    );
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}
#[tokio::test]
async fn range_ignored_restarts_without_duplicate_prefix() {
    let (_d, c, _ws, mut job, store, mut j) = fixture().await;
    let bytes = b"complete-file-new".to_vec();
    job.arguments["sha256"] = json!(crate::hash(&bytes));
    prefix(&c, &mut j, b"old");
    j.etag = Some("\"v1\"".into());
    j.save(&store).await.unwrap();
    let (raw, digest) = transfer_journal::restore_prefix(
        transfer_journal::open_data(&j.data_path(&c), false).unwrap(),
        j.offset,
        &j.prefix_sha256,
    )
    .unwrap();
    let mut file = tokio::fs::File::from_std(raw);
    let (url, task) = static_source(bytes.clone(), "ignored").await;
    fetch(
        &http().unwrap(),
        &url,
        &mut file,
        &job,
        &Progress::new(&job.id),
        &store,
        &mut j,
        digest,
    )
    .await
    .unwrap();
    drop(file);
    assert_eq!(std::fs::read(j.data_path(&c)).unwrap(), bytes);
    assert_eq!(j.content_sha256, Some(crate::hash(&bytes)));
    task.abort();
}
#[tokio::test]
async fn invalid_range_does_not_append_or_publish() {
    let (_d, c, ws, job, store, mut j) = fixture().await;
    prefix(&c, &mut j, b"old");
    j.etag = Some("\"v1\"".into());
    j.total = Some(10);
    j.save(&store).await.unwrap();
    let (raw, digest) = transfer_journal::restore_prefix(
        transfer_journal::open_data(&j.data_path(&c), false).unwrap(),
        j.offset,
        &j.prefix_sha256,
    )
    .unwrap();
    let mut f = tokio::fs::File::from_std(raw);
    let (url, task) = static_source(b"0123456789".to_vec(), "wrong_offset").await;
    assert!(
        fetch(
            &http().unwrap(),
            &url,
            &mut f,
            &job,
            &Progress::new(&job.id),
            &store,
            &mut j,
            digest
        )
        .await
        .is_err()
    );
    assert!(!ws.root.join("received.bin").exists());
    assert_eq!(std::fs::read(j.data_path(&c)).unwrap(), b"old");
    task.abort();
}
#[tokio::test]
async fn expired_source_keeps_checkpoint_for_explicit_authorization_refresh() {
    let (_d, c, _ws, job, store, mut j) = fixture().await;
    prefix(&c, &mut j, b"old");
    j.etag = Some("\"v1\"".into());
    j.save(&store).await.unwrap();
    let (raw, digest) = transfer_journal::restore_prefix(
        transfer_journal::open_data(&j.data_path(&c), false).unwrap(),
        j.offset,
        &j.prefix_sha256,
    )
    .unwrap();
    let mut f = tokio::fs::File::from_std(raw);
    let (url, task) = static_source(vec![], "expired").await;
    let e = fetch(
        &http().unwrap(),
        &url,
        &mut f,
        &job,
        &Progress::new(&job.id),
        &store,
        &mut j,
        digest,
    )
    .await
    .unwrap_err();
    let fault = e.downcast_ref::<transfers::SourceHttpFault>().unwrap();
    assert_eq!(fault.status, 403);
    let diagnostic = fault.diagnostic();
    assert_eq!(diagnostic["failure_boundary"], "source_http_authorization");
    assert_eq!(diagnostic["http_request_started"], true);
    assert_eq!(diagnostic["destination_changed"], false);
    assert!(!diagnostic.to_string().contains("/source"));
    assert_eq!(fault.to_string(), "source_http_authorization_rejected");
    assert_eq!(std::fs::read(j.data_path(&c)).unwrap(), b"old");
    task.abort();
}
#[test]
fn prefix_recovery_truncates_uncommitted_tail_and_detects_corruption() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("partial");
    std::fs::write(&path, b"goodUNCOMMITTED").unwrap();
    transfer_journal::restore_prefix(
        transfer_journal::open_data(&path, false).unwrap(),
        4,
        &crate::hash("good"),
    )
    .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"good");
    std::fs::write(&path, b"evil").unwrap();
    assert!(
        transfer_journal::restore_prefix(
            transfer_journal::open_data(&path, false).unwrap(),
            4,
            &crate::hash("good")
        )
        .is_err()
    );
}
#[test]
fn restored_bytes_are_not_counted_as_new_throughput() {
    let p = Progress::new(&uuid::Uuid::new_v4().to_string());
    p.restore(CHUNK as u64, Some((CHUNK * 2) as u64), CHUNK as u64);
    let s = p.snapshot().unwrap();
    assert_eq!(s.average_bps, 0.0);
    assert_eq!(s.confirmed_bytes, Some(CHUNK as u64));
    assert_eq!(s.resumed_bytes, CHUNK as u64);
    assert!(s.valid());
    p.advance(CHUNK as u64 + 10, Some((CHUNK * 2) as u64));
    assert_eq!(p.snapshot().unwrap().confirmed_bytes, Some(CHUNK as u64));
    p.confirm(CHUNK as u64 + 10);
    assert_eq!(
        p.snapshot().unwrap().confirmed_bytes,
        Some(CHUNK as u64 + 10)
    );
}
#[tokio::test]
async fn changed_job_or_origin_cannot_adopt_previous_partial() {
    let (_d, c, ws, job, store, _j) = fixture().await;
    let mut changed = job.clone();
    changed.arguments["path"] = json!("another");
    assert!(
        Journal::load_or_create(&c, &ws, &changed, &store, Arc::default())
            .await
            .is_err()
    );
    let mut cfg = c;
    cfg.gateway_url = "https://other.example".into();
    assert!(
        Journal::load_or_create(&cfg, &ws, &job, &store, Arc::default())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn retained_transfers_have_a_bounded_quota_and_expire() {
    let (_d, c, ws, job, store, mut first) = fixture().await;
    prefix(&c, &mut first, b"old");
    first.expires_at = crate::now() - 1;
    first.save(&store).await.unwrap();
    let reservations = crate::transfers::DISK_CAP as usize / crate::transfers::DEFAULT_MAX_BYTES;
    for n in 0..reservations {
        let mut next = job.clone();
        next.id = uuid::Uuid::new_v4().to_string();
        Journal::load_or_create(&c, &ws, &next, &store, Arc::default())
            .await
            .unwrap_or_else(|e| panic!("{n}:{e}"));
    }
    let mut extra = job;
    extra.id = uuid::Uuid::new_v4().to_string();
    assert!(
        Journal::load_or_create(&c, &ws, &extra, &store, Arc::default())
            .await
            .is_err()
    );
    assert!(!first.data_path(&c).exists());
    assert_eq!(
        store
            .get::<Journal>("transfer_local", &first.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        "expired"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_monitor_interrupts_a_stalled_network_future() {
    let (_d, mut c, _ws, job, _store, _j) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.gateway_url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/device/transfer-control/{id}",
        get(|| async { axum::Json(json!({"protocol":2,"revision":0,"cancel_requested":true})) }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut guard = Guard {
        client: http().unwrap(),
        config: &c,
        id: &job.id,
        revision: 0,
        next_check: tokio::time::Instant::now(),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        guard.wait(std::future::pending::<Result<()>>()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(result.downcast_ref::<Fault>().unwrap().state, "cancelled");
    server.abort();
}
#[tokio::test]
async fn publication_recovery_does_not_undo_an_already_committed_target() {
    let (_d, mut c, ws, job, store, mut j) = fixture().await;
    let bytes = b"already committed";
    std::fs::write(ws.root.join("received.bin"), bytes).unwrap();
    j.phase = "publishing".into();
    j.total = Some(bytes.len() as u64);
    j.offset = bytes.len() as u64;
    j.content_sha256 = Some(crate::hash(bytes));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.gateway_url = format!("http://{}", listener.local_addr().unwrap());
    j.origin = c.gateway_url.clone();
    j.save(&store).await.unwrap();
    let router = Router::new().route(
        "/device/transfer-control/{id}",
        get(|| async { axum::Json(json!({"protocol":2,"revision":0,"cancel_requested":true})) }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let result = run(
        &c,
        &ws,
        &job,
        &Progress::new(&job.id),
        &store,
        Arc::default(),
    )
    .await
    .unwrap();
    assert_eq!(result["state"], "completed");
    assert_eq!(result["publication_recovered"], true);
    assert_eq!(std::fs::read(ws.root.join("received.bin")).unwrap(), bytes);
    server.abort();
}

// Spawned only by the following test. This is a new OS process using a temporary
// fixture directory and synthetic credentials; it never touches production state.
#[test]
fn inbound_child_process() {
    let Some(path) = std::env::var_os("RH040_CHECKPOINT_FIXTURE") else {
        return;
    };
    let v: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let config: AgentConfig = serde_json::from_value(v["config"].clone()).unwrap();
        let job: Job = serde_json::from_value(v["job"].clone()).unwrap();
        let store = Store::open(&config.state_dir).await.unwrap();
        let mut j: Journal = store.get("transfer_local", &job.id).await.unwrap().unwrap();
        let (raw, digest) = transfer_journal::restore_prefix(
            transfer_journal::open_data(&j.data_path(&config), true).unwrap(),
            j.offset,
            &j.prefix_sha256,
        )
        .unwrap();
        let mut file = tokio::fs::File::from_std(raw);
        let url = reqwest::Url::parse(v["url"].as_str().unwrap()).unwrap();
        fetch(
            &http().unwrap(),
            &url,
            &mut file,
            &job,
            &Progress::new(&job.id),
            &store,
            &mut j,
            digest,
        )
        .await
        .unwrap();
    });
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_checkpoint_survives_a_real_process_kill_and_resumes_only_the_tail() {
    let (d, c, _ws, mut job, store, mut j) = fixture().await;
    let bytes: Vec<u8> = (0..CHUNK + 333).map(|i| (i % 251) as u8).collect();
    job.arguments["sha256"] = json!(crate::hash(&bytes));
    j.fingerprint = crate::hash(serde_json::to_vec(&job).unwrap());
    j.save(&store).await.unwrap();
    let hold = Arc::new(AtomicBool::new(true));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let h = hold.clone();
    let seen = requests.clone();
    let source = bytes.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/source", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/source",
        get(move |headers: reqwest::header::HeaderMap| {
            let source = source.clone();
            let h = h.clone();
            let seen = seen.clone();
            async move {
                let range = headers
                    .get("range")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                seen.lock().unwrap().push(range.clone());
                if h.load(Ordering::SeqCst) {
                    let first = Bytes::copy_from_slice(&source[..CHUNK]);
                    let stream =
                        futures_util::stream::once(async move { Ok::<_, std::io::Error>(first) })
                            .chain(futures_util::stream::pending());
                    Response::builder()
                        .status(200)
                        .header("etag", "\"immutable-v1\"")
                        .header("content-length", source.len())
                        .body(Body::from_stream(stream))
                        .unwrap()
                } else {
                    assert_eq!(range, format!("bytes={CHUNK}-"));
                    Response::builder()
                        .status(206)
                        .header("etag", "\"immutable-v1\"")
                        .header(
                            "content-range",
                            format!("bytes {CHUNK}-{}/{}", source.len() - 1, source.len()),
                        )
                        .body(Body::from(source[CHUNK..].to_vec()))
                        .unwrap()
                }
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let spec = d.path().join("child.json");
    std::fs::write(&spec, json!({"config":c,"job":job,"url":url}).to_string()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_transfer::tests::inbound_child_process",
            "--nocapture",
        ])
        .env("RH040_CHECKPOINT_FIXTURE", &spec)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let reached = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if store
                .get::<Journal>("transfer_local", &job.id)
                .await
                .unwrap()
                .unwrap()
                .offset
                == CHUNK as u64
            {
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before checkpoint"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let _ = child.kill();
    let _ = child.wait();
    assert!(reached.is_ok(), "durable checkpoint was not reached");
    hold.store(false, Ordering::SeqCst);
    let reopened = Store::open(&c.state_dir).await.unwrap();
    let mut saved: Journal = reopened
        .get("transfer_local", &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.offset, CHUNK as u64);
    let (raw, digest) = transfer_journal::restore_prefix(
        transfer_journal::open_data(&saved.data_path(&c), false).unwrap(),
        saved.offset,
        &saved.prefix_sha256,
    )
    .unwrap();
    let mut file = tokio::fs::File::from_std(raw);
    fetch(
        &http().unwrap(),
        &reqwest::Url::parse(&url).unwrap(),
        &mut file,
        &job,
        &Progress::new(&job.id),
        &reopened,
        &mut saved,
        digest,
    )
    .await
    .unwrap();
    drop(file);
    assert_eq!(std::fs::read(saved.data_path(&c)).unwrap(), bytes);
    assert_eq!(saved.content_sha256, Some(crate::hash(&bytes)));
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        ["", &format!("bytes={CHUNK}-")]
    );
    server.abort();
}

// Actual source authorization failure followed by an independently spawned
// transfer process. All URLs/state/credentials belong to this temporary fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_source_refresh_resumes_same_checkpoint_in_a_new_process() {
    let (d, c, ws, mut job, store, mut j) = fixture().await;
    let bytes = b"old-and-unchanged-tail".to_vec();
    job.arguments["sha256"] = json!(crate::hash(&bytes));
    j.fingerprint = crate::hash(serde_json::to_vec(&job).unwrap());
    prefix(&c, &mut j, b"old");
    j.etag = Some("\"v1\"".into());
    j.save(&store).await.unwrap();
    let (raw, digest) = transfer_journal::restore_prefix(
        transfer_journal::open_data(&j.data_path(&c), false).unwrap(),
        j.offset,
        &j.prefix_sha256,
    )
    .unwrap();
    let mut file = tokio::fs::File::from_std(raw);
    let (expired, expired_server) = static_source(vec![], "expired").await;
    let error = fetch(
        &http().unwrap(),
        &expired,
        &mut file,
        &job,
        &Progress::new(&job.id),
        &store,
        &mut j,
        digest,
    )
    .await
    .unwrap_err();
    let fault = error.downcast_ref::<transfers::SourceHttpFault>().unwrap();
    assert_eq!(fault.status, 403);
    let diagnostic = fault.diagnostic();
    assert_eq!(diagnostic["failure_boundary"], "source_http_authorization");
    assert_eq!(diagnostic["http_request_started"], true);
    assert_eq!(diagnostic["destination_changed"], false);
    assert!(!diagnostic.to_string().contains("/source"));
    drop(file);
    expired_server.abort();
    assert_eq!(std::fs::read(j.data_path(&c)).unwrap(), b"old");
    assert!(!ws.root.join("received.bin").exists());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fresh = format!("http://{}/source", listener.local_addr().unwrap());
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let captured = ranges.clone();
    let tail = bytes[3..].to_vec();
    let router = Router::new().route(
        "/source",
        get(move |headers: axum::http::HeaderMap| {
            let captured = captured.clone();
            let tail = tail.clone();
            async move {
                let range = headers.get("range").unwrap().to_str().unwrap().to_owned();
                captured.lock().unwrap().push(range);
                Response::builder()
                    .status(206)
                    .header("etag", "\"v1\"")
                    .header(
                        "content-range",
                        format!("bytes 3-{}/{}", tail.len() + 2, tail.len() + 3),
                    )
                    .body(Body::from(tail))
                    .unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let spec = d.path().join("refresh-child.json");
    std::fs::write(&spec, json!({"config":c,"job":job,"url":fresh}).to_string()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_transfer::tests::inbound_child_process",
            "--nocapture",
        ])
        .env("RH040_CHECKPOINT_FIXTURE", &spec)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let finished = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if finished.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    server.abort();
    assert!(
        finished.unwrap().success(),
        "refreshed transfer child failed"
    );
    let reopened = Store::open(&c.state_dir).await.unwrap();
    let saved: Journal = reopened
        .get("transfer_local", &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.operation_id, job.id);
    assert_eq!(saved.offset, bytes.len() as u64);
    assert_eq!(saved.content_sha256, Some(crate::hash(&bytes)));
    assert_eq!(std::fs::read(saved.data_path(&c)).unwrap(), bytes);
    assert_eq!(ranges.lock().unwrap().as_slice(), ["bytes=3-"]);
}

#[tokio::test]
async fn destination_permission_suspension_preserves_recovery_state_and_boundary() {
    let (_d, mut c, ws, job, store, mut j) = fixture().await;
    prefix(&c, &mut j, b"checkpoint");
    j.save(&store).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    c.gateway_url = format!("http://{}", listener.local_addr().unwrap());
    j.origin = c.gateway_url.clone();
    let status_posts = Arc::new(AtomicUsize::new(0));
    let seen_posts = status_posts.clone();
    let router = Router::new().route(
        "/device/transfer-status/{id}",
        post(move || {
            let seen_posts = seen_posts.clone();
            async move {
                seen_posts.fetch_add(1, Ordering::SeqCst);
                axum::Json(json!({"accepted":true}))
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let staging_error = transfers::classify_destination_error(
        anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "secret /private/staging/path",
        )),
        "destination_staging",
        Some(false),
    );
    let staging_fault = staging_error
        .downcast_ref::<transfers::DestinationPermissionFault>()
        .unwrap();
    let staging = suspended(
        &c,
        &store,
        &mut j,
        &Progress::new(&job.id),
        "paused",
        "destination_file_permission_denied",
        Some(staging_fault.diagnostic()),
    )
    .await
    .unwrap();
    assert_eq!(staging["state"], "paused");
    assert_eq!(staging["failure_boundary"], "destination_file_permission");
    assert_eq!(staging["diagnostic"]["stage"], "destination_staging");
    assert_eq!(staging["destination_changed"], false);
    assert_eq!(
        staging["next_action"],
        "fix_destination_permissions_then_resume_original"
    );
    assert!(!staging.to_string().contains("/private/staging/path"));
    let saved: Journal = store.get("transfer_local", &job.id).await.unwrap().unwrap();
    assert_eq!(saved.phase, "paused");
    assert_eq!(saved.offset, b"checkpoint".len() as u64);
    assert!(saved.result.is_none());
    assert!(!ws.root.join("received.bin").exists());

    j.phase = "publishing".into();
    j.publication_temp = Some(".remote-hosts-transfer-fixture.tmp".into());
    j.publication_identity = Some((1, 2));
    j.save(&store).await.unwrap();
    let publication_error = transfers::classify_destination_error(
        anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private publication target",
        )),
        "destination_publication",
        None,
    );
    let publication_fault = publication_error
        .downcast_ref::<transfers::DestinationPermissionFault>()
        .unwrap();
    let publication = suspended(
        &c,
        &store,
        &mut j,
        &Progress::new(&job.id),
        "paused",
        "destination_file_permission_denied",
        Some(publication_fault.diagnostic()),
    )
    .await
    .unwrap();
    assert_eq!(publication["state"], "paused");
    assert_eq!(publication["publication_pending"], true);
    assert_eq!(
        publication["diagnostic"]["stage"],
        "destination_publication"
    );
    assert!(publication["destination_changed"].is_null());
    assert_eq!(
        publication["next_action"],
        "inspect_destination_before_resuming_original"
    );
    assert_eq!(
        publication["retry_policy"],
        "resume_original_only_after_destination_permissions_and_state_confirmed"
    );
    assert!(
        !publication
            .to_string()
            .contains("private publication target")
    );
    let saved: Journal = store.get("transfer_local", &job.id).await.unwrap().unwrap();
    assert_eq!(saved.phase, "publishing");
    assert_eq!(
        saved.publication_temp.as_deref(),
        Some(".remote-hosts-transfer-fixture.tmp")
    );
    assert_eq!(saved.publication_identity, Some((1, 2)));
    assert_eq!(saved.offset, b"checkpoint".len() as u64);
    assert!(saved.result.is_none());
    assert_eq!(status_posts.load(Ordering::SeqCst), 2);
    assert!(!ws.root.join("received.bin").exists());
    server.abort();
}

#[tokio::test]
async fn legacy_transfer_journal_without_publication_metadata_decodes_with_safe_defaults() {
    let (_d, _c, _ws, _job, _store, journal) = fixture().await;
    let mut legacy = serde_json::to_value(journal).unwrap();
    let fields = legacy.as_object_mut().unwrap();
    assert!(fields.remove("publication_identity").is_some());
    assert!(fields.remove("data_cleaned").is_some());

    let decoded: Journal = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.publication_identity, None);
    assert!(!decoded.data_cleaned);
}

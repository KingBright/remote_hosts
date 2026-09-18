//! Controlled contention, temporary state, and a real Agent/Gateway loop.
use super::*;
use crate::{DeviceRegistration, GatewayConfig, gateway::Gateway, hash, now, random};

fn job(a: &Agent, tool: &str, arguments: Value) -> Job {
    Job {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: a.config.device_id.clone(),
        owner: "fixture".into(),
        tool: tool.into(),
        arguments,
    }
}
async fn workspace(a: &Agent, root: &Path) -> String {
    a.execute(&job(
        a,
        "workspace_open",
        json!({"device_id":a.config.device_id,
        "root":root,"idempotency_key":random()}),
    ))
    .await
    .unwrap()["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_owned()
}
async fn fixture() -> (tempfile::TempDir, Agent, String, String, String) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("projects");
    for p in ["a", "b"] {
        std::fs::create_dir_all(root.join(p)).unwrap();
    }
    let a = Agent::new(AgentConfig {
        gateway_url: "https://example.invalid".into(),
        device_id: uuid::Uuid::new_v4().to_string(),
        device_token: random(),
        state_dir: d.path().join("state"),
        roots: vec![root.clone()],
        allow_write: true,
        allow_exec: true,
        shell: "/bin/sh".into(),
    })
    .await
    .unwrap();
    let wa = workspace(&a, &root.join("a")).await;
    let alias = workspace(&a, &root.join("a")).await;
    let wb = workspace(&a, &root.join("b")).await;
    (d, a, wa, alias, wb)
}
fn edit(a: &Agent, ws: &str, name: &str) -> Job {
    job(
        a,
        "code_apply_edits",
        json!({"workspace_id":ws,"idempotency_key":name,
        "files":[{"path":name,"action":"create","expected_version":"absent","content":"ok"}]}),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_writes_do_not_take_sibling_execution_slots() {
    let (_d, a, wa, alias, wb) = fixture().await;
    let held = a
        .scheduler
        .writes
        .acquire(&a.config.roots[0].join("a"))
        .await
        .unwrap();
    let x = edit(&a, &wa, "one");
    let y = edit(&a, &alias, "two");
    let aa = a.clone();
    let first = tokio::spawn(async move { aa.execute(&x).await });
    let aa = a.clone();
    let second = tokio::spawn(async move { aa.execute(&y).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while a.progress.snapshots().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        a.execute(&edit(&a, &wb, "independent")),
    )
    .await;
    drop(held);
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let result = result
        .expect("two writes waiting on A occupied both execution slots; B starved")
        .unwrap();
    assert!(result.get("error").is_none(), "{result}");
    assert_eq!(
        std::fs::read(a.config.roots[0].join("b/independent")).unwrap(),
        b"ok"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_poll_skips_backlog_for_same_root_aliases_and_then_drains_it() {
    let (d, mut a, wa, alias, wb) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    Arc::make_mut(&mut a.config).gateway_url = format!("http://{address}");
    let g = Gateway::new(GatewayConfig {
        allowed_origins: crate::default_mcp_client_origins(),
        public_url: format!("https://{address}"),
        bind: "127.0.0.1:0".into(),
        state_dir: d.path().join("gateway"),
        owner: "fixture".into(),
        password_hash: "unused".into(),
        redirect_uris: vec![],
        devices: vec![DeviceRegistration {
            id: a.config.device_id.clone(),
            name: "fixture".into(),
            token_hash: hash(&a.config.device_token),
            scopes: vec![
                "code:read".into(),
                "code:write".into(),
                "terminal:exec".into(),
            ],
        }],
    })
    .await
    .unwrap();
    let held = a
        .scheduler
        .writes
        .acquire(&a.config.roots[0].join("a"))
        .await
        .unwrap();
    let mut backlog = vec![];
    for n in 0..24 {
        backlog.push(edit(
            &a,
            if n % 2 == 0 { &wa } else { &alias },
            &format!("pending-{n}"),
        ));
    }
    let independent = edit(&a, &wb, "independent-loop");
    for (n, j) in backlog
        .iter()
        .chain(std::iter::once(&independent))
        .enumerate()
    {
        sqlx::query("INSERT INTO jobs VALUES(?,?,?,?,?,NULL,'queued',?)")
            .bind(&j.id)
            .bind(&j.device_id)
            .bind(random())
            .bind(random())
            .bind(serde_json::to_string(j).unwrap())
            .bind(now() - 60 + n as i64)
            .execute(&g.store.pool)
            .await
            .unwrap();
    }
    let router = g.router().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let aa = a.clone();
    let worker = tokio::spawn(async move { aa.run().await });
    let result: Result<Value> = async {
        // Startup is not scheduling latency. Wait for an authenticated write-lane
        // round trip, without requiring B to finish or releasing the held A root.
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let ready = a.store.get::<Value>("runtime", "readiness").await.unwrap();
                if ready.is_some_and(|r| r["lanes"]["write"].as_i64().is_some_and(|n| n > 0)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("fixture write lane did not become ready")?;
        let value = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (value,): (Option<String>,) =
                    sqlx::query_as("SELECT result FROM jobs WHERE id=?")
                        .bind(&independent.id)
                        .fetch_one(&g.store.pool)
                        .await
                        .unwrap();
                if let Some(value) = value {
                    break serde_json::from_str::<Value>(&value).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("blocked A aliases hid runnable B after startup")?;
        let (completed,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE result IS NOT NULL")
                .fetch_one(&g.store.pool)
                .await?;
        ensure!(
            completed == 1,
            "blocked A work completed before its root was released"
        );
        Ok(value)
    }
    .await;
    // Release even on failure: no background task leaks from an assertion.
    drop(held);
    if result.is_err() {
        worker.abort();
        server.abort();
    }
    let result = result.expect("queued writes for aliases of blocked A hid runnable project B");
    assert!(result.get("error").is_none(), "{result}");
    let drained: Result<()> = async {
        // Prove A becomes schedulable within the original watchdog. Finishing
        // all 24 durable edits is a separate completeness check, not a latency
        // benchmark for the host's fsync throughput.
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let (resumed,): (i64,) =
                    sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE id<>? AND result IS NOT NULL")
                        .bind(&independent.id)
                        .fetch_one(&g.store.pool)
                        .await
                        .unwrap();
                if resumed > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("released resource did not become schedulable again")?;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let (remaining,): (i64,) =
                    sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE result IS NULL")
                        .fetch_one(&g.store.pool)
                        .await
                        .unwrap();
                if remaining == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("resumed backlog did not finish every durable edit")?;
        Ok(())
    }
    .await;
    worker.abort();
    let _ = worker.await;
    server.abort();
    drained.expect("released-root scheduling or completeness failed");
    for n in 0..24 {
        assert_eq!(
            std::fs::read(a.config.roots[0].join(format!("a/pending-{n}"))).unwrap(),
            b"ok"
        );
    }
}

//! Bootstrap recovery is a closed whitelist, never a tool-call replay path.
//! Each step has a durable identity; failed attempts survive successful recovery.
use super::{Adapter, persist};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;

const TOTAL_BUDGET: Duration = Duration::from_secs(25);
const ATTEMPT_BUDGET: Duration = Duration::from_secs(8);
const MAX_ATTEMPTS: usize = 3;

fn recoverable(code: &str) -> bool {
    matches!(
        code,
        "upstream_transport_outcome_unknown"
            | "upstream_collection_incomplete"
            | "upstream_initialize_notification_failed"
            | "bootstrap_attempt_timeout"
    )
}
fn safe_code(error: &anyhow::Error) -> String {
    let text = error.to_string();
    // Persist only our fixed error labels. Never store response bodies or URLs.
    if text.len() <= 96
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        text
    } else {
        "bootstrap_protocol_invalid".into()
    }
}

impl Adapter {
    pub(super) async fn bootstrap(&mut self) -> Result<Value> {
        let deadline = Instant::now() + TOTAL_BUDGET;
        let init = self.bootstrap_step("initialize", json!({
            "protocolVersion": self.protocol, "capabilities": {},
            "clientInfo": {"name": "remote-hosts-generated-adapter", "version": env!("CARGO_PKG_VERSION")}
        }), deadline).await?;
        self.protocol = init["protocolVersion"]
            .as_str()
            .context("initialize_protocol_missing")?
            .to_owned();
        self.bootstrap_step("notifications/initialized", json!({}), deadline)
            .await?;
        self.bootstrap_step("tools/list", json!({}), deadline).await
    }

    async fn bootstrap_step(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Value> {
        ensure!(
            matches!(
                method,
                "initialize" | "notifications/initialized" | "tools/list"
            ),
            "bootstrap_method_not_allowed"
        );
        let id = format!("req_{}", uuid::Uuid::new_v4().simple());
        let started = Instant::now();
        let mut record = json!({"kind": "adapter_bootstrap", "request_id": id,
            "method": method, "state": "bootstrap_started", "at": crate::now(),
            "operation_id": null, "execution_state": "not_started", "tool_calls_submitted": 0,
            "max_attempts": MAX_ATTEMPTS, "attempts": [], "evidence_complete": false,
            "retry_policy": "bounded_bootstrap_only_never_replay_tools"});
        persist(&self.state_dir, &id, &record).context("bootstrap_receipt_storage_failed")?;
        for attempt in 1..=MAX_ATTEMPTS {
            if Instant::now() >= deadline {
                record["state"] = json!("bootstrap_failed");
                record["error_code"] = json!("bootstrap_deadline_exceeded");
                persist(&self.state_dir, &id, &record)?;
                anyhow::bail!(
                    "bootstrap_deadline_exceeded; bootstrap_request={id}; tools_not_submitted"
                );
            }
            record["attempt_in_flight"] = json!(attempt);
            persist(&self.state_dir, &id, &record)?;
            let attempt_started = Instant::now();
            let result =
                tokio::time::timeout_at(deadline.min(Instant::now() + ATTEMPT_BUDGET), async {
                    if method == "notifications/initialized" {
                        self.notify_initialized().await?;
                        Ok(Value::Null)
                    } else {
                        self.rpc(method, params.clone(), &id).await
                    }
                })
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("bootstrap_attempt_timeout")));
            let code = result.as_ref().err().map(safe_code);
            record["attempts"]
                .as_array_mut()
                .expect("attempt list")
                .push(json!({
                    "number": attempt, "elapsed_ms": attempt_started.elapsed().as_millis(),
                    "state": if result.is_ok() {"response_complete"} else {"response_unconfirmed"},
                    "error_code": code
                }));
            record["attempt_in_flight"] = Value::Null;
            record["elapsed_ms"] = json!(started.elapsed().as_millis());
            record["at"] = json!(crate::now());
            if let Ok(value) = result {
                record["state"] = json!("bootstrap_step_completed");
                record["evidence_complete"] = json!(true);
                persist(&self.state_dir, &id, &record)?;
                return Ok(value);
            }
            let code = code.expect("failed attempt has code");
            let retry = attempt < MAX_ATTEMPTS && recoverable(&code) && Instant::now() < deadline;
            record["state"] = json!(if retry {
                "bootstrap_retry_pending"
            } else {
                "bootstrap_failed"
            });
            record["error_code"] = json!(code);
            persist(&self.state_dir, &id, &record)?;
            if !retry {
                anyhow::bail!("{code}; bootstrap_request={id}; tools_not_submitted");
            }
            tokio::time::sleep_until(
                deadline.min(Instant::now() + Duration::from_millis(100 * attempt as u64)),
            )
            .await;
        }
        unreachable!("bounded bootstrap returns a success or a persisted failure")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Principal;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    enum Reply {
        BrokenBody,
        Json,
        Forbidden,
        WrongId,
        Stall,
    }
    async fn fixture(
        replies: Vec<Reply>,
    ) -> (
        tempfile::TempDir,
        Adapter,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<Vec<Value>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let server_count = count.clone();
        let server = tokio::spawn(async move {
            let mut seen = Vec::new();
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0u8; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(i) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                        break i + 4;
                    }
                    assert!(bytes.len() < 16384);
                };
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let length: usize = header
                    .lines()
                    .find_map(|s| {
                        s.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let request: Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
                server_count.fetch_add(1, Ordering::Relaxed);
                let id = request["id"].clone();
                seen.push(request);
                let response = match reply {
                    Reply::BrokenBody => {
                        "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{"
                            .to_owned()
                    }
                    Reply::Forbidden => {
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_owned()
                    }
                    Reply::Stall => {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        String::new()
                    }
                    Reply::Json | Reply::WrongId => {
                        let id = if matches!(reply, Reply::WrongId) {
                            json!("wrong")
                        } else {
                            id
                        };
                        let body =
                            json!({"jsonrpc":"2.0","id":id,"result":{"tools":[]}}).to_string();
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                };
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
            seen
        });
        let adapter = Adapter {
            client: reqwest::Client::builder()
                .no_proxy()
                .http1_only()
                .build()
                .unwrap(),
            origin,
            token: Arc::from("synthetic-token"),
            protocol: "2025-06-18".into(),
            session: None,
            catalog: Vec::new(),
            scopes: Principal {
                owner: "fixture".into(),
                scopes: Vec::new(),
            },
            state_dir: dir.path().to_owned(),
            host_report_file: None,
        };
        (dir, adapter, count, server)
    }
    fn saved(dir: &std::path::Path) -> Value {
        let path = std::fs::read_dir(dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }
    #[tokio::test]
    async fn incomplete_bootstrap_body_recovers_with_one_identity_and_no_tool() {
        let (dir, adapter, calls, server) = fixture(vec![Reply::BrokenBody, Reply::Json]).await;
        let value = adapter
            .bootstrap_step(
                "tools/list",
                json!({}),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert!(value["tools"].is_array());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let seen = server.await.unwrap();
        assert_eq!(seen[0]["id"], seen[1]["id"]);
        assert!(seen.iter().all(|v| v["method"] == "tools/list"));
        let record = saved(dir.path());
        assert_eq!(record["state"], "bootstrap_step_completed");
        assert_eq!(
            record["attempts"][0]["error_code"],
            "upstream_collection_incomplete"
        );
        assert_eq!(record["tool_calls_submitted"], 0);
        assert_eq!(record["attempts"].as_array().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn exhausted_bootstrap_retains_all_attempts_without_unbounded_reconnect() {
        let (dir, adapter, calls, server) = fixture(vec![
            Reply::BrokenBody,
            Reply::BrokenBody,
            Reply::BrokenBody,
        ])
        .await;
        assert!(
            adapter
                .bootstrap_step(
                    "initialize",
                    json!({}),
                    Instant::now() + Duration::from_secs(3)
                )
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        server.await.unwrap();
        let record = saved(dir.path());
        assert_eq!(record["state"], "bootstrap_failed");
        assert_eq!(record["execution_state"], "not_started");
        assert_eq!(record["attempts"].as_array().unwrap().len(), 3);
    }
    #[tokio::test]
    async fn authorization_or_protocol_mismatch_is_not_retried() {
        for reply in [Reply::Forbidden, Reply::WrongId] {
            let (dir, adapter, calls, server) = fixture(vec![reply]).await;
            assert!(
                adapter
                    .bootstrap_step(
                        "tools/list",
                        json!({}),
                        Instant::now() + Duration::from_secs(3)
                    )
                    .await
                    .is_err()
            );
            server.await.unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(saved(dir.path())["state"], "bootstrap_failed");
        }
    }
    #[tokio::test]
    async fn tool_calls_cannot_enter_bootstrap_recovery() {
        let (dir, adapter, calls, server) = fixture(vec![]).await;
        assert!(
            adapter
                .bootstrap_step(
                    "tools/call",
                    json!({}),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
    #[tokio::test]
    async fn bootstrap_deadline_is_shared_and_enforced() {
        let (dir, adapter, calls, server) = fixture(vec![Reply::Stall]).await;
        let started = Instant::now();
        // Leave room for the mandatory durable receipt before network I/O.
        // Three seconds is still below both the stalled peer's five seconds
        // and the production per-attempt budget of eight seconds.
        assert!(
            adapter
                .bootstrap_step("initialize", json!({}), started + Duration::from_secs(3))
                .await
                .is_err()
        );
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let record = saved(dir.path());
        assert_eq!(record["execution_state"], "not_started");
        assert_eq!(
            record["attempts"][0]["error_code"],
            "bootstrap_attempt_timeout"
        );
        server.abort();
    }
    #[tokio::test]
    async fn already_expired_budget_sends_nothing_and_retains_failure() {
        let (dir, adapter, calls, server) = fixture(vec![]).await;
        assert!(
            adapter
                .bootstrap_step(
                    "initialize",
                    json!({}),
                    Instant::now() - Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let record = saved(dir.path());
        assert_eq!(record["state"], "bootstrap_failed");
        assert_eq!(record["error_code"], "bootstrap_deadline_exceeded");
        assert_eq!(record["tool_calls_submitted"], 0);
        assert!(record["attempts"].as_array().unwrap().is_empty());
    }
    #[tokio::test]
    async fn established_tool_with_truncated_response_is_never_replayed() {
        let (_dir, adapter, calls, server) = fixture(vec![Reply::BrokenBody]).await;
        let request =
            serde_json::from_value(json!({"name":"devices_list","arguments":{}})).unwrap();
        let result = adapter.forward(request).await;
        let seen = server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(seen[0]["method"], "tools/call");
        assert_eq!(result.is_error, Some(true));
        let value = result.structured_content.unwrap();
        assert_eq!(value["execution_state"], "unknown");
        assert_eq!(value["request_id"], seen[0]["id"]);
        assert_eq!(value["retry_policy"], "do_not_replay_observe_request_id");
    }
    #[tokio::test]
    async fn receipt_storage_failure_prevents_bootstrap_network_request() {
        let (_dir, mut adapter, calls, server) = fixture(vec![]).await;
        adapter.state_dir = adapter.state_dir.join("missing");
        assert!(
            adapter
                .bootstrap_step(
                    "initialize",
                    json!({}),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        server.await.unwrap();
    }
}

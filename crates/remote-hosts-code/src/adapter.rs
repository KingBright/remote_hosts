//! Native stdio-to-Streamable-HTTP adapter using the Gateway's generated Tool catalog.
//! OAuth consent/token refresh belongs to the authenticated client configuration.
//! This adapter never retries a tools/call and never invents final host exposure.
use crate::{auth::Principal, contract, hash, now, receipts};
use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, HeaderValue};
use rmcp::{RoleServer, ServerHandler, ServiceExt, model::*, service::RequestContext};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub origin: String,
    pub access_token_file: PathBuf,
    pub state_dir: PathBuf,
    #[serde(default)]
    pub host_report_file: Option<PathBuf>,
}
#[derive(Clone)]
pub struct Adapter {
    client: reqwest::Client,
    origin: String,
    token: Arc<str>,
    protocol: String,
    session: Option<String>,
    catalog: Vec<Tool>,
    scopes: Principal,
    state_dir: PathBuf,
    host_report_file: Option<PathBuf>,
}
const MAX_RESPONSE: usize = 2 * 1024 * 1024;
mod bootstrap;
fn private_regular(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink() && meta.len() <= 65536,
        "adapter_config_file_invalid"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            meta.permissions().mode() & 0o077 == 0,
            "adapter_credentials_must_be_private"
        );
    }
    Ok(())
}
fn persist(dir: &Path, id: &str, value: &Value) -> Result<()> {
    ensure!(receipts::valid_request_id(id), "invalid_request_id");
    let mut f = tempfile::NamedTempFile::new_in(dir)?;
    use std::io::Write;
    f.write_all(&serde_json::to_vec(value)?)?;
    f.as_file().sync_all()?;
    f.persist(dir.join(format!("{id}.json")))
        .map_err(|e| e.error)?;
    Ok(())
}
impl Adapter {
    pub async fn connect(config: Config) -> Result<Self> {
        crate::validate_url(&config.origin)?;
        private_regular(&config.access_token_file)?;
        let token = std::fs::read_to_string(&config.access_token_file)?
            .trim()
            .to_owned();
        ensure!(
            !token.is_empty() && token.len() <= 8192 && !token.chars().any(char::is_whitespace),
            "invalid_access_token_file"
        );
        for ancestor in config.state_dir.ancestors() {
            if let Ok(meta) = std::fs::symlink_metadata(ancestor) {
                ensure!(!meta.file_type().is_symlink(), "adapter_state_symlink");
            }
        }
        std::fs::create_dir_all(&config.state_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut adapter = Self {
            client,
            origin: config.origin.trim_end_matches('/').to_owned(),
            token: token.into(),
            protocol: "2025-06-18".into(),
            session: None,
            catalog: Vec::new(),
            scopes: Principal {
                owner: "upstream-authorized".into(),
                scopes: Vec::new(),
            },
            state_dir: config.state_dir,
            host_report_file: config.host_report_file,
        };
        let list = adapter.bootstrap().await?;
        adapter.catalog = serde_json::from_value(list["tools"].clone())?;
        ensure!(
            adapter.catalog.len() <= crate::tools::catalog().len(),
            "upstream_catalog_drift"
        );
        let local = crate::tools::catalog();
        for tool in &adapter.catalog {
            ensure!(
                local
                    .iter()
                    .any(|candidate| serde_json::to_value(candidate).ok()
                        == serde_json::to_value(tool).ok()),
                "upstream_catalog_drift: regenerate adapter from the same release"
            );
        }
        let mut scopes = std::collections::BTreeSet::new();
        for tool in &adapter.catalog {
            if let Some(scope) = crate::tools::scope(&tool.name) {
                scopes.insert(scope.to_owned());
            }
        }
        adapter.scopes.scopes = scopes.into_iter().collect();
        Ok(adapter)
    }
    fn headers(&self, id: &str) -> Result<HeaderMap> {
        let mut headers = contract::adapter_headers(&self.scopes);
        // The actual exposed upstream list, not an assumed full-permission list.
        headers.insert(
            "x-rh-adapter-catalog-sha256",
            HeaderValue::from_str(&hash(serde_json::to_vec(&self.catalog)?))?,
        );
        headers.insert("x-rh-request-id", HeaderValue::from_str(id)?);
        if let Some(path) = &self.host_report_file {
            private_regular(path)?;
            let report: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            let sha = report["catalog_sha256"]
                .as_str()
                .context("host_report_hash_missing")?;
            ensure!(
                sha.len() == 64
                    && sha
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "invalid_host_report_hash"
            );
            let names: Vec<String> = serde_json::from_value(report["tool_names"].clone())?;
            ensure!(
                names.len() <= 128
                    && names.iter().all(|n| !n.is_empty()
                        && n.len() <= 128
                        && n.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))),
                "invalid_host_report_names"
            );
            headers.insert("x-rh-host-catalog-sha256", HeaderValue::from_str(sha)?);
            headers.insert("x-rh-host-tools", HeaderValue::from_str(&names.join(","))?);
        }
        Ok(headers)
    }
    async fn rpc(&self, method: &str, params: Value, request_id: &str) -> Result<Value> {
        let mut request = self
            .client
            .post(format!("{}/mcp", self.origin))
            .bearer_auth(self.token.as_ref())
            .header("accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", &self.protocol)
            .headers(self.headers(request_id)?)
            .json(&json!({"jsonrpc":"2.0","id":request_id,"method":method,"params":params}));
        if let Some(session) = &self.session {
            request = request.header("Mcp-Session-Id", session);
        }
        let response = request
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("upstream_transport_outcome_unknown"))?;
        ensure!(
            response.status().is_success(),
            "upstream_http_status_{}",
            response.status().as_u16()
        );
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("upstream_collection_incomplete"))?
        {
            ensure!(
                bytes.len() + chunk.len() <= MAX_RESPONSE,
                "upstream_response_budget"
            );
            bytes.extend_from_slice(&chunk);
        }
        let response: Value =
            serde_json::from_slice(&bytes).context("upstream_response_not_json")?;
        ensure!(
            response["id"] == request_id,
            "upstream_response_id_mismatch"
        );
        ensure!(response.get("error").is_none(), "upstream_protocol_error");
        response
            .get("result")
            .cloned()
            .context("upstream_result_missing")
    }
    async fn notify_initialized(&self) -> Result<()> {
        let response = self
            .client
            .post(format!("{}/mcp", self.origin))
            .bearer_auth(self.token.as_ref())
            .header("accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", &self.protocol)
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("upstream_initialize_notification_failed"))?;
        ensure!(
            response.status().is_success(),
            "upstream_http_status_{}",
            response.status().as_u16()
        );
        // Fully consume a bounded acknowledgement so its connection is reusable.
        // A malformed or oversized reply is never silently accepted.
        let mut response = response;
        let mut collected = 0usize;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("upstream_collection_incomplete"))?
        {
            collected = collected
                .checked_add(chunk.len())
                .context("upstream_response_budget")?;
            ensure!(collected <= MAX_RESPONSE, "upstream_response_budget");
        }
        Ok(())
    }
}
impl ServerHandler for Adapter {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: self.catalog.clone(),
            ..Default::default()
        })
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self.forward(request).await)
    }
}
fn forward_error(id: &str, error: &str) -> CallToolResult {
    let code = error.split(':').next().unwrap_or("transport_error");
    let rejected = matches!(
        code,
        "upstream_http_status_401" | "upstream_http_status_403"
    );
    let value = json!({"error":if rejected{"upstream_authorization_rejected"}else{"upstream_call_unconfirmed"},
        "error_code":code,"request_id":id,"operation_id":null,
        "failure_boundary":if rejected{"gateway_authorization"}else{"connector_transport"},
        "last_confirmed_stage":if rejected{"gateway_rejected_authentication"}else{"connector_persisted_request"},
        "execution_state":if rejected{"not_started"}else{"unknown"},"business_state":"not_evaluated",
        "evidence_complete":false,"stale":!rejected,"retry_policy":"do_not_replay_observe_request_id",
        "next_action":if rejected{"refresh_authorized_credentials_then_reconnect"}else{"operation_get with this request_id after reconnect"},
        "user_action":"none"});
    let mut result = CallToolResult::error(vec![ContentBlock::text(if rejected {
        "Gateway rejected authentication; no tool execution was authorized."
    } else {
        "Upstream outcome is unconfirmed; observe the same request_id, do not replay."
    })]);
    result.structured_content = Some(value);
    result
}
impl Adapter {
    async fn forward(&self, request: CallToolRequestParams) -> CallToolResult {
        let id = format!("req_{}", uuid::Uuid::new_v4().simple());
        let before = json!({"request_id":id,"tool":request.name,"state":"connector_received","at":now(),"operation_id":null});
        if persist(&self.state_dir, &id, &before).is_err() {
            let value = json!({"error":"connector_receipt_storage_failed","request_id":id,"operation_id":null,
                "failure_boundary":"connector","execution_state":"not_started","evidence_complete":false,
                "retry_policy":"correct_storage_before_new_request","user_action":"none"});
            let mut result = CallToolResult::error(vec![ContentBlock::text(
                "connector receipt could not be saved; nothing sent",
            )]);
            result.structured_content = Some(value);
            return result;
        }
        let outcome = self
            .rpc(
                "tools/call",
                serde_json::to_value(request).unwrap_or_default(),
                &id,
            )
            .await;
        let mut result = match outcome {
            Ok(value) => match serde_json::from_value::<CallToolResult>(value) {
                Ok(v) => v,
                Err(_) => forward_error(&id, "upstream_result_schema_mismatch"),
            },
            Err(error) => forward_error(&id, &error.to_string()),
        };
        let structured = result
            .structured_content
            .as_ref()
            .cloned()
            .unwrap_or(Value::Null);
        let receipt = json!({"request_id":id,"operation_id":structured["operation_id"],"at":now(),
            "state":if structured["execution_state"]=="unknown"{"upstream_unconfirmed"}else{"response_received"},
            "last_confirmed_stage":structured["last_confirmed_stage"],"execution_state":structured["execution_state"],
            "receipt":structured["receipt"],"error_code":structured["error_code"]});
        if persist(&self.state_dir, &id, &receipt).is_err() {
            let value = result.structured_content.get_or_insert(json!({}));
            value["receipt_persistence_error"] = json!(true);
        }
        result
    }
}

pub async fn serve(config_path: &Path) -> Result<()> {
    private_regular(config_path)?;
    let config: Config = serde_json::from_slice(&std::fs::read(config_path)?)?;
    let adapter = Adapter::connect(config).await?;
    adapter
        .serve(rmcp::transport::stdio())
        .await?
        .waiting()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use std::sync::atomic::{AtomicUsize, Ordering};
    async fn fixture(
        invalid_json: bool,
    ) -> (
        tempfile::TempDir,
        Adapter,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Option<HeaderMap>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(tokio::sync::Mutex::new(None));
        let count = calls.clone();
        let headers_seen = seen.clone();
        let router=Router::new().route("/mcp",post(move |headers:HeaderMap,Json(body):Json<Value>|{
            let calls=count.clone();let seen=headers_seen.clone();async move {
                calls.fetch_add(1,Ordering::Relaxed);*seen.lock().await=Some(headers);
                if invalid_json {return Json(json!({"not":"a JSON-RPC result"}));}
                Json(json!({"jsonrpc":"2.0","id":body["id"],"result":{"content":[{"type":"text","text":"ok"}],
                    "structuredContent":{"request_id":body["id"],"state":"completed"}}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        // This test-only instance uses loopback HTTP. Production connect() still
        // requires an HTTPS origin and does not disable certificate verification.
        let scopes = Principal {
            owner: "fixture".into(),
            scopes: vec!["code:read".into()],
        };
        let adapter = Adapter {
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap(),
            origin,
            token: Arc::from("synthetic-test-token"),
            protocol: "2025-06-18".into(),
            session: None,
            catalog: contract::authorized_tools(&scopes),
            scopes,
            state_dir: dir.path().to_owned(),
            host_report_file: None,
        };
        (dir, adapter, calls, seen, server)
    }
    fn request() -> CallToolRequestParams {
        serde_json::from_value(json!({"name":"devices_list","arguments":{}})).unwrap()
    }
    #[tokio::test]
    async fn transport_automatically_reports_adapter_catalog_without_claiming_host_exposure() {
        let (_dir, adapter, calls, seen, server) = fixture(false).await;
        let value = adapter.forward(request()).await;
        assert_ne!(value.is_error, Some(true));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let headers = seen.lock().await.clone().unwrap();
        assert!(headers.contains_key("x-rh-adapter-catalog-sha256"));
        assert!(!headers.contains_key("x-rh-host-catalog-sha256"));
        let id = headers["x-rh-request-id"].to_str().unwrap();
        assert_eq!(value.structured_content.as_ref().unwrap()["request_id"], id);
        assert!(adapter.state_dir.join(format!("{id}.json")).exists());
        server.abort();
    }
    #[tokio::test]
    async fn malformed_upstream_result_retains_request_identity_and_never_retries() {
        let (_dir, adapter, calls, seen, server) = fixture(true).await;
        let value = adapter.forward(request()).await;
        assert_eq!(value.is_error, Some(true));
        let v = value.structured_content.unwrap();
        assert_eq!(v["execution_state"], "unknown");
        assert_eq!(v["retry_policy"], "do_not_replay_observe_request_id");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let headers = seen.lock().await.clone().unwrap();
        assert_eq!(
            v["request_id"],
            headers["x-rh-request-id"].to_str().unwrap()
        );
        let saved: Value = serde_json::from_slice(
            &std::fs::read(
                adapter
                    .state_dir
                    .join(format!("{}.json", v["request_id"].as_str().unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(saved["state"], "upstream_unconfirmed");
        server.abort();
    }
    #[tokio::test]
    async fn connector_receipt_failure_prevents_any_upstream_request() {
        let (_dir, mut adapter, calls, _seen, server) = fixture(false).await;
        adapter.state_dir = adapter.state_dir.join("missing");
        let value = adapter.forward(request()).await;
        assert_eq!(
            value.structured_content.unwrap()["execution_state"],
            "not_started"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        server.abort();
    }
}

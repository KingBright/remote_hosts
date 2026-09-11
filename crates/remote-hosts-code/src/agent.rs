//! Outbound authenticated device worker; mutations are journaled before execution.
use crate::{
    AgentConfig,
    delivery::Delivery,
    files::{self, Workspace},
    gateway::{DeviceHello, Job},
    scheduler::{ActiveJobs, ActiveResource, Lane, ResourceFilter, Scheduler},
    store::Store,
    terminal::Terminals,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Clone)]
pub struct Agent {
    pub config: Arc<AgentConfig>,
    pub store: Store,
    terminals: Terminals,
    scheduler: Arc<Scheduler>,
    active: Arc<ActiveJobs>,
    progress: Arc<crate::progress::Registry>,
}
#[derive(Serialize, Deserialize)]
struct LocalOperation {
    fingerprint: String,
    state: String,
    result: Option<Value>,
    #[serde(default)]
    resumable: bool,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    updated_at: i64,
}
impl Agent {
    pub async fn new(mut config: AgentConfig) -> Result<Self> {
        crate::validate_url(&config.gateway_url)?;
        uuid::Uuid::parse_str(&config.device_id)?;
        ensure!(config.device_token.len() >= 32, "device token too short");
        ensure!(
            !config.roots.is_empty(),
            "at least one allowed root is required"
        );
        for root in &mut config.roots {
            *root = root.canonicalize()?;
            ensure!(root.is_dir(), "root is not a directory");
        }
        config.state_dir = absolute(&config.state_dir)?;
        ensure!(
            !config.roots.iter().any(|r| config.state_dir.starts_with(r)),
            "agent state must be outside exposed code roots"
        );
        let store = Store::open(&config.state_dir).await?;
        // An interrupted mutation has an unknown outcome; never blindly re-execute it.
        sqlx::query("UPDATE kv SET value=json_set(value,'$.state','unknown') WHERE kind='local_operation' AND json_extract(value,'$.state')='running' AND COALESCE(json_extract(value,'$.resumable'),0)=0").execute(&store.pool).await?;
        let terminals = Terminals::new(
            store.clone(),
            config.state_dir.join("terminals"),
            config.device_token.clone(),
        )
        .await?;
        store.put("runtime", "build", &json!({"version":env!("CARGO_PKG_VERSION"),"pid":std::process::id(),"started_at":crate::now()}), i64::MAX).await?;
        Ok(Self {
            config: Arc::new(config),
            store,
            terminals,
            scheduler: Arc::default(),
            active: Arc::default(),
            progress: Arc::default(),
        })
    }
    pub async fn execute(&self, job: &Job) -> Result<Value> {
        self.execute_with_delivery(job, None).await
    }
    async fn execute_with_delivery(&self, job: &Job, delivery: Option<&Delivery>) -> Result<Value> {
        uuid::Uuid::parse_str(&job.id).context("invalid operation id")?;
        ensure!(job.device_id == self.config.device_id, "wrong device");
        let _guard = self.scheduler.operations.lock(&job.id).await?;
        let fingerprint = crate::hash(serde_json::to_vec(job)?);
        if let Some(op) = self
            .store
            .get::<LocalOperation>("local_operation", &job.id)
            .await?
        {
            ensure!(
                op.fingerprint == fingerprint,
                "operation fingerprint conflict"
            );
            if !(op.resumable && op.result.is_none() && crate::durable_transfer::is_file(&job.tool))
            {
                let result = op.result.unwrap_or_else(||json!({"error":"outcome_unknown","operation_id":job.id,"recovery":"Inspect files, terminal status and local journal. This operation is never automatically repeated after runtime loss."}));
                if let Some(delivery) = delivery {
                    delivery.enqueue(&job.id, &result).await?;
                }
                return Ok(result);
            }
        }
        let progress = self.progress.start(&job.id);
        progress.phase("waiting_resource");
        self.store
            .put(
                "local_operation",
                &job.id,
                &LocalOperation {
                    fingerprint: fingerprint.clone(),
                    state: "running".into(),
                    result: None,
                    resumable: crate::durable_transfer::is_file(&job.tool),
                    workspace_id: job.arguments["workspace_id"].as_str().map(str::to_owned),
                    tool: Some(job.tool.clone()),
                    updated_at: crate::now(),
                },
                i64::MAX,
            )
            .await?;
        progress.phase("running");
        let mut result = match self.perform(job, &progress).await {
            Ok(v) => v,
            Err(e) => {
                crate::diagnostics::error(&job.tool, &e.to_string(), Some(&job.id), "agent_execute")
            }
        };
        if crate::durable_transfer::is_suspended(&result) {
            self.store
                .put(
                    "local_operation",
                    &job.id,
                    &LocalOperation {
                        fingerprint,
                        state: "paused".into(),
                        result: None,
                        resumable: true,
                        workspace_id: job.arguments["workspace_id"].as_str().map(str::to_owned),
                        tool: Some(job.tool.clone()),
                        updated_at: crate::now(),
                    },
                    i64::MAX,
                )
                .await?;
            return Ok(result);
        }
        progress.phase(match result.get("state").and_then(Value::as_str) {
            Some("cancelled") => "cancelled",
            Some("failed") => "failed",
            _ if result.get("error").is_some() => "failed",
            _ => "completed",
        });
        if matches!(job.tool.as_str(), "file_upload" | "file_download") {
            result["progress"] = serde_json::to_value(progress.snapshot())?;
        }
        if result.is_object() {
            result["timing"] = progress.timings();
        }
        if serde_json::to_vec(&result)?.len() > 250 * 1024 {
            result = json!({"error":"response_budget_exceeded","error_code":"response_budget_exceeded","outcome":"unknown","recovery_action":"inspect_original_operation_without_reexecution","automatic_replay_safe":false,"timing":progress.timings()});
        }
        let completed = LocalOperation {
            fingerprint,
            state: "done".into(),
            result: Some(result.clone()),
            resumable: false,
            workspace_id: job.arguments["workspace_id"].as_str().map(str::to_owned),
            tool: Some(job.tool.clone()),
            updated_at: crate::now(),
        };
        if let Some(delivery) = delivery {
            delivery.complete(&job.id, &completed, &result).await?;
        } else {
            self.store
                .put("local_operation", &job.id, &completed, i64::MAX)
                .await?;
        }
        Ok(result)
    }
    async fn perform(&self, job: &Job, progress: &crate::progress::Progress) -> Result<Value> {
        let v = &job.arguments;
        let scope = crate::tools::scope(&job.tool).context("unsupported tool")?;
        ensure!(
            scope != "code:write" || self.config.allow_write,
            "device file writes disabled"
        );
        ensure!(
            scope != "terminal:exec" || self.config.allow_exec,
            "device terminal execution disabled"
        );
        // Revalidate nested types, bounds and required fields at the device boundary.
        crate::tools::validate(&job.tool, v)?;
        if job.tool == "workspace_open" {
            let _permit = self.scheduler.acquire(Lane::Write).await?;
            ensure!(
                files::text(v, "device_id")? == self.config.device_id,
                "wrong device"
            );
            let root = PathBuf::from(files::text(v, "root")?).canonicalize()?;
            ensure!(
                root.is_dir() && self.config.roots.iter().any(|r| root.starts_with(r)),
                "root outside allowed directories"
            );
            let id = format!("{}:{}", self.config.device_id, uuid::Uuid::new_v4());
            let ws = Workspace {
                id: id.clone(),
                device_id: self.config.device_id.clone(),
                root,
            };
            self.store.put("workspace", &id, &ws, i64::MAX).await?;
            return Ok(
                json!({"workspace":ws,"allow_write":self.config.allow_write,"allow_exec":self.config.allow_exec,"agent_version":env!("CARGO_PKG_VERSION"),"file_transfer":true,"terminal_authority":"local_user; not confined by code roots"}),
            );
        }
        let id = files::text(v, "workspace_id")?;
        let ws: Workspace = self
            .store
            .get("workspace", id)
            .await?
            .context("workspace not found on this device")?;
        ensure!(
            ws.device_id == self.config.device_id,
            "workspace device mismatch"
        );
        let canonical = ws.root.canonicalize()?;
        ensure!(
            canonical == ws.root && self.config.roots.iter().any(|r| canonical.starts_with(r)),
            "workspace root moved or authorization revoked"
        );
        progress.phase("waiting_resource");
        let _write = if matches!(
            job.tool.as_str(),
            "code_apply_edits" | "change_resume" | "files_sync"
        ) {
            progress.phase("waiting_resource");
            Some(self.scheduler.writes.acquire(&ws.root).await?)
        } else {
            None
        };
        let _terminal = if job.tool == "terminal_input" {
            Some(
                self.scheduler
                    .terminals
                    .lock(files::text(v, "terminal_id")?)
                    .await?,
            )
        } else {
            None
        };
        // Resource waiters must not consume capacity intended for runnable work.
        // Controls never take the terminal-input lock, so cancellation stays independent.
        let permit = self.scheduler.acquire(Lane::for_tool(&job.tool)).await?;
        progress.phase("running");
        match job.tool.as_str() {
            "file_upload" | "file_download" => {
                crate::durable_transfer::run(
                    &self.config,
                    &ws,
                    job,
                    progress,
                    &self.store,
                    self.scheduler.writes.clone(),
                )
                .await
            }
            "workspace_context" => crate::workspace_context::read(&self.store, &ws, v).await,
            "workspace_gc" => crate::storage_gc::run(&self.config, &self.store, &ws, v).await,
            "terminal_exec" => self.terminals.start(&self.config, &ws, v, &job.id).await,
            "terminal_read" => self.terminals.read(&ws, v).await,
            "terminal_input" => self.terminals.input(&ws, v).await,
            "terminal_cancel" => self.terminals.cancel(&ws, v).await,
            "code_diff" => crate::change_review::read(&ws, v).await,
            "change_resume" => {
                let original = files::text(v, "change_set_id")?;
                uuid::Uuid::parse_str(original).context("invalid change_set_id")?;
                let op: LocalOperation = self
                    .store
                    .get("local_operation", original)
                    .await?
                    .context("change_set_unavailable: original local operation not found")?;
                ensure!(
                    op.tool.as_deref() == Some("code_apply_edits")
                        && op.workspace_id.as_deref() == Some(&ws.id),
                    "change_set_unavailable: original edit belongs to another workspace or tool"
                );
                let journal = self
                    .config
                    .state_dir
                    .join("edits")
                    .join(format!("{original}.json"));
                let ws = ws.clone();
                let progress = progress.clone();
                tokio::task::spawn_blocking(move || {
                    let _write_guard = _write;
                    let _execution_permit = permit;
                    progress.phase("running");
                    files::resume(&ws, &journal)
                })
                .await?
            }
            name => {
                let name = name.to_owned();
                let v = v.clone();
                let journal = self
                    .config
                    .state_dir
                    .join("edits")
                    .join(format!("{}.json", job.id));
                let progress = progress.clone();
                tokio::task::spawn_blocking(move || {
                    // Keep the write guard until the actual blocking edit finishes,
                    // even if its async observer is cancelled.
                    let _write_guard = _write;
                    let _execution_permit = permit;
                    progress.phase("running");
                    match name.as_str() {
                        "code_list" => files::list(&ws, &v),
                        "code_search" => files::search(&ws, &v),
                        "code_read" => files::read(&ws, &v),
                        "code_symbols" => files::symbols(&ws, &v),
                        "code_apply_edits" => files::apply(&ws, &v, &journal),
                        "files_sync" => crate::files_sync::run(&ws, &v, &journal),
                        _ => bail!("unsupported device tool"),
                    }
                })
                .await?
            }
        }
    }
    pub async fn run(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        let delivery = Delivery::new(self.store.clone(), self.config.clone()).await?;
        let hello = DeviceHello {
            version: env!("CARGO_PKG_VERSION").into(),
            session: crate::random(),
            roots: self
                .config
                .roots
                .iter()
                .map(|p| p.to_string_lossy().into())
                .collect(),
            allow_write: self.config.allow_write,
            allow_exec: self.config.allow_exec,
            transfer_limits: Some(crate::capabilities::TransferLimits::current()),
        };
        // A launchd process marker is not readiness. Only successful authenticated
        // poll round-trips populate the per-lane timestamps for this exact session.
        self.store
            .put(
                "runtime",
                "readiness",
                &json!({"pid":std::process::id(),
            "session":hello.session,"version":hello.version,"started_at":crate::now(),
            "phase":"gateway_wait","lanes":{}}),
                i64::MAX,
            )
            .await?;
        let mut attempt = 0u32;
        loop {
            let health = async {
                client
                    .get(format!("{}/healthz", self.config.gateway_url))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await
            }
            .await;
            if health.is_ok_and(|v| {
                v["dispatch_protocol"] == 2
                    && v["resource_dispatch_protocol"] == 1
                    && v["transfer_protocol"] == 2
            }) {
                break;
            }
            attempt = attempt.saturating_add(1);
            // No URL, body or credentials in diagnostics. Stay in this process on
            // transient startup outages instead of creating a launchd restart loop.
            tracing::warn!(
                attempt,
                "gateway not ready or protocol incompatible; retrying startup"
            );
            tokio::time::sleep(Duration::from_secs(2u64.pow(attempt.min(4)))).await;
        }
        tokio::try_join!(
            self.run_lane(&client, &hello, Lane::Read, &delivery),
            self.run_lane(&client, &hello, Lane::Write, &delivery),
            self.run_lane(&client, &hello, Lane::Transfer, &delivery),
            self.run_lane(&client, &hello, Lane::Terminal, &delivery),
            self.run_lane(&client, &hello, Lane::Control, &delivery),
            self.heartbeat_loop(&client, &hello),
            delivery.run(&client),
        )?;
        Ok(())
    }
    async fn run_lane(
        &self,
        client: &reqwest::Client,
        hello: &DeviceHello,
        lane: Lane,
        delivery: &Delivery,
    ) -> Result<()> {
        let mut workers = tokio::task::JoinSet::new();
        let mut failures = 0u32;
        loop {
            while let Some(result) = workers.try_join_next() {
                Self::worker_result(result);
            }
            if workers.len() >= lane.limit() {
                if let Some(result) = workers.join_next().await {
                    Self::worker_result(result);
                }
                continue;
            }
            match self.poll_job(client, hello, lane).await {
                Ok(Some(job)) => {
                    failures = 0;
                    // Reserve the job's canonical scope before the next poll,
                    // not after its worker happens to get CPU time.
                    let resource = self.resource_hint(&job).await;
                    let Some(lease) = self.active.enter_scoped(&job.id, resource)? else {
                        continue;
                    };
                    let agent = self.clone();
                    let delivery = delivery.clone();
                    workers.spawn(async move {
                        let _lease = lease;
                        agent.execute_report(&delivery, job).await
                    });
                }
                Ok(None) => {
                    failures = 0;
                }
                Err(_) => {
                    failures = failures.saturating_add(1);
                    // Errors can contain URLs or request details; log only lane/attempt.
                    tracing::warn!(?lane, attempt = failures, "device polling unavailable");
                    tokio::time::sleep(Duration::from_secs(2u64.pow(failures.min(5)))).await;
                }
            }
        }
    }
    fn worker_result(result: std::result::Result<Result<()>, tokio::task::JoinError>) {
        if !matches!(result, Ok(Ok(()))) {
            tracing::warn!("device worker failed; durable journal retained for recovery");
        }
    }
    async fn heartbeat_loop(&self, client: &reqwest::Client, hello: &DeviceHello) -> Result<()> {
        let mut acknowledged_terminal_fingerprint = String::new();
        let mut last_reconcile = tokio::time::Instant::now() - Duration::from_secs(30);
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if last_reconcile.elapsed() >= Duration::from_secs(15) {
                match self.terminals.reconcile_orphans(30).await {
                    Ok(count) if count > 0 => {
                        tracing::warn!(count, "reconciled orphaned terminal state")
                    }
                    Ok(_) => {}
                    Err(_) => tracing::warn!("terminal state reconciliation failed; will retry"),
                }
                last_reconcile = tokio::time::Instant::now();
            }
            let active = self.active.list()?;
            let terminals = crate::terminal_sync::collect(&self.store)
                .await
                .unwrap_or_default();
            let fingerprint = crate::hash(serde_json::to_vec(&terminals)?);
            let running_terminal = terminals
                .iter()
                .any(|s| matches!(s.state.as_str(), "starting" | "running"));
            if active.is_empty()
                && !running_terminal
                && fingerprint == acknowledged_terminal_fingerprint
            {
                continue;
            }
            let mut request = serde_json::to_value(hello)?;
            request["active_operations"] = json!(active);
            request["progress"] = json!(self.progress.snapshots());
            request["terminal_updates"] = json!(terminals);
            let response = client
                .post(format!("{}/device/heartbeat", self.config.gateway_url))
                .bearer_auth(&self.config.device_token)
                .json(&request)
                .timeout(Duration::from_secs(5))
                .send()
                .await;
            if response.is_ok_and(|r| r.status().is_success()) {
                acknowledged_terminal_fingerprint = fingerprint;
            }
        }
    }
    async fn resource_hint(&self, job: &Job) -> ActiveResource {
        let mut resource = ActiveResource::default();
        if matches!(
            job.tool.as_str(),
            "code_apply_edits" | "change_resume" | "files_sync"
        ) && let Some(id) = job.arguments.get("workspace_id").and_then(Value::as_str)
            && let Ok(Some(ws)) = self.store.get::<Workspace>("workspace", id).await
            && ws.device_id == self.config.device_id
        {
            resource.write_root = Some(ws.root);
        }
        if job.tool == "terminal_input"
            && let Some(id) = job.arguments.get("terminal_id").and_then(Value::as_str)
            && uuid::Uuid::parse_str(id).is_ok()
        {
            resource.terminal_input = Some(id.to_owned());
        }
        resource
    }
    async fn resource_filter(&self, lane: Lane) -> Result<ResourceFilter> {
        let mut filter = ResourceFilter::default();
        if !matches!(lane, Lane::Write | Lane::Terminal) {
            return Ok(filter);
        }
        let active = self.active.resources()?;
        if lane == Lane::Terminal {
            filter.terminal_inputs = active
                .into_iter()
                .filter_map(|r| r.terminal_input)
                .collect();
            return Ok(filter);
        }
        let mut roots = self.scheduler.writes.roots()?;
        roots.extend(active.into_iter().filter_map(|r| r.write_root));
        roots.sort();
        roots.dedup();
        if roots.is_empty() {
            return Ok(filter);
        }
        // Resolve aliases and ancestors using canonical stored roots, not guessed
        // logical IDs. Path-component separators prevent a/foo blocking a/foobar.
        let separator = std::path::MAIN_SEPARATOR.to_string();
        let rows: Vec<(String,)> = sqlx::query_as("SELECT key FROM kv WHERE kind='workspace' AND expires>? AND json_extract(value,'$.device_id')=? AND EXISTS (SELECT 1 FROM json_each(?) AS busy WHERE instr(rtrim(json_extract(kv.value,'$.root'),?) || ?, rtrim(busy.value,?) || ?)=1 OR instr(rtrim(busy.value,?) || ?, rtrim(json_extract(kv.value,'$.root'),?) || ?)=1) ORDER BY key LIMIT 1025")
            .bind(crate::now()).bind(&self.config.device_id).bind(serde_json::to_string(&roots)?)
            .bind(&separator).bind(&separator).bind(&separator).bind(&separator)
            .bind(&separator).bind(&separator).bind(&separator).bind(&separator)
            .fetch_all(&self.store.pool).await?;
        if rows.len() > 1024 {
            // Explicit bounded fallback for pathological alias counts. Reads and
            // controls stay available; no partial alias list is silently trusted.
            filter.all_writes = true;
        } else {
            filter.write_workspaces = rows.into_iter().map(|(id,)| id).collect();
        }
        Ok(filter)
    }
    async fn poll_job(
        &self,
        client: &reqwest::Client,
        hello: &DeviceHello,
        lane: Lane,
    ) -> Result<Option<Job>> {
        let mut request = serde_json::to_value(hello)?;
        request["lanes"] = json!([lane]);
        let filter = self.resource_filter(lane).await?;
        request["poll_wait_ms"] = json!(if filter.is_empty() { 20000 } else { 250 });
        request["resource_filter"] = serde_json::to_value(filter)?;
        request["runtime_features"] = json!(crate::capabilities::RuntimeFeatures::current());
        request["terminal_updates"] = json!(
            crate::terminal_sync::collect(&self.store)
                .await
                .unwrap_or_default()
        );
        request["receipt_delivery"] = json!(
            self.store
                .get::<crate::delivery::Status>("runtime", "receipt_delivery")
                .await
                .ok()
                .flatten()
                .filter(crate::delivery::Status::valid)
        );
        request["active_operations"] = json!(self.active.list()?);
        request["progress"] = json!(self.progress.snapshots());
        let response = client
            .post(format!("{}/device/poll", self.config.gateway_url))
            .bearer_auth(&self.config.device_token)
            .json(&request)
            .send()
            .await
            .context("device poll transport failed")?;
        ensure!(
            response.status().is_success(),
            "device poll rejected ({})",
            response.status()
        );
        let bytes = response.bytes().await?;
        ensure!(bytes.len() <= 512 * 1024, "oversized gateway response");
        let response: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            response.get("job").is_some(),
            "gateway response missing job field"
        );
        let job = if response["job"].is_null() {
            None
        } else {
            let job: Job = serde_json::from_value(response["job"].clone())?;
            ensure!(
                Lane::for_tool(&job.tool) == lane && job.device_id == self.config.device_id,
                "gateway ignored execution lane or device"
            );
            Some(job)
        };
        let lane_name = serde_json::to_value(lane)?
            .as_str()
            .context("invalid lane")?
            .to_owned();
        // Telemetry persistence failure must not discard an already received job.
        if sqlx::query("UPDATE kv SET value=json_set(value,'$.phase','polling',?,?, '$.updated_at',?) WHERE kind='runtime' AND key='readiness' AND json_extract(value,'$.session')=?")
            .bind(format!("$.lanes.{lane_name}")).bind(crate::now()).bind(crate::now()).bind(&hello.session)
            .execute(&self.store.pool).await.is_err() {
            tracing::warn!("readiness persistence unavailable; work continues");
        }
        Ok(job)
    }
    async fn execute_report(&self, delivery: &Delivery, job: Job) -> Result<()> {
        // Releasing an execution worker does not discard delivery intent. The
        // independent sender owns retries, including after an Agent restart.
        self.execute_with_delivery(&job, Some(delivery)).await?;
        Ok(())
    }
}
fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.into())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(test)]
#[path = "agent_scheduling_tests.rs"]
mod scheduling_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn change_resume_routes_only_to_original_workspace_change_set() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let device = uuid::Uuid::new_v4().to_string();
        let a = Agent::new(AgentConfig {
            gateway_url: "https://example.com:8443".into(),
            device_id: device.clone(),
            device_token: crate::random(),
            state_dir: state.path().into(),
            roots: vec![root.path().into()],
            allow_write: true,
            allow_exec: false,
            shell: "/bin/sh".into(),
        })
        .await
        .unwrap();
        let ws = Workspace {
            id: format!("{}:{}", device, uuid::Uuid::new_v4()),
            device_id: device.clone(),
            root: root.path().canonicalize().unwrap(),
        };
        a.store
            .put("workspace", &ws.id, &ws, i64::MAX)
            .await
            .unwrap();
        let original = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: device.clone(),
            owner: "owner".into(),
            tool: "code_apply_edits".into(),
            arguments: json!({"workspace_id":ws.id,"idempotency_key":"edit","files":[{"path":"a.txt","expected_version":"absent","action":"create","content":"hello"}]}),
        };
        let first = a.execute(&original).await.unwrap();
        assert_eq!(first["state"], "completed");
        assert_eq!(first["change_set"]["change_set_id"], original.id);
        let resume = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: device,
            owner: "owner".into(),
            tool: "change_resume".into(),
            arguments: json!({"workspace_id":ws.id,"idempotency_key":"resume","change_set_id":original.id}),
        };
        let second = a.execute(&resume).await.unwrap();
        assert_eq!(second["state"], "completed");
        assert_eq!(second["already_applied"], 1);
        assert_eq!(
            std::fs::read_to_string(root.path().join("a.txt")).unwrap(),
            "hello"
        );
    }
    #[tokio::test]
    async fn deduplicates_mutations_and_rejects_other_devices() {
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let device = uuid::Uuid::new_v4().to_string();
        let a = Agent::new(AgentConfig {
            gateway_url: "https://example.com:8443".into(),
            device_id: device.clone(),
            device_token: crate::random(),
            state_dir: state.path().into(),
            roots: vec![root.path().into()],
            allow_write: true,
            allow_exec: false,
            shell: "/bin/sh".into(),
        })
        .await
        .unwrap();
        let job = Job {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: device,
            owner: "owner".into(),
            tool: "workspace_open".into(),
            arguments: json!({"device_id":a.config.device_id,"root":root.path(),"idempotency_key":"open"}),
        };
        let first = a.execute(&job).await.unwrap();
        assert_eq!(first, a.execute(&job).await.unwrap());
        let mut bad = job;
        bad.device_id = uuid::Uuid::new_v4().to_string();
        assert!(a.execute(&bad).await.is_err());
    }
}

//! Explicit gateway schema identity and agent-reported features, never version guesses.
use crate::{hash, tools};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::LazyLock;

static CATALOG_SHA: LazyLock<String> =
    LazyLock::new(|| hash(serde_json::to_vec(&tools::catalog()).expect("static catalog")));
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferLimits {
    pub protocol: u32,
    pub default_max_bytes: u64,
    pub hard_max_bytes: u64,
    pub checkpoint_bytes: u64,
}
impl TransferLimits {
    pub fn current() -> Self {
        Self {
            protocol: 1,
            default_max_bytes: crate::transfers::DEFAULT_MAX_BYTES as u64,
            hard_max_bytes: crate::transfers::MAX_BYTES as u64,
            checkpoint_bytes: crate::transfer_receiver::CHUNK as u64,
        }
    }
    pub fn valid(&self) -> bool {
        self.protocol == 1
            && self.default_max_bytes > 0
            && self.default_max_bytes <= self.hard_max_bytes
            && self.hard_max_bytes <= crate::transfers::MAX_BYTES as u64
            && self.checkpoint_bytes == crate::transfer_receiver::CHUNK as u64
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFeatures {
    pub protocol: u32,
    pub names: Vec<String>,
}
impl RuntimeFeatures {
    pub fn current() -> Self {
        Self {
            protocol: 1,
            names: [
                "terminal_wait_v1",
                "line_fragments_v1",
                "partial_reads_v1",
                "request_snapshot_v1",
                "poll_readiness_v1",
                "resource_filter_v1",
                "durable_receipts_v1",
                "durable_transfers_v2",
                "transfer_cancel_v1",
                "transfer_resume_v1",
                "workspace_context_v1",
                "workspace_context_pages_v1",
                "workspace_events_v1",
                "files_sync_v1",
                "complete_diff_v1",
                "terminal_observation_v1",
                "diagnostics_v1",
                "operation_lifecycle_v1",
                "change_set_resume_v1",
                "workspace_gc_v1",
                "large_file_transfer_v1",
                "source_authorization_status_v1",
                "terminal_reconcile_v1",
                "transfer_recovery_guards_v1",
            ]
            .map(str::to_owned)
            .to_vec(),
        }
    }
    pub fn valid(&self) -> bool {
        self.protocol == 1
            && self.names.len() <= 32
            && self.names.iter().all(|s| {
                !s.is_empty()
                    && s.len() <= 64
                    && s.bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
            })
    }
}
pub(crate) fn gateway_manifest(known: Option<&str>) -> Value {
    json!({"version":env!("CARGO_PKG_VERSION"),"tools_sha256":*CATALOG_SHA,
        "tool_count":tools::catalog().len(),"dispatch_protocol":2,"readiness_protocol":1,
        "observation_protocol":2,"capabilities_protocol":1,"schema_diagnostics_protocol":1,"resource_dispatch_protocol":1,"transfer_protocol":2,"transfer_limits_protocol":1,"maintenance_protocol":1,"terminal_observation_protocol":1,"change_set_protocol":1,"storage_gc_protocol":1,"checkpoint_bytes":crate::transfer_receiver::CHUNK,"default_file_bytes":crate::transfers::DEFAULT_MAX_BYTES,"max_file_bytes":crate::transfers::MAX_BYTES,"storage_reserve_bytes":crate::transfers::STORAGE_RESERVE_BYTES,
        "tool_constraints":{"file_upload":{"default_max_bytes":crate::transfers::DEFAULT_MAX_BYTES,"hard_max_bytes":crate::transfers::MAX_BYTES,"over_default_requires_agent_feature":"large_file_transfer_v1"},"file_download":{"default_max_bytes":crate::transfers::DEFAULT_MAX_BYTES,"hard_max_bytes":crate::transfers::MAX_BYTES,"over_default_requires_agent_feature":"large_file_transfer_v1"}},
        "optional_inputs":{"all_tools":["response_mode"],"operation_get":["operation_ids","wait_ms","cursor","max_bytes"],
            "terminal_exec":["wait_ms"],"code_read":["allow_partial","requests[].line_byte_offset"],
            "devices_list":["known_tools_sha256"],
            "workspace_context":["active_only","terminal_cursor","transfer_after","cursor","after_event"],"code_diff":["include_untracked","expected_version"],"files_sync":["mode","manifest_id","bundle_path","bundle_sha256"],"change_resume":["change_set_id"],"workspace_gc":["action","older_than_seconds","max_items","preview_id"]},
        "client_schema_comparison":match known { None=>"not_provided",Some(s) if s==*CATALOG_SHA=>"match",Some(_)=>"mismatch" },
        "host_schema_status":match known { None=>"unknown_not_reported",Some(s) if s==*CATALOG_SHA=>"current",Some(_)=>"stale" },
        "refresh_required":known.is_some_and(|s|s!=*CATALOG_SHA),
        "refresh_guidance":"Compare the supplied client catalog hash. A mismatch needs host-side tool refresh; this server cannot refresh the conversation schema. Agent feature reports are separate."})
}

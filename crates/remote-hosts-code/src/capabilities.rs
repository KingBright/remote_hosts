//! Explicit gateway schema identity and agent-reported features, never version guesses.
use crate::{hash, tools};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::LazyLock;

static CATALOG_SHA: LazyLock<String> =
    LazyLock::new(|| hash(serde_json::to_vec(&tools::catalog()).expect("static catalog")));

/// Device/gateway wire protocol. New agents require this protocol from the gateway;
/// new gateways still accept the immediately previous legacy agent protocol so the
/// gateway can be upgraded first without taking the fleet offline.
pub(crate) const WIRE_PROTOCOL: u32 = 2;
pub(crate) const MIN_AGENT_WIRE_PROTOCOL: u32 = 1;
pub(crate) const MIN_GATEWAY_WIRE_PROTOCOL: u32 = 2;

pub(crate) fn tool_schema_revision() -> &'static str {
    CATALOG_SHA.as_str()
}

const EMBEDDED_SKILL: &[(&str, &str)] = &[
    (
        "SKILL.md",
        include_str!("../../../skills/remote-hosts-agent/SKILL.md"),
    ),
    (
        "agents/openai.yaml",
        include_str!("../../../skills/remote-hosts-agent/agents/openai.yaml"),
    ),
    (
        "references/connection-and-errors.md",
        include_str!("../../../skills/remote-hosts-agent/references/connection-and-errors.md"),
    ),
    (
        "references/host-registry.md",
        include_str!("../../../skills/remote-hosts-agent/references/host-registry.md"),
    ),
    (
        "references/instance-sync.md",
        include_str!("../../../skills/remote-hosts-agent/references/instance-sync.md"),
    ),
    (
        "references/mcp-workflows.md",
        include_str!("../../../skills/remote-hosts-agent/references/mcp-workflows.md"),
    ),
    (
        "references/minio-relay.md",
        include_str!("../../../skills/remote-hosts-agent/references/minio-relay.md"),
    ),
    (
        "references/setup-and-runtime.md",
        include_str!("../../../skills/remote-hosts-agent/references/setup-and-runtime.md"),
    ),
    (
        "references/topology-and-inventory.md",
        include_str!("../../../skills/remote-hosts-agent/references/topology-and-inventory.md"),
    ),
];
static SKILL_SHA: LazyLock<String> =
    LazyLock::new(|| hash(serde_json::to_vec(EMBEDDED_SKILL).expect("embedded Skill bundle")));

pub(crate) fn embedded_skill_revision() -> &'static str {
    SKILL_SHA.as_str()
}

pub(crate) fn sync_embedded_skill() -> std::io::Result<()> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "home directory unavailable")
        })?;
    sync_embedded_skill_at(&home)
}

pub(crate) fn sync_embedded_skill_at(home: &std::path::Path) -> std::io::Result<()> {
    for target in [
        home.join(".codex/skills/remote-hosts-agent"),
        home.join(".gemini/config/skills/remote-hosts-agent"),
    ] {
        for (relative, content) in EMBEDDED_SKILL {
            let path = target.join(relative);
            let Some(parent) = path.parent() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "embedded skill path has no parent",
                ));
            };
            std::fs::create_dir_all(parent)?;
            if std::fs::read(&path).ok().as_deref() == Some(content.as_bytes()) {
                continue;
            }
            let temporary = parent.join(format!(
                ".{}.tmp-{}",
                path.file_name().and_then(|v| v.to_str()).unwrap_or("skill"),
                std::process::id()
            ));
            std::fs::write(&temporary, content.as_bytes())?;
            #[cfg(windows)]
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            std::fs::rename(&temporary, &path)?;
        }
    }
    Ok(())
}

pub(crate) fn installed_skill_revision() -> (Option<String>, Option<bool>) {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    let Some(home) = home else {
        return (None, None);
    };
    installed_skill_revision_at(&std::path::PathBuf::from(home))
}

pub(crate) fn installed_skill_revision_at(
    home: &std::path::Path,
) -> (Option<String>, Option<bool>) {
    let bundle_hash = |root: std::path::PathBuf| -> Option<String> {
        let files: Option<Vec<_>> = EMBEDDED_SKILL
            .iter()
            .map(|(path, _)| {
                std::fs::read_to_string(root.join(path))
                    .ok()
                    .map(|text| (*path, text))
            })
            .collect();
        Some(hash(serde_json::to_vec(&files?).ok()?))
    };
    let codex = bundle_hash(home.join(".codex/skills/remote-hosts-agent"));
    let antigravity = bundle_hash(home.join(".gemini/config/skills/remote-hosts-agent"));
    let revision = codex.clone().or_else(|| antigravity.clone());
    let consistent = match (&codex, &antigravity) {
        (Some(left), Some(right)) => Some(left == right),
        (None, None) => None,
        _ => Some(false),
    };
    (revision, consistent)
}

pub(crate) fn compatibility(agent_wire: Option<u32>) -> Value {
    let wire = agent_wire.unwrap_or(1);
    let status = if wire < MIN_AGENT_WIRE_PROTOCOL {
        "agent_too_old"
    } else if wire > WIRE_PROTOCOL {
        "agent_too_new"
    } else if wire == WIRE_PROTOCOL {
        "compatible"
    } else {
        "legacy_compatible"
    };
    json!({
        "status": status,
        "agent_wire_protocol": wire,
        "gateway_wire_protocol": WIRE_PROTOCOL,
        "min_agent_wire_protocol": MIN_AGENT_WIRE_PROTOCOL,
        "max_agent_wire_protocol": WIRE_PROTOCOL
    })
}
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
                "token_compaction_v1",
                "wire_compatibility_v1",
                "fleet_status_v1",
                "skill_revision_v1",
                "semantic_outcome_guard_v1",
                "diagnostics_v2",
                "bounded_observe_defaults_v1",
                "terminal_preview_v1",
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
    let comparison = match known {
        None => "not_provided",
        Some(value) if value == tool_schema_revision() => "match",
        Some(_) => "mismatch",
    };
    // A model-supplied known server hash is only a comparison hint. Actual
    // adapter/host exposure is reported by the authenticated transport separately.
    let host_status = "unknown_not_reported";
    let mut manifest = serde_json::Map::new();
    for (key, value) in [
        ("version", json!(env!("CARGO_PKG_VERSION"))),
        ("wire_protocol", json!(WIRE_PROTOCOL)),
        ("skill_revision", json!(embedded_skill_revision())),
        ("min_agent_wire_protocol", json!(MIN_AGENT_WIRE_PROTOCOL)),
        ("max_agent_wire_protocol", json!(WIRE_PROTOCOL)),
        (
            "min_gateway_wire_protocol",
            json!(MIN_GATEWAY_WIRE_PROTOCOL),
        ),
        ("tools_sha256", json!(tool_schema_revision())),
        ("tool_schema_revision", json!(tool_schema_revision())),
        ("tool_count", json!(tools::catalog().len())),
        ("dispatch_protocol", json!(2)),
        ("progress_protocol", json!(1)),
        ("readiness_protocol", json!(1)),
        ("fleet_protocol", json!(1)),
        ("skill_revision_protocol", json!(1)),
        ("outcome_guard_protocol", json!(1)),
        ("observation_protocol", json!(2)),
        ("capabilities_protocol", json!(2)),
        ("schema_diagnostics_protocol", json!(2)),
        ("admin_status_protocol", json!(1)),
        ("request_receipt_protocol", json!(2)),
        ("resource_dispatch_protocol", json!(1)),
        ("transfer_protocol", json!(2)),
        ("transfer_limits_protocol", json!(1)),
        ("maintenance_protocol", json!(1)),
        ("terminal_observation_protocol", json!(2)),
        ("change_set_protocol", json!(1)),
        ("storage_gc_protocol", json!(1)),
        ("checkpoint_bytes", json!(crate::transfer_receiver::CHUNK)),
        (
            "default_file_bytes",
            json!(crate::transfers::DEFAULT_MAX_BYTES),
        ),
        ("max_file_bytes", json!(crate::transfers::MAX_BYTES)),
        (
            "storage_reserve_bytes",
            json!(crate::transfers::STORAGE_RESERVE_BYTES),
        ),
        ("client_schema_comparison", json!(comparison)),
        ("host_schema_status", json!(host_status)),
        (
            "refresh_required",
            json!(known.is_some_and(|value| value != tool_schema_revision())),
        ),
    ] {
        manifest.insert(key.into(), value);
    }
    manifest.insert(
        "tool_constraints".into(),
        json!({
            "file_upload": {
                "default_max_bytes": crate::transfers::DEFAULT_MAX_BYTES,
                "hard_max_bytes": crate::transfers::MAX_BYTES,
                "over_default_requires_agent_feature": "large_file_transfer_v1"
            },
            "file_download": {
                "default_max_bytes": crate::transfers::DEFAULT_MAX_BYTES,
                "hard_max_bytes": crate::transfers::MAX_BYTES,
                "over_default_requires_agent_feature": "large_file_transfer_v1"
            }
        }),
    );
    manifest.insert(
        "optional_inputs".into(),
        json!({
            "all_tools": ["response_mode"],
            "operation_get": ["request_id", "operation_ids", "wait_ms", "cursor", "terminal_cursor", "max_bytes"],
            "terminal_exec": ["wait_ms"],
            "terminal_read": ["output_mode"],
            "code_read": ["allow_partial", "requests[].line_byte_offset"],
            "devices_list": ["known_tools_sha256"],
            "workspace_context": ["active_only", "terminal_cursor", "transfer_after", "cursor", "after_event"],
            "code_diff": ["include_untracked", "expected_version"],
            "files_sync": ["mode", "manifest_id", "bundle_path", "bundle_sha256"],
            "change_resume": ["change_set_id"],
            "workspace_gc": ["action", "older_than_seconds", "max_items", "preview_id"]
        }),
    );
    manifest.insert(
        "refresh_guidance".into(),
        json!("Compare the supplied client catalog hash. A mismatch needs host-side tool refresh; this server cannot refresh the conversation schema. Agent feature reports are separate."),
    );
    Value::Object(manifest)
}

/// Static machine-readable contract used by health, release packaging and host adapters.
/// Runtime/session comparison fields are deliberately excluded so every consumer hashes
/// and validates the same server-owned capability truth instead of copying constants.
pub fn release_manifest() -> Value {
    let mut manifest = gateway_manifest(None);
    if let Some(object) = manifest.as_object_mut() {
        for key in [
            "client_schema_comparison",
            "host_schema_status",
            "refresh_required",
            "refresh_guidance",
        ] {
            object.remove(key);
        }
        object.insert("machine_contract_protocol".into(), json!(1));
    }
    manifest
}

#[cfg(test)]
mod release_manifest_tests {
    use super::*;

    #[test]
    fn release_manifest_is_static_and_derived_from_live_catalog() {
        let manifest = release_manifest();
        assert_eq!(manifest["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(manifest["tool_count"], tools::catalog().len());
        assert_eq!(manifest["tool_schema_revision"], tool_schema_revision());
        assert_eq!(manifest["tools_sha256"], tool_schema_revision());
        assert_eq!(manifest["terminal_observation_protocol"], 2);
        assert_eq!(manifest["request_receipt_protocol"], 2);
        assert_eq!(manifest["machine_contract_protocol"], 1);
        assert!(manifest.get("host_schema_status").is_none());
        assert!(manifest.get("client_schema_comparison").is_none());
        assert!(manifest.get("refresh_required").is_none());
    }
}

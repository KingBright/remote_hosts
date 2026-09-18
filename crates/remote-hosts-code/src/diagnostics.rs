//! Stable recovery guidance. Never serialize raw commands, URLs, or OS error chains.
use crate::now;
use serde_json::{Value, json};
pub(crate) fn error(tool: &str, message: &str, operation: Option<&str>, stage: &str) -> Value {
    let (code, recovery, outcome) = if message.contains("device_draining") {
        (
            "device_draining",
            "observe_original_updater",
            "not_executed",
        )
    } else if message.starts_with("invalid_arguments") {
        ("invalid_arguments", "correct_arguments", "not_executed")
    } else if message.contains("source_authorization_required") {
        (
            "source_authorization_required",
            "refresh_original_file_authorization_with_transfer_resume",
            "not_executed_or_paused",
        )
    } else if message.contains("file_source_rejected") {
        (
            "file_source_rejected",
            "inspect_original_file_identity_and_authorization",
            "not_executed_or_paused",
        )
    } else if message.contains("storage_capacity_insufficient")
        || message.contains("gateway_storage_limit")
        || message.contains("transfer_storage_limit")
    {
        (
            "storage_capacity_insufficient",
            "free_storage_or_request_a_smaller_transfer",
            "not_executed_or_paused",
        )
    } else if message.contains("device_feature_unavailable") {
        (
            "device_feature_unavailable",
            "upgrade_selected_device_and_refresh_capabilities",
            "not_executed",
        )
    } else if message.contains("change_set_unavailable")
        || message.contains("change_set workspace identity conflict")
    {
        (
            "change_set_unavailable",
            "inspect_workspace_change_sets_and_original_edit",
            "not_executed",
        )
    } else if message.contains("gc_preview_changed") {
        (
            "gc_preview_changed",
            "run_workspace_gc_preview_again_before_apply",
            "not_executed",
        )
    } else if message.contains("operation_unavailable") || message.contains("operation not found") {
        (
            "operation_unavailable",
            "inspect_original_operation_identifiers_without_reexecution",
            "not_executed",
        )
    } else if message.contains("version_conflict") || message.contains("unique match") {
        (
            "version_conflict",
            "read_current_version_and_merge",
            "not_executed_or_partial",
        )
    } else if message.contains("idempotency_conflict") || message.contains("fingerprint conflict") {
        (
            "idempotency_conflict",
            "observe_original_operation",
            "not_executed",
        )
    } else if message.contains("device_offline") {
        (
            "device_offline",
            "reconnect_selected_device",
            "not_executed",
        )
    } else if message.contains("cursor_expired") || message.contains("cursor") {
        (
            "invalid_or_expired_cursor",
            "refresh_scoped_snapshot",
            "not_executed",
        )
    } else if message.contains("scope")
        || message.contains("permission")
        || message.contains("authorized")
        || message.contains("authorization revoked")
        || message.contains("outside allowed")
    {
        (
            "access_denied",
            "check_original_device_and_permissions",
            "not_executed_or_partial",
        )
    } else if message.contains("workspace not found") {
        (
            "workspace_unavailable",
            "open_original_device_workspace",
            "not_executed",
        )
    } else if message.contains("budget")
        || message.contains("too large")
        || message.contains("limit")
    {
        (
            "capacity_limit",
            "narrow_request_and_observe_original",
            "unknown",
        )
    } else {
        (
            "execution_failed",
            "observe_original_and_inspect_scoped_evidence",
            "unknown",
        )
    };
    let (failure_boundary, last_confirmed_stage) = match stage {
        "gateway_dispatch" => ("gateway", "gateway_received_request"),
        "agent_execute" => ("device", "device_received_operation"),
        "host" => ("host", "host_observed_request"),
        _ => ("connector", "connector_observed_request"),
    };
    let execution_state = match outcome {
        "not_executed" => "not_started",
        "not_executed_or_paused" => "not_started_or_paused",
        "not_executed_or_partial" => "not_started_or_partial",
        _ => "unknown",
    };
    let retry_policy = match code {
        "invalid_arguments" => "retry_only_after_correcting_arguments",
        "device_offline" | "device_feature_unavailable" | "storage_capacity_insufficient" => {
            "retry_only_after_reported_condition_changes"
        }
        "idempotency_conflict" | "operation_unavailable" => "do_not_replay_observe_original",
        "version_conflict" | "change_set_unavailable" | "gc_preview_changed" => {
            "do_not_replay_reconcile_state_first"
        }
        _ if outcome == "not_executed" => "safe_only_after_confirming_precondition",
        _ => "do_not_replay_until_original_outcome_is_observed",
    };
    let user_action = match code {
        "device_offline" => "reconnect_selected_device",
        "source_authorization_required" => "refresh_original_file_authorization",
        "storage_capacity_insufficient" => "free_storage_or_reduce_request",
        "invalid_arguments" => "correct_arguments",
        "access_denied" => "check_account_and_device_permissions",
        _ => "none_until_observation_or_recovery_guidance_requires_it",
    };
    json!({"error":"tool_failed","error_code":code,"message":code,
        "stage":stage,"failure_boundary":failure_boundary,"last_confirmed_stage":last_confirmed_stage,
        "execution_state":execution_state,"outcome":outcome,"recovery_action":recovery,
        "retry_policy":retry_policy,"user_action":user_action,"observed_at":now(),
        "evidence":{"error_code":code,"sanitized":true},
        "automatic_replay_safe":false,"operation_id":operation,"tool":tool,"diagnostic_protocol":2})
}
pub(crate) fn error_with_request(
    tool: &str,
    message: &str,
    request_id: &str,
    operation: Option<&str>,
    stage: &str,
) -> Value {
    let mut value = error(tool, message, operation, stage);
    value["request_id"] = json!(request_id);
    value
}
#[cfg(test)]
mod tests {
    #[test]
    fn errors_never_expose_input_values() {
        let v = super::error(
            "terminal_exec",
            "failure https://host/?token=secret command-password",
            Some("id"),
            "execute",
        );
        let s = v.to_string();
        assert!(!s.contains("secret"));
        assert!(!s.contains("command-password"));
        assert_eq!(v["outcome"], "unknown");
        assert_eq!(v["diagnostic_protocol"], 2);
        assert_eq!(v["failure_boundary"], "connector");
        assert_eq!(v["last_confirmed_stage"], "connector_observed_request");
        assert_eq!(v["execution_state"], "unknown");
        assert_eq!(
            v["retry_policy"],
            "do_not_replay_until_original_outcome_is_observed"
        );
        assert_eq!(v["evidence"]["sanitized"], true);
        assert!(v["observed_at"].as_i64().is_some());
        assert_eq!(v["automatic_replay_safe"], false);
    }
    #[test]
    fn request_receipt_is_distinct_from_remote_operation() {
        let value = super::error_with_request(
            "code_read",
            "device_offline",
            "req_fixture",
            None,
            "gateway_dispatch",
        );
        assert_eq!(value["request_id"], "req_fixture");
        assert!(value["operation_id"].is_null());
        assert_eq!(value["last_confirmed_stage"], "gateway_received_request");
        assert_eq!(value["execution_state"], "not_started");
    }
    #[test]
    fn workflow_recovery_codes_are_specific() {
        let source = super::error(
            "file_upload",
            "source_authorization_required",
            Some("id"),
            "agent_execute",
        );
        assert_eq!(source["error_code"], "source_authorization_required");
        assert_eq!(source["outcome"], "not_executed_or_paused");
        let storage = super::error(
            "file_download",
            "storage_capacity_insufficient",
            Some("id"),
            "agent_execute",
        );
        assert_eq!(
            storage["recovery_action"],
            "free_storage_or_request_a_smaller_transfer"
        );
        let change = super::error(
            "change_resume",
            "change_set_unavailable: journal missing",
            None,
            "agent_execute",
        );
        assert_eq!(change["error_code"], "change_set_unavailable");
        let gc = super::error(
            "workspace_gc",
            "gc_preview_changed: run preview again",
            None,
            "agent_execute",
        );
        assert_eq!(
            gc["recovery_action"],
            "run_workspace_gc_preview_again_before_apply"
        );
        let feature = super::error(
            "workspace_gc",
            "device_feature_unavailable: upgrade",
            None,
            "gateway_dispatch",
        );
        assert_eq!(feature["outcome"], "not_executed");
        assert_eq!(feature["failure_boundary"], "gateway");
        assert_eq!(feature["last_confirmed_stage"], "gateway_received_request");
        assert_eq!(feature["execution_state"], "not_started");
        assert_eq!(
            feature["retry_policy"],
            "retry_only_after_reported_condition_changes"
        );
    }
    #[test]
    fn conflicts_are_actionable_not_retry_promises() {
        let v = super::error(
            "code_apply_edits",
            "version_conflict: path",
            None,
            "execute",
        );
        assert_eq!(v["error_code"], "version_conflict");
        assert_eq!(v["recovery_action"], "read_current_version_and_merge");
    }
}

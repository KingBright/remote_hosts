//! Stable recovery guidance. Never serialize raw commands, URLs, or OS error chains.
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
    json!({"error":"tool_failed","error_code":code,"message":code,
        "stage":stage,"outcome":outcome,"recovery_action":recovery,
        "automatic_replay_safe":false,"operation_id":operation,"tool":tool,"diagnostic_protocol":1})
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
        assert_eq!(v["automatic_replay_safe"], false);
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

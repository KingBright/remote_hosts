"""Acceptance-only receipt handoff checks; never issues authority or executes work."""
TASK_POLICY_CODES = frozenset({
    "task_authorization_missing", "task_authorization_version_required",
    "task_authorization_version_changed", "task_authorization_revoked",
    "task_scope_denied", "task_device_denied",
})

def continuation(context, receipt, *, task_id, device_id, scope,
                 exposed_resume=False, version_parameter=False):
    """Classify trusted connector observations; arbitrary input is not proof."""
    if context.get("task_id") != task_id:
        return {"action": "blocked", "reason": "task_context_mismatch"}
    if receipt.get("task_id") != task_id:
        return {"action": "blocked", "reason": "receipt_task_mismatch"}
    operation = receipt.get("operation_id")
    state = receipt.get("execution_state")
    # Completed/running/unknown work always keeps its original observation handle.
    if operation:
        if state != "queued":
            return {"action": "observe_original", "operation_id": operation}
        if receipt.get("timing_record_present") is not True:
            return {"action": "observe_original", "operation_id": operation}
        if receipt.get("dispatched_at") is not None:
            return {"action": "observe_original", "operation_id": operation}
    elif state != "not_started":
        return {"action": "blocked", "reason": "no_confirmed_recovery_boundary"}
    # Platform approval is independent of persistent Gateway TaskGrant.
    if receipt.get("failure_boundary") != "task_authorization":
        return {"action": "blocked", "reason": "outside_task_resume_boundary"}
    if receipt.get("error_code") not in TASK_POLICY_CODES:
        return {"action": "blocked", "reason": "not_task_policy_rejection"}
    auth = context.get("authorization", {})
    if auth.get("state") != "authorized":
        return {"action": "blocked", "reason": "no_active_task_grant"}
    if auth.get("task_id") != task_id:
        return {"action": "blocked", "reason": "grant_task_mismatch"}
    version = auth.get("version")
    if type(version) is not int or version < 1:
        return {"action": "blocked", "reason": "no_confirmed_grant_version"}
    if device_id not in auth.get("devices", []) or scope not in auth.get("scopes", []):
        return {"action": "blocked", "reason": "task_scope_or_device_mismatch"}
    if not (exposed_resume and version_parameter):
        return {"action": "blocked", "reason": "host_catalog_missing_resume_binding"}
    handle = {"operation_id": operation} if operation else {"request_id": receipt.get("request_id")}
    if not next(iter(handle.values())):
        return {"action": "blocked", "reason": "original_handle_missing"}
    return {"action": "resume_original", **handle, "authorization_version": version}

def verify_completion(before, after):
    """Reconnect means reread the same operation, not another submission."""
    fields = ("task_id", "operation_id", "input_fingerprint", "execution_count")
    if any(not isinstance(before.get(k), str) or not before[k]
           for k in fields[:3]):
        raise ValueError("original_binding_missing")
    if any(before.get(k) != after.get(k) for k in fields):
        raise ValueError("original_operation_changed")
    if after.get("execution_count") != 1:
        raise ValueError("execution_not_single")
    if after.get("exit_code") != 0 or after.get("output_complete") is not True:
        raise ValueError("completion_not_confirmed")
    return {"state": "verified", "operation_id": after["operation_id"],
            "task_id": after["task_id"], "replayed": False}

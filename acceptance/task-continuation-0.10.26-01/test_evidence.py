"""Synthetic authorization fixtures; these are not owner consent or live grants."""
import unittest
from evidence import continuation, verify_completion

TASK = "01a1184e-4ede-76a2-aad4-3141c7b4c03c"
DEVICE = "ba3bf113-2390-466e-88bc-40d5b4f02884"

class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.context = {"task_id": TASK, "authorization": {
            "state": "authorized", "task_id": TASK, "version": 3, "devices": [DEVICE],
            "scopes": ["code:read", "code:write", "terminal:exec"]}}
        self.receipt = {"task_id": TASK, "request_id": "synthetic-request",
            "operation_id": None, "execution_state": "not_started",
            "failure_boundary": "task_authorization", "error_code": "task_authorization_revoked"}

    def decide(self, **changes):
        return continuation(self.context, self.receipt, task_id=TASK,
            device_id=DEVICE, scope="terminal:exec", exposed_resume=True,
            version_parameter=True, **changes)

    def test_task_id_and_human_approval_quote_do_not_create_grant(self):
        self.context["authorization"] = {"state": "correlation_only",
            "grants_authority": False, "human_quote": "批准本次升级"}
        self.assertEqual(self.decide()["reason"], "no_active_task_grant")

    def test_known_platform_rejection_cannot_use_task_resume(self):
        # Actual maintenance initially met auto-review rejection even with delegated scope.
        self.receipt.update(failure_boundary="platform_approval",
                            error_code="automatic_approval_rejected")
        self.assertEqual(self.decide()["reason"], "outside_task_resume_boundary")

    def test_grant_version_omission_is_not_recovered_from_task_label(self):
        del self.context["authorization"]["version"]
        self.assertEqual(self.decide()["reason"], "no_confirmed_grant_version")

    def test_other_tasks_grant_cannot_be_wrapped_in_this_task_context(self):
        self.context["authorization"]["task_id"] = "another-task"
        self.assertEqual(self.decide()["reason"], "grant_task_mismatch")

    def test_missing_original_binding_cannot_pass_by_equal_null_values(self):
        with self.assertRaisesRegex(ValueError, "original_binding_missing"):
            verify_completion({"execution_count": 1},
                {"execution_count": 1, "exit_code": 0, "output_complete": True})

    def test_wrong_device_is_rejected(self):
        self.context["authorization"]["devices"] = []
        self.assertEqual(self.decide()["reason"], "task_scope_or_device_mismatch")

    def test_wrong_scope_is_rejected(self):
        self.context["authorization"]["scopes"] = ["code:read"]
        self.assertEqual(self.decide()["reason"], "task_scope_or_device_mismatch")

    def test_catalog_gap_blocks_recovery(self):
        value = continuation(self.context, self.receipt, task_id=TASK,
            device_id=DEVICE, scope="terminal:exec")
        self.assertEqual(value["reason"], "host_catalog_missing_resume_binding")

    def test_policy_rejection_preserves_original_request(self):
        value = self.decide()
        self.assertEqual(value, {"action": "resume_original",
            "request_id": "synthetic-request", "authorization_version": 3})

    def test_completed_operation_only_observed(self):
        self.receipt.update(operation_id="synthetic-operation", execution_state="exited")
        self.assertEqual(self.decide(), {"action": "observe_original",
                                        "operation_id": "synthetic-operation"})

    def test_unknown_operation_never_resubmitted(self):
        self.receipt.update(operation_id="synthetic-operation", execution_state="outcome_unknown")
        self.assertEqual(self.decide()["action"], "observe_original")

    def test_missing_dispatch_evidence_does_not_mean_not_dispatched(self):
        self.receipt.update(operation_id="synthetic-operation", execution_state="queued")
        self.assertEqual(self.decide()["action"], "observe_original")

    def test_dispatched_queue_is_only_observed(self):
        self.receipt.update(operation_id="synthetic-operation", execution_state="queued",
                            timing_record_present=True, dispatched_at=123)
        self.assertEqual(self.decide()["action"], "observe_original")

    def test_verified_undispatched_queue_keeps_original_operation(self):
        self.receipt.update(operation_id="synthetic-operation", execution_state="queued",
                            timing_record_present=True, dispatched_at=None)
        self.assertEqual(self.decide()["operation_id"], "synthetic-operation")

    def test_other_task_cannot_supply_replacement_receipt(self):
        self.receipt["task_id"] = "another-task"
        self.assertEqual(self.decide()["reason"], "receipt_task_mismatch")

    def test_source_policy_rejection_is_not_authorization_recovery(self):
        self.receipt.update(failure_boundary="source_policy",
                            error_code="source_address_policy_rejected")
        self.assertEqual(self.decide()["reason"], "outside_task_resume_boundary")

    def test_reconnect_requires_same_completed_handle_and_one_execution(self):
        before = dict(task_id=TASK, operation_id="synthetic-operation",
                      input_fingerprint="same-intent", execution_count=1)
        after = dict(before, exit_code=0, output_complete=True)
        self.assertFalse(verify_completion(before, after)["replayed"])
        with self.assertRaisesRegex(ValueError, "original_operation_changed"):
            verify_completion(before, dict(after, operation_id="replacement-operation"))
        with self.assertRaisesRegex(ValueError, "original_operation_changed"):
            verify_completion(before, dict(after, execution_count=2))

if __name__ == "__main__":
    unittest.main()

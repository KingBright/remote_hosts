# Remote Hosts 0.6.0

0.6.0 adds unified operation lifecycle timing, durable multi-file change-set recovery, and explicit workspace-scoped GC with preview/apply identity binding.

## Verification

- Final fixed source snapshot: `ceac45675ee4467756baeaedc48d9011eb02ce86d707cfd55be801afcc3a4f9e`
- Full candidate verification: 343 passed, 0 failed (234 Rust + 109 Python); one opt-in Python probe skipped; two timing benchmarks ignored.
- macOS arm64 and Linux amd64 release builds passed and the package manifest SHA-256 is `c764e2e3e6de699b11b58bd73b7d55a5120d1bce791b4e0e9deab1b64d0f9b88`.
- After release, NAS gateway, MacBook-M2-Max and Mac-Studio all reported runtime version 0.6.0 with verified installed binary hashes and stable authenticated readiness.

## Acceptance follow-up

The first automated functional acceptance stopped before scenarios because `check-code-gateway.py` still expected the pre-0.6 tool catalog. Runtime services were already upgraded successfully. The source acceptance helper now derives the expected catalog by release version and includes the 0.5 `files_sync` and 0.6 `change_resume` / `workspace_gc` tools. Its focused tests and the full Python test suite pass. The original failed acceptance receipts are retained under `publish-a3/`; no reinstall is required for this helper-only failure.

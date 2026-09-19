# 0.10.10 Windows Agent operation

This candidate addresses Windows console allocation and missing poll diagnostics. Build and delivery remain on the owner's development machines; GitHub Actions and GitHub Releases are not required.

## Behavior

- The Windows executable uses the Windows subsystem. `agent --config <file>` does not attach to or create a console. The existing scheduled task continues to execute the same installation path, account and configuration.
- `agent --foreground` attaches to the invoking console for diagnosis. Other command-line subcommands preserve inherited stdout/stderr pipes and may attach to the parent console. Native CLI output and exit codes must be tested on Windows before deployment.
- Noninteractive remote commands use real pipes and `CREATE_NO_WINDOW`. Interactive PTY behavior is unchanged.
- Agent logs are retained in `<state_dir>/logs/agent.log` and three rotated files, each limited to 4 MiB. Foreground and Unix service runs also retain their existing stderr output.
- Poll failures include a bounded category, stage, optional HTTP status, attempt count and retry interval. Unchanged outages are reported at most once per minute per lane; a category change is reported immediately. A successful poll after failure records recovery and monotonic outage duration.
- Poll health transitions reuse the existing `runtime` records and the current Agent session. Poll retries do not replay accepted commands. HTTP errors never copy a response body, URL or credential into diagnostics.

## Evidence boundaries

The old Agent wrote only to stderr and discarded the actual polling error. A reported historical failure at 14:54 cannot be classified retroactively from the generic message alone. Current successful connectivity does not prove historical failures were harmless.

The existing Windows Startup shortcut invokes a PowerShell control script that starts the same scheduled task. It is redundant, not proof of two running Agent processes. Archive that exact shortcut only after confirming the scheduled task has its own logon trigger. Keep the task visible in Task Scheduler; hiding task metadata is not window management.

## Required acceptance

1. Local full regression and three-platform builds from one frozen source snapshot.
2. Fault injection: HTTP 503, malformed JSON and oversized body stay classified and redacted; recovery does not create work. Retry suppression, category changes and wall-clock adjustment are covered.
3. Windows PE subsystem equals 2, `--version` and configuration checking still return valid output and exit codes through redirected pipes.
4. Launch on the real Windows machine with the existing identity, verify one Agent process, no associated visible console window, persistent logs and authenticated readiness.
5. Native Windows terminal success/failure, delayed output, read/edit/file roundtrip and original-operation recovery still pass.
6. Preserve old runtime and configuration, maintenance/drain, independently supervised updater and rollback. A successful build is not Windows execution evidence.

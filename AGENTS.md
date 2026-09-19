# Repository delivery rules

## Build and deployment authority

- Build, test, package, and deploy on the owner's authorized development machines. Mac Studio is the primary release builder; reuse its persistent leased Cargo target directory.
- Do not add or enable GitHub Actions workflows for builds, tests, release gates, or deployments. Do not wait for GitHub check-runs. Git hosting is source storage only; optional release mirroring must never block delivery.
- Use the pinned local Rust toolchain and Cargo to produce macOS arm64, Linux x86_64 musl, and Windows x86_64 MSVC artifacts. Use the existing cargo-zigbuild/cargo-xwin linkers and SDKs. A successful cross-build is not target-platform execution evidence.
- Verify one frozen source snapshot, preserve test and build receipts, then transfer its immutable bundle directly through the authorized Remote Hosts or configured management channel. Do not download deployment input from GitHub Releases.
- Keep main as the collaboration branch. Preserve unrelated worktree changes. Stage explicit task files only.
- Upgrade Gateway first, then Agents using existing maintenance leases, platform signing, exact SHA-256 validation, health observations, and rollback paths. Never bypass local macOS signing or device authorization.
- Do not restart other projects or kill unrelated compiler processes. Cancel only operations whose ownership is confirmed.
- Run bounded regression tests on the appropriate owned target machine. Investigate hangs locally; disabling cloud CI does not authorize skipping the equivalent tests.
- Never overwrite a published version or its artifacts. A retry preserves its original request/operation and evidence. Ambiguous execution requires observation, not replay.
- Completion means deployed runtime identity plus real read/edit/execute/transfer/recovery acceptance. Report unknown, skipped, failed, and passed separately. Keep full evidence locally; return compact summaries.

## Existing entry points

- `scripts/source_snapshot.py`: capture/check the declared source inputs.
- `scripts/release-code.py`: local verification and three-platform Cargo cross-builds in a leased build slot; no cloud runner and no deployment.
- `scripts/fleet-upgrade.py`: verified local bundle distribution and per-platform updaters. Use only a verified local package and explicit deployment configuration.
- `scripts/check-code-gateway.py`: live acceptance, with authoritative results rather than presentation-only compaction.

The current release coordinator still contains transitional Python helpers. Rust produces the executables; do not describe the whole coordinator as Python-free until those helpers are actually replaced and tested.

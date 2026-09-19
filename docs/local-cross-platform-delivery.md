# Development-machine build and direct deployment

## Decision

GitHub Actions is not a build, verification, packaging, or deployment dependency. The repository workflow has been disabled and removed. Source commits may still be pushed to the Git remote; optional artifact mirroring is outside the critical path.

## Pipeline

1. Confirm the development worktree, toolchain, available disk, and existing build lease. Capture immutable declared inputs with `scripts/source_snapshot.py`.
2. On Mac Studio, invoke the snapshot's `scripts/release-code.py`. It uses the existing stable build slot, the pinned Rust toolchain, and independent step receipts.
3. Run the source checks locally. Build the three targets with:

   ```sh
   cargo build -p remote-hosts-code --release --locked
   cargo zigbuild -p remote-hosts-code --release --locked --target x86_64-unknown-linux-musl
   cargo xwin build -p remote-hosts-code --release --locked --target x86_64-pc-windows-msvc
   ```

   The macOS builder must be arm64. Cross-compilation also needs the platform linker/SDK already installed on the development host; `--target` alone does not replace these.
4. Package and validate the local manifest, source receipt, sizes, and SHA-256 values. Archive the source commit, snapshot identity, toolchain, target, commands, and test counts with this bundle.
5. Transfer the same bundle directly with Remote Hosts file tools or a previously authorized management route. Verify on receipt before extraction. The distribution source is the development machine, not a GitHub release URL.
6. Upgrade Gateway first, then Agents with the existing independent updaters. Preserve credentials, configuration, service identity, macOS signing, backup and rollback receipts.
7. Execute target-platform tests on owned machines. A Linux-only hang must be bounded and diagnosed on Linux, not relabeled as a cloud-only problem.
8. Accept each installed executable by hash and version, followed by real command success/failure, output consistency, file roundtrip, precise edits, idempotency, task recovery, and status checks.

## Recovery

A running operation is observed by its original handle. A completed step is reused only when its inputs and artifacts are still verified. A failed node does not trigger rebuilds or restarts on already accepted nodes. Never overwrite an immutable released version.

The cloud workflow's historical results remain historical evidence. They are not an active gate and will not be polled or restarted by this delivery path. Local source and target checks replace them; tests are not removed to manufacture a successful release.

## Current implementation boundary

The runtime and cross-built artifacts are Rust. The existing coordinator/updater helpers include Python and PowerShell. Their replacement by the native Rust release executor is a separate, testable migration, not a prerequisite to removing cloud builds now.

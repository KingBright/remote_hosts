# Android and transfer work on main

Android controlled-device Agent development and Remote Hosts Code transfer
recovery share the main branch. The original Android commit `f404557` and RH-009
commit `2647880` remain in its history; temporary task branches are not a release
lane. Keep commits scoped, preserve unrelated work, and use regular pushes.

Mainline source snapshots now bind Android Kotlin, manifests, Gradle files and
the pinned wrapper alongside Rust/Python inputs. SDK paths, build outputs,
signing material and generated debug CA resources stay outside the snapshot.
Default Python test discovery reuses the existing ten mixed Android/desktop
fleet tests. Android APKs keep their own signing/version format and never fall
through to Linux desktop packages.

For this integration, unchanged Android sources were checked against all 37
hashes in the existing build receipt. The exact 152196-byte release APK SHA-256
is `923a1aec4fc2ca22002e07f31781a42d5da81463316c8844039976e4eabe51b4`.
The eleven artifact/signature checks and ten mixed-fleet tests were rerun. The
matching historical receipts establish five JVM tests, release lint with zero
errors and seventeen warnings, thirty-one isolated Android emulator checks,
and installation/foreground launch of this same release APK. No emulator or
phone was rerun or reset for this source-only integration. See
[Android release acceptance](android-release-0.1.0.md) for its scope.

The integrated mainline Python discovery ran 340 tests: 339 passed and the
disposable launchd probe was skipped because its explicit opt-in was absent.
Clippy passed for all Remote Hosts Code targets with warnings denied.

Desktop transfer checks use the existing fixed acceptance script: thirteen
exact Rust cases cover original-operation recovery, owned process restart,
source DNS/permission/address-policy diagnostics and current source
authorization on a paused import. Python tests cover the coordinator's refusal
to auto-resume a source-diagnosis result. These are temporary protocol fixtures;
the separate ordinary live upload remains blocked before any source HTTP
request, with zero confirmed bytes and no destination file. Old runtime
receipts discarded the exact hostname/reason, so that network cause remains
unknown. The source fix has not changed the installed Gateway or Agent.

Next mainline work follows the same tool/task/operation records. Use the
hostname-only diagnostics from the normally installed fixed runtime to inspect
the original source before any explicit resume; confirm connectivity and current
authorization, without changing DNS/proxy/VPN/security or creating a replacement
transfer. For Android, continue physical-phone permission/lifecycle and real
release-connection acceptance when a device is connected and that scope is
authorized. Emulator evidence does not establish OEM behavior, reboot recovery,
or production enrollment. Neither project needs a second scheduler.

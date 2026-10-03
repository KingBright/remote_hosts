# Android controlled-device agent 0.1.0

This is a separate, configurable APK for controlling an owner's Android phone through the existing Remote Hosts Code gateway. It is not the Remote Play phone-to-desktop client. Minimum Android 11 (API 30); target API 36. One pure-DEX APK serves arm64, ARM and x86 Android runtimes. Kotlin implements Android lifecycle/platform APIs and the existing wire-2 contract; the existing Rust gateway and desktop agents are not rewritten.

## Installation and enrollment

The release APK contains no gateway URL, device token, owner account, test CA, instrumentation, advertising or analytics SDK. It is non-debuggable and signed by an owner-held release key. Signing material is outside the repository in the builder's private application state.

Install `remote-hosts-android-0.1.0.apk`. The gateway administrator must first register a **new, independently revocable device** using the existing gateway CLI. Do not reuse a computer's device ID/token: that would make two devices compete for the same identity.

Run on the gateway's administrative host with its actual configuration path:

```sh
remote-hosts-code enroll \
  --gateway-config /actual/private/gateway.json \
  --agent-config /actual/private/android-device.json \
  --name Android-Phone \
  --state-dir /android/private \
  --root /android/shared \
  --allow-write --allow-exec --shell /system/bin/sh
```

The CLI writes a private device configuration. Reload the gateway configuration using the deployment's existing controlled service restart procedure. Version 0.10.24 holds registrations in memory; merely writing a registration file is not live enrollment. This Android addition does not itself restart or upgrade a production gateway.

Import the generated JSON in the APK, or enter `gateway_url` (HTTPS origin, including port), `device_id` (UUID) and `device_token`. The APK derives its real shared-directory root and reports it to the gateway; the CLI's placeholder roots are not exposed as arbitrary Android filesystem roots. Configuration is encrypted with Android Keystore AES-GCM and excluded from Android backups. Do not send device tokens in chat or commit them.

Enable Remote Hosts under Android Accessibility settings for UI observation/actions. On some sideloaded installations, the phone's app settings require manually allowing restricted settings first. Grant notifications, then choose **开启远程控制**. Stop from the APK or its ongoing notification at any time. The configuration Activity is FLAG_SECURE and password fields are redacted in structured observations. Different permissions are reported independently.

## From the existing ChatGPT connector

No tool-catalog refresh is required. `devices_list` shows `platform=android` and actual `android_*` runtime features. Open the exact reported shared root once with `workspace_open`, then use existing tools. `terminal_exec` on this device is an **Android command dispatcher**, not a POSIX shell:

```text
android help
android status
android observe
android text {"observation_id":"...","node_id":3,"text":"你好"}
android click {"observation_id":"...","node_id":5}
android tap {"observation_id":"...","x":100,"y":200}
android swipe {"observation_id":"...","x":100,"y":600,"to_x":100,"to_y":200}
android screenshot
android apps {"offset":0,"limit":80}
android launch {"package":"com.example.app"}
android install {"path":"imports/application.apk","sha256":"..."}
android shell {"command":"id","timeout_seconds":30}
android pull {"source":"/sdcard/Download/example.txt","path":"imports/example.txt"}
```

Also supported: `long_click`, `scroll`, `back`, `home`, `recents`, `notifications`, `quick_settings`, `lock`. UI node/coordinate actions require a fresh observation ID (30-second lifetime). Successful interaction results explicitly distinguish accepted/completed gestures from application-level verification. Re-observe after acting. Password text is never returned from UI trees. Node signatures and window identity are checked before execution. Each explicit observation refreshes the framework node cache on API 33+, or individually refreshes bounded nodes on API 30-32, so a previously cached label is not mistaken for the post-action screen. The traversal has time, depth, node and output limits; it does not subscribe to keystroke or text-change events.

Screenshots return a workspace-relative PNG path, not inline base64. Retrieve it with `file_download`. Standard `file_upload`, `file_download`, `code_list`, `code_read`, `workspace_context` and bounded terminal result reads work for the shared directory. All other desktop code tools explicitly report unsupported. Legacy full-stream transfer supports at most 64 MiB per file, SHA-256 checks and atomic destination publication. All agent-controlled publication paths share a writer lock and recheck the expected version before a same-filesystem atomic rename in the app-private workspace; they do not rely on hard links that Android may reject. **This version does not claim resumable transfer v2 or large-file support.** `terminal_cancel` currently reports completed commands, not asynchronous live command cancellation; local Stop interrupts transport and the helper.

## Built-in ADB/shell helper

The helper is inside the same APK; Shizuku is not required. It does **not** magically obtain shell privileges. Enable the APK's shell checkbox while connected and activate through an explicitly authorized ADB connection:

```sh
python3 scripts/android-activate-shell.py --serial YOUR_ADB_SERIAL
```

The APK can also copy the exact activation command for its current session. Activation metadata is available only to Android shell (UID 2000), root, or the app's own UID; it contains no gateway credentials. The actual duplex connection is stricter: only a kernel-authenticated shell/root Binder caller may obtain its unnamed socket file descriptor. The helper verifies the installed application's UID before opening that provider. No named socket, TCP/ADB listener, shared credential file, or SELinux policy change is required. A short-lived one-use activation identifier binds the descriptor to the current local-consent session. The authorized app_process helper uses the same external-provider acquisition mechanism as Android's built-in `content` shell command, with its real UID and shell package attribution rather than a system-app Context. Provider UID/package ownership is checked before receiving the descriptor. Private platform signatures are isolated in ShellProviderConnection; physical-phone and newer-OS compatibility remain explicit acceptance requirements. Stop shuts down the socket before closing its descriptors, interrupting pending reads, and ends the helper-owned active process. Arbitrary detached descendants spawned by a user shell command are not promised to be cancelled.

Shell activation must be repeated after stopping the agent, process loss, or device reboot. **Built-in wireless-ADB pairing is not included in 0.1.0.** ADB may be USB or a previously paired wireless connection on an authorized computer. Shell authority is not root authority and does not bypass SELinux, app-private storage, secure windows, biometrics or lockscreen authentication.

With an activated bridge, APK installation streams the APK into the platform package manager and checks the installed package/version. Without it, installation uses a phone notification and Android's user-confirmation installer; enable installation from this source and notifications first. The agent never reports pending confirmation as installed.

## Reliability, lifecycle and storage

The SQLite journal durably records each job fingerprint before any action. A duplicate operation is never re-executed. A process restart marks unfinished work `outcome_unknown`; it does not replay taps, submissions or installations. Saved receipts alone are retried when connectivity recovers. Device/origin and owner/workspace bindings prevent redirecting private data to a different registration. Up to 512 unacknowledged results are retained; new execution fails closed at that limit. Acknowledged operation fingerprints are retained for seven days; terminal output is bounded and pruned. Screenshot history is bounded, with old captures removed and a hard stop on excessive new captures. Shared user files are not silently deleted.

Polling is serial for phone state changes, with independent heartbeat and receipt delivery. Idle receipt delivery waits on a signal rather than continuously querying SQLite. Foreground notification remains visible. Wake locks are bounded to active operations, not permanently held. Automatic boot reconnection is opt-in and still subject to Android/OEM rules. Lack of shell or accessibility never becomes a false claim of authority.

## Mixed desktop and Android fleets

Android has its own signed APK/version lane. `scripts/fleet-upgrade.py` excludes registered Android devices from desktop binary distribution and desktop acceptance, records them explicitly under `excluded_devices`, and reports `desktop_devices` or `online_desktop_devices` scope. The gateway's original `all_converged` fact is preserved: a successful desktop upgrade is not relabelled as a successful Android upgrade. Android paths can never silently fall through to Linux classification. `scripts/test-android-fleet-scope.py` covers these boundaries without changing live services.

## Build and verification

Build on the owner's authorized development machine:

```sh
python3 scripts/build-android.py
python3 scripts/test-android-integration.py \
  --serial emulator-5580 \
  --gateway-binary /actual/verified/remote-hosts-code-macos-arm64
```

Gradle 8.13 distribution checksum, AGP 8.13.2, Kotlin 2.3.21 and JDK 17 are pinned. Unit checks, release lint, debug instrumentation and R8 release assembly run on owner hardware. The integration test refuses physical phones and uses its own gateway configuration, TLS certificate and registered emulator. Its Python TLS fixture binds only to loopback and forwards only to the explicitly selected isolated local gateway; it does not change the release APK's trust policy. Its CA and ProbeActivity exist **only in the distinct debug application**. The real release APK is inspected, installed via the shell bridge, and launch-smoke-tested; a physical phone is a separate acceptance step. JSON receipts state exactly which checks completed. Never substitute the existence of this test script for a passed receipt.

After actual acceptance passes, publish without rebuilding or changing the tested bytes:

```sh
python3 scripts/publish-android.py --acceptance android/build/integration/ACTUAL_RUN_ID/acceptance.json
```

The publisher requires matching APK SHA-256 across build, packaging and successful integration receipts, checks every built Android source hash, requires passing unit tests and lint, and reruns the mixed-fleet regressions. Build success alone cannot publish; the old build-only `--publish` switch explicitly refuses. Publication exclusively creates a new version directory and writes `COMPLETE` last. No signing keys or device credentials enter that directory. The bundled receipts distinguish emulator validation from still-required physical-phone and production enrollment acceptance.

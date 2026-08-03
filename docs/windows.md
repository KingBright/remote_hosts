# Windows Installation and Operations

Remote Hosts runs natively on 64-bit Windows 10/11 and Windows Server. The release is cross-compiled
to the MSVC target from macOS or Linux with Rust and `cargo-xwin`; a Windows build machine, Visual
Studio, NSSM, and WinSW are not required.

## Install a Release

Extract the Windows ZIP, open Windows PowerShell in the extracted directory, and run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\remote-hosts-service.ps1 Install
.\remote-hosts-service.ps1 Status
```

Open <http://127.0.0.1:8787/admin> after both tasks report `Running`.

The installer does not require an administrator shell. It creates two current-user Task Scheduler
tasks that start at logon and restart after failures:

- `Remote Hosts API`
- `Remote Hosts Connector`

Current-user tasks are deliberate. The connector keeps access to the same SSH Agent, private keys,
`known_hosts`, user profile, and encrypted local vault used by Codex or Antigravity. A LocalSystem
Windows service would have a different identity and would make those credentials harder to use.

## Lifecycle

Run the installed manager at `%LOCALAPPDATA%\RemoteHosts\bin\remote-hosts-service.ps1`, or use the
copy in a newly extracted release for `Update`:

```powershell
.\remote-hosts-service.ps1 Stage
.\remote-hosts-service.ps1 Update
.\remote-hosts-service.ps1 Restart
.\remote-hosts-service.ps1 Start
.\remote-hosts-service.ps1 Stop
.\remote-hosts-service.ps1 Status
.\remote-hosts-service.ps1 Logs
.\remote-hosts-service.ps1 Ui
.\remote-hosts-service.ps1 Skills
.\remote-hosts-service.ps1 Doctor
.\remote-hosts-service.ps1 PrintConfig
```

`Stage` installs the new executable into a versioned release directory while the old executable is
still running. `Update` stages first, then asks the Rust CLI to verify that no operation, PTY input,
live PTY, or write lease would be interrupted. It switches the scheduled tasks only after that
drain gate passes. `-Force` is available only for an intentional interruption.

The UI remains a separate file. `Ui` replaces it atomically and does not restart the API or
connector. `Uninstall` removes the scheduled tasks but preserves local data; add `-PurgeData` only
when the database, vault key, artifacts, logs, and installed releases should also be deleted.

## Local Paths

| Purpose | Default path |
| --- | --- |
| Root | `%LOCALAPPDATA%\RemoteHosts` |
| Configuration | `%LOCALAPPDATA%\RemoteHosts\config\service.json` |
| Vault master key | `%LOCALAPPDATA%\RemoteHosts\config\vault-master-password` |
| SQLite database | `%LOCALAPPDATA%\RemoteHosts\data\remote-hosts.sqlite` |
| Output artifacts | `%LOCALAPPDATA%\RemoteHosts\data\artifacts` |
| Hot-swappable UI | `%LOCALAPPDATA%\RemoteHosts\data\ui\admin.html` |
| Versioned binaries | `%LOCALAPPDATA%\RemoteHosts\releases` |
| Stable MCP launcher | `%LOCALAPPDATA%\RemoteHosts\bin\remote-hosts-launcher.exe` |
| Logs | `%LOCALAPPDATA%\RemoteHosts\logs` |
| Service manager | `%LOCALAPPDATA%\RemoteHosts\bin\remote-hosts-service.ps1` |

The generated vault key is protected with a current-user ACL. Host passwords and private keys stay
inside the encrypted database and are never written into `service.json`.
SQLx receives Windows database paths as `sqlite://C:/...`; the installer normalizes backslashes and
does not add the Unix-only third slash used by absolute macOS paths.

## Agent Integration

`Install`, `Update`, and `Skills` synchronize the bundled Skill into:

- `%USERPROFILE%\.codex\skills\remote-hosts-agent`
- `%USERPROFILE%\.gemini\config\skills\remote-hosts-agent`

Run `PrintConfig` to display the exact `mcp-stdio` command and paths for the current installation.
The MCP configuration points to the small stable native launcher, which reads
`config\current-binary.txt` and starts the selected version with inherited stdio. New conversations
therefore pick up a staged release without editing client configuration or keeping a PowerShell
proxy alive for every MCP process.
The API and connector are background tasks; MCP stdio remains a child owned by the agent client so
each conversation receives its own Agent Session while all conversations share the connector's
bounded SSH transport pool.

## SSH Behavior on Windows

The Windows connector uses the native Rust `russh` backend. It supports pooled exec channels,
PowerShell, native PTYs, SFTP upload/download, output artifacts, password fallback, and bounded
public-key bootstrap. It can authenticate through the Windows OpenSSH Agent named pipe, Pageant,
default user keys, or encrypted Remote Hosts credentials.

The Unix-only OpenSSH `ControlMaster` compatibility backend is intentionally unavailable. Selecting
`--ssh-backend openssh` on Windows returns an explicit platform error instead of silently falling
back or spawning unmanaged SSH processes.

## Cross-Compile on macOS or Linux

Install the current toolchain and build the package:

```bash
rustup target add x86_64-pc-windows-msvc
cargo install cargo-xwin --version 0.23.0 --locked
# macOS
brew install nasm

scripts/build-windows-cross.sh
```

On Debian or Ubuntu, install `nasm` and `zip` from the system package manager. The build script
requires `cargo-xwin` 0.23.0 or newer, compiles with `--locked`, creates a ZIP under `dist/`, and
writes a sibling `.sha256` file. The archive contains the native executable, service manager,
external admin UI, release manifest, Windows guide, and Agent Skill.

The MSVC runtime is normally present on supported Windows systems. If Windows reports a missing
runtime DLL, install the current Microsoft Visual C++ Redistributable for x64.

## Troubleshooting

Use `Status` first, then `Logs`. Task Scheduler result `0` means the last process exited normally;
a running service normally shows task state `Running`. The task settings retry a failed process
after one minute and reject duplicate instances.

If an update is refused, let the reported operations, PTYs, input events, or write leases drain and
run `Update` again. Do not create a second connector or delete SQLite rows to bypass the gate.

If authentication behaves differently from an interactive terminal, confirm the scheduled tasks
belong to the same Windows account and that the user's OpenSSH Agent or Pageant session is available
after logon. Stored password credentials remain a valid fallback and can bootstrap a public key when
the target route supports it.

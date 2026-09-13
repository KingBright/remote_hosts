#!/usr/bin/env python3
"""Install the standalone code agent as a per-user macOS launch service.

Run on each Mac after staging the binary and private agent configuration. Does not
restart or modify the existing Remote Hosts connector or its database.
"""
import argparse
import os
import pathlib
import plistlib
import subprocess
import macos_code_identity


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--binary", required=True, type=pathlib.Path)
    p.add_argument("--config", required=True, type=pathlib.Path)
    args = p.parse_args()
    label = "com.remote-hosts.code-agent"
    domain = f"gui/{os.getuid()}"
    if subprocess.run(["launchctl", "print", f"{domain}/{label}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        raise SystemExit("Agent already loaded; inspect active terminals before a deliberate restart")
    release_binary, config = args.binary.resolve(), args.config.resolve()
    base = pathlib.Path.home() / ".local/share/remote-hosts-code"
    signing = macos_code_identity.status(base, create_if_missing=True)
    if signing.get("state") != "ready":
        signing = macos_code_identity.authorize(base)
    if signing.get("state") != "ready":
        raise SystemExit("macOS code-signing trust must be approved before installing the agent")
    binary = base / "bin/remote-hosts-code"
    binary.parent.mkdir(parents=True, exist_ok=True)
    signed = macos_code_identity.sign_copy(release_binary, binary, base)
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
    subprocess.run([str(binary), "check", "--agent", "--config", str(config)], check=True)
    macos_code_identity.save_installed_metadata(base, {
        "version": version.removeprefix("remote-hosts-code "),
        "candidate_sha256": macos_code_identity.sha(release_binary),
        "installed_sha256": signed["installed_sha256"],
        "certificate_sha1": signing["certificate_sha1"],
        "code_identifier": signing["code_identifier"],
        "designated_requirement": signing["designated_requirement"],
    })
    config.chmod(0o600)
    logs = config.parent / "logs"
    logs.mkdir(mode=0o700, exist_ok=True)
    launch_file = pathlib.Path.home() / "Library/LaunchAgents" / (label + ".plist")
    launch_file.parent.mkdir(parents=True, exist_ok=True)
    # PATH is explicit because launchd does not read the user's interactive shell config.
    plist = {"Label": label, "ProgramArguments": [str(binary), "agent", "--config", str(config)],
             "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
             "ProcessType": "Background", "Umask": 0o077,
             "EnvironmentVariables": {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "RUST_LOG": "info"},
             "StandardOutPath": str(logs / "agent.out.log"), "StandardErrorPath": str(logs / "agent.err.log")}
    with launch_file.open("wb") as f:
        plistlib.dump(plist, f)
    launch_file.chmod(0o600)
    subprocess.run(["launchctl", "bootstrap", domain, str(launch_file)], check=True)
    print("Installed and started " + label)


if __name__ == "__main__":
    main()

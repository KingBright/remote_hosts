#!/usr/bin/env python3
"""Install an already staged NAS bundle, then validate and reload only Caddy routing."""
import os
import pathlib
import shutil
import subprocess
import urllib.request
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true", help="Resume only when the staged and installed service/site match exactly")
    args = parser.parse_args()
    app = pathlib.Path("/opt/remote-hosts-code")
    state = pathlib.Path("/volume1/docker/remote-hosts-code")
    app.chmod(0o755)
    binary, config = app / "remote-hosts-code", app / "gateway.json"
    binary.chmod(0o755)
    os.chown(binary, 0, 0)
    wrapper = app / "run-code-gateway.py"
    os.chown(wrapper, 0, 0)
    wrapper.chmod(0o755)
    config.chmod(0o600)
    subprocess.run([str(binary), "check", "--config", str(config)], check=True)
    os.chown(config, 18787, 18787)
    config.chmod(0o400)
    state.mkdir(exist_ok=True)
    os.chown(state, 18787, 18787)
    state.chmod(0o700)
    service = pathlib.Path("/etc/systemd/system/remote-hosts-code-gateway.service")
    if service.exists():
        if not args.resume or service.read_bytes() != (app / service.name).read_bytes():
            raise SystemExit("Gateway service exists or differs; inspect before an explicit resume/update")
    else:
        shutil.copyfile(app / service.name, service)
    service.chmod(0o644)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", service.name], check=True)
    subprocess.run(["systemctl", "start", service.name], check=True)
    # The HTTP authority must match the public origin even for loopback readiness.
    import time
    for attempt in range(20):
        try:
            request = urllib.request.Request("http://127.0.0.1:18787/healthz", headers={"Host": "mcp.hackerlife.fun:8443"})
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(0.5)
    else:
        raise SystemExit("Gateway readiness failed; inspect its systemd journal")
    site = pathlib.Path("/etc/caddy/sites/remote-hosts-code.caddy")
    created_site = not site.exists()
    if site.exists():
        if not args.resume or site.read_bytes() != (app / "code-gateway.caddy").read_bytes():
            raise SystemExit("Caddy site exists or differs; inspect before an explicit resume/update")
    else:
        shutil.copyfile(app / "code-gateway.caddy", site)
    site.chmod(0o644)
    validation = subprocess.run(["/usr/local/bin/caddy", "validate", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"], capture_output=True)
    if validation.returncode:
        if created_site:
            site.unlink()  # Only the newly created site; running Caddy is unchanged.
        log = app / "caddy-validation.log"
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(validation.stdout + validation.stderr)
        raise SystemExit("Caddy validation failed; new site withdrawn; diagnostics retained privately")
    subprocess.run(["systemctl", "reload", "caddy"], check=True)
    print("Gateway healthy on loopback; Caddy configuration validated and reloaded")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Install a generic Linux Remote Hosts Code Gateway service and optional Caddy site.

This installer intentionally takes deployment identity as arguments. It must not
encode a maintainer's domain, UID/GID, storage volume, or device inventory.
"""
import argparse
import os
import pathlib
import pwd
import grp
import re
import shutil
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent


def private_name(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise SystemExit(f"invalid {label}")
    return value


def hostname(value: str) -> str:
    value = value.strip().lower().rstrip(".")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", value) or "." not in value:
        raise SystemExit("--public-host must be a fully qualified hostname")
    return value


def render(source: pathlib.Path, replacements: dict[str, str]) -> bytes:
    text = source.read_text()
    for key, value in replacements.items():
        text = text.replace("{{" + key + "}}", value)
    unresolved = re.findall(r"\{\{[A-Z0-9_]+\}\}", text)
    if unresolved:
        raise SystemExit("unresolved template values: " + ", ".join(sorted(set(unresolved))))
    return text.encode()


def install_exact(path: pathlib.Path, data: bytes, mode: int, resume: bool) -> bool:
    """Install one generated file. Return True only when the file was newly created."""
    if path.exists():
        if not resume or path.read_bytes() != data:
            raise SystemExit(f"{path} exists or differs; inspect before an explicit --resume")
        path.chmod(mode)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=pathlib.Path)
    parser.add_argument("--config", required=True, type=pathlib.Path)
    parser.add_argument("--state-dir", required=True, type=pathlib.Path)
    parser.add_argument("--public-host", required=True)
    parser.add_argument("--gateway-bind", default="127.0.0.1:18787")
    parser.add_argument("--service-user", default="remote-hosts-code")
    parser.add_argument("--service-group", default="remote-hosts-code")
    parser.add_argument(
        "--service-path",
        type=pathlib.Path,
        default=pathlib.Path("/etc/systemd/system/remote-hosts-code-gateway.service"),
    )
    parser.add_argument(
        "--caddy-site",
        type=pathlib.Path,
        default=pathlib.Path("/etc/caddy/sites/remote-hosts-code.caddy"),
    )
    parser.add_argument("--caddy-config", type=pathlib.Path, default=pathlib.Path("/etc/caddy/Caddyfile"))
    parser.add_argument("--caddy-bin", default="caddy")
    parser.add_argument("--skip-caddy", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    public_host = hostname(args.public_host)
    service_user = private_name(args.service_user, "service user")
    service_group = private_name(args.service_group, "service group")
    try:
        uid = pwd.getpwnam(service_user).pw_uid
        gid = grp.getgrnam(service_group).gr_gid
    except KeyError as error:
        raise SystemExit("service user/group must already exist") from error

    binary = args.binary.resolve(strict=True)
    config = args.config.resolve(strict=True)
    state = args.state_dir.resolve()
    if not os.access(binary, os.X_OK):
        raise SystemExit("gateway binary is not executable")
    config.chmod(0o600)
    subprocess.run([str(binary), "check", "--config", str(config)], check=True)
    os.chown(config, uid, gid)
    config.chmod(0o400)
    state.mkdir(parents=True, exist_ok=True)
    os.chown(state, uid, gid)
    state.chmod(0o700)

    replacements = {
        "SERVICE_USER": service_user,
        "SERVICE_GROUP": service_group,
        "BINARY_PATH": str(binary),
        "CONFIG_PATH": str(config),
        "STATE_DIR": str(state),
    }
    service_data = render(ROOT / "remote-hosts-code-gateway.service", replacements)
    install_exact(args.service_path, service_data, 0o644, args.resume)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", args.service_path.name], check=True)
    subprocess.run(["systemctl", "start", args.service_path.name], check=True)

    # The HTTP authority must match the public origin even for loopback readiness.
    bind_host, _, bind_port = args.gateway_bind.rpartition(":")
    if not bind_host or not bind_port.isdigit():
        raise SystemExit("--gateway-bind must look like 127.0.0.1:18787")
    readiness_url = f"http://{bind_host}:{bind_port}/healthz"
    for _ in range(20):
        try:
            request = urllib.request.Request(readiness_url, headers={"Host": public_host})
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(0.5)
    else:
        raise SystemExit("Gateway readiness failed; inspect its systemd journal")

    if args.skip_caddy:
        print("Gateway healthy on loopback; Caddy installation skipped")
        return

    caddy_data = render(
        ROOT / "code-gateway.caddy",
        {"PUBLIC_HOST": public_host, "GATEWAY_BIND": args.gateway_bind},
    )
    created_site = install_exact(args.caddy_site, caddy_data, 0o644, args.resume)
    validation = subprocess.run(
        [args.caddy_bin, "validate", "--config", str(args.caddy_config), "--adapter", "caddyfile"],
        capture_output=True,
    )
    if validation.returncode:
        if created_site:
            args.caddy_site.unlink()
        log = state / "caddy-validation.log"
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(validation.stdout + validation.stderr)
        raise SystemExit("Caddy validation failed; new site withdrawn; diagnostics retained privately")
    subprocess.run(["systemctl", "reload", "caddy"], check=True)
    print("Gateway healthy on loopback; generic Caddy site validated and reloaded")


if __name__ == "__main__":
    main()

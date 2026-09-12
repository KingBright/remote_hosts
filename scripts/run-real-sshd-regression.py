#!/usr/bin/env python3
"""Run the opt-in connector regression against an isolated real OpenSSH sshd."""
import argparse
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time


def free_port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--large", action="store_true", help="transfer and verify a 1 GiB file")
    parser.add_argument("--sshd", default="/usr/sbin/sshd")
    parser.add_argument("--cargo", default=shutil.which("cargo") or "cargo")
    args = parser.parse_args()
    sshd = pathlib.Path(args.sshd)
    if not sshd.is_file():
        raise SystemExit(f"sshd unavailable at {sshd}")
    root = pathlib.Path(__file__).resolve().parents[1]
    user = os.environ.get("USER") or subprocess.check_output(["id", "-un"], text=True).strip()
    with tempfile.TemporaryDirectory(prefix="remote-hosts-real-sshd-") as temporary:
        directory = pathlib.Path(temporary)
        host_key = directory / "host"
        client_key = directory / "client"
        authorized_keys = directory / "authorized_keys"
        pid_file = directory / "sshd.pid"
        log_file = directory / "sshd.log"
        config = directory / "sshd_config"
        run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(host_key))
        run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(client_key))
        authorized_keys.write_bytes((client_key.with_suffix(".pub")).read_bytes())
        authorized_keys.chmod(0o600)
        port = free_port()
        config.write_text(
            "\n".join(
                [
                    f"Port {port}",
                    "ListenAddress 127.0.0.1",
                    f"HostKey {host_key}",
                    f"PidFile {pid_file}",
                    f"AuthorizedKeysFile {authorized_keys}",
                    "StrictModes no",
                    "PasswordAuthentication no",
                    "KbdInteractiveAuthentication no",
                    "ChallengeResponseAuthentication no",
                    "UsePAM no",
                    "PermitRootLogin no",
                    f"AllowUsers {user}",
                    "Subsystem sftp internal-sftp",
                    "LogLevel VERBOSE",
                    "",
                ]
            )
        )
        run(str(sshd), "-t", "-f", str(config))
        sshd_process = subprocess.Popen(
            [str(sshd), "-D", "-f", str(config), "-E", str(log_file)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(100):
                if sshd_process.poll() is not None:
                    raise RuntimeError(f"isolated sshd exited early: {sshd_process.returncode}")
                if log_file.is_file() and "Server listening" in log_file.read_text(errors="replace"):
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError("isolated sshd did not become ready")
            env = os.environ.copy()
            env.update(
                REMOTE_HOSTS_REAL_SSH_PORT=str(port),
                REMOTE_HOSTS_REAL_SSH_USER=user,
                REMOTE_HOSTS_REAL_SSH_KEY=str(client_key),
                REMOTE_HOSTS_REAL_SSH_LARGE_BYTES=str(1024**3 if args.large else 2 * 1024**2),
            )
            command = [args.cargo, "test"]
            if args.large:
                command.append("--release")
            command.extend([
                "-p",
                "remote-hosts-connector",
                "tests::real_sshd_russh_reuses_transport_sftp_and_forward",
                "--",
                "--ignored",
                "--exact",
                "--nocapture",
            ])
            completed = subprocess.run(command, cwd=root, env=env)
            if completed.returncode:
                print("--- isolated sshd log ---")
                print(log_file.read_text(errors="replace")[-12000:])
                raise SystemExit(completed.returncode)
            print(
                f"real sshd regression passed: russh pooled exec + SFTP + local forward; "
                f"bytes={env['REMOTE_HOSTS_REAL_SSH_LARGE_BYTES']}"
            )
        finally:
            if sshd_process.poll() is None:
                sshd_process.terminate()
                try:
                    sshd_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    sshd_process.kill()
                    sshd_process.wait(timeout=5)


if __name__ == "__main__":
    main()

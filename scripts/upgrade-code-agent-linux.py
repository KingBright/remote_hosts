#!/usr/bin/env python3
"""One-shot Linux Code Agent upgrade for an independent transient user unit.

The controller must start this helper outside the agent process (for example with
`systemd-run --user`). It preserves the device identity/config, acquires the
same gateway maintenance lease used by macOS, waits for active work to drain,
atomically swaps the verified candidate, restarts the existing service and
requires fresh authenticated gateway observations. Failure after cutover restores
the exact previous binary.
"""
import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid

from agent_upgrade_support import gateway_observation
from maintenance_client import MaintenanceLease


def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_copy(source, target):
    target = pathlib.Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as out:
        temporary = pathlib.Path(out.name)
        try:
            with pathlib.Path(source).open("rb") as src:
                shutil.copyfileobj(src, out)
            out.flush()
            os.fsync(out.fileno())
            os.fchmod(out.fileno(), 0o755)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def save(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as out:
        temporary = pathlib.Path(out.name)
        try:
            out.write((json.dumps(record, indent=2) + "\n").encode())
            out.flush(); os.fsync(out.fileno()); os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def active_work(database):
    with contextlib.closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as db:
        terminals = db.execute("SELECT COUNT(*) FROM kv WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting')").fetchone()[0]
        operations = db.execute("SELECT COUNT(*) FROM kv WHERE kind='local_operation' AND json_extract(value,'$.state')='running'").fetchone()[0]
    return terminals, operations


def wait_gateway(config, version, baseline, timeout=240):
    deadline = time.monotonic() + timeout
    session = None
    fresh = 0
    last_seen = baseline
    while time.monotonic() < deadline:
        value = gateway_observation(config, attempts=1)
        if value.get("agent_version") == version and value.get("ready") is True and value.get("last_seen", 0) > last_seen:
            current = value.get("session")
            if session != current:
                session = current; fresh = 1
            else:
                fresh += 1
            last_seen = value["last_seen"]
            if fresh >= 3:
                return {"gateway_verified": True, "session": session, "samples": fresh, "last_seen": last_seen}
        time.sleep(2)
    raise RuntimeError("gateway readiness did not converge")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--candidate", required=True, type=pathlib.Path)
    p.add_argument("--sha256", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--result", required=True, type=pathlib.Path)
    p.add_argument("--binary-path", type=pathlib.Path, default=pathlib.Path.home()/".local/bin/remote-hosts-code")
    p.add_argument("--config-path", type=pathlib.Path, default=pathlib.Path.home()/".local/share/remote-hosts-code/agent.json")
    p.add_argument("--service-name", default="remote-hosts-code-agent.service")
    p.add_argument("--idle-timeout", type=int, default=180)
    args = p.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version) or not re.fullmatch(r"[0-9a-f]{64}", args.sha256):
        p.error("invalid version/checksum")
    record = {"state":"preflight","version":args.version,"candidate_sha256":args.sha256,"service_changed":False}
    backup = maintenance = None
    try:
        if sha(args.candidate) != args.sha256:
            raise RuntimeError("candidate checksum mismatch")
        if subprocess.check_output([str(args.candidate), "--version"], text=True, timeout=10).strip() != "remote-hosts-code " + args.version:
            raise RuntimeError("candidate version mismatch")
        config = json.loads(args.config_path.read_text())
        subprocess.run([str(args.candidate), "check", "--agent", "--config", str(args.config_path)], check=True, stdout=subprocess.DEVNULL, timeout=15)
        before = gateway_observation(config)
        maintenance = MaintenanceLease(config); maintenance.acquire(record)
        database = pathlib.Path(config["state_dir"]) / "state.sqlite"
        deadline = time.monotonic() + args.idle_timeout
        while True:
            terminals, operations = active_work(database)
            record.update(active_terminals=terminals, active_operations=operations, phase="waiting_idle")
            maintenance.report(record)
            if not terminals and not operations:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("agent still has active work; upgrade not applied")
            time.sleep(2)
        old_version = subprocess.check_output([str(args.binary_path), "--version"], text=True, timeout=10).strip().removeprefix("remote-hosts-code ")
        backup_dir = pathlib.Path.home()/".local/share/remote-hosts-code/releases"/("before-"+args.version+"-"+time.strftime("%Y%m%dT%H%M%S")+"-"+uuid.uuid4().hex[:8])
        backup_dir.mkdir(parents=True, exist_ok=False); backup = backup_dir/"remote-hosts-code"; shutil.copy2(args.binary_path, backup)
        record.update(previous_sha256=sha(backup), backup=str(backup), phase="installing"); maintenance.report(record)
        atomic_copy(args.candidate, args.binary_path); record["service_changed"] = True
        if sha(args.binary_path) != args.sha256:
            raise RuntimeError("installed checksum mismatch")
        subprocess.run(["systemctl", "--user", "restart", args.service_name], check=True, timeout=20)
        ready = wait_gateway(config, args.version, before.get("last_seen", 0))
        record.update(state="upgraded", phase="completed", installed_sha256=sha(args.binary_path), previous_version=old_version, **ready)
    except Exception as error:
        record.update(state="failed", error=str(error))
        if record["service_changed"] and backup is not None:
            try:
                atomic_copy(backup, args.binary_path)
                subprocess.run(["systemctl", "--user", "restart", args.service_name], check=True, timeout=20)
                record["rollback"] = "restored_previous_binary"
            except Exception as rollback:
                record["rollback"] = "failed:" + type(rollback).__name__
    if maintenance is not None:
        record["maintenance"] = maintenance.close(record)
    save(args.result, record); print(json.dumps(record), flush=True)
    if record["state"] != "upgraded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

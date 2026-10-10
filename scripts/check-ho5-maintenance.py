#!/usr/bin/env python3
"""Isolated HO5 candidate verification; fixed toolchain, leased existing target, no host maintenance."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import build_slot


def verify(root, evidence, slot, lock, lock_sha):
    if os.geteuid() == 0:
        raise RuntimeError("ordinary_user_tests_required")
    if evidence.exists() or not (slot / "slot.json").is_file() or not (slot / "cargo-target").is_dir():
        raise RuntimeError("fresh_evidence_and_existing_managed_build_slot_required")
    if lock.is_symlink() or hashlib.sha256(lock.read_bytes()).hexdigest() != lock_sha:
        raise RuntimeError("candidate_lock_digest_mismatch")
    evidence.mkdir(parents=True)
    spec = importlib.util.spec_from_file_location("admin_snapshot", root / "scripts/admin-helper-snapshot.py")
    snapshot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(snapshot)
    source = evidence / "source"
    manifest = snapshot.capture(root, source)
    (source / "Cargo.lock").write_bytes(lock.read_bytes())
    report_path = evidence / "report.json"
    report = dict(protocol=1, state="running", snapshot_id=manifest["snapshot_id"], checks=[],
                  lock_sha256=lock_sha, toolchain="1.94.1", uid=os.geteuid(),
                  build_slot=str(slot), host_maintenance_executed=False, deployed=False,
                  child_pid=None, started_at=int(time.time()))
    cancelled = False
    def on_signal(_signum, _frame):
        nonlocal cancelled
        cancelled = True
    old = {s: signal.signal(s, on_signal) for s in (signal.SIGINT, signal.SIGTERM)}
    def save():
        report["heartbeat_at"] = int(time.time())
        build_slot.atomic_json(report_path, report)
    def emit(value):
        print(json.dumps(value), flush=True)
    def stop_owned(child):
        for sig, bound in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            if not build_slot.process_alive(child.pid, group=True):
                return
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + bound
            while time.monotonic() < deadline:
                child.poll()
                if not build_slot.process_alive(child.pid, group=True):
                    return
                time.sleep(.1)
        raise RuntimeError("owned_test_process_cleanup_unconfirmed")
    def check(name, argv, env, limit=600):
        row = dict(name=name, state="running", log=str(evidence / (name + ".log")))
        report["checks"].append(row)
        save()
        started = last = time.monotonic()
        with Path(row["log"]).open("wb") as stream:
            child = subprocess.Popen(argv, cwd=source, env=env, stdout=stream,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            report["child_pid"] = child.pid
            row["owned_process_group"] = child.pid
            save()
            emit(dict(stage=name, state="started", pid=child.pid))
            try:
                while child.poll() is None:
                    now = time.monotonic()
                    if cancelled or now-started > limit:
                        stop_owned(child)
                        raise RuntimeError("cancelled_or_bounded_test_timeout")
                    if now-last >= 30:
                        row["elapsed_seconds"] = round(now-started, 1)
                        save()
                        emit(dict(stage=name, state="running", elapsed_seconds=row["elapsed_seconds"]))
                        last = now
                    time.sleep(.2)
                code = child.wait(timeout=5)
                if build_slot.process_alive(child.pid, group=True):
                    stop_owned(child)
            except BaseException:
                stop_owned(child)
                raise
            finally:
                if not build_slot.process_alive(child.pid, group=True):
                    report["child_pid"] = None
        row.update(exit_code=code, elapsed_seconds=round(time.monotonic()-started, 2),
                   sha256=build_slot.digest(Path(row["log"])), state="passed" if code == 0 else "failed")
        save()
        emit(dict(stage=name, state=row["state"], exit_code=code, elapsed_seconds=row["elapsed_seconds"]))
        if code:
            raise RuntimeError("check_failed_" + name)
    save()
    try:
        with build_slot.lease(slot, report_path, manifest["snapshot_id"]):
            env = dict(os.environ, CARGO_TARGET_DIR=str(slot/"cargo-target"), CARGO_BUILD_JOBS="1")
            cargo = str(Path.home()/".cargo/bin/cargo")
            common = ["--offline", "--locked", "--manifest-path", str(source/"Cargo.toml")]
            check("format", [cargo, "+1.94.1", "fmt", "--manifest-path", str(source/"Cargo.toml"), "--", "--check"], env)
            check("tests", [cargo, "+1.94.1", "test", *common, "--", "--test-threads=1"], env, 1200)
            check("clippy", [cargo, "+1.94.1", "clippy", *common, "--all-targets", "--", "-D", "warnings"], env)
            check("build", [cargo, "+1.94.1", "build", *common], env)
            binary = slot/"cargo-target/debug/remote-hosts-admin"
            check("cli_help", [str(binary), "ho5", "--help"], env, 30)
            check("cli_receipt_missing", [str(binary), "ho5", "receipt", "--state-dir",
                  str(evidence/"missing-state"), "--request-id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"], env, 30)
            assert not (evidence/"missing-state").exists()
            assert build_slot.digest(source/"Cargo.lock") == lock_sha
            report["binary_sha256"] = build_slot.digest(binary)
            report["state"] = "passed"
    except BaseException as error:
        report["state"] = "failed"
        report["error_code"] = type(error).__name__ + ":" + str(error)
        raise
    finally:
        report["finished_at"] = int(time.time())
        save()
        for sig, handler in old.items():
            signal.signal(sig, handler)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--evidence-dir", required=True, type=Path)
    p.add_argument("--build-slot", required=True, type=Path)
    p.add_argument("--dependency-lock", required=True, type=Path)
    p.add_argument("--accept-lock-sha256", required=True)
    a = p.parse_args()
    result = verify(Path(__file__).resolve().parents[1], a.evidence_dir.absolute(),
                    a.build_slot.absolute(), a.dependency_lock.absolute(), a.accept_lock_sha256)
    print(json.dumps(result), flush=True)

#!/usr/bin/env python3
"""HO5 native candidate checks in an existing RH target; no package/DNS/reboot calls."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

RESERVE = 4 * 1024**3
LOCK_SHA = "bd6590a8545d05bd638e8838b0d759f517c7ab6a68eaea765f214e666ef25d2d"

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def atomic(path, value):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        name = Path(stream.name)
        try:
            stream.write((json.dumps(value, indent=2)+"\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(name, path)
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        finally: name.unlink(missing_ok=True)

def validate_source(source):
    manifest = json.loads((source/"source-manifest.json").read_text())
    for name, digest in manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("unsafe_snapshot_input")
        path = source/relative
        if path.is_symlink() or not path.is_file() or sha(path) != digest:
            raise RuntimeError("snapshot_input_mismatch")
    actual = hashlib.sha256(json.dumps(manifest["files"],sort_keys=True,separators=(",",":")).encode()).hexdigest()
    if actual != manifest["snapshot_id"]:
        raise RuntimeError("snapshot_manifest_digest_mismatch")
    if sha(source/"Cargo.lock") != LOCK_SHA:
        raise RuntimeError("candidate_lock_mismatch")
    return manifest["snapshot_id"]

def run(source, target, evidence, expected_snapshot):
    if os.geteuid() == 0 or evidence.exists() or not target.is_dir():
        raise RuntimeError("ordinary_uid_fresh_evidence_existing_target_required")
    for path in [source, target]:
        if path != path.resolve() or path.is_symlink():
            raise RuntimeError("canonical_existing_paths_required")
    if target != Path("/var/home/liang/workspace/remote_hosts/target"):
        raise RuntimeError("owned_rh_target_required")
    snapshot = validate_source(source)
    if snapshot != expected_snapshot:
        raise RuntimeError("frozen_candidate_snapshot_mismatch")
    evidence.mkdir(mode=0o700)
    report_path = evidence/"report.json"
    report = dict(protocol=1, state="running", snapshot_id=snapshot, uid=os.geteuid(),
                  target=str(target), toolchain="1.94.1", reserve_bytes=RESERVE,
                  host_maintenance_executed=False, installed=False, child_pid=None, checks=[])
    def save(): atomic(report_path, report)
    def free(): return os.statvfs(target).f_bavail * os.statvfs(target).f_frsize
    def stop(child):
        for sig, timeout in [(signal.SIGTERM, 10), (signal.SIGKILL, 5)]:
            if child.poll() is not None: return
            os.killpg(child.pid, sig)
            try: child.wait(timeout=timeout); return
            except subprocess.TimeoutExpired: pass
        raise RuntimeError("owned_child_cleanup_unconfirmed")
    def check(name, argv, env, timeout=600):
        if free() < RESERVE:
            raise RuntimeError("reserve_reached_before_child")
        row = dict(name=name, state="running")
        report["checks"].append(row)
        log = evidence/(name+".log")
        started = last = time.monotonic()
        with log.open("wb") as out:
            child = subprocess.Popen(argv, cwd=source, env=env, stdout=out,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            report["child_pid"] = child.pid
            save()
            print(json.dumps(dict(stage=name,state="running",pid=child.pid)),flush=True)
            try:
                while child.poll() is None:
                    now = time.monotonic()
                    if free() < RESERVE or now-started > timeout:
                        stop(child); raise RuntimeError("reserve_or_timeout")
                    if now-last >= 30:
                        print(json.dumps(dict(stage=name,state="running",elapsed=round(now-started))),flush=True)
                        last = now
                    time.sleep(.2)
                code = child.wait()
            except BaseException:
                stop(child); raise
            finally:
                report["child_pid"] = None
        row.update(state="passed" if code==0 else "failed",exit_code=code,
                   elapsed=round(time.monotonic()-started,2),log_sha256=sha(log))
        save()
        print(json.dumps(row),flush=True)
        if code: raise RuntimeError("check_failed_"+name)
    def cancelled(_signum, _frame):
        raise KeyboardInterrupt("cancelled_owned_check")
    handlers = {s: signal.signal(s, cancelled) for s in (signal.SIGINT, signal.SIGTERM)}
    lockfd = os.open(target/".ho5-maintenance-build.lock", os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lockfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="1")
        cargo = "/home/liang/.cargo/bin/cargo"
        common = ["--offline","--locked","--manifest-path",str(source/"Cargo.toml")]
        save()
        check("format",[cargo,"+1.94.1","fmt","--","--check"],env)
        check("ho5_tests",[cargo,"+1.94.1","test",*common,"--lib","ho5","--","--test-threads=1"],env,1200)
        check("clippy",[cargo,"+1.94.1","clippy",*common,"--all-targets","--","-D","warnings"],env)
        check("native_build",[cargo,"+1.94.1","build",*common],env)
        binary = target/"debug/remote-hosts-admin"
        check("read_only_inspect",[str(binary),"ho5","inspect"],env,120)
        info = json.loads((evidence/"read_only_inspect.log").read_text())
        if info.get("execution_enabled") is not False or info.get("dns_enabled") is not False:
            raise RuntimeError("candidate_mutation_gate_not_closed")
        if info["caller_identity"]["uid"] != os.geteuid():
            raise RuntimeError("caller_identity_mismatch")
        with tempfile.TemporaryDirectory(prefix="journal-fixture-",dir=evidence) as tmp:
            state = Path(tmp)
            task = "aaaaaaab-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            request = "aaaaaaac-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            check("prepare_local_fixture",[str(binary),"ho5","prepare","--state-dir",str(state),
                  "--task-id",task,"--request-id",request,"--action","system-deps"],env,120)
            record = state/(request+".json")
            before = sha(record)
            check("receipt_cold_process",[str(binary),"ho5","receipt","--state-dir",str(state),"--request-id",request],env,30)
            if sha(record) != before:
                raise RuntimeError("receipt_read_mutated_record")
            check("verify_cold_process",[str(binary),"ho5","verify","--state-dir",str(state),"--request-id",request],env,120)
            verified = json.loads((evidence/"verify_cold_process.log").read_text())["receipt"]
            if verified["dispatch_count"] != 0 or verified["state"] != "prepared":
                raise RuntimeError("read_only_fixture_dispatched")
            report["namespace_journal_fixture"] = dict(protocol=verified["plan"]["protocol"],
                caller_identity_bound=True,store_identity_bound=True,dispatch_count=0,
                receipt_read_unchanged=True)
        validate_source(source)
        report.update(state="passed",binary_sha256=sha(binary),free_bytes=free())
    except BaseException as error:
        report.update(state="failed",error_code=type(error).__name__+":"+str(error))
        raise
    finally:
        report["finished_at"] = int(time.time())
        save()
        os.close(lockfd)
        for sig, handler in handlers.items(): signal.signal(sig, handler)
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source",type=Path,required=True)
    parser.add_argument("--existing-target",type=Path,required=True)
    parser.add_argument("--evidence-dir",type=Path,required=True)
    parser.add_argument("--expected-snapshot",required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.source.absolute(),args.existing_target.absolute(),args.evidence_dir.absolute(),args.expected_snapshot)),flush=True)

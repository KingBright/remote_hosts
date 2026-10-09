#!/usr/bin/env python3
"""Bounded task-authorization regression on the existing leased Cargo target."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

import build_slot
import source_snapshot

ROOT = Path(__file__).resolve().parents[1]
OWNED = [
    "crates/remote-hosts-code/src/task_authorization.rs",
    "crates/remote-hosts-code/src/task_authorization_tests.rs",
    "crates/remote-hosts-code/src/gateway.rs",
    "crates/remote-hosts-code/src/receipts.rs",
    "crates/remote-hosts-code/src/activity.rs",
    "crates/remote-hosts-code/src/diagnostics.rs",
    "crates/remote-hosts-code/src/job_dispatch.rs",
    "crates/remote-hosts-code/src/tools.rs",
    "crates/remote-hosts-code/src/lib.rs",
]

def stage(argv, checkout, target, report, name, state, limit):
    log = report.parent / (name + ".log")
    env = dict(os.environ)
    env["CARGO_TARGET_DIR"] = str(target)
    before = source_snapshot.check(checkout)["snapshot_id"]
    started = time.monotonic()
    with log.open("xb") as output:
        proc = subprocess.Popen(argv, cwd=checkout, env=env, stdout=output,
                                stderr=subprocess.STDOUT, start_new_session=True)
        state.update(phase=name, child_pid=proc.pid)
        build_slot.atomic_json(report, state)
        last = 0
        stopped = None
        while proc.poll() is None:
            elapsed = time.monotonic()-started
            free = shutil.disk_usage(target).free
            if elapsed >= limit or free < 5*1024**3:
                stopped = "timeout" if elapsed >= limit else "disk_floor"
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                break
            if elapsed-last >= 25:
                print(json.dumps({"phase":name,"elapsed_seconds":int(elapsed),
                                  "disk_free_gib":round(free/1024**3,2)}), flush=True)
                last = elapsed
            time.sleep(0.5)
    unchanged = source_snapshot.check(checkout)["snapshot_id"] == before
    state["stages"][name] = {
        "exit_code":proc.returncode,"stopped":stopped,"source_unchanged":unchanged,
        "elapsed_seconds":round(time.monotonic()-started,2),"log":str(log),
        "log_sha256":hashlib.sha256(log.read_bytes()).hexdigest(),
    }
    state.pop("child_pid",None)
    build_slot.atomic_json(report,state)
    if proc.returncode != 0 or stopped or not unchanged:
        raise RuntimeError("stage_failed:"+name)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--extended", action="store_true")
    parser.add_argument("--export-contract", action="store_true")
    parser.add_argument("--acceptance", action="store_true")
    args=parser.parse_args()
    evidence=args.evidence_dir.absolute()
    evidence.mkdir(parents=True,exist_ok=False)
    report=evidence/"report.json"
    snapshot=evidence/"snapshot"
    manifest=source_snapshot.create(ROOT,snapshot)
    slot=ROOT/"target/release-slot"
    target=slot/"cargo-target"
    if shutil.disk_usage(ROOT).free < 10*1024**3:
        raise RuntimeError("insufficient_starting_disk_budget")
    state={"state":"running","phase":"lease","snapshot_id":manifest["snapshot_id"],
           "source_root":str(ROOT),"target":str(target),"stages":{},
           "disk_floor_bytes":4*1024**3,"stop_threshold_bytes":5*1024**3}
    build_slot.atomic_json(report,state)
    try:
        with build_slot.lease(slot,report,manifest["snapshot_id"]) as checkout:
            state["sync"]=build_slot.synchronize(snapshot,checkout)
            build_slot.atomic_json(report,state)
            stage(["rustfmt","--edition","2024","--check","--config","skip_children=true",*OWNED],
                  checkout,target,report,"format",state,60)
            selected=["cargo","test","--offline","--locked","-p","remote-hosts-code","--lib"]
            stage([*selected,"task_authorization","--","--nocapture"],
                  checkout,target,report,"task_authorization",state,300)
            if args.extended:
                stage([*selected,"job_dispatch","--","--nocapture"],
                      checkout,target,report,"queue_dispatch",state,120)
                stage([*selected,"receipts","--","--nocapture"],
                      checkout,target,report,"receipts",state,120)
                stage([*selected,"diagnostics","--","--nocapture"],
                      checkout,target,report,"diagnostics",state,120)
            if args.export_contract:
                stage(["cargo","run","--offline","--locked","-p","remote-hosts-code",
                       "--bin","remote-hosts-code","--","adapter-contract","--output",
                       str(evidence/"adapter-contract.json")],
                      checkout,target,report,"contract_export",state,300)
            if args.acceptance:
                stage([*selected,"contract","--","--nocapture"],
                      checkout,target,report,"catalog_contract",state,120)
                stage(["cargo","test","--offline","--locked","-p","remote-hosts-code",
                       "--test","experience_contract","--","--nocapture"],
                      checkout,target,report,"experience_contract",state,300)
        state.update(state="passed",phase="complete")
    except Exception as error:
        state.update(state="failed",error_class=type(error).__name__,phase=state.get("phase"))
        build_slot.atomic_json(report,state)
        raise
    finally:
        state["disk_free_bytes"]=shutil.disk_usage(ROOT).free
        build_slot.atomic_json(report,state)
    print(json.dumps({"state":state["state"],"report":str(report),"snapshot_id":manifest["snapshot_id"],
                      "stages":state["stages"]}),flush=True)

if __name__ == "__main__":
    main()

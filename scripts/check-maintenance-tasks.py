#!/usr/bin/env python3
"""Serial ordinary-user verification with a pinned candidate lock and bounded child lifecycle.

No install, helper contact or elevation. Reuses the existing leased Cargo target.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import tomllib
import uuid

import build_slot


def run(root, evidence, device_id, dependency_lock, accept_lock_sha256):
    if os.geteuid() == 0:
        raise RuntimeError('ordinary_user_verification_required')
    if str(uuid.UUID(device_id)) != device_id:
        raise ValueError('canonical_device_uuid_required')
    if dependency_lock.is_symlink() or not dependency_lock.is_file():
        raise ValueError('regular_candidate_lock_required')
    lock_data=dependency_lock.read_bytes()
    lock_sha256=hashlib.sha256(lock_data).hexdigest()
    if lock_sha256 != accept_lock_sha256:
        raise ValueError('candidate_lock_digest_mismatch')
    packages=tomllib.loads(lock_data.decode())['package']
    if not any(p['name']=='remote-hosts-admin' and p['version']=='0.1.0' for p in packages):
        raise ValueError('candidate_lock_missing_helper')
    evidence=evidence.absolute()
    if evidence.exists():
        raise FileExistsError('evidence_exists_observe_original_report')
    evidence.mkdir(parents=True)
    module=importlib.util.spec_from_file_location('admin_snapshot',root/'scripts/admin-helper-snapshot.py')
    snapshot=importlib.util.module_from_spec(module)
    module.loader.exec_module(snapshot)
    manifest=snapshot.capture(root,evidence/'source')
    (evidence/'source/Cargo.lock').write_bytes(lock_data)
    script_sha256=build_slot.digest(Path(__file__))
    verification_id=hashlib.sha256((manifest['snapshot_id']+lock_sha256+script_sha256).encode()).hexdigest()
    report_path=evidence/'report.json'
    report=dict(protocol=2,snapshot_id=manifest['snapshot_id'],verification_id=verification_id,
                dependency_lock_sha256=lock_sha256,verification_script_sha256=script_sha256,
                dependency_lock_evidence='explicit candidate lock; not repository-lock equivalence',
                state='running',checks=[],uid=os.geteuid(),gid=os.getegid(),child_pid=None,
                device_identity_evidence='caller_declared_not_gateway_authenticated',
                system_changes=False,administrator_execution_verified=False,deployed=False)
    overall_start=time.monotonic()
    cancelled=False
    def cancel(_signum,_frame):
        nonlocal cancelled
        cancelled=True
    old_handlers={s:signal.signal(s,cancel) for s in (signal.SIGINT,signal.SIGTERM)}
    def save():
        report['heartbeat_at']=int(time.time())
        build_slot.atomic_json(report_path,report)
    def announce(value):
        print(json.dumps(value),flush=True)
    def cleanup(child):
        # Only this runner's recorded, separately created process group is eligible.
        for sig,wait_seconds in ((signal.SIGTERM,10),(signal.SIGKILL,5)):
            if not build_slot.process_alive(child.pid,group=True):
                break
            try:
                os.killpg(child.pid,sig)
            except ProcessLookupError:
                break
            deadline=time.monotonic()+wait_seconds
            while time.monotonic()<deadline:
                child.poll()
                if not build_slot.process_alive(child.pid,group=True):
                    break
                time.sleep(0.2)
        child.poll()
        if build_slot.process_alive(child.pid,group=True):
            raise RuntimeError('owned_child_group_cleanup_not_confirmed')
    save()
    def check(name,argv,expected=0,parse=False,limit=120):
        if cancelled:
            raise RuntimeError('verification_cancelled')
        log=evidence/(name+'.log')
        row=dict(name=name,state='running',expected_exit_code=expected,log=str(log),limit_seconds=limit)
        report['checks'].append(row)
        report['phase']=name
        save()
        started=time.monotonic()
        child=None
        failure=None
        last_announcement=started
        with log.open('wb') as stream:
            try:
                child=subprocess.Popen(argv,cwd=evidence/'source',env=env,stdout=stream,
                                       stderr=subprocess.STDOUT,start_new_session=True)
                report['child_pid']=child.pid
                row['child_pid']=child.pid
                row['owned_process_group']=child.pid
                save()
                announce({'stage':name,'state':'started','pid':child.pid,'report':str(report_path)})
                while child.poll() is None:
                    now=time.monotonic()
                    row.update(elapsed_seconds=round(now-started,1),log_bytes=log.stat().st_size)
                    if cancelled or now-started>limit or now-overall_start>2400:
                        failure='cancelled' if cancelled else 'bounded_compile_or_verification_timeout'
                        cleanup(child)
                        break
                    save()
                    if now-last_announcement>=30:
                        announce({'stage':name,'state':'running','pid':child.pid,
                                  'elapsed_seconds':row['elapsed_seconds'],'log_bytes':row['log_bytes']})
                        last_announcement=now
                    time.sleep(2)
                exit_code=child.wait(timeout=5)
                if build_slot.process_alive(child.pid,group=True):
                    failure='owned_child_group_remained_after_exit'
                    cleanup(child)
            except BaseException:
                if child is not None:
                    cleanup(child)
                row['state']='failed_before_confirmed_completion'
                raise
            finally:
                if child is not None and not build_slot.process_alive(child.pid,group=True):
                    report['child_pid']=None
                row['elapsed_seconds']=round(time.monotonic()-started,1)
                row['sha256']=build_slot.digest(log)
                save()
        row.update(exit_code=exit_code,state='passed' if failure is None and exit_code==expected else 'failed',failure=failure)
        save()
        announce({'stage':name,'state':row['state'],'exit_code':exit_code,'elapsed_seconds':row['elapsed_seconds']})
        if row['state']!='passed':
            raise RuntimeError(failure or 'verification_failed_'+name)
        return json.loads(log.read_text()) if parse else None
    try:
        with build_slot.lease(root/'target/admin-maintenance-task-slot',report_path,verification_id):
            env=dict(os.environ,CARGO_TARGET_DIR=str(root/'target/admin-maintenance-task-slot/cargo-target'))
            cargo_manifest=str(evidence/'source/Cargo.toml')
            check('format',['cargo','fmt','--manifest-path',cargo_manifest,'--','--check'])
            common=['--offline','--locked','--manifest-path',cargo_manifest,'-p','remote-hosts-admin']
            check('tests',['cargo','test',*common],limit=1200)
            check('clippy',['cargo','clippy',*common,'--all-targets','--','-D','warnings'],limit=600)
            check('build',['cargo','build',*common],limit=600)
            report['resolved_cargo_lock_sha256']=build_slot.digest(evidence/'source/Cargo.lock')
            if report['resolved_cargo_lock_sha256']!=lock_sha256:
                raise RuntimeError('candidate_lock_changed')
            binary=Path(env['CARGO_TARGET_DIR'])/'debug/remote-hosts-admin'
            report['binary_sha256']=build_slot.digest(binary)
            state=evidence/'ordinary-task-records'
            state.mkdir(mode=0o700)
            def cli(name,words,expected=0):
                return check(name,[str(binary),'task',*words],expected,True,30)
            description=cli('describe',['describe','--action','repair-remoteplay-mesh-ownership'])
            assert description['privileged_executor_implemented'] is False and description['arbitrary_commands'] is False
            common=['--state-dir',str(state)]
            request_id=str(uuid.uuid4())
            words=['prepare',*common,'--request-id',request_id,'--device-id',device_id,'--action','inspect-remoteplay-mesh']
            prepared=cli('prepare',words)
            assert prepared==cli('prepare-replay',words)
            execute=['run',*common,'--request-id',request_id,'--plan-sha256',prepared['plan_sha256']]
            result=cli('run',execute)
            assert result['state']=='succeeded' and result['system_changes_confirmed'] is False
            assert result==cli('run-replay',execute)
            verification=cli('verify',['verify',*common,'--request-id',request_id])
            assert verification['comparison']=='unchanged' and verification['administrator_repair_verified'] is False
            blocked_id=str(uuid.uuid4())
            blocked=cli('prepare-privileged',['prepare',*common,'--request-id',blocked_id,'--device-id',device_id,'--action','repair-remoteplay-mesh-ownership'])
            words=['run',*common,'--request-id',blocked_id,'--plan-sha256',blocked['plan_sha256']]
            outcome=cli('block-privileged',words,2)
            assert outcome['state']=='awaiting_platform_authorization' and outcome['system_changes_confirmed'] is False
            assert outcome==cli('block-privileged-replay',words,2)
            report['acceptance']=dict(ordinary_metadata_check='passed',duplicate_requests='original_receipt_retained',
                                      privileged_request='blocked_without_execution',file_contents_read=False,
                                      service_runtime_state='not_observed',root_authorization='not_granted')
            report['state']='passed'
    except Exception as error:
        report.update(state='failed',error_type=type(error).__name__,error_code=str(error),
                      next_action='Observe retained stage/process/log evidence; no automatic retry.')
        raise
    finally:
        report['elapsed_seconds']=round(time.monotonic()-overall_start,1)
        save()
        for sig,handler in old_handlers.items():
            signal.signal(sig,handler)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir',type=Path,required=True)
    parser.add_argument('--device-id',required=True)
    parser.add_argument('--dependency-lock',type=Path,required=True)
    parser.add_argument('--accept-lock-sha256',required=True)
    args=parser.parse_args()
    result=run(Path(__file__).resolve().parents[1],args.evidence_dir,args.device_id,args.dependency_lock,args.accept_lock_sha256)
    print(json.dumps({'state':result['state'],'verification_id':result['verification_id'],
                      'snapshot_id':result['snapshot_id'],'report':str(args.evidence_dir.absolute()/'report.json'),
                      'administrator_execution_verified':False,'deployed':False}))

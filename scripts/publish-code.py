#!/usr/bin/env python3
"""Publish a verified package on explicitly configured targets, independently.
Gateway first, then per-agent drain/update/receipt/acceptance. No hidden background scheduler.
"""
import argparse
import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import release_receipts as rr
from release_targets import selected
from release_client import Client

def acceptance_run_id(version, device_id):
    value='release-'+version+'-'+device_id
    value=re.sub(r'[^A-Za-z0-9-]+','-',value).strip('-')
    if not value or not value.replace('-','').isalnum():raise ValueError('invalid acceptance run id')
    return value

def signed_install_ready(receipt):
    signing=receipt.get('signing') or {}
    installed=receipt.get('installed_sha256','')
    certificate=signing.get('certificate_sha1','')
    requirement=signing.get('designated_requirement','')
    return (re.fullmatch(r'[0-9a-f]{64}',installed) is not None
            and signing.get('state')=='ready'
            and signing.get('installed_sha256')==installed
            and re.fullmatch(r'[0-9a-f]{40}',certificate) is not None
            and signing.get('code_identifier')=='com.remote-hosts.code-agent'
            and requirement==f'identifier "com.remote-hosts.code-agent" and certificate leaf = H"{certificate}"')


def acceptance_ready(device, version, sha):
    receipt=(device.get('upgrade') or {}).get('receipt') or {}
    return (device.get('online') is True and device.get('capabilities',{}).get('version')==version
            and device.get('maintenance',{}).get('state')=='open'
            and receipt.get('state')=='upgraded' and receipt.get('version')==version
            and receipt.get('candidate_sha256')==sha and signed_install_ready(receipt)
            and receipt.get('gateway_verified') is True and receipt.get('all_lanes_verified') is True
            and receipt.get('stable_seconds',0)>=15)


def validate(config):
    if not config['agents'] or len(config['agents'])>8:raise ValueError('bounded explicit agent set required')
    seen=set()
    for agent in config['agents']:
        uuid.UUID(agent['device_id'])
        if agent['device_id'] in seen or not agent['workspace_id'].startswith(agent['device_id']+':'):raise ValueError('device/workspace identity conflict')
        seen.add(agent['device_id'])
        if not pathlib.PurePosixPath(agent['home']).is_absolute():raise ValueError('absolute home required')
    gateway=config['gateway']
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.@-]*',gateway['ssh_host']) or not 1<=gateway['ssh_port']<=65535:raise ValueError('invalid SSH target')
    if not pathlib.PurePosixPath(gateway['root']).is_absolute():raise ValueError('absolute gateway root required')
    return config

def stage_remote_artifact(run, ssh, scp, host, local, dest, expected, attempts=3):
    """Stage one immutable artifact with bounded retry and atomic publication.

    A failed SCP may still have delivered the complete temporary file, so every
    retry re-checks its SHA before deciding to retransmit. Partial files never
    become the final candidate name.
    """
    if attempts < 1 or attempts > 5:
        raise ValueError('invalid staging retry bound')
    temp = dest + '.partial-' + expected[:12]
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            final_sha = run(ssh + ['if test -f '+shlex.quote(dest)+'; then sha256sum '+shlex.quote(dest)+'; else printf absent; fi']).split()[0]
            if final_sha == expected:
                return {'state':'staged','attempts':attempt-1,'reused':True}
            if final_sha != 'absent':
                raise ValueError('remote artifact conflict')
            temp_sha = run(ssh + ['if test -f '+shlex.quote(temp)+'; then sha256sum '+shlex.quote(temp)+'; else printf absent; fi']).split()[0]
            if temp_sha not in ('absent', expected):
                run(ssh + ['rm -f '+shlex.quote(temp)])
                temp_sha = 'absent'
            if temp_sha != expected:
                try:
                    run(scp + [str(local), host+':'+temp], 600)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                    last_error = type(error).__name__
                temp_sha = run(ssh + ['if test -f '+shlex.quote(temp)+'; then sha256sum '+shlex.quote(temp)+'; else printf absent; fi']).split()[0]
            if temp_sha != expected:
                raise RuntimeError('staged temporary artifact hash mismatch')
            run(ssh + ['mv -f '+shlex.quote(temp)+' '+shlex.quote(dest)])
            if run(ssh + ['sha256sum '+shlex.quote(dest)]).split()[0] != expected:
                raise RuntimeError('remote hash mismatch after atomic publish')
            return {'state':'staged','attempts':attempt,'reused':False}
        except ValueError:
            raise
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, RuntimeError) as error:
            last_error = type(error).__name__
            if attempt == attempts:
                break
            time.sleep(min(2 ** (attempt - 1), 4))
    raise RuntimeError('gateway staging failed after bounded retries: '+str(last_error))


def acceptance_scripts(root):
    """Acceptance-only recovery uses the currently validated controller scripts.

    The runtime package remains immutable; acceptance logic may be fixed and
    revalidated independently without rebuilding or reinstalling the service.
    """
    return root/'scripts'/'check-code-gateway.py', root/'scripts'/'check-collaboration.py'


def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True,type=pathlib.Path);p.add_argument('--pipeline',required=True,type=pathlib.Path);p.add_argument('--report-dir',required=True,type=pathlib.Path);p.add_argument('--version',required=True);p.add_argument('--apply',action='store_true');p.add_argument('--accept-only',action='store_true',help='rerun acceptance for already verified installed agents; never install or restart');args=p.parse_args()
    config=validate(json.loads(args.config.read_text()));root=pathlib.Path(config.get('project_root',pathlib.Path(__file__).resolve().parents[1])).resolve(strict=True);plan=rr.verified_build(args.pipeline,args.version)
    directory=args.report_dir.resolve();directory.mkdir(parents=True,exist_ok=True);package=root/'dist'/('remote-hosts-code-'+args.version)
    if not args.apply:print(json.dumps({'state':'verified_plan','version':args.version,'devices':[x['device_id'] for x in config['agents']],'manifest_sha256':plan['manifest_sha256']}));return
    if not package.exists():
        package.parent.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=package.parent,prefix='.release-') as temp:
            target=pathlib.Path(temp)/'candidate';shutil.copytree(plan['package'],target);target.rename(package)
    if rr.digest(package/'manifest.json')!=plan['manifest_sha256']:raise ValueError('existing release package differs')
    for name,info in plan['artifacts'].items():
        if rr.digest(package/name)!=info['sha256']:raise ValueError('artifact changed before publishing')
    manifest=json.loads((package/'manifest.json').read_text());sha=manifest['artifacts']['remote-hosts-code-macos-arm64']['sha256']
    if args.accept_only:
        if not args.apply:raise ValueError('--accept-only requires --apply; it is a bounded live acceptance action')
        standard_script,collaboration_script=acceptance_scripts(root)
        revision=(rr.digest(standard_script)[:12]+'-'+rr.digest(collaboration_script)[:12])
        attempt=rr.identity({'report_dir':str(directory),'revision':revision})[:12]
        recovery=directory/('accept-only-'+revision+'-'+attempt);recovery.mkdir(parents=True,exist_ok=True)
        client=Client(config['origin'],pathlib.Path(config['password_file']));summary={'version':args.version,'state':'running','mode':'accept_only','revision':revision,'attempt':attempt,'agents':{},'all_targets_accepted':False};accept_lock=threading.Lock()
        def save_recovery():rr.atomic_json(recovery/'summary.json',summary)
        try:
            client.login();inventory=client.tool('devices_list',{})
            if inventory.get('gateway',{}).get('version')!=args.version:raise RuntimeError('gateway version does not match acceptance target')
            current={d['device_id']:d for d in inventory['devices']}
            binding={'version':args.version,'manifest':plan['manifest_sha256'],'acceptance_revision':revision,'acceptance_attempt':attempt}
            def accept(ident):
                agent=next(a for a in config['agents'] if a['device_id']==ident);device=current.get(ident)
                if not device or not acceptance_ready(device,args.version,sha):
                    return {'device_id':ident,'name':agent['name'],'state':'needs_recovery','phase':'preflight','recovery':'verify original updater/runtime; accept-only never installs'}
                ad=recovery/ident;ad.mkdir(exist_ok=True);result={'device_id':ident,'name':agent['name'],'state':'running','phase':'standard'}
                run_id=acceptance_run_id(args.version,ident)+'-'+attempt[:8]
                rr.atomic_json(ad/'status.json',result)
                try:
                    with rr.StepJournal(ad/'steps.json',{**binding,'device':ident}) as journal:
                        def standard():
                            report=ad/'standard.json';log=ad/'standard.log';cmd=['/opt/homebrew/bin/python3',str(standard_script),'--origin',config['origin'],'--password-file',config['password_file'],'--report',str(report),'--run-id',run_id,'--expected-version',args.version,'--dispatch-protocol','2','--device-id',ident]
                            with log.open('x') as out:subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=1200)
                            value=json.loads(report.read_text());
                            if value.get('state')!='passed' or value.get('test_oauth_grant_revoked') is not True:raise RuntimeError('standard acceptance incomplete')
                            return {'state':'passed','report':str(report)}
                        result['standard']=journal.step('standard',binding,standard);result['phase']='collaboration';rr.atomic_json(ad/'status.json',result)
                        def collaboration():
                            report=ad/'collaboration.json';log=ad/'collaboration.log';cmd=['/opt/homebrew/bin/python3',str(collaboration_script),'--config',str(args.config.resolve()),'--device-id',ident,'--version',args.version,'--run-id',run_id,'--report',str(report)]
                            with log.open('x') as out:subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=1200)
                            value=json.loads(report.read_text());
                            if value.get('state')!='passed' or value.get('temporary_oauth_revoked') is not True:raise RuntimeError('collaboration acceptance incomplete')
                            return {'state':'passed','report':str(report)}
                        result['collaboration']=journal.step('collaboration',binding,collaboration)
                    result.update(state='accepted',phase='complete');rr.atomic_json(ad/'status.json',result);return result
                except Exception as error:
                    result.update(state='needs_recovery',failure_type=type(error).__name__,recovery='inspect this acceptance-only journal; do not reinstall');rr.atomic_json(ad/'status.json',result);return result
            def update_accept(ident,value):
                with accept_lock:summary['agents'][ident]=value;save_recovery()
            outcomes=selected([a['device_id'] for a in config['agents']],accept,update_accept);summary['all_targets_accepted']=outcomes['all_targets_accepted'];summary['state']='accepted' if outcomes['all_targets_accepted'] else 'partial'
        finally:
            try:summary['temporary_oauth_revoked']=client.close()
            except Exception:summary['temporary_oauth_revoked']=False;summary['all_targets_accepted']=False;summary['state']='partial'
            save_recovery()
        print(json.dumps({'state':summary['state'],'mode':'accept_only','all_targets_accepted':summary['all_targets_accepted'],'report':str(recovery/'summary.json')}),flush=True)
        if not summary['all_targets_accepted']:raise SystemExit(1)
        return
    publication_attempt=rr.identity({'report_dir':str(directory),'manifest':plan['manifest_sha256']})[:12]
    state={'version':args.version,'state':'running','phase':'verified_inputs','targets':[x['device_id'] for x in config['agents']], 'manifest_sha256':plan['manifest_sha256'],'tests':plan['tests'],'publication_attempt':publication_attempt,'agents':{},'all_targets_accepted':False};lock=threading.Lock()
    def save():rr.atomic_json(directory/'deployment.json',state)
    def command(argv,timeout=180):return subprocess.run(argv,cwd=root,check=True,capture_output=True,text=True,timeout=timeout).stdout
    binding={'version':args.version,'manifest':plan['manifest_sha256'],'config':rr.identity(config)}
    previous=directory/'deployment.json'
    if previous.exists():
        saved=json.loads(previous.read_text())
        if saved.get('manifest_sha256')!=plan['manifest_sha256'] or saved.get('targets')!=state['targets']:raise ValueError('publication identity changed; preserve original report')
        if saved.get('all_targets_accepted'):
            print(json.dumps({'state':'already_accepted','report':str(previous)}));return
        state.update(saved);state['state']='running'
    main_client=Client(config['origin'],pathlib.Path(config['password_file']));save()
    try:
        main_client.login()
        gateway=config['gateway'];ssh=['ssh','-p',str(gateway['ssh_port']),'-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',gateway['ssh_host']];remote=gateway['root']+'/releases/'+args.version
        with rr.StepJournal(directory/'gateway-steps.json',binding) as journal:
            def stage_gateway():
                command(ssh+['umask 077; mkdir -p '+shlex.quote(remote)])
                staged={}
                scp=['scp','-O','-P',str(gateway['ssh_port']),'-o','BatchMode=yes','-o','StrictHostKeyChecking=yes']
                for name in ('remote-hosts-code-linux-amd64','upgrade-code-gateway.py','manifest.json'):
                    dest=remote+'/'+name;expected=rr.digest(package/name)
                    staged[name]=stage_remote_artifact(command,ssh,scp,gateway['ssh_host'],package/name,dest,expected)
                return {'state':'staged','artifacts':staged}
            journal.step('stage',binding,stage_gateway)
            def update_gateway():
                argv=['python3',remote+'/upgrade-code-gateway.py','--candidate',remote+'/remote-hosts-code-linux-amd64','--sha256',manifest['artifacts']['remote-hosts-code-linux-amd64']['sha256'],'--version',args.version,'--result',remote+'/deployment.json']
                command(ssh+[shlex.join(argv)],180)
                value=json.loads(command(ssh+['cat '+shlex.quote(remote+'/deployment.json')]))
                if value.get('state')!='upgraded' or value['installed_sha256']!=manifest['artifacts']['remote-hosts-code-linux-amd64']['sha256']:raise ValueError('gateway not verified')
                rr.atomic_json(directory/'gateway-updater.json',value);return value
            state['gateway']=journal.step('upgrade',binding,update_gateway);save()
        state['phase']='independent_agent_pipelines';save()
        # Build once. Only binary and verified Python helpers are sent to a Mac.
        names={n for n in manifest['artifacts'] if n.endswith('.py')}|{'remote-hosts-code-macos-arm64','manifest.json'}
        archive=directory/'agent-package.tgz'
        if not archive.exists():
            with tarfile.open(archive,'x:gz') as t:
                for name in sorted(names):t.add(package/name,arcname=name)
        archive_sha=rr.digest(archive)
        # Snapshot/export the shared candidate before ANY local updater drains
        # the controller. Healthy peers no longer depend on its execution lane.
        exported=main_client.tool('file_download',{'workspace_id':config['controller_workspace'],'path':str(archive.relative_to(root)),'expected_version':archive_sha,'idempotency_key':'release-'+args.version+'-common-export','max_bytes':67108864})
        if exported.get('state')!='completed' or exported.get('sha256')!=archive_sha:raise RuntimeError('common artifact export not verified')
        state['source_artifact']={'operation_id':exported['operation_id'],'sha256':archive_sha,'size':exported['size']};save()
        def run_agent(ident):
            agent=next(a for a in config['agents'] if a['device_id']==ident);c=Client(config['origin'],access=main_client.access)
            ad=directory/ident;ad.mkdir(exist_ok=True);phase='staging';result={'device_id':ident,'name':agent['name'],'state':'running'}
            def record():result['phase']=phase;rr.atomic_json(ad/'status.json',result)
            try:
                with rr.StepJournal(ad/'steps.json',binding) as journal:
                    if agent.get('local'):
                        local_package=package
                    else:
                        destination=agent['home']+'/.local/share/remote-hosts-code/releases/'+args.version
                        remote_archive='.remote-hosts-release-staging-'+args.version+'/agent-package.tgz'
                        def stage_agent():
                            imported=c.tool('file_upload',{'workspace_id':agent['workspace_id'],'path':remote_archive,'expected_version':'absent','sha256':archive_sha,'idempotency_key':'release-'+args.version+'-import-'+ident,'file':{'file_id':exported['artifact_id'],'download_url':exported['download_url'],'file_name':'agent-package.tgz'}})
                            if imported['state']!='completed' or imported['sha256']!=archive_sha:raise RuntimeError('staging transfer not completed; observe original operation')
                            code="import pathlib,tarfile,hashlib,json\np=pathlib.Path("+repr(remote_archive)+");assert hashlib.sha256(p.read_bytes()).hexdigest()=="+repr(archive_sha)+"\nr=pathlib.Path("+repr(destination)+");r.mkdir(parents=True,exist_ok=True)\nwith tarfile.open(p) as t:\n assert set(t.getnames())=="+repr(names)+" and all(x.isfile() for x in t.getmembers())\n payload={n:t.extractfile(n).read() for n in t.getnames()}\n assert hashlib.sha256(payload['manifest.json']).hexdigest()=="+repr(plan['manifest_sha256'])+"\n manifest=json.loads(payload['manifest.json'])\n for n,b in payload.items():\n  if n!='manifest.json':assert hashlib.sha256(b).hexdigest()==manifest['artifacts'][n]['sha256']\n  q=r/n\n  if q.exists():assert q.read_bytes()==b\n  else:\n   with q.open('xb') as f:f.write(b)\n  q.chmod(0o755 if n=='remote-hosts-code-macos-arm64' else 0o600)\nprint('staged')"
                            c.terminal(agent['workspace_id'],'/opt/homebrew/bin/python3 -c '+shlex.quote(code),'release-'+args.version+'-extract-'+ident,60)
                            return {'state':'staged','import_operation':imported['operation_id'],'archive_sha256':archive_sha}
                        journal.step('stage',{'archive':archive_sha},stage_agent);local_package=pathlib.Path(destination)
                    phase='launch_updater';record()
                    def launch():
                        argv=['/opt/homebrew/bin/python3',str(local_package/'launch-code-upgrade.py'),'--candidate',str(local_package/'remote-hosts-code-macos-arm64'),'--sha256',sha,'--version',args.version,'--start']
                        output=command(argv,30) if agent.get('local') else c.terminal(agent['workspace_id'],shlex.join(argv),'release-'+args.version+'-launch-'+ident,30)
                        value=json.loads(output)
                        if value.get('state') not in ('started','already_requested','authorization_required'):raise RuntimeError('updater launch not accepted')
                        return value
                    launched=journal.step('launch',{'candidate':sha},launch);result['launch']=launched
                    if launched.get('state')=='authorization_required':
                        result.update(state='needs_user_authorization',signing=launched.get('signing'),
                                      next_action=launched.get('next_action'));phase='signing_authorization';record();return result
                    phase='observe_original_updater';record()
                    deadline=time.monotonic()+390
                    while time.monotonic()<deadline:
                        inventory=c.tool('devices_list',{});device=next(d for d in inventory['devices'] if d['device_id']==ident)
                        receipt=(device.get('upgrade') or {}).get('receipt',{})
                        if agent.get('local') and pathlib.Path(launched['result']).is_file():receipt=json.loads(pathlib.Path(launched['result']).read_text())
                        if receipt.get('candidate_sha256')==sha:
                            result['updater']=receipt;record()
                            if receipt.get('state')=='failed':
                                result['state']='deferred_busy' if receipt.get('error_code')=='device_busy' or 'active work' in receipt.get('error','') else 'upgrade_failed';record();return result
                            if receipt.get('state')=='upgraded' and device.get('maintenance',{}).get('state')=='open':
                                if not(signed_install_ready(receipt) and receipt.get('gateway_verified') and receipt.get('all_lanes_verified') and receipt.get('stable_seconds',0)>=15):raise RuntimeError('updater readiness mismatch')
                                if device['online'] and device['capabilities']['version']==args.version and device['capabilities']['session']==receipt['session']:break
                        time.sleep(2)
                    else:raise RuntimeError('original updater outcome pending; inspect saved result path without reinstalling')
                    rr.atomic_json(ad/'updater.json',receipt);phase='functional_acceptance';record()
                    run_id=acceptance_run_id(args.version,ident)+'-'+publication_attempt[:8]
                    cmd=['/opt/homebrew/bin/python3',str(package/'check-code-gateway.py'),'--origin',config['origin'],'--password-file',config['password_file'],'--report',str(ad/'acceptance.json'),'--run-id',run_id,'--expected-version',args.version,'--dispatch-protocol','2','--device-id',ident]
                    def standard():
                        with (ad/'acceptance.log').open('x') as log:subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=1200)
                        value=json.loads((ad/'acceptance.json').read_text())
                        if value['state']!='passed' or not value['test_oauth_grant_revoked']:raise RuntimeError('acceptance incomplete')
                        return {'state':'passed','report':str(ad/'acceptance.json')}
                    result['standard']=journal.step('standard',{'version':args.version,'device':ident},standard)
                    phase='collaboration_acceptance';record()
                    cmd=['/opt/homebrew/bin/python3',str(package/'check-collaboration.py'),'--config',str(args.config.resolve()),'--device-id',ident,'--version',args.version,'--run-id',run_id,'--report',str(ad/'collaboration.json')]
                    def features():
                        with (ad/'collaboration.log').open('x') as log:subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=1200)
                        value=json.loads((ad/'collaboration.json').read_text())
                        if value['state']!='passed' or not value['temporary_oauth_revoked']:raise RuntimeError('collaboration acceptance incomplete')
                        return {'state':'passed','report':str(ad/'collaboration.json')}
                    result['collaboration']=journal.step('collaboration',{'version':args.version,'device':ident},features)
                    phase='complete';result['state']='accepted';record();return result
            except Exception as error:
                result.update(state='needs_recovery',failure_type=type(error).__name__,next_action='inspect original per-target stage and receipts; do not repeat installation');record();return result
        def update(ident,value):
            with lock:state['agents'][ident]=value;save()
        outcomes=selected([a['device_id'] for a in config['agents']],run_agent,update)
        state['all_targets_accepted']=outcomes['all_targets_accepted'];state['state']='selected_targets_deployed_and_accepted' if outcomes['all_targets_accepted'] else 'partial';state['phase']='finished'
    except Exception as error:state.update(state='needs_recovery',failure_type=type(error).__name__)
    finally:
        try:state['temporary_oauth_revoked']=main_client.close()
        except Exception:state['temporary_oauth_revoked']=False;state['all_targets_accepted']=False
        save();print(json.dumps({'state':state['state'],'all_targets_accepted':state['all_targets_accepted'],'report':str(directory/'deployment.json')}),flush=True)
    if not state['all_targets_accepted']:raise SystemExit(1)

if __name__=='__main__':main()

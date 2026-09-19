#!/usr/bin/env python3
"""Converge one Remote Hosts Code gateway and its authorized Agent fleet.

One invocation owns one immutable package/manifest identity. The gateway is
upgraded first. Agents are discovered from `fleet_status`, one release bundle is
exported through the existing verified file-transfer path, and platform-specific
one-shot updaters perform local cutovers. The controller never replays an
uncertain mutation: its journal is the recovery boundary.
"""
import argparse
import json
import pathlib
import shlex
import subprocess
import tarfile
import time

import release_receipts as rr
from release_client import Client

ROOT = pathlib.Path(__file__).resolve().parents[1]


def platform(device):
    caps=device.get('capabilities') or {}
    value=caps.get('platform') or ''
    if value in ('macos','linux','windows'): return value
    roots=caps.get('roots') or []
    root=roots[0] if roots else ''
    if '\\' in root or (len(root)>2 and root[1:3]==':\\'): return 'windows'
    if root.startswith('/Users/'): return 'macos'
    return 'linux'


def home(device):
    caps=device.get('capabilities') or {}
    if caps.get('home_dir'): return caps['home_dir']
    roots=caps.get('roots') or []
    root=roots[0] if roots else ''
    if platform(device)=='macos': return '/'.join(root.split('/')[:3])
    if platform(device)=='linux':
        parts=root.split('/')
        return '/'.join(parts[:4] if root.startswith('/var/home/') else parts[:3])
    clean=root.removeprefix('\\\\?\\')
    parts=clean.split('\\')
    return '\\'.join(parts[:3]) if len(parts)>=3 else clean


def root_path(device):
    roots=(device.get('capabilities') or {}).get('roots') or []
    if not roots: raise RuntimeError('device did not report a project root: '+device['device_id'])
    return roots[0]


def command(argv, timeout=180):
    return subprocess.run(argv,cwd=ROOT,check=True,capture_output=True,text=True,timeout=timeout).stdout


def open_workspace(client, device, root, key):
    return client.tool('workspace_open',{'device_id':device,'root':root,'idempotency_key':key})['workspace']['id']


def verify_package(package, version):
    manifest=json.loads((package/'manifest.json').read_text())
    if manifest.get('version')!=version: raise ValueError('package version mismatch')
    for name,meta in manifest['artifacts'].items():
        path=package/name
        if not path.is_file() or rr.digest(path)!=meta['sha256'] or path.stat().st_size!=meta['size']:
            raise ValueError('package artifact mismatch: '+name)
    return manifest


def ensure_bundle(package, manifest):
    path=package.parent/(package.name+'-bundle.tgz')
    names=sorted([*manifest['artifacts'],'manifest.json','source-verification.json','SHA256SUMS','README.md'])
    if path.exists(): return path,rr.digest(path)
    with tarfile.open(path,'x:gz') as archive:
        for name in names: archive.add(package/name,arcname=name)
    return path,rr.digest(path)


def ps_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def controller_device_id(config):
    explicit=config.get('controller_device_id')
    if explicit:return explicit
    workspace=str(config.get('controller_workspace') or '')
    return workspace.split(':',1)[0] if ':' in workspace else None


def exported_size(exported, bundle):
    value=exported.get('size')
    return value if isinstance(value,int) and value>=0 else bundle.stat().st_size


def verified_export(client, exported, bundle, bundle_sha, version):
    value=exported
    for attempt in range(1,6):
        if value.get('state')=='completed':
            if value.get('sha256')!=bundle_sha or not value.get('artifact_id') or not value.get('download_url'):
                raise RuntimeError('bundle export completed without verified artifact identity')
            return value
        ident=value.get('operation_id')
        if not ident: raise RuntimeError('bundle export missing operation identity')
        if value.get('state')=='already_finished' and value.get('next_action')=='operation_get':
            value=client.tool('operation_get',{'operation_id':ident});continue
        if value.get('state') in ('paused','awaiting_source') and value.get('next_action')=='transfer_resume':
            client.tool('transfer_resume',{'operation_id':ident,'idempotency_key':'fleet-'+version+'-bundle-export-resume-'+str(attempt)})
            value=client.tool('operation_get',{'operation_id':ident});continue
        raise RuntimeError('bundle export not recoverable: '+str(value.get('state')))
    raise RuntimeError('bundle export did not complete after bounded resume attempts')


def verified_import(client, imported, exported, bundle, bundle_sha, version, ident):
    if imported.get('state')=='completed' and imported.get('sha256')==bundle_sha:return imported
    if imported.get('state') in ('paused','awaiting_source') and imported.get('next_action')=='transfer_resume':
        resumed=client.tool('transfer_resume',{'operation_id':imported['operation_id'],
            'idempotency_key':'fleet-'+version+'-bundle-resume-'+ident,
            'file':{'file_id':exported['artifact_id'],'download_url':exported['download_url'],'file_name':bundle.name}})
        if resumed.get('state')=='completed' and resumed.get('sha256')==bundle_sha:return resumed
    raise RuntimeError('bundle import not verified: '+ident)


def accept_fleet(config,package,directory,version,fleet):
    ids=[d['device_id'] for d in fleet['devices']]
    acceptance=directory/'acceptance.json';run_id='fleet-'+version.replace('.','-')+'-'+rr.identity(ids)[:8]
    command(['python3',str(package/'check-code-gateway.py'),'--origin',config['origin'],'--password-file',config['password_file'],'--report',str(acceptance),'--run-id',run_id,'--expected-version',version,'--dispatch-protocol','2',*[x for ident in ids for x in ('--device-id',ident)]],1200)
    result=json.loads(acceptance.read_text())
    if result.get('state')!='passed':raise RuntimeError('fleet capability acceptance failed')
    return acceptance


def rollout_targets(devices, controller_id):
    online=[d for d in devices if d.get('online')]
    deferred=[d for d in devices if not d.get('online')]
    ordered=[d for d in online if d['device_id']!=controller_id]+[d for d in online if d['device_id']==controller_id]
    return ordered,deferred


def finish_observed_fleet(config,package,directory,version,state,fleet):
    """Accept the actual reachable scope; never label offline devices as upgraded."""
    if fleet.get('all_converged'):
        acceptance=accept_fleet(config,package,directory,version,fleet)
        state.update(state='passed',phase='finished',all_converged=True,online_converged=True,
                     pending_device_ids=[],acceptance_scope='all_devices',acceptance=str(acceptance))
    else:
        pending=[d for d in fleet['devices'] if not d['converged']]
        if not fleet.get('summary',{}).get('gateway_converged') or not pending or any(d.get('online') for d in pending):
            return False
        online=[d for d in fleet['devices'] if d.get('online')]
        acceptance=None
        if online:
            scope=directory/'online-only';scope.mkdir(parents=True,exist_ok=True)
            acceptance=accept_fleet(config,package,scope,version,dict(fleet,devices=online))
        for d in pending:
            state['agents'][d['device_id']]={'name':d['name'],'platform':platform(d),'state':'waiting_online'}
        state.update(state='waiting_online',phase='offline_devices',all_converged=False,
                     online_converged=bool(online),pending_device_ids=[d['device_id'] for d in pending],
                     acceptance_scope='online_devices',acceptance=str(acceptance) if acceptance else None,
                     next_action='rerun this fleet command after the deferred devices reconnect; no offline upgrade was queued')
    state['fleet']=fleet
    rr.atomic_json(directory/'fleet.json',state)
    print(json.dumps({k:state.get(k) for k in ('state','version','all_converged','online_converged','pending_device_ids','acceptance_scope')}))
    return True


def gateway_upgrade_api(client,version,bundle_sha,timeout=600):
    """Prefer the narrow authenticated self-upgrade endpoint; None means legacy gateway."""
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            status,_,body=client.call('/admin/gateway-upgrade',{'version':version,'bundle_sha256':bundle_sha},auth=True)
        except Exception:
            time.sleep(2);continue
        if status in (404,405): return None
        if status not in (200,202): raise RuntimeError('gateway self-upgrade request rejected: '+str(status))
        value=json.loads(body)
        if value.get('state') in ('upgraded','no_change'): return value
        if value.get('state') in ('needs_recovery','failed'):
            raise RuntimeError('gateway self-upgrade needs recovery; inspect original unit/receipt')
        time.sleep(2)
    raise RuntimeError('gateway self-upgrade outcome pending; observe original request')


def gateway_upgrade_ssh(config,package,manifest,version,directory):
    gateway=config['gateway'];remote=gateway['root']+'/releases/'+version
    ssh=['ssh','-p',str(gateway['ssh_port']),'-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10',gateway['ssh_host']]
    scp=['scp','-O','-P',str(gateway['ssh_port']),'-o','BatchMode=yes','-o','StrictHostKeyChecking=yes']
    binding={'version':version,'manifest':rr.digest(package/'manifest.json'),'gateway':gateway['ssh_host']}
    with rr.StepJournal(directory/'gateway-steps.json',binding) as journal:
        def stage():
            command(ssh+['umask 077; mkdir -p '+shlex.quote(remote)])
            for name in ('remote-hosts-code-linux-amd64','upgrade-code-gateway.py','manifest.json'):
                command(scp+[str(package/name),gateway['ssh_host']+':'+remote+'/'+name],300)
                got=command(ssh+['sha256sum '+shlex.quote(remote+'/'+name)]).split()[0]
                if got!=rr.digest(package/name): raise RuntimeError('gateway staged artifact mismatch: '+name)
            return {'state':'staged'}
        journal.step('stage',binding,stage)
        def upgrade():
            args=['python3',remote+'/upgrade-code-gateway.py','--candidate',remote+'/remote-hosts-code-linux-amd64',
                  '--sha256',manifest['artifacts']['remote-hosts-code-linux-amd64']['sha256'],'--version',version,
                  '--result',remote+'/deployment.json','--binary-path',gateway.get('binary_path',gateway['root']+'/remote-hosts-code'),
                  '--config-path',gateway.get('config_path',gateway['root']+'/gateway.json'),
                  '--backup-root',gateway.get('backup_root',gateway['root']+'/releases'),
                  '--service-name',gateway.get('service_name','remote-hosts-code-gateway.service'),
                  '--gateway-bind',gateway.get('gateway_bind','127.0.0.1:18787')]
            command(ssh+[shlex.join(args)],240)
            value=json.loads(command(ssh+['cat '+shlex.quote(remote+'/deployment.json')]))
            if value.get('state')!='upgraded' or value.get('installed_sha256')!=manifest['artifacts']['remote-hosts-code-linux-amd64']['sha256']:
                raise RuntimeError('gateway upgrade not verified')
            return value
        return journal.step('upgrade',binding,upgrade)


def extract_command(device,remote_bundle,destination,bundle_sha):
    if platform(device)=='windows':
        return ("$ErrorActionPreference='Stop';$p="+ps_quote(remote_bundle)+
                ";if ((Get-FileHash -Algorithm SHA256 $p).Hash.ToLowerInvariant() -ne '"+bundle_sha+"'){throw 'bundle hash mismatch'};"+
                "$r="+ps_quote(destination)+";New-Item -ItemType Directory -Force $r|Out-Null;tar.exe -xzf $p -C $r")
    code=("import hashlib,pathlib,tarfile\n"
          f"p=pathlib.Path({remote_bundle!r});assert hashlib.sha256(p.read_bytes()).hexdigest()=={bundle_sha!r}\n"
          f"r=pathlib.Path({destination!r});r.mkdir(parents=True,exist_ok=True)\n"
          "with tarfile.open(p) as t:\n"
          " m=t.getmembers();assert all(x.isfile() for x in m);t.extractall(r,filter='data')\n"
          "print('staged')")
    return 'python3 -c '+shlex.quote(code)


def launch_command(device,destination,manifest,version):
    if platform(device)=='macos':
        sha=manifest['artifacts']['remote-hosts-code-macos-arm64']['sha256']
        return shlex.join(['python3',destination+'/launch-code-upgrade.py','--candidate',destination+'/remote-hosts-code-macos-arm64','--sha256',sha,'--version',version,'--start'])
    if platform(device)=='linux':
        sha=manifest['artifacts']['remote-hosts-code-linux-amd64']['sha256'];unit='remote-hosts-code-upgrade-'+version.replace('.','-')
        args=['systemd-run','--user','--unit',unit,'--collect','--working-directory',destination,'python3',destination+'/upgrade-code-agent-linux.py','--candidate',destination+'/remote-hosts-code-linux-amd64','--sha256',sha,'--version',version,'--result',destination+'/upgrade-result.json']
        return shlex.join(args)
    sha=manifest['artifacts']['remote-hosts-code-windows-amd64.exe']['sha256']
    return ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File '+ps_quote(destination+'\\launch-code-upgrade-windows.ps1')+
            ' -Updater '+ps_quote(destination+'\\upgrade-code-agent-windows.ps1')+' -Candidate '+ps_quote(destination+'\\remote-hosts-code-windows-amd64.exe')+
            ' -Sha256 '+sha+' -Version '+version+' -Result '+ps_quote(destination+'\\upgrade-result.json'))


def main():
    p=argparse.ArgumentParser();p.add_argument('--version',required=True);p.add_argument('--package',type=pathlib.Path,required=True);p.add_argument('--config',type=pathlib.Path,required=True);p.add_argument('--report-dir',type=pathlib.Path,required=True);args=p.parse_args()
    package=args.package.resolve();directory=args.report_dir.resolve();directory.mkdir(parents=True,exist_ok=True);config=json.loads(args.config.read_text());manifest=verify_package(package,args.version);bundle,bundle_sha=ensure_bundle(package,manifest)
    state={'state':'running','phase':'gateway','version':args.version,'manifest_sha256':rr.digest(package/'manifest.json'),'bundle_sha256':bundle_sha,'agents':{}}
    def save(): rr.atomic_json(directory/'fleet.json',state)
    save();client=Client(config['origin'],pathlib.Path(config['password_file'])).login()
    try:
        # Local package is the authority. The configured management path stages
        # verified local bytes before cutover; GitHub releases/check-runs are not
        # consulted. API-only setups must have staged the same bundle explicitly.
        if 'gateway' in config:
            gateway=gateway_upgrade_ssh(config,package,manifest,args.version,directory)
        else:
            gateway=gateway_upgrade_api(client,args.version,bundle_sha)
            if gateway is None:
                raise RuntimeError('local gateway management configuration required')
        state['gateway']=gateway;save()
        fleet=client.tool('fleet_status',{'desired_version':args.version});devices=fleet['devices'];state['fleet']=fleet;save()
        if finish_observed_fleet(config,package,directory,args.version,state,fleet):return
        controller_id=controller_device_id(config)
        local_candidates=[d for d in devices if d['online'] and platform(d) in ('macos','linux')
                          and any(str(bundle).startswith(root.rstrip('/')+'/') for root in (d.get('capabilities') or {}).get('roots',[]))]
        controller=next((d for d in local_candidates if d['device_id']==controller_id),None) if controller_id else None
        if controller is None and local_candidates: controller=local_candidates[0]
        if controller is None: raise RuntimeError('no online device exposes the local release bundle inside an authorized root')
        controller_root=next(root for root in (controller.get('capabilities') or {}).get('roots',[]) if str(bundle).startswith(root.rstrip('/')+'/'))
        relative=str(bundle.relative_to(pathlib.Path(controller_root)))
        cws=open_workspace(client,controller['device_id'],controller_root,'fleet-'+args.version+'-controller')
        export_attempt=rr.identity([args.version,str(directory)])[:12]
        exported=client.tool('file_download',{'workspace_id':cws,'path':relative,'expected_version':bundle_sha,'max_bytes':268435456,'idempotency_key':'fleet-'+args.version+'-bundle-export-'+export_attempt})
        exported=verified_export(client,exported,bundle,bundle_sha,args.version)
        state['source_artifact']={'operation_id':exported['operation_id'],'sha256':bundle_sha,'size':exported_size(exported,bundle)};save()
        ordered,deferred=rollout_targets(devices,controller_id)
        for device in deferred:
            state['agents'][device['device_id']]={'name':device['name'],'platform':platform(device),'state':'waiting_online'}
        save()
        for device in ordered:
            ident=device['device_id'];state['agents'][ident]={'name':device['name'],'platform':platform(device),'state':'already_converged' if device['converged'] else 'staging'};save()
            if device['converged']: continue
            ws=open_workspace(client,ident,root_path(device),'fleet-'+args.version+'-'+ident)
            remote_bundle='.remote-hosts-release-staging-'+args.version+'/'+bundle.name
            imported=client.tool('file_upload',{'workspace_id':ws,'path':remote_bundle,'expected_version':'absent','sha256':bundle_sha,'max_bytes':268435456,'idempotency_key':'fleet-'+args.version+'-bundle-import-'+ident,'file':{'file_id':exported['artifact_id'],'download_url':exported['download_url'],'file_name':bundle.name}})
            imported=verified_import(client,imported,exported,bundle,bundle_sha,args.version,ident)
            h=home(device);destination=(h+'/.local/share/remote-hosts-code/releases/'+args.version) if platform(device)!='windows' else (h+'\\.local\\share\\remote-hosts-code\\releases\\'+args.version)
            client.terminal(ws,extract_command(device,remote_bundle,destination,bundle_sha),'fleet-'+args.version+'-extract-'+ident,120)
            output=client.terminal(ws,launch_command(device,destination,manifest,args.version),'fleet-'+args.version+'-launch-'+ident,60)
            state['agents'][ident].update(state='upgrading',launch_output=output[-2048:]);save()
            if ident==controller_id:
                state.update(state='handoff_pending',phase='controller_handoff',
                    next_action='controller updater is independent; let this invoking terminal exit, then rerun the same fleet command after the controller reconnects')
                save();print(json.dumps({'state':state['state'],'version':args.version,'all_converged':False,'report':str(directory/'fleet.json')}));return
        deadline=time.monotonic()+900
        while time.monotonic()<deadline:
            fleet=client.tool('fleet_status',{'desired_version':args.version});state['fleet']=fleet;save()
            if finish_observed_fleet(config,package,directory,args.version,state,fleet):return
            time.sleep(3)
        else: raise RuntimeError('fleet did not converge; inspect fleet.json without replaying upgrades')
    except Exception as error:
        state.update(state='needs_recovery',phase='finished',error_type=type(error).__name__,next_action='inspect fleet.json and original operation/updater receipts; do not replay blindly');save();raise
    finally: client.close()
    print(json.dumps({'state':state['state'],'version':args.version,'all_converged':state.get('all_converged',False),'report':str(directory/'fleet.json')}))

if __name__=='__main__':main()

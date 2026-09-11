"""Publish verified 0.4.1 to the three user-selected hosts and verify original receipts.

Runs independently of code-agent. Every mutation stage has a durable intent;
uncertain actions are never restarted automatically. No credentials in records.
"""
import hashlib, importlib.util, json, os, pathlib, shlex, shutil, subprocess, sys, tarfile, tempfile, time
R=pathlib.Path('/Users/jinliang/Workspace/remote_hosts')
D=R/'docs/releases/0.4.1';D.mkdir(parents=True,exist_ok=True)
WORK=R/'target/iteration-041-r1';VERSION='0.4.1'
BUILD=R/'target/iteration-041-r2/pipeline.json'
helper=R/'target/source-snapshots/041-r2/scripts/release_receipts.py'
assert hashlib.sha256(helper.read_bytes()).hexdigest()=='a2c70b5735c4d924173acb4b8f6d3250b1b2d1f008ad3e685ca1e0abf6a93398'
sys.path.insert(0,str(helper.parent));import release_receipts as rr
plan=rr.verified_build(BUILD,VERSION)
MAC='ba3bf113-2390-466e-88bc-40d5b4f02884';STUDIO='8af88e35-f316-4dd9-9810-fa5bd3a22196'
MW=MAC+':1d51494e-0c96-4a2e-92e6-1ee24ca33bb2';SW=STUDIO+':70854d05-8758-40ba-9aae-e71e10299d5b'
SSH=['ssh','-p','222','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=10','root@hackerlife.fun']
state={'version':VERSION,'state':'running','phase':'verified_inputs','targets':['NAS gateway','MacBook-M2-Max','Mac-Studio'],
       'manifest_sha256':plan['manifest_sha256'],'tests':plan['tests'],'targets_accepted':False,'operations':[]}
previous=D/'deployment-r1.json'
if previous.exists():
    saved=json.loads(previous.read_text())
    assert saved['manifest_sha256']==plan['manifest_sha256'] and saved['version']==VERSION
    if saved.get('targets_accepted') is True:
        print(json.dumps({'state':'already_accepted','report':str(previous)}));raise SystemExit(0)
    state.update(saved)
    state['state']='running'
client=None

def save():
    state['observed_at_epoch']=int(time.time());rr.atomic_json(D/'deployment-r1.json',state)
def phase(name):state['phase']=name;save();print(name,flush=True)
def run(argv,timeout=180):
    return subprocess.run(argv,cwd=R,capture_output=True,text=True,check=True,timeout=timeout).stdout

def terminal(ws,command,key,timeout=120):
    start=client.tool('terminal_exec',{'workspace_id':ws,'command':command,'idempotency_key':key,'timeout_seconds':timeout,'wait_ms':1000})
    state['operations'].append({'key':key,'workspace_id':ws,'terminal_id':start['terminal_id']});save()
    output=start.get('output','');cursor=start.get('cursor',0);status=start.get('terminal',{})
    deadline=time.monotonic()+timeout+60
    while not (status.get('exit_code') is not None and status.get('output_complete')):
        if time.monotonic()>=deadline:raise rr.ReleaseError('terminal_observation_timeout','observe original terminal without rerunning',start['terminal_id'])
        v=client.tool('terminal_read',{'workspace_id':ws,'terminal_id':start['terminal_id'],'cursor':cursor,'max_bytes':16000})
        output+=v['output'];cursor=v['cursor'];status=v['terminal'];time.sleep(.4)
    if status['exit_code']!=0:raise rr.ReleaseError('stage_command_failed','inspect original terminal output',start['terminal_id'])
    return output

try:
    binding={'version':VERSION,'manifest_sha256':plan['manifest_sha256'],'devices':[MAC,STUDIO],'gateway':'https://mcp.hackerlife.fun'}
    with rr.StepJournal(D/'publication-steps-r1.json',binding) as journal:
        source=pathlib.Path(plan['package']);package=R/'dist/remote-hosts-code-0.4.1'
        def preserve_package():
            package.parent.mkdir(exist_ok=True)
            if not package.exists():
                with tempfile.TemporaryDirectory(prefix='.release041-',dir=package.parent) as tmp:
                    stage=pathlib.Path(tmp)/'package';shutil.copytree(source,stage);stage.rename(package)
            assert rr.digest(package/'manifest.json')==plan['manifest_sha256']
            for name,info in plan['artifacts'].items():assert rr.digest(package/name)==info['sha256']
            return {'path':str(package),'manifest_sha256':plan['manifest_sha256']}
        journal.step('preserve_package',{'manifest':plan['manifest_sha256']},preserve_package)
        manifest=json.loads((package/'manifest.json').read_text());state['package']=str(package);save()
        for name,value in [('verification.json',json.loads((package/'source-verification.json').read_text())),('manifest.json',manifest),('pipeline.json',json.loads(BUILD.read_text()))]:
            p=D/name
            if p.exists():assert json.loads(p.read_text())==value
            else:rr.atomic_json(p,value)
        spec=importlib.util.spec_from_file_location('publication_client',R/'target/iteration-031/live-031.py')
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);client=m.Client();client.login()
        phase('stage_studio')
        names={'remote-hosts-code-macos-arm64','upgrade-code-agent.py','agent_upgrade_support.py','launch-code-upgrade.py','manifest.json'}
        archive=WORK/'studio-package.tgz'
        if not archive.exists():
            with tarfile.open(archive,'x:gz') as t:
                for name in sorted(names):t.add(package/name,arcname=name)
        archive_sha=rr.digest(archive)
        def stage_studio():
            exported=client.tool('file_download',{'workspace_id':MW,'path':str(archive.relative_to(R)),'expected_version':archive_sha,'idempotency_key':'r041-studio-export','max_bytes':67108864})
            state['operations'].append({'stage':'export','operation_id':exported['operation_id']});save()
            imported=client.tool('file_upload',{'workspace_id':SW,'path':'.remote-hosts-release-staging-041/package-r1.tgz','expected_version':'absent','sha256':archive_sha,'file':{'file_id':exported['artifact_id'],'download_url':exported['download_url'],'file_name':'package-r1.tgz'},'idempotency_key':'r041-studio-import','max_bytes':67108864})
            assert imported['state']=='completed' and imported['sha256']==archive_sha
            state['operations'].append({'stage':'import','operation_id':imported['operation_id']});save()
            code=f'''import pathlib,tarfile,hashlib,json
p=pathlib.Path.home()/'.local/share/remote-hosts-code/releases/0.4.1'
a=pathlib.Path('/Users/jinliang/workspace/.remote-hosts-release-staging-041/package-r1.tgz')
assert hashlib.sha256(a.read_bytes()).hexdigest()=={archive_sha!r}
with tarfile.open(a) as t:
 assert set(t.getnames())=={names!r} and all(m.isfile() for m in t.getmembers())
 data={{name:t.extractfile(name).read() for name in t.getnames()}}
 assert hashlib.sha256(data['manifest.json']).hexdigest()=={plan['manifest_sha256']!r}
 manifest=json.loads(data['manifest.json']);p.mkdir(parents=True,exist_ok=True)
 for name,payload in data.items():
  if name!='manifest.json':assert hashlib.sha256(payload).hexdigest()==manifest['artifacts'][name]['sha256']
  dest=p/name
  if dest.exists():assert dest.read_bytes()==payload
  else:
   with dest.open('xb') as f:f.write(payload)
  dest.chmod(0o755 if name=='remote-hosts-code-macos-arm64' else 0o600)
print(json.dumps({{'state':'staged','version':manifest['version']}}))'''
            output=terminal(SW,'/opt/homebrew/bin/python3 -c '+shlex.quote(code),'r041-studio-extract')
            assert json.loads(output)['version']==VERSION
            return {'archive_sha256':archive_sha,'import_operation_id':imported['operation_id']}
        journal.step('studio_stage',{'archive_sha256':archive_sha,'workspace':SW},stage_studio)
        phase('stage_nas')
        nas_dir='/opt/remote-hosts-code/releases/0.4.1'
        def stage_nas():
            run(SSH+['umask 077; mkdir -p '+shlex.quote(nas_dir)])
            for name in ('remote-hosts-code-linux-amd64','upgrade-code-gateway.py','manifest.json'):
                expected=rr.digest(package/name);dest=nas_dir+'/'+name
                current=run(SSH+['if test -e '+shlex.quote(dest)+'; then sha256sum '+shlex.quote(dest)+'; else printf absent; fi']).split()[0]
                if current=='absent':
                    run(['scp','-O','-P','222','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',str(package/name),'root@hackerlife.fun:'+nas_dir+'/'],timeout=600)
                else:assert current==expected
                assert run(SSH+['sha256sum '+shlex.quote(dest)]).split()[0]==expected
            return {'path':nas_dir,'manifest_sha256':plan['manifest_sha256']}
        journal.step('nas_stage',{'manifest':plan['manifest_sha256'],'destination':nas_dir},stage_nas)
        phase('upgrade_gateway')
        def upgrade_nas():
            cmd=['python3',nas_dir+'/upgrade-code-gateway.py','--candidate',nas_dir+'/remote-hosts-code-linux-amd64','--sha256',plan['artifacts']['remote-hosts-code-linux-amd64']['sha256'],'--version',VERSION,'--result',nas_dir+'/deployment-r1.json']
            run(SSH+[shlex.join(cmd)],timeout=180)
            result=json.loads(run(SSH+['cat '+shlex.quote(nas_dir+'/deployment-r1.json')]))
            assert result['state']=='upgraded' and result['version']==VERSION
            assert result['installed_sha256']==plan['artifacts']['remote-hosts-code-linux-amd64']['sha256']
            rr.atomic_json(D/'nas-updater.json',result)
            return result
        journal.step('nas_upgrade',{'candidate':plan['artifacts']['remote-hosts-code-linux-amd64']},upgrade_nas)
        phase('start_agent_upgrades')
        sha=plan['artifacts']['remote-hosts-code-macos-arm64']['sha256']
        studio_pkg='/Users/jinliang/.local/share/remote-hosts-code/releases/0.4.1'
        argv=['/opt/homebrew/bin/python3',studio_pkg+'/launch-code-upgrade.py','--candidate',studio_pkg+'/remote-hosts-code-macos-arm64','--sha256',sha,'--version',VERSION,'--start']
        sl=journal.step('studio_launch',{'sha256':sha,'device':STUDIO},lambda:json.loads(terminal(SW,shlex.join(argv),'r041-studio-launch',30)))
        ml=journal.step('macbook_launch',{'sha256':sha,'device':MAC},lambda:json.loads(run(['/opt/homebrew/bin/python3',str(package/'launch-code-upgrade.py'),'--candidate',str(package/'remote-hosts-code-macos-arm64'),'--sha256',sha,'--version',VERSION,'--start'],timeout=30)))
        for value in (ml,sl):assert value['state'] in ('started','already_requested')
        state['launches']={'macbook':ml,'studio':sl};save();phase('observe_original_updaters')
        deadline=time.monotonic()+360
        while True:
            inventory=client.tool('devices_list',{})
            selected=[d for d in inventory['devices'] if d['device_id'] in (MAC,STUDIO)]
            state['runtime_observation']=[{'device_id':d['device_id'],'online':d['online'],'version':d['capabilities']['version'],'last_seen':d['last_seen']} for d in selected];save()
            if len(selected)==2 and all(d['online'] and d['capabilities']['version']==VERSION for d in selected):break
            if time.monotonic()>deadline:raise rr.ReleaseError('runtime_not_ready','inspect original updater paths; do not reinstall')
            time.sleep(2)
        # One read-only Studio observer waits for its original result. It is not
        # recreated on each poll and never changes the updater or service.
        code="import pathlib,time; p=pathlib.Path("+repr(sl['result'])+");end=time.monotonic()+290\nwhile not p.exists():\n if time.monotonic()>end:raise TimeoutError('original receipt not yet present')\n time.sleep(1)\nprint(p.read_text())"
        observer=journal.step('studio_receipt_observer',{'original_result':sl['result']},lambda:client.tool('terminal_exec',{'workspace_id':SW,'command':'/opt/homebrew/bin/python3 -c '+shlex.quote(code),'timeout_seconds':300,'idempotency_key':'r041-studio-original-receipt'}))
        def studio_receipt():
            v=client.tool('terminal_read',{'workspace_id':SW,'terminal_id':observer['terminal_id'],'cursor':0,'max_bytes':16384})
            if v['terminal'].get('exit_code') is None or not v['terminal'].get('output_complete'):return None
            if v['terminal']['exit_code']!=0:raise rr.ReleaseError('receipt_reader_failed','inspect original receipt observer',observer['terminal_id'])
            return json.loads(v['output'])
        def mac_receipt():
            p=pathlib.Path(ml['result']);return json.loads(p.read_text()) if p.exists() else None
        def observe_updaters():
            receipts=rr.wait_receipts({'macbook':mac_receipt,'studio':studio_receipt},
                {'macbook':{'version':VERSION,'sha256':sha},'studio':{'version':VERSION,'sha256':sha}},
                on_update=lambda updates:(state.update(updater_observation=updates),save()))
            for name,value in receipts.items():rr.atomic_json(D/(name+'-updater.json'),value)
            return receipts
        receipts=journal.step('updaters_verified',{'macbook_result':ml['result'],'studio_result':sl['result'],'sha256':sha},observe_updaters)
        phase('both_device_functional_acceptance')
        def check_standard():
            path=D/'acceptance-two-devices.json';log=D/'acceptance-two-devices.log'
            cmd=['/opt/homebrew/bin/python3',str(package/'check-code-gateway.py'),'--origin','https://mcp.hackerlife.fun','--password-file','/Users/jinliang/.local/share/remote-hosts-code/setup/gateway-login-password.txt','--report',str(path),'--run-id','release041r1','--expected-version',VERSION,'--dispatch-protocol','2','--device-id',MAC,'--device-id',STUDIO]
            with log.open('x') as f:out=subprocess.run(cmd,cwd=R,stdout=f,stderr=subprocess.STDOUT,timeout=1200)
            assert out.returncode==0
            v=json.loads(path.read_text());assert v['state']=='passed' and v['test_oauth_grant_revoked'] and {d['device_id'] for d in v['devices']}=={MAC,STUDIO}
            return {'state':'passed','report':str(path)}
        journal.step('standard_acceptance',{'version':VERSION,'devices':[MAC,STUDIO],'run':'r1'},check_standard)
        phase('new_feature_acceptance')
        feature_script=WORK/'live_features.py'
        def check_new():
            env=dict(os.environ,PYTHONPATH=str(package))
            with (D/'features-r1.log').open('x') as f:
                out=subprocess.run(['/opt/homebrew/bin/python3',str(feature_script)],cwd=R,stdout=f,stderr=subprocess.STDOUT,env=env,timeout=900)
            assert out.returncode==0
            v=json.loads((D/'features-r1.json').read_text());assert v['state']=='passed' and v['temporary_oauth_revoked']
            assert {d['device_id'] for d in v['devices']}=={MAC,STUDIO}
            return {'state':'passed','report':str(D/'features-r1.json')}
        journal.step('feature_acceptance',{'script_sha256':rr.digest(feature_script)},check_new)
        phase('two_device_parallel_acceptance')
        parallel_script=WORK/'live_parallel.py'
        def check_parallel():
            env=dict(os.environ,PYTHONPATH=str(package))
            with (D/'parallel-r1.log').open('x') as f:
                out=subprocess.run(['/opt/homebrew/bin/python3',str(parallel_script)],cwd=R,stdout=f,stderr=subprocess.STDOUT,env=env,timeout=900)
            assert out.returncode==0
            v=json.loads((D/'parallel-r1.json').read_text());assert v['state']=='passed' and v['temporary_oauth_revoked']
            assert {d['device_id'] for d in v['devices']}=={MAC,STUDIO}
            return {'state':'passed','report':str(D/'parallel-r1.json'),'overlap_observed':v['overlap_observed']}
        journal.step('parallel_acceptance',{'script_sha256':rr.digest(parallel_script)},check_parallel)
        inventory=client.tool('devices_list',{})
        for ident,who in ((MAC,'macbook'),(STUDIO,'studio')):
            live=next(d for d in inventory['devices'] if d['device_id']==ident)
            assert live['online'] and live['capabilities']['version']==VERSION and live['capabilities']['session']==receipts[who]['session']
        rr.atomic_json(D/'runtime-final.json',inventory)
        state.update(state='selected_targets_deployed_and_accepted',phase='complete',targets_accepted=True)
except Exception as error:
    state.update(state='needs_recovery',failure_type=type(error).__name__,
        failure_code=getattr(error,'code',None),next_action=getattr(error,'recovery','inspect the saved stage result without repeating successful updates'))
finally:
    if client:
        try:state['temporary_oauth_revoked']=client.close()
        except Exception:state['temporary_oauth_revoked']=False
        if not state['temporary_oauth_revoked']:state['state']='needs_recovery';state['targets_accepted']=False
    save();print(json.dumps({'state':state['state'],'phase':state['phase'],'targets_accepted':state['targets_accepted'],'report':str(D/'deployment-r1.json')}),flush=True)
if not state['targets_accepted']:raise SystemExit(1)

#!/usr/bin/env python3
"""Explicit deployed collaboration acceptance. Isolated worktree fixtures; no service changes."""
import argparse
import hashlib
import json
import pathlib
import shlex
import time
from release_client import Client
from release_receipts import atomic_json

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=pathlib.Path,required=True);p.add_argument('--device-id',required=True);p.add_argument('--version',required=True);p.add_argument('--report',type=pathlib.Path,required=True);args=p.parse_args()
    if args.report.exists():raise SystemExit('existing probe retained; inspect original result')
    config=json.loads(args.config.read_text());agent=next(a for a in config['agents'] if a['device_id']==args.device_id)
    c=Client(config['origin'],config['password_file']);r={'version':args.version,'device_id':args.device_id,'state':'running','checks':{},'operation_ids':[]};phase='login';folder='remote_hosts_accept_050_'+args.device_id[:8];key='collab-'+args.version+'-'+args.device_id
    def record():r['phase']=phase;atomic_json(args.report,r)
    def term(ws,code,stage):return c.terminal(ws,'/opt/homebrew/bin/python3 -c '+shlex.quote(code),key+'-'+stage,60)
    try:
        c.login();inventory=c.tool('devices_list',{});device=next(d for d in inventory['devices'] if d['device_id']==args.device_id)
        assert device['online'] and device['capabilities']['version']==args.version
        phase='fixture';record()
        term(agent['workspace_id'],"import pathlib,subprocess,os;p=pathlib.Path("+repr(folder)+");p.mkdir(exist_ok=False);subprocess.run(['git','init','-q',str(p)],env=dict(os.environ,GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1'),check=True)",'fixture')
        workspace=c.tool('workspace_open',{'device_id':args.device_id,'root':agent['root']+'/'+folder,'idempotency_key':key+'-open'})['workspace']['id'];r['workspace_id']=workspace;record()
        files=json.loads(term(workspace,"import pathlib,json,hashlib;d=pathlib.Path('dst');d.mkdir();files=[]\nfor i in range(100):\n old=bytes([i,0,255])*17;new=bytes([i,1,254])*17 if i<3 else old;p=d/f'f{i}.bin';p.write_bytes(old);files.append({'path':str(p),'sha256':hashlib.sha256(new).hexdigest(),'size':len(new),'executable':False})\nprint(json.dumps(files))",'files'))
        before=c.tool('workspace_context',{'workspace_id':workspace,'limit':50});event_cursor=before['events']['cursor']
        phase='complete_review';record()
        review=c.tool('code_diff',{'workspace_id':workspace,'paths':['dst'],'max_bytes':4096});assert review['untracked_count']==100 and review['diff'];r['checks']['untracked_review']={'passed':True,'count':100,'empty_diff':False}
        phase='delta_plan';record()
        plan=c.tool('files_sync',{'workspace_id':workspace,'idempotency_key':key+'-plan','mode':'plan','files':files})
        changed=[x for x in plan['actions'] if x['action']=='upload'];assert len(changed)==3
        selected=[x for x in plan['files'] if x['path'] in {x['path'] for x in changed}]
        header={'manifest_id':plan['manifest_id'],'entries':selected}
        code="import pathlib,json,struct,hashlib;h=json.dumps("+repr(header)+",separators=(',',':')).encode();b=b'RHSYNC1\\n'+struct.pack('<I',len(h))+h+b''.join(bytes([i,1,254])*17 for i in range(3));pathlib.Path('bundle.source').write_bytes(b);print(json.dumps({'sha256':hashlib.sha256(b).hexdigest(),'size':len(b)}))"
        bundle=json.loads(term(workspace,code,'bundle'))
        exported=c.tool('file_download',{'workspace_id':workspace,'path':'bundle.source','expected_version':bundle['sha256'],'idempotency_key':key+'-export'})
        imported=c.tool('file_upload',{'workspace_id':workspace,'path':'bundle.received','expected_version':'absent','sha256':bundle['sha256'],'idempotency_key':key+'-import','file':{'file_id':exported['artifact_id'],'download_url':exported['download_url'],'file_name':'bundle.bin'}})
        assert imported['state']=='completed' and imported['sha256']==bundle['sha256']
        phase='delta_apply';record()
        apply={'workspace_id':workspace,'idempotency_key':key+'-apply','mode':'apply','files':plan['files'],'manifest_id':plan['manifest_id'],'bundle_path':'bundle.received','bundle_sha256':bundle['sha256']}
        applied=c.tool('files_sync',apply);r['operation_ids'].append(applied['operation_id'])
        assert applied['state']=='completed' and applied['changed_files']==3 and applied['reused_files']==97 and applied['bytes_written']==153
        assert c.tool('files_sync',apply)==applied
        r['checks']['delta_sync']={'passed':True,'manifest_files':100,'changed_files':3,'reused_files':97,'content_bytes_written':153,'network_bundle_bytes':bundle['size'],'same_key_replay_unchanged':True};record()
        phase='terminal_observation';record()
        started=c.tool('terminal_exec',{'workspace_id':workspace,'command':'sleep 0.2; printf observed; exit 9','idempotency_key':key+'-observe','timeout_seconds':5})
        r['operation_ids'].append(started['operation_id']);end=time.monotonic()+30;polls=0
        while time.monotonic()<end:
            value=c.tool('operation_get',{'operation_id':started['operation_id']});polls+=1
            terminal=value.get('terminal_observation',{}).get('terminal',{})
            if terminal.get('exit_code')==9 and terminal.get('output_complete'):break
            time.sleep(.5)
        else:raise TimeoutError('original terminal status not replicated')
        output=c.tool('terminal_read',{'workspace_id':workspace,'terminal_id':started['terminal_id'],'cursor':0,'max_bytes':1024});assert output['output']=='observed'
        r['checks']['unified_terminal_observation']={'passed':True,'status_observations':polls,'output_read_calls':1,'exit_code':9}
        phase='event_handoff';record()
        events=c.tool('workspace_context',{'workspace_id':workspace,'after_event':event_cursor,'limit':50})['events'];assert events['items'];assert any(e['entity_id']==applied['operation_id'] and e['state']=='done' for e in events['items'])
        assert 'download_url' not in json.dumps(events);r['checks']['event_replay']={'passed':True,'events':len(events['items']),'same_manifest_completion_present':True}
        phase='conflict_recovery';record()
        term(workspace,"import pathlib;pathlib.Path('dst/f0.bin').write_bytes(b'user-edit')",'user-edit')
        conflict=dict(apply,idempotency_key=key+'-conflict');rejected=c.raw('files_sync',conflict)
        while rejected.get('pending'):time.sleep(.5);rejected=c.raw('operation_get',{'operation_id':rejected['operation_id']})
        assert rejected.get('error_code')=='version_conflict';assert rejected['automatic_replay_safe'] is False
        r['checks']['concurrent_edit_protection']={'passed':True,'error_code':rejected['error_code'],'recovery_action':rejected['recovery_action']}
        phase='compact_response';record()
        full=c.rpc('tools/call',{'name':'devices_list','arguments':{}});compact=c.rpc('tools/call',{'name':'devices_list','arguments':{'response_mode':'compact'}})
        full_text=sum(len(x.get('text','').encode()) for x in full['content']);compact_text=sum(len(x.get('text','').encode()) for x in compact['content']);assert compact_text<256 and len(compact['structuredContent']['devices'])==len(full['structuredContent']['devices'])
        r['checks']['compact_response']={'passed':True,'text_bytes_full':full_text,'text_bytes_compact':compact_text,'scope':'opt-in text channel only; structured content preserved; not token benchmark'}
        phase='cleanup';record()
        term(workspace,"import pathlib,hashlib,json;p=pathlib.Path('.');assert {x.name for x in p.iterdir()}=={'.git','dst','bundle.source','bundle.received'};assert {x.name for x in (p/'dst').iterdir()}=={f'f{i}.bin' for i in range(100)}\nfor i in range(100):\n expected=b'user-edit' if i==0 else (bytes([i,1,254])*17 if i<3 else bytes([i,0,255])*17);assert(p/'dst'/f'f{i}.bin').read_bytes()==expected\nassert hashlib.sha256((p/'bundle.received').read_bytes()).hexdigest()=="+repr(bundle['sha256']), 'verify-cleanup')
        term(agent['workspace_id'],"import pathlib,shutil;p=pathlib.Path("+repr(folder)+");assert p.is_dir() and not p.is_symlink();shutil.rmtree(p)",'cleanup')
        r['fixture_removed']=True;r['state']='passed';phase='complete'
    except Exception as error:r.update(state='failed',failure_type=type(error).__name__,recovery='inspect recorded original operations and fixture; do not reinstall')
    finally:
        try:r['temporary_oauth_revoked']=c.close()
        except Exception:r['temporary_oauth_revoked']=False;r['state']='failed'
        record();print(json.dumps(r),flush=True)
    if r['state']!='passed':raise SystemExit(1)

if __name__=='__main__':main()

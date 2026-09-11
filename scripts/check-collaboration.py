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
    c=Client(config['origin'],config['password_file']);r={'version':args.version,'device_id':args.device_id,'state':'running','checks':{},'operation_ids':[]};phase='login';folder='remote_hosts_accept_070_'+args.device_id[:8];key='collab-'+args.version+'-'+args.device_id;version=tuple(map(int,args.version.split('.')))
    def record():r['phase']=phase;atomic_json(args.report,r)
    def term(ws,code,stage):return c.terminal(ws,'/opt/homebrew/bin/python3 -c '+shlex.quote(code),key+'-'+stage,60)
    try:
        c.login();inventory=c.tool('devices_list',{});device=next(d for d in inventory['devices'] if d['device_id']==args.device_id)
        assert device['online'] and device['capabilities']['version']==args.version
        if version >= (0,7,0):
            limits=device['capabilities'].get('transfer_limits');features=device.get('runtime_features',{}).get('names',[])
            assert limits and limits['protocol']==1 and limits['default_max_bytes']==67108864 and limits['hard_max_bytes']==268435456 and limits['checkpoint_bytes']==4194304
            assert 'large_file_transfer_v1' in features and 'source_authorization_status_v1' in features
            r['checks']['negotiated_transfer_limits']={'passed':True,'default_max_bytes':limits['default_max_bytes'],'hard_max_bytes':limits['hard_max_bytes'],'checkpoint_bytes':limits['checkpoint_bytes']}
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
        if version >= (0,7,0):
            phase='large_file_roundtrip';record();large_size=64*1024*1024+1024*1024
            large=json.loads(term(workspace,"import pathlib,hashlib,json;p=pathlib.Path('large.source');p.write_bytes(b'');p.open('r+b').truncate("+str(64*1024*1024+1024*1024)+");h=hashlib.sha256();f=p.open('rb')\nwhile b:=f.read(1024*1024):h.update(b)\nprint(json.dumps({'size':p.stat().st_size,'sha256':h.hexdigest()}))",'large-create'))
            assert large['size']==large_size
            large_export=c.tool('file_download',{'workspace_id':workspace,'path':'large.source','expected_version':large['sha256'],'max_bytes':70*1024*1024,'idempotency_key':key+'-large-export'});r['operation_ids'].append(large_export['operation_id'])
            large_import=c.tool('file_upload',{'workspace_id':workspace,'path':'large.received','expected_version':'absent','sha256':large['sha256'],'max_bytes':70*1024*1024,'idempotency_key':key+'-large-import','file':{'file_id':large_export['artifact_id'],'download_url':large_export['download_url'],'file_name':'large.bin'}});r['operation_ids'].append(large_import['operation_id'])
            assert large_import['state']=='completed' and large_import['sha256']==large['sha256'] and large_import['size']==large_size
            observed=json.loads(term(workspace,"import pathlib,hashlib,json;p=pathlib.Path('large.received');h=hashlib.sha256();f=p.open('rb')\nwhile b:=f.read(1024*1024):h.update(b)\nprint(json.dumps({'size':p.stat().st_size,'sha256':h.hexdigest()}))",'large-verify'))
            assert observed==large
            r['checks']['large_file_roundtrip']={'passed':True,'bytes':large_size,'sha256':large['sha256'],'directions':['device_to_gateway','gateway_to_device'],'explicit_max_bytes':70*1024*1024};record()
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
        lifecycle=value['operation_lifecycle'];gateway_timing=lifecycle['gateway'];agent_timing=value['timing']
        assert gateway_timing['clock']=='gateway_unix_ms_same_host' and gateway_timing['queue_ms'] is not None and gateway_timing['dispatch_to_result_ms'] is not None
        assert agent_timing['clock']=='agent_monotonic' and 'waiting_resource' in agent_timing['phase_ms'] and 'running' in agent_timing['phase_ms']
        r['checks']['unified_terminal_observation']={'passed':True,'status_observations':polls,'output_read_calls':1,'exit_code':9,
            'lifecycle':{'gateway_queue_ms':gateway_timing['queue_ms'],'gateway_dispatch_to_result_ms':gateway_timing['dispatch_to_result_ms'],
                         'agent_phase_ms':agent_timing['phase_ms'],'cross_clock_subtraction_used':False}}
        phase='change_set_recovery';record()
        edit=c.tool('code_apply_edits',{'workspace_id':workspace,'idempotency_key':key+'-change-create','files':[
            {'path':'change/a.txt','expected_version':'absent','action':'create','content':'alpha\n'},
            {'path':'change/b.txt','expected_version':'absent','action':'create','content':'beta\n'}]})
        change_id=edit['change_set']['change_set_id'];assert edit['state']=='completed'
        resumed=c.tool('change_resume',{'workspace_id':workspace,'idempotency_key':key+'-change-resume','change_set_id':change_id})
        assert resumed['state']=='completed' and resumed['already_applied']==2
        term(workspace,"import pathlib;pathlib.Path('change/a.txt').write_text('user edit\\n')",'change-user-edit')
        conflict=c.raw('change_resume',{'workspace_id':workspace,'idempotency_key':key+'-change-conflict','change_set_id':change_id})
        while conflict.get('pending'):time.sleep(.5);conflict=c.raw('operation_get',{'operation_id':conflict['operation_id']})
        assert conflict['state']=='partial' and conflict['error_code']=='version_or_io_conflict' and conflict['automatic_replay_safe'] is False
        assert term(workspace,"import pathlib;print(pathlib.Path('change/a.txt').read_text(),end='')",'change-verify')=='user edit\n'
        r['checks']['change_set_recovery']={'passed':True,'change_set_id':change_id,'already_applied':2,'concurrent_user_edit_preserved':True,
            'recovery_action':conflict['recovery_action']}
        phase='workspace_gc';record()
        preview=c.tool('workspace_gc',{'workspace_id':workspace,'idempotency_key':key+'-gc-preview','action':'preview','older_than_seconds':3600,'max_items':100})
        assert preview['state']=='preview' and preview['gc_protocol']==1 and 'local_idempotency_results' in preview['protected']
        applied_gc=c.tool('workspace_gc',{'workspace_id':workspace,'idempotency_key':key+'-gc-apply','action':'apply','older_than_seconds':3600,'max_items':100,'preview_id':preview['preview_id']})
        assert applied_gc['state']=='completed' and applied_gc['idempotency_records_preserved'] is True
        r['checks']['workspace_gc']={'passed':True,'preview_candidates':preview['candidate_count'],'removed_count':applied_gc['removed_count'],
            'idempotency_records_preserved':True,'policy_bound_by_preview':True}
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
        expected_names={'.git','dst','change','bundle.source','bundle.received'}|({'large.source','large.received'} if version >= (0,7,0) else set())
        term(workspace,"import pathlib,hashlib,json;p=pathlib.Path('.');assert {x.name for x in p.iterdir()}=="+repr(expected_names)+";assert {x.name for x in (p/'dst').iterdir()}=={f'f{i}.bin' for i in range(100)}\nfor i in range(100):\n expected=b'user-edit' if i==0 else (bytes([i,1,254])*17 if i<3 else bytes([i,0,255])*17);assert(p/'dst'/f'f{i}.bin').read_bytes()==expected\nassert (p/'change'/'a.txt').read_text()=='user edit\\n' and (p/'change'/'b.txt').read_text()=='beta\\n';assert hashlib.sha256((p/'bundle.received').read_bytes()).hexdigest()=="+repr(bundle['sha256']), 'verify-cleanup')
        term(agent['workspace_id'],"import pathlib,shutil;p=pathlib.Path("+repr(folder)+");assert p.is_dir() and not p.is_symlink();shutil.rmtree(p)",'cleanup')
        r['fixture_removed']=True;r['state']='passed';phase='complete'
    except Exception as error:r.update(state='failed',failure_type=type(error).__name__,recovery='inspect recorded original operations and fixture; do not reinstall')
    finally:
        try:r['temporary_oauth_revoked']=c.close()
        except Exception:r['temporary_oauth_revoked']=False;r['state']='failed'
        record();print(json.dumps(r),flush=True)
    if r['state']!='passed':raise SystemExit(1)

if __name__=='__main__':main()

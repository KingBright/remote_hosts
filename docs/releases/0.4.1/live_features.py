"""Selected live 0.4.1 checks. Synthetic fixtures, existing owner OAuth, no service changes."""
import hashlib, importlib.util, json, pathlib, secrets, shlex, time, urllib.request
R=pathlib.Path('/Users/jinliang/Workspace/remote_hosts')
D=R/'docs/releases/0.4.1';D.mkdir(parents=True,exist_ok=True)
REPORT=D/'features-r1.json'
if REPORT.exists():
    saved=json.loads(REPORT.read_text())
    if saved.get('state')=='passed':print(json.dumps({'state':'existing_pass','report':str(REPORT)}));raise SystemExit(0)
    raise SystemExit('Retain the original failed feature report; inspect its operation IDs')
spec=importlib.util.spec_from_file_location('live_client',R/'target/iteration-031/live-031.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
c=m.Client();result={'version':'0.4.1','state':'running','devices':[]};phase='login'

def record():
    from release_receipts import atomic_json
    atomic_json(REPORT,result)

def term(ws,code,key):
    t=c.tool('terminal_exec',{'workspace_id':ws,'idempotency_key':key,
        'command':'/opt/homebrew/bin/python3 -c '+shlex.quote(code),'timeout_seconds':30,'wait_ms':2000})
    result.setdefault('terminal_ids',[]).append(t['terminal_id']);record()
    output=t.get('output','');cursor=t.get('cursor',0)
    terminal=t.get('terminal',{})
    end=time.monotonic()+60
    while not (terminal.get('exit_code') is not None and terminal.get('output_complete')):
        if time.monotonic()>end:raise TimeoutError('fixture observer timed out')
        v=c.tool('terminal_read',{'workspace_id':ws,'terminal_id':t['terminal_id'],'cursor':cursor,'max_bytes':16384})
        output+=v['output'];cursor=v['cursor'];terminal=v['terminal'];time.sleep(.2)
    if terminal['exit_code']!=0:raise RuntimeError('fixture command failed; original terminal saved')
    return output

try:
    c.login();catalog=c.rpc('tools/list',{})['tools']
    context=next(t for t in catalog if t['name']=='workspace_context')
    assert {'active_only','terminal_cursor'}.issubset(context['inputSchema']['properties'])
    for device in (m.MACBOOK,m.STUDIO):
        ws=m.WORKSPACES[device];name='r041-'+device[:8];folder='.remote-hosts-041-live-r1'
        phase='fixture:'+device
        data=json.loads(term(ws,"import pathlib,hashlib,json;d=pathlib.Path("+repr(folder)+");d.mkdir(exist_ok=False);b=bytes(range(256))*513;(d/'source.bin').write_bytes(b);print(json.dumps({'sha256':hashlib.sha256(b).hexdigest(),'size':len(b)}))",name+'-fixture'))
        sha=data['sha256'];size=data['size']
        exported=c.tool('file_download',{'workspace_id':ws,'path':folder+'/source.bin','expected_version':sha,'idempotency_key':name+'-export'})
        result.setdefault('operation_ids',[]).append(exported['operation_id']);record()
        phase='range_validation:'+device
        request=urllib.request.Request(exported['download_url'],headers={'Range':'bytes=9-','If-Range':'"different"','User-Agent':'RemoteHosts-Acceptance/0.4.1'})
        with urllib.request.urlopen(request,timeout=60) as response:
            assert response.status==200
            actual=response.read(size+1);assert len(actual)==size and hashlib.sha256(actual).hexdigest()==sha
        for range_value,validator,expected in [('bytes=-17',None,bytes(range(256))[-17:]),('bytes=9-25','"'+sha+'"',bytes(range(256))[9:26])]:
            headers={'Range':range_value,'User-Agent':'RemoteHosts-Acceptance/0.4.1'}
            if validator:headers['If-Range']=validator
            with urllib.request.urlopen(urllib.request.Request(exported['download_url'],headers=headers),timeout=60) as response:
                assert response.status==206 and response.read(100)==expected
        phase='context_pages:'+device
        page=c.tool('workspace_context',{'workspace_id':ws,'limit':2});ids=[];pages=0
        assert 'summary' in page
        while True:
            pages+=1;ids += [v['id'] for v in page['terminals']]
            nxt=page.get('next_terminal_cursor')
            if not nxt or pages>=3:break
            page=c.tool('workspace_context',{'workspace_id':ws,'limit':2,'terminal_cursor':nxt})
        assert len(ids)==len(set(ids))
        active=c.tool('workspace_context',{'workspace_id':ws,'limit':50,'active_only':True})
        assert all(t['state'] in ('running','starting') for t in active['terminals'])
        assert 'download_url' not in json.dumps(active)
        phase='cancelled_progress:'+device
        reference={'file_id':'fixture-'+secrets.token_hex(8),'download_url':m.ORIGIN+'/files/'+secrets.token_hex(32)+'/expired','mime_type':'application/octet-stream'}
        paused=c.tool('file_upload',{'workspace_id':ws,'path':folder+'/cancelled.bin','file':reference,'sha256':sha,'idempotency_key':name+'-cancel-fixture','expected_version':'absent'})
        original=paused['operation_id'];result['operation_ids'].append(original);record()
        assert paused['state'] in ('paused','awaiting_source') and paused['resumable']
        c.tool('transfer_cancel',{'operation_id':original,'idempotency_key':name+'-cancel'})
        final=c.tool('operation_get',{'operation_id':original})
        assert final['state']=='cancelled' and final['cleanup_complete'] and final['progress']['phase']=='cancelled'
        term(ws,"import pathlib,hashlib;d=pathlib.Path("+repr(folder)+");assert not(d/'cancelled.bin').exists();assert hashlib.sha256((d/'source.bin').read_bytes()).hexdigest()=="+repr(sha)+";(d/'source.bin').unlink();d.rmdir()",name+'-cleanup')
        result['devices'].append({'device_id':device,'workspace_id':ws,'if_range_mismatch':'passed','matching_range':'passed',
            'suffix_range':'passed','terminal_pagination':'passed','pages_observed':pages,'unique_terminal_ids':len(ids),
            'active_only':'passed','cancelled_progress':'passed','cancel_cleanup':'passed','fixture_removed':True})
        record()
    result['state']='passed'
except Exception as error:
    result.update(state='failed',phase=phase,error_type=type(error).__name__)
finally:
    try:result['temporary_oauth_revoked']=c.close()
    except Exception:result['temporary_oauth_revoked']=False;result['state']='failed'
    record();print(json.dumps(result),flush=True)
if result['state']!='passed':raise SystemExit(1)

"""Two selected devices exporting concurrently while code/terminal probes run."""
import concurrent.futures, hashlib, importlib.util, json, pathlib, shlex, time
from release_receipts import atomic_json
R=pathlib.Path('/Users/jinliang/Workspace/remote_hosts');D=R/'docs/releases/0.4.1';REPORT=D/'parallel-r1.json'
if REPORT.exists():
    saved=json.loads(REPORT.read_text())
    if saved.get('state')=='passed':raise SystemExit(0)
    raise SystemExit('inspect original parallel run; do not duplicate its transfers')
spec=importlib.util.spec_from_file_location('parallel_client',R/'target/iteration-031/live-031.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
c=m.Client();report={'version':'0.4.1','state':'running','devices':[],'overlap_observed':False};folder='.remote-hosts-041-parallel-r1'

def raw(client,name,args):
    r=client.rpc('tools/call',{'name':name,'arguments':args});v=r.get('structuredContent')
    if v is None or 'error' in v or r.get('isError'):raise RuntimeError('parallel tool failed; original IDs retained')
    return v

def sibling():
    x=m.Client();x.access=c.access;return x

def run_term(client,ws,code,key):
    v=client.tool('terminal_exec',{'workspace_id':ws,'command':'/opt/homebrew/bin/python3 -c '+shlex.quote(code),'idempotency_key':key,'timeout_seconds':30,'wait_ms':2000})
    cursor=v.get('cursor',0);output=v.get('output','');t=v.get('terminal',{});deadline=time.monotonic()+60
    while not(t.get('exit_code') is not None and t.get('output_complete')):
        if time.monotonic()>deadline:raise TimeoutError('original fixture terminal still pending')
        page=client.tool('terminal_read',{'workspace_id':ws,'terminal_id':v['terminal_id'],'cursor':cursor,'max_bytes':4096});cursor=page['cursor'];output+=page['output'];t=page['terminal'];time.sleep(.1)
    assert t['exit_code']==0
    return output

try:
    c.login();ids=(m.MACBOOK,m.STUDIO);clients={d:sibling() for d in ids};fixtures={}
    for d in ids:
        code="import pathlib,hashlib,json;d=pathlib.Path("+repr(folder)+");d.mkdir(exist_ok=False);b=bytes(range(256))*256\nf=d.joinpath('data.bin').open('xb');h=hashlib.sha256()\nfor _ in range(256):f.write(b);h.update(b)\nf.close();(d/'read.txt').write_text('parallel-read-ok\\n');print(json.dumps({'size':256*len(b),'sha256':h.hexdigest()}))"
        fixtures[d]=json.loads(run_term(clients[d],m.WORKSPACES[d],code,'r041-parallel-fixture-'+d))
    exports={};started=time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        pending={d:pool.submit(raw,clients[d],'file_download',{'workspace_id':m.WORKSPACES[d],'path':folder+'/data.bin','max_bytes':fixtures[d]['size'],'expected_version':fixtures[d]['sha256'],'idempotency_key':'r041-parallel-export-'+d}) for d in ids}
        exports={d:f.result() for d,f in pending.items()}
    report['exports']={d:{'operation_id':v['operation_id'],'pending_at_probe_start':bool(v.get('pending'))} for d,v in exports.items()};atomic_json(REPORT,report)
    def probe(d):
        client=clients[d];ws=m.WORKSPACES[d];begin=time.monotonic()
        r=client.tool('code_read',{'workspace_id':ws,'requests':[{'path':folder+'/read.txt','start_line':1,'end_line':1}]});assert r['ranges'][0]['text']=='parallel-read-ok\n'
        client.tool('code_apply_edits',{'workspace_id':ws,'idempotency_key':'r041-parallel-write-'+d,'files':[{'path':folder+'/write.txt','action':'create','expected_version':'absent','content':'parallel-write-ok\n'}]})
        output=run_term(client,ws,"print('parallel-command-ok')",'r041-parallel-terminal-'+d);assert output=='parallel-command-ok\n'
        after=raw(client,'operation_get',{'operation_id':exports[d]['operation_id']})
        return {'device_id':d,'read_write_terminal':'passed','probe_seconds':round(time.monotonic()-begin,3),
                'export_still_pending_after_probes':bool(after.get('pending')),'operation_id':exports[d]['operation_id']}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        report['devices']=list(pool.map(probe,ids))
    report['overlap_observed']=all(report['exports'][d]['pending_at_probe_start'] for d in ids) and any(v['export_still_pending_after_probes'] for v in report['devices'])
    atomic_json(REPORT,report)
    for d in ids:
        v=clients[d].tool('operation_get',{'operation_id':exports[d]['operation_id']})
        assert v['state']=='completed' and v['sha256']==fixtures[d]['sha256'] and v['size']==fixtures[d]['size']
        entry=next(x for x in report['devices'] if x['device_id']==d)
        entry.update(export_sha256=v['sha256'],bytes=v['size'],confirmed_bytes=v.get('confirmed_bytes'),export='passed')
        clean="import pathlib,hashlib;d=pathlib.Path("+repr(folder)+");h=hashlib.sha256((d/'data.bin').read_bytes()).hexdigest();assert h=="+repr(fixtures[d]['sha256'])+";assert (d/'read.txt').read_text()=='parallel-read-ok\\n';assert (d/'write.txt').read_text()=='parallel-write-ok\\n'\nfor n in ('data.bin','read.txt','write.txt'):(d/n).unlink()\nd.rmdir()"
        run_term(clients[d],m.WORKSPACES[d],clean,'r041-parallel-cleanup-'+d)
        entry['fixture_removed']=True
    report.update(state='passed',elapsed_seconds=round(time.monotonic()-started,3))
except Exception as e:report.update(state='failed',error_type=type(e).__name__)
finally:
    try:report['temporary_oauth_revoked']=c.close()
    except Exception:report['temporary_oauth_revoked']=False;report['state']='failed'
    atomic_json(REPORT,report);print(json.dumps(report),flush=True)
if report['state']!='passed':raise SystemExit(1)

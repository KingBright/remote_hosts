"""Explicit live MCP checks using existing local owner authorization.
No credentials or signed links in persisted reports. No service changes.
"""
import importlib.util
import json
import pathlib
import time

ROOT=pathlib.Path.cwd()
spec=importlib.util.spec_from_file_location('prior_live_client',ROOT/'target/iteration-031/live-031.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
REPORT=ROOT/'docs/releases/0.3.2/features-macbook.json'
if REPORT.exists():raise SystemExit('fresh report required; previous evidence not overwritten')
c=module.Client()
result={'version':'0.3.2','state':'running','interface':'authenticated_direct_mcp; host_schema_not_assumed_refreshed','device_id':module.MACBOOK}
try:
    c.login()
    c.opener.addheaders=[('User-Agent','RemoteHosts-Acceptance/0.3.2')]
    catalog=c.rpc('tools/list',{})
    descriptors={t['name']:t for t in catalog['tools']}
    assert 'operation_ids' in descriptors['operation_get']['inputSchema']['properties']
    devices=c.tool('devices_list',{})
    assert devices['gateway']['version']=='0.3.2'
    stamp=devices['gateway']['tools_sha256']
    good=c.tool('devices_list',{'known_tools_sha256':stamp})
    bad=c.tool('devices_list',{'known_tools_sha256':'0'*64})
    assert good['gateway']['client_schema_comparison']=='match' and not good['gateway']['refresh_required']
    assert bad['gateway']['refresh_required']
    selected=next(d for d in devices['devices'] if d['device_id']==module.MACBOOK)
    assert selected['online'] and selected['capabilities']['version']=='0.3.2'
    assert selected['runtime_features_status']=='reported_by_agent'
    assert 'terminal_wait_v1' in selected['runtime_features']['names']
    ws=module.WORKSPACES[module.MACBOOK]
    a=c.tool('code_read',{'workspace_id':ws,'requests':[{'path':'Cargo.toml','start_line':1,'end_line':2}]})
    b=c.tool('code_read',{'workspace_id':ws,'requests':[{'path':'crates/remote-hosts-code/Cargo.toml','start_line':1,'end_line':4}]})
    request={'operation_ids':[a['operation_id'],b['operation_id']]}
    started=time.monotonic();batch=c.tool('operation_get',request);elapsed=time.monotonic()-started
    assert batch['operations']==[a,b] and batch['pending_count']==0
    same=c.tool('operation_get',dict(request,cursor=batch['observation']['cursor'],wait_ms=1000))
    assert not same['observation']['changed']
    invalid=c.rpc('tools/call',{'name':'operation_get','arguments':{'operation_ids':[a['operation_id'],a['operation_id']]}})
    assert invalid.get('isError') is True
    result.update(state='passed',tool_count=len(descriptors),tools_sha256=stamp,
        schema_match_and_mismatch='passed',runtime_features=selected['runtime_features'],
        batch_result_count=2,batch_order_and_exact_payload='passed',unchanged_cursor='passed',
        duplicate_request_rejected='passed',single_batch_request_seconds=elapsed,
        network_timing_scope='one observed request, not a performance benchmark',
        queued_probe_work='two bounded code reads only; no code mutations',
        operation_ids=request['operation_ids'])
except Exception as error:
    result.update(state='failed',error_type=type(error).__name__)
    raise
finally:
    result['temporary_oauth_revoked']=c.close()
    REPORT.parent.mkdir(parents=True,exist_ok=True)
    with REPORT.open('x') as out:json.dump(result,out,indent=2)
    print(json.dumps(result),flush=True)

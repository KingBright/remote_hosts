#!/usr/bin/env python3
"""Scoped live acceptance/staging. Uses existing owner authorization locally.
No signed URLs, access tokens or passwords are persisted in evidence.
"""
import argparse
import base64
import contextlib
import hashlib
import http.cookiejar
import json
import pathlib
import secrets
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request

MACBOOK = 'ba3bf113-2390-466e-88bc-40d5b4f02884'
STUDIO = '8af88e35-f316-4dd9-9810-fa5bd3a22196'
WORKSPACES = {
 MACBOOK: MACBOOK + ':1d51494e-0c96-4a2e-92e6-1ee24ca33bb2',
 STUDIO: STUDIO + ':70854d05-8758-40ba-9aae-e71e10299d5b',
}
ORIGIN = 'https://mcp.hackerlife.fun'

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

class Client:
    def __init__(self):
        self.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.opener.addheaders = [('User-Agent', 'RemoteHosts-Acceptance/0.3.1')]
        self.access = None
        self.refresh = None
        self.sequence = 0
    def call(self, path, data=None, form=False, auth=False):
        headers = {'Accept':'application/json, text/event-stream'}
        if data is not None:
            headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
            data = (urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
        if auth:
            headers['Authorization'] = 'Bearer ' + self.access
        req = urllib.request.Request(ORIGIN+path, data=data, headers=headers)
        try:
            response = self.opener.open(req, timeout=30)
        except urllib.error.HTTPError as response:
            return response.code, response.headers, response.read(1048576)
        with response:
            return response.status, response.headers, response.read(1048576)
    def parsed(self, path, data=None, form=False, auth=False):
        status, _, body = self.call(path,data,form,auth)
        if status not in (200,201):
            raise RuntimeError('HTTP %s at %s; body suppressed' % (status,path.split('?')[0]))
        return json.loads(body)
    def login(self):
        metadata = self.parsed('/.well-known/oauth-protected-resource')
        assert metadata['resource']==ORIGIN+'/mcp'
        redirect='https://chatgpt.com/connector_platform_oauth_redirect'
        client=self.parsed('/oauth/register',{'redirect_uris':[redirect],'token_endpoint_auth_method':'none'})
        verifier=secrets.token_urlsafe(48)
        challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
        state='acceptance031-'+secrets.token_hex(8)
        query=urllib.parse.urlencode({'client_id':client['client_id'],'redirect_uri':redirect,'response_type':'code','code_challenge':challenge,'code_challenge_method':'S256','state':state,'resource':metadata['resource']})
        status,headers,_=self.call('/oauth/authorize?'+query)
        assert status==200
        nonce=headers['Set-Cookie'].split(';',1)[0].split('=',1)[1]
        password=pathlib.Path.home()/'.local/share/remote-hosts-code/setup/gateway-login-password.txt'
        status,headers,_=self.call('/oauth/approve',{'nonce':nonce,'password':password.read_text().strip()},form=True)
        assert status==303
        returned=urllib.parse.parse_qs(urllib.parse.urlsplit(headers['Location']).query)
        assert returned['iss']==[ORIGIN] and returned['state']==[state]
        tokens=self.parsed('/oauth/token',{'grant_type':'authorization_code','client_id':client['client_id'],'code':returned['code'][0],'code_verifier':verifier,'redirect_uri':redirect,'resource':metadata['resource']},form=True)
        self.access=tokens['access_token'];self.refresh=tokens['refresh_token']
        self.rpc('initialize',{'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'release031-live','version':'1'}})
        return self
    def rpc(self,method,params):
        self.sequence+=1
        result=self.parsed('/mcp',{'jsonrpc':'2.0','id':self.sequence,'method':method,'params':params},auth=True)
        if 'error' in result: raise RuntimeError('MCP protocol error '+str(result['error'].get('code')))
        return result['result']
    def tool(self,name,args):
        result=self.rpc('tools/call',{'name':name,'arguments':args})
        value=result.get('structuredContent')
        if value is None:
            if result.get('isError'): raise RuntimeError(name+': tool error; response suppressed')
            value=json.loads(result['content'][0]['text'])
        deadline=time.monotonic()+600
        last_phase=None
        while value.get('pending'):
            if time.monotonic()>deadline:raise RuntimeError('observation timeout; original operation '+value['operation_id'])
            phase=value.get('progress',{}).get('phase')
            if phase!=last_phase:
                print(json.dumps({'tool':name,'operation_id':value['operation_id'],'phase':phase,'bytes_done':value.get('progress',{}).get('bytes_done')}),flush=True)
                last_phase=phase
            time.sleep(1)
            response=self.rpc('tools/call',{'name':'operation_get','arguments':{'operation_id':value['operation_id']}})
            value=response.get('structuredContent') or {'error':'observation_error'}
        if 'error' in value:raise RuntimeError(name+': '+str(value.get('message',value['error']))[:500])
        return value
    def close(self):
        if self.refresh:
            status,_,_=self.call('/oauth/revoke',{'token':self.refresh},form=True)
            self.refresh=None
            return status in (200,204)
        return True

def file_hash(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1048576),b''):h.update(data)
    return h.hexdigest()

def stage(c,root):
    release=root/'dist/remote-hosts-code-0.3.1'
    names=('remote-hosts-code-macos-arm64','upgrade-code-agent.py','agent_upgrade_support.py','launch-code-upgrade.py','manifest.json')
    manifest=json.loads((release/'manifest.json').read_text())
    for name in names:
        if name!='manifest.json':assert file_hash(release/name)==manifest['artifacts'][name]['sha256']
    archive=root/'target/iteration-031/studio-package.tgz'
    if not archive.exists():
        with tarfile.open(archive,'x:gz') as t:
            for name in names:t.add(release/name,arcname=name)
    sha=file_hash(archive)
    exported=c.tool('file_download',{'workspace_id':WORKSPACES[MACBOOK],'path':str(archive.relative_to(root)),'expected_version':sha,'idempotency_key':'release031-studio-stage-export-e1'})
    imported=c.tool('file_upload',{'workspace_id':WORKSPACES[STUDIO],'path':'.remote-hosts-release-staging-031/studio-package.tgz','expected_version':'absent','sha256':sha,'idempotency_key':'release031-studio-stage-import-e1','file':{'file_id':'artifact-'+exported['artifact_id'],'download_url':exported['download_url'],'file_name':'studio-package.tgz','mime_type':'application/gzip'}})
    assert imported['sha256']==sha and imported['size']==archive.stat().st_size
    return {'state':'passed','scope':'owner-authorized gateway artifact copy to selected Mac-Studio workspace','sha256':sha,'size':imported['size'],'destination':imported['path'],'export_operation':exported['operation_id'],'import_operation':imported['operation_id']}

def features(c,root,selected):
    health=c.parsed('/healthz')
    assert health['version']=='0.3.1' and health['readiness_protocol']==1
    tools={t['name']:t for t in c.rpc('tools/list',{})['tools']}
    assert tools['terminal_exec']['inputSchema']['properties']['wait_ms']['maximum']==2000
    assert 'allow_partial' in tools['code_read']['inputSchema']['properties']
    report={'state':'passed','version':'0.3.1','gateway':health,'tool_count':len(tools),'devices':[]}
    devices={d['device_id']:d for d in c.tool('devices_list',{})['devices']}
    for device in selected:
        assert devices[device]['online'] and devices[device]['capabilities']['version']=='0.3.1'
        ws=WORKSPACES[device]; prefix='.remote-hosts-acceptance-031-features-'+device[:8]
        text='你🧪'*11000+'\nend\n'
        changes=c.tool('code_apply_edits',{'workspace_id':ws,'idempotency_key':'release031-features-create-e1','files':[{'path':prefix+'/long.txt','action':'create','expected_version':'absent','content':text},{'path':prefix+'/short.txt','action':'create','expected_version':'absent','content':'short line\n'}]})
        versions={p['path']:p['version'] for p in changes['changed']}
        request={'path':prefix+'/long.txt','start_line':1,'end_line':2}; joined=''; pages=0
        while True:
            out=c.tool('code_read',{'workspace_id':ws,'requests':[request],'max_bytes':32768})['ranges'][0]
            assert out['text'];joined+=out['text'];pages+=1
            if not out['truncated']:break
            assert pages<10
            request.update(start_line=out['next_line'],line_byte_offset=out['next_line_byte_offset'],expected_version=out['version'])
        assert joined==text
        out=c.tool('code_read',{'workspace_id':ws,'requests':[{'path':prefix+'/short.txt','start_line':1,'end_line':1}]*20})
        assert out['read_stats']['physical_reads']==1 and out['read_stats']['cache_hits']==19
        partial=c.tool('code_read',{'workspace_id':ws,'allow_partial':True,'requests':[{'path':prefix+'/missing','start_line':1,'end_line':1},{'path':prefix+'/short.txt','start_line':1,'end_line':1}]})
        assert partial['ranges'][0]['error']['code']=='read_failed' and partial['ranges'][1]['text']=='short line\n'
        term_args={'workspace_id':ws,'idempotency_key':'release031-features-shortcmd-e1','command':'printf release031-live','wait_ms':2000,'timeout_seconds':10}
        term=c.tool('terminal_exec',term_args)
        assert term['output']=='release031-live' and term['terminal']['exit_code']==0 and term['terminal']['output_complete']
        duplicate=c.tool('terminal_exec',term_args)
        assert duplicate==term
        c.tool('code_apply_edits',{'workspace_id':ws,'idempotency_key':'release031-features-cleanup-e1','files':[{'path':path,'action':'delete','expected_version':sha} for path,sha in versions.items()]})
        report['devices'].append({'device_id':device,'name':devices[device]['name'],'agent_version':'0.3.1','utf8_bytes':len(text.encode()),'fragment_pages':pages,'long_line_exact_reconstruction':True,'physical_reads_for_20_ranges':1,'cache_hits':19,'partial_batch':True,'short_command_single_result':True,'short_command_replay_equal':True,'test_files_removed':True,'terminal_operation':term['operation_id']})
        print(json.dumps(report['devices'][-1],ensure_ascii=False),flush=True)
    return report

def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=('stage','features'));p.add_argument('--report',required=True,type=pathlib.Path);p.add_argument('--device-id',action='append',choices=(MACBOOK,STUDIO));args=p.parse_args()
    assert not args.report.exists(),'use a new evidence path'
    client=Client();result={'state':'not_started','mode':args.mode}
    try:
        client.login()
        result=stage(client,pathlib.Path.cwd()) if args.mode=='stage' else features(client,pathlib.Path.cwd(),args.device_id or (MACBOOK,STUDIO))
    except Exception as e:
        result={'state':'failed','mode':args.mode,'error_type':type(e).__name__,'message':str(e)[:500]}
    finally:
        result['oauth_grant_revoked']=client.close()
        args.report.parent.mkdir(parents=True,exist_ok=True)
        with args.report.open('x') as f:json.dump(result,f,ensure_ascii=False,indent=2)
    print(json.dumps(result,ensure_ascii=False),flush=True)
    if result['state']!='passed' or not result['oauth_grant_revoked']:raise SystemExit(1)
if __name__=='__main__':main()

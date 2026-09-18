"""Authenticated owner MCP client for explicit release checks; no secrets in public receipts."""
import base64
import hashlib
import http.cookiejar
import json
import pathlib
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class Client:
    def __init__(self,origin,password_file=None,access=None):
        u=urllib.parse.urlsplit(origin)
        if u.scheme!='https' or not u.hostname or u.username or u.query or u.fragment or u.path:
            raise ValueError('explicit HTTPS origin without path/userinfo required')
        self.origin=origin;self.password_file=password_file;self.access=access;self.refresh=None;self.sequence=0
        self.opener=urllib.request.build_opener(NoRedirect(),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.opener.addheaders=[('User-Agent','RemoteHosts-Release/0.5.0')]
    def call(self,path,data=None,form=False,auth=False):
        headers={'Accept':'application/json, text/event-stream'}
        if data is not None:
            headers['Content-Type']='application/x-www-form-urlencoded' if form else 'application/json'
            data=(urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
        if auth:headers['Authorization']='Bearer '+self.access
        request=urllib.request.Request(self.origin+path,data=data,headers=headers)
        try:response=self.opener.open(request,timeout=30)
        except urllib.error.HTTPError as error:response=error
        with response:
            body=response.read(1048577)
            if len(body)>1048576:raise RuntimeError('release_response_budget')
            return response.status,response.headers,body
    def parsed(self,path,data=None,form=False,auth=False):
        status,_,body=self.call(path,data,form,auth)
        if status not in (200,201):raise RuntimeError('release_http_'+str(status)+'; body suppressed')
        return json.loads(body)
    def login(self):
        metadata=self.parsed('/.well-known/oauth-protected-resource')
        if metadata['resource']!=self.origin+'/mcp':raise ValueError('unexpected OAuth resource')
        redirect='https://chatgpt.com/connector_platform_oauth_redirect'
        client=self.parsed('/oauth/register',{'redirect_uris':[redirect],'token_endpoint_auth_method':'none'})
        verifier=secrets.token_urlsafe(48);challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=');state=secrets.token_hex(24)
        query=urllib.parse.urlencode({'client_id':client['client_id'],'redirect_uri':redirect,'response_type':'code','code_challenge':challenge,'code_challenge_method':'S256','state':state,'resource':metadata['resource']})
        status,headers,_=self.call('/oauth/authorize?'+query)
        if status!=200:raise RuntimeError('authorization_page_failed')
        nonce=headers['Set-Cookie'].split(';',1)[0].split('=',1)[1]
        status,headers,_=self.call('/oauth/approve',{'nonce':nonce,'password':pathlib.Path(self.password_file).read_text().strip()},form=True)
        if status!=303:raise RuntimeError('owner_authorization_failed')
        returned=urllib.parse.parse_qs(urllib.parse.urlsplit(headers['Location']).query)
        if returned.get('iss')!=[self.origin] or returned.get('state')!=[state]:raise RuntimeError('authorization_identity_conflict')
        tokens=self.parsed('/oauth/token',{'grant_type':'authorization_code','client_id':client['client_id'],'code':returned['code'][0],'code_verifier':verifier,'redirect_uri':redirect,'resource':metadata['resource']},form=True)
        self.access=tokens['access_token'];self.refresh=tokens['refresh_token']
        self.rpc('initialize',{'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'remote-hosts-release','version':'0.5.0'}});return self
    def rpc(self,method,params):
        self.sequence+=1
        payload={'jsonrpc':'2.0','id':self.sequence,'method':method,'params':params}
        # Only observations may be retried automatically. A lost mutation response
        # is not proof that execution did not start; retain the original key/handle.
        readonly = method in ('initialize', 'tools/list') or (method == 'tools/call' and params.get('name') in ('devices_list', 'fleet_status', 'operation_get', 'task_context'))
        attempts = 5 if readonly else 1
        for attempt in range(attempts):
            try:
                value=self.parsed('/mcp',payload,auth=True)
                break
            except (TimeoutError, urllib.error.URLError, OSError):
                if attempt + 1 == attempts: raise
                time.sleep(min(2**attempt,8))
        if 'error' in value:raise RuntimeError('MCP protocol error '+str(value['error'].get('code')))
        return value['result']
    def raw(self,name,args):
        # Machine clients consume the stable full contract, never the model's
        # presentation view. Copy inputs so the caller's request identity is intact.
        arguments=dict(args);arguments.setdefault('response_mode','full')
        response=self.rpc('tools/call',{'name':name,'arguments':arguments});value=response.get('structuredContent')
        if value is None:
            if response.get('isError'):raise RuntimeError('tool_error:'+name+'; inspect original bounded response')
            value=json.loads(response['content'][0]['text'])
        return value
    def tool(self,name,args):
        value=self.raw(name,args);end=time.monotonic()+900
        while value.get('pending'):
            if time.monotonic()>end:raise RuntimeError('observation_timeout:'+value['operation_id'])
            time.sleep(.5);value=self.raw('operation_get',{'operation_id':value['operation_id']})
        if 'error' in value:raise RuntimeError(name+':'+str(value.get('error_code',value['error'])))
        return value
    def terminal(self,ws,command,key,timeout=60):
        value=self.tool('terminal_exec',{'workspace_id':ws,'command':command,'idempotency_key':key,'timeout_seconds':timeout,'wait_ms':1000})
        ident=value['terminal_id'];text=value.get('output','');cursor=value.get('cursor',0);status=value.get('terminal',{});more=value.get('has_more',False);end=time.monotonic()+timeout+60
        while more or not(status.get('exit_code') is not None and status.get('output_complete')):
            if time.monotonic()>end:raise RuntimeError('terminal_observation_timeout:'+ident)
            page=self.tool('terminal_read',{'workspace_id':ws,'terminal_id':ident,'cursor':cursor,'max_bytes':65536,'output_mode':'full'});text+=page.get('output','');cursor=page['cursor'];status=page['terminal'];more=page['has_more'];time.sleep(.2)
        if status.get('output_truncated') or status.get('output_error'):
            raise RuntimeError('terminal_evidence_incomplete:'+ident)
        if status['exit_code']!=0:raise RuntimeError('terminal_failed:'+ident)
        return text
    def close(self):
        if self.refresh:
            status,_,_=self.call('/oauth/revoke',{'token':self.refresh},form=True);self.refresh=None;return status in (200,204)
        return True

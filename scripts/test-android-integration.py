#!/usr/bin/env python3
"""Real Rust Gateway + TLS + MCP + Android emulator acceptance. Never touches the live gateway.

Requires the separate debug APK and its test APK built by build-android.py. The shipped
release APK has neither fixture Activity, instrumentation, nor test CA configuration.
"""
import argparse, hashlib, http.cookiejar, json, os, pathlib, re, shlex, socket, ssl, subprocess, sys, time, urllib.request, uuid
from release_client import Client, NoRedirect
ROOT=pathlib.Path(__file__).resolve().parents[1]
PACKAGE='io.remotehosts.agent.debug'

def main():
    p=argparse.ArgumentParser();p.add_argument('--serial',required=True);p.add_argument('--gateway-binary',required=True,type=pathlib.Path)
    p.add_argument('--adb',default=str(pathlib.Path.home()/'Library/Android/sdk/platform-tools/adb'));a=p.parse_args()
    if not re.fullmatch(r'emulator-\d+',a.serial):raise SystemExit('dedicated Android emulator required; physical phones are never test fixtures')
    adb=[a.adb,'-s',a.serial]
    def run(args,**kwargs):return subprocess.run(args,check=True,capture_output=True,timeout=120,**kwargs)
    def shell(*args):return run(adb+['shell',*args],text=True).stdout.strip()
    if shell('getprop','ro.kernel.qemu')!='1':raise SystemExit('not an emulator')
    runid=str(uuid.uuid4()); base=ROOT/'android/build/integration'/runid;base.mkdir(parents=True,mode=0o700)
    checks=[]; children=[];handles=[];client=None;instrument=None;reverse_ready=False
    def checked(name,truth,**facts):
        if not truth:raise AssertionError(name)
        checks.append(dict(name=name,passed=True,**facts));print('PASS '+name,flush=True)
    def spawn(args,filename):
        h=(base/filename).open('wb');handles.append(h);proc=subprocess.Popen(args,stdout=h,stderr=subprocess.STDOUT,start_new_session=True);children.append(proc);return proc
    def freeport():
        with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
    tlsport,innerport=freeport(),freeport();origin=f'https://localhost:{tlsport}'
    gateway=a.gateway_binary.resolve(strict=True);cfg=base/'gateway.json';password=base/'owner-password';agent=base/'android-device.json'
    report={'run_id':runid,'environment':'isolated Rust gateway and dedicated Android emulator','checks':checks,'release_apk_tested_as_install_target':False,'physical_phone_tested':False,'passed':False}
    try:
        version=run([str(gateway),'--version'],text=True).stdout.strip();report['gateway_version']=version
        run([str(gateway),'init-gateway','--config',str(cfg),'--public-url',origin,'--bind',f'127.0.0.1:{innerport}','--state-dir',str(base/'gateway-state'),'--password-file',str(password),'--owner','android-fixture'])
        run([str(gateway),'enroll','--gateway-config',str(cfg),'--agent-config',str(agent),'--name','Android Integration Emulator','--state-dir','/android/private','--root','/android/shared','--allow-write','--allow-exec','--shell','/system/bin/sh'])
        tls=ROOT/'android/build/integration-tls';cert=tls/'fixture_ca.pem';key=tls/'fixture.key'
        spawn([str(gateway),'gateway','--config',str(cfg)],'gateway.log')
        spawn([sys.executable,str(ROOT/'scripts/android-fixture-tls-proxy.py'),'--listen-port',str(tlsport),
               '--upstream-port',str(innerport),'--cert',str(cert),'--key',str(key)],'tls.log')
        context=ssl.create_default_context(cafile=str(cert))
        for _ in range(100):
            try:
                with urllib.request.urlopen(origin+'/healthz',context=context,timeout=2) as r:health=json.load(r)
                break
            except Exception:time.sleep(.2)
        else:raise RuntimeError('isolated gateway did not start; inspect fixture logs')
        checked('real_gateway_wire_protocol',health.get('wire_protocol')==2)
        run(adb+['reverse',f'tcp:{tlsport}',f'tcp:{tlsport}']);reverse_ready=True
        run(adb+['install','-r',str(ROOT/'android/app/build/outputs/apk/debug/app-debug.apk')])
        run(adb+['install','-r','-t',str(ROOT/'android/app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk')])
        result=run(adb+['shell','am','instrument','-w',PACKAGE+'.test/io.remotehosts.agent.AgentInstrumentation'],text=True).stdout
        (base/'runtime-checks.txt').write_text(result)
        checked('android_keystore_sqlite_runtime_checks','8 runtime checks passed' in result)
        run(adb+['shell','pm','grant',PACKAGE,'android.permission.POST_NOTIFICATIONS'])
        previous=shell('settings','get','secure','enabled_accessibility_services')
        services=[] if previous in ('','null') else previous.split(':')
        component=PACKAGE+'/io.remotehosts.agent.AccessService'
        services=[item for item in services if item!=component]
        # Instrumentation restarts the target process. Enable this fixture's
        # accessibility service AFTER that restart, preserving unrelated services.
        if services:shell('settings','put','secure','enabled_accessibility_services',':'.join(services))
        else:shell('settings','delete','secure','enabled_accessibility_services')
        data=json.loads(agent.read_text());device=data['device_id']
        run(adb+['shell','run-as',PACKAGE,'mkdir','-p','files'])
        run(adb+['shell','run-as',PACKAGE,'sh','-c',shlex.quote('cat > files/integration-config.json')],input=agent.read_bytes())
        instrument=spawn(adb+['shell','am','instrument','-w','-e','mode','fixture',PACKAGE+'.test/io.remotehosts.agent.AgentInstrumentation'],'fixture-instrumentation.log')
        client=Client(origin,password_file=password,transport='legacy')
        client.opener=urllib.request.build_opener(NoRedirect(),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),urllib.request.HTTPSHandler(context=context),urllib.request.ProxyHandler({}))
        client.login()
        selected=None
        for _ in range(90):
            rows=client.tool('devices_list',{}).get('devices',[])
            selected=next((d for d in rows if d['device_id']==device and d.get('online')),None)
            if selected:break
            if instrument.poll() is not None:raise RuntimeError('fixture instrumentation exited; inspect its log')
            time.sleep(1)
        checked('android_registered_online_via_tls',selected is not None)
        root=selected['capabilities']['roots'][0]
        ws=client.tool('workspace_open',{'device_id':device,'root':root,'idempotency_key':runid+'-workspace'})['workspace']['id']
        counter=0
        def cmd(name,args=None,key=None):
            nonlocal counter;counter+=1
            arguments={'workspace_id':ws,'command':'android '+name+((' '+json.dumps(args,ensure_ascii=False)) if args is not None else ''),'idempotency_key':key or runid+'-'+str(counter),'timeout_seconds':240}
            value=client.tool('terminal_exec',arguments)
            parsed=json.loads(value['output'])
            (base/f'{counter:03}-{name}.json').write_text(json.dumps(parsed,indent=2,ensure_ascii=False)+'\n')
            return parsed,value
        shell('settings','put','secure','enabled_accessibility_services',':'.join(services+[component]))
        shell('settings','put','secure','accessibility_enabled','1')
        for _ in range(30):
            status,_=cmd('status')
            if status.get('accessibility'):break
            time.sleep(.5)
        checked('accessibility_capability_live',status['accessibility'] is True and status['shell_connected'] is False)
        missing,_=cmd('shell',{'command':'id'});checked('shell_denied_before_local_activation',missing.get('error')=='shell_activation_required')
        launched=shell('am','start','-W','-n',PACKAGE+'/io.remotehosts.agent.ProbeActivity')
        (base/'probe-launch.txt').write_text(launched)
        node=None;system_waits=0
        for _ in range(20):
            obs,_=cmd('observe')
            node=next((n for n in obs.get('nodes',[]) if n.get('description')=='fixture-input'),None)
            if node is not None and obs.get('package')==PACKAGE:break
            # The dedicated emulator may show its own System UI ANR after a
            # heavy cold build. Observe and press only its explicit Wait button,
            # through the real agent, rather than dismissing arbitrary dialogs.
            if obs.get('package')=='android' and any(n.get('text')=="System UI isn't responding" for n in obs.get('nodes',[])) and system_waits<3:
                wait=next((n for n in obs['nodes'] if n.get('resource_id')=='android:id/aerr_wait'),None)
                if wait:
                    outcome,_=cmd('click',{'observation_id':obs['observation_id'],'node_id':wait['id']})
                    demand_wait=outcome.get('accepted') is True
                    if not demand_wait:raise AssertionError('observed_system_dialog_wait_rejected')
                    system_waits+=1;report['emulator_system_ui_wait_dialogs']=system_waits
            time.sleep(.5)
        (base/'observed-screen.json').write_text(json.dumps(obs,indent=2,ensure_ascii=False)+'\n')
        checked('fixture_screen_in_foreground',node is not None and obs.get('package')==PACKAGE)
        checked('structured_screen_tree',len(obs.get('nodes',[]))>=4)
        checked('password_nodes_redacted',any(n.get('password') is True and n.get('text')=='<redacted>' for n in obs['nodes']) and 'fixture-sensitive-value' not in json.dumps(obs))
        for attempt in range(5):
            value,_=cmd('text',{'observation_id':obs['observation_id'],'node_id':node['id'],'text':'你好 Remote Hosts'})
            if value.get('accepted') is True:break
            if value.get('error') not in ('node_changed_observe_again','screen_changed_observe_again'):break
            # These are explicit pre-action refusals, not uncertain executions.
            # Acquire a NEW observation and operation; never replay a lost result.
            time.sleep(.3);obs,_=cmd('observe')
            node=next(n for n in obs['nodes'] if n.get('description')=='fixture-input')
        checked('unicode_text_action_accepted',value.get('accepted') is True)
        stale,_=cmd('click',{'observation_id':obs['observation_id'],'node_id':node['id']})
        checked('stale_observation_rejected',stale.get('error') in ('observation_required','stale_observation','node_changed_observe_again'))
        for attempt in range(5):
            obs,_=cmd('observe');button=next(n for n in obs['nodes'] if (n.get('text') or '').casefold()=='apply fixture text' and n.get('class')=='android.widget.Button')
            value,_=cmd('click',{'observation_id':obs['observation_id'],'node_id':button['id']})
            if value.get('accepted') is True:break
            if value.get('error') not in ('node_changed_observe_again','screen_changed_observe_again'):break
            time.sleep(.3)
        if value.get('accepted') is not True:raise AssertionError('fixture_button_click_rejected: '+json.dumps(value))
        for _ in range(10):
            obs,_=cmd('observe')
            if any(n.get('text')=='已确认：你好 Remote Hosts' for n in obs.get('nodes',[])):break
            time.sleep(.2)
        checked('click_verified_by_changed_screen',any(n.get('text')=='已确认：你好 Remote Hosts' for n in obs.get('nodes',[])))
        shot,_=cmd('screenshot');checked('screenshot_created',shot.get('size',0)>0 and shot.get('secure_windows_bypassed') is False)
        artifact=client.tool('file_download',{'workspace_id':ws,'path':shot['path'],'idempotency_key':runid+'-screenshot-download'})
        png=client.read_artifact(artifact);(base/'probe.png').write_bytes(png)
        checked('binary_screenshot_download_verified',png.startswith(b'\x89PNG\r\n\x1a\n') and hashlib.sha256(png).hexdigest()==artifact['sha256'])
        import io
        from PIL import Image
        def content_extrema(raw):
            with Image.open(io.BytesIO(raw)) as image:
                image.load();width,height=image.size
                if width<100 or height<100:raise AssertionError('invalid screenshot dimensions')
                box=(width//10,height*15//100,width*9//10,height*85//100)
                return image.size,box,image.convert('RGB').crop(box).getextrema()
        normal_size,normal_box,normal_extrema=content_extrema(png)
        checked('ordinary_screen_has_visible_pixels',max(high for low,high in normal_extrema)>32)
        try:
            imported=client.tool('file_upload',{'workspace_id':ws,'path':'roundtrip/probe.png','file':{'file_id':'fixture-'+runid,'download_url':artifact['download_url']},'sha256':artifact['sha256'],'idempotency_key':runid+'-upload'})
            checked('binary_file_upload_verified',imported.get('sha256')==artifact['sha256'] and imported.get('atomic') is True)
            roundtrip=client.tool('file_download',{'workspace_id':ws,'path':'roundtrip/probe.png','idempotency_key':runid+'-roundtrip-download'})
            checked('file_roundtrip_identical',client.read_artifact(roundtrip)==png)
            conflict=False
            try:
                client.tool('file_upload',{'workspace_id':ws,'path':'roundtrip/probe.png','file':{'file_id':'fixture-'+runid,'download_url':artifact['download_url']},'sha256':artifact['sha256'],'idempotency_key':runid+'-conflicting-upload'})
            except RuntimeError as exc:
                conflict='version_conflict' in str(exc)
            checked('upload_refuses_unapproved_overwrite',conflict)
            preserved=client.tool('file_download',{'workspace_id':ws,'path':'roundtrip/probe.png','idempotency_key':runid+'-after-conflict'})
            checked('failed_upload_preserved_original',preserved['sha256']==artifact['sha256'] and client.read_artifact(preserved)==png)
        except Exception as exc:
            report.setdefault('failed_checks',[]).append({'name':'file_upload_roundtrip','error':str(exc)})
            print('FAIL file_upload_roundtrip: '+str(exc),flush=True)
            # Keep testing the independent shell/install lane, but never mark
            # this run passed or publish it while any mandatory check failed.
        helper_log='/data/local/tmp/rh-helper-'+runid+'.log'
        run(['python3',str(ROOT/'scripts/android-activate-shell.py'),'--adb',a.adb,'--serial',a.serial,'--package',PACKAGE,'--diagnostic-log',helper_log])
        for _ in range(20):
            status,_=cmd('status')
            if status.get('shell_connected'):break
            time.sleep(.3)
        helper_diagnostic=shell('cat',helper_log)
        (base/'helper-startup.txt').write_text(helper_diagnostic)
        checked('builtin_shell_bridge_activated',status.get('shell_connected') is True and status.get('shell_uid')==2000)
        identity,_=cmd('shell',{'command':'id'});checked('real_adb_shell_uid',identity.get('exit_code')==0 and 'uid=2000' in identity.get('output',''))
        source='/data/local/tmp/rh-counter-'+runid;repeatkey=runid+'-repeat'
        first,firstmeta=cmd('shell',{'command':'echo once >> '+source},key=repeatkey)
        second,secondmeta=cmd('shell',{'command':'echo once >> '+source},key=repeatkey)
        counted,_=cmd('shell',{'command':'wc -l < '+source})
        checked('mutating_idempotency_verified',firstmeta['terminal_id']==secondmeta['terminal_id'] and counted.get('output','').strip()=='1')
        pulled,_=cmd('pull',{'source':source,'path':'fixtures/counter.txt'})
        checked('shell_file_pull',pulled.get('exit_code')==0 and pulled.get('size')==5)
        text=client.tool('code_read',{'workspace_id':ws,'requests':[{'path':'fixtures/counter.txt','start_line':1,'end_line':3}]})
        checked('shared_file_text_read',text['ranges'][0]['text'].strip()=='once')
        apk=ROOT/'android/app/build/outputs/apk/release/app-release.apk';apkhash=hashlib.sha256(apk.read_bytes()).hexdigest()
        remote_apk=root+'/fixtures/release.apk'
        run(adb+['shell','run-as',PACKAGE,'sh','-c',shlex.quote('cat > '+shlex.quote(remote_apk))],input=apk.read_bytes())
        installed,_=cmd('install',{'path':'fixtures/release.apk','sha256':apkhash})
        checked('release_apk_installed_by_shell_bridge',installed.get('installed') is True and installed.get('package')=='io.remotehosts.agent')
        installed_path=next(line.removeprefix('package:') for line in shell('pm','path','io.remotehosts.agent').splitlines() if line.endswith('/base.apk'))
        installed_hash,_=cmd('shell',{'command':'sha256sum '+shlex.quote(installed_path)})
        checked('installed_release_apk_bytes_match',installed_hash.get('exit_code')==0 and installed_hash.get('output','').split()[0]==apkhash)
        report['release_apk_tested_as_install_target']=True;report['release_apk_sha256']=apkhash
        launched,_=cmd('launch',{'package':'io.remotehosts.agent'})
        checked('release_launch_requested_through_gateway',launched.get('accepted') is True)
        time.sleep(1)
        running=shell('pidof','io.remotehosts.agent');checked('signed_release_launch_smoke',bool(running))
        for _ in range(10):
            result,_=cmd('screenshot')
            if str(result.get('error','')).startswith('screenshot_denied') or result.get('package')=='io.remotehosts.agent':break
            time.sleep(.3)
        if str(result.get('error','')).startswith('screenshot_denied'):
            checked('secure_configuration_window_not_captured',True,protection='explicit_platform_denial')
        else:
            # Whole-display capture may succeed while the secure window is
            # replaced by black pixels. Verify the actual returned bytes, not
            # just an API return code or the agent's own protection claim.
            checked('signed_release_is_foreground',result.get('package')=='io.remotehosts.agent')
            secure_artifact=client.tool('file_download',{'workspace_id':ws,'path':result['path'],'idempotency_key':runid+'-secure-screenshot-download'})
            secure_png=client.read_artifact(secure_artifact)
            checked('secure_capture_checksum_verified',hashlib.sha256(secure_png).hexdigest()==secure_artifact['sha256'])
            (base/'secure-window.png').write_bytes(secure_png)
            size,box,extrema=content_extrema(secure_png)
            checked('secure_configuration_window_not_captured',size==normal_size and all(high<=3 for low,high in extrema),
                    protection='platform_redacted_pixels',content_box=box,channel_extrema=extrema)
        run(adb+['shell','run-as',PACKAGE,'touch','files/integration-stop']);instrument.wait(timeout=30)
        time.sleep(.5)
        services=shell('dumpsys','activity','services',PACKAGE)
        checked('local_stop_removed_foreground_service','ServiceRecord' not in services or 'io.remotehosts.agent.AgentService' not in services)
        report['passed']=not bool(report.get('failed_checks'))
    finally:
        if client:
            try:client.close()
            except Exception:pass
        if instrument and instrument.poll() is None:
            try:run(adb+['shell','run-as',PACKAGE,'touch','files/integration-stop']);instrument.wait(timeout=10)
            except Exception:pass
        for proc in reversed(children):
            if proc.poll() is None:
                proc.terminate()
                try:proc.wait(timeout=10)
                except subprocess.TimeoutExpired:proc.kill();proc.wait()
        for h in handles:h.close()
        if reverse_ready:
            try:subprocess.run(adb+['reverse','--remove',f'tcp:{tlsport}'],capture_output=True,timeout=15)
            except subprocess.TimeoutExpired:pass
        report['finished_at']=int(time.time());(base/'acceptance.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
        print('Acceptance receipt: '+str((base/'acceptance.json').relative_to(ROOT)),flush=True)
    print(json.dumps({'passed':report['passed'],'checks':len(checks),'failed_checks':report.get('failed_checks',[]),'receipt':str(base/'acceptance.json')}),flush=True)
    if not report['passed']:raise SystemExit(1)
if __name__=='__main__':main()

#!/usr/bin/env python3
"""Activate the APK's built-in helper using an explicitly selected, authorized ADB device."""
import argparse,re,shlex,subprocess
p=argparse.ArgumentParser(); p.add_argument('--serial',required=True); p.add_argument('--adb',default='adb'); p.add_argument('--package',default='io.remotehosts.agent'); p.add_argument('--diagnostic-log'); a=p.parse_args()
if a.diagnostic_log and not re.fullmatch(r'/data/local/tmp/rh-helper-[A-Za-z0-9-]+\.log',a.diagnostic_log):raise SystemExit('invalid helper diagnostic path')
if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_.]+',a.package): raise SystemExit('invalid package')
adb=[a.adb,'-s',a.serial]
row=subprocess.run(adb+['shell','content','query','--uri',f'content://{a.package}.bridge/status'],check=True,capture_output=True,text=True).stdout
socket=re.search(r'socket=(rh-\d+-[a-f0-9]{32})',row); uid=re.search(r'uid=(\d+)',row)
if not socket or not uid:raise SystemExit('Open the APK, start remote control and locally allow shell before activation.')
paths=subprocess.run(adb+['shell','pm','path',a.package],check=True,capture_output=True,text=True).stdout.splitlines()
path=next((x.removeprefix('package:') for x in paths if x.startswith('package:') and x.endswith('/base.apk')),None)
if not path:raise SystemExit('installed APK not found')
output=shlex.quote(a.diagnostic_log) if a.diagnostic_log else '/dev/null'
cmd=f'CLASSPATH={shlex.quote(path)} nohup app_process /system/bin io.remotehosts.agent.ShellMain {socket[1]} {uid[1]} </dev/null >{output} 2>&1 &'
subprocess.run(adb+['shell',cmd],check=True,capture_output=True,timeout=10)
print('Activation requested. Verify shell_connected and shell_uid with android status; this command does not grant or bypass ADB authorization.')

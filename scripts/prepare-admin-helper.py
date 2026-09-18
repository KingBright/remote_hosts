#!/usr/bin/env python3
"""Prepare a checksummed offline admin-helper bundle. Never installs or elevates."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import plistlib
import shutil
import struct
import uuid


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda: f.read(1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()


def binary_machine(path: Path, target: str) -> str:
    with path.open('rb') as f:
        header = f.read(64)
    if target == 'linux' and header[:4] == b'\x7fELF' and len(header) >= 20:
        order = '<' if header[5] == 1 else '>'
        machine = struct.unpack(order+'H', header[18:20])[0]
        return {62: 'x86_64', 183: 'aarch64'}[machine]
    if target == 'macos' and header[:4] == b'\xcf\xfa\xed\xfe':
        cpu = struct.unpack('<I', header[4:8])[0]
        return {0x0100000C: 'arm64', 0x01000007: 'x86_64'}[cpu]
    raise ValueError('Binary is not a supported executable for this platform')


def make_service(target: str) -> bytes:
    if target == 'macos':
        state = '/Library/Application Support/RemoteHostsAdmin'
        config = dict(Label='com.remotehosts.admin', RunAtLoad=True, KeepAlive=True,
                      UserName='root', GroupName='wheel', Umask=0o077,
                      ProgramArguments=['/Library/PrivilegedHelperTools/com.remotehosts.admin',
                                        'serve', '--policy', state+'/policy.json',
                                        '--state-dir', state+'/receipts'],
                      StandardOutPath=state+'/helper.stdout.log',
                      StandardErrorPath=state+'/helper.stderr.log',
                      ThrottleInterval=10, ExitTimeOut=130)
        # The runtime directory must be recreated after reboot; launchd does not offer RuntimeDirectory.
        # Use the persistent protected directory for this socket instead; it is not world-writable.
        config['ProgramArguments'] += ['--socket', state+'/helper.sock']
        return plistlib.dumps(config, sort_keys=True)
    return b'''[Unit]
Description=Restricted Remote Hosts administration helper
After=local-fs.target

[Service]
Type=simple
User=root
Group=root
ExecStart=/var/lib/remote-hosts-admin/bin/remote-hosts-admin serve --policy /var/lib/remote-hosts-admin/policy.json --state-dir /var/lib/remote-hosts-admin/receipts
Restart=on-failure
RestartSec=5
RuntimeDirectory=remote-hosts-admin
RuntimeDirectoryMode=0755
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ReadWritePaths=/etc/systemd/system /var/lib/remote-hosts-admin /run/remote-hosts-admin
ProtectHome=read-only
RestrictAddressFamilies=AF_UNIX
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
TimeoutStopSec=130

[Install]
WantedBy=multi-user.target
'''


UNINSTALL = '''#!/usr/bin/env python3
"""Remove only the helper; keep policy, receipts and quarantined legacy files for audit."""
import sys
if not sys.flags.isolated:raise SystemExit("Use python3 -I for this administrator script.")
import argparse, os, pathlib, platform, subprocess
p=argparse.ArgumentParser();p.add_argument('--apply',action='store_true');a=p.parse_args()
mac=platform.system()=='Darwin'
service=pathlib.Path('/Library/LaunchDaemons/com.remotehosts.admin.plist' if mac else '/etc/systemd/system/remote-hosts-admin.service')
binary=pathlib.Path('/Library/PrivilegedHelperTools/com.remotehosts.admin' if mac else '/var/lib/remote-hosts-admin/bin/remote-hosts-admin')
print('Remove helper only; keep receipts/backups; never restore old EasyTier automatically.')
if not a.apply: print(service,binary,sep='\\n');raise SystemExit(0)
if os.geteuid()!=0:raise SystemExit('Separate administrator authorization is required.')
env={'PATH':'/usr/bin:/bin:/usr/sbin:/sbin','LC_ALL':'C'}
command=['/bin/launchctl','bootout','system/com.remotehosts.admin'] if mac else ['/usr/bin/systemctl','disable','--now','remote-hosts-admin.service']
subprocess.run(command,check=True,env=env,stdin=subprocess.DEVNULL,timeout=150)
for path in (service,binary):
    if path.is_symlink():raise SystemExit('Refuse symlink: '+str(path))
    path.unlink()
if not mac:subprocess.run(['/usr/bin/systemctl','daemon-reload'],check=True,env=env,timeout=30)
'''


def prepare(binary: Path, output: Path, target: str, device_id: str, account: str,
            uid: int, gid: int, home: str) -> dict:
    if binary.is_symlink() or not binary.is_file():
        raise ValueError('Provide a built regular executable')
    device_id = str(uuid.UUID(device_id))
    if uid <= 0 or gid < 0 or not account or not home.startswith('/') or any(
            part in ('', '.', '..') for part in home.split('/')[1:]) or any(ord(c)<32 for c in home):
        raise ValueError('Invalid account grant')
    machine = binary_machine(binary, target)
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(binary, output/'remote-hosts-admin')
    (output/'remote-hosts-admin').chmod(0o755)
    policy = dict(protocol=1, device_id=device_id, grant_id=str(uuid.uuid4()),
                  allowed_uid=uid, allowed_gid=gid, home=home, platform=target, enabled=True)
    (output/'policy.json').write_text(json.dumps(policy, indent=2)+'\n')
    (output/'service.template').write_bytes(make_service(target))
    shutil.copyfile(Path(__file__).with_name('admin-helper-install.py'), output/'install.py')
    (output/'uninstall.py').write_text(UNINSTALL)
    names = ['remote-hosts-admin', 'policy.json', 'service.template', 'install.py', 'uninstall.py']
    manifest = dict(protocol=1, helper_version='0.1.0', status='prepared_not_installed',
                    platform=target, machine=machine, device_id=device_id, account=account,
                    files={n: sha(output/n) for n in names})
    (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    identity = sha(output/'manifest.json')
    (output/'MANIFEST.sha256').write_text(identity+'  manifest.json\n')
    (output/'INSTALL.txt').write_text(
        'Prepared candidate, NOT installed. No cleanup runs during installation.\n'
        'First verify this manifest digest through an independent trusted record:\n'+identity+'\n'
        'Preview without privileges: python3 -I install.py\n'
        'In a separately authorized administrator session:\n'
        'python3 -I install.py --apply --grant-legacy-cleanup --agent-config /path/to/existing/agent.json --accept-manifest-sha256 '+identity+'\n'
        'Do not transmit administrator passwords through Remote Hosts.\n'
        'After install, use the normal authorized user and the shared helper CLI.\n'
        'A live network alone does not create an administrator authorization.\n')
    return dict(output=str(output), manifest_sha256=identity, installed=False, **manifest)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--platform',choices=['macos','linux'],required=True);p.add_argument('--device-id',required=True)
    p.add_argument('--account',required=True);p.add_argument('--uid',type=int,required=True)
    p.add_argument('--gid',type=int,required=True);p.add_argument('--home',required=True)
    a=p.parse_args()
    print(json.dumps(prepare(a.binary,a.output,a.platform,a.device_id,a.account,a.uid,a.gid,a.home),indent=2))
if __name__=='__main__':main()

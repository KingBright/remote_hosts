#!/usr/bin/env python3
"""Fixed runner launched by the authenticated Gateway self-upgrade endpoint."""
import argparse,hashlib,json,pathlib,subprocess,tarfile,tempfile,os

def digest(path):
    h=hashlib.sha256()
    with pathlib.Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def main():
    p=argparse.ArgumentParser();p.add_argument('--bundle',type=pathlib.Path,required=True);p.add_argument('--bundle-sha256',required=True);p.add_argument('--version',required=True);p.add_argument('--result',type=pathlib.Path,required=True);p.add_argument('--binary-path',required=True);p.add_argument('--config-path',required=True);p.add_argument('--backup-root',required=True);p.add_argument('--service-name',default='remote-hosts-code-gateway.service');p.add_argument('--gateway-bind',default='127.0.0.1:18787');args=p.parse_args()
    if digest(args.bundle)!=args.bundle_sha256:raise SystemExit('bundle checksum mismatch')
    release=args.bundle.parent;manifest_path=release/'manifest.json'
    with tarfile.open(args.bundle) as archive:
        members=archive.getmembers()
        if any(not m.isfile() or pathlib.PurePosixPath(m.name).name!=m.name for m in members):raise SystemExit('bundle must contain flat regular files only')
        for member in members:
            target=release/member.name
            data=archive.extractfile(member).read()
            if target.exists() and target.read_bytes()!=data:raise SystemExit('existing staged file differs: '+member.name)
            if not target.exists():
                with tempfile.NamedTemporaryFile(dir=release,delete=False) as out:tmp=pathlib.Path(out.name);out.write(data);out.flush();os.fsync(out.fileno())
                os.replace(tmp,target)
    manifest=json.loads(manifest_path.read_text())
    if manifest.get('version')!=args.version:raise SystemExit('manifest version mismatch')
    for name,meta in manifest['artifacts'].items():
        path=release/name
        if not path.is_file() or digest(path)!=meta['sha256'] or path.stat().st_size!=meta['size']:raise SystemExit('artifact mismatch: '+name)
    binary=release/'remote-hosts-code-linux-amd64';updater=release/'upgrade-code-gateway.py'
    subprocess.run(['python3',str(updater),'--candidate',str(binary),'--sha256',manifest['artifacts'][binary.name]['sha256'],'--version',args.version,'--result',str(args.result),'--binary-path',args.binary_path,'--config-path',args.config_path,'--backup-root',args.backup_root,'--service-name',args.service_name,'--gateway-bind',args.gateway_bind],check=True,timeout=300)
if __name__=='__main__':main()

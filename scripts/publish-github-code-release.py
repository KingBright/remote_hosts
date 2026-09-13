#!/usr/bin/env python3
"""Publish one immutable, self-contained Remote Hosts Code GitHub release."""
import argparse,json,pathlib,re,subprocess

ROOT=pathlib.Path(__file__).resolve().parents[1]
def run(argv,check=True):
    r=subprocess.run(argv,cwd=ROOT,text=True,capture_output=True,timeout=300)
    if check and r.returncode: raise RuntimeError('github release command failed; stderr suppressed')
    return r

def main():
    p=argparse.ArgumentParser();p.add_argument('--version',required=True);p.add_argument('--package',required=True,type=pathlib.Path);p.add_argument('--bundle',required=True,type=pathlib.Path);p.add_argument('--target',default='main');args=p.parse_args()
    if not re.fullmatch(r'\d+\.\d+\.\d+',args.version):p.error('invalid version')
    package=args.package.resolve();bundle=args.bundle.resolve();manifest=json.loads((package/'manifest.json').read_text())
    if manifest.get('version')!=args.version or bundle.name!=f'remote-hosts-code-{args.version}-bundle.tgz':raise SystemExit('release identity mismatch')
    tag='v'+args.version
    existing=run(['gh','release','view',tag,'--json','tagName'],check=False)
    if existing.returncode==0:
        raise SystemExit('GitHub release already exists; immutable releases are never overwritten')
    status=run(['git','status','--porcelain']).stdout.strip()
    if status:raise SystemExit('working tree must be clean before publishing release')
    branch=run(['git','rev-parse','--abbrev-ref','HEAD']).stdout.strip()
    if branch!='main':raise SystemExit('release must be published from main')
    assets=[bundle,package/'manifest.json',package/'SHA256SUMS',package/'remote-hosts-code-macos-arm64',package/'remote-hosts-code-linux-amd64',package/'remote-hosts-code-windows-amd64.exe']
    notes=(f'Remote Hosts Code {args.version}.\n\nThe `*-bundle.tgz` asset is the immutable fleet release identity and contains all verified platform binaries and updater helpers.\n')
    run(['gh','release','create',tag,'--target',args.target,'--title',f'Remote Hosts {args.version}','--notes',notes,*map(str,assets)])
    print(json.dumps({'state':'published','version':args.version,'tag':tag,'bundle':bundle.name,'assets':[p.name for p in assets]}))
if __name__=='__main__':main()

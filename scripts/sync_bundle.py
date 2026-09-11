#!/usr/bin/env python3
"""Create a bounded RHSYNC1 bundle from a files_sync plan. No network or remote writes."""
import argparse
import hashlib
import json
import pathlib
import struct
import os
import tempfile

LIMIT=64*1024*1024

def pack(root,plan,destination):
    root=pathlib.Path(root).resolve(strict=True);destination=pathlib.Path(destination)
    if destination.exists():raise ValueError('bundle destination exists')
    files=plan['files'];actions={e['path']:e['action'] for e in plan['actions']}
    selected=[e for e in files if actions[e['path']]=='upload']
    header=json.dumps({'manifest_id':plan['manifest_id'],'entries':selected},separators=(',',':')).encode()
    size=12+len(header)+sum(e['size'] for e in selected)
    if len(files)>256 or len(header)>128*1024 or size>LIMIT:raise ValueError('bundle capacity exceeded')
    destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent,delete=False) as f:
        name=pathlib.Path(f.name)
        try:
            f.write(b'RHSYNC1\n');f.write(struct.pack('<I',len(header)));f.write(header)
            for e in selected:
                rel=pathlib.PurePosixPath(e['path'])
                if rel.is_absolute() or any(p in ('..','.') for p in rel.parts):raise ValueError('nonrelative path')
                path=root
                for part in rel.parts:
                    path=path/part
                    if path.is_symlink():raise ValueError('symlink input not supported')
                if not path.resolve(strict=True).is_relative_to(root):raise ValueError('source outside root')
                digest=hashlib.sha256();length=0
                with path.open('rb') as source:
                    while part:=source.read(1024*1024):
                        length+=len(part)
                        if length>e['size']:raise ValueError('source changed or exceeds declared size')
                        digest.update(part);f.write(part)
                if length!=e['size'] or digest.hexdigest()!=e['sha256']:raise ValueError('source changed since planning')
            f.flush();os.fsync(f.fileno())
            # A generated artifact must not overwrite a concurrent writer.
            os.link(name,destination)
        finally:name.unlink(missing_ok=True)
    sha=hashlib.sha256(destination.read_bytes()).hexdigest()
    return {'manifest_id':plan['manifest_id'],'changed_files':len(selected),'reused_files':len(files)-len(selected),'size':size,'sha256':sha,'path':str(destination)}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=pathlib.Path,required=True);parser.add_argument('--plan',type=pathlib.Path,required=True);parser.add_argument('--output',type=pathlib.Path,required=True);args=parser.parse_args()
    print(json.dumps(pack(args.source,json.loads(args.plan.read_text()),args.output)))

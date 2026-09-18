#!/usr/bin/env python3
"""Capture only the helper and its resolved workspace settings, not unrelated dirty code.

The resulting standalone Cargo workspace can be built on either platform. No build,
installation, privilege escalation, network request or service modification is performed.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tomllib


def toml_value(value):
    if isinstance(value,str):return json.dumps(value,ensure_ascii=False)
    if isinstance(value,bool):return 'true' if value else 'false'
    if isinstance(value,(int,float)):return str(value)
    if isinstance(value,list):return '['+', '.join(toml_value(x) for x in value)+']'
    if isinstance(value,dict):return '{ '+', '.join(f'{json.dumps(k)} = {toml_value(v)}' for k,v in value.items())+' }'
    raise TypeError(type(value))


def capture(root:Path,destination:Path):
    workspace=tomllib.loads((root/'Cargo.toml').read_text())['workspace']
    source=root/'crates/remote-hosts-admin'
    crate=tomllib.loads((source/'Cargo.toml').read_text())
    lines=['[workspace]','', '[package]']
    for key,value in crate['package'].items():
        if isinstance(value,dict) and value.get('workspace'):
            value=workspace['package'][key]
        lines.append(f'{key} = {toml_value(value)}')
    for section,dependencies in [('dependencies',crate['dependencies'])]+[
            (f'target.{json.dumps(target)}.dependencies',config['dependencies'])
            for target,config in crate.get('target',{}).items()]:
        lines+=['',f'[{section}]']
        for name,value in dependencies.items():
            if isinstance(value,dict) and value.get('workspace'):
                value=workspace['dependencies'][name]
            lines.append(f'{json.dumps(name)} = {toml_value(value)}')
    destination.mkdir(parents=True,exist_ok=False)
    (destination/'Cargo.toml').write_text('\n'.join(lines)+'\n')
    (destination/'rust-toolchain.toml').write_text((root/'rust-toolchain.toml').read_text())
    shutil.copytree(source/'src',destination/'src')
    (destination/'scripts').mkdir()
    for name in ['prepare-admin-helper.py','admin-helper-install.py']:
        shutil.copyfile(root/'scripts'/name,destination/'scripts'/name)
    (destination/'scripts/tests').mkdir()
    shutil.copyfile(root/'scripts/tests/test_admin_helper_preparation.py',
                    destination/'scripts/tests/test_admin_helper_preparation.py')
    if (root/'docs/admin-maintenance.md').is_file():
        shutil.copyfile(root/'docs/admin-maintenance.md',destination/'README.md')
    inputs={str(p.relative_to(destination)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(destination.rglob('*')) if p.is_file()}
    snapshot_id=hashlib.sha256(json.dumps(inputs,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    manifest=dict(snapshot_id=snapshot_id,files=inputs,state='captured_not_installed',
                  excluded='Other repository code and existing uncommitted storage/upgrade changes')
    (destination/'source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--destination',type=Path,required=True)
    a=p.parse_args();root=Path(__file__).resolve().parents[1]
    print(json.dumps(capture(root,a.destination.resolve()),indent=2))

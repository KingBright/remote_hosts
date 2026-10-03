#!/usr/bin/env python3
"""Publish one immutable, owner-built APK only with matching successful evidence."""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--acceptance', required=True, type=pathlib.Path)
    args = parser.parse_args()
    reports = ROOT / 'android/app/build/reports'
    build_path = reports / 'delivery.json'
    artifact_path = reports / 'artifact-verification.json'
    build = json.loads(build_path.read_text())
    artifact = json.loads(artifact_path.read_text())
    acceptance_path = args.acceptance.resolve(strict=True)
    if not acceptance_path.is_relative_to(ROOT / 'android/build/integration'):
        raise SystemExit('Use an owner-machine Android integration receipt from this repository.')
    acceptance = json.loads(acceptance_path.read_text())
    apk = ROOT / 'android/app/build/outputs/apk/release/app-release.apk'
    expected = build['apk_sha256']
    if digest(apk) != expected or artifact.get('sha256') != expected or not artifact.get('passed'):
        raise SystemExit('Artifact evidence does not match the current APK.')
    if not acceptance.get('passed') or acceptance.get('release_apk_sha256') != expected or not acceptance.get('release_apk_tested_as_install_target'):
        raise SystemExit('A successful real gateway/Android acceptance for this exact release APK is required.')
    for name, expected_source in build['source_hashes'].items():
        path = (ROOT / name).resolve(strict=True)
        if not path.is_relative_to(ROOT) or digest(path) != expected_source:
            raise SystemExit('Built source changed: ' + name)
    suites = [ET.parse(p).getroot() for p in (ROOT/'android/app/build/test-results/testDebugUnitTest').glob('TEST-*.xml')]
    if not suites or any(int(s.get('failures','0')) or int(s.get('errors','0')) for s in suites):
        raise SystemExit('Missing or failed Android unit tests.')
    lint = ET.parse(reports/'lint-results-release.xml').getroot().findall('issue')
    if any(i.get('severity') in ('Error','Fatal') for i in lint):
        raise SystemExit('Android lint has release-blocking errors.')
    fleet = subprocess.run(['python3',str(ROOT/'scripts/test-android-fleet-scope.py')], capture_output=True, text=True, timeout=60)
    if fleet.returncode:
        raise SystemExit('Mixed-fleet regression failed: ' + fleet.stderr)
    version = build['version']
    if version != '0.1.0' or build.get('package') != 'io.remotehosts.agent':
        raise SystemExit('Unexpected release identity.')
    target = ROOT/'dist/android'/version
    target.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation: no published/partially published directory is overwritten.
    target.mkdir()
    apk_name = f'remote-hosts-android-{version}.apk'
    shutil.copy2(apk,target/apk_name)
    shutil.copy2(build_path,target/'build-receipt.json')
    shutil.copy2(artifact_path,target/'artifact-verification.json')
    shutil.copy2(acceptance_path,target/'integration-acceptance.json')
    shutil.copy2(ROOT/'docs/android-install-zh.md',target/'INSTALL-zh.md')
    (target/'mixed-fleet-tests.txt').write_text(fleet.stdout+fleet.stderr)
    publication = {
        'version':version,'package':build['package'],'apk':apk_name,
        'sha256':expected,'bytes':apk.stat().st_size,'published_at':int(time.time()),
        'source_commit':subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip(),
        'unit_tests':sum(int(s.get('tests','0')) for s in suites),
        'artifact_checks':len(artifact['checks']),
        'integration_checks':len(acceptance['checks']),
        'mixed_fleet_tests':10,'lint_errors':0,'lint_warnings':len(lint),
        'physical_phone_tested':False,'production_gateway_modified':False,
        'configuration_embedded':False,'wireless_adb_pairing_ui_included':False,
        'scope':'Signed APK packaging and isolated Android emulator acceptance; not physical-phone or production enrollment acceptance'
    }
    (target/'publication.json').write_text(json.dumps(publication,indent=2,ensure_ascii=False)+'\n')
    files = sorted(p for p in target.iterdir() if p.is_file())
    (target/'SHA256SUMS').write_text(''.join(f'{digest(p)}  {p.name}\n' for p in files))
    # Completion marker is written last. It carries only an integrity hash.
    (target/'COMPLETE').write_text(digest(target/'SHA256SUMS')+'\n')
    print(json.dumps({'published':True,'directory':str(target.relative_to(ROOT)),**publication},ensure_ascii=False))

if __name__ == '__main__':
    main()

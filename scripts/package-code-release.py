#!/usr/bin/env python3
"""Package already-built candidates, tying them to verified source fingerprints.

Does not deploy, register devices, change credentials or claim runtime acceptance.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import tarfile

ROOT = pathlib.Path(__file__).resolve().parents[1]

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as file:
        for data in iter(lambda: file.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()

def machine_contract(binary, version):
    try:
        contract = json.loads(subprocess.check_output(
            [str(binary), 'release-manifest'], text=True, timeout=30))
    except (subprocess.SubprocessError, json.JSONDecodeError) as error:
        raise ValueError('native machine contract unavailable') from error
    if contract.get('version') != version:
        raise ValueError('machine contract version mismatch')
    if (contract.get('machine_contract_protocol') != 1
            or not isinstance(contract.get('tool_count'), int) or contract['tool_count'] <= 0
            or contract.get('tool_schema_revision') != contract.get('tools_sha256')
            or len(contract.get('tool_schema_revision', '')) != 64):
        raise ValueError('native machine contract identity invalid')
    forbidden = {'client_schema_comparison', 'host_schema_status', 'refresh_required', 'refresh_guidance'}
    if forbidden.intersection(contract):
        raise ValueError('release machine contract contains session-specific fields')
    return contract

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--version', required=True)
    p.add_argument('--verification', required=True, type=pathlib.Path)
    args = p.parse_args()
    proof_path = args.verification.resolve()
    proof = json.loads(proof_path.read_text())
    required = {'fmt', 'clippy', 'rust_tests', 'python_tests', 'workspace'}
    if (proof['version'] != args.version or proof.get('state') != 'passed'
            or not proof.get('source_inputs_unchanged')
            or proof['functional_tests']['failed'] != 0
            or not required.issubset(proof.get('checks', {}))
            or any(proof['checks'][name].get('exit_code') != 0 for name in required)):
        raise SystemExit('verification is incomplete, failed, stale or does not match release')
    spec = importlib.util.spec_from_file_location('release_source_check', ROOT/'scripts/check-code-source.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    current = verifier.inputs(ROOT)
    mismatches = sorted(name for name in current.keys() | proof['source_inputs'].keys()
                        if current.get(name) != proof['source_inputs'].get(name))
    if mismatches or not verifier.receipt_current(proof, ROOT):
        raise SystemExit('verified source changed or evidence incomplete: ' + ', '.join(mismatches))
    metadata = json.loads(subprocess.check_output(['cargo','metadata','--format-version','1','--no-deps','--locked'], cwd=ROOT))
    target = pathlib.Path(metadata['target_directory'])
    files = {
        'remote-hosts-code-macos-arm64': target/'release/remote-hosts-code',
        'remote-hosts-code-linux-amd64': target/'x86_64-unknown-linux-musl/release/remote-hosts-code',
        'remote-hosts-code-windows-amd64.exe': target/'x86_64-pc-windows-msvc/release/remote-hosts-code.exe',
        'upgrade-code-agent.py': ROOT/'scripts/upgrade-code-agent.py',
        'agent_upgrade_support.py': ROOT/'scripts/agent_upgrade_support.py',
        'launch-code-upgrade.py': ROOT/'scripts/launch-code-upgrade.py',
        'code_upgrade_runner.py': ROOT/'scripts/code_upgrade_runner.py',
        'macos_code_identity.py': ROOT/'scripts/macos_code_identity.py',
        'upgrade-code-gateway.py': ROOT/'scripts/upgrade-code-gateway.py',
        'check-code-gateway.py': ROOT/'scripts/check-code-gateway.py',
        'check-terminal-delivery.py': ROOT/'scripts/check-terminal-delivery.py',
        'release_receipts.py': ROOT/'scripts/release_receipts.py',
        'maintenance_client.py': ROOT/'scripts/maintenance_client.py',
        'release_targets.py': ROOT/'scripts/release_targets.py',
        'sync_bundle.py': ROOT/'scripts/sync_bundle.py',
        'release_client.py': ROOT/'scripts/release_client.py',
        'native_release_client.py': ROOT/'scripts/native_release_client.py',
        'publish-code.py': ROOT/'scripts/publish-code.py',
        'check-collaboration.py': ROOT/'scripts/check-collaboration.py',
        'install-code-agent.py': ROOT/'scripts/install-code-agent.py',
        'upgrade-code-agent-linux.py': ROOT/'scripts/upgrade-code-agent-linux.py',
        'upgrade-code-agent-windows.ps1': ROOT/'scripts/upgrade-code-agent-windows.ps1',
        'launch-code-upgrade-windows.ps1': ROOT/'scripts/launch-code-upgrade-windows.ps1',
        'fleet-upgrade.py': ROOT/'scripts/fleet-upgrade.py',
        'gateway_self_upgrade_runner.py': ROOT/'scripts/gateway_self_upgrade_runner.py',
    }
    for path in files.values():
        if not path.is_file():
            raise SystemExit('missing artifact: ' + str(path))
    native = files['remote-hosts-code-macos-arm64']
    actual = subprocess.check_output([str(native), '--version'], text=True).strip()
    if actual != 'remote-hosts-code ' + args.version:
        raise SystemExit('native artifact version mismatch')
    try:
        contract = machine_contract(native, args.version)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    if files['remote-hosts-code-windows-amd64.exe'].read_bytes()[:2] != b'MZ':
        raise SystemExit('Windows artifact is not a PE executable')
    # Compile helper source without importing it or running any deployment action.
    for path in files.values():
        if path.suffix == '.py':
            compile(path.read_text(), str(path), 'exec')
    dest = ROOT/'dist'/('remote-hosts-code-'+args.version)
    dest.parent.mkdir(exist_ok=True)
    if dest.exists():
        raise SystemExit('release directory already exists; do not overwrite a published candidate')
    with tempfile.TemporaryDirectory(prefix='.release-', dir=dest.parent) as temp:
        stage = pathlib.Path(temp)
        artifacts = {}
        for name, source in files.items():
            shutil.copyfile(source, stage/name)
            (stage/name).chmod(0o755 if not name.endswith('.py') else 0o644)
            artifacts[name] = {'sha256':digest(stage/name), 'size':(stage/name).stat().st_size}
        shutil.copyfile(proof_path, stage/'source-verification.json')
        manifest = {
            'schema_version':1, **contract,
            'packaged_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'snapshot_id':proof.get('snapshot_id'),
            'build_execution_root':str(ROOT),
            'source_verification_sha256':digest(proof_path),
            'source_inputs':proof['source_inputs'], 'artifacts':artifacts,
            'tests':proof['functional_tests'], 'deployed':False,
            'limitations':['64 MiB default per file; 0.7+ negotiated large-file mode permits explicit requests up to 256 MiB when storage reserve and agent capability gates pass; retained partials expire after 24 hours',
                            'inbound prefix recovery needs a strong validator or original expected SHA-256',
                            'expired authorization requires explicit transfer_resume with a refreshed file reference',
                            'publication already committed wins a concurrent cancellation',
                            'paused operations require an explicit resume; arbitrary shell outcomes never replay',
                            'workspace_context is scoped runtime state, not Git or chat-history reconstruction',
                            'native attachment and deployed-process acceptance are separate gates'],
        }
        (stage/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
        (stage/'SHA256SUMS').write_text(''.join(digest(stage/name)+'  '+name+'\n' for name in sorted([*files,'manifest.json','source-verification.json'])))
        (stage/'README.md').write_text('# Remote Hosts '+args.version+'\n\nThe bundle is the release identity. Upgrade the Gateway before Agents, or use fleet-upgrade.py to converge the whole fleet. Keep existing device identities.\nAll platform binaries and updater helpers are bound by manifest.json.\nRecord actual runtime versions and check binary SHA-256 against manifest.json.\nAcceptance is capability-based: required capabilities must be present; additive tools are allowed.\nSource verification is not a deployment acceptance receipt.\n')
        stage.rename(dest)
    bundle=dest.parent/(dest.name+'-bundle.tgz')
    if bundle.exists(): raise SystemExit('release bundle already exists; do not overwrite')
    with tarfile.open(bundle,'x:gz') as archive:
        for path in sorted(dest.iterdir(), key=lambda p:p.name):
            if path.is_file(): archive.add(path,arcname=path.name)
    print(json.dumps({'release_dir':str(dest), 'manifest_sha256':digest(dest/'manifest.json'),
                      'bundle':str(bundle),'bundle_sha256':digest(bundle),'bundle_size':bundle.stat().st_size,
                      'artifacts':artifacts, 'deployed':False}))

if __name__ == '__main__':
    main()

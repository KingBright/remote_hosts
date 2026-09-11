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

ROOT = pathlib.Path(__file__).resolve().parents[1]

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as file:
        for data in iter(lambda: file.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()

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
        'upgrade-code-agent.py': ROOT/'scripts/upgrade-code-agent.py',
        'agent_upgrade_support.py': ROOT/'scripts/agent_upgrade_support.py',
        'launch-code-upgrade.py': ROOT/'scripts/launch-code-upgrade.py',
        'upgrade-code-gateway.py': ROOT/'scripts/upgrade-code-gateway.py',
        'check-code-gateway.py': ROOT/'scripts/check-code-gateway.py',
        'release_receipts.py': ROOT/'scripts/release_receipts.py',
        'maintenance_client.py': ROOT/'scripts/maintenance_client.py',
        'release_targets.py': ROOT/'scripts/release_targets.py',
        'sync_bundle.py': ROOT/'scripts/sync_bundle.py',
        'release_client.py': ROOT/'scripts/release_client.py',
        'publish-code.py': ROOT/'scripts/publish-code.py',
        'check-collaboration.py': ROOT/'scripts/check-collaboration.py',
    }
    for path in files.values():
        if not path.is_file():
            raise SystemExit('missing artifact: ' + str(path))
    actual = subprocess.check_output([str(files['remote-hosts-code-macos-arm64']), '--version'], text=True).strip()
    if actual != 'remote-hosts-code ' + args.version:
        raise SystemExit('native artifact version mismatch')
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
            'schema_version':1, 'version':args.version,
            'packaged_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'dispatch_protocol':2, 'progress_protocol':1, 'readiness_protocol':1,
            'resource_dispatch_protocol':1, 'transfer_protocol':2, 'transfer_limits_protocol':1, 'tool_count':21, 'maintenance_protocol':1, 'terminal_observation_protocol':1, 'observation_protocol':2, 'change_set_protocol':1, 'storage_gc_protocol':1,
            'checkpoint_bytes':4194304, 'default_file_bytes':67108864, 'max_file_bytes':268435456, 'storage_reserve_bytes':268435456, 'snapshot_id':proof.get('snapshot_id'),
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
        (stage/'README.md').write_text('# Remote Hosts '+args.version+'\n\nGateway must be upgraded before agents. Keep existing device identities.\nRun the bundled upgrade scripts from a deployment session independent of the agent being replaced.\nRecord actual runtime versions and check binary SHA-256 against manifest.json.\nRun check-code-gateway.py with --expected-version '+args.version+' --dispatch-protocol 2 and explicit --device-id for each authorized device.\nSource verification is not a deployment or native browser-file acceptance receipt.\n')
        stage.rename(dest)
    print(json.dumps({'release_dir':str(dest), 'manifest_sha256':digest(dest/'manifest.json'), 'artifacts':artifacts, 'deployed':False}))

if __name__ == '__main__':
    main()

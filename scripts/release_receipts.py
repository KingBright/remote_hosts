#!/usr/bin/env python3
"""Verified publication inputs, bounded original-receipt observation, durable steps.

This module never executes shell, installs software, restarts services or obtains
credentials. The caller supplies explicitly selected actions and read observers.
An acknowledged launch is not an installed/healthy release.
"""
import contextlib
import hashlib
import json
import os
import pathlib
import re
import tempfile
import time

GATES = {'fmt', 'clippy', 'rust_tests', 'python_tests', 'workspace'}
STAGES = {'verification', 'macos_release', 'linux_release', 'windows_release', 'package'}

class ReleaseError(RuntimeError):
    def __init__(self, code, recovery, target=None):
        self.code, self.recovery, self.target = code, recovery, target
        super().__init__(code + (': '+target if target else '') + '; ' + recovery)


def digest(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open('rb') as f:
        for part in iter(lambda: f.read(1048576), b''):
            h.update(part)
    return h.hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def atomic_json(path, value):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        temporary = pathlib.Path(f.name)
        try:
            f.write((json.dumps(value, ensure_ascii=False, indent=2)+'\n').encode())
            f.flush()
            os.fsync(f.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def verified_build(report, expected_version):
    """Fail immediately without a finished producer. Validate exact packaged bytes."""
    report = pathlib.Path(report)
    if not report.is_file():
        raise ReleaseError('needs_build', 'start or locate the original build producer; no publication started')
    p = json.loads(report.read_text())
    if (p.get('state') != 'passed' or p.get('version') != expected_version
            or p.get('source_inputs_unchanged') is not True or p.get('verify_only') is not False
            or not STAGES.issubset(p.get('stages', {}))
            or any(p['stages'][s].get('exit_code') != 0 or p['stages'][s].get('state') != 'finished' for s in STAGES)):
        raise ReleaseError('build_not_verified', 'inspect the original build report; do not install or start an empty waiter')
    package = pathlib.Path(p['package']['path']).resolve(strict=True)
    manifest_file = package/'manifest.json'
    if manifest_file.is_symlink() or digest(manifest_file) != p['package']['manifest_sha256']:
        raise ReleaseError('manifest_conflict', 'retain original artifacts and verify provenance')
    m = json.loads(manifest_file.read_text())
    proof_file = package/'source-verification.json'
    proof_sha = digest(proof_file)
    if (proof_file.is_symlink() or proof_sha != m.get('source_verification_sha256')
            or proof_sha != p['verification']['sha256']):
        raise ReleaseError('verification_conflict', 'restore the verified package, not an unverified rebuild')
    v = json.loads(proof_file.read_text())
    if (v.get('state') != 'passed' or v.get('version') != expected_version or m.get('version') != expected_version
            or v.get('source_inputs_unchanged') is not True
            or v.get('source_inputs') != m.get('source_inputs')
            or m.get('snapshot_id') != p.get('snapshot_id')
            or v.get('snapshot_id') != p.get('snapshot_id')
            or not GATES.issubset(v.get('checks', {}))
            or any(v['checks'][s].get('exit_code') != 0 or v['checks'][s].get('state') != 'finished' for s in GATES)
            or v.get('functional_tests', {}).get('failed') != 0
            or v.get('functional_tests', {}).get('passed', 0) < 1
            or v.get('functional_tests', {}).get('test_gates_completed_successfully') is not True):
        raise ReleaseError('verification_incomplete', 'complete the original fixed-input verification before publication')
    artifacts = m.get('artifacts', {})
    required = {'remote-hosts-code-macos-arm64','remote-hosts-code-linux-amd64',
                'remote-hosts-code-windows-amd64.exe','upgrade-code-agent.py','agent_upgrade_support.py','launch-code-upgrade.py',
                'code_upgrade_runner.py','macos_code_identity.py',
                'upgrade-code-gateway.py','check-code-gateway.py'}
    if not required.issubset(artifacts):
        raise ReleaseError('package_incomplete', 'use the complete immutable release package')
    for name, entry in artifacts.items():
        file = package/name
        if (pathlib.Path(name).name != name or name in ('.', '..') or file.is_symlink()
                or not re.fullmatch('[0-9a-f]{64}', entry.get('sha256',''))
                or not file.is_file() or file.stat().st_size != entry.get('size')
                or digest(file) != entry['sha256']):
            raise ReleaseError('artifact_conflict', 'retain the original package and investigate changed bytes')
    return {'version': expected_version, 'package': str(package), 'manifest_sha256': digest(manifest_file),
            'snapshot_id': m['snapshot_id'], 'artifacts': artifacts, 'tests': v['functional_tests']}


def wait_receipts(readers, expected, *, timeout=300, interval=2, on_update=None, clock=None, sleep=None):
    """Observe each *original* updater. Missing receipts are pending, never failed installs."""
    clock, sleep = clock or time.monotonic, sleep or time.sleep
    if set(readers) != set(expected) or not readers or timeout <= 0 or interval <= 0:
        raise ValueError('explicit identical nonempty target sets and positive deadlines required')
    deadline = clock()+timeout
    receipts, last = {}, {}
    while True:
        statuses = {}
        for target, reader in readers.items():
            if target in receipts:
                statuses[target] = 'verified'
                continue
            try:
                receipt = reader()
            except (OSError, TimeoutError):
                receipt = None
                statuses[target] = 'observer_unavailable'
            if receipt is None:
                statuses.setdefault(target, 'awaiting_original_receipt')
                continue
            if not isinstance(receipt, dict):
                raise ReleaseError('invalid_updater_receipt', 'inspect the original result without reinstalling', target)
            state = receipt.get('state')
            if state in ('prepared','started','preflight','running','waiting_idle'):
                statuses[target] = 'updater_'+state
                continue
            if state == 'failed':
                raise ReleaseError('updater_failed', 'inspect the original failure and rollback evidence; successful peers stay installed', target)
            e = expected[target]
            if state == 'no_change':
                raise ReleaseError('requires_runtime_acceptance', 'same bytes do not prove health; inspect runtime and original install receipt', target)
            if (state != 'upgraded' or receipt.get('version') != e['version']
                    or receipt.get('installed_sha256') != e['sha256']
                    or receipt.get('candidate_sha256') != e['sha256']
                    or receipt.get('gateway_verified') is not True
                    or receipt.get('all_lanes_verified') is not True
                    or receipt.get('stable_seconds', 0) < 15
                    or receipt.get('samples', 0) < 2
                    or not isinstance(receipt.get('pid'), int) or receipt['pid'] <= 1
                    or not re.fullmatch('[0-9a-f]{64}', receipt.get('session',''))):
                raise ReleaseError('updater_receipt_conflict', 'check candidate, runtime and the original immutable result', target)
            receipts[target] = receipt
            statuses[target] = 'verified'
        if statuses != last and on_update:
            on_update(dict(statuses))
        last = statuses
        if len(receipts) == len(readers):
            return receipts
        if clock() >= deadline:
            raise ReleaseError('receipt_observation_timeout', 'query the saved original updater paths; do not install again',
                               ','.join(sorted(set(readers)-set(receipts))))
        sleep(min(interval, deadline-clock()))


class StepJournal:
    """At-most-one automatic side-effect attempt per stage; uncertain work is observed.

Hold the context for an entire publishing process. A separate observer reads
report JSON without acquiring this lock. Callables remain responsible for their
own authorization and bounded I/O; this is not a command execution interface.
"""
    def __init__(self, path, binding):
        self.path = pathlib.Path(path)
        self.binding = binding
        self.record = None
        self.lock = None

    def __enter__(self):
        import fcntl
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
        self.lock = os.fdopen(os.open(str(self.path)+'.lock', flags, 0o600), 'r+b')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                self.record = json.loads(self.path.read_text())
                if self.record.get('binding') != self.binding:
                    raise ReleaseError('publication_identity_conflict', 'observe the existing release; do not overwrite its journal')
            else:
                self.record = {'schema_version':1,'binding':self.binding,'steps':{}}
                self.save()
            return self
        except BaseException:
            self.lock.close()
            self.lock = None
            raise

    def __exit__(self, *_):
        if self.lock:
            self.lock.close()
            self.lock = None

    def save(self):
        self.record['updated_at_epoch'] = time.time()
        atomic_json(self.path, self.record)

    def step(self, name, inputs, action):
        if self.lock is None:
            raise RuntimeError('step journal must be held')
        expected = identity(inputs)
        saved = self.record['steps'].get(name)
        if saved:
            if saved['input_sha256'] != expected:
                raise ReleaseError('stage_identity_conflict', 'inspect the original stage inputs', name)
            if saved['state'] == 'completed':
                return saved['result']
            raise ReleaseError('stage_outcome_unknown', 'observe and reconcile the recorded original action before resuming', name)
        saved = {'state':'requested','input_sha256':expected,'requested_at_epoch':time.time()}
        self.record['steps'][name] = saved
        self.save()
        try:
            result = action()
            json.dumps(result)
        except BaseException as error:
            saved.update(state='outcome_unknown', failure_type=type(error).__name__)
            self.save()
            raise
        saved.update(state='completed', result=result)
        self.save()
        return result

    def reconcile(self, name, inputs, observe):
        if self.lock is None:
            raise RuntimeError('step journal must be held')
        saved = self.record['steps'].get(name)
        if not saved or saved['input_sha256'] != identity(inputs):
            raise ReleaseError('stage_identity_conflict', 'supply the original recorded inputs', name)
        if saved['state'] == 'completed':
            return saved['result']
        result = observe()
        if result is None:
            raise ReleaseError('stage_outcome_unknown', 'retain the original action; observation has not confirmed completion', name)
        json.dumps(result)
        saved.update(state='completed', result=result, recovered_by_observation=True)
        self.save()
        return result

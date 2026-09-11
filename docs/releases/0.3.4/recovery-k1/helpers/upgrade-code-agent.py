#!/usr/bin/env python3
"""Idempotent local upgrade. Run independently of the agent being replaced.

Use an explicitly one-shot launcher, never a KeepAlive or periodic updater.
Identical installed bytes cause no restart and do not replace the last receipt.
Installation requires stable authenticated gateway observation and actual local
poll acknowledgements. Functional release acceptance remains a separate gate.
Never prints or rewrites the device credential.
"""
import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid
from agent_upgrade_support import GatewayObservationError, gateway_observation, wait_ready


@contextlib.contextmanager
def upgrade_lock(base):
    """Serialize decision, installation, rollback and receipt publication."""
    flags = os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(base / 'upgrade.lock', flags, 0o600)
    with os.fdopen(fd, 'r+b') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN):
                raise RuntimeError('another upgrade is running; no service changed') from error
            raise
        yield


def current_launchd_pid(text):
    """Read top-level launchctl fields, not nested resource-group state."""
    fields = dict(re.findall(r'^\t(state|pid) = ([^\n]+)$', text, re.MULTILINE))
    pid = fields.get('pid', '')
    return int(pid) if fields.get('state') == 'running' and pid.isdecimal() and int(pid) > 1 else None


def save_attempt(result_path, record):
    """Keep every attempt and atomically publish the latest complete receipt."""
    result_path.parent.mkdir(parents=True, exist_ok=True)
    history = result_path.parent / (result_path.stem + '-attempts')
    history.mkdir(mode=0o700, exist_ok=True)
    attempt_path = history / (uuid.uuid4().hex + '.json')
    record['attempt_receipt'] = str(attempt_path)
    payload = json.dumps(record, indent=2).encode()
    with attempt_path.open('xb') as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    with tempfile.NamedTemporaryFile(dir=result_path.parent, delete=False) as file:
        temporary = pathlib.Path(file.name)
        try:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
            os.replace(temporary, result_path)
        finally:
            temporary.unlink(missing_ok=True)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_copy(source, target):
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as out:
        temporary = pathlib.Path(out.name)
        try:
            with source.open('rb') as src:
                shutil.copyfileobj(src, out)
            out.flush()
            os.fsync(out.fileno())
            os.fchmod(out.fileno(), 0o755)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', required=True, type=pathlib.Path)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--result', required=True, type=pathlib.Path)
    parser.add_argument('--idle-timeout', type=int, default=180)
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?', args.version):
        parser.error('invalid release version')
    if not re.fullmatch(r'[0-9a-f]{64}', args.sha256):
        parser.error('sha256 must be 64 lowercase hexadecimal characters')
    if args.idle_timeout < 0:
        parser.error('idle-timeout must not be negative')
    os.umask(0o077)
    base = pathlib.Path.home() / '.local/share/remote-hosts-code'
    with upgrade_lock(base):
        perform_upgrade(args, base)


def perform_upgrade(args, base):
    binary = base / 'bin/remote-hosts-code'
    config_path = base / 'agent.json'
    service = 'gui/%d/com.remote-hosts.code-agent' % os.getuid()
    record = {'version': args.version, 'candidate_sha256': args.sha256, 'state': 'preflight'}
    backup = None
    changed = False
    old_version = None
    try:
        if sha(args.candidate) != args.sha256:
            raise RuntimeError('candidate checksum mismatch')
        version = subprocess.check_output([str(args.candidate), '--version'], text=True, timeout=10).strip()
        if version != 'remote-hosts-code ' + args.version:
            raise RuntimeError('candidate version mismatch')
        installed = sha(binary)
        if installed == args.sha256:
            # Re-entry is not permission to restart an unhealthy service. Preserve
            # the last install/rollback receipt instead of reporting a fake upgrade.
            record.update(state='no_change', installed_sha256=installed,
                          service_changed=False, health='not_evaluated',
                          latest_receipt_preserved=args.result.exists())
            if not args.result.exists():
                save_attempt(args.result, record)
            print(json.dumps(record), flush=True)
            return
        config = json.loads(config_path.read_text())
        database = pathlib.Path(config['state_dir']) / 'state.sqlite'
        record['phase'] = 'gateway_preflight'
        remote_before = gateway_observation(config)
        record['gateway_preflight'] = remote_before.get('_transport', {})
        record['phase'] = 'candidate_validation'
        old_version = subprocess.check_output([str(binary), '--version'], text=True, timeout=10).strip().removeprefix('remote-hosts-code ')
        subprocess.run([str(args.candidate), 'check', '--agent', '--config', str(config_path)], check=True, stdout=subprocess.DEVNULL, timeout=15)
        record['phase'] = 'waiting_idle'
        deadline = time.monotonic() + args.idle_timeout
        while True:
            with contextlib.closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
                active = db.execute("SELECT COUNT(*) FROM kv WHERE kind='terminal' AND json_extract(value,'$.state') IN ('running','starting')").fetchone()[0]
                busy = db.execute("SELECT COUNT(*) FROM kv WHERE kind='local_operation' AND json_extract(value,'$.state')='running'").fetchone()[0]
            if not active and not busy:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('agent still has active work; upgrade not applied')
            time.sleep(1)
        backup_dir = base / 'releases' / ('before-' + args.version + '-' + time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8])
        backup_dir.mkdir(parents=True, exist_ok=False)
        backup = backup_dir / 'remote-hosts-code'
        shutil.copy2(binary, backup)
        record.update(backup=str(backup), previous_sha256=sha(backup))
        record['phase'] = 'installing'
        cutover_started = int(time.time())
        atomic_copy(args.candidate, binary)
        changed = True
        if sha(binary) != args.sha256:
            raise RuntimeError('installed binary does not match candidate checksum')
        subprocess.run(['launchctl', 'kickstart', '-k', service], check=True, timeout=10)
        requires_lanes = tuple(int(n) for n in args.version.split('-')[0].split('+')[0].split('.')) >= (0, 3, 1)
        record['phase'] = 'verifying_readiness'
        ready = wait_ready(base, config, args.version, cutover_started,
                           remote_before.get('last_seen', 0), require_lanes=requires_lanes,
                           previous_session=remote_before.get('session'))
        if sha(binary) != args.sha256:
            raise RuntimeError('installed binary changed during readiness verification')
        record.update(state='upgraded', phase='completed', service_changed=True, installed_sha256=sha(binary), **ready)
    except Exception as error:
        record.update(state='failed', service_changed=changed, error=str(error))
        if isinstance(error, GatewayObservationError):
            record['diagnostic'] = error.diagnostic
        if changed and backup:
            try:
                if sha(backup) != record['previous_sha256']:
                    raise RuntimeError('rollback backup checksum mismatch')
                rollback_started = int(time.time())
                atomic_copy(backup, binary)
                subprocess.run(['launchctl', 'kickstart', '-k', service], check=True, timeout=10)
                record['rollback'] = 'binary_restored; readiness_pending'
                record['rollback_readiness'] = wait_ready(base, config, old_version, rollback_started,
                    remote_before.get('last_seen', 0), require_lanes=False,
                    previous_session=remote_before.get('session'))
                record['rollback'] = 'restored_previous_binary_and_verified_gateway'
            except Exception as rollback_error:
                record['rollback'] = 'failed: ' + str(rollback_error)
    save_attempt(args.result, record)
    print(json.dumps(record), flush=True)
    if record['state'] != 'upgraded':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

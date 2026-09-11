#!/usr/bin/env python3
"""Prepare an immutable one-shot updater; --start explicitly bootstraps it.

No KeepAlive, periodic trigger or launchctl submit. An uncertain bootstrap is
recorded and is never automatically repeated. This file does not imply readiness.
"""
import argparse
import fcntl
import hashlib
import json
import os
import pathlib
import plistlib
import re
import subprocess
import sys
import tempfile

FILES = ('upgrade-code-agent.py', 'agent_upgrade_support.py', 'maintenance_client.py', 'job.plist')


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def write_new(path, data):
    with path.open('xb') as f:
        os.fchmod(f.fileno(), 0o600)
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def write_json(path, value):
    write_new(path, (json.dumps(value, indent=2)+'\n').encode())


def make_plist(label, python, script, candidate, checksum, version, result, directory):
    return {'Label': label, 'ProgramArguments': [str(python), str(script), '--candidate', str(candidate),
            '--sha256', checksum, '--version', version, '--result', str(result)],
            'RunAtLoad': True, 'KeepAlive': False, 'ProcessType': 'Background', 'Umask': 0o077,
            'StandardOutPath': str(directory / 'stdout.log'), 'StandardErrorPath': str(directory / 'stderr.log'),
            'EnvironmentVariables': {'PATH': '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin'}}


def validate_prepared(record):
    directory = pathlib.Path(record['directory'])
    if set(record.get('file_sha256', {})) != set(FILES):
        raise ValueError('prepared file manifest changed or incomplete')
    for name, expected in record['file_sha256'].items():
        if not (directory/name).is_file() or sha(directory/name) != expected:
            raise ValueError('prepared updater changed; refuse reuse')
    spec = plistlib.loads((directory/'job.plist').read_bytes())
    if (spec.get('Label') != record['label'] or spec.get('KeepAlive') is not False
            or spec.get('RunAtLoad') is not True
            or any(k in spec for k in ('StartInterval', 'StartCalendarInterval', 'WatchPaths', 'QueueDirectories', 'Sockets'))):
        raise ValueError('prepared launch policy changed')
    if record.get('candidate') and sha(pathlib.Path(record['candidate'])) != record['candidate_sha256']:
        raise ValueError('prepared candidate changed')
    return record


def prepare(args, base, python=None):
    candidate = args.candidate.resolve(strict=True)
    source = pathlib.Path(__file__).resolve().parent
    if sha(candidate) != args.sha256:
        raise ValueError('candidate checksum mismatch')
    python = pathlib.Path(python or sys.executable).resolve(strict=True)
    sources = {name: (source/name).read_bytes() for name in FILES if name != 'job.plist'}
    identity_input = {'candidate': str(candidate), 'sha256': args.sha256, 'version': args.version,
                      'python': str(python), 'helpers': {n: hashlib.sha256(b).hexdigest() for n,b in sources.items()}}
    identity = hashlib.sha256(json.dumps(identity_input, sort_keys=True).encode()).hexdigest()[:24]
    jobs = base/'releases'/'upgrade-jobs'
    jobs.mkdir(parents=True, mode=0o700, exist_ok=True)
    directory = jobs/identity
    label = 'com.remote-hosts.code-upgrade.'+identity
    # Preparation is transactional: a crash cannot publish a half-written job.
    with (jobs/'.prepare.lock').open('a+b') as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        if directory.exists():
            return validate_prepared(json.loads((directory/'prepared.json').read_text()))
        with tempfile.TemporaryDirectory(prefix='.prepare-', dir=jobs) as tmp:
            stage = pathlib.Path(tmp)
            for name, data in sources.items():
                write_new(stage/name, data)
            result = directory/'result.json'
            spec = make_plist(label, python, directory/'upgrade-code-agent.py', candidate, args.sha256,
                              args.version, result, directory)
            write_new(stage/'job.plist', plistlib.dumps(spec))
            record = {'state': 'prepared', 'label': label, 'directory': str(directory), 'result': str(result),
                      'version': args.version, 'candidate': str(candidate), 'candidate_sha256': args.sha256,
                      'file_sha256': {name: sha(stage/name) for name in FILES}}
            write_json(stage/'prepared.json', record)
            stage.rename(directory)
    return record


def start_once(record):
    directory = pathlib.Path(record['directory'])
    marker = directory/'start-requested.json'
    # Check content before making an irreversible launch-intent record.
    validate_prepared(record)
    try:
        write_json(marker, {'state': 'bootstrap_requested', 'label': record['label']})
    except FileExistsError:
        return {'state': 'already_requested', 'result': record['result'], 'service_changed': False,
                'recovery': 'inspect saved bootstrap and updater result; do not automatically resubmit'}
    outcome = {'label': record['label'], 'result': record['result']}
    try:
        result = subprocess.run(['launchctl', 'bootstrap', 'gui/%d' % os.getuid(), str(directory/'job.plist')],
                                text=True, capture_output=True, timeout=10)
        outcome.update(state='started' if result.returncode == 0 else 'bootstrap_failed',
                       exit_code=result.returncode)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        outcome.update(state='bootstrap_outcome_unknown',
                       recovery='inspect launchd and result; launch may have succeeded, do not replay')
    except OSError:
        outcome.update(state='bootstrap_not_started',
                       recovery='launchctl could not be invoked; inspect environment before a new authorized attempt')
    write_json(directory/'bootstrap-result.json', outcome)
    return outcome


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--candidate', required=True, type=pathlib.Path)
    p.add_argument('--sha256', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--start', action='store_true')
    args = p.parse_args()
    if not re.fullmatch(r'[0-9a-f]{64}', args.sha256) or not re.fullmatch(r'\d+\.\d+\.\d+', args.version):
        p.error('invalid checksum or version')
    if sys.platform != 'darwin':
        p.error('launchd upgrades require macOS')
    os.umask(0o077)
    record = prepare(args, pathlib.Path.home()/'.local/share/remote-hosts-code')
    result = start_once(record) if args.start else record
    print(json.dumps(result))
    if result['state'] not in ('prepared', 'started', 'already_requested'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()

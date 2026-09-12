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

import macos_code_identity

FILES = ('upgrade-code-agent.py', 'agent_upgrade_support.py', 'maintenance_client.py', 'macos_code_identity.py', 'job.plist')
STABLE_LABEL = 'com.remote-hosts.code-upgrade'
RUNNER_FILE = 'code_upgrade_runner.py'


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


def atomic_write(path, data, mode=0o600):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary = pathlib.Path(file.name)
        try:
            os.fchmod(file.fileno(), mode)
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def stable_paths(base):
    directory = pathlib.Path(base) / 'updater'
    return {
        'directory': directory,
        'runner': directory / 'runner.py',
        'request': directory / 'request.json',
        'plist': directory / 'job.plist',
    }


def stable_plist(python, paths):
    return {
        'Label': STABLE_LABEL,
        'ProgramArguments': [str(python), str(paths['runner']), '--request', str(paths['request'])],
        'RunAtLoad': False, 'KeepAlive': False, 'ProcessType': 'Background', 'Umask': 0o077,
        'StandardOutPath': str(paths['directory'] / 'stdout.log'),
        'StandardErrorPath': str(paths['directory'] / 'stderr.log'),
        'EnvironmentVariables': {'PATH': '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin'},
    }


def service_state(domain, label=STABLE_LABEL):
    service = f'{domain}/{label}'
    result = subprocess.run(['launchctl', 'print', service], text=True, capture_output=True, timeout=10)
    if result.returncode != 0:
        return {'loaded': False, 'running': False, 'service': service}
    fields = dict(re.findall(r'^\t(state|pid) = ([^\n]+)$', result.stdout or '', re.MULTILINE))
    pid = fields.get('pid', '')
    running = fields.get('state') == 'running' and pid.isdecimal() and int(pid) > 1
    return {'loaded': True, 'running': running, 'service': service,
            'state': fields.get('state'), 'pid': int(pid) if pid.isdecimal() else None}


def cleanup_legacy_jobs(domain):
    result = subprocess.run(['launchctl', 'print', domain], text=True, capture_output=True, timeout=10)
    if result.returncode != 0:
        return 0
    labels = sorted(set(re.findall(r'com\\.remote-hosts\\.code-upgrade\\.[A-Za-z0-9._-]+', result.stdout or '')))
    removed = 0
    for label in labels:
        state = service_state(domain, label)
        if state['loaded'] and not state['running']:
            subprocess.run(['launchctl', 'bootout', state['service']], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=10)
            removed += 1
    return removed


def install_stable_dispatcher(base):
    paths = stable_paths(base)
    paths['directory'].mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(paths['directory'], 0o700)
    source = pathlib.Path(__file__).resolve().parent / RUNNER_FILE
    runner = source.read_bytes()
    python = pathlib.Path(sys.executable).resolve(strict=True)
    plist = plistlib.dumps(stable_plist(python, paths))
    changed = False
    if not paths['runner'].is_file() or paths['runner'].read_bytes() != runner:
        atomic_write(paths['runner'], runner)
        changed = True
    if not paths['plist'].is_file() or paths['plist'].read_bytes() != plist:
        atomic_write(paths['plist'], plist)
        changed = True
    return paths, changed


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
            record = {'state': 'prepared', 'label': label, 'base': str(pathlib.Path(base)),
                      'directory': str(directory), 'result': str(result),
                      'version': args.version, 'candidate': str(candidate), 'candidate_sha256': args.sha256,
                      'file_sha256': {name: sha(stage/name) for name in FILES}}
            write_json(stage/'prepared.json', record)
            stage.rename(directory)
    return record


def start_once(record, require_identity=True):
    directory = pathlib.Path(record['directory'])
    base = pathlib.Path(record['base'])
    marker = directory/'start-requested.json'
    validate_prepared(record)
    if marker.exists():
        return {'state': 'already_requested', 'result': record['result'], 'service_changed': False,
                'recovery': 'inspect the stable updater and saved result; do not automatically resubmit'}
    if require_identity:
        signing = macos_code_identity.status(base, create_if_missing=True)
        if signing.get('state') != 'ready':
            return {'state': 'authorization_required', 'result': record['result'],
                    'service_changed': False, 'signing': signing,
                    'next_action': 'authorize the Remote Hosts local signing identity once, then retry this same launch'}
    domain = 'gui/%d' % os.getuid()
    current = service_state(domain)
    if current['running']:
        return {'state': 'updater_busy', 'result': record['result'], 'service_changed': False,
                'next_action': 'observe the running stable updater; do not overwrite its request'}
    legacy_removed = cleanup_legacy_jobs(domain)
    paths, dispatcher_changed = install_stable_dispatcher(base)
    current = service_state(domain)
    if dispatcher_changed and current['loaded']:
        subprocess.run(['launchctl', 'bootout', current['service']], check=True, timeout=10)
        current = {'loaded': False, 'running': False, 'service': current['service']}
    request = {'directory': record['directory'], 'prepared_sha256': sha(directory/'prepared.json')}
    atomic_write(paths['request'], (json.dumps(request, indent=2)+'\n').encode())
    try:
        write_json(marker, {'state': 'kickstart_requested', 'label': STABLE_LABEL,
                            'prepared_sha256': request['prepared_sha256']})
    except FileExistsError:
        return {'state': 'already_requested', 'result': record['result'], 'service_changed': False,
                'recovery': 'inspect the stable updater and saved result; do not automatically resubmit'}
    outcome = {'label': STABLE_LABEL, 'result': record['result'],
               'legacy_jobs_removed': legacy_removed, 'dispatcher_changed': dispatcher_changed}
    try:
        bootstrapped = False
        if not current['loaded']:
            result = subprocess.run(['launchctl', 'bootstrap', domain, str(paths['plist'])],
                                    text=True, capture_output=True, timeout=10)
            if result.returncode != 0:
                outcome.update(state='bootstrap_failed', exit_code=result.returncode, service_changed=False)
                write_json(directory/'bootstrap-result.json', outcome)
                return outcome
            bootstrapped = True
        result = subprocess.run(['launchctl', 'kickstart', f'{domain}/{STABLE_LABEL}'],
                                text=True, capture_output=True, timeout=10)
        outcome.update(state='started' if result.returncode == 0 else 'kickstart_failed',
                       exit_code=result.returncode, service_changed=result.returncode == 0,
                       service_bootstrapped=bootstrapped)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        outcome.update(state='kickstart_outcome_unknown', service_changed=False,
                       recovery='inspect stable launchd service and saved updater result; do not replay automatically')
    except OSError:
        outcome.update(state='kickstart_not_started', service_changed=False,
                       recovery='launchctl could not be invoked; inspect environment before another authorized attempt')
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
    if result['state'] not in ('prepared', 'started', 'already_requested', 'authorization_required'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()

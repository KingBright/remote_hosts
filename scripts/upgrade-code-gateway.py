#!/usr/bin/env python3
"""Upgrade one configured Code Gateway binary with a bounded local rollback.

Deployment identity is supplied explicitly. DNS, reverse-proxy configuration and
provider-specific routing belong to the private ops layer and are not mutated here.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=str(destination.parent), delete=False) as stream:
        temp = pathlib.Path(stream.name)
        try:
            with source.open('rb') as src:
                shutil.copyfileobj(src, stream)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), 0o755)
            os.replace(str(temp), str(destination))
        finally:
            temp.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', required=True, type=pathlib.Path)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--result', required=True, type=pathlib.Path)
    parser.add_argument('--binary-path', required=True, type=pathlib.Path)
    parser.add_argument('--config-path', required=True, type=pathlib.Path)
    parser.add_argument('--backup-root', required=True, type=pathlib.Path)
    parser.add_argument('--service-name', default='remote-hosts-code-gateway.service')
    parser.add_argument('--gateway-bind', default='127.0.0.1:18787')
    args = parser.parse_args()

    os.umask(0o077)
    if os.getuid() != 0:
        raise SystemExit('gateway upgrade requires administrator/root authority')
    if '/' in args.service_name or not args.service_name.endswith('.service'):
        raise SystemExit('invalid systemd service name')
    bind_host, separator, bind_port = args.gateway_bind.rpartition(':')
    if not separator or not bind_host or not bind_port.isdigit():
        raise SystemExit('--gateway-bind must look like 127.0.0.1:18787')

    binary = args.binary_path.resolve(strict=True)
    config = args.config_path.resolve(strict=True)
    backup_root = args.backup_root.resolve()
    gateway_config = json.loads(config.read_text())
    state_dir = pathlib.Path(gateway_config['state_dir']).resolve()
    record = {'state': 'preflight', 'version': args.version, 'candidate_sha256': args.sha256}
    changed = False
    backup = backup_root / ('before-' + args.version + '-' + time.strftime('%Y%m%dT%H%M%S'))

    try:
        if checksum(args.candidate) != args.sha256:
            raise RuntimeError('candidate checksum mismatch')
        reported = subprocess.check_output([str(args.candidate), '--version'], text=True).strip()
        if reported != 'remote-hosts-code ' + args.version:
            raise RuntimeError('candidate version mismatch')
        subprocess.check_call(
            [str(args.candidate), 'check', '--config', str(config)],
            stdout=subprocess.DEVNULL,
        )
        backup.mkdir(parents=True, exist_ok=False)
        shutil.copy2(binary, backup / binary.name)
        database = state_dir / 'state.sqlite'
        if database.exists():
            with sqlite3.connect(str(database)) as src:
                with sqlite3.connect(str(backup / 'state.sqlite')) as dst:
                    src.backup(dst)
            record['database_backup'] = str(backup / 'state.sqlite')
        record['backup'] = str(backup)
        record['previous_sha256'] = checksum(backup / binary.name)

        atomic_copy(args.candidate, binary)
        changed = True
        subprocess.check_call(['systemctl', 'restart', args.service_name])
        authority = urllib.parse.urlsplit(gateway_config['public_url']).netloc
        deadline = time.monotonic() + 30
        health = None
        while time.monotonic() < deadline:
            try:
                request = urllib.request.Request(
                    f'http://{bind_host}:{bind_port}/healthz',
                    headers={'Host': authority},
                )
                with urllib.request.urlopen(request, timeout=2) as response:
                    health = json.load(response)
                if health.get('version') == args.version and health.get('file_transfer') is True:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError('new gateway readiness failed')

        pid_text = subprocess.check_output(
            ['systemctl', 'show', args.service_name, '--property=MainPID'], text=True
        ).strip().split('=', 1)[1]
        if int(pid_text) <= 1 or checksum(pathlib.Path('/proc') / pid_text / 'exe') != args.sha256:
            raise RuntimeError('running gateway executable checksum mismatch')
        record.update(
            state='upgraded',
            pid=int(pid_text),
            installed_sha256=checksum(binary),
            health=health,
        )
    except Exception as error:
        record.update(state='failed', error=str(error))
        if changed:
            try:
                atomic_copy(backup / binary.name, binary)
                subprocess.check_call(['systemctl', 'restart', args.service_name])
                record['rollback'] = 'restored_previous_binary; live database preserved'
            except Exception as rollback_error:
                record['rollback'] = 'failed: ' + str(rollback_error)

    args.result.parent.mkdir(parents=True, exist_ok=True)
    args.result.write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record), flush=True)
    if record['state'] != 'upgraded':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

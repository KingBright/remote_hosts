#!/usr/bin/env python3
"""Upgrade the existing NAS gateway only, with binary/database/site backups.

Credentials and device registrations remain unchanged. Must run as the existing
NAS administrator. This is not an installer for another machine.
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
import urllib.request


def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def atomic_copy(source, destination):
    with tempfile.NamedTemporaryFile(dir=str(destination.parent), delete=False) as f:
        temp = pathlib.Path(f.name)
        try:
            with source.open('rb') as src:
                shutil.copyfileobj(src, f)
            f.flush()
            os.fsync(f.fileno())
            os.fchmod(f.fileno(), 0o755)
            os.replace(str(temp), str(destination))
        finally:
            if temp.exists():
                temp.unlink()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', required=True, type=pathlib.Path)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--result', required=True, type=pathlib.Path)
    args = parser.parse_args()
    os.umask(0o077)
    if os.getuid() != 0:
        raise SystemExit('Use the existing NAS administrator')
    app = pathlib.Path('/opt/remote-hosts-code')
    binary = app / 'remote-hosts-code'
    config = app / 'gateway.json'
    site = pathlib.Path('/etc/caddy/sites/remote-hosts-code.caddy')
    c = json.loads(config.read_text())
    state = pathlib.Path(c['state_dir'])
    record = {'state': 'preflight', 'version': args.version, 'candidate_sha256': args.sha256}
    changed = False
    site_changed = False
    backup = app / 'releases' / ('before-' + args.version + '-' + time.strftime('%Y%m%dT%H%M%S'))
    try:
        if checksum(args.candidate) != args.sha256:
            raise RuntimeError('candidate checksum mismatch')
        reported = subprocess.check_output([str(args.candidate), '--version'], universal_newlines=True).strip()
        if reported != 'remote-hosts-code ' + args.version:
            raise RuntimeError('candidate version mismatch')
        subprocess.check_call([str(args.candidate), 'check', '--config', str(config)], stdout=subprocess.DEVNULL)
        backup.mkdir(parents=True, exist_ok=False)
        shutil.copy2(str(binary), str(backup / binary.name))
        shutil.copy2(str(site), str(backup / site.name))
        with sqlite3.connect(str(state / 'state.sqlite')) as src:
            with sqlite3.connect(str(backup / 'state.sqlite')) as dst:
                src.backup(dst)
        record['backup'] = str(backup)
        record['previous_sha256'] = checksum(backup / binary.name)
        atomic_copy(args.candidate, binary)
        changed = True
        subprocess.check_call(['systemctl', 'restart', 'remote-hosts-code-gateway.service'])
        authority = urllib.parse.urlsplit(c['public_url']).netloc
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                req = urllib.request.Request('http://127.0.0.1:18787/healthz', headers={'Host': authority})
                with urllib.request.urlopen(req, timeout=2) as response:
                    health = json.load(response)
                if health.get('version') == args.version and health.get('file_transfer') is True:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError('new gateway readiness failed')
        # The new gateway handles OAuth headers itself. The old blanket proxy
        # override would incorrectly change download links back to same-origin.
        old_site = site.read_text()
        lines = old_site.splitlines(keepends=True)
        new_site = ''.join(line for line in lines if not line.strip().startswith(('header_down Referrer-Policy ', 'header_down Content-Security-Policy ')))
        if new_site != old_site:
            site.write_text(new_site)
            site.chmod(0o644)
            site_changed = True
            validation = subprocess.run(['/usr/local/bin/caddy', 'validate', '--config', '/etc/caddy/Caddyfile', '--adapter', 'caddyfile'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if validation.returncode:
                (backup / 'caddy-validation.log').write_bytes(validation.stdout)
                raise RuntimeError('Caddy validation failed; diagnostics retained privately')
            subprocess.check_call(['systemctl', 'reload', 'caddy'])
        pid = subprocess.check_output(['systemctl', 'show', 'remote-hosts-code-gateway.service', '--property=MainPID'], universal_newlines=True).strip().split('=', 1)[1]
        if int(pid) <= 1 or checksum(pathlib.Path('/proc') / pid / 'exe') != args.sha256:
            raise RuntimeError('running gateway executable checksum mismatch')
        record.update(state='upgraded', pid=int(pid), installed_sha256=checksum(binary), health=health, caddy_override_removed=site_changed)
    except Exception as error:
        record.update(state='failed', error=str(error))
        if changed:
            try:
                if site_changed:
                    shutil.copy2(str(backup / site.name), str(site))
                atomic_copy(backup / binary.name, binary)
                subprocess.check_call(['systemctl', 'restart', 'remote-hosts-code-gateway.service'])
                if site_changed:
                    subprocess.check_call(['systemctl', 'reload', 'caddy'])
                record['rollback'] = 'restored_previous_binary_and_site; live database preserved'
            except Exception as rollback_error:
                record['rollback'] = 'failed: ' + str(rollback_error)
    args.result.parent.mkdir(parents=True, exist_ok=True)
    args.result.write_text(json.dumps(record, indent=2))
    print(json.dumps(record), flush=True)
    if record['state'] != 'upgraded':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Offline, explicit first installation of a restricted helper. Default: dry run.

No password handling, sudo invocation, download, legacy cleanup, or automatic update.
The administrator must independently verify the manifest digest printed by preparation.
"""
from __future__ import annotations
import sys
if not sys.flags.isolated:
    raise SystemExit("Run this installer with python3 -I to exclude user-controlled import paths.")
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import pwd
import shutil
import stat
import subprocess
import sys
import tempfile


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def json_snapshot(path: Path, expected_sha256: str | None = None,
                  max_bytes: int = 65536) -> tuple[dict, str]:
    """Parse and authenticate the SAME bounded bytes from one regular-file descriptor.

    Never re-open a user-writable manifest between parsing and hashing: a replacement
    could otherwise make the accepted digest refer to different installation content.
    """
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        meta = os.fstat(source.fileno())
        if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
            raise ValueError('JSON input must be a single-link regular file')
        if meta.st_size > max_bytes:
            raise ValueError('JSON input exceeds size limit')
        data = source.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError('JSON input exceeds size limit')
    actual = hashlib.sha256(data).hexdigest()
    if expected_sha256 is not None and actual != expected_sha256:
        raise ValueError('JSON snapshot checksum mismatch')
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError('JSON input must be an object')
    return value, actual


def trusted_directory(path: Path) -> None:
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Unsafe installation directory')
    for part in [*reversed(path.parents), path]:
        m = part.lstat()
        if not stat.S_ISDIR(m.st_mode) or m.st_uid != 0 or m.st_mode & 0o022:
            raise ValueError(f'Not a root-owned, non-writable directory: {part}')


def mkdir_root(path: Path, mode: int = 0o755) -> None:
    if path.exists() or path.is_symlink():
        trusted_directory(path)
        return
    mkdir_root(path.parent)
    path.mkdir(mode=mode)
    os.chown(path, 0, 0)
    path.chmod(mode)
    trusted_directory(path)


def copy_new(source: Path, destination: Path, mode: int) -> None:
    trusted_directory(destination.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(destination, flags, mode)
    with os.fdopen(fd, 'wb') as out, source.open('rb') as incoming:
        shutil.copyfileobj(incoming, out, length=1024 * 1024)
        out.flush()
        os.fchown(out.fileno(), 0, 0)
        os.fchmod(out.fileno(), mode)
        os.fsync(out.fileno())
    parent_fd = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def paths(target: str) -> dict[str, Path]:
    if target == 'macos':
        state = Path('/Library/Application Support/RemoteHostsAdmin')
        return dict(state=state, binary=Path('/Library/PrivilegedHelperTools/com.remotehosts.admin'),
                    runtime=state,
                    service=Path('/Library/LaunchDaemons/com.remotehosts.admin.plist'))
    if target == 'linux':
        state = Path('/var/lib/remote-hosts-admin')
        return dict(state=state, binary=state/'bin/remote-hosts-admin',
                    runtime=Path('/run/remote-hosts-admin'),
                    service=Path('/etc/systemd/system/remote-hosts-admin.service'))
    raise ValueError('Unsupported platform')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--grant-legacy-cleanup', action='store_true')
    parser.add_argument('--accept-manifest-sha256')
    parser.add_argument('--agent-config', type=Path, help='Existing local agent configuration; only its device_id is inspected')
    args = parser.parse_args(argv)
    bundle = Path(__file__).resolve().parent
    manifest_file = bundle/'manifest.json'
    if manifest_file.is_symlink():
        raise ValueError('Manifest must be a regular file')
    manifest, manifest_sha = json_snapshot(manifest_file)
    # This independent digest prevents accepting a replaced manifest + executable pair.
    if args.apply and args.accept_manifest_sha256 != manifest_sha:
        raise ValueError('Explicit, independently verified manifest SHA-256 is required')
    expected_names = {'remote-hosts-admin', 'policy.json', 'service.template', 'install.py', 'uninstall.py'}
    if set(manifest['files']) != expected_names:
        raise ValueError('Unexpected bundle contents')
    for name, expected in manifest['files'].items():
        path = bundle/name
        if path.is_symlink() or not path.is_file() or digest(path) != expected:
            raise ValueError(f'Bundle verification failed: {name}')
    p = paths(manifest['platform'])
    summary = dict(mode='apply' if args.apply else 'dry_run', manifest_sha256=manifest_sha,
                   platform=manifest['platform'], device_id=manifest['device_id'],
                   paths={k: str(v) for k, v in p.items()},
                   legacy_cleanup_executed=False, modifies_uu=False, modifies_existing_agent=False)
    if not args.apply:
        print(json.dumps(summary, indent=2))
        return 0
    if os.geteuid() != 0 or not args.grant_legacy_cleanup:
        raise PermissionError('Use a separately authorized administrator session and --grant-legacy-cleanup')
    actual = {'Darwin': 'macos', 'Linux': 'linux'}.get(platform.system())
    if actual != manifest['platform'] or platform.machine() != manifest['machine']:
        raise ValueError('Bundle OS/architecture does not match this host')
    if args.agent_config is None:
        raise ValueError('--agent-config is required to check the enrolled device identity')
    fd = os.open(args.agent_config, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as agent_file:
        if not stat.S_ISREG(os.fstat(agent_file.fileno()).st_mode):
            raise ValueError('Agent config must be a regular file')
        data = agent_file.read(65537)
    if len(data) > 65536 or json.loads(data).get('device_id') != manifest['device_id']:
        raise ValueError('Enrolled device identity does not match this bundle')
    del data
    account = pwd.getpwnam(manifest['account'])
    policy, _ = json_snapshot(bundle/'policy.json', manifest['files']['policy.json'])
    if (account.pw_uid, account.pw_gid, str(Path(account.pw_dir).resolve())) != (
            policy['allowed_uid'], policy['allowed_gid'], policy['home']):
        raise ValueError('Account UID/GID/home does not match the prepared grant')
    if account.pw_uid == 0 or not policy['enabled'] or policy['device_id'] != manifest['device_id']:
        raise ValueError('Invalid grant')
    # First-install only: never overwrite another installation or its receipts.
    for path in [p['binary'], p['service'], p['state']/'policy.json', p['state']/'receipts']:
        if path.exists() or path.is_symlink():
            raise FileExistsError(f'Existing installation requires explicit review: {path}')
    # Stage in a private root-owned directory, then verify again to close the source-copy race.
    with tempfile.TemporaryDirectory(prefix='remote-hosts-admin-') as temp:
        stage = Path(temp)
        for name, expected in manifest['files'].items():
            src = bundle/name
            fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as source, (stage/name).open('xb') as out:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ValueError('Bundle member changed type')
                shutil.copyfileobj(source, out, length=1024 * 1024)
            if digest(stage/name) != expected:
                raise ValueError(f'Bundle changed during staging: {name}')
        for directory in [p['binary'].parent, p['service'].parent, p['state'], p['runtime']]:
            mkdir_root(directory)
        mkdir_root(p['state']/'receipts', 0o700)
        copy_new(stage/'remote-hosts-admin', p['binary'], 0o755)
        copy_new(stage/'policy.json', p['state']/'policy.json', 0o600)
        copy_new(stage/'service.template', p['service'], 0o644)
        copy_new(stage/'uninstall.py', p['state']/'uninstall.py', 0o700)
        safe_env = dict(PATH='/usr/bin:/bin:/usr/sbin:/sbin', LC_ALL='C')
        if actual == 'macos':
            subprocess.run(['/bin/launchctl', 'bootstrap', 'system', str(p['service'])],
                           check=True, env=safe_env, stdin=subprocess.DEVNULL, timeout=45)
        else:
            subprocess.run(['/usr/bin/systemctl', 'daemon-reload'], check=True, env=safe_env,
                           stdin=subprocess.DEVNULL, timeout=30)
            subprocess.run(['/usr/bin/systemctl', 'enable', '--now', 'remote-hosts-admin.service'],
                           check=True, env=safe_env, stdin=subprocess.DEVNULL, timeout=45)
    summary['status'] = 'installed_service_started_requires_client_and_cleanup_acceptance'
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps(dict(ok=False, error=str(error),
                              note='Do not blindly rerun after partial installation; inspect the fixed target paths.')), file=sys.stderr)
        sys.exit(2)

#!/usr/bin/env python3
"""Capture a source-only release workspace without links to mutable inputs.

This is a stable input snapshot, not a filesystem sandbox. Checks/builds run from
this copy and verify its input fingerprint before and after every release stage.
The editor's working tree may continue changing without invalidating this copy.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
import pathlib
import stat
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST = 'source-snapshot.json'
MAX_BYTES = 512 * 1024 * 1024


def verifier(root):
    spec = importlib.util.spec_from_file_location('snapshot_source_verifier', root/'scripts/check-code-source.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def identity(files, executable):
    payload = {'files': files, 'executable_files': sorted(executable)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def executable_files(root, files):
    return sorted(name for name in files if (root/name).stat().st_mode & 0o111)


def copy_checked(source, destination, expected):
    """Create a new independent regular file and hash the bytes actually copied."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(source, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(fd, 'rb') as src, destination.open('xb') as dst:
        before = os.fstat(src.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BYTES:
            raise ValueError('snapshot input must be a regular file')
        h = hashlib.sha256()
        copied = 0
        for chunk in iter(lambda: src.read(1048576), b''):
            copied += len(chunk)
            if copied > before.st_size:
                raise ValueError('source grew during capture; snapshot not published')
            h.update(chunk)
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
        os.fchmod(dst.fileno(), 0o755 if before.st_mode & 0o111 else 0o644)
        after = os.fstat(src.fileno())
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if h.hexdigest() != expected or any(getattr(before, n) != getattr(after, n) for n in fields):
            raise ValueError('source changed while copying; snapshot not published')
        return before.st_size


def create(root, destination, input_provider=None):
    root = root.resolve(strict=True)
    destination = destination.absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError('snapshot destination exists; choose a new immutable candidate')
    provider = input_provider or verifier(root).inputs
    files = provider(root)
    if not files or sum((root/name).stat().st_size for name in files) > MAX_BYTES:
        raise ValueError('source snapshot empty or exceeds 512 MiB; narrow the explicit input scope')
    executable = executable_files(root, files)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # An exclusive intent file also protects concurrent, cooperating publishers.
    claim = destination.with_name('.'+destination.name+'.capture')
    claim_fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(claim_fd)
    try:
        with tempfile.TemporaryDirectory(prefix='.capturing-', dir=destination.parent) as tmp:
            stage = pathlib.Path(tmp)
            total = 0
            for name, expected in files.items():
                relative = pathlib.PurePosixPath(name)
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError('snapshot input outside repository')
                source = root/name
                if any(p.is_symlink() for p in (source, *source.parents) if p != root.parent):
                    # root is already canonical; parent aliases within inputs are rejected.
                    raise ValueError('symlink input is not an immutable repository file')
                total += copy_checked(source, stage/name, expected)
            if provider(root) != files or executable_files(root, files) != executable:
                raise ValueError('source changed during capture; retry after completing the edit')
            if provider(stage) != files:
                raise ValueError('snapshot input inventory differs from source')
            manifest = {'schema_version': 1, 'state': 'captured', 'source_root': str(root),
                        'snapshot_id': identity(files, executable),
                        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        'source_inputs': files, 'executable_files': executable,
                        'file_count': len(files), 'bytes': total,
                        'copy_semantics': 'independent regular files; no hard links or symlinks',
                        'scope': 'declared repository inputs; runtime credentials and build outputs excluded'}
            with (stage/MANIFEST).open('x') as f:
                json.dump(manifest, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            if destination.exists() or destination.is_symlink():
                raise FileExistsError('snapshot destination appeared during capture')
            stage.rename(destination)
    finally:
        claim.unlink(missing_ok=True)
    return manifest


def check(root, input_provider=None):
    root = root.resolve(strict=True)
    manifest = json.loads((root/MANIFEST).read_text())
    provider = input_provider or verifier(root).inputs
    files = provider(root)
    executable = executable_files(root, files)
    if (manifest.get('state') != 'captured' or manifest.get('schema_version') != 1
            or files != manifest.get('source_inputs')
            or executable != manifest.get('executable_files')
            or identity(files, executable) != manifest.get('snapshot_id')):
        raise ValueError('snapshot changed; do not reuse its verification or build receipts')
    return manifest


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--destination', type=pathlib.Path)
    p.add_argument('--check', type=pathlib.Path)
    args = p.parse_args()
    if bool(args.destination) == bool(args.check):
        p.error('supply exactly one of --destination or --check')
    result = check(args.check) if args.check else create(ROOT, args.destination)
    print(json.dumps({k: result[k] for k in ('state','snapshot_id','file_count','bytes')})
          if args.check else json.dumps({'snapshot_root': str(args.destination.absolute()),
                                        **{k: result[k] for k in ('state','snapshot_id','file_count','bytes')}}))


if __name__ == '__main__':
    main()

"""Leased, reusable build checkout. It is not the editable repository or a sandbox.

Only declared snapshot files are synchronized. Identical files keep their mtime,
so Cargo can reuse a stable checkout path without modifying frozen candidates.
"""
import contextlib
import hashlib
import json
import os
import pathlib
import stat
import tempfile
import time

import source_snapshot


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = pathlib.Path(stream.name)
        try:
            stream.write((json.dumps(value, indent=2)+'\n').encode())
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def no_links(path):
    path = pathlib.Path(path).absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('build slot paths must not contain symlinks')
    return path


class SlotBusy(RuntimeError):
    def __init__(self, owner):
        self.owner = owner
        super().__init__('build_slot_busy: observe the original report; do not start another build')


@contextlib.contextmanager
def lease(directory, report, snapshot_id):
    if os.name != 'posix':
        raise RuntimeError('build slot locking currently requires POSIX')
    import fcntl
    directory = no_links(directory)
    directory.mkdir(parents=True, exist_ok=True)
    marker = no_links(directory/'slot.json')
    no_links(directory/'owner.json')
    # Refuse to adopt a pre-existing populated directory owned by someone else.
    if not marker.exists() and any(p.name != '.lock' for p in directory.iterdir()):
        raise ValueError('build slot directory is not managed by this runner')
    fd = os.open(directory/'.lock', os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(fd, 'r+b') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                owner = json.loads((directory/'owner.json').read_text())
            except (OSError, ValueError):
                owner = {'state': 'owner_not_yet_published'}
            raise SlotBusy(owner) from None
        if marker.exists():
            if json.loads(marker.read_text()) != {'schema_version': 1, 'purpose': 'remote-hosts-release-slot'}:
                raise ValueError('build slot identity mismatch')
        else:
            atomic_json(marker, {'schema_version': 1, 'purpose': 'remote-hosts-release-slot'})
        prior_owner = directory/'owner.json'
        if prior_owner.exists():
            previous = json.loads(prior_owner.read_text())
            if previous.get('state') != 'released':
                raise ValueError('build_slot_recovery_required: previous owner did not confirm cleanup; retain its report and inspect owned child processes before reuse')
        owner = {'pid': os.getpid(), 'report': str(pathlib.Path(report).absolute()),
                 'snapshot_id': snapshot_id, 'started_at': int(time.time()), 'state': 'running'}
        atomic_json(directory/'owner.json', owner)
        try:
            yield directory/'checkout'
        finally:
            owner.update(state='released', released_at=int(time.time()))
            atomic_json(directory/'owner.json', owner)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(1048576), b''):
            h.update(part)
    return h.hexdigest()


def synchronize(snapshot, checkout):
    """Must be called under lease. Do not repair/erase an unexpectedly dirty slot."""
    snapshot = no_links(snapshot).resolve(strict=True)
    checkout = no_links(checkout)
    if checkout == snapshot or checkout in snapshot.parents or snapshot in checkout.parents:
        raise ValueError('build checkout and frozen snapshot must be disjoint')
    manifest = source_snapshot.check(snapshot)
    previous = None
    if checkout.exists():
        if not checkout.is_dir():
            raise ValueError('checkout is not a directory')
        if any(checkout.iterdir()):
            previous = source_snapshot.check(checkout)
    else:
        checkout.mkdir(parents=True)
    old = previous['source_inputs'] if previous else {}
    files = manifest['source_inputs']
    executable = set(manifest['executable_files'])
    changed = [n for n, h in files.items() if old.get(n) != h
               or bool((checkout/n).stat().st_mode & 0o111) != (n in executable)]
    removed = sorted(old.keys()-files.keys())
    # Only files in the previously verified inventory can be removed. Extra input
    # files cause check(checkout) above to fail rather than being silently deleted.
    for name in sorted(removed, key=len, reverse=True):
        (checkout/name).unlink()
    for name in sorted(changed):
        destination = checkout/name
        relative = pathlib.PurePosixPath(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('invalid declared snapshot path')
        if destination.exists() and not stat.S_ISREG(destination.stat().st_mode):
            raise ValueError('build input is not a regular file')
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.sync-', dir=destination.parent) as tmp:
            temporary = pathlib.Path(tmp)/'input'
            source_snapshot.copy_checked(snapshot/name, temporary, files[name])
            os.replace(temporary, destination)
    # Once installed, verify both sides. Any interrupted/dirty sync is unusable,
    # never a partially valid source tree. A later retry must reconcile it first.
    source_snapshot.check(snapshot)
    atomic_json(checkout/source_snapshot.MANIFEST, manifest)
    installed = source_snapshot.check(checkout)
    if installed['snapshot_id'] != manifest['snapshot_id']:
        raise ValueError('build checkout does not match frozen inputs')
    return {'snapshot_id': manifest['snapshot_id'], 'files': len(files),
            'written_files': len(changed), 'preserved_files': len(files)-len(changed),
            'removed_files': len(removed), 'checkout': str(checkout),
            'cache_semantics': 'unchanged bytes and executable modes keep mtime; no hardlinks'}


def cpu_time(text):
    days = 0.0
    if '-' in text:
        day, text = text.split('-', 1)
        days = float(day)*86400
    parts = [float(x) for x in text.split(':')]
    if not 1 <= len(parts) <= 3 or any(x < 0 for x in parts):
        raise ValueError('invalid process CPU time')
    total = 0.0
    for value in parts:
        total = total*60+value
    return days+total


def descendants(table, pid):
    """Parse only process ids, parent ids and CPU counters, never command arguments."""
    rows = {}
    for line in table.splitlines():
        values = line.split()
        if len(values) != 3:
            continue
        try:
            child, parent = int(values[0]), int(values[1])
            rows[child] = (parent, cpu_time(values[2]))
        except ValueError:
            continue
    selected = {pid}
    while True:
        following = selected | {child for child, (parent, _) in rows.items() if parent in selected}
        if following == selected:
            break
        selected = following
    return {child: rows[child][1] for child in selected if child in rows}

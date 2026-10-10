"""Observe owned POSIX build processes without a total wall-clock deadline.

Compiler output goes directly to an exclusive persistent file. Status heartbeats
never count as build progress. Inactivity needs unchanged output, no CPU or
process activity, and successful process observations; observation errors have
their own failure boundary. Only process groups created by this attempt are
cleaned up, and the original child return code survives supervision failures.
"""
import datetime
import os
import pathlib
import signal
import subprocess
import time

import build_slot


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def process_table():
    value = subprocess.run(
        ['ps', '-axo', 'pid=,ppid=,pgid=,state=,time=,comm='],
        capture_output=True, text=True, timeout=10)
    if value.returncode != 0:
        raise RuntimeError('process_observation_failed')
    result = {}
    for line in value.stdout.splitlines():
        fields = line.split(None, 5)
        if len(fields) != 6:
            continue
        try:
            pid, parent, group = map(int, fields[:3])
            result[pid] = {'parent': parent, 'group': group, 'state': fields[3],
                           'cpu': build_slot.cpu_time(fields[4]),
                           'name': pathlib.Path(fields[5]).name.strip('()')}
        except ValueError:
            continue
    return result


def descendants(rows, pid):
    selected = {pid}
    while True:
        more = selected | {p for p, r in rows.items() if r['parent'] in selected}
        if more == selected:
            return {p: rows[p] for p in selected if p in rows}
        selected = more


def live_groups(groups):
    rows = process_table()
    return sorted({r['group'] for r in rows.values()
                   if r['group'] in groups and not r['state'].startswith('Z')})


def stop_owned(process, groups, grace=8, kill_grace=5):
    """Reap the leader and all observed owned groups, even after leader exit."""
    groups = set(groups) | {process.pid}
    groups.discard(os.getpgrp())
    groups = {g for g in groups if g > 1}
    errors = []

    def send(values, sig):
        for group in values:
            try:
                os.killpg(group, sig)
            except ProcessLookupError:
                pass
            except OSError as error:
                errors.append(type(error).__name__)

    send(groups, signal.SIGTERM)
    remaining = sorted(groups)
    for duration, sig in ((grace, None), (kill_grace, signal.SIGKILL)):
        if sig is not None:
            send(remaining, sig)
        deadline = time.monotonic() + duration
        while True:
            process.poll()
            try:
                remaining = live_groups(groups)
            except (OSError, RuntimeError, subprocess.TimeoutExpired):
                # An unavailable observer cannot prove cleanup succeeded.
                remaining = []
                for group in groups:
                    try:
                        os.killpg(group, 0)
                        remaining.append(group)
                    except ProcessLookupError:
                        pass
                    except OSError as error:
                        remaining.append(group)
                        errors.append(type(error).__name__)
            if not remaining and process.returncode is not None:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        if not remaining and process.returncode is not None:
            break
    return {'state': 'complete' if not remaining and process.returncode is not None else 'unconfirmed',
            'remaining_groups': remaining, 'errors': errors,
            'exit_code': process.returncode}


def run(argv, root, path, env, idle_timeout=900, on_update=None,
        observed_log=None, exclude_root_cpu=False, poll_interval=3):
    """An idle budget resets on actual work; elapsed duration never stops a job."""
    if idle_timeout <= 0 or poll_interval <= 0 or os.name != 'posix':
        raise ValueError('positive inactivity policy and POSIX builder required')
    path = pathlib.Path(path)
    started = time.monotonic()
    last_progress = started
    progress_at = stamp()
    previous_cpu, previous_members = {}, set()
    previous_log, previous_size = None, -1
    groups = set()
    process = None
    cleanup = {'state': 'not_required', 'remaining_groups': []}
    result = {'state': 'starting', 'exit_code': None, 'raw_exit_code': None,
              'log': str(path), 'idle_timeout_seconds': idle_timeout,
              'wall_timeout_seconds': None, 'output_complete': False,
              'output_truncated': False, 'supervision_protocol': 2}
    observer_error_since = None

    def publish(activity, size, cpu_delta, members, diagnostic=None, gate=None):
        now = time.monotonic()
        result.update(activity=activity, stage_elapsed_seconds=round(now-started, 3),
                      seconds_since_observed_progress=round(now-last_progress, 3),
                      log_bytes=size, cpu_delta_seconds=round(cpu_delta, 4),
                      child_processes_observed=len(members),
                      heartbeat={'observed_at': stamp(), 'observer_state': 'error' if diagnostic else 'ok',
                                 'error_type': diagnostic},
                      progress={'last_observed_at': progress_at, 'log_bytes': size,
                                'cpu_delta_seconds': round(cpu_delta, 4)})
        if gate is not None:
            result['verification_gate'] = gate
        if on_update:
            on_update(dict(result))

    try:
        with path.open('xb', buffering=0) as output:
            process = subprocess.Popen(argv, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                       stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            groups.add(process.pid)
            result.update(state='running', process_id=process.pid, process_group=process.pid)
            while True:
                try:
                    process.wait(timeout=min(poll_interval, max(0.05, idle_timeout/4)))
                except subprocess.TimeoutExpired:
                    pass
                selected = {}
                diagnostic = None
                try:
                    selected = descendants(process_table(), process.pid)
                    groups.update(r['group'] for r in selected.values()
                                  if r['group'] > 1 and r['group'] != os.getpgrp())
                    observer_error_since = None
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    diagnostic = type(error).__name__
                    if observer_error_since is None:
                        observer_error_since = time.monotonic()
                observed, gate = observed_log() if observed_log else (path, None)
                observed = pathlib.Path(observed)
                result['observed_log'] = str(observed)
                size = observed.stat().st_size
                with observed.open('rb') as source:
                    source.seek(max(0, size-4096))
                    tail = source.read().decode('utf-8', errors='replace')
                workers = {p: r for p, r in selected.items()
                           if r['name'] != 'ps' and not r['state'].startswith('Z')
                           and not (exclude_root_cpu and p == process.pid)}
                cpus = {p: r['cpu'] for p, r in workers.items()}
                delta = sum(max(0, value-previous_cpu.get(p, value)) for p, value in cpus.items())
                members = set(workers)
                output_advanced = str(observed) != previous_log or size != previous_size
                topology_advanced = not diagnostic and members != previous_members
                lock_wait = 'Blocking waiting for file lock' in tail and len(workers) <= 1
                cpu_advanced = delta > 0 and not lock_wait
                if output_advanced or cpu_advanced or topology_advanced:
                    last_progress, progress_at = time.monotonic(), stamp()
                activity = ('cpu_active' if cpu_advanced else 'output_advanced' if output_advanced
                            else 'process_activity' if topology_advanced else 'cargo_lock_wait_hint'
                            if lock_wait else 'observation_unavailable' if diagnostic
                            else 'quiet_not_proven_stalled')
                publish(activity, size, delta, members, diagnostic, gate)
                previous_cpu, previous_members = cpus, members
                previous_log, previous_size = str(observed), size
                if process.returncode is not None:
                    result.update(state='finished', exit_code=process.returncode,
                                  raw_exit_code=process.returncode)
                    break
                idle = time.monotonic()-last_progress
                if diagnostic and observer_error_since is not None and idle >= min(idle_timeout, 60):
                    result.update(state='supervision_failed', failure_type='ProcessObservationUnavailable',
                                  stop_evidence={'idle_seconds': idle, 'observer_error': diagnostic})
                    break
                if not diagnostic and idle >= idle_timeout:
                    result.update(state='stalled', failure_type='ConfirmedBuildInactivity',
                                  stop_evidence={'idle_seconds': idle, 'unchanged_log_bytes': size,
                                                 'cpu_delta_seconds': delta,
                                                 'process_states': {str(p): r['state'] for p, r in workers.items()}})
                    break
            if result['state'] != 'finished':
                cleanup = stop_owned(process, groups)
            else:
                remaining = live_groups(groups)
                if remaining:
                    result.update(state='supervision_failed', failure_type='OwnedChildrenAfterLeaderExit')
                    cleanup = stop_owned(process, groups)
            output.flush()
            os.fsync(output.fileno())
    except BaseException as error:
        result.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'start_or_collection_failed',
                      failure_type=type(error).__name__)
        if process is not None:
            cleanup = stop_owned(process, groups)
    finally:
        # Never replace a collected child exit with None after another error.
        if process is not None:
            process.poll()
            result.update(exit_code=process.returncode, raw_exit_code=process.returncode)
        result.update(cleanup=cleanup, elapsed_seconds=round(time.monotonic()-started, 3),
                      completed_at=stamp(), output_complete=bool(process is not None
                      and process.returncode is not None and path.is_file()
                      and cleanup['state'] in ('complete', 'not_required')))
        receipt = path.with_name(path.name+'.exit.json')
        result['exit_receipt'] = str(receipt)
        if on_update:
            try:
                on_update(dict(result))
            except BaseException as error:
                result.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'supervision_failed',
                              publication_error=type(error).__name__)
        try:
            build_slot.atomic_json(receipt, result)
        except OSError as error:
            result.update(state='supervision_failed', exit_receipt_error=type(error).__name__)
    return result

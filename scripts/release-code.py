#!/usr/bin/env python3
"""Verify/build/package one frozen candidate in a stable leased checkout.

No deployment, credentials, service restart, git reset or implicit test-result reuse.
Status is durable and queryable without spawning another build. Only selected
fixed commands run; the source itself is executable input, not a sandbox.
"""
import argparse
import datetime
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid

import build_slot
import source_snapshot

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text())


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def current_status(report):
    result = load(report)
    result['observation'] = {'observed_at': stamp(), 'mutates_build': False,
                             'seconds_since_update': max(0, time.time()-result.get('updated_epoch', time.time()))}
    return result


def process_cpu(pid):
    try:
        p = subprocess.run(['ps', '-axo', 'pid=,ppid=,time='], text=True, capture_output=True, timeout=3)
        return build_slot.descendants(p.stdout, pid) if p.returncode == 0 else {}
    except (OSError, subprocess.TimeoutExpired):
        return {}


def stop_owned(process):
    # The verifier handles SIGTERM and stops its own nested process group.
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def progress_log(name, checkout, directory, fallback):
    """Observe the actual Cargo log, not only the verifier's stage summaries."""
    if name != 'verification':
        return fallback, None
    try:
        proof = load(directory/'verification.json')
        active = [(key, value) for key, value in proof.get('checks', {}).items()
                  if value.get('state') == 'running']
        if active:
            key, value = active[-1]
            path = checkout/value['log']
            if path.resolve().is_relative_to(checkout.resolve()) and path.is_file():
                return path, key
        return fallback, proof.get('state')
    except (OSError, ValueError, KeyError, TypeError):
        return fallback, None


def run_stage(name, argv, checkout, directory, report, state, env, timeout=1800):
    path = directory/(name+'.log')
    started = time.monotonic()
    check = {'state': 'running', 'started_at': stamp(), 'log': str(path), 'exit_code': None}
    state['stages'][name] = check
    state.update(state='running', phase=name, phase_started_at=stamp(), activity='starting')

    def publish():
        state.update(updated_at=stamp(), updated_epoch=time.time())
        build_slot.atomic_json(report, state)

    publish()
    print(json.dumps({'phase': name, 'state': 'running', 'report': str(report)}), flush=True)
    last_size = 0
    last_log = None
    previous_cpu = {}
    last_progress = started
    with path.open('xb') as output:
        process = subprocess.Popen(argv, cwd=checkout, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        state['child_pid'] = process.pid
        publish()
        try:
            while process.poll() is None:
                remaining = timeout-(time.monotonic()-started)
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(name, timeout)
                try:
                    process.wait(timeout=min(3, remaining))
                except subprocess.TimeoutExpired:
                    pass
                observed_log, gate = progress_log(name, checkout, directory, path)
                size = observed_log.stat().st_size
                output_advanced = size != last_size or observed_log != last_log
                cpus = process_cpu(process.pid)
                active_cpu = sum(max(0, value-previous_cpu.get(pid, value)) for pid, value in cpus.items())
                with observed_log.open('rb') as source:
                    source.seek(max(0, size-4096))
                    tail = source.read().decode('utf-8', errors='replace')
                if active_cpu > 0.01:
                    activity = 'cpu_active'
                elif output_advanced:
                    activity = 'output_advanced'
                elif 'Blocking waiting for file lock' in tail:
                    activity = 'cargo_lock_wait_hint'
                else:
                    activity = 'quiet_not_proven_stalled'
                if active_cpu > 0.01 or output_advanced:
                    last_progress = time.monotonic()
                state.update(activity=activity, child_processes_observed=len(cpus),
                             stage_elapsed_seconds=round(time.monotonic()-started, 3),
                             seconds_since_observed_progress=round(time.monotonic()-last_progress, 3),
                             log_bytes=size, observed_log=str(observed_log), cpu_delta_seconds=round(active_cpu, 3))
                if gate is not None:
                    state['verification_gate'] = gate
                publish()
                previous_cpu, last_size, last_log = cpus, size, observed_log
            check.update(state='finished', exit_code=process.returncode)
        except (Exception, KeyboardInterrupt) as error:
            stop_owned(process)
            check.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed',
                         exit_code=process.returncode, failure_type=type(error).__name__)
            raise
        finally:
            check.update(elapsed_seconds=round(time.monotonic()-started, 3), sha256=build_slot.digest(path))
            state.pop('child_pid', None)
            publish()
    if process.returncode != 0:
        raise RuntimeError('stage_failed:'+name+'; inspect saved log, do not duplicate the build')
    source_snapshot.check(checkout)
    print(json.dumps({'phase': name, 'state': 'finished', 'elapsed_seconds': check['elapsed_seconds']}), flush=True)


def run_pipeline(snapshot, report, slot, verify_only=False):
    snapshot = snapshot.resolve(strict=True)
    report = report.absolute()
    manifest = source_snapshot.check(snapshot)
    version = tomllib.loads((snapshot/'crates/remote-hosts-code/Cargo.toml').read_text())['package']['version']
    report.parent.mkdir(parents=True, exist_ok=True)
    if report.exists():
        previous = load(report)
        if (previous.get('snapshot_id') != manifest['snapshot_id']
                or previous.get('verify_only') != verify_only):
            raise ValueError('report belongs to different inputs or mode; never overwrite')
        print(json.dumps({'state': 'existing_attempt', 'original_state': previous['state'],
                          'report': str(report), 'action': 'observe_original; no build started'}), flush=True)
        return previous
    state = {'schema_version': 1, 'version': version, 'state': 'preparing', 'phase': 'lease',
             'snapshot_id': manifest['snapshot_id'], 'snapshot': str(snapshot), 'verify_only': verify_only,
             'started_at': stamp(), 'pid': os.getpid(), 'stages': {}, 'deployed': False}
    # Atomic claim prevents racing requests with the same report id.
    with report.open('x') as stream:
        json.dump(state, stream, indent=2)
    try:
        with build_slot.lease(slot, report, manifest['snapshot_id']) as checkout:
            state['synchronization'] = build_slot.synchronize(snapshot, checkout)
            if not verify_only and (checkout/'dist'/('remote-hosts-code-'+version)).exists():
                raise FileExistsError('release_version_already_packaged; observe its original report or use a new version')
            directory = report.parent/(report.stem+'-logs')
            directory.mkdir(exist_ok=False)
            state['execution_root'] = str(checkout)
            env = dict(os.environ, CARGO_TERM_COLOR='never')
            # Stable path per slot; unrelated editor builds cannot overwrite these
            # final binary filenames in the user's globally configured target-dir.
            env['CARGO_TARGET_DIR'] = str(slot/'cargo-target')
            state['cargo_target_dir'] = env['CARGO_TARGET_DIR']
            state['toolchain'] = subprocess.check_output(['rustc', '-vV'], cwd=checkout, env=env, text=True, timeout=10).strip()
            run_stage('verification', [sys.executable, 'scripts/check-code-source.py', '--report', str(directory/'verification.json')],
                      checkout, directory, report, state, env)
            proof = load(directory/'verification.json')
            verifier = source_snapshot.verifier(checkout)
            if not verifier.receipt_current(proof, checkout):
                raise ValueError('source verification failed or is no longer current')
            state['verification'] = {'path': str(directory/'verification.json'), 'sha256': build_slot.digest(directory/'verification.json'),
                                     'tests': proof['functional_tests']}
            if not verify_only:
                for name, args in [('macos_release', ['cargo', 'build', '-p', 'remote-hosts-code', '--release', '--locked']),
                                   ('linux_release', ['cargo', 'zigbuild', '-p', 'remote-hosts-code', '--release', '--locked', '--target', 'x86_64-unknown-linux-musl']),
                                   ('windows_release', ['cargo', 'xwin', 'build', '-p', 'remote-hosts-code', '--release', '--locked', '--target', 'x86_64-pc-windows-msvc'])]:
                    run_stage(name, args, checkout, directory, report, state, env)
                run_stage('package', [sys.executable, 'scripts/package-code-release.py', '--version', version,
                                     '--verification', str(directory/'verification.json')], checkout, directory, report, state, env)
                release = checkout/'dist'/('remote-hosts-code-'+version)
                packaged = load(release/'manifest.json')
                if packaged['source_inputs'] != manifest['source_inputs']:
                    raise ValueError('packaged source differs from verified snapshot')
                for filename, info in packaged['artifacts'].items():
                    if pathlib.Path(filename).name != filename or build_slot.digest(release/filename) != info['sha256']:
                        raise ValueError('packaged artifact checksum mismatch')
                state['package'] = {'path': str(release), 'manifest_sha256': build_slot.digest(release/'manifest.json'),
                                    'artifacts': packaged['artifacts']}
            source_snapshot.check(snapshot)
            source_snapshot.check(checkout)
            state.update(state='passed', phase='finished', source_inputs_unchanged=True)
    except build_slot.SlotBusy as error:
        state.update(state='busy', phase='lease', owner=error.owner,
                     next_action='observe owner.report; this attempt did not start compilation')
    except (Exception, KeyboardInterrupt) as error:
        state.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed',
                     failure_type=type(error).__name__, next_action='inspect the saved stage log; do not rerun an unknown operation')
        if isinstance(error, (ValueError, FileExistsError, RuntimeError)):
            state['failure_detail'] = str(error)[:400]
    finally:
        state.update(updated_at=stamp(), updated_epoch=time.time())
        build_slot.atomic_json(report, state)
    print(json.dumps({k: state.get(k) for k in ('state', 'version', 'phase', 'snapshot_id', 'verification', 'package', 'next_action')}), flush=True)
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snapshot', type=pathlib.Path)
    parser.add_argument('--report', type=pathlib.Path, required=True)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    if args.status:
        print(json.dumps(current_status(args.report)), flush=True)
        return
    if not args.snapshot or os.name != 'posix':
        parser.error('a snapshot and POSIX build host are required')
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    result = run_pipeline(args.snapshot, args.report, ROOT/'target/release-slot', args.verify_only)
    if result['state'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

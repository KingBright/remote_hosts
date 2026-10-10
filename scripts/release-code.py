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
import build_process
import source_snapshot

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text())


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def prepare_build_limits(resource_api=None, platform=None):
    """Raise only the builder's soft limit before expensive compilation.

    macOS launchd commonly supplies 256 descriptors, which is insufficient for
    large Zig link steps. Never alter the system setting or hard resource limit.
    """
    if (platform or sys.platform) != 'darwin':
        return {'state': 'not_required', 'scope': 'builder_process_tree'}
    if resource_api is None:
        import resource as resource_api
    soft, hard = resource_api.getrlimit(resource_api.RLIMIT_NOFILE)
    minimum = 4096
    if soft != resource_api.RLIM_INFINITY and soft < minimum:
        if hard != resource_api.RLIM_INFINITY and hard < minimum:
            raise ValueError('build_fd_limit_insufficient: hard limit below 4096; no compiler started')
        resource_api.setrlimit(resource_api.RLIMIT_NOFILE, (minimum, hard))
    after, after_hard = resource_api.getrlimit(resource_api.RLIMIT_NOFILE)
    if after_hard != hard or (after != resource_api.RLIM_INFINITY and after < minimum):
        raise ValueError('build_fd_limit_unconfirmed; no compiler started')
    return {'state': 'ready', 'before_soft': soft, 'after_soft': after,
            'hard': hard, 'scope': 'builder_process_tree', 'global_limits_changed': False}


def current_status(report):
    report = pathlib.Path(report)
    if not report.exists():
        launch = report.with_name(report.stem+'-launch.json')
        result = load(launch)
        result['phase'] = 'worker_startup'
        result['observation'] = {'observed_at': stamp(), 'mutates_build': False}
        return result
    result = load(report)
    result['observation'] = {'observed_at': stamp(), 'mutates_build': False,
                             'seconds_since_update': max(0, time.time()-result.get('updated_epoch', time.time()))}
    return result


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
    """Observe durable output and raw exit; timeout bounds inactivity only."""
    path = directory/(name+'.log')
    check = {'state': 'running', 'started_at': stamp(), 'log': str(path), 'exit_code': None}
    state['stages'][name] = check
    state.update(state='running', phase=name, phase_started_at=stamp(), activity='starting')

    def progress(value):
        check.update(value)
        state.update({key: value[key] for key in
                      ('activity', 'stage_elapsed_seconds', 'seconds_since_observed_progress',
                       'log_bytes', 'cpu_delta_seconds', 'child_processes_observed',
                       'heartbeat', 'progress', 'verification_gate', 'observed_log') if key in value})
        if value.get('process_id'):
            state['child_pid'] = value['process_id']
        state.update(updated_at=stamp(), updated_epoch=time.time())
        build_slot.atomic_json(report, state)

    build_slot.atomic_json(report, state)
    print(json.dumps({'phase': name, 'state': 'running', 'report': str(report)}), flush=True)
    outcome = build_process.run(
        argv, checkout, path, env, idle_timeout=timeout, on_update=progress,
        observed_log=lambda: progress_log(name, checkout, directory, path),
        exclude_root_cpu=name == 'verification')
    check.update(outcome, sha256=build_slot.digest(path) if path.is_file() else None)
    state.pop('child_pid', None)
    state.update(updated_at=stamp(), updated_epoch=time.time())
    build_slot.atomic_json(report, state)
    print(json.dumps({'phase': name, 'state': check['state'], 'exit_code': check['exit_code'],
                      'output_complete': check['output_complete'], 'exit_receipt': check['exit_receipt'],
                      'elapsed_seconds': check['elapsed_seconds']}), flush=True)
    if outcome['state'] == 'interrupted':
        raise KeyboardInterrupt
    if outcome['state'] != 'finished' or outcome['exit_code'] != 0 or not outcome['output_complete']:
        raise RuntimeError('stage_failed:'+name+'; inspect saved log, do not duplicate the build')
    source_snapshot.check(checkout)


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
            state['build_process_limits'] = prepare_build_limits()
            build_slot.atomic_json(report, state)
            state['synchronization'] = build_slot.synchronize(snapshot, checkout)
            if not verify_only and (checkout/'dist'/('remote-hosts-code-'+version)).exists():
                raise FileExistsError('release_version_already_packaged; observe its original report or use a new version')
            directory = report.parent/(report.stem+'-logs')
            directory.mkdir(exist_ok=False)
            state['execution_root'] = str(checkout)
            env = dict(os.environ, CARGO_TERM_COLOR='never', PYTHONUNBUFFERED='1', CARGO_BUILD_JOBS='1')
            # Stable path per slot; unrelated editor builds cannot overwrite these
            # final binary filenames in the user's globally configured target-dir.
            env['CARGO_TARGET_DIR'] = str(slot/'cargo-target')
            state['cargo_target_dir'] = env['CARGO_TARGET_DIR']
            # Cold toolchain startup on a busy workstation is not a lease failure.
            state.update(phase='toolchain_probe', state='running')
            build_slot.atomic_json(report, state)
            state['toolchain'] = subprocess.check_output(['rustc', '-vV'], cwd=checkout, env=env, text=True, timeout=60).strip()
            run_stage('verification', [sys.executable, 'scripts/check-code-source.py', '--report', str(directory/'verification.json')],
                      checkout, directory, report, state, env, timeout=5400)
            proof = load(directory/'verification.json')
            verifier = source_snapshot.verifier(checkout)
            if not verifier.receipt_current(proof, checkout):
                raise ValueError('source verification failed or is no longer current')
            state['verification'] = {'path': str(directory/'verification.json'), 'sha256': build_slot.digest(directory/'verification.json'),
                                     'tests': proof['functional_tests']}
            # The completed same-source receipt supersedes the last sampled
            # progress tick, which may still have said running.
            state['verification_gate'] = 'passed'
            build_slot.atomic_json(report, state)
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
                bundle = release.parent/(release.name+'-bundle.tgz')
                if not bundle.is_file():
                    raise ValueError('release bundle missing after package stage')
                state['package'] = {'path': str(release), 'manifest_sha256': build_slot.digest(release/'manifest.json'),
                                    'bundle': str(bundle), 'bundle_sha256': build_slot.digest(bundle),
                                    'bundle_size': bundle.stat().st_size, 'artifacts': packaged['artifacts']}
            source_snapshot.check(snapshot)
            source_snapshot.check(checkout)
            state.update(state='passed', phase='finished', source_inputs_unchanged=True)
    except build_slot.SlotBusy as error:
        state.update(state='busy', phase='lease', owner=error.owner,
                     next_action='observe owner.report; this attempt did not start compilation')
    except (Exception, KeyboardInterrupt) as error:
        state.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed',
                     failure_type=type(error).__name__, next_action='inspect the saved stage log; do not rerun an unknown operation')
        if isinstance(error, (ValueError, FileExistsError, RuntimeError, subprocess.TimeoutExpired)):
            state['failure_detail'] = str(error)[:400]
    finally:
        state.update(updated_at=stamp(), updated_epoch=time.time())
        build_slot.atomic_json(report, state)
    print(json.dumps({k: state.get(k) for k in ('state', 'version', 'phase', 'snapshot_id', 'verification', 'package', 'next_action')}), flush=True)
    return state


def start_pipeline(snapshot, report, verify_only=False):
    """Submit once; observation or launcher timeout never owns the worker group."""
    snapshot = snapshot.resolve(strict=True)
    report = report.absolute()
    manifest = source_snapshot.check(snapshot)
    report.parent.mkdir(parents=True, exist_ok=True)
    launch = report.with_name(report.stem+'-launch.json')
    driver_log = report.with_name(report.stem+'-driver.log')
    if report.exists() or launch.exists():
        print(json.dumps({'state': 'existing_attempt', 'report': str(report),
                          'launch_receipt': str(launch), 'action': 'observe_original; no process started'}), flush=True)
        return
    # An interrupted launch claim remains evidence; no automatic second spawn.
    intent = {'schema_version': 1, 'state': 'launching', 'snapshot_id': manifest['snapshot_id'],
              'snapshot': str(snapshot), 'report': str(report), 'driver_log': str(driver_log),
              'verify_only': verify_only, 'started_at': stamp(), 'launcher_pid': os.getpid()}
    try:
        with launch.open('x') as stream:
            json.dump(intent, stream, indent=2);stream.flush();os.fsync(stream.fileno())
    except FileExistsError:
        print(json.dumps({'state': 'existing_attempt', 'launch_receipt': str(launch),
                          'action': 'observe_original; no process started'}), flush=True)
        return
    worker_pid = None
    try:
        argv = [sys.executable, str(pathlib.Path(__file__).resolve()),
                '--snapshot', str(snapshot), '--report', str(report)]
        if verify_only:
            argv.append('--verify-only')
        # posix_spawn has no un-reaped Popen object in this short-lived launcher.
        # The new session owns its stdout file and survives a tool's observation timeout.
        with driver_log.open('xb', buffering=0) as output, open(os.devnull, 'rb') as null:
            actions = [(os.POSIX_SPAWN_DUP2, null.fileno(), 0),
                       (os.POSIX_SPAWN_DUP2, output.fileno(), 1),
                       (os.POSIX_SPAWN_DUP2, output.fileno(), 2)]
            worker_pid = os.posix_spawn(sys.executable, argv,
                                       dict(os.environ, PYTHONUNBUFFERED='1'),
                                       file_actions=actions, setsid=True)
        intent.update(state='submitted', worker_pid=worker_pid, worker_process_group=worker_pid)
        build_slot.atomic_json(launch, intent)
    except BaseException as error:
        # If spawn succeeded, retain its identity; never cancel it on observation failure.
        intent.update(state='submitted_receipt_error' if worker_pid else 'launch_failed',
                      failure_type=type(error).__name__, worker_pid=worker_pid)
        build_slot.atomic_json(launch, intent)
        raise
    print(json.dumps(intent), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snapshot', type=pathlib.Path)
    parser.add_argument('--report', type=pathlib.Path, required=True)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--start', action='store_true', help='Submit one detached, durable build; never replay an existing report/launch')
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    if args.status:
        print(json.dumps(current_status(args.report)), flush=True)
        return
    if not args.snapshot or os.name != 'posix':
        parser.error('a snapshot and POSIX build host are required')
    if args.start:
        start_pipeline(args.snapshot, args.report, args.verify_only)
        return
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    result = run_pipeline(args.snapshot, args.report, ROOT/'target/release-slot', args.verify_only)
    if result['state'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

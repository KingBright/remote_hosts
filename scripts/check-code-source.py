#!/usr/bin/env python3
"""Fixed source verification gates with incremental, input-bound receipts.

No deployment, shell command interpolation, branch change or prior-success reuse.
--check-receipt only compares inputs and never starts verification commands.
"""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
REQUIRED = {'fmt', 'clippy', 'rust_tests', 'python_tests', 'workspace'}


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def inputs(root):
    # Include path dependencies and fixtures, not only the primary crate. Added
    # and deleted files are part of identity, as are compiler/Cargo settings.
    paths = {root/n for n in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain', 'rust-toolchain.toml', 'build.rs',
                             'rustfmt.toml', '.rustfmt.toml', 'clippy.toml', '.clippy.toml') if (root/n).is_file()}
    # sqlx::migrate!("../../migrations") and root fixtures are compiled/tested
    # inputs too. Include their membership, not only .rs files inside crates.
    for subtree in ('crates', '.cargo', 'migrations', 'fixtures', 'tests', 'assets', '.sqlx'):
        if (root/subtree).is_symlink():
            raise ValueError('unsupported linked verification subtree: '+subtree)
        for directory, children, files in os.walk(root/subtree, followlinks=False):
            children[:] = sorted(n for n in children if n not in ('target', '.git', 'node_modules', '__pycache__'))
            if any((pathlib.Path(directory)/n).is_symlink() for n in children):
                raise ValueError('linked directory cannot be omitted from verification: '+str(directory))
            paths.update(pathlib.Path(directory)/n for n in files)
    paths.update((root/'scripts').glob('*.py'))
    for name in (
        'remote-hosts-service',
        'remote-hosts-systemd-service',
        'remote-hosts-service.ps1',
        'remote-hosts-code-gateway.service',
        'code-gateway.caddy',
    ):
        path = root/'scripts'/name
        if path.is_file():
            paths.add(path)
    paths.update((root/'scripts/tests').rglob('*.py'))
    result = {}
    for path in sorted(paths):
        name = str(path.relative_to(root))
        if path.is_symlink():
            # Do not claim to have verified inputs outside this repository.
            raise ValueError('unsupported symlink verification input: '+name)
        if path.is_file():
            result[name] = digest(path)
    return result


def publish(path, record):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        tmp = pathlib.Path(f.name)
        try:
            f.write((json.dumps(record, indent=2)+'\n').encode())
            f.flush()
            os.fsync(f.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)


def run_command(argv, root, path, env, timeout):
    """Bound a whole verification process group, not merely its cargo parent."""
    with path.open('xb') as output:
        process = subprocess.Popen(argv, cwd=root, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=os.name == 'posix')
        try:
            return {'exit_code': process.wait(timeout=timeout), 'state': 'finished'}
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
            def terminate(force):
                if os.name == 'posix':
                    try:
                        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                elif process.poll() is None:
                    process.kill() if force else process.terminate()
            terminate(False)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                terminate(True)
                process.wait(timeout=3)
            else:
                # A child can retain the original group after the parent exits.
                if os.name == 'posix':
                    terminate(True)
            return {'exit_code': process.returncode,
                    'state': 'interrupted' if isinstance(error, KeyboardInterrupt) else 'timed_out'}


def summarize(checks, logs):
    rust_path, py_path = logs/'rust_tests.log', logs/'python_tests.log'
    rust = rust_path.read_text(errors='replace') if rust_path.exists() else ''
    py = py_path.read_text(errors='replace') if py_path.exists() else ''
    counts = re.findall(r'test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored;', rust)
    ran = re.search(r'Ran (\d+) tests?', py)
    summary = re.search(r'^(?:FAILED|OK)(?: \(([^\n]*)\))?$', py, re.MULTILINE)
    detail = dict((k, int(v)) for k, v in re.findall(r'(failures|errors|skipped|expected failures|unexpected successes)=(\d+)', summary[1] or '')) if summary else {}
    py_failed = detail.get('failures', 0)+detail.get('errors', 0)+detail.get('unexpected successes', 0)
    py_skipped = detail.get('skipped', 0)+detail.get('expected failures', 0)
    py_passed = max(0, int(ran[1])-py_failed-py_skipped) if ran and summary else 0
    rust_passed = sum(int(p) for p, f, i in counts)
    rust_failed = sum(int(f) for p, f, i in counts)
    complete = all(checks.get(name, {}).get('state') == 'finished' and checks[name].get('exit_code') == 0
                   for name in ('rust_tests', 'python_tests'))
    return {'passed': rust_passed+py_passed, 'failed': rust_failed+py_failed,
            'rust_passed': rust_passed, 'python_passed': py_passed, 'python_failed': py_failed,
            'python_skipped': py_skipped, 'ignored_benchmarks': sum(int(i) for p, f, i in counts),
            'test_gates_completed_successfully': complete}


def snapshot_identity(root):
    path = root/'source-snapshot.json'
    if not path.exists():
        return None
    manifest = json.loads(path.read_text())
    current = inputs(root)
    executable = sorted(name for name in current if (root/name).stat().st_mode & 0o111)
    identity = hashlib.sha256(json.dumps({'files':current,'executable_files':executable},
                               sort_keys=True,separators=(',', ':')).encode()).hexdigest()
    if manifest.get('state') != 'captured' or manifest.get('snapshot_id') != identity:
        raise ValueError('snapshot input identity changed')
    return identity


def receipt_current(proof, root):
    return (proof.get('state') == 'passed' and proof.get('source_inputs_unchanged') is True
            and REQUIRED.issubset(proof.get('checks', {}))
            and all(proof['checks'][name].get('exit_code') == 0 for name in REQUIRED)
            and proof.get('functional_tests', {}).get('failed') == 0
            and proof.get('functional_tests', {}).get('test_gates_completed_successfully') is True
            and proof.get('source_inputs') == inputs(root)
            and proof.get('snapshot_id') == snapshot_identity(root))


def run_verification(root, report, gates=None, timeout=900):
    report = report.resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    # Claim destination before any expensive command; never discover a collision
    # only after rerunning all tests, and never overwrite an earlier receipt.
    with report.open('x') as f:
        json.dump({'state': 'preparing', 'deployed': False}, f)
    result = {'state': 'preparing', 'checks': {}, 'source_inputs': {}, 'deployed': False}
    run = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    logs = root/'target/source-verification'/run
    logs.mkdir(parents=True)
    try:
        result.update(version=tomllib.loads((root/'crates/remote-hosts-code/Cargo.toml').read_text())['package']['version'],
                      run_id=run, scope='native source verification, not deployment', source_inputs=inputs(root), state='running',
                      execution_root=str(root.resolve()), snapshot_id=snapshot_identity(root))
        publish(report, result)
        if gates is None:
            gates = [
                ('fmt', ['cargo', 'fmt', '-p', 'remote-hosts-code', '--', '--check']),
                ('clippy', ['cargo', 'clippy', '-p', 'remote-hosts-code', '--all-targets', '--locked', '--', '-D', 'warnings']),
                ('rust_tests', ['cargo', 'test', '-p', 'remote-hosts-code', '-p', 'remote-hosts-mcp', '--locked', '--no-fail-fast', '--', '--test-threads=2', '--color', 'never']),
                ('python_tests', [sys.executable, '-W', 'error::ResourceWarning', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-v']),
                ('workspace', ['cargo', 'check', '--workspace', '--locked']),
            ]
        env = dict(os.environ, CARGO_TERM_COLOR='never')
        for name, argv in gates:
            path = logs/(name+'.log')
            started = time.monotonic()
            result['checks'][name] = {'state': 'running', 'exit_code': None, 'log': str(path.relative_to(root))}
            publish(report, result)
            try:
                outcome = run_command(argv, root, path, env, timeout)
            except Exception as error:
                outcome = {'state': 'start_or_collection_failed', 'exit_code': None, 'failure_type': type(error).__name__}
            result['checks'][name].update(outcome, elapsed_seconds=time.monotonic()-started,
                                         sha256=digest(path) if path.exists() else None)
            publish(report, result)
            print(json.dumps({'gate': name, **result['checks'][name]}), flush=True)
            if outcome['state'] != 'finished' or outcome['exit_code'] != 0:
                result['state'] = 'interrupted' if outcome['state'] == 'interrupted' else 'failed'
                break
        else:
            result['state'] = 'passed'
    except (Exception, KeyboardInterrupt) as error:
        result.update(state='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed', failure_type=type(error).__name__)
    try:
        after = inputs(root)
        result['source_inputs_unchanged'] = (result['source_inputs'] == after
                                             and result.get('snapshot_id') == snapshot_identity(root))
        if not result['source_inputs_unchanged']:
            result['check_outcome_before_stale'] = result['state']
            result['state'] = 'stale'
            before = result['source_inputs']
            result['changed_inputs'] = sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
    except Exception as error:
        result.update(state='stale', source_inputs_unchanged=False, input_failure_type=type(error).__name__)
    result.update(functional_tests=summarize(result['checks'], logs), verified_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    publish(report, result)
    print(json.dumps({'state': result['state'], 'report': str(report), **result['functional_tests']}), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', type=pathlib.Path, required=True)
    parser.add_argument('--check-receipt', action='store_true')
    args = parser.parse_args()
    if args.check_receipt:
        try:
            current = receipt_current(json.loads(args.report.read_text()), ROOT)
        except Exception:
            current = False
        print(json.dumps({'state': 'current' if current else 'stale_or_invalid', 'report': str(args.report)}))
        raise SystemExit(0 if current else 1)
    if os.name == 'posix':
        def interrupted(_signal, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
    result = run_verification(ROOT, args.report)
    if result['state'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

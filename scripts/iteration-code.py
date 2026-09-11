#!/usr/bin/env python3
"""One durable entry point for validate -> push -> publish iterations.

The runner deliberately keeps production side effects behind already verified
artifacts and the existing publish-code state machine. It chooses the cheapest
safe verification profile from the changed paths, refuses source drift, pushes
validated work on main, and reuses original reports instead of spawning duplicate
builds or releases.
"""
import argparse
import datetime
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import time

import release_receipts as rr
import source_snapshot

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOC_PREFIXES = ('docs/',)
DOC_FILES = {'README.md', 'README_EN.md', '.gitignore'}


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(argv, *, check=True, capture=False, timeout=7200):
    result = subprocess.run(argv, cwd=ROOT, text=True,
                            capture_output=capture, timeout=timeout)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, argv,
                                            output=result.stdout, stderr=result.stderr)
    return result.stdout if capture else result.returncode


def changed_paths():
    tracked = run(['git', 'diff', '--name-only', '-z', 'HEAD'], capture=True).split('\0')
    untracked = run(['git', 'ls-files', '--others', '--exclude-standard', '-z'], capture=True).split('\0')
    return sorted({p for p in tracked + untracked if p})


def classify(paths):
    if not paths:
        return 'clean'
    def doc(path):
        return path in DOC_FILES or path.startswith(DOC_PREFIXES)
    if all(doc(path) for path in paths):
        return 'docs'
    if all(doc(path) or (path.startswith('scripts/') and path.endswith('.py')) for path in paths):
        return 'release_python'
    return 'runtime'


def fingerprint(paths):
    payload = []
    for name in paths:
        path = ROOT / name
        if path.is_file():
            payload.append((name, hashlib.sha256(path.read_bytes()).hexdigest()))
        else:
            payload.append((name, 'absent'))
    return hashlib.sha256(json.dumps(payload, separators=(',', ':')).encode()).hexdigest()


def check_json(paths):
    for name in paths:
        if name.endswith('.json') and (ROOT/name).is_file():
            json.loads((ROOT/name).read_text())


def audit_commit_scope(paths):
    """Reject common generated release blobs and private bearer capabilities."""
    forbidden_suffixes = ('.tgz', '.tar.gz', '.sqlite', '.sqlite3')
    bearer = re.compile(r"https://[^\s\"'<>]+/files/[0-9a-f]{24,}(?:/|$)", re.IGNORECASE)
    for name in paths:
        lower = name.lower()
        if (lower.endswith(forbidden_suffixes) or 'agent-package.tgz' in lower
                or (lower.endswith('.lock') and pathlib.PurePosixPath(name).name != 'Cargo.lock')):
            raise ValueError('generated/private release artifact must not be committed: '+name)
        path = ROOT/name
        if not path.is_file() or path.stat().st_size > 2*1024*1024:
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        if bearer.search(text):
            raise ValueError('temporary bearer file URL must not be committed: '+name)


def verify_light(profile, paths):
    audit_commit_scope(paths)
    run(['git', 'diff', '--check'])
    check_json(paths)
    if (ROOT/'docs/product/backlog.json').is_file():
        run([sys.executable, 'scripts/product-backlog.py', '--check'])
    if profile == 'release_python':
        changed_python = [name for name in paths if name.endswith('.py') and (ROOT/name).is_file()]
        if changed_python:
            run([sys.executable, '-m', 'py_compile', *changed_python])
        run([sys.executable, '-W', 'error::ResourceWarning', '-m', 'unittest',
             'discover', '-s', 'scripts/tests', '-v'])


def git_push(paths, message):
    if run(['git', 'branch', '--show-current'], capture=True).strip() != 'main':
        raise RuntimeError('iteration push requires main')
    run(['git', 'fetch', 'origin', 'main'])
    local_only, remote_only = map(int, run(['git', 'rev-list', '--left-right', '--count',
                                            'HEAD...origin/main'], capture=True).split())
    if remote_only:
        raise RuntimeError('origin/main is ahead; reconcile before automated push')
    # Stage only the exact files that were validated. -A records deletions too.
    run(['git', 'add', '-A', '--', *paths])
    staged = {p for p in run(['git', 'diff', '--cached', '--name-only', '-z'], capture=True).split('\0') if p}
    if not staged or not staged.issubset(set(paths)):
        raise RuntimeError('staged files differ from validated iteration scope')
    run(['git', 'diff', '--cached', '--check'])
    run(['git', 'commit', '-m', message])
    commit = run(['git', 'rev-parse', 'HEAD'], capture=True).strip()
    run(['git', 'push', 'origin', 'main'])
    return commit


def compact_publish_status(directory):
    path = pathlib.Path(directory)/'deployment.json'
    if not path.is_file():
        return {'state': 'not_started', 'report': str(path)}
    value = json.loads(path.read_text())
    agents = {ident: {'name': row.get('name'), 'state': row.get('state'), 'phase': row.get('phase')}
              for ident, row in value.get('agents', {}).items()}
    gateway = value.get('gateway') or {}
    return {'state': value.get('state'), 'phase': value.get('phase'),
            'version': value.get('version'), 'all_targets_accepted': value.get('all_targets_accepted', False),
            'gateway': {'state': gateway.get('state'), 'version': gateway.get('version')},
            'agents': agents, 'report': str(path)}


def save(path, state):
    state['updated_at'] = stamp()
    rr.atomic_json(path, state)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--report', type=pathlib.Path, required=True)
    p.add_argument('--status', action='store_true')
    p.add_argument('--publish-status-dir', type=pathlib.Path)
    p.add_argument('--commit-message')
    p.add_argument('--no-push', action='store_true')
    p.add_argument('--publish-config', type=pathlib.Path)
    p.add_argument('--publish-report-dir', type=pathlib.Path)
    args = p.parse_args()
    if args.no_push and args.publish_config:
        p.error('--no-push cannot be combined with publication')
    report = args.report.resolve()
    if args.status:
        value = {'state': 'not_started', 'report': str(report)} if not report.is_file() else json.loads(report.read_text())
        if args.publish_status_dir:
            value['live_publish_status'] = compact_publish_status(args.publish_status_dir)
        print(json.dumps(value)); return

    if report.exists():
        previous = json.loads(report.read_text())
        paths = previous.get('paths') or []
        initial = previous.get('input_fingerprint')
        if not paths or not initial or fingerprint(paths) != initial:
            raise SystemExit('existing iteration inputs changed; preserve the report and investigate')
        current = changed_paths()
        # Before push the same dirty scope must still be present. After a recorded
        # push, a clean tree is expected and publication can safely resume.
        pushed = previous.get('stages', {}).get('git_push', {}).get('state') == 'passed'
        if (not pushed and current != paths) or (pushed and current and not set(current).issubset(set(paths))):
            raise SystemExit('working tree diverged from the recorded iteration scope')
        state = previous
        profile = state['profile']
        if state.get('state') == 'passed':
            print(json.dumps(state)); return
    else:
        paths = changed_paths()
        profile = classify(paths)
        if profile == 'clean':
            raise SystemExit('working tree has no iteration changes')
        initial = fingerprint(paths)
        state = {'schema_version': 1, 'state': 'running', 'phase': 'preflight',
                 'profile': profile, 'paths': paths, 'input_fingerprint': initial,
                 'started_at': stamp(), 'stages': {}}
        report.parent.mkdir(parents=True, exist_ok=True); save(report, state)

    try:
        if state['stages'].get('verification', {}).get('state') != 'passed':
            state['phase'] = 'verification'; save(report, state)
            audit_commit_scope(paths)
            if profile == 'runtime':
                root = report.parent
                snapshot = root/'snapshot'
                pipeline = root/'pipeline.json'
                if not snapshot.exists():
                    source_snapshot.create(ROOT, snapshot)
                run([sys.executable, 'scripts/release-code.py', '--snapshot', str(snapshot), '--report', str(pipeline)])
                built = json.loads(pipeline.read_text())
                if built.get('state') != 'passed':
                    raise RuntimeError('full release pipeline did not pass')
                state['pipeline'] = str(pipeline)
                state['version'] = built['version']
                state['manifest_sha256'] = built['package']['manifest_sha256']
            else:
                verify_light(profile, paths)
            if fingerprint(paths) != initial:
                raise RuntimeError('iteration inputs changed during verification')
            state['stages']['verification'] = {'state': 'passed', 'profile': profile}
            save(report, state)

        if not args.no_push and state['stages'].get('git_push', {}).get('state') != 'passed':
            if not args.commit_message:
                raise ValueError('--commit-message is required unless --no-push is used')
            state['phase'] = 'git_push'; save(report, state)
            audit_commit_scope(paths)
            commit = git_push(paths, args.commit_message)
            state['stages']['git_push'] = {'state': 'passed', 'commit': commit, 'branch': 'main'}
            save(report, state)

        if args.publish_config:
            if profile != 'runtime':
                raise ValueError('publication is only valid after the full runtime release profile')
            if not args.publish_report_dir:
                raise ValueError('--publish-report-dir is required with --publish-config')
            if state['stages'].get('publication', {}).get('state') != 'passed':
                state['phase'] = 'publication'; save(report, state)
                run([sys.executable, 'scripts/publish-code.py', '--config', str(args.publish_config),
                     '--pipeline', state['pipeline'], '--report-dir', str(args.publish_report_dir),
                     '--version', state['version'], '--apply'])
                summary = compact_publish_status(args.publish_report_dir)
                if not summary.get('all_targets_accepted'):
                    state['publish_status'] = summary; save(report, state)
                    raise RuntimeError('publication incomplete; observe saved report, do not reinstall blindly')
                state['stages']['publication'] = {'state': 'passed', 'summary': summary}
                save(report, state)

        if args.no_push and state['stages'].get('git_push', {}).get('state') != 'passed':
            state.update(state='validated', phase='awaiting_push',
                         next_action='rerun this exact report without --no-push to commit and push validated inputs')
        else:
            state.pop('next_action', None)
            state.update(state='passed', phase='finished')
    except Exception as error:
        state.update(state='failed', failure_type=type(error).__name__,
                     failure_detail=str(error)[:500],
                     next_action='observe this report and original stage receipts; do not duplicate uncertain side effects')
    save(report, state)
    print(json.dumps(state, ensure_ascii=False))
    if state['state'] not in ('passed', 'validated'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()

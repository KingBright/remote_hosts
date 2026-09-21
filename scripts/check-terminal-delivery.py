#!/usr/bin/env python3
"""Bounded live acceptance using one installed Rust adapter; no service changes.

One authorized workspace per device. Fixed harmless shell probes, exact output,
original operation recovery only. Failed and slow samples remain in the report.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import statistics
import tempfile
import time

from native_release_client import NativeTransportError
from release_client import Client, evidence_is_durable


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as out:
        tmp = pathlib.Path(out.name)
        try:
            out.write((json.dumps(value, indent=2) + '\n').encode())
            out.flush()
            os.fsync(out.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--origin', required=True)
    parser.add_argument('--password-file', required=True, type=pathlib.Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--workspace', action='append', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--report', required=True, type=pathlib.Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9-]{1,40}', args.run_id):
        parser.error('run-id must be 1..40 alphanumeric/hyphen characters')
    if args.report.exists():
        parser.error('original report exists; observe its operations, do not replay')
    if not 1 <= len(args.workspace) <= 4 or len(set(args.workspace)) != len(args.workspace):
        parser.error('supply 1..4 distinct authorized workspaces')
    os.umask(0o077)
    client = Client(args.origin, args.password_file, transport='native')
    report = {'state':'running', 'version':args.version, 'run_id':args.run_id,
        'started_at':int(time.time()), 'scope':'native Rust adapter, exact bounded live samples; not statistical performance guarantees',
        'controller_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
        'cases':[], 'active_operation':None, 'calls':[], 'oauth_grant_issued':False}
    save(args.report, report)

    def call(name, arguments):
        start = time.monotonic()
        value = client.raw(name, dict(arguments, response_mode='full'))
        report['calls'].append({'tool':name, 'operation_id':value.get('operation_id'),
            'request_id':value.get('request_id'), 'elapsed_ms':round((time.monotonic()-start)*1000),
            'state':value.get('state'), 'evidence_complete':value.get('receipt',{}).get('evidence_complete'),
            'error_code':value.get('error_code')})
        if value.get('operation_id'):
            report['active_operation'] = value['operation_id']
        save(args.report, report)
        if value.get('error'):
            raise RuntimeError('tool_error:' + str(value.get('error_code', 'unknown')))
        return value

    try:
        client.login()
        report['oauth_grant_issued'] = True
        fleet = client.raw('devices_list', {})
        for workspace in args.workspace:
            device = workspace.split(':', 1)[0]
            node = next(d for d in fleet['devices'] if d['device_id'] == device)
            if not node['online'] or node['capabilities']['version'] != args.version:
                raise RuntimeError('requested_device_version_or_readiness_mismatch:' + device)
            windows = node['capabilities']['platform'] == 'windows'
            for case, exit_code in [('short', 0), ('failure', 7), ('delayed', 0)]:
                marker = 'RH-DELIVERY-' + args.run_id + '-' + case
                emit = lambda text: "Write-Output '" + text + "'" if windows else "printf '" + text + "\\n'"
                command = emit(marker)
                expected = marker + '\n'
                if case == 'failure':
                    command += '; exit 7'
                if case == 'delayed':
                    command += '; ' + ('Start-Sleep -Seconds 5' if windows else 'sleep 5') + '; ' + emit(marker+'-end')
                    expected += marker + '-end\n'
                first = len(report['calls'])
                start = time.monotonic()
                value = call('terminal_exec', {'workspace_id':workspace, 'command':command,
                    'timeout_seconds':20, 'wait_ms':1500,
                    'idempotency_key':args.run_id+'-'+device+'-'+case})
                original = value['operation_id']
                while (value.get('terminal',{}).get('exit_code') is None
                       or value.get('receipt',{}).get('evidence_complete') is not True
                       or not evidence_is_durable(value.get('receipt',{}))):
                    if time.monotonic()-start > 90:
                        raise TimeoutError('original_operation_unconfirmed:' + original)
                    value = call('operation_get', {'operation_id':original,'wait_ms':5000,'max_bytes':16384})
                    if value.get('operation_id') != original:
                        raise RuntimeError('operation_identity_changed')
                status = value['terminal']
                output = value.get('output', '').replace('\r\n','\n')
                if (status['state'] != 'exited' or value['state'] != 'exited'
                        or status['exit_code'] != exit_code or output != expected
                        or status.get('output_truncated') or status.get('output_error')
                        or status.get('output_complete') is not True or value.get('has_more')):
                    raise RuntimeError('terminal_evidence_mismatch:' + original)
                row = {'device':node['name'],'case':case,'operation_id':original,
                    'elapsed_ms':round((time.monotonic()-start)*1000),'tool_calls':len(report['calls'])-first,
                    'expected_exit_code':exit_code,'output_complete':True,'state_consistent':True,
                    'output_sha256':hashlib.sha256(output.encode()).hexdigest(),
                    'gateway_lifecycle':value.get('operation_lifecycle'),'agent_timing':value.get('timing')}
                report['cases'].append(row)
                report['active_operation'] = None
                save(args.report, report)
        report['functional_passed'] = True
        report['two_call_target_met'] = all(row['tool_calls'] <= 2 for row in report['cases'])
        report['short_latency_ms'] = [row['elapsed_ms'] for row in report['cases'] if row['case']=='short']
        report['short_median_ms'] = statistics.median(report['short_latency_ms'])
        report['absolute_latency_target_met'] = all(ms <= 5000 for ms in report['short_latency_ms'])
        report['state'] = 'passed' if report['two_call_target_met'] and report['absolute_latency_target_met'] else 'needs_improvement'
    except Exception as error:
        report.update(state='failed', error_type=type(error).__name__, error=str(error)[:300])
        if isinstance(error, NativeTransportError):
            report['original_request_recovery'] = error.requests
            report['recovery_directory'] = error.evidence_dir
    finally:
        try:
            report['cleanup_success'] = client.close()
            report['oauth_grant_revoked'] = report['oauth_grant_issued'] and client.refresh is None
        except Exception as error:
            report.update(cleanup_success=False, oauth_grant_revoked=False, cleanup_error=type(error).__name__)
        if not report['cleanup_success']:
            report['state'] = 'cleanup_unconfirmed'
        report['transport'] = client.transport_info()
        report['finished_at'] = int(time.time())
        save(args.report, report)
    print(json.dumps({'state':report['state'],'cases':report['cases'],
        'two_call_target_met':report.get('two_call_target_met'),
        'absolute_latency_target_met':report.get('absolute_latency_target_met'),
        'cleanup_success':report['cleanup_success'],'report':str(args.report)}), flush=True)
    if report['state'] == 'passed':
        return 0
    return 2 if report['state'] == 'needs_improvement' else 1


if __name__ == '__main__':
    raise SystemExit(main())

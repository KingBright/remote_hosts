"""Owner release client: OAuth bootstrap plus one persistent native Rust MCP session.

The explicit legacy mode is retained for recovery/comparison. No call falls back
from a selected native session to HTTP after an uncertain execution.
"""
import base64
import hashlib
import http.cookiejar
import json
import os
import pathlib
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request

from native_release_client import NativeSession, installed_binary


def evidence_is_durable(receipt):
    """A query trace may be unretained while its original facts are durable.

    Protocol 2 must explicitly prove the new split. Unknown or contradictory
    forms are not accepted as durable execution evidence.
    """
    if not isinstance(receipt, dict):
        return False
    protocol = receipt.get('protocol', 1)
    if type(protocol) is not int:
        return False
    if protocol == 1:
        return receipt.get('durable', True) is True
    if protocol != 2 or receipt.get('evidence_durable') is not True:
        return False
    if receipt.get('request_record_persisted') is True:
        return receipt.get('durable') is True
    return (receipt.get('request_record_persisted') is False
            and receipt.get('durable') is False
            and receipt.get('durability_scope') == 'observed_facts_only_not_this_query')


class ReleaseHttpError(RuntimeError):
    """Bounded decision fields, never upstream bodies, tokens or query strings."""
    def __init__(self, status, path, headers, body):
        edge_code = None
        declared_no_retry = False
        try:
            value = json.loads(body)
            if isinstance(value, dict):
                code = value.get('error_code')
                if type(code) is int and 1000 <= code <= 1999:
                    edge_code = code
                declared_no_retry = value.get('retryable') is False
        except (ValueError, TypeError):
            pass
        ray = headers.get('CF-Ray') if headers else None
        if not isinstance(ray, str) or not re.fullmatch(r'[A-Za-z0-9-]{1,80}', ray):
            ray = None
        self.diagnostic = {
            'http_status': status,
            'path': urllib.parse.urlsplit(path).path[:180],
            'failure_boundary': 'edge_policy' if status == 403 and edge_code else 'upstream_http',
            'edge_error_code': edge_code,
            'cf_ray': ray,
            'retryable': not declared_no_retry and status in (408, 425, 500, 502, 503, 504, 520, 522, 523, 524),
            'user_action': 'review_owner_edge_policy' if status == 403 and edge_code else 'none',
            'body_retained': False,
        }
        super().__init__('release_http_' + str(status) + '; ' + json.dumps(self.diagnostic, sort_keys=True))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class OperationIncomplete(RuntimeError):
    """A completed communication is not a completed remote transfer.

    Preserve the original operation for explicit recovery; never resume or
    resubmit it merely because a convenience caller expected a final result.
    """
    def __init__(self, value):
        self.operation_id = value.get('operation_id')
        self.state = value.get('state')
        self.next_action = value.get('next_action') or value.get('receipt', {}).get('next_action')
        self.receipt = dict(value.get('receipt') or {})
        self.resumable = value.get('resumable') is True
        super().__init__('operation_incomplete:' + str(self.operation_id)
                         + '; state=' + str(self.state)
                         + '; next_action=' + str(self.next_action))


class Client:
    def __init__(self, origin, password_file=None, access=None, *, transport=None,
                 native_binary=None, native_state_dir=None):
        u = urllib.parse.urlsplit(origin)
        if (u.scheme != 'https' or not u.hostname or u.username or u.password
                or u.query or u.fragment or u.path):
            raise ValueError('explicit HTTPS origin without path/userinfo required')
        self.origin = origin
        self.password_file = password_file
        self.access = access
        self.refresh = None
        self.sequence = 0
        mode = transport or os.environ.get('REMOTE_HOSTS_RELEASE_TRANSPORT', 'auto')
        if mode not in ('auto', 'native', 'legacy'):
            raise ValueError('invalid_release_transport')
        configured = native_binary or os.environ.get('REMOTE_HOSTS_NATIVE_ADAPTER')
        # Explicit legacy mode must not depend on an installed native runtime.
        binary = pathlib.Path(configured) if configured else (installed_binary() if mode != 'legacy' else None)
        available = binary is not None and binary.is_file() and os.access(binary, os.X_OK)
        if (mode == 'native' or (mode == 'auto' and configured)) and not available:
            raise ValueError('native_adapter_binary_unavailable')
        self.transport_mode = 'native' if mode == 'native' or (mode == 'auto' and available) else 'legacy'
        self.transport_reason = 'explicit_legacy' if mode == 'legacy' else ('canonical_native_available' if available else 'native_absent_before_any_rpc')
        self.native_binary = binary
        state = native_state_dir or os.environ.get('REMOTE_HOSTS_NATIVE_STATE_DIR')
        self.native_state_dir = pathlib.Path(state) if state else None
        self._native = None
        self._closed = False
        self.http_observations = []
        self.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.opener.addheaders = [('User-Agent', 'RemoteHosts-Release/0.5.0')]

    def call(self, path, data=None, form=False, auth=False):
        # OAuth and occasional admin/artifact requests retain the existing proxy,
        # TLS validation, cookie handling, redirect policy and response budgets.
        headers = {'Accept': 'application/json, text/event-stream'}
        if data is not None:
            headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
            data = (urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
        if auth:
            headers['Authorization'] = 'Bearer ' + self.access
        request = urllib.request.Request(self.origin + path, data=data, headers=headers)
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read(1048577)
            if len(body) > 1048576:
                raise RuntimeError('release_response_budget')
            return response.status, response.headers, body

    def parsed(self, path, data=None, form=False, auth=False):
        # Only public, side-effect-free metadata GETs may recover automatically.
        # OAuth writes and MCP requests keep their original recovery semantics.
        safe_get = data is None and path in ('/.well-known/oauth-protected-resource', '/.well-known/oauth-authorization-server', '/healthz')
        attempts = 3 if safe_get else 1
        for attempt in range(1, attempts + 1):
            status, headers, body = self.call(path, data, form, auth)
            if status in (200, 201):
                return json.loads(body)
            error = ReleaseHttpError(status, path, headers, body)
            self.http_observations.append(dict(error.diagnostic, attempt=attempt))
            self.http_observations = self.http_observations[-32:]
            if not safe_get or not error.diagnostic['retryable'] or attempt == attempts:
                raise error
            time.sleep(min(attempt, 2))
        raise AssertionError('unreachable HTTP retry state')

    def login(self):
        metadata = self.parsed('/.well-known/oauth-protected-resource')
        if metadata['resource'] != self.origin + '/mcp':
            raise ValueError('unexpected OAuth resource')
        redirect = 'https://chatgpt.com/connector_platform_oauth_redirect'
        client = self.parsed('/oauth/register', {'redirect_uris': [redirect], 'token_endpoint_auth_method': 'none'})
        verifier = secrets.token_urlsafe(48)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
        state = secrets.token_hex(24)
        query = urllib.parse.urlencode({'client_id': client['client_id'], 'redirect_uri': redirect, 'response_type': 'code', 'code_challenge': challenge, 'code_challenge_method': 'S256', 'state': state, 'resource': metadata['resource']})
        status, headers, _ = self.call('/oauth/authorize?' + query)
        if status != 200:
            raise RuntimeError('authorization_page_failed')
        nonce = headers['Set-Cookie'].split(';', 1)[0].split('=', 1)[1]
        status, headers, _ = self.call('/oauth/approve', {'nonce': nonce, 'password': pathlib.Path(self.password_file).read_text().strip()}, form=True)
        if status != 303:
            raise RuntimeError('owner_authorization_failed')
        returned = urllib.parse.parse_qs(urllib.parse.urlsplit(headers['Location']).query)
        if returned.get('iss') != [self.origin] or returned.get('state') != [state]:
            raise RuntimeError('authorization_identity_conflict')
        tokens = self.parsed('/oauth/token', {'grant_type': 'authorization_code', 'client_id': client['client_id'], 'code': returned['code'][0], 'code_verifier': verifier, 'redirect_uri': redirect, 'resource': metadata['resource']}, form=True)
        self.access, self.refresh = tokens['access_token'], tokens['refresh_token']
        self.rpc('initialize', {'protocolVersion': '2025-11-25', 'capabilities': {}, 'clientInfo': {'name': 'remote-hosts-release', 'version': '0.5.0'}})
        return self

    def rpc(self, method, params):
        if self._closed:
            raise RuntimeError('release_client_closed; no implicit reconnect')
        if self.transport_mode == 'native':
            if self._native is None:
                self._native = NativeSession(self.origin, self.access, self.native_binary, self.native_state_dir)
            return self._native.rpc(method, params)
        self.sequence += 1
        payload = {'jsonrpc': '2.0', 'id': self.sequence, 'method': method, 'params': params}
        readonly = method in ('initialize', 'tools/list') or (method == 'tools/call' and params.get('name') in ('devices_list', 'fleet_status', 'operation_get', 'task_context'))
        attempts = 5 if readonly else 1
        for attempt in range(attempts):
            try:
                value = self.parsed('/mcp', payload, auth=True)
                break
            except (TimeoutError, urllib.error.URLError, OSError):
                if attempt + 1 == attempts:
                    raise
                time.sleep(min(2 ** attempt, 8))
        if 'error' in value:
            raise RuntimeError('MCP protocol error ' + str(value['error'].get('code')))
        return value['result']

    def raw(self, name, args):
        arguments = dict(args)
        arguments.setdefault('response_mode', 'full')
        response = self.rpc('tools/call', {'name': name, 'arguments': arguments})
        value = response.get('structuredContent')
        if value is None:
            if response.get('isError'):
                raise RuntimeError('tool_error:' + name + '; inspect original bounded response')
            value = json.loads(response['content'][0]['text'])
        return value

    def tool(self, name, args, *, deadline=None):
        end = deadline if deadline is not None else time.monotonic() + 900
        value = self.raw(name, args)
        while value.get('pending'):
            if time.monotonic() > end:
                raise RuntimeError('observation_timeout:' + value['operation_id'])
            value = self.raw('operation_get', {'operation_id': value['operation_id'], 'wait_ms': 5000})
        if 'error' in value:
            raise RuntimeError(name + ':' + str(value.get('error_code', value['error'])) + '; operation=' + str(value.get('operation_id')))
        if value.get('state') in ('paused', 'awaiting_source'):
            raise OperationIncomplete(value)
        return value

    def terminal(self, ws, command, key, timeout=60):
        """Follow one operation. Full history reads are only an evidence fallback."""
        end = time.monotonic() + timeout + 60
        value = self.tool('terminal_exec', {'workspace_id': ws, 'command': command, 'idempotency_key': key, 'timeout_seconds': timeout, 'wait_ms': 1000, 'response_mode': 'full'}, deadline=end)
        ident = value.get('terminal_id') or value.get('terminal', {}).get('id')
        operation = value.get('operation_id') or ident
        if not ident:
            raise RuntimeError('terminal_identity_unconfirmed:' + str(operation))
        status = value.get('terminal', {})
        while status.get('exit_code') is None or status.get('output_complete') is not True:
            if time.monotonic() > end:
                raise RuntimeError('terminal_observation_timeout:' + ident)
            value = self.tool('operation_get', {'operation_id': operation, 'wait_ms': 5000, 'max_bytes': 65536}, deadline=end)
            status = value.get('terminal', {})
            if status.get('id', ident) != ident:
                raise RuntimeError('terminal_identity_changed:' + ident)
        if status.get('output_truncated') or status.get('output_error'):
            raise RuntimeError('terminal_evidence_incomplete:' + ident)
        if status['exit_code'] != 0 or status.get('state', 'exited') != 'exited':
            raise RuntimeError('terminal_failed:' + ident)
        if (value.get('receipt', {}).get('evidence_complete') is True
                and evidence_is_durable(value.get('receipt', {}))
                and not value.get('has_more') and not value.get('result_omitted')
                and value.get('raw_cursor_start', 0) == 0):
            text = value.get('output')
            # A complete transport receipt may still carry a compressed summary.
            # Only return exact UTF-8 bytes; otherwise recover the original log.
            if (isinstance(text, str) and value.get('output_view') in ('full', 'delta')
                    and value.get('cursor') == len(text.encode('utf-8'))):
                return text
            if text in (None, '') and value.get('cursor') == 0:
                return ''
        # Never append a bounded tail to a different prefix. Read from byte zero
        # when a preview does not prove a complete contiguous log.
        parts, cursor = [], 0
        prefix = value.get('output')
        if (value.get('has_more') and not value.get('result_omitted')
                and value.get('output_view') in ('full', 'delta')
                and value.get('raw_cursor_start', 0) == 0 and isinstance(prefix, str)
                and value.get('cursor') == len(prefix.encode('utf-8'))):
            parts, cursor = [prefix], value['cursor']
        while True:
            if time.monotonic() > end:
                raise RuntimeError('terminal_output_timeout:' + ident)
            page = self.tool('terminal_read', {'workspace_id': ws, 'terminal_id': ident, 'cursor': cursor, 'max_bytes': 65536, 'output_mode': 'full'}, deadline=end)
            status, text, next_cursor = page['terminal'], page.get('output', ''), page['cursor']
            if (status.get('id', ident) != ident or page.get('raw_cursor_start', cursor) != cursor
                    or next_cursor < cursor or next_cursor - cursor != len(text.encode('utf-8'))
                    or status.get('output_truncated') or status.get('output_error')):
                raise RuntimeError('terminal_evidence_incomplete:' + ident)
            parts.append(text)
            if not page.get('has_more'):
                if (status.get('output_complete') is not True or status.get('exit_code') != 0
                        or status.get('state', 'exited') != 'exited'):
                    raise RuntimeError('terminal_final_evidence_unconfirmed:' + ident)
                return ''.join(parts)
            if next_cursor == cursor:
                raise RuntimeError('terminal_output_cursor_stalled:' + ident)
            cursor = next_cursor

    def transport_info(self):
        if self._native is not None:
            return dict(self._native.status)
        return {'mode': self.transport_mode, 'selection_reason': self.transport_reason, 'auto_replay': False}

    def close(self):
        native_closed = True
        try:
            if self._native is not None:
                native_closed = self._native.close()
        except Exception:
            native_closed = False
        finally:
            self._closed = True
        if self.refresh:
            status, _, _ = self.call('/oauth/revoke', {'token': self.refresh}, form=True)
            if status not in (200, 204):
                return False
            self.refresh, self.access = None, None
        return native_closed

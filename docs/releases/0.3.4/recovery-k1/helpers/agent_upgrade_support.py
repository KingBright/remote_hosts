"""Read-only release gates. Never poll jobs, renew leases or create business tasks."""
import contextlib
import errno
import http.client
import socket
import ssl
import urllib.error
import json
import os
import pathlib
import re
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request

LANES = {'read', 'write', 'transfer', 'terminal', 'control'}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GatewayObservationError(RuntimeError):
    """Only allowlisted diagnostics, never request URLs, headers or exception text."""
    def __init__(self, diagnostic):
        self.diagnostic = diagnostic
        super().__init__('gateway_readiness_unavailable: '+diagnostic['category']+
                         '; attempts='+str(diagnostic.get('attempts', 1))+
                         '; next_action='+diagnostic['next_action'])


def gateway_error(error):
    result = {'stage': 'gateway_readiness', 'category': 'unexpected_client_error',
              'retryable': False, 'next_action': 'inspect_updater_environment'}
    if isinstance(error, urllib.error.HTTPError):
        result.update(category='http_rejected', http_status=error.code,
                      retryable=error.code in (408, 425, 500, 502, 503, 504),
                      next_action='inspect_gateway_or_edge_status')
        if error.code in (401, 403):
            result['next_action'] = 'review_gateway_auth_or_edge_policy; do_not_change_credentials_automatically'
        if error.code == 429:
            result['next_action'] = 'respect_server_rate_limit_before_retry'
        ray = error.headers.get('CF-Ray') if error.headers else None
        if ray and re.fullmatch(r'[A-Za-z0-9-]{1,80}', ray):
            result['cf_ray'] = ray
        error.close()
        return result
    reason = error.reason if isinstance(error, urllib.error.URLError) else error
    if isinstance(reason, ssl.SSLCertVerificationError):
        result.update(category='tls_certificate', next_action='inspect_trust_store_and_certificate; keep_verification_enabled')
        code = getattr(reason, 'verify_code', None)
        if isinstance(code, int): result['verify_code'] = code
    elif isinstance(reason, ssl.SSLError):
        result.update(category='tls_handshake', next_action='inspect_tls_handshake')
    elif isinstance(reason, (TimeoutError, socket.timeout)):
        result.update(category='network_timeout', retryable=True, next_action='retry_same_readonly_probe_with_backoff')
    elif isinstance(reason, socket.gaierror):
        result.update(category='dns_resolution', retryable=reason.errno == socket.EAI_AGAIN,
                      next_action='inspect_configured_gateway_dns')
    elif isinstance(reason, (http.client.IncompleteRead, http.client.RemoteDisconnected)):
        result.update(category='incomplete_response', retryable=True, next_action='retry_same_readonly_probe_with_backoff')
    elif isinstance(reason, OSError):
        transient = {errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED,
                     errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ETIMEDOUT, errno.EPIPE}
        result.update(category='network_connection', retryable=reason.errno in transient,
                      next_action='inspect_network_connectivity')
        if isinstance(reason.errno, int): result['errno'] = reason.errno
    elif isinstance(reason, (ValueError, UnicodeError)):
        result.update(category='invalid_response', next_action='inspect_gateway_response_without_disclosing_body')
    return result


def gateway_observation(config, *, attempts=3, request_timeout=5):
    if type(attempts) is not int or not 1 <= attempts <= 3 or not 0 < request_timeout <= 10:
        raise ValueError('invalid readiness retry bounds')
    origin = config['gateway_url'].rstrip('/')
    parsed = urllib.parse.urlsplit(origin)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
        raise RuntimeError('readiness requires a configured HTTPS gateway origin')
    opener = urllib.request.build_opener(NoRedirect())
    start = time.monotonic()
    for attempt in range(1, attempts+1):
        request = urllib.request.Request(origin + '/device/readiness', headers={
            'Authorization': 'Bearer ' + config['device_token'],
            'User-Agent': 'RemoteHosts-Updater/0.3.1', 'Accept': 'application/json',
            'Cache-Control': 'no-cache',
        })
        try:
            with opener.open(request, timeout=request_timeout) as response:
                body = response.read(32769)
                if len(body) > 32768:
                    raise ValueError('oversized readiness')
                value = json.loads(body)
        except Exception as error:
            diagnostic = gateway_error(error)
            diagnostic.update(attempts=attempt, elapsed_ms=int((time.monotonic()-start)*1000))
            if not diagnostic['retryable'] or attempt == attempts:
                raise GatewayObservationError(diagnostic) from None
            time.sleep(min(0.5 * 2**(attempt-1), 2))
            continue
        if (not isinstance(value, dict) or value.get('readiness_protocol') != 1
                or value.get('device_id') != config['device_id']):
            raise GatewayObservationError({'stage':'gateway_readiness',
                'category':'identity_or_protocol_mismatch', 'retryable':False,
                'attempts':attempt, 'next_action':'verify_original_device_and_gateway_mapping'})
        value['_transport'] = {'attempts':attempt, 'elapsed_ms':int((time.monotonic()-start)*1000)}
        return value


def local_observation(base):
    config = json.loads((base / 'agent.json').read_text())
    database = pathlib.Path(config['state_dir']) / 'state.sqlite'
    service = 'gui/%d/com.remote-hosts.code-agent' % os.getuid()
    process = subprocess.run(['launchctl', 'print', service], text=True, capture_output=True, timeout=5)
    fields = dict(re.findall(r'^\t(state|pid|runs) = ([^\n]+)$', process.stdout, re.MULTILINE))
    pid = int(fields['pid']) if fields.get('pid', '').isdecimal() else None
    with contextlib.closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        rows = db.execute("SELECT key,value FROM kv WHERE kind='runtime' AND key IN ('build','readiness')").fetchall()
    runtime = {key: json.loads(value) for key, value in rows}
    return {'running': process.returncode == 0 and fields.get('state') == 'running' and bool(pid and pid > 1),
            'pid': pid, 'runs': fields.get('runs'), **runtime}


def sample_identity(local, remote, version, after, gateway_baseline, require_lanes=True,
                    now=None, previous_session=None):
    now = int(time.time()) if now is None else now
    build = local.get('build', {})
    seen, observed = remote.get('last_seen'), remote.get('observed_at')
    started = build.get('started_at')
    # A declared ready flag is insufficient. Use the gateway's own time for its
    # lease age, and tolerate a small clock offset without accepting stale caches.
    if not (type(seen) is int and type(observed) is int and type(started) is int
            and 0 <= observed-seen < 45 and abs(now-observed) <= 90
            and after <= started <= now and local.get('running') is True
            and local.get('pid') == build.get('pid') and build.get('version') == version
            and remote.get('ready') is True and remote.get('agent_version') == version
            and seen > gateway_baseline and isinstance(remote.get('session'), str)
            and len(remote['session']) == 64 and remote['session'] != previous_session):
        return None
    if require_lanes:
        ready = local.get('readiness', {})
        lanes = ready.get('lanes', {})
        if not (isinstance(lanes, dict) and ready.get('pid') == local['pid']
                and ready.get('version') == version and ready.get('session') == remote['session']
                and ready.get('phase') == 'polling' and LANES.issubset(lanes)
                and all(type(lanes[lane]) is int and after <= lanes[lane] <= now
                        and now-lanes[lane] <= 45 for lane in LANES)):
            return None
    return local['pid'], remote['session'], local.get('runs')


def wait_ready(base, config, version, after, gateway_baseline, timeout=120,
               stable_seconds=15, require_lanes=True, previous_session=None):
    """Require stable identity AND continuing remote/local poll progress.

    Observing one cached successful sample repeatedly does not prove health.
    Every lane must acknowledge another poll within the same identity window.
    """
    if timeout <= stable_seconds or stable_seconds < 1:
        raise ValueError('invalid readiness time bounds')
    deadline = time.monotonic() + timeout
    stable_since = previous = first_seen = None
    first_lanes = {}
    samples = 0
    last_error = 'not_ready'
    while time.monotonic() < deadline:
        try:
            local = local_observation(base)
            remote = gateway_observation(config, attempts=1)
            identity = sample_identity(local, remote, version, after, gateway_baseline,
                                       require_lanes, previous_session=previous_session)
            if identity is not None:
                lanes = local.get('readiness', {}).get('lanes', {})
                if identity != previous:
                    stable_since = time.monotonic()
                    samples = 0
                    first_seen = remote['last_seen']
                    first_lanes = dict(lanes)
                previous = identity
                samples += 1
                advanced_lanes = sorted(lane for lane in LANES
                    if lanes.get(lane, 0) > first_lanes.get(lane, 0))
                progressing = remote['last_seen'] > first_seen and (
                    not require_lanes or set(advanced_lanes) == LANES)
                if (progressing and samples >= 3
                        and time.monotonic()-stable_since >= stable_seconds):
                    return {'pid': local['pid'], 'started_at': local['build']['started_at'],
                            'gateway_verified': True, 'readiness': 'stable_authenticated_control_plane',
                            'session': remote['session'], 'runs': local.get('runs'),
                            'stable_seconds': time.monotonic()-stable_since, 'samples': samples,
                            'gateway_seen_advanced': True, 'advanced_lanes': advanced_lanes,
                            'all_lanes_verified': require_lanes,
                            'functional_acceptance': 'separate_required_gate'}
                last_error = 'waiting_for_continued_poll_progress'
            else:
                previous = stable_since = None
                samples = 0
                last_error = 'pid_session_or_poll_acknowledgements_not_ready'
        except GatewayObservationError as error:
            previous = stable_since = None
            samples = 0
            last_error = 'gateway_'+error.diagnostic['category']
            if not error.diagnostic['retryable']:
                raise
        except Exception:
            previous = stable_since = None
            samples = 0
            last_error = 'local_observation_unavailable'
        time.sleep(1)
    raise RuntimeError('readiness_timeout: ' + last_error)

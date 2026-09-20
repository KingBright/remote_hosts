"""Thin stdio bridge to the installed Rust MCP adapter, not another HTTP client.

Rust owns persistent HTTPS, catalog verification and durable request receipts.
One bridge is reused for a Client's lifetime. A broken stream is poisoned: it is
never restarted or replayed automatically. Python still bootstraps OAuth.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import queue
import re
import subprocess
import tempfile
import threading
import time
import urllib.parse
from typing import Any

MAX_FRAME = 2 * 1024 * 1024
REQUEST_ID = re.compile(r"req_[0-9a-f]{32}\Z")


def installed_binary() -> pathlib.Path:
    """Use only the canonical installed Agent, never an arbitrary PATH program."""
    if os.name == 'nt':
        base = os.environ.get('LOCALAPPDATA')
        if not base:
            raise ValueError('native_adapter_localappdata_missing')
        return pathlib.Path(base) / 'RemoteHostsCode' / 'bin' / 'remote-hosts-code.exe'
    return pathlib.Path.home() / '.local/share/remote-hosts-code/bin/remote-hosts-code'


class NativeTransportError(RuntimeError):
    """Transport uncertainty; evidence references must survive client cleanup."""

    def __init__(self, code: str, evidence_dir: pathlib.Path,
                 requests: list[dict[str, Any]]):
        self.code = code
        self.evidence_dir = str(evidence_dir)
        self.requests = requests
        super().__init__(code + '; inspect retained adapter receipts: ' + str(evidence_dir))


class NativeSession:
    """A serialized, bounded MCP stdio session with a single owned child process."""

    def __init__(self, origin: str, access: str, binary: pathlib.Path,
                 state_root: pathlib.Path | None = None, timeout: float = 45.0):
        url = urllib.parse.urlsplit(origin)
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.path or url.query or url.fragment):
            raise ValueError('native_adapter_origin_invalid')
        if not access or len(access) > 8192 or any(c.isspace() for c in access):
            raise ValueError('native_adapter_token_invalid')
        if not 0 < timeout <= 120:
            raise ValueError('native_adapter_timeout_invalid')
        self.origin, self.binary, self.timeout = origin, pathlib.Path(binary), timeout
        if not self.binary.is_file() or not os.access(self.binary, os.X_OK):
            raise ValueError('native_adapter_binary_unavailable')
        with self.binary.open('rb') as source:
            binary_sha256 = hashlib.file_digest(source, 'sha256').hexdigest()
        root = state_root or pathlib.Path.home() / '.local/share/remote-hosts-code/release-clients'
        root = pathlib.Path(root).absolute()
        for path in (root, *root.parents):
            if path.is_symlink():
                raise ValueError('native_adapter_state_symlink')
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory = pathlib.Path(tempfile.mkdtemp(prefix='session-', dir=root))
        self.receipts = self.directory / 'receipts'
        self._token = self.directory / 'access.txt'
        self._config = self.directory / 'adapter.json'
        try:
            self._write_private(self._token, access.encode())
            self._write_private(self._config, json.dumps({
                'origin': origin, 'access_token_file': str(self._token),
                'state_dir': str(self.receipts),
            }).encode())
        except OSError:
            self._token.unlink(missing_ok=True)
            self._config.unlink(missing_ok=True)
            raise RuntimeError('native_adapter_credentials_prepare_failed') from None
        self.process: subprocess.Popen | None = None
        self._log = None
        self._reader_thread: threading.Thread | None = None
        self._writer_thread: threading.Thread | None = None
        self._inbox: queue.Queue = queue.Queue(maxsize=32)
        self._outbox: queue.Queue = queue.Queue(maxsize=4)
        self._fault: str | None = None
        self._lock = threading.RLock()
        self._sequence = 0
        self._acknowledged: set[str] = set()
        self._init: dict[str, Any] | None = None
        self.closed = False
        self.poisoned = False
        self.status: dict[str, Any] = {
            'mode': 'native_rust_adapter', 'state': 'prepared',
            'binary': str(self.binary), 'binary_sha256': binary_sha256,
            'evidence_dir': str(self.directory),
            'auto_replay': False, 'started_at': int(time.time()), 'rpc_calls': 0,
            'tool_calls_submitted': 0,
        }
        try:
            self._persist_status()
        except OSError:
            self._token.unlink(missing_ok=True)
            self._config.unlink(missing_ok=True)
            raise RuntimeError('native_adapter_state_unavailable_before_start') from None

    @staticmethod
    def _write_private(path: pathlib.Path, payload: bytes) -> None:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())

    def _persist_status(self) -> None:
        tmp = self.directory / 'session.json.tmp'
        with tmp.open('w', encoding='utf-8') as out:
            os.chmod(tmp, 0o600)
            json.dump(self.status, out, indent=2)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, self.directory / 'session.json')

    def _emit(self, kind: str, value: Any) -> None:
        try:
            self._inbox.put_nowait((kind, value))
        except queue.Full:
            self._fault = 'native_adapter_notification_budget'

    def _reader(self) -> None:
        try:
            while True:
                line = self.process.stdout.readline(MAX_FRAME + 1)
                if not line:
                    self._emit('error', 'native_adapter_stdout_closed')
                    return
                if len(line) > MAX_FRAME or not line.endswith(b'\n'):
                    self._emit('error', 'native_adapter_frame_budget_or_incomplete')
                    return
                self._emit('line', line)
        except (OSError, ValueError):
            self._emit('error', 'native_adapter_stdout_failed')

    def _writer(self) -> None:
        try:
            while True:
                frame = self._outbox.get()
                if frame is None:
                    break
                self.process.stdin.write(frame)
                self.process.stdin.flush()
        except (OSError, ValueError):
            self._emit('error', 'native_adapter_stdin_failed')
        finally:
            try:
                self.process.stdin.close()
            except (OSError, ValueError):
                pass

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self.closed or self.poisoned:
                raise RuntimeError('native_adapter_session_closed; no restart or replay')
            if self._init is not None:
                return self._init
            started = time.monotonic()
            try:
                self._log = (self.directory / 'adapter-stderr.log').open('xb')
                os.chmod(self.directory / 'adapter-stderr.log', 0o600)
                options: dict[str, Any] = {}
                if os.name == 'nt':
                    options['creationflags'] = subprocess.CREATE_NO_WINDOW
                else:
                    options['start_new_session'] = True
                self.process = subprocess.Popen(
                    [str(self.binary), 'adapter', '--config', str(self._config)],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self._log,
                    cwd=self.directory, **options)
                self._reader_thread = threading.Thread(target=self._reader, daemon=True)
                self._writer_thread = threading.Thread(target=self._writer, daemon=True)
                self._reader_thread.start()
                self._writer_thread.start()
                self._init = self._exchange('initialize', {
                    'protocolVersion': '2025-11-25', 'capabilities': {},
                    'clientInfo': {'name': 'remote-hosts-local-release', 'version': '1'},
                })
                if not isinstance(self._init, dict) or not isinstance(self._init.get('protocolVersion'), str):
                    self._fail('native_adapter_initialize_invalid')
                self._send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
                self.status.update(state='connected', startup_ms=round((time.monotonic()-started)*1000))
                self._persist_status()
                return self._init
            except NativeTransportError:
                raise
            except Exception:
                self._fail('native_adapter_start_failed')

    def _send(self, request: dict[str, Any]) -> None:
        frame = (json.dumps(request, ensure_ascii=False) + '\n').encode('utf-8')
        if len(frame) > MAX_FRAME:
            # Nothing from this request was sent, but a poisoned session is still
            # safer than falling back to a second transport without caller choice.
            self._fail('native_adapter_request_budget')
        try:
            self._outbox.put_nowait(frame)
            if request.get('method') == 'tools/call':
                self.status['tool_calls_submitted'] += 1
        except queue.Full:
            self._fail('native_adapter_write_queue_full')

    def _exchange(self, method: str, params: dict[str, Any]) -> Any:
        self._sequence += 1
        ident = self._sequence
        self._send({'jsonrpc': '2.0', 'id': ident, 'method': method, 'params': params})
        deadline = time.monotonic() + self.timeout
        notifications = 0
        while True:
            if self._fault:
                self._fail(self._fault)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fail('native_adapter_response_timeout')
            try:
                kind, payload = self._inbox.get(timeout=remaining)
            except queue.Empty:
                self._fail('native_adapter_response_timeout')
            if kind == 'error':
                self._fail(payload)
            try:
                value = json.loads(payload)
            except (ValueError, UnicodeError):
                self._fail('native_adapter_response_not_json')
            if not isinstance(value, dict) or value.get('jsonrpc') != '2.0':
                self._fail('native_adapter_response_schema')
            if 'id' not in value and isinstance(value.get('method'), str):
                notifications += 1
                if notifications > 16:
                    self._fail('native_adapter_notification_budget')
                continue
            if type(value.get('id')) is not int or value['id'] != ident:
                self._fail('native_adapter_response_id_mismatch')
            if 'error' in value or 'result' not in value:
                self._fail('native_adapter_protocol_error')
            result = value['result']
            structured = result.get('structuredContent') if isinstance(result, dict) else None
            if isinstance(structured, dict) and isinstance(structured.get('request_id'), str):
                self._acknowledged.add(structured['request_id'])
            return result

    def rpc(self, method: str, params: dict[str, Any]) -> Any:
        with self._lock:
            self.start()
            if method == 'initialize':
                return self._init
            self.status['rpc_calls'] += 1
            return self._exchange(method, params)

    def _unconfirmed_receipts(self) -> list[dict[str, Any]]:
        records = []
        for path in self.receipts.glob('req_*.json'):
            if path.is_symlink() or not REQUEST_ID.fullmatch(path.stem) or path.stem in self._acknowledged:
                continue
            try:
                if path.stat().st_size > MAX_FRAME:
                    continue
                value = json.loads(path.read_text())
                if value.get('kind') == 'adapter_bootstrap':
                    continue
                records.append({k: value.get(k) for k in ('request_id', 'operation_id', 'state')})
            except (OSError, ValueError):
                continue
        return records

    def _fail(self, code: str) -> None:
        self.poisoned = True
        self.status.update(state='outcome_unconfirmed', error_code=code)
        try:
            self.close()
        except Exception:
            # Cleanup failure must not replace the original uncertain-call handle.
            self.status.update(close_success=False, cleanup_error='cleanup_unconfirmed')
        records = self._unconfirmed_receipts()
        self.status['unconfirmed_requests'] = records
        self.status['execution_state'] = ('not_started' if self.status['tool_calls_submitted'] == 0 else 'unknown')
        bootstrap = []
        for path in self.receipts.glob('req_*.json'):
            if path.is_symlink() or not REQUEST_ID.fullmatch(path.stem):
                continue
            try:
                if path.stat().st_size > MAX_FRAME:
                    continue
                value = json.loads(path.read_text())
                if value.get('kind') == 'adapter_bootstrap':
                    bootstrap.append({k: value.get(k) for k in
                        ('request_id', 'method', 'state', 'error_code', 'attempts', 'tool_calls_submitted')})
            except (OSError, ValueError):
                continue
        self.status['bootstrap_receipts'] = bootstrap
        try:
            self._persist_status()
        except OSError:
            self.status.update(close_success=False, metadata_persisted=False)
        raise NativeTransportError(code, self.receipts, records)

    def close(self) -> bool:
        """Reap only our adapter; retain receipts, remove its temporary credentials."""
        with self._lock:
            if self.closed:
                return self.status.get('close_success', self.status.get('process_reaped', True))
            self.closed = True
            reaped = True
            if self.process is not None:
                try:
                    self._outbox.put_nowait(None)
                except queue.Full:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=5 if not self.poisoned else 0.2)
                except subprocess.TimeoutExpired:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait(timeout=3)
                for thread in (self._writer_thread, self._reader_thread):
                    if thread is not None:
                        thread.join(timeout=2)
                        reaped = reaped and not thread.is_alive()
                for stream in (self.process.stdin, self.process.stdout):
                    if stream is not None:
                        stream.close()
                self.status['adapter_exit_code'] = self.process.returncode
            if self._log is not None:
                self._log.close()
            self._token.unlink(missing_ok=True)
            self._config.unlink(missing_ok=True)
            successful = reaped and (self.poisoned or self.process is None or self.process.returncode == 0)
            self.status.update(process_reaped=reaped, close_success=successful, credentials_removed=True, closed_at=int(time.time()))
            if not self.poisoned:
                self.status['state'] = 'closed' if successful else 'closed_with_error'
            try:
                self._persist_status()
            except OSError:
                self.status.update(close_success=False, metadata_persisted=False)
                return False
            return successful

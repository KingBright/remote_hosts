#!/usr/bin/env python3
"""Owner-entered browser OAuth. Fixed Gateway resource/scopes; no secret files."""
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import urllib.parse

import release_receipts as rr

ORIGIN = "https://mcp.example.com"  # Reserved fixture/example origin.
SCOPES = ("code:read", "code:write")


class BrowserOAuthError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__()


def require(condition, code):
    if not condition:
        raise BrowserOAuthError(code)


def checked_origin(value):
    require(isinstance(value, str) and 1 <= len(value) <= 2048
            and not re.search(r"[\x00-\x20\x7f\\]", value), "invalid_gateway_origin")
    try:
        uri = urllib.parse.urlsplit(value)
        port = uri.port
    except ValueError:
        raise BrowserOAuthError("invalid_gateway_origin") from None
    require(uri.scheme == "https" and bool(uri.hostname)
            and uri.username is None and uri.password is None and not uri.path
            and not uri.query and not uri.fragment
            and (port is None or 1 <= port <= 65535)
            and urllib.parse.urlunsplit(uri) == value, "invalid_gateway_origin")
    return value


def durable_json(path, value):
    rr.atomic_json(path, value)
    fd = os.open(Path(path).parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Callback:
    def __init__(self, state, port=0, *, origin=ORIGIN):
        self.origin = checked_origin(origin)
        self.state = state
        self.code = None
        self.invalid_requests = 0
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass  # Request URLs contain an OAuth code: never log them.
            def respond(self, status, body=b"", location=None):
                self.send_response(status)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("Content-Security-Policy", "default-src 'none'")
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                if location is not None:
                    self.send_header("Location", location)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def do_GET(self):
                parsed = urllib.parse.urlsplit(self.path)
                expected_host = "127.0.0.1:" + str(owner.server.server_port)
                if self.headers.get("Host") != expected_host:
                    owner.invalid_requests += 1
                    return self.respond(400)
                if parsed.path == "/complete" and not parsed.query:
                    return self.respond(200, "授权已收取，请返回发布终端。无需复制或发送令牌。".encode())
                query = urllib.parse.parse_qs(parsed.query, strict_parsing=False)
                valid = (parsed.path == "/callback" and set(query) == {"code", "state", "iss"}
                         and all(len(v) == 1 for v in query.values())
                         and query["iss"][0] == owner.origin
                         and bool(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query["state"][0]))
                         and secrets.compare_digest(query["state"][0], owner.state)
                         and re.fullmatch(r"[A-Za-z0-9_-]{1,256}", query["code"][0]))
                if not valid:
                    owner.invalid_requests += 1
                    return self.respond(400, b"Invalid authorization callback.")
                if owner.code is not None:
                    return self.respond(409, b"Callback already received.")
                owner.code = query["code"][0]
                return self.respond(303, location="/complete")
            def do_POST(self):
                owner.invalid_requests += 1
                self.respond(405)
        self.server = HTTPServer(("127.0.0.1", port), Handler)
        self.server.timeout = 1
        original_get_request = self.server.get_request
        def bounded_request():
            connection, address = original_get_request()
            connection.settimeout(5)
            return connection, address
        self.server.get_request = bounded_request

    @property
    def redirect_uri(self):
        return "http://127.0.0.1:" + str(self.server.server_port) + "/callback"

    def wait(self, seconds=600):
        require(0 < seconds <= 600, "invalid_owner_login_budget")
        end = time.monotonic() + seconds
        while self.code is None and time.monotonic() < end:
            require(self.invalid_requests <= 32, "invalid_callback_budget_exceeded")
            self.server.handle_request()
        require(self.code is not None, "owner_login_timed_out")
        # Serve the immediate redirect so the owner sees a code-free completion URL.
        self.server.handle_request()
        return self.code

    def close(self):
        self.server.server_close()
        self.code = None
        self.state = None


class BrowserOAuth:
    def __init__(self, client, directory, *, opener=None):
        self.client = client
        self.origin = checked_origin(client.origin)
        self.directory = Path(directory)
        self.record_path = self.directory / "oauth-public-client.json"
        self.progress_path = self.directory / "oauth-progress.json"
        self.opener = opener or self.open_browser

    @staticmethod
    def open_browser(url):
        # LaunchServices reuses the owner's default browser/profile; no AppleScript,
        # password entry, cookie reading, or system permission change.
        result = subprocess.run(["/usr/bin/open", url], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, check=False)
        require(result.returncode == 0, "owner_browser_not_opened")

    def progress(self, phase, **fields):
        durable_json(self.progress_path, {"phase": phase, "pid": os.getpid(),
                     "updated_at": int(time.time()), "resource": self.origin + "/mcp",
                     "scopes": list(SCOPES), "credential_storage": False, **fields})

    def metadata(self):
        resource = self.client.parsed("/.well-known/oauth-protected-resource")
        metadata = self.client.parsed("/.well-known/oauth-authorization-server")
        require(resource.get("resource") == self.origin + "/mcp"
                and metadata.get("issuer") == self.origin
                and metadata.get("authorization_endpoint") == self.origin + "/oauth/authorize"
                and metadata.get("token_endpoint") == self.origin + "/oauth/token"
                and metadata.get("registration_endpoint") == self.origin + "/oauth/register"
                and "S256" in metadata.get("code_challenge_methods_supported", []),
                "gateway_oauth_metadata_identity_mismatch")

    def previous(self):
        if not self.record_path.exists():
            return None
        require(not self.record_path.is_symlink()
                and self.record_path.stat().st_size <= 8192, "unsafe_public_client_record")
        record = json.loads(self.record_path.read_text())
        require(record.get("resource") == self.origin + "/mcp"
                and record.get("scopes") == list(SCOPES), "public_client_identity_conflict")
        require(record.get("phase") == "registered", "observe_original_registration_do_not_repeat")
        require(bool(re.fullmatch(r"[A-Za-z0-9_-]{1,256}", record.get("client_id", ""))),
                "public_client_id_invalid")
        uri = urllib.parse.urlsplit(record["redirect_uri"])
        require(uri.scheme == "http" and uri.hostname == "127.0.0.1"
                and uri.port and uri.path == "/callback" and not uri.query
                and not uri.fragment and uri.username is None and uri.password is None,
                "public_callback_identity_invalid")
        return record

    def registered(self, callback, previous):
        if previous:
            require(previous["redirect_uri"] == callback.redirect_uri, "registered_callback_changed")
            return previous["client_id"]
        record = {"phase": "registration_requesting", "resource": self.origin + "/mcp",
                  "scopes": list(SCOPES), "redirect_uri": callback.redirect_uri,
                  "requested_at": int(time.time()), "registration_expiry": "none"}
        durable_json(self.record_path, record)
        try:
            status, _, body = self.client.call("/oauth/register", {
                "client_name": "Remote Hosts Gateway release 0.10.26",
                "redirect_uris": [callback.redirect_uri],
                "token_endpoint_auth_method": "none",
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"]})
            require(status == 201, "client_registration_not_confirmed")
            value = json.loads(body)
            require(value.get("token_endpoint_auth_method") == "none"
                    and "client_secret" not in value
                    and value.get("redirect_uris") == [callback.redirect_uri]
                    and bool(re.fullmatch(r"[A-Za-z0-9_-]{1,256}", value.get("client_id", ""))),
                    "registered_client_contract_mismatch")
            record.update(phase="registered", client_id=value["client_id"])
            durable_json(self.record_path, record)  # public ID/URI only; no credential
            return value["client_id"]
        except Exception as error:
            self.progress("registration_outcome_unknown", error_type=type(error).__name__)
            raise BrowserOAuthError("observe_original_registration_do_not_repeat") from None

    def authenticate(self):
        require(sys.stdin.isatty() and sys.stderr.isatty(), "owner_terminal_required")
        require(not self.directory.is_symlink(), "unsafe_oauth_directory")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.metadata()
        previous = self.previous()
        if self.progress_path.exists():
            require(not self.progress_path.is_symlink()
                    and self.progress_path.stat().st_size <= 8192, "unsafe_oauth_progress")
            progress = json.loads(self.progress_path.read_text())
            # An interrupted exchange/session cannot be recovered from disk: no token is stored.
            require(progress.get("phase") not in ("token_exchange_requested",
                    "token_exchange_outcome_unknown", "session_acquired"),
                    "original_oauth_session_requires_observation")
            require(progress.get("phase") != "waiting_owner_login"
                    or int(time.time()) > progress.get("expires_at", 2**63-1) + 120,
                    "original_owner_login_still_pending")
        state = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(48)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        port = urllib.parse.urlsplit(previous["redirect_uri"]).port if previous else 0
        callback = Callback(state, port, origin=self.origin)
        try:
            client_id = self.registered(callback, previous)
            query = urllib.parse.urlencode({"client_id": client_id,
                "redirect_uri": callback.redirect_uri, "response_type": "code",
                "code_challenge": challenge, "code_challenge_method": "S256",
                "state": state, "resource": self.origin + "/mcp", "scope": " ".join(SCOPES)})
            self.progress("waiting_owner_login", expires_at=int(time.time()) + 600,
                          client_id=client_id)
            print("授权范围已确认。请在自动打开的 Gateway 页面直接输入密码并点击“登录并授权”。", file=sys.stderr)
            self.opener(self.origin + "/oauth/authorize?" + query)  # URL stays out of logs/receipts
            self.progress("waiting_owner_login", expires_at=int(time.time()) + 600,
                          client_id=client_id, browser_launch_confirmed=True,
                          authorization_origin=self.origin,
                          page_title_from_source="Remote Hosts 授权")
            code = callback.wait()
            self.progress("token_exchange_requested", client_id=client_id)
            try:
                status, _, body = self.client.call("/oauth/token", {
                    "grant_type": "authorization_code", "client_id": client_id,
                    "code": code, "code_verifier": verifier,
                    "redirect_uri": callback.redirect_uri, "resource": self.origin + "/mcp"}, form=True)
                require(status == 200, "token_exchange_not_confirmed")
                value = json.loads(body)
                require(value.get("resource") == self.origin + "/mcp"
                        and value.get("token_type") == "Bearer"
                        and type(value.get("expires_in")) is int and value["expires_in"] == 3600
                        and value.get("scope", "").split() == list(SCOPES)
                        and bool(re.fullmatch(r"[A-Za-z0-9._~+/=-]{1,4096}", value.get("access_token", ""))),
                        "issued_authorization_scope_or_identity_mismatch")
                token = value["access_token"]
                value.clear()  # refresh token is not persisted, refreshed, printed or sent elsewhere
                self.progress("session_acquired", access_expires_at=int(time.time()) + 3600,
                              refresh_issued_by_protocol=True, refresh_retained=False,
                              client_id=client_id)
                return token
            except Exception as error:
                self.progress("token_exchange_outcome_unknown", error_type=type(error).__name__)
                raise BrowserOAuthError("observe_original_token_exchange_do_not_repeat") from None
        finally:
            callback.close()
            verifier = None
            state = None

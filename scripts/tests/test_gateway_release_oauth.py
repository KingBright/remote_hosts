"""Bounded, isolated browser OAuth tests; no real server or credential access."""
import contextlib
import hashlib
from http.client import HTTPConnection
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import urllib.parse
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gateway_release_oauth as oauth
import release_receipts as rr


class FakeClient:
    def __init__(self):
        self.origin = oauth.ORIGIN
        self.calls = []
        self.register_fail = False
        self.exchange_fail = False
        self.scope = "code:read code:write"
        self.redirect = None
        self.exchange = None

    def parsed(self, path):
        self.calls.append(path)
        if path.endswith("oauth-protected-resource"):
            return {"resource": oauth.ORIGIN + "/mcp"}
        return {"issuer": oauth.ORIGIN,
                "authorization_endpoint": oauth.ORIGIN + "/oauth/authorize",
                "token_endpoint": oauth.ORIGIN + "/oauth/token",
                "registration_endpoint": oauth.ORIGIN + "/oauth/register",
                "code_challenge_methods_supported": ["S256"]}

    def call(self, path, data, form=False):
        self.calls.append(path)
        if path == "/oauth/register":
            if self.register_fail:
                raise OSError("SYNTHETIC_SECRET_ERROR_NEVER_PRINT")
            self.redirect = data["redirect_uris"][0]
            return 201, {}, json.dumps({"client_id": "synthetic-public-id",
                "redirect_uris": [self.redirect], "token_endpoint_auth_method": "none"}).encode()
        assert path == "/oauth/token" and form
        self.exchange = data
        if self.exchange_fail:
            raise OSError("SYNTHETIC_SECRET_ERROR_NEVER_PRINT")
        return 200, {}, json.dumps({"access_token": "synthetic-access-secret",
            "refresh_token": "synthetic-refresh-secret", "token_type": "Bearer",
            "expires_in": 3600, "scope": self.scope,
            "resource": oauth.ORIGIN + "/mcp"}).encode()


class FakeCallback:
    last = None
    def __init__(self, state, port=0, *, origin=oauth.ORIGIN):
        self.origin = origin
        self.state, self.port = state, port or 54321
        self.closed = False
        FakeCallback.last = self
    @property
    def redirect_uri(self):
        return "http://127.0.0.1:" + str(self.port) + "/callback"
    def wait(self):
        return "synthetic-code-secret"
    def close(self):
        self.closed = True


class OAuthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "receipt"
        self.client = FakeClient()
        self.urls = []
        self.flow = oauth.BrowserOAuth(self.client, self.directory, opener=self.urls.append)

    def authenticate(self):
        with contextlib.redirect_stderr(io.StringIO()), \
             mock.patch.object(oauth.sys.stdin, "isatty", return_value=True), \
             mock.patch.object(oauth.sys.stderr, "isatty", return_value=True), \
             mock.patch.object(oauth, "Callback", FakeCallback):
            return self.flow.authenticate()

    def test_exact_scope_resource_pkce_and_memory_only_credential(self):
        self.assertEqual(self.authenticate(), "synthetic-access-secret")
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.urls[0]).query)
        self.assertEqual(query["scope"], ["code:read code:write"])
        self.assertEqual(query["resource"], [oauth.ORIGIN + "/mcp"])
        self.assertEqual(query["code_challenge_method"], ["S256"])
        verifier = self.client.exchange["code_verifier"]
        import base64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        self.assertEqual(query["code_challenge"], [challenge])
        self.assertTrue(FakeCallback.last.closed)
        receipts = "".join(p.read_text() for p in self.directory.glob("*.json"))
        for secret in ("synthetic-access-secret", "synthetic-refresh-secret",
                       "synthetic-code-secret", verifier, query["state"][0]):
            self.assertNotIn(secret, receipts)
        progress = json.loads(self.flow.progress_path.read_text())
        self.assertFalse(progress["refresh_retained"])
        self.assertFalse(progress["credential_storage"])
        self.assertEqual(self.client.calls.count("/oauth/register"), 1)
        self.assertEqual(self.client.calls.count("/oauth/token"), 1)

    def test_non_interactive_stops_before_network_or_registration(self):
        with mock.patch.object(oauth.sys.stdin, "isatty", return_value=False):
            with self.assertRaises(oauth.BrowserOAuthError) as caught:
                self.flow.authenticate()
        self.assertEqual(caught.exception.code, "owner_terminal_required")
        self.assertEqual(self.client.calls, [])
        self.assertFalse(self.directory.exists())

    def test_unknown_registration_is_durable_and_never_retried(self):
        self.client.register_fail = True
        for _ in range(2):
            with self.assertRaises(oauth.BrowserOAuthError):
                self.authenticate()
        self.assertEqual(self.client.calls.count("/oauth/register"), 1)
        self.assertEqual(self.urls, [])
        self.assertNotIn("SYNTHETIC_SECRET", self.flow.progress_path.read_text())

    def test_unknown_exchange_and_acquired_session_never_repeat_authorization(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                self.directory = Path(self.temp.name) / str(failed)
                self.client = FakeClient()
                self.client.exchange_fail = failed
                self.flow = oauth.BrowserOAuth(self.client, self.directory, opener=self.urls.append)
                if failed:
                    with self.assertRaises(oauth.BrowserOAuthError):
                        self.authenticate()
                else:
                    self.authenticate()
                count = len(self.urls)
                with self.assertRaises(oauth.BrowserOAuthError):
                    self.authenticate()
                self.assertEqual(len(self.urls), count)
                self.assertEqual(self.client.calls.count("/oauth/token"), 1)
                self.assertEqual(self.client.calls.count("/oauth/register"), 1)

    def test_existing_public_client_reused_without_registration(self):
        self.directory.mkdir()
        rr.atomic_json(self.flow.record_path, {"phase": "registered",
            "resource": oauth.ORIGIN + "/mcp", "scopes": list(oauth.SCOPES),
            "client_id": "synthetic-public-id", "redirect_uri": "http://127.0.0.1:54321/callback"})
        self.authenticate()
        self.assertNotIn("/oauth/register", self.client.calls)

    def test_scope_expansion_rejected_before_any_publish_session(self):
        self.client.scope += " terminal:exec"
        with self.assertRaises(oauth.BrowserOAuthError):
            self.authenticate()
        self.assertEqual(json.loads(self.flow.progress_path.read_text())["phase"],
                         "token_exchange_outcome_unknown")

    def test_pending_owner_login_does_not_open_another_browser(self):
        self.directory.mkdir()
        rr.atomic_json(self.flow.progress_path, {"phase": "waiting_owner_login",
                                                "expires_at": 2**62})
        with self.assertRaises(oauth.BrowserOAuthError) as caught:
            self.authenticate()
        self.assertEqual(caught.exception.code, "original_owner_login_still_pending")
        self.assertEqual(self.urls, [])


class CallbackTests(unittest.TestCase):
    def test_callback_rejects_wrong_identity_and_serves_code_free_redirect(self):
        callback = oauth.Callback("synthetic-state", origin="https://other.example")
        self.addCleanup(callback.close)
        code = []
        worker = threading.Thread(target=lambda: code.append(callback.wait(seconds=10)))
        worker.start()
        conn = HTTPConnection("127.0.0.1", callback.server.server_port, timeout=3)
        base = {"code": "synthetic-code", "state": "synthetic-state", "iss": "https://other.example"}
        with contextlib.redirect_stderr(io.StringIO()):
            for altered in (dict(base, state="wrong"), dict(base, iss=oauth.ORIGIN)):
                conn.request("GET", "/callback?" + urllib.parse.urlencode(altered))
                response = conn.getresponse()
                self.assertEqual(response.status, 400)
                response.read()
            conn.request("GET", "/callback?" + urllib.parse.urlencode(base))
            response = conn.getresponse()
            self.assertEqual(response.status, 303)
            self.assertEqual(response.getheader("Location"), "/complete")
            response.read()
            conn.request("GET", "/complete")
            response = conn.getresponse()
            self.assertEqual(response.status, 200)
            body = response.read()
            self.assertNotIn(b"synthetic-code", body)
        conn.close()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(code, ["synthetic-code"])


if __name__ == "__main__":
    unittest.main()

"""Bounded artifact GET recovery, with no real network or device mutations."""
import hashlib
import io
import pathlib
import ssl
import sys
import unittest
import urllib.error
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client


class Response:
    def __init__(self, data=b'evidence', status=200):
        self.status, self.headers = status, {}
        self.data = io.BytesIO(data)
        self.closed = False
    def read(self, size):
        return self.data.read(size)
    def __enter__(self):
        return self
    def __exit__(self, *_):
        self.closed = True


class Opener:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.requests = []
    def open(self, request, timeout):
        self.requests.append((request.get_method(), request.full_url, timeout))
        value = self.outcomes.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


class VerifiedArtifactReadTests(unittest.TestCase):
    def fixture(self, *outcomes):
        c = Client('https://fixture.example', transport='legacy')
        c.opener = Opener(*outcomes)
        a = {'state': 'completed', 'download_available': True,
             'operation_id': 'original-export',
             'download_url': 'https://fixture.example/files/private-capability/evidence',
             'size': 8, 'sha256': hashlib.sha256(b'evidence').hexdigest()}
        return c, a

    @patch('release_client.time.sleep')
    def test_timeout_retries_only_the_identical_get(self, sleep):
        response = Response()
        c, a = self.fixture(TimeoutError(), response)
        self.assertEqual(c.read_artifact(a), b'evidence')
        self.assertEqual(c.opener.requests[0], c.opener.requests[1])
        self.assertEqual(c.opener.requests[0][0], 'GET')
        self.assertTrue(response.closed)
        self.assertFalse(c.http_observations[0]['producer_replayed'])
        self.assertNotIn('private-capability', str(c.http_observations))
        sleep.assert_called_once()

    @patch('release_client.time.sleep')
    def test_transient_520_can_recover(self, sleep):
        c, a = self.fixture(Response(b'error', 520), Response())
        self.assertEqual(c.read_artifact(a), b'evidence')
        self.assertEqual(c.http_observations[0]['http_status'], 520)

    def test_denial_and_rate_limit_never_retry(self):
        for code in (401, 403, 429):
            c, a = self.fixture(Response(b'{}', code))
            with self.assertRaises(RuntimeError):
                c.read_artifact(a)
            self.assertEqual(len(c.opener.requests), 1)

    def test_certificate_error_never_retries(self):
        c, a = self.fixture(urllib.error.URLError(ssl.SSLCertVerificationError('private details')))
        with self.assertRaisesRegex(RuntimeError, 'tls_certificate') as raised:
            c.read_artifact(a)
        self.assertEqual(len(c.opener.requests), 1)
        self.assertNotIn('private details', str(raised.exception))

    def test_checksum_mismatch_is_not_retried(self):
        c, a = self.fixture(Response(b'not-good'))
        with self.assertRaisesRegex(ValueError, 'artifact_identity_mismatch'):
            c.read_artifact(a)
        self.assertEqual(len(c.opener.requests), 1)

    def test_wrong_origin_and_unfinished_export_are_rejected_before_network(self):
        for changes in ({'download_url': 'https://other.example/files/id/a'}, {'state': 'paused'},
                        {'size': True}, {'size': 67108865}, {'download_available': False}):
            c, a = self.fixture()
            a.update(changes)
            with self.assertRaises(ValueError):
                c.read_artifact(a)
            self.assertFalse(c.opener.requests)

    @patch('release_client.time.sleep')
    def test_retry_budget_is_finite_and_diagnostics_redacted(self, sleep):
        c, a = self.fixture(TimeoutError('private-capability'), TimeoutError(), TimeoutError())
        with self.assertRaises(RuntimeError) as raised:
            c.read_artifact(a)
        self.assertEqual(len(c.opener.requests), 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertNotIn('private-capability', str(raised.exception))

    def test_redirect_is_not_followed_or_retried(self):
        c, a = self.fixture(Response(b'', 302))
        with self.assertRaises(RuntimeError):
            c.read_artifact(a)
        self.assertEqual(len(c.opener.requests), 1)


if __name__ == '__main__':
    unittest.main()

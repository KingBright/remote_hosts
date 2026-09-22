"""Only public metadata GET and same-grant revocation may retry here."""
import pathlib
import ssl
import sys
import unittest
import urllib.error
from unittest.mock import Mock, patch
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client

class TransportRecoveryTests(unittest.TestCase):
    def client(self):
        return Client('https://fixture.example', transport='legacy')

    @patch('release_client.time.sleep')
    def test_public_get_timeout_recovers_with_no_oauth_or_tool(self, sleep):
        c=self.client(); c.call=Mock(side_effect=[TimeoutError('private'),(200,{},b'{"ok":true}')])
        self.assertTrue(c.parsed('/.well-known/oauth-protected-resource')['ok'])
        self.assertEqual(c.call.call_count,2)
        self.assertEqual(c.call.call_args_list[0],c.call.call_args_list[1])
        self.assertNotIn('private',str(c.http_observations));sleep.assert_called_once()

    @patch('release_client.time.sleep')
    def test_public_get_dns_and_body_failure_share_three_attempt_budget(self, sleep):
        c=self.client(); c.call=Mock(side_effect=urllib.error.URLError('private-host'))
        with self.assertRaisesRegex(RuntimeError,'metadata_read_unconfirmed') as raised:
            c.parsed('/healthz')
        self.assertEqual(c.call.call_count,3);self.assertEqual(sleep.call_count,2)
        self.assertNotIn('private-host',str(raised.exception))

    @patch('release_client.time.sleep')
    def test_certificate_error_never_retried(self,sleep):
        c=self.client();c.call=Mock(side_effect=urllib.error.URLError(ssl.SSLCertVerificationError('private')))
        with self.assertRaisesRegex(RuntimeError,'tls_certificate'):c.parsed('/healthz')
        self.assertEqual(c.call.call_count,1);sleep.assert_not_called()

    @patch('release_client.time.sleep')
    def test_oauth_writes_and_tool_calls_are_not_replayed(self,sleep):
        for path in ['/oauth/token','/oauth/register','/oauth/approve','/mcp']:
            c=self.client();c.call=Mock(side_effect=TimeoutError('original'))
            with self.assertRaises(TimeoutError):c.parsed(path,{'request':'same'})
            self.assertEqual(c.call.call_count,1)
        sleep.assert_not_called()

    def test_other_get_and_json_errors_do_not_retry(self):
        c=self.client();c.call=Mock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):c.parsed('/admin/status')
        self.assertEqual(c.call.call_count,1)
        c=self.client();c.call=Mock(return_value=(200,{},b'not-json'))
        with self.assertRaises(ValueError):c.parsed('/healthz')
        self.assertEqual(c.call.call_count,1)

    @patch('release_client.time.sleep')
    def test_revocation_retries_same_grant_and_separates_cleanup_facts(self,sleep):
        c=self.client();c.refresh='private-refresh';c.access='private-access'
        c.call=Mock(side_effect=[TimeoutError('private'),(200,{},b'{}')])
        self.assertTrue(c.close());self.assertTrue(c.oauth_revoke_complete)
        self.assertTrue(c.native_close_complete);self.assertIsNone(c.refresh)
        self.assertEqual(c.call.call_args_list[0],c.call.call_args_list[1])
        self.assertNotIn('private',str(c.http_observations))
        self.assertTrue(c.close());self.assertEqual(c.call.call_count,2)

    @patch('release_client.time.sleep')
    def test_revocation_stops_on_denial_or_rate_limit(self,sleep):
        for status in [401,403,429]:
            c=self.client();c.refresh='retain';c.call=Mock(return_value=(status,{},b'{}'))
            self.assertFalse(c.close());self.assertEqual(c.refresh,'retain')
            self.assertFalse(c.oauth_revoke_complete);self.assertEqual(c.call.call_count,1)
        sleep.assert_not_called()

    @patch('release_client.time.sleep')
    def test_unconfirmed_revoke_retains_grant_for_explicit_recovery(self,sleep):
        c=self.client();c.refresh='retain';c.call=Mock(side_effect=TimeoutError())
        self.assertFalse(c.close());self.assertEqual(c.call.call_count,3)
        self.assertEqual(c.refresh,'retain');self.assertFalse(c.oauth_revoke_complete)
        c.call=Mock(return_value=(204,{},b''));self.assertTrue(c.close());self.assertIsNone(c.refresh)

    def test_native_cleanup_failure_does_not_mean_grant_not_revoked(self):
        c=self.client();c.refresh='grant';c._native=Mock();c._native.close.return_value=False
        c.call=Mock(return_value=(200,{},b'{}'))
        self.assertFalse(c.close());self.assertTrue(c.oauth_revoke_complete)
        self.assertFalse(c.native_close_complete);self.assertIsNone(c.refresh)

    @patch('release_client.time.sleep')
    def test_revocation_certificate_failure_never_retries(self,sleep):
        c=self.client();c.refresh='grant';c.call=Mock(side_effect=ssl.SSLCertVerificationError('private'))
        self.assertFalse(c.close());self.assertEqual(c.call.call_count,1)
        sleep.assert_not_called();self.assertNotIn('private',str(c.http_observations))

if __name__=='__main__':unittest.main()

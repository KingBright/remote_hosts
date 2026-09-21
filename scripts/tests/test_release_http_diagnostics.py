"""Synthetic upstream responses only; no credentials or network access."""
import json
import pathlib
import sys
import unittest
from unittest import mock
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client, ReleaseHttpError

class HttpDiagnosticsTests(unittest.TestCase):
    def client(self):
        return Client('https://fixture.example', access='synthetic', transport='legacy')

    def test_edge_policy_rejection_preserves_code_and_ray_without_retry(self):
        c=self.client();body=json.dumps({'error_code':1010,'retryable':False,'detail':'SECRET_BODY'}).encode()
        with mock.patch.object(c,'call',return_value=(403,{'CF-Ray':'abc123-LHR'},body)) as call, mock.patch('release_client.time.sleep') as sleep:
            with self.assertRaises(ReleaseHttpError) as caught:c.parsed('/.well-known/oauth-protected-resource')
        self.assertEqual(call.call_count,1);sleep.assert_not_called()
        d=caught.exception.diagnostic
        self.assertEqual(d['edge_error_code'],1010);self.assertEqual(d['cf_ray'],'abc123-LHR')
        self.assertFalse(d['retryable']);self.assertEqual(d['user_action'],'review_owner_edge_policy')
        self.assertNotIn('SECRET_BODY',str(caught.exception))

    def test_transient_metadata_error_retries_only_the_same_get(self):
        c=self.client()
        with mock.patch.object(c,'call',side_effect=[(520,{},b''),(200,{},b'{"resource":"ok"}')]) as call, mock.patch('release_client.time.sleep'):
            self.assertEqual(c.parsed('/.well-known/oauth-protected-resource'),{'resource':'ok'})
        self.assertEqual(call.call_count,2)
        self.assertEqual(call.call_args_list[0],call.call_args_list[1])
        self.assertEqual(c.http_observations[0]['http_status'],520)

    def test_oauth_token_and_mcp_posts_never_retry(self):
        for path in ['/oauth/token','/oauth/register','/mcp']:
            c=self.client()
            with mock.patch.object(c,'call',return_value=(520,{},b'')) as call, mock.patch('release_client.time.sleep') as sleep:
                with self.assertRaises(ReleaseHttpError):c.parsed(path,{'secret':'not disclosed'})
            self.assertEqual(call.call_count,1);sleep.assert_not_called()

    def test_transient_get_has_a_fixed_three_attempt_budget(self):
        c=self.client()
        with mock.patch.object(c,'call',return_value=(524,{},b'')) as call, mock.patch('release_client.time.sleep'):
            with self.assertRaises(ReleaseHttpError):c.parsed('/healthz')
        self.assertEqual(call.call_count,3);self.assertEqual(len(c.http_observations),3)

    def test_declared_non_retryable_error_is_not_retried(self):
        c=self.client()
        with mock.patch.object(c,'call',return_value=(520,{},b'{"retryable":false}')) as call:
            with self.assertRaises(ReleaseHttpError):c.parsed('/healthz')
        self.assertEqual(call.call_count,1)

    def test_query_secrets_and_untrusted_header_text_are_not_returned(self):
        e=ReleaseHttpError(403,'/oauth/token?secret=TOKEN',{'CF-Ray':'bad\nTOKEN'},b'{"detail":"TOKEN"}')
        self.assertNotIn('TOKEN',str(e));self.assertIsNone(e.diagnostic['cf_ray'])
        self.assertEqual(e.diagnostic['path'],'/oauth/token')

    def test_plain_html_does_not_invent_an_edge_code(self):
        e=ReleaseHttpError(403,'/healthz',{},b'<h1>Access denied</h1>')
        self.assertIsNone(e.diagnostic['edge_error_code']);self.assertEqual(e.diagnostic['failure_boundary'],'upstream_http')
        self.assertFalse(e.diagnostic['retryable'])

    def test_unknown_get_paths_and_rate_limits_are_not_auto_retried(self):
        for status,path in [(520,'/admin/action'),(429,'/healthz')]:
            c=self.client()
            with mock.patch.object(c,'call',return_value=(status,{},b'')) as call:
                with self.assertRaises(ReleaseHttpError):c.parsed(path)
            self.assertEqual(call.call_count,1)

if __name__=='__main__':unittest.main()

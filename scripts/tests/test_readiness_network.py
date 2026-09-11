"""Readiness preflight transport regressions; no live network or services."""
import contextlib
import errno
import importlib.util
import io
import json
import pathlib
import socket
import ssl
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

SCRIPTS=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SCRIPTS))
import agent_upgrade_support as support

class NetworkReadinessTests(unittest.TestCase):
    def setUp(self):
        self.config={'gateway_url':'https://gateway.example:8443','device_id':'fixture-device','device_token':'never-log-this-credential'}
        self.good={'device_id':'fixture-device','readiness_protocol':1,'ready':True,'session':'a'*64}
        self.opener=mock.Mock()
        self.open_patch=mock.patch.object(support.urllib.request,'build_opener',return_value=self.opener)
        self.sleep_patch=mock.patch.object(support.time,'sleep')
        self.build=self.open_patch.start(); self.sleep=self.sleep_patch.start()
    def tearDown(self):
        self.open_patch.stop(); self.sleep_patch.stop()
    def response(self,value=None):
        return io.BytesIO(json.dumps(value or self.good).encode())
    def reject(self,status,ray='safe-ray-AMS'):
        return urllib.error.HTTPError('https://do-not-log.example/private?secret=never-log-this-credential',status,'never-log-this-credential',{'CF-Ray':ray},io.BytesIO(b'never-log-this-credential'))
    def test_transient_timeout_retries_same_authenticated_request(self):
        self.opener.open.side_effect=[TimeoutError('never-log-this-credential'),self.response()]
        value=support.gateway_observation(self.config)
        self.assertEqual(value['_transport']['attempts'],2)
        self.assertEqual(self.opener.open.call_count,2)
        self.sleep.assert_called_once_with(0.5)
        for call in self.opener.open.call_args_list:
            request=call.args[0]
            self.assertEqual(request.full_url,'https://gateway.example:8443/device/readiness')
            self.assertEqual(request.get_header('Authorization'),'Bearer '+self.config['device_token'])
            self.assertEqual(request.get_header('User-agent'),'RemoteHosts-Updater/0.3.1')
            self.assertEqual(call.kwargs['timeout'],5)
    def test_exhaustion_is_bounded_and_diagnostic_is_secret_free(self):
        self.opener.open.side_effect=urllib.error.URLError(TimeoutError('never-log-this-credential'))
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertEqual(self.opener.open.call_count,3)
        self.assertEqual(caught.exception.diagnostic['category'],'network_timeout')
        self.assertEqual(caught.exception.diagnostic['attempts'],3)
        text=str(caught.exception)+json.dumps(caught.exception.diagnostic)
        self.assertNotIn(self.config['device_token'],text)
        self.assertNotIn('https://',text)
    def test_auth_or_edge_rejection_is_not_automatically_retried(self):
        for code in (401,403):
            self.opener.open.reset_mock(); self.sleep.reset_mock()
            self.opener.open.side_effect=self.reject(code)
            with self.assertRaises(support.GatewayObservationError) as caught:
                support.gateway_observation(self.config)
            d=caught.exception.diagnostic
            self.assertEqual(d['http_status'],code); self.assertFalse(d['retryable'])
            self.assertEqual(d['cf_ray'],'safe-ray-AMS')
            self.opener.open.assert_called_once(); self.sleep.assert_not_called()
            self.assertNotIn(self.config['device_token'],str(caught.exception)+json.dumps(d))
    def test_transient_gateway_status_retries_without_changing_target(self):
        self.opener.open.side_effect=[self.reject(503),self.response()]
        self.assertEqual(support.gateway_observation(self.config)['_transport']['attempts'],2)
    def test_rate_limit_does_not_enter_fast_retry_loop(self):
        self.opener.open.side_effect=self.reject(429)
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertIn('rate_limit',caught.exception.diagnostic['next_action']);self.sleep.assert_not_called()
    def test_certificate_failure_is_not_hidden_or_retried(self):
        self.opener.open.side_effect=urllib.error.URLError(ssl.SSLCertVerificationError(1,'never-log-this-credential'))
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertEqual(caught.exception.diagnostic['category'],'tls_certificate')
        self.assertFalse(caught.exception.diagnostic['retryable']);self.opener.open.assert_called_once()
        self.assertIn('keep_verification_enabled',str(caught.exception))
    def test_dns_and_connection_failure_categories(self):
        for error,category in [(socket.gaierror(socket.EAI_AGAIN,'sensitive'),'dns_resolution'),(ConnectionResetError(errno.ECONNRESET,'sensitive'),'network_connection')]:
            self.opener.open.reset_mock();self.opener.open.side_effect=urllib.error.URLError(error)
            with self.assertRaises(support.GatewayObservationError) as caught:
                support.gateway_observation(self.config,attempts=1)
            self.assertEqual(caught.exception.diagnostic['category'],category)
            self.assertTrue(caught.exception.diagnostic['retryable'])
    def test_malformed_or_oversized_body_fails_without_disclosing_body(self):
        for body in (b'never-log-this-credential',b'x'*32769):
            self.opener.open.reset_mock();self.opener.open.side_effect=None
            self.opener.open.return_value=io.BytesIO(body)
            with self.assertRaises(support.GatewayObservationError) as caught:
                support.gateway_observation(self.config)
            self.assertEqual(caught.exception.diagnostic['category'],'invalid_response')
            self.opener.open.assert_called_once()
    def test_wrong_device_is_never_retried_or_accepted(self):
        self.opener.open.return_value=self.response(dict(self.good,device_id='other'))
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertEqual(caught.exception.diagnostic['category'],'identity_or_protocol_mismatch')
        self.opener.open.assert_called_once()
    def test_retry_options_and_insecure_origins_are_rejected_before_network(self):
        for attempts in (0,4,True):
            with self.assertRaises(ValueError):support.gateway_observation(self.config,attempts=attempts)
        with self.assertRaises(RuntimeError):support.gateway_observation(dict(self.config,gateway_url='http://gateway.example'))
        self.opener.open.assert_not_called()
    def test_redirect_is_not_followed_with_device_credential(self):
        self.opener.open.side_effect=self.reject(302)
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertEqual(caught.exception.diagnostic['http_status'],302)
        self.assertIsInstance(self.build.call_args.args[0],support.NoRedirect)
        self.assertIsNone(support.NoRedirect().redirect_request(None,None,302,None,None,'https://other.example'))
    def test_untrusted_ray_header_is_not_echoed(self):
        self.opener.open.side_effect=self.reject(403,ray='secret\nAuthorization: value')
        with self.assertRaises(support.GatewayObservationError) as caught:
            support.gateway_observation(self.config)
        self.assertNotIn('cf_ray',caught.exception.diagnostic)
    def test_permanent_failure_stops_stability_wait(self):
        self.opener.open.side_effect=self.reject(403)
        with mock.patch.object(support,'local_observation',return_value={}):
            with self.assertRaises(support.GatewayObservationError):
                support.wait_ready(pathlib.Path('/unused'),self.config,'0.3.4',0,0,timeout=3,stable_seconds=1)
        self.opener.open.assert_called_once();self.sleep.assert_not_called()

class UpdaterDiagnosticTests(unittest.TestCase):
    def test_preflight_error_receipt_proves_no_install_and_keeps_safe_diagnostic(self):
        spec=importlib.util.spec_from_file_location('updater_network_test',SCRIPTS/'upgrade-code-agent.py')
        updater=importlib.util.module_from_spec(spec);spec.loader.exec_module(updater)
        import argparse
        with tempfile.TemporaryDirectory() as directory:
            base=pathlib.Path(directory);(base/'bin').mkdir()
            (base/'bin/remote-hosts-code').write_bytes(b'old')
            candidate=base/'candidate';candidate.write_bytes(b'new')
            (base/'agent.json').write_text(json.dumps({'state_dir':str(base/'state')}))
            result=base/'result.json'
            args=argparse.Namespace(candidate=candidate,sha256=updater.sha(candidate),version='0.3.4',result=result,idle_timeout=0)
            error=support.GatewayObservationError({'stage':'gateway_readiness','category':'network_timeout','retryable':True,'attempts':3,'next_action':'retry_same_readonly_probe_with_backoff'})
            with mock.patch.object(updater,'gateway_observation',side_effect=error),mock.patch.object(updater.subprocess,'check_output',return_value='remote-hosts-code 0.3.4'),mock.patch.object(updater.subprocess,'run') as run,contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit):updater.perform_upgrade(args,base)
            proof=json.loads(result.read_text())
            self.assertEqual(proof['phase'],'gateway_preflight');self.assertFalse(proof['service_changed'])
            self.assertEqual(proof['diagnostic']['category'],'network_timeout');run.assert_not_called()
            self.assertEqual((base/'bin/remote-hosts-code').read_bytes(),b'old')

if __name__=='__main__':unittest.main()

import importlib.util
import io
import json
import hashlib
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import sync_bundle
import maintenance_client

class Collaboration(unittest.TestCase):
    def test_only_changed_files_are_bundled_and_conflicts_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);(root/'a').write_bytes(b'changed');(root/'b').write_bytes(b'old')
            files=[{'path':n,'sha256':hashlib.sha256(data).hexdigest(),'size':len(data),'expected_version':'absent'} for n,data in [('a',b'changed'),('b',b'old')]]
            plan={'manifest_id':'id','files':files,'actions':[{'path':'a','action':'upload'},{'path':'b','action':'unchanged'}]}
            result=sync_bundle.pack(root,plan,root/'bundle');self.assertEqual(result['changed_files'],1);self.assertEqual(result['reused_files'],1)
            with self.assertRaises(ValueError):sync_bundle.pack(root,plan,root/'bundle')
            (root/'a').write_bytes(b'other')
            with self.assertRaises(ValueError):sync_bundle.pack(root,plan,root/'changed')
            self.assertFalse((root/'changed').exists())
    def test_symlink_bundle_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);(root/'a').write_bytes(b'x');(root/'link').symlink_to('a');plan={'manifest_id':'id','files':[{'path':'link','size':1,'sha256':hashlib.sha256(b'x').hexdigest()}],'actions':[{'path':'link','action':'upload'}]}
            with self.assertRaises(ValueError):sync_bundle.pack(root,plan,root/'bundle')
    def test_lease_uses_bounded_owned_action_and_filters_private_fields(self):
        lease=maintenance_client.MaintenanceLease({'gateway_url':'https://example.test','device_token':'secret-value'})
        with mock.patch.object(lease,'_call',return_value={}) as call:
            lease.acquire({'state':'waiting_idle'});self.assertTrue(lease.active)
            lease.close({'state':'upgraded'});self.assertFalse(lease.active)
            self.assertEqual([v.args[0] for v in call.call_args_list],['acquire','report','report','release'])
    def test_failed_release_is_not_reported_as_released(self):
        lease=maintenance_client.MaintenanceLease({});lease.active=True
        with mock.patch.object(lease,'_call',side_effect=RuntimeError('offline')):
            result=lease.close({'state':'failed'});self.assertFalse(result['released']);self.assertIn('expires',result['recovery'])

class TargetRelease(unittest.TestCase):
    def test_failed_target_does_not_block_peer_acceptance(self):
        import release_targets
        seen=[]
        def target(name):
            if name=='busy':return {'state':'deferred_busy','service_changed':False}
            return {'state':'accepted'}
        v=release_targets.selected(['busy','healthy'],target,lambda t,r:seen.append((t,r['state'])))
        self.assertFalse(v['all_targets_accepted']);self.assertEqual(v['targets']['healthy']['state'],'accepted');self.assertEqual(len(seen),2)
    def test_exception_preserves_other_target_result(self):
        import release_targets
        def action(t):
            if t=='bad':raise RuntimeError('failure')
            return {'state':'accepted'}
        v=release_targets.selected(['bad','good'],action,lambda *x:None);self.assertEqual(v['targets']['good']['state'],'accepted');self.assertEqual(v['targets']['bad']['failure_type'],'RuntimeError')

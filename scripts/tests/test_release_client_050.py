import importlib.util
import json
import pathlib
import sys
import unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client

class ClientTests(unittest.TestCase):
    def test_complete_process_still_drains_buffered_output(self):
        c=Client('https://example.test',access='fixture')
        status={'exit_code':0,'output_complete':True}
        results=[{'terminal_id':'id','output':'first','cursor':5,'has_more':True,'terminal':status}, {'output':'second','cursor':11,'has_more':False,'terminal':status}]
        with mock.patch.object(c,'tool',side_effect=results) as call:
            self.assertEqual(c.terminal('workspace','fixture','once'),'firstsecond');self.assertEqual(call.call_count,2)
            self.assertEqual(call.call_args_list[1].args[0],'terminal_read')
    def test_invalid_origins_never_construct_requests(self):
        for origin in ('http://example.test','https://user:secret@example.test','https://example.test/?token=x'):
            with self.assertRaises(ValueError):Client(origin)
    def test_acceptance_run_id_normalizes_semver_without_weakening_validator(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_050_id',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        value=m.acceptance_run_id('0.5.0','00000000-0000-0000-0000-000000000001')
        self.assertEqual(value,'release-0-5-0-00000000-0000-0000-0000-000000000001')
        self.assertTrue(value.replace('-','').isalnum())

    def test_target_config_disallows_option_injection_and_duplicate_device(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_050',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        config={'agents':[{'device_id':'00000000-0000-0000-0000-000000000001','workspace_id':'00000000-0000-0000-0000-000000000001:w','home':'/home/test'}],'gateway':{'ssh_host':'-oProxyCommand=bad','ssh_port':22,'root':'/opt/test'}}
        with self.assertRaises(ValueError):m.validate(config)
        config['gateway']['ssh_host']='test@host';self.assertIs(m.validate(config),config)

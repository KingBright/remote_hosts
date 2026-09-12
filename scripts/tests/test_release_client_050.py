import importlib.util
import json
import pathlib
import sys
import unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from release_client import Client

COLLAB_PATH=pathlib.Path(__file__).resolve().parents[1]/'check-collaboration.py'
COLLAB_SPEC=importlib.util.spec_from_file_location('check_collaboration_080',COLLAB_PATH)
check_collaboration=importlib.util.module_from_spec(COLLAB_SPEC);COLLAB_SPEC.loader.exec_module(check_collaboration)

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

    def test_gateway_staging_retries_partial_scp_and_atomically_publishes(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_080_stage',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        expected='a'*64;dest='/release/artifact';temp=dest+'.partial-'+expected[:12];remote={};scp_calls=[]
        def run(argv, timeout=180):
            if argv[0]=='scp':
                scp_calls.append(list(argv))
                if len(scp_calls)==1:
                    remote[temp]='partial'
                    raise __import__('subprocess').CalledProcessError(1,argv)
                remote[temp]=expected
                return ''
            command=argv[-1]
            if command.startswith('if test -f '):
                key=temp if temp in command else dest
                return (remote.get(key,'absent')+'  '+key) if key in remote else 'absent'
            if command.startswith('rm -f '):
                remote.pop(temp,None);return ''
            if command.startswith('mv -f '):
                remote[dest]=remote.pop(temp);return ''
            if command.startswith('sha256sum '):
                return remote[dest]+'  '+dest
            raise AssertionError(command)
        result=m.stage_remote_artifact(run,['ssh'],['scp'],'root@example',pathlib.Path('/local/artifact'),dest,expected,attempts=3)
        self.assertEqual(result['state'],'staged');self.assertEqual(result['attempts'],2)
        self.assertEqual(len(scp_calls),2);self.assertEqual(remote[dest],expected);self.assertNotIn(temp,remote)

    def test_accept_only_uses_current_controller_acceptance_scripts(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_080_scripts',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        standard,collaboration=m.acceptance_scripts(pathlib.Path('/project'))
        self.assertEqual(standard,pathlib.Path('/project/scripts/check-code-gateway.py'))
        self.assertEqual(collaboration,pathlib.Path('/project/scripts/check-collaboration.py'))

    def test_collaboration_attempt_identity_is_stable_unique_and_gitignored(self):
        a=check_collaboration.acceptance_identity('release-0-8-0-device-attempt-a','12345678-device')
        b=check_collaboration.acceptance_identity('release-0-8-0-device-attempt-b','12345678-device')
        self.assertEqual(a,check_collaboration.acceptance_identity('release-0-8-0-device-attempt-a','12345678-device'))
        self.assertNotEqual(a,b)
        self.assertTrue(a[0].startswith('target/remote-hosts-accept/12345678-'))
        self.assertLessEqual(len(a[1]),64)
        with self.assertRaises(ValueError):check_collaboration.acceptance_identity('../bad','12345678-device')

    def test_collaboration_workspace_open_has_bounded_same_key_retry(self):
        source=(pathlib.Path(__file__).resolve().parents[1]/'check-collaboration.py').read_text()
        self.assertIn("'idempotency_key':key+'-open'",source)
        self.assertIn('for attempt in range(3):',source)
        self.assertIn("c.tool('workspace_open',arguments)",source)

    def test_accept_only_requires_verified_installed_runtime(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_080_accept',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        sha='a'*64
        device={'online':True,'capabilities':{'version':'0.8.0'},'maintenance':{'state':'open'},
                'upgrade':{'receipt':{'state':'upgraded','version':'0.8.0','candidate_sha256':sha,
                'installed_sha256':sha,'gateway_verified':True,'all_lanes_verified':True,'stable_seconds':15.1}}}
        self.assertTrue(m.acceptance_ready(device,'0.8.0',sha))
        for mutation in ('offline','version','maintenance','sha','lanes','stability'):
            changed=json.loads(json.dumps(device))
            if mutation=='offline':changed['online']=False
            elif mutation=='version':changed['capabilities']['version']='0.7.1'
            elif mutation=='maintenance':changed['maintenance']['state']='draining'
            elif mutation=='sha':changed['upgrade']['receipt']['installed_sha256']='b'*64
            elif mutation=='lanes':changed['upgrade']['receipt']['all_lanes_verified']=False
            else:changed['upgrade']['receipt']['stable_seconds']=14.9
            self.assertFalse(m.acceptance_ready(changed,'0.8.0',sha),mutation)

    def test_target_config_disallows_option_injection_and_duplicate_device(self):
        path=pathlib.Path(__file__).resolve().parents[1]/'publish-code.py';spec=importlib.util.spec_from_file_location('publish_050',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        config={'agents':[{'device_id':'00000000-0000-0000-0000-000000000001','workspace_id':'00000000-0000-0000-0000-000000000001:w','home':'/home/test'}],'gateway':{'ssh_host':'-oProxyCommand=bad','ssh_port':22,'root':'/opt/test'}}
        with self.assertRaises(ValueError):m.validate(config)
        config['gateway']['ssh_host']='test@host';self.assertIs(m.validate(config),config)

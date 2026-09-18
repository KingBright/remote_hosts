"""Offline devices must not block reachable upgrades or count as accepted."""
import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('fleet_offline_0104', SCRIPTS/'fleet-upgrade.py')
fleet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fleet)


class OfflineFleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = pathlib.Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.controller = {'device_id':'controller','name':'controller','online':True,'converged':True}
        self.worker = {'device_id':'worker','name':'worker','online':True,'converged':True}
        self.offline = {'device_id':'offline','name':'offline','online':False,'converged':False}
        self.observation = {'all_converged':False,'summary':{'gateway_converged':True},'devices':[self.controller,self.offline]}
        self.state = {'agents':{},'version':'0.10.4'}

    def finish(self, observation):
        with contextlib.redirect_stdout(io.StringIO()):
            return fleet.finish_observed_fleet({},self.directory,self.directory,'0.10.4',self.state,observation)

    def test_online_devices_run_first_controller_last_and_offline_is_deferred(self):
        ordered,deferred = fleet.rollout_targets([self.controller,self.offline,self.worker],'controller')
        self.assertEqual([d['device_id'] for d in ordered],['worker','controller'])
        self.assertEqual(deferred,[self.offline])

    def test_offline_controller_is_not_contacted(self):
        self.assertEqual(fleet.rollout_targets([self.offline],'offline'),([],[self.offline]))

    def test_only_online_scope_is_accepted_and_offline_is_reported_pending(self):
        with mock.patch.object(fleet,'accept_fleet',return_value=self.directory/'proof.json') as accept:
            self.assertTrue(self.finish(self.observation))
        self.assertEqual(accept.call_args.args[-1]['devices'],[self.controller])
        self.assertEqual(self.state['state'],'waiting_online')
        self.assertFalse(self.state['all_converged'])
        self.assertTrue(self.state['online_converged'])
        self.assertEqual(self.state['pending_device_ids'],['offline'])
        self.assertEqual(json.loads((self.directory/'fleet.json').read_text())['acceptance_scope'],'online_devices')

    def test_online_failure_cannot_be_hidden_by_an_offline_device(self):
        observation = dict(self.observation,devices=[dict(self.controller,converged=False),self.offline])
        with mock.patch.object(fleet,'accept_fleet') as accept:
            self.assertFalse(self.finish(observation))
        accept.assert_not_called()

    def test_old_gateway_cannot_count_as_online_success(self):
        with mock.patch.object(fleet,'accept_fleet') as accept:
            self.assertFalse(self.finish(dict(self.observation,summary={'gateway_converged':False})))
        accept.assert_not_called()

    def test_full_convergence_has_full_acceptance_scope(self):
        observation = dict(self.observation,all_converged=True,devices=[self.controller,self.worker])
        with mock.patch.object(fleet,'accept_fleet',return_value=self.directory/'proof.json'):
            self.assertTrue(self.finish(observation))
        self.assertEqual(self.state['state'],'passed')
        self.assertTrue(self.state['all_converged'])
        self.assertEqual(self.state['acceptance_scope'],'all_devices')
        self.assertEqual(self.state['pending_device_ids'],[])

    def test_no_online_agent_does_not_fabricate_acceptance(self):
        with mock.patch.object(fleet,'accept_fleet') as accept:
            self.assertTrue(self.finish(dict(self.observation,devices=[self.offline])))
        accept.assert_not_called()
        self.assertFalse(self.state['online_converged'])
        self.assertIsNone(self.state['acceptance'])


if __name__ == '__main__':
    unittest.main()

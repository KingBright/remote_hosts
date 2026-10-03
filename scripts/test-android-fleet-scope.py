#!/usr/bin/env python3
"""Offline regression tests for mixed desktop/Android package selection."""
import copy, importlib.util, pathlib, tempfile, unittest
from unittest import mock

path = pathlib.Path(__file__).with_name('fleet-upgrade.py')
spec = importlib.util.spec_from_file_location('fleet_upgrade', path)
fleet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fleet)

def device(name, platform, converged=True, online=True):
    return {'device_id': name, 'name': name, 'capabilities': {'platform': platform},
            'converged': converged, 'online': online}

def snapshot(devices, converged=False, gateway=True):
    return {'devices': devices, 'all_converged': converged, 'summary': {'gateway_converged': gateway}}

class AndroidFleetScopeTest(unittest.TestCase):
    def test_desktop_platforms_unchanged(self):
        for platform in ('macos', 'linux', 'windows'):
            with self.subTest(platform=platform):
                self.assertEqual(fleet.platform(device(platform, platform)), platform)

    def test_android_never_falls_back_to_linux(self):
        with self.assertRaisesRegex(ValueError, 'android_requires_separate_apk_release'):
            fleet.platform(device('phone', 'android'))

    def test_mixed_scope_excludes_android_but_keeps_receipt(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos'), device('phone', 'android', False)]))
        self.assertEqual([d['device_id'] for d in value['devices']], ['mac'])
        self.assertEqual(value['excluded_devices'][0]['device_id'], 'phone')
        self.assertTrue(value['desktop_converged'])
        self.assertFalse(value['all_converged'])

    def test_does_not_mutate_gateway_snapshot(self):
        original = snapshot([device('mac', 'macos'), device('phone', 'android', False)])
        expected = copy.deepcopy(original)
        fleet.desktop_scope(original)
        self.assertEqual(original, expected)

    def test_pending_desktop_not_converged(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos', False), device('phone', 'android', False)]))
        self.assertFalse(value['desktop_converged'])

    def test_offline_desktop_not_hidden(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos', False, False), device('phone', 'android', False)]))
        self.assertFalse(value['desktop_converged'])
        ordered, deferred = fleet.rollout_targets(value['devices'], None)
        self.assertFalse(ordered)
        self.assertEqual([d['device_id'] for d in deferred], ['mac'])

    def test_gateway_not_upgraded_is_not_convergence(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos')], gateway=False))
        self.assertFalse(value['desktop_converged'])

    def test_mixed_report_never_claims_all_devices_upgraded(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos'), device('phone', 'android', False)]))
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            state = {'agents': {}}
            with mock.patch.object(fleet, 'accept_fleet', return_value=root/'accepted.json') as accept:
                self.assertTrue(fleet.finish_observed_fleet({}, root, root, '0.10.24', state, value))
                self.assertEqual([d['device_id'] for d in accept.call_args.args[-1]['devices']], ['mac'])
            self.assertEqual(state['acceptance_scope'], 'desktop_devices')
            self.assertTrue(state['desktop_converged'])
            self.assertFalse(state['all_converged'])
            self.assertEqual(state['excluded_devices'][0]['device_id'], 'phone')

    def test_desktop_only_report_preserves_full_scope(self):
        value = fleet.desktop_scope(snapshot([device('mac', 'macos')], converged=True))
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); state = {'agents': {}}
            with mock.patch.object(fleet, 'accept_fleet', return_value=root/'accepted.json'):
                self.assertTrue(fleet.finish_observed_fleet({}, root, root, '0.10.24', state, value))
            self.assertTrue(state['all_converged'])
            self.assertEqual(state['acceptance_scope'], 'all_devices')

    def test_android_only_does_not_invoke_desktop_validator(self):
        value = fleet.desktop_scope(snapshot([device('phone', 'android', False)]))
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); state = {'agents': {}}
            with mock.patch.object(fleet, 'accept_fleet') as accept:
                self.assertTrue(fleet.finish_observed_fleet({}, root, root, '0.10.24', state, value))
                accept.assert_not_called()
            self.assertFalse(state['all_converged'])
            self.assertIsNone(state['acceptance'])

if __name__ == '__main__':
    unittest.main(verbosity=2)

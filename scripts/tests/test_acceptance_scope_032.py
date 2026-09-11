"""Pure receipt tests; no network, credentials or device mutations."""
import copy
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('acceptance032', pathlib.Path(__file__).resolve().parents[1]/'check-code-gateway.py')
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)

class ReceiptTests(unittest.TestCase):
    def test_tool_catalog_tracks_release_features(self):
        self.assertEqual(len(acceptance.expected_tool_names('0.3.2')), 15)
        self.assertEqual(len(acceptance.expected_tool_names('0.4.0')), 18)
        self.assertEqual(len(acceptance.expected_tool_names('0.5.0')), 19)
        tools = acceptance.expected_tool_names('0.6.0')
        self.assertEqual(len(tools), 21)
        self.assertIn('change_resume', tools)
        self.assertIn('workspace_gc', tools)

    def report(self, count=1):
        rows = [{'device_id':str(i),'name':'device-'+str(i),'agent_version':'0.3.2'} for i in range(count)]
        return {'origin':'https://example.invalid','version':'0.3.2','run_id':'test',
                'selected_device_ids':sorted(d['device_id'] for d in rows), 'devices':rows,
                'test_oauth_grant_revoked':True}

    def test_stable_operation_receipt_ignores_only_live_lifecycle(self):
        base={'operation_id':'op','state':'completed','changed':[{'path':'a'}],
              'operation_lifecycle':{'protocol':1,'device_receipt_delivery':{'pending':0}}}
        replay=copy.deepcopy(base);replay['operation_lifecycle']['device_receipt_delivery']['pending']=1
        self.assertEqual(acceptance.stable_operation_receipt(base), acceptance.stable_operation_receipt(replay))
        changed=copy.deepcopy(replay);changed['changed'][0]['path']='b'
        self.assertNotEqual(acceptance.stable_operation_receipt(base), acceptance.stable_operation_receipt(changed))
        self.assertIn('operation_lifecycle', base)

    def test_one_device_summary_never_claims_two(self):
        report=self.report()
        message=acceptance.acceptance_summary(report)
        self.assertIn('1 selected device(s)', message)
        self.assertNotIn('both', message)
        self.assertNotIn('device-1', message)

    def test_two_device_summary_names_exactly_the_selection(self):
        report=self.report(2)
        message=acceptance.acceptance_summary(report)
        self.assertIn('2 selected device(s)', message)
        self.assertIn('device-0, device-1', message)

    def test_incomplete_empty_wrong_version_or_live_grant_cannot_succeed(self):
        for kind in ('missing','empty','version','grant','duplicate'):
            report=self.report(2)
            if kind=='missing':report['devices'].pop()
            elif kind=='empty':report['devices']=[];report['selected_device_ids']=[]
            elif kind=='version':report['devices'][0]['agent_version']='0.3.1'
            elif kind=='grant':report['test_oauth_grant_revoked']=False
            else:report['devices'][1]=copy.deepcopy(report['devices'][0])
            with self.assertRaises(ValueError, msg=kind):acceptance.acceptance_summary(report)

    def test_saved_report_must_match_origin_version_run_and_selection(self):
        base=self.report(2)
        for field in ('origin','version','run_id','selected_device_ids'):
            report=copy.deepcopy(base)
            report[field]=['0'] if field=='selected_device_ids' else 'different'
            with self.assertRaises(ValueError):
                acceptance.validate_report_scope(report,base['origin'],'0.3.2','test',base['devices'])

    def test_partial_matching_report_can_resume_without_counting_unselected_rows(self):
        report=self.report(2);selected=copy.deepcopy(report['devices']);report['devices'].pop()
        acceptance.validate_report_scope(report,report['origin'],'0.3.2','test',selected)
        report['devices'].append({'device_id':'other','name':'other','agent_version':'0.3.2'})
        with self.assertRaises(ValueError):acceptance.validate_report_scope(report,report['origin'],'0.3.2','test',selected)

if __name__=='__main__':unittest.main()

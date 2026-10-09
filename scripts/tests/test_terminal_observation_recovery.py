"""Resume terminal evidence from an original operation without credentials/replay."""
import pathlib
import sys
import unittest
from unittest import mock

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parents[1])]
from release_client import Client


class TerminalContinuationTests(unittest.TestCase):
    def client(self):
        return Client('https://fixture.example', access='synthetic', transport='legacy')

    @staticmethod
    def final(output='complete\n', **fields):
        return {'operation_id':'original', 'terminal_id':'terminal',
                'terminal':{'id':'terminal','state':'exited','exit_code':0,
                            'output_complete':True,'output_truncated':False},
                'output':output,'cursor':len(output.encode()),'raw_cursor_start':0,
                'has_more':False,'output_view':'full',
                'receipt':{'protocol':2,'evidence_complete':True,'evidence_durable':True,
                           'request_record_persisted':False,'durable':False,
                           'durability_scope':'observed_facts_only_not_this_query'}, **fields}

    @staticmethod
    def omitted():
        # Actual 0.10.27 single-operation budget omission has NO terminal metadata.
        return {'operation_id':'original','result_omitted':True,'reason':'response_budget',
                'next_action':'query_this_operation_individually','evidence_complete':False,
                'receipt':{'protocol':2,'execution_state':'exited','process_exit_code':0,
                           'output_complete':True,'evidence_complete':False,
                           'evidence_durable':True,'request_record_persisted':False,
                           'durable':False,'durability_scope':'observed_facts_only_not_this_query'}}

    def test_initial_budget_omission_recovers_identity_by_original_operation(self):
        c=self.client()
        with mock.patch.object(c,'tool',side_effect=[self.omitted(),self.final()]) as call:
            self.assertEqual(c.terminal('workspace','already submitted','original-key'),'complete\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],['terminal_exec','operation_get'])
        self.assertEqual(call.call_args.args[1]['operation_id'],'original')
        self.assertEqual(call.call_args.args[1]['max_bytes'],131072)

    def test_budget_omission_after_running_goes_to_original_terminal_history(self):
        c=self.client()
        running=self.final('begin\n')
        running['terminal'].update(state='running',exit_code=None,output_complete=False)
        with mock.patch.object(c,'tool',side_effect=[running,self.omitted(),self.final()]) as call:
            self.assertEqual(c.terminal('workspace','submitted once','original-key'),'complete\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],
                         ['terminal_exec','operation_get','terminal_read'])
        self.assertEqual(call.call_args.args[1]['terminal_id'],'terminal')
        self.assertEqual(call.call_args.args[1]['cursor'],0)

    def test_new_client_recovers_completed_operation_without_terminal_exec(self):
        c=self.client()
        with mock.patch.object(c,'tool',return_value=self.final()) as call:
            self.assertEqual(c.observe_terminal('workspace','original'),'complete\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],['operation_get'])
        self.assertEqual(call.call_args.args[1]['operation_id'],'original')

    def test_new_client_recovers_identity_then_exact_utf8_history(self):
        c=self.client()
        tail=self.final('tail\n',output_view='compact',cursor=99)
        first=self.final('中文\n',has_more=True)
        cursor=len('中文\n'.encode())
        last=self.final('tail\n',raw_cursor_start=cursor,cursor=cursor+5)
        with mock.patch.object(c,'tool',side_effect=[self.omitted(),tail,first,last]) as call:
            self.assertEqual(c.observe_terminal('workspace','original'),'中文\ntail\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],
                         ['operation_get','operation_get','terminal_read','terminal_read'])
        self.assertTrue(all(x.args[1].get('operation_id','original')=='original'
                            for x in call.call_args_list))
        self.assertEqual(call.call_args_list[-1].args[1]['cursor'],cursor)

    def test_identity_probe_is_bounded_and_never_replays(self):
        c=self.client()
        with mock.patch.object(c,'tool',side_effect=[self.omitted(),self.omitted()]) as call:
            with self.assertRaisesRegex(RuntimeError,'terminal_identity_unconfirmed:original'):
                c.observe_terminal('workspace','original')
        self.assertEqual([x.args[0] for x in call.call_args_list],['operation_get','operation_get'])

    def test_changed_original_operation_is_rejected_before_history(self):
        c=self.client();changed=self.final();changed['operation_id']='replacement'
        with mock.patch.object(c,'tool',return_value=changed) as call:
            with self.assertRaisesRegex(RuntimeError,'observation_identity_changed:original'):
                c.observe_terminal('workspace','original')
        self.assertEqual(call.call_count,1)

    def test_changed_operation_during_poll_is_rejected(self):
        c=self.client()
        running=self.final();running['terminal'].update(state='running',exit_code=None,output_complete=False)
        changed=self.final();changed['operation_id']='replacement'
        with mock.patch.object(c,'tool',side_effect=[running,changed]):
            with self.assertRaisesRegex(RuntimeError,'observation_identity_changed:original'):
                c.observe_terminal('workspace','original')

    def test_history_must_prove_durable_final_evidence(self):
        c=self.client()
        tail=self.final('tail\n',output_view='compact',cursor=99)
        bad=self.final();bad['receipt']['evidence_durable']=False
        with mock.patch.object(c,'tool',side_effect=[tail,bad]):
            with self.assertRaisesRegex(RuntimeError,'terminal_final_evidence_unconfirmed'):
                c.observe_terminal('workspace','original')

    def test_nonzero_final_exit_does_not_reexecute_or_read_history(self):
        c=self.client();bad=self.final();bad['terminal']['exit_code']=7
        with mock.patch.object(c,'tool',return_value=bad) as call:
            with self.assertRaisesRegex(RuntimeError,'terminal_failed:terminal'):
                c.observe_terminal('workspace','original')
        self.assertEqual(call.call_count,1)

    def test_identity_change_during_probe_is_rejected(self):
        c=self.client();changed=self.final();changed['operation_id']='replacement'
        with mock.patch.object(c,'tool',side_effect=[self.omitted(),changed]) as call:
            with self.assertRaisesRegex(RuntimeError,'observation_identity_changed:original'):
                c.observe_terminal('workspace','original')
        self.assertEqual(call.call_count,2)

    def test_omitted_running_status_can_finish_through_exact_history(self):
        c=self.client()
        running=self.final('begin\n')
        running['terminal'].update(state='running',exit_code=None,output_complete=False)
        page=self.final('begin\n')
        page['terminal'].update(state='running',exit_code=None,output_complete=False)
        page['receipt']['evidence_complete']=False
        finished=self.final('begin\nend\n')
        last=self.final('end\n',raw_cursor_start=6,cursor=10)
        with mock.patch.object(c,'tool',side_effect=[
                running,self.omitted(),page,finished,last]) as call:
            self.assertEqual(c.observe_terminal('workspace','original'),'begin\nend\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],
                         ['operation_get','operation_get','terminal_read',
                          'operation_get','terminal_read'])
        self.assertEqual(call.call_args.args[1]['cursor'],6)

    def test_legacy_terminal_alias_can_drain_the_original_log(self):
        c=self.client()
        status={'exit_code':0,'output_complete':True}
        first={'terminal_id':'legacy','terminal':status,'output':'first',
               'cursor':5,'raw_cursor_start':0,'has_more':True,'output_view':'full'}
        last={'terminal':status,'output':'second','cursor':11,
              'raw_cursor_start':5,'has_more':False,'output_view':'full'}
        with mock.patch.object(c,'tool',side_effect=[first,last]) as call:
            self.assertEqual(c.terminal('workspace','legacy submitted once','key'),'firstsecond')
        self.assertEqual([x.args[0] for x in call.call_args_list],
                         ['terminal_exec','terminal_read'])
        self.assertEqual(call.call_args.args[1]['terminal_id'],'legacy')

    def test_protocol2_cannot_guess_an_operation_from_terminal_id(self):
        c=self.client();value=self.final();del value['operation_id']
        with mock.patch.object(c,'tool',return_value=value) as call:
            with self.assertRaisesRegex(RuntimeError,'observation_missing_operation_identity'):
                c.terminal('workspace','submitted once','key')
        self.assertEqual(call.call_count,1)

    def test_legacy_alias_rejects_an_explicit_changed_operation(self):
        c=self.client()
        first={'terminal_id':'legacy','terminal':{'exit_code':None,'output_complete':False}}
        changed=self.final();changed['operation_id']='replacement'
        with mock.patch.object(c,'tool',side_effect=[first,changed]):
            with self.assertRaisesRegex(RuntimeError,'observation_identity_changed:legacy'):
                c.terminal('workspace','legacy submitted once','key')

    def test_native_disconnect_does_not_fallback_or_recreate_session(self):
        with mock.patch('release_client.NativeSession') as session:
            c=Client('https://fixture.example',access='synthetic',transport='native',
                     native_binary=pathlib.Path(sys.executable))
            session.return_value.rpc.side_effect=TimeoutError('fixture disconnect')
            with mock.patch.object(c,'parsed') as http:
                with self.assertRaises(TimeoutError):
                    c.observe_terminal('workspace','original')
                http.assert_not_called()
            self.assertEqual(session.call_count,1)
            self.assertEqual(session.return_value.rpc.call_count,1)


if __name__=='__main__':
    unittest.main()

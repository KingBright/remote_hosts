"""Client routing, recovery and output completeness contracts; no live credentials."""
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path[:0] = [str(pathlib.Path(__file__).resolve().parents[1]), str(pathlib.Path(__file__).resolve().parent)]
from release_client import Client, OperationIncomplete


class NativeRoutingTests(unittest.TestCase):
    def test_default_uses_installed_native_once(self):
        with mock.patch('release_client.installed_binary', return_value=pathlib.Path(sys.executable)), \
             mock.patch('release_client.NativeSession') as session:
            client = Client('https://fixture.example', access='synthetic', transport='auto')
            with mock.patch.object(client,'parsed') as http:
                for _ in range(3):client.rpc('tools/list',{})
            self.assertEqual(session.call_count,1)
            self.assertEqual(session.return_value.rpc.call_count,3)
            http.assert_not_called()
            self.assertEqual(client.transport_mode,'native')

    def test_explicit_native_missing_fails_without_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            missing=pathlib.Path(temp)/'missing'
            for mode in ('native','auto'):
                with self.assertRaisesRegex(ValueError,'binary_unavailable'):
                    Client('https://fixture.example',access='synthetic',transport=mode,native_binary=missing)

    def test_auto_legacy_is_only_selected_before_any_request_when_native_absent(self):
        with mock.patch('release_client.installed_binary',return_value=pathlib.Path('/not-installed/native')):
            c=Client('https://fixture.example',access='synthetic',transport='auto')
            self.assertEqual(c.transport_mode,'legacy')
            self.assertEqual(c.transport_info()['selection_reason'],'native_absent_before_any_rpc')

    def test_legacy_does_not_require_native_discovery(self):
        with mock.patch('release_client.installed_binary',side_effect=RuntimeError) as discover:
            c=Client('https://fixture.example',access='synthetic',transport='legacy')
            self.assertEqual(c.transport_mode,'legacy')
            discover.assert_not_called()

    def test_native_exception_never_falls_back_or_recreates_session(self):
        with mock.patch('release_client.NativeSession') as session:
            c=Client('https://fixture.example',access='synthetic',transport='native',native_binary=pathlib.Path(sys.executable))
            session.return_value.rpc.side_effect=TimeoutError('uncertain')
            with mock.patch.object(c,'parsed') as http:
                with self.assertRaises(TimeoutError):c.rpc('tools/call',{'name':'terminal_exec'})
                self.assertEqual(session.return_value.rpc.call_count,1)
                http.assert_not_called()
            self.assertEqual(session.call_count,1)

    def test_native_cleanup_failure_still_revokes_grant(self):
        c=Client('https://fixture.example',access='synthetic',transport='legacy')
        c.refresh='synthetic-refresh'
        c._native=mock.Mock()
        c._native.close.side_effect=OSError('fixture')
        with mock.patch.object(c,'call',return_value=(200,{},b'')) as revoke:
            self.assertFalse(c.close())
        self.assertIsNone(c.refresh)
        revoke.assert_called_once()

    def test_unsuccessful_revocation_retains_handle_for_explicit_retry(self):
        c=Client('https://fixture.example',access='synthetic',transport='legacy')
        c.refresh='synthetic-refresh'
        with mock.patch.object(c,'call',side_effect=[(503,{},b''),(200,{},b'')]):
            self.assertFalse(c.close())
            self.assertEqual(c.refresh,'synthetic-refresh')
            self.assertTrue(c.close())
            self.assertIsNone(c.refresh)
        with self.assertRaisesRegex(RuntimeError,'no implicit reconnect'):c.rpc('tools/list',{})

    def test_pending_operation_uses_bounded_wait_without_resubmission(self):
        c=Client('https://fixture.example',access='synthetic',transport='legacy')
        with mock.patch.object(c,'raw',side_effect=[{'pending':True,'operation_id':'original'},{'operation_id':'original','state':'completed'}]) as call:
            c.tool('code_read',{'workspace_id':'w'})
        self.assertEqual(call.call_args_list[1].args,('operation_get',{'operation_id':'original','wait_ms':5000}))


class TransferCompletionTests(unittest.TestCase):
    def test_paused_communication_retains_recovery_without_resubmission(self):
        for state in ('paused', 'awaiting_source'):
            with self.subTest(state=state):
                c=Client('https://fixture.example',access='synthetic',transport='legacy')
                v={'operation_id':'original-transfer','state':state,'pending':False,
                   'resumable':True,'next_action':'transfer_resume',
                   'receipt':{'evidence_complete':False,'retry_policy':'resume_original_transfer_only'}}
                with mock.patch.object(c,'raw',return_value=v) as raw:
                    with self.assertRaises(OperationIncomplete) as caught:
                        c.tool('file_download',{'path':'probe'})
                self.assertEqual(raw.call_count,1)
                self.assertEqual(caught.exception.operation_id,'original-transfer')
                self.assertEqual(caught.exception.state,state)
                self.assertEqual(caught.exception.next_action,'transfer_resume')
                self.assertTrue(caught.exception.resumable)
                self.assertFalse(caught.exception.receipt['evidence_complete'])

    def test_pending_then_paused_is_not_returned_as_success(self):
        c=Client('https://fixture.example',access='synthetic',transport='legacy')
        paused={'operation_id':'original-transfer','state':'paused','pending':False,
                'receipt':{'next_action':'transfer_resume'}}
        with mock.patch.object(c,'raw',side_effect=[{'operation_id':'original-transfer','pending':True},paused]) as raw:
            with self.assertRaises(OperationIncomplete):c.tool('file_upload',{})
        self.assertEqual([v.args[0] for v in raw.call_args_list],['file_upload','operation_get'])
        self.assertEqual(raw.call_args.args[1]['operation_id'],'original-transfer')

    def test_raw_keeps_paused_receipt_available_for_explicit_recovery(self):
        c=Client('https://fixture.example',access='synthetic',transport='legacy')
        value={'operation_id':'original-transfer','state':'paused','resumable':True}
        with mock.patch.object(c,'rpc',return_value={'structuredContent':value}):
            self.assertIs(c.raw('operation_get',{'operation_id':'original-transfer'}),value)


class TerminalObserverTests(unittest.TestCase):
    def client(self):return Client('https://fixture.example',access='synthetic',transport='legacy')

    @staticmethod
    def final(output='ok\n',**fields):
        return {'operation_id':'operation','terminal_id':'terminal','state':'exited',
                'terminal':{'id':'terminal','exit_code':0,'state':'exited','output_complete':True,'output_truncated':False},
                'output':output,'cursor':len(output.encode()),'raw_cursor_start':0,'has_more':False,
                'receipt':{'evidence_complete':True},'output_view':'full',**fields}

    def test_short_command_returns_without_history_read(self):
        c=self.client()
        with mock.patch.object(c,'tool',return_value=self.final()) as call:
            self.assertEqual(c.terminal('w','true','key'),'ok\n')
        self.assertEqual(call.call_count,1)
        self.assertEqual(call.call_args.args[0],'terminal_exec')

    def test_running_command_uses_original_operation_not_recursive_terminal_read(self):
        c=self.client();running=self.final('begin\n');running['terminal']=dict(running['terminal'],exit_code=None,output_complete=False,state='running')
        with mock.patch.object(c,'tool',side_effect=[running,self.final('begin\nend\n')]) as call:
            self.assertEqual(c.terminal('w','sleep 1','key'),'begin\nend\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],['terminal_exec','operation_get'])
        self.assertEqual(call.call_args.args[1]['operation_id'],'operation')

    def test_confirmed_empty_log_can_omit_output(self):
        c=self.client();v=self.final('');del v['output']
        with mock.patch.object(c,'tool',return_value=v):self.assertEqual(c.terminal('w','true','key'),'')

    def test_transport_complete_summary_is_not_the_original_log(self):
        c=self.client();summary=self.final('summary\n');summary.update(output_view='compact',cursor=500)
        raw=self.final('original\n')
        with mock.patch.object(c,'tool',side_effect=[summary,raw]) as call:
            self.assertEqual(c.terminal('w','test','key'),'original\n')
        self.assertEqual(call.call_args.args[0],'terminal_read')
        self.assertEqual(call.call_args.args[1]['cursor'],0)

    def test_complete_response_with_wrong_byte_count_requires_history(self):
        c=self.client();bad=self.final('中文\n');bad['cursor']=3
        with mock.patch.object(c,'tool',side_effect=[bad,self.final('中文\n')]) as call:
            self.assertEqual(c.terminal('w','test','key'),'中文\n')
        self.assertEqual(call.call_count,2)

    def test_timed_out_process_cannot_succeed_via_zero_exit_code(self):
        c=self.client();bad=self.final();bad['terminal']['state']='timed_out'
        with mock.patch.object(c,'tool',return_value=bad):
            with self.assertRaisesRegex(RuntimeError,'terminal_failed'):
                c.terminal('w','test','key')

    def test_tail_preview_requires_exact_paginated_history(self):
        c=self.client();tail=self.final('tail\n',result_omitted=True,raw_cursor_start=500)
        first=self.final('中文\n',has_more=True);last=self.final('tail\n')
        cursor=len('中文\n'.encode());last.update(raw_cursor_start=cursor,cursor=cursor+len('tail\n'))
        with mock.patch.object(c,'tool',side_effect=[tail,first,last]) as call:
            self.assertEqual(c.terminal('w','large command','key'),'中文\ntail\n')
        self.assertEqual([x.args[0] for x in call.call_args_list],['terminal_exec','terminal_read','terminal_read'])
        self.assertEqual(call.call_args_list[1].args[1]['cursor'],0)
        self.assertEqual(call.call_args_list[2].args[1]['cursor'],cursor)

    def test_evidence_gaps_truncation_and_nonzero_are_not_green(self):
        for key,value in [('output_truncated',True),('output_error','lost'),('exit_code',7)]:
            with self.subTest(key=key):
                c=self.client();bad=self.final();bad['terminal'][key]=value
                with mock.patch.object(c,'tool',return_value=bad) as call:
                    with self.assertRaises(RuntimeError):c.terminal('w','test','key')
                self.assertEqual(call.call_count,1)

    def test_changed_observed_terminal_is_rejected(self):
        c=self.client();first=self.final();first['terminal']['exit_code']=None
        second=self.final();second['terminal']['id']='different'
        with mock.patch.object(c,'tool',side_effect=[first,second]):
            with self.assertRaisesRegex(RuntimeError,'identity_changed'):c.terminal('w','test','key')

    def test_history_gap_or_cursor_regression_is_rejected(self):
        for fields in ({'raw_cursor_start':5},{'cursor':-1},{'cursor':4}):
            with self.subTest(fields=fields):
                c=self.client();tail=self.final(result_omitted=True);bad=self.final('ok\n',**fields)
                with mock.patch.object(c,'tool',side_effect=[tail,bad]):
                    with self.assertRaisesRegex(RuntimeError,'evidence_incomplete'):c.terminal('w','test','key')

    def test_output_complete_is_required_after_history_recovery(self):
        c=self.client();tail=self.final(result_omitted=True);bad=self.final();bad['terminal']['output_complete']=False
        with mock.patch.object(c,'tool',side_effect=[tail,bad]):
            with self.assertRaisesRegex(RuntimeError,'final_evidence_unconfirmed'):c.terminal('w','test','key')

    def test_no_progress_in_history_is_an_error(self):
        c=self.client();tail=self.final(result_omitted=True);empty=self.final('',has_more=True)
        with mock.patch.object(c,'tool',side_effect=[tail,empty]):
            with self.assertRaisesRegex(RuntimeError,'cursor_stalled'):c.terminal('w','test','key')


if __name__=='__main__':unittest.main()

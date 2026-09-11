"""Temporary receipt and state-machine tests. No network, launchd or production I/O."""
import copy
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import release_receipts as r

class Receipts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.expected = {'version':'0.4.1','sha256':'a'*64}
        self.good = {'state':'upgraded','version':'0.4.1','installed_sha256':'a'*64,'candidate_sha256':'a'*64,
                     'gateway_verified':True,'all_lanes_verified':True,'stable_seconds':20,'samples':9,
                     'pid':123,'session':'b'*64}

    def build(self):
        pkg = self.root/'package'; pkg.mkdir()
        files = {'remote-hosts-code-macos-arm64','remote-hosts-code-linux-amd64','upgrade-code-agent.py',
                 'agent_upgrade_support.py','launch-code-upgrade.py','upgrade-code-gateway.py','check-code-gateway.py'}
        artifacts = {}
        for name in files:
            p=pkg/name;p.write_bytes(b'fixture')
            artifacts[name]={'size':p.stat().st_size,'sha256':r.digest(p)}
        proof={'state':'passed','version':'0.4.1','source_inputs_unchanged':True,'snapshot_id':'c'*64,
               'source_inputs':{'x':'d'*64},'checks':{n:{'state':'finished','exit_code':0} for n in r.GATES},
               'functional_tests':{'passed':123,'failed':0,'test_gates_completed_successfully':True}}
        (pkg/'source-verification.json').write_text(json.dumps(proof))
        manifest={'version':'0.4.1','snapshot_id':'c'*64,'source_inputs':proof['source_inputs'],
                  'source_verification_sha256':r.digest(pkg/'source-verification.json'),'artifacts':artifacts}
        (pkg/'manifest.json').write_text(json.dumps(manifest))
        report={'state':'passed','version':'0.4.1','source_inputs_unchanged':True,'snapshot_id':'c'*64,'verify_only':False,
                'stages':{n:{'state':'finished','exit_code':0} for n in r.STAGES},
                'verification':{'sha256':r.digest(pkg/'source-verification.json')},
                'package':{'path':str(pkg),'manifest_sha256':r.digest(pkg/'manifest.json')}}
        path=self.root/'pipeline.json';path.write_text(json.dumps(report))
        return path,pkg

    def test_finished_build_binds_all_artifacts_and_proof(self):
        path,pkg=self.build();v=r.verified_build(path,'0.4.1')
        self.assertEqual(v['tests']['passed'],123);self.assertEqual(v['package'],str(pkg.resolve()))

    def test_missing_producer_fails_immediately(self):
        with self.assertRaises(r.ReleaseError) as e:r.verified_build(self.root/'missing','0.4.1')
        self.assertEqual(e.exception.code,'needs_build')

    def test_running_producer_is_not_a_publishable_plan(self):
        path,_=self.build();v=json.loads(path.read_text());v['state']='running';path.write_text(json.dumps(v))
        with self.assertRaises(r.ReleaseError):r.verified_build(path,'0.4.1')

    def test_artifact_change_is_not_silently_rebuilt(self):
        path,pkg=self.build();p=pkg/'remote-hosts-code-macos-arm64';p.write_bytes(b'changed')
        with self.assertRaises(r.ReleaseError):r.verified_build(path,'0.4.1')
        self.assertEqual(p.read_bytes(),b'changed')

    def test_missing_gate_and_wrong_snapshot_are_rejected(self):
        path,pkg=self.build();v=json.loads(path.read_text());v['stages'].pop('linux_release');path.write_text(json.dumps(v))
        with self.assertRaises(r.ReleaseError):r.verified_build(path,'0.4.1')
        v['stages']['linux_release']={'state':'finished','exit_code':0};v['snapshot_id']='wrong';path.write_text(json.dumps(v))
        with self.assertRaises(r.ReleaseError):r.verified_build(path,'0.4.1')

    def test_missing_proof_and_symlinked_artifact_rejected(self):
        path,pkg=self.build();p=pkg/'source-verification.json';p.write_text('{}')
        with self.assertRaises(r.ReleaseError):r.verified_build(path,'0.4.1')

    def test_both_original_receipts_wait_independently(self):
        clock=[0];seen=[];calls={'a':0,'b':0}
        def reader(name,after):
            def read():
                calls[name]+=1
                return copy.deepcopy(self.good) if clock[0]>=after else None
            return read
        out=r.wait_receipts({'a':reader('a',0),'b':reader('b',4)}, {'a':self.expected,'b':self.expected},
            timeout=8,interval=1,clock=lambda:clock[0],sleep=lambda n:clock.__setitem__(0,clock[0]+n),on_update=seen.append)
        self.assertEqual(set(out),{'a','b'});self.assertEqual(calls['a'],1);self.assertEqual(clock[0],4)
        self.assertEqual(seen[-1],{'a':'verified','b':'verified'})

    def test_absent_receipt_times_out_without_any_install(self):
        clock=[0]
        with self.assertRaises(r.ReleaseError) as e:
            r.wait_receipts({'a':lambda:None},{'a':self.expected},timeout=3,interval=1,
                clock=lambda:clock[0],sleep=lambda n:clock.__setitem__(0,clock[0]+n))
        self.assertEqual(e.exception.code,'receipt_observation_timeout');self.assertEqual(clock[0],3)

    def test_no_change_failed_wrong_version_are_not_healthy(self):
        for replacement in ({'state':'no_change'},{'state':'failed'},{'version':'0.4.0'},{'all_lanes_verified':False},{'stable_seconds':0}):
            value=dict(self.good,**replacement)
            with self.subTest(replacement=replacement),self.assertRaises(r.ReleaseError):
                r.wait_receipts({'a':lambda:value},{'a':self.expected})

    def test_selected_target_sets_must_match(self):
        with self.assertRaises(ValueError):r.wait_receipts({'a':lambda:self.good},{'b':self.expected})

    def test_completed_step_is_not_executed_twice_across_restart(self):
        path=self.root/'steps.json';calls=[]
        for _ in range(2):
            with r.StepJournal(path,{'manifest':'x'}) as j:
                self.assertEqual(j.step('install',{'hash':'a'},lambda:(calls.append(1) or {'result':'ok'})),{'result':'ok'})
        self.assertEqual(calls,[1])

    def test_uncertain_action_cannot_be_replayed_but_can_be_observed(self):
        path=self.root/'steps.json';calls=[]
        def lost():calls.append(1);raise TimeoutError()
        with r.StepJournal(path,{'manifest':'x'}) as j:
            with self.assertRaises(TimeoutError):j.step('install',{},lost)
        with r.StepJournal(path,{'manifest':'x'}) as j:
            with self.assertRaises(r.ReleaseError):j.step('install',{},lost)
            result=j.reconcile('install',{},lambda:{'state':'upgraded'})
            self.assertEqual(result['state'],'upgraded')
        self.assertEqual(calls,[1])

    def test_plan_identity_cannot_replace_previous_history(self):
        path=self.root/'steps.json'
        with r.StepJournal(path,{'manifest':'a'}):pass
        before=path.read_bytes()
        with self.assertRaises(r.ReleaseError),r.StepJournal(path,{'manifest':'b'}):pass
        self.assertEqual(path.read_bytes(),before)

    def test_concurrent_publishers_cannot_take_same_journal(self):
        path=self.root/'steps.json'
        with r.StepJournal(path,{'manifest':'a'}):
            with self.assertRaises(BlockingIOError),r.StepJournal(path,{'manifest':'a'}):pass

if __name__=='__main__':unittest.main()

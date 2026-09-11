"""Disposable process fault injection for build-slot recovery; no production jobs."""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import build_slot


@unittest.skipUnless(os.name == 'posix', 'POSIX build-slot locking')
class BuildRecoveryTests(unittest.TestCase):
    def test_dead_owner_does_not_authorize_reuse_while_detached_child_survives(self):
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory).resolve()
            slot = base/'slot'
            code = '''import sys,pathlib,subprocess,time,json,os
sys.path.insert(0,sys.argv[1]);import build_slot
base=pathlib.Path(sys.argv[2])
with build_slot.lease(base/'slot',base/'report.json','fixture'):
 child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
 (base/'child.json').write_text(json.dumps({'pid':child.pid}))
 while True:time.sleep(.05)
'''
            parent = subprocess.Popen([sys.executable, '-c', code, str(SCRIPTS), str(base)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            child_pid = None
            try:
                deadline=time.monotonic()+5
                while not (base/'child.json').exists() and time.monotonic()<deadline:
                    self.assertIsNone(parent.poll())
                    time.sleep(.01)
                child_pid=json.loads((base/'child.json').read_text())['pid']
                parent.kill();parent.wait(timeout=5)
                os.kill(child_pid,0)
                original=(slot/'owner.json').read_bytes()
                with self.assertRaisesRegex(ValueError,'recovery_required'):
                    with build_slot.lease(slot,base/'another.json','other'):
                        self.fail('unclean slot was reused')
                self.assertEqual((slot/'owner.json').read_bytes(),original)
                self.assertFalse((slot/'checkout').exists())
            finally:
                if parent.poll() is None:
                    parent.kill();parent.wait(timeout=5)
                if child_pid is not None:
                    try:os.kill(child_pid,signal.SIGKILL)
                    except ProcessLookupError:pass

    def test_normal_release_remains_reusable(self):
        with tempfile.TemporaryDirectory() as directory:
            slot=pathlib.Path(directory).resolve()/'slot'
            for candidate in ('a','b'):
                with build_slot.lease(slot,slot.parent/(candidate+'.json'),candidate):
                    self.assertEqual(json.loads((slot/'owner.json').read_text())['state'],'running')
                self.assertEqual(json.loads((slot/'owner.json').read_text())['state'],'released')

if __name__=='__main__':unittest.main()

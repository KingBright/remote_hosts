"""All tests prepare files or inspect templates; never authorize/execute an installation."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import plistlib
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid

SCRIPT=Path(__file__).resolve().parents[1]/'prepare-admin-helper.py'
spec=importlib.util.spec_from_file_location('prepare_admin_helper',SCRIPT)
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.binary=self.root/'helper'
        self.binary.write_bytes(b'\xcf\xfa\xed\xfe'+struct.pack('<I',0x0100000C)+bytes(56))
    def tearDown(self): self.temp.cleanup()
    def prepare(self):
        return m.prepare(self.binary,self.root/'bundle','macos',str(uuid.uuid4()),'test',501,20,'/Users/test')
    def test_default_installer_is_dry_run(self):
        result=self.prepare()
        run=subprocess.run([sys.executable,'-I',str(self.root/'bundle/install.py')],text=True,capture_output=True,check=True)
        report=json.loads(run.stdout)
        self.assertEqual(report['mode'],'dry_run')
        self.assertFalse(report['legacy_cleanup_executed'])
        self.assertFalse(result['installed'])
    def test_nonisolated_installer_refused(self):
        self.prepare()
        run=subprocess.run([sys.executable,str(self.root/'bundle/install.py')],text=True,capture_output=True)
        self.assertNotEqual(run.returncode,0)
        self.assertIn('python3 -I',run.stderr)
    def test_local_python_module_cannot_shadow_stdlib(self):
        self.prepare()
        (self.root/'bundle/json.py').write_text('raise RuntimeError("local module executed")')
        run=subprocess.run([sys.executable,'-I',str(self.root/'bundle/install.py')],text=True,capture_output=True,check=True)
        self.assertEqual(json.loads(run.stdout)['mode'],'dry_run')
    def test_checksums_cover_installer_and_binary(self):
        result=self.prepare()
        self.assertEqual(len(result['files']),5)
        for name,digest in result['files'].items(): self.assertEqual(m.sha(self.root/'bundle'/name),digest)
    def test_tampered_binary_refused(self):
        self.prepare();(self.root/'bundle/remote-hosts-admin').write_bytes(b'replaced')
        run=subprocess.run([sys.executable,'-I',str(self.root/'bundle/install.py')],text=True,capture_output=True)
        self.assertNotEqual(run.returncode,0)
        self.assertIn('verification failed',run.stderr)
    def test_apply_without_independent_digest_refused(self):
        self.prepare()
        run=subprocess.run([sys.executable,'-I',str(self.root/'bundle/install.py'),'--apply','--grant-legacy-cleanup'],text=True,capture_output=True)
        self.assertNotEqual(run.returncode,0)
        self.assertIn('manifest SHA-256 is required',run.stderr)
    def test_output_refuses_overwrite(self):
        self.prepare()
        with self.assertRaises(FileExistsError):self.prepare()
    def test_no_root_uid_grant(self):
        with self.assertRaises(ValueError):m.prepare(self.binary,self.root/'bad','macos',str(uuid.uuid4()),'root',0,0,'/root')
    def test_home_traversal_rejected(self):
        with self.assertRaises(ValueError):m.prepare(self.binary,self.root/'bad','macos',str(uuid.uuid4()),'test',501,20,'/Users/test/../root')
    def test_binary_platform_checked(self):
        with self.assertRaises(ValueError):m.binary_machine(self.binary,'linux')
    def test_macos_socket_persists_across_reboots(self):
        service=plistlib.loads(m.make_service('macos'))
        self.assertIn('/Library/Application Support/RemoteHostsAdmin/helper.sock',service['ProgramArguments'])
        self.assertNotIn('/bin/sh',service['ProgramArguments'])
        self.assertEqual(service['UserName'],'root')
    def test_linux_preserves_agent_sandbox(self):
        service=m.make_service('linux').decode()
        self.assertIn('NoNewPrivileges=yes',service)
        self.assertIn('RestrictAddressFamilies=AF_UNIX',service)
        self.assertNotIn('sudo',service)
        self.assertNotIn('remote-hosts-code.service',service)
    def test_no_auto_cleanup_in_service(self):
        for target in ('macos','linux'):
            service=m.make_service(target).decode()
            self.assertNotIn(' apply ',service)
            self.assertNotIn('easytier',service)
    def test_symlink_binary_rejected(self):
        linked=self.root/'link';linked.symlink_to(self.binary)
        with self.assertRaises(ValueError):m.prepare(linked,self.root/'bad','macos',str(uuid.uuid4()),'test',501,20,'/Users/test')
    def test_bundle_does_not_include_password_fields(self):
        self.prepare()
        policy=json.loads((self.root/'bundle/policy.json').read_text())
        self.assertFalse(any('password' in key or 'token' in key for key in policy))

    def isolated_installer(self, code):
        return subprocess.run([sys.executable, '-I', '-c', code, str(self.root/'bundle')],
                              text=True, capture_output=True, timeout=10)
    def test_manifest_parsing_and_digest_share_one_snapshot(self):
        self.prepare()
        run=self.isolated_installer('''
import hashlib, pathlib, runpy, sys
root=pathlib.Path(sys.argv[1]); module=runpy.run_path(str(root/'install.py'))
path=root/'manifest.json'; original=path.read_bytes()
value, sha=module['json_snapshot'](path)
path.write_text('{"replaced":true}')
assert sha == hashlib.sha256(original).hexdigest()
assert value['helper_version'] == '0.1.0' and 'replaced' not in value
''')
        self.assertEqual(run.returncode,0,run.stderr)
    def test_installer_never_rehashes_a_reopened_manifest(self):
        self.prepare()
        run=self.isolated_installer('''
import pathlib, runpy, sys
root=pathlib.Path(sys.argv[1]); module=runpy.run_path(str(root/'install.py'))
main=module['main']; globals_=main.__globals__; original=globals_['digest']
def guard(path):
    if path.name == 'manifest.json':
        raise AssertionError('Manifest was reopened for its digest')
    return original(path)
globals_['digest']=guard
assert main([]) == 0
''')
        self.assertEqual(run.returncode,0,run.stderr)
        self.assertEqual(json.loads(run.stdout)['mode'],'dry_run')
    def test_oversized_manifest_refused(self):
        self.prepare();(self.root/'bundle/manifest.json').write_bytes(b'{'+b' '*65536+b'}')
        run=self.isolated_installer('''
import pathlib, runpy, sys
module=runpy.run_path(str(pathlib.Path(sys.argv[1])/'install.py')); module['main']([])
''')
        self.assertNotEqual(run.returncode,0)
        self.assertIn('size limit',run.stderr)
    def test_fifo_manifest_refused_without_waiting(self):
        import os
        self.prepare();path=self.root/'bundle/manifest.json';path.unlink();os.mkfifo(path)
        run=self.isolated_installer('''
import pathlib, runpy, sys
module=runpy.run_path(str(pathlib.Path(sys.argv[1])/'install.py')); module['main']([])
''')
        self.assertNotEqual(run.returncode,0)
        self.assertIn('single-link regular file',run.stderr)
    def test_policy_snapshot_requires_the_expected_digest(self):
        self.prepare()
        run=self.isolated_installer('''
import pathlib, runpy, sys
root=pathlib.Path(sys.argv[1]); module=runpy.run_path(str(root/'install.py'))
module['json_snapshot'](root/'policy.json', '0'*64)
''')
        self.assertNotEqual(run.returncode,0)
        self.assertIn('checksum mismatch',run.stderr)

if __name__=='__main__':unittest.main()

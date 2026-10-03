#!/usr/bin/env python3
"""Check the actual release artifact, independently of the Gradle variant name."""
import argparse, hashlib, json, pathlib, re, subprocess, zipfile

ALLOWED_PERMISSIONS = {
    'INTERNET', 'ACCESS_NETWORK_STATE', 'FOREGROUND_SERVICE',
    'FOREGROUND_SERVICE_SPECIAL_USE', 'POST_NOTIFICATIONS', 'RECEIVE_BOOT_COMPLETED',
    'WAKE_LOCK', 'REQUEST_INSTALL_PACKAGES', 'QUERY_ALL_PACKAGES',
}

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--apk', required=True, type=pathlib.Path)
    p.add_argument('--build-tools', type=pathlib.Path,
                   default=pathlib.Path.home()/'Library/Android/sdk/build-tools/35.0.0')
    p.add_argument('--report', required=True, type=pathlib.Path)
    a = p.parse_args()
    checks = []
    def check(name, value):
        if not value:
            raise SystemExit('Release artifact check failed: ' + name)
        checks.append(name)
    def run(*command):
        return subprocess.run(command, check=True, capture_output=True, text=True, timeout=90).stdout
    signature = run(str(a.build_tools/'apksigner'), 'verify', '--verbose', '--print-certs', str(a.apk))
    check('apk_signature_verified', 'Verifies' in signature)
    badging = run(str(a.build_tools/'aapt'), 'dump', 'badging', str(a.apk))
    check('release_package_identity', "package: name='io.remotehosts.agent'" in badging)
    check('not_debuggable', 'application-debuggable' not in badging)
    check('android_11_minimum', "sdkVersion:'30'" in badging)
    permissions = set(re.findall(r"uses-permission(?:-sdk-\d+)?: name='([^']+)'", badging))
    check('only_declared_remote_operation_permissions', bool(permissions) and
          permissions <= {'android.permission.' + item for item in ALLOWED_PERMISSIONS})
    manifest = run(str(a.build_tools/'aapt'), 'dump', 'xmltree', str(a.apk), 'AndroidManifest.xml')
    def disabled(name):
        lines = [line.strip() for line in manifest.splitlines() if 'android:' + name + '(' in line]
        return len(lines) == 1 and '(type 0x12)0x0' in lines[0]
    check('backups_disabled', disabled('allowBackup'))
    check('cleartext_network_disabled', disabled('usesCleartextTraffic'))
    check('no_release_test_trust_override', 'networkSecurityConfig' not in manifest)
    check('no_release_instrumentation', 'E: instrumentation' not in manifest)
    with zipfile.ZipFile(a.apk) as z:
        names = z.namelist()
        check('no_signing_material_or_fixture_resources', not any(
            item.lower().endswith(('.p12', '.pfx', '.jks', '.keystore', '.pem', '.key')) or
            any(term in item.lower() for term in ('fixture_ca', 'integration_network_security', 'integration-config'))
            for item in names))
        dex = [z.read(item) for item in names if re.fullmatch(r'classes\d*\.dex', item)]
        check('no_fixture_or_instrumentation_classes', bool(dex) and not any(
            marker in data for data in dex
            for marker in (b'ProbeActivity', b'AgentInstrumentation', b'fixture-sensitive-value', b'FIXTURE_READY')))
    report = {'apk': a.apk.name, 'bytes': a.apk.stat().st_size,
              'sha256': hashlib.sha256(a.apk.read_bytes()).hexdigest(),
              'passed': True, 'checks': checks, 'permissions': sorted(permissions),
              'signing_certificate_verification': signature,
              'scope': 'Artifact packaging checks, not a claim of complete security audit or physical-phone acceptance'}
    a.report.parent.mkdir(parents=True, exist_ok=True)
    a.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps({'passed': True, 'checks': len(checks), 'sha256': report['sha256'], 'bytes': report['bytes']}))

if __name__ == '__main__':
    main()

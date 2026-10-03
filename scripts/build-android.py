#!/usr/bin/env python3
"""Build on an owner's development machine; signing secrets never enter source or command arguments."""
import argparse, hashlib, json, os, pathlib, secrets, shutil, subprocess, time
ROOT = pathlib.Path(__file__).resolve().parents[1]

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda:stream.read(65536), b''): h.update(data)
    return h.hexdigest()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--publish', action='store_true')
    args=parser.parse_args()
    if args.publish:
        parser.error('Build first, run real Android acceptance, then use scripts/publish-android.py --acceptance RECEIPT. Build success alone cannot publish.')
    sdk=pathlib.Path(os.environ.get('ANDROID_HOME',str(pathlib.Path.home()/'Library/Android/sdk')))
    env=os.environ.copy(); env['ANDROID_HOME']=str(sdk); env['ANDROID_SDK_ROOT']=str(sdk)
    env.setdefault('JAVA_HOME','/Library/Java/JavaVirtualMachines/ibm-semeru-open-17.jdk/Contents/Home')
    env['PATH']=str(pathlib.Path(env['JAVA_HOME'])/'bin')+os.pathsep+env.get('PATH','')
    secret=pathlib.Path.home()/'.local/share/remote-hosts-code/android-signing'
    secret.mkdir(parents=True,exist_ok=True,mode=0o700); secret.chmod(0o700)
    password=secret/'password'; key=secret/'release.p12'
    if not password.exists():
        with os.fdopen(os.open(password,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f: f.write(secrets.token_urlsafe(48))
    env['RH_ANDROID_STORE_PASSWORD']=password.read_text().strip(); env['RH_ANDROID_KEYSTORE']=str(key)
    if not key.exists():
        subprocess.run(['keytool','-genkeypair','-keystore',str(key),'-storetype','PKCS12','-storepass:env','RH_ANDROID_STORE_PASSWORD',
          '-keypass:env','RH_ANDROID_STORE_PASSWORD','-alias','remote-hosts-android','-keyalg','RSA','-keysize','3072','-validity','10000',
          '-dname','CN=Remote Hosts Android,OU=Owner Build,O=Remote Hosts'],env=env,check=True,capture_output=True)
        key.chmod(0o600)
    fixture=ROOT/'android/build/integration-tls'
    fixture.mkdir(parents=True,exist_ok=True)
    cert=fixture/'fixture_ca.pem'; tlskey=fixture/'fixture.key'
    if not cert.exists():
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(tlskey),'-out',str(cert),'-days','30',
          '-subj','/CN=Remote Hosts Local Integration CA','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1',
          '-addext','basicConstraints=critical,CA:TRUE'],check=True,capture_output=True)
        tlskey.chmod(0o600)
    raw=ROOT/'android/app/src/debug/res/raw'; raw.mkdir(parents=True,exist_ok=True)
    shutil.copy2(cert,raw/'fixture_ca.pem')
    subprocess.run(['./gradlew','--no-daemon','testDebugUnitTest','lintRelease','assembleDebug','assembleDebugAndroidTest','assembleRelease'],cwd=ROOT/'android',env=env,check=True)
    apk=ROOT/'android/app/build/outputs/apk/release/app-release.apk'
    if not apk.exists(): raise SystemExit('signed release APK missing')
    tools=sdk/'build-tools/35.0.0'
    verify=subprocess.run([str(tools/'apksigner'),'verify','--verbose','--print-certs',str(apk)],env=env,check=True,capture_output=True,text=True).stdout
    badging=subprocess.run([str(tools/'aapt'),'dump','badging',str(apk)],check=True,capture_output=True,text=True).stdout
    if 'application-debuggable' in badging: raise SystemExit('release is debuggable')
    declared=[p for p in (ROOT/'android').rglob('*') if p.is_file() and not any(part in {'build','.gradle','.kotlin','.idea'} for part in p.relative_to(ROOT/'android').parts) and p.name!='local.properties']
    sources={str(p.relative_to(ROOT)):digest(p) for p in sorted(declared)}
    report={'version':'0.1.0','package':'io.remotehosts.agent','apk_sha256':digest(apk),'bytes':apk.stat().st_size,'built_at':int(time.time()),
      'source_hashes':sources,'signature':verify,'badging':badging,'runtime_acceptance':'separate receipt required','signing_material_in_package':False}
    reports=ROOT/'android/app/build/reports'; reports.mkdir(parents=True,exist_ok=True)
    (reports/'delivery.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'apk':str(apk.relative_to(ROOT)),'sha256':report['apk_sha256'],'bytes':report['bytes'],'signed_release':True}))
if __name__=='__main__':main()

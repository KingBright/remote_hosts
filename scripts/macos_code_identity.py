#!/usr/bin/env python3
"""Persistent per-host macOS code-signing identity for Remote Hosts.

The release artifact remains immutable and checksum-verified. On macOS, the
updater signs a local install copy with one host-local certificate so TCC sees a
stable designated requirement across upgrades. Certificate trust is deliberately
a one-time user-authorized action; automation never weakens global trust policy.
"""
import argparse
import fcntl
import hashlib
import json
import os
import pathlib
import secrets
import shutil
import subprocess
import sys
import tempfile

CERT_NAME = "Remote Hosts Local Code Signing"
CODE_IDENTIFIER = "com.remote-hosts.code-agent"


def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _paths(base):
    root = pathlib.Path(base) / "signing"
    return {
        "root": root,
        "keychain": root / "remote-hosts-signing.keychain-db",
        "password": root / "keychain-password",
        "certificate": root / "remote-hosts-code-signing.pem",
        "metadata": root / "identity.json",
        "lock": root / "identity.lock",
        "installed": root / "installed.json",
    }


def _openssl():
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/local/bin/openssl", shutil.which("openssl")):
        if candidate and pathlib.Path(candidate).is_file():
            return candidate
    raise RuntimeError("openssl unavailable; cannot create local signing identity")


def _security(*args, check=True, capture=False, timeout=30):
    options = {"capture_output": True} if capture else {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    return subprocess.run(
        ["/usr/bin/security", *map(str, args)],
        check=check,
        text=True,
        timeout=timeout,
        **options,
    )


def _certificate_sha1(keychain):
    result = _security("find-certificate", "-c", CERT_NAME, "-Z", keychain, capture=True)
    for line in result.stdout.splitlines():
        if line.startswith("SHA-1 hash: "):
            value = line.split(":", 1)[1].strip().lower()
            if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
                return value
    raise RuntimeError("local signing certificate fingerprint unavailable")


def _user_keychains():
    result = _security("list-keychains", "-d", "user", capture=True)
    return [pathlib.Path(line.strip().strip('"')) for line in result.stdout.splitlines() if line.strip()]


def _keychain_in_search_list(keychain):
    target = pathlib.Path(keychain).expanduser().resolve()
    return any(path.expanduser().resolve() == target for path in _user_keychains())


def _ensure_keychain_search_list(keychain):
    target = pathlib.Path(keychain).expanduser().resolve()
    existing = _user_keychains()
    if any(path.expanduser().resolve() == target for path in existing):
        return False
    _security("list-keychains", "-d", "user", "-s", target, *existing)
    return True


def _valid_identity(keychain, fingerprint):
    result = _security("find-identity", "-v", "-p", "codesigning", keychain, check=False, capture=True)
    expected = fingerprint.upper()
    usable = any(
        expected in line and CERT_NAME in line and "CSSMERR_" not in line
        for line in result.stdout.splitlines()
    )
    return result.returncode == 0 and usable and _keychain_in_search_list(keychain)


def _atomic_json(path, value):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary = pathlib.Path(file.name)
        try:
            os.fchmod(file.fileno(), 0o600)
            file.write((json.dumps(value, indent=2) + "\n").encode())
            file.flush()
            os.fsync(file.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def _read_metadata(paths):
    if not paths["metadata"].is_file():
        return None
    value = json.loads(paths["metadata"].read_text())
    if value.get("certificate_name") != CERT_NAME or value.get("code_identifier") != CODE_IDENTIFIER:
        raise RuntimeError("local signing identity metadata changed")
    return value


def create(base):
    """Create private signing material once, without changing trust settings."""
    if sys.platform != "darwin":
        return {"state": "not_required", "platform": sys.platform}
    paths = _paths(base)
    paths["root"].mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(paths["root"], 0o700)
    with paths["lock"].open("a+b") as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        metadata = _read_metadata(paths)
        if metadata is not None:
            required = (paths["keychain"], paths["password"], paths["certificate"])
            if not all(path.is_file() for path in required):
                raise RuntimeError("local signing identity is incomplete; refuse silent regeneration")
            if _certificate_sha1(paths["keychain"]) != metadata["certificate_sha1"]:
                raise RuntimeError("local signing certificate changed")
            return metadata
        partial = [paths[name] for name in ("keychain", "password", "certificate") if paths[name].exists()]
        if partial:
            raise RuntimeError("partial local signing identity exists; inspect before recovery")
        password = secrets.token_hex(32)
        openssl = _openssl()
        with tempfile.TemporaryDirectory(prefix=".identity-", dir=paths["root"]) as tmp:
            stage = pathlib.Path(tmp)
            key = stage / "key.pem"
            cert = stage / "cert.pem"
            bundle = stage / "identity.p12"
            keychain = stage / "signing.keychain-db"
            subprocess.run(
                [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3650", "-sha256",
                 "-subj", f"/CN={CERT_NAME}/O=Remote Hosts",
                 "-addext", "basicConstraints=critical,CA:FALSE",
                 "-addext", "keyUsage=critical,digitalSignature",
                 "-addext", "extendedKeyUsage=codeSigning",
                 "-keyout", str(key), "-out", str(cert)],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
            )
            subprocess.run(
                [openssl, "pkcs12", "-export", "-legacy", "-inkey", str(key), "-in", str(cert),
                 "-out", str(bundle), "-passout", "pass:" + password],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
            )
            _security("create-keychain", "-p", password, keychain)
            try:
                _security("unlock-keychain", "-p", password, keychain)
                _security("set-keychain-settings", "-lut", "315360000", keychain)
                _security("import", bundle, "-k", keychain, "-P", password, "-T", "/usr/bin/codesign")
                partition = _security("set-key-partition-list", "-S", "apple-tool:,apple:", "-s", "-k", password, keychain, check=False, capture=True)
                if partition.returncode != 0:
                    raise RuntimeError("failed to authorize codesign access to local signing key")
                fingerprint = _certificate_sha1(keychain)
                cert_bytes = cert.read_bytes()
                os.replace(keychain, paths["keychain"])
                with paths["password"].open("xb") as file:
                    os.fchmod(file.fileno(), 0o600)
                    file.write((password + "\n").encode())
                    file.flush()
                    os.fsync(file.fileno())
                with paths["certificate"].open("xb") as file:
                    os.fchmod(file.fileno(), 0o600)
                    file.write(cert_bytes)
                    file.flush()
                    os.fsync(file.fileno())
            except Exception:
                _security("delete-keychain", keychain, check=False)
                raise
        metadata = {
            "schema_version": 1,
            "certificate_name": CERT_NAME,
            "certificate_sha1": fingerprint,
            "code_identifier": CODE_IDENTIFIER,
            "keychain": str(paths["keychain"]),
            "certificate": str(paths["certificate"]),
        }
        _atomic_json(paths["metadata"], metadata)
        return metadata


def status(base, create_if_missing=False):
    if sys.platform != "darwin":
        return {"state": "not_required", "platform": sys.platform}
    paths = _paths(base)
    metadata = _read_metadata(paths)
    if metadata is None and create_if_missing:
        metadata = create(base)
    if metadata is None:
        return {"state": "missing", "next_action": "prepare_local_signing_identity"}
    required = (paths["keychain"], paths["password"], paths["certificate"])
    if not all(path.is_file() for path in required):
        raise RuntimeError("local signing identity is incomplete")
    fingerprint = _certificate_sha1(paths["keychain"])
    if fingerprint != metadata["certificate_sha1"]:
        raise RuntimeError("local signing certificate fingerprint changed")
    password = paths["password"].read_text().strip()
    _security("unlock-keychain", "-p", password, paths["keychain"])
    requirement = f'identifier "{CODE_IDENTIFIER}" and certificate leaf = H"{fingerprint}"'
    if not _valid_identity(paths["keychain"], fingerprint):
        return {
            "state": "authorization_required",
            "certificate_name": CERT_NAME,
            "certificate_sha1": fingerprint,
            "certificate": str(paths["certificate"]),
            "keychain": str(paths["keychain"]),
            "code_identifier": CODE_IDENTIFIER,
            "designated_requirement": requirement,
            "next_action": "authorize_local_signing_identity_once",
        }
    return {
        "state": "ready",
        "certificate_name": CERT_NAME,
        "certificate_sha1": fingerprint,
        "keychain": str(paths["keychain"]),
        "code_identifier": CODE_IDENTIFIER,
        "designated_requirement": requirement,
    }


def authorize(base):
    """Request the one-time macOS user trust authorization for this certificate."""
    value = status(base, create_if_missing=True)
    if value["state"] == "ready":
        return value
    if value["state"] != "authorization_required":
        raise RuntimeError("local signing identity cannot be authorized")
    paths = _paths(base)
    _ensure_keychain_search_list(paths["keychain"])
    try:
        result = _security(
            "add-trusted-cert", "-r", "trustRoot", "-p", "codeSign", paths["certificate"],
            check=False, capture=True, timeout=120,
        )
    except subprocess.TimeoutExpired:
        return {
            **value,
            "state": "authorization_required",
            "authorization_outcome": "timed_out_without_confirmation",
            "next_action": "approve_the_single_macos_trust_prompt_from_the_logged_in_user_session_then_retry",
        }
    if result.returncode != 0:
        return {
            **value,
            "state": "authorization_required",
            "authorization_outcome": "not_confirmed",
            "next_action": "approve_the_single_macos_trust_prompt_from_the_logged_in_user_session_then_retry",
        }
    ready = status(base)
    if ready["state"] != "ready":
        raise RuntimeError("macOS accepted trust command but signing identity is still unavailable")
    return ready


def sign_copy(source, destination, base):
    """Sign a copy, leaving the immutable release artifact untouched."""
    value = status(base)
    if value.get("state") != "ready":
        raise RuntimeError("local signing identity is not ready")
    paths = _paths(base)
    source = pathlib.Path(source)
    destination = pathlib.Path(destination)
    shutil.copyfile(source, destination)
    os.chmod(destination, 0o755)
    password = paths["password"].read_text().strip()
    _security("unlock-keychain", "-p", password, paths["keychain"])
    _ensure_keychain_search_list(paths["keychain"])
    with tempfile.NamedTemporaryFile(prefix=".remote-hosts-requirement-", dir=destination.parent) as requirement_file:
        requirement_file.write(("designated => " + value["designated_requirement"] + "\n").encode())
        requirement_file.flush()
        subprocess.run(
            ["/usr/bin/codesign", "--force", "--sign", value["certificate_sha1"], "--keychain", str(paths["keychain"]),
             "--identifier", CODE_IDENTIFIER, "--requirements", requirement_file.name,
             "--timestamp=none", str(destination)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=60,
        )
    subprocess.run(
        ["/usr/bin/codesign", "--verify", "--strict", "--verbose=2", str(destination)],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=30,
    )
    requirement = subprocess.check_output(
        ["/usr/bin/codesign", "-dr", "-", str(destination)], stderr=subprocess.STDOUT, text=True, timeout=30
    )
    if value["designated_requirement"] not in requirement:
        raise RuntimeError("signed candidate designated requirement mismatch")
    return {**value, "installed_sha256": sha(destination)}


def save_installed_metadata(base, value):
    _atomic_json(_paths(base)["installed"], value)


def installed_metadata(base):
    path = _paths(base)["installed"]
    return json.loads(path.read_text()) if path.is_file() else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("status", "prepare", "authorize"))
    parser.add_argument("--base", type=pathlib.Path, default=pathlib.Path.home() / ".local/share/remote-hosts-code")
    args = parser.parse_args()
    if args.action == "prepare":
        create(args.base)
        value = status(args.base)
    elif args.action == "authorize":
        value = authorize(args.base)
    else:
        value = status(args.base)
    print(json.dumps(value, indent=2))
    if value.get("state") not in ("ready", "not_required", "authorization_required", "missing"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

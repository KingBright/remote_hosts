import importlib.util
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "macos_code_identity.py"
SPEC = importlib.util.spec_from_file_location("macos_code_identity_test", SCRIPT)
IDENTITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(IDENTITY)


class MacOsCodeIdentityTests(unittest.TestCase):
    def authorization_required(self, base):
        paths = IDENTITY._paths(base)
        return {
            "state": "authorization_required",
            "certificate_name": IDENTITY.CERT_NAME,
            "certificate_sha1": "a" * 40,
            "certificate": str(paths["certificate"]),
            "keychain": str(paths["keychain"]),
            "code_identifier": IDENTITY.CODE_IDENTIFIER,
            "designated_requirement": f'identifier "{IDENTITY.CODE_IDENTIFIER}" and certificate leaf = H"{"a" * 40}"',
        }

    def test_authorize_scopes_one_time_trust_to_code_signing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            waiting = self.authorization_required(base)
            ready = {**waiting, "state": "ready"}
            completed = subprocess.CompletedProcess([], 0, "", "")
            with mock.patch.object(IDENTITY, "status", side_effect=[waiting, ready]), \
                 mock.patch.object(IDENTITY, "_security", return_value=completed) as security:
                result = IDENTITY.authorize(base)
            self.assertEqual(result["state"], "ready")
            args = security.call_args.args
            self.assertEqual(args[:5], ("add-trusted-cert", "-r", "trustRoot", "-p", "codeSign"))
            self.assertNotIn("-k", args)
            self.assertEqual(security.call_args.kwargs["timeout"], 120)

    def test_valid_identity_rejects_untrusted_or_unsearchable_identity(self):
        fingerprint = "a" * 40
        keychain = pathlib.Path("/tmp/remote-hosts-signing.keychain-db")
        trusted = subprocess.CompletedProcess([], 0,
            f'1) {fingerprint.upper()} "{IDENTITY.CERT_NAME}"\n', "")
        untrusted = subprocess.CompletedProcess([], 0,
            f'1) {fingerprint.upper()} "{IDENTITY.CERT_NAME}" (CSSMERR_TP_NOT_TRUSTED)\n', "")
        searchable = subprocess.CompletedProcess([], 0, f'"{keychain}"\n', "")
        missing = subprocess.CompletedProcess([], 0, '"/Users/test/Library/Keychains/login.keychain-db"\n', "")
        with mock.patch.object(IDENTITY, "_security", return_value=untrusted):
            self.assertFalse(IDENTITY._valid_identity(keychain, fingerprint))
        with mock.patch.object(IDENTITY, "_security", side_effect=[trusted, missing]):
            self.assertFalse(IDENTITY._valid_identity(keychain, fingerprint))
        with mock.patch.object(IDENTITY, "_security", side_effect=[trusted, searchable]):
            self.assertTrue(IDENTITY._valid_identity(keychain, fingerprint))

    def test_ensure_keychain_search_list_prepends_private_keychain_once(self):
        keychain = pathlib.Path("/tmp/remote-hosts-signing.keychain-db")
        listed = subprocess.CompletedProcess([], 0, '"/Users/test/Library/Keychains/login.keychain-db"\n', "")
        completed = subprocess.CompletedProcess([], 0, "", "")
        with mock.patch.object(IDENTITY, "_security", side_effect=[listed, completed]) as security:
            self.assertTrue(IDENTITY._ensure_keychain_search_list(keychain))
        self.assertEqual(security.call_args_list[1].args[:5],
                         ("list-keychains", "-d", "user", "-s", keychain.resolve()))

    def test_authorize_timeout_remains_recoverable_and_does_not_claim_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            waiting = self.authorization_required(base)
            with mock.patch.object(IDENTITY, "status", return_value=waiting), \
                 mock.patch.object(IDENTITY, "_ensure_keychain_search_list", return_value=False), \
                 mock.patch.object(IDENTITY, "_security", side_effect=subprocess.TimeoutExpired("security", 120)):
                result = IDENTITY.authorize(base)
            self.assertEqual(result["state"], "authorization_required")
            self.assertEqual(result["authorization_outcome"], "timed_out_without_confirmation")
            self.assertIn("logged_in_user_session", result["next_action"])

    def test_sign_copy_embeds_explicit_stable_designated_requirement(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            paths = IDENTITY._paths(base)
            paths["root"].mkdir(parents=True)
            paths["password"].write_text("fixture-password\n")
            paths["keychain"].write_bytes(b"fixture-keychain")
            source = base / "candidate"
            destination = base / "installed"
            source.write_bytes(b"candidate-bytes")
            ready = {
                "state": "ready",
                "certificate_name": IDENTITY.CERT_NAME,
                "certificate_sha1": "a" * 40,
                "keychain": str(paths["keychain"]),
                "code_identifier": IDENTITY.CODE_IDENTIFIER,
                "designated_requirement": f'identifier "{IDENTITY.CODE_IDENTIFIER}" and certificate leaf = H"{"a" * 40}"',
            }
            embedded = []

            def run(command, **_kwargs):
                if command[0] == "/usr/bin/codesign" and "--requirements" in command:
                    requirement = pathlib.Path(command[command.index("--requirements") + 1])
                    embedded.append(requirement.read_text())
                return subprocess.CompletedProcess(command, 0, "", "")

            requirement_output = "Executable=fixture\ndesignated => " + ready["designated_requirement"] + "\n"
            with mock.patch.object(IDENTITY, "status", return_value=ready), \
                 mock.patch.object(IDENTITY, "_security"), \
                 mock.patch.object(IDENTITY.subprocess, "run", side_effect=run), \
                 mock.patch.object(IDENTITY.subprocess, "check_output", return_value=requirement_output):
                result = IDENTITY.sign_copy(source, destination, base)
            self.assertEqual(embedded, ["designated => " + ready["designated_requirement"] + "\n"])
            self.assertEqual(result["installed_sha256"], IDENTITY.sha(source))


if __name__ == "__main__":
    unittest.main()

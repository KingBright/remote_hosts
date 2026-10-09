#!/usr/bin/env python3
"""Public-metadata staging/observation only. Never authenticate or upgrade."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile

VERSION = "0.10.26"
SERVICE = "remote-hosts-code-gateway.service"
APPROVED_ROOT = Path("/opt/remote-hosts-code")
RESERVE = 4 * 1024**3
MAX_BUNDLE = 64 * 1024**2
MAX_EXPANDED = 256 * 1024**2


class StageError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__()


def require(condition, code):
    if not condition:
        raise StageError(code)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def regular(path):
    require(not path.is_symlink() and path.is_file(), "not_regular_file")


def identify():
    require(os.geteuid() == 0, "existing_management_account_required")
    result = subprocess.run(
        ["systemctl", "show", SERVICE, "--property=MainPID", "--value"],
        capture_output=True, timeout=10, check=False)
    require(result.returncode == 0, "gateway_service_identity_unavailable")
    pid = int(result.stdout.decode("ascii").strip())
    require(pid > 1, "gateway_not_running")
    value = os.readlink("/proc/" + str(pid) + "/exe")
    require(not value.endswith(" (deleted)"), "gateway_executable_deleted")
    binary = Path(value)
    require(binary.name == "remote-hosts-code", "unexpected_gateway_executable")
    root = binary.parent
    require(root == APPROVED_ROOT or APPROVED_ROOT in root.parents,
            "gateway_outside_existing_installation")
    for parent in [root] + list(root.parents):
        require(not parent.is_symlink(), "symlink_in_installation")
    regular(binary)
    return {"root": root, "binary": binary, "pid": pid, "running": True,
            "installed_sha256": digest(binary)}


def release_paths(identity):
    root = identity["root"]
    releases = root / "releases"
    release = releases / VERSION
    for directory in (releases, release):
        require(not directory.is_symlink(), "symlink_release_directory")
        if directory.exists():
            require(directory.is_dir(), "not_release_directory")
    return release, release / ("remote-hosts-code-" + VERSION + "-bundle.tgz")


def small_json(path):
    regular(path)
    require(path.stat().st_size <= 65536, "receipt_budget_exceeded")
    return json.loads(path.read_text())


def safe_result(value):
    require(isinstance(value, dict), "malformed_upgrade_receipt")
    result = {"state": value.get("state") if value.get("state") in
              ("upgraded", "failed", "preflight", "no_change") else "unrecognized"}
    if value.get("version") == VERSION:
        result["version"] = VERSION
    for key in ("installed_sha256", "candidate_sha256", "previous_sha256"):
        item = value.get(key)
        if isinstance(item, str) and len(item) == 64 and all(c in "0123456789abcdef" for c in item):
            result[key] = item
    if type(value.get("pid")) is int and value["pid"] > 1:
        result["pid"] = value["pid"]
    if type(value.get("service_stopped")) is bool:
        result["service_stopped"] = value["service_stopped"]
    rollback = value.get("rollback")
    if isinstance(rollback, str):
        if rollback.startswith("blocked_task_authorization_policy;"):
            result["rollback"] = "blocked_task_authorization_policy"
        elif rollback.startswith("restored_previous_binary;"):
            result["rollback"] = "restored_previous_binary_live_database_preserved"
        elif rollback.startswith("unchanged_binary_restarted;"):
            result["rollback"] = "unchanged_binary_restarted_live_database_preserved"
        else:
            result["rollback"] = "failed_or_unrecognized"
    # Raw errors, config, credentials, database contents and process args excluded.
    return result


def validate_plan(plan):
    require(plan.get("version") == VERSION, "wrong_gateway_version")
    require(type(plan.get("bundle_bytes")) is int
            and 0 < plan["bundle_bytes"] <= MAX_BUNDLE, "bundle_size_out_of_range")
    for key in ("bundle_sha256", "manifest_sha256", "candidate_sha256"):
        value = plan.get(key)
        require(isinstance(value, str) and len(value) == 64
                and all(c in "0123456789abcdef" for c in value), "invalid_identity")


def inspect_bundle(path, plan):
    regular(path)
    require(path.stat().st_size == plan["bundle_bytes"]
            and digest(path) == plan["bundle_sha256"], "bundle_identity_mismatch")
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        names = [item.name for item in members]
        require(len(names) == len(set(names)), "duplicate_archive_member")
        require(all(item.isfile() and PurePosixPath(item.name).name == item.name
                    and item.name not in (".", "..") for item in members),
                "archive_must_be_flat_regular_files")
        require(sum(item.size for item in members) <= MAX_EXPANDED,
                "expanded_bundle_budget_exceeded")
        require("manifest.json" in names, "manifest_missing")
        manifest_bytes = archive.extractfile("manifest.json").read(MAX_EXPANDED + 1)
        require(hashlib.sha256(manifest_bytes).hexdigest() == plan["manifest_sha256"],
                "manifest_identity_mismatch")
        manifest = json.loads(manifest_bytes)
        require(manifest.get("version") == VERSION
                and type(manifest.get("task_authorization_protocol")) is int
                and manifest["task_authorization_protocol"] == 1,
                "manifest_version_or_policy_mismatch")
        artifacts = manifest["artifacts"]
        require(isinstance(artifacts, dict), "invalid_artifact_catalog")
        expected = set(artifacts) | {"manifest.json", "source-verification.json",
                                     "SHA256SUMS", "README.md"}
        require(set(names) == expected, "archive_inventory_mismatch")
        require(artifacts.get("remote-hosts-code-linux-amd64", {}).get("sha256")
                == plan["candidate_sha256"], "gateway_candidate_mismatch")
        for name, meta in artifacts.items():
            item = archive.getmember(name)
            require(item.size == meta["size"], "artifact_size_mismatch")
            h = hashlib.sha256()
            with archive.extractfile(item) as stream:
                for block in iter(lambda: stream.read(1048576), b""):
                    h.update(block)
            require(h.hexdigest() == meta["sha256"], "artifact_hash_mismatch")
        proof = archive.extractfile("source-verification.json").read(MAX_EXPANDED + 1)
        require(hashlib.sha256(proof).hexdigest()
                == manifest["source_verification_sha256"], "verification_hash_mismatch")
        return manifest


def probe(plan):
    validate_plan(plan)
    try:
        identity = identify()
    except StageError as error:
        # A policy-protected rollback may deliberately leave the service stopped.
        # Read only the release rooted in previously verified executable metadata.
        require(error.code == "gateway_not_running" and isinstance(plan.get("gateway_root"), str),
                "gateway_identity_unavailable")
        root = Path(plan["gateway_root"])
        require(root.is_absolute() and (root == APPROVED_ROOT or APPROVED_ROOT in root.parents)
                and ".." not in root.parts, "unverified_observation_root")
        for parent in [root] + list(root.parents):
            require(not parent.is_symlink(), "symlink_in_observation_root")
        binary = root / "remote-hosts-code"
        regular(binary)
        identity = {"root": root, "binary": binary, "pid": 0, "running": False,
                    "installed_sha256": digest(binary)}
    if plan.get("gateway_root"):
        require(str(identity["root"]) == plan["gateway_root"], "gateway_root_changed")
    release, bundle = release_paths(identity)
    value = {"version": VERSION, "gateway_pid": identity["pid"], "running": identity["running"],
             "gateway_executable": str(identity["binary"]),
             "installed_sha256": identity["installed_sha256"],
             "bundle_path": str(bundle), "bundle_staged": False,
             "marker_present": False, "result_present": False}
    if os.path.lexists(bundle):
        inspect_bundle(bundle, plan)
        value["bundle_staged"] = True
        value["bundle_sha256"] = plan["bundle_sha256"]
        value["bundle_bytes"] = plan["bundle_bytes"]
    marker_path = release / "self-upgrade-request.json"
    result_path = release / "self-upgrade-result.json"
    if os.path.lexists(marker_path):
        marker = small_json(marker_path)
        require(marker.get("version") == VERSION
                and marker.get("bundle_sha256") == plan["bundle_sha256"],
                "existing_upgrade_identity_conflict")
        value["marker_present"] = True
    if os.path.lexists(result_path):
        value["result"] = safe_result(small_json(result_path))
        require(value["result"].get("version") == VERSION,
                "upgrade_result_version_conflict")
        candidate = value["result"].get("candidate_sha256")
        require(candidate is None or candidate == plan["candidate_sha256"],
                "upgrade_result_candidate_conflict")
        value["result_present"] = True
    return value


def stage(plan, stream):
    before = probe(plan)
    require(before["running"], "gateway_not_running")
    require(not before["marker_present"] and not before["result_present"],
            "existing_upgrade_requires_observation")
    if before["bundle_staged"]:
        return before
    identity = identify()
    release, bundle = release_paths(identity)
    require(shutil.disk_usage(identity["root"]).free >= RESERVE + MAX_EXPANDED
            + plan["bundle_bytes"], "four_gib_reserve_required")
    for directory in (release.parent, release):
        if not directory.exists():
            directory.mkdir(mode=0o755)
        require(directory.is_dir() and not directory.is_symlink(),
                "unsafe_release_directory")
    partial = None
    try:
        fd, name = tempfile.mkstemp(prefix=".gateway-release-", suffix=".part", dir=release)
        partial = Path(name)
        count = 0
        with os.fdopen(fd, "wb") as output:
            while count < plan["bundle_bytes"]:
                block = stream.read(min(1048576, plan["bundle_bytes"] - count))
                require(bool(block), "bundle_transport_incomplete")
                output.write(block)
                count += len(block)
            require(not stream.read(1), "bundle_transport_oversize")
            output.flush()
            os.fsync(output.fileno())
        inspect_bundle(partial, plan)
        after = identify()
        require(after["binary"] == identity["binary"] and after["pid"] == identity["pid"]
                and after["installed_sha256"] == identity["installed_sha256"],
                "gateway_identity_changed_during_import")
        require(not os.path.lexists(release / "self-upgrade-request.json")
                and not os.path.lexists(release / "self-upgrade-result.json"),
                "upgrade_started_during_import")
        os.chmod(partial, 0o644)
        # Exclusive publication: never replace an existing version/artifact.
        os.link(partial, bundle)
        partial.unlink()
        partial = None
        directory_fd = os.open(release, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return probe(plan)
    finally:
        if partial is not None:
            partial.unlink(missing_ok=True)


def main():
    os.umask(0o077)
    phase = "request"
    try:
        raw = sys.stdin.buffer.readline(65537)
        require(len(raw) <= 65536, "request_budget_exceeded")
        request = json.loads(raw)
        require(set(request) == {"action", "plan"}
                and request["action"] in ("probe", "stage"), "invalid_staging_action")
        phase = request["action"]
        result = stage(request["plan"], sys.stdin.buffer) if phase == "stage" else probe(request["plan"])
        print(json.dumps(result, sort_keys=True))
    except Exception as error:
        print(json.dumps({"state": "blocked", "phase": phase,
                          "error_code": error.code if isinstance(error, StageError) else "stage_unconfirmed",
                          "error_type": type(error).__name__}))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

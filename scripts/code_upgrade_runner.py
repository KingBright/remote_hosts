#!/usr/bin/env python3
"""Stable launchd entrypoint for immutable Remote Hosts upgrade requests."""
import argparse
import hashlib
import json
import os
import pathlib
import sys


def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_request(request_path):
    request_path = pathlib.Path(request_path).resolve(strict=True)
    request = json.loads(request_path.read_text())
    directory = pathlib.Path(request["directory"]).resolve(strict=True)
    prepared_path = directory / "prepared.json"
    if sha(prepared_path) != request.get("prepared_sha256"):
        raise RuntimeError("stable updater request changed")
    prepared = json.loads(prepared_path.read_text())
    if prepared.get("directory") != str(directory):
        raise RuntimeError("prepared updater directory mismatch")
    manifest = prepared.get("file_sha256", {})
    required = {"upgrade-code-agent.py", "agent_upgrade_support.py", "maintenance_client.py", "macos_code_identity.py", "job.plist"}
    if set(manifest) != required:
        raise RuntimeError("prepared updater helper manifest incomplete")
    for name, expected in manifest.items():
        path = directory / name
        if not path.is_file() or sha(path) != expected:
            raise RuntimeError("prepared updater helper changed: " + name)
    candidate = pathlib.Path(prepared["candidate"]).resolve(strict=True)
    if sha(candidate) != prepared["candidate_sha256"]:
        raise RuntimeError("prepared updater candidate changed")
    result = pathlib.Path(prepared["result"])
    if result.parent != directory:
        raise RuntimeError("prepared updater result escaped request directory")
    return prepared


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True, type=pathlib.Path)
    args = parser.parse_args()
    prepared = validate_request(args.request)
    python = pathlib.Path(sys.executable).resolve(strict=True)
    script = pathlib.Path(prepared["directory"]) / "upgrade-code-agent.py"
    argv = [
        str(python), str(script),
        "--candidate", prepared["candidate"],
        "--sha256", prepared["candidate_sha256"],
        "--version", prepared["version"],
        "--result", prepared["result"],
    ]
    os.execv(str(python), argv)


if __name__ == "__main__":
    main()

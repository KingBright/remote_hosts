#!/usr/bin/env python3
"""Live OAuth/MCP and multi-device acceptance. Secrets stay in memory.

Creates, edits, executes and deletes a tiny probe file in each authorized device's
first root. This is intentionally a mutating acceptance test, not a health probe.
"""
import argparse
import atexit
import shlex
import base64
import hashlib
import http.cookiejar
import json
import pathlib
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_report_scope(report, origin, version, run_id, selected):
    """Do not turn a cached or differently scoped receipt into current success."""
    ids = [d['device_id'] for d in selected]
    if not ids or len(set(ids)) != len(ids):
        raise ValueError('acceptance requires a nonempty unique device selection')
    if (report.get('origin') != origin or report.get('version') != version
            or report.get('run_id') != run_id or report.get('selected_device_ids') != sorted(ids)):
        raise ValueError('receipt origin, version, run or device selection changed')
    rows = report.get('devices', [])
    completed = [d['device_id'] for d in rows]
    if (len(set(completed)) != len(completed) or not set(completed).issubset(ids)
            or any(d.get('agent_version') != version for d in rows)):
        raise ValueError('receipt has duplicate, unselected or wrong-version devices')


def expected_tool_names(version):
    tools = {'devices_list','workspace_open','code_list','code_search','code_read','code_symbols',
             'code_apply_edits','code_diff','terminal_exec','terminal_read','terminal_input',
             'terminal_cancel','operation_get','file_upload','file_download'}
    parsed = tuple(map(int, version.split('.')))
    if parsed >= (0, 4, 0):
        tools |= {'workspace_context','transfer_cancel','transfer_resume'}
    if parsed >= (0, 5, 0):
        tools |= {'files_sync'}
    if parsed >= (0, 6, 0):
        tools |= {'change_resume','workspace_gc'}
    return tools


def stable_operation_receipt(value):
    """Compare durable operation content, not live observation snapshots."""
    stable = dict(value)
    stable.pop('operation_lifecycle', None)
    return stable


def acceptance_summary(report):
    selected = report.get('selected_device_ids', [])
    rows = report.get('devices', [])
    if (not selected or len(selected) != len(set(selected))
            or len(rows) != len(selected)
            or {d['device_id'] for d in rows} != set(selected)
            or any(d.get('agent_version') != report.get('version') for d in rows)
            or report.get('test_oauth_grant_revoked') is not True):
        raise ValueError('acceptance incomplete: no success summary may be emitted')
    names = ', '.join(d['name'] for d in rows)
    return (f"Public TLS/OAuth/MCP and {len(rows)} selected device(s) passed: {names}; "
            f"version {report['version']}; temporary OAuth grant revoked")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin", required=True)
    parser.add_argument("--password-file", required=True, type=pathlib.Path)
    parser.add_argument("--report", required=True, type=pathlib.Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-version", default="0.2.0")
    parser.add_argument("--dispatch-protocol", type=int)
    parser.add_argument("--device-id", action="append", default=[], help="Limit probes to explicit device IDs")
    args = parser.parse_args()
    if not args.run_id.replace("-", "").isalnum():
        raise SystemExit("run-id must be alphanumeric with optional hyphens")
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    opener.addheaders = [("User-Agent", "RemoteHosts-Acceptance/" + args.expected_version)]

    def call(path, data=None, form=False, bearer=None):
        headers = {"Accept": "application/json, text/event-stream"}
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
            data = (urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
        if bearer:
            headers["Authorization"] = "Bearer " + bearer
        req = urllib.request.Request(args.origin + path, data=data, headers=headers)
        try:
            response = opener.open(req, timeout=30)
        except urllib.error.HTTPError as response:
            return response.code, response.headers, response.read()
        with response:
            return response.status, response.headers, response.read()

    def parsed(path, data=None, form=False, bearer=None):
        status, headers, body = call(path, data, form, bearer)
        if status not in (200, 201):
            raise RuntimeError(f"HTTP {status} at {path.split('?')[0]}; response suppressed")
        return json.loads(body)

    metadata = parsed("/.well-known/oauth-protected-resource")
    assert metadata["resource"] == args.origin + "/mcp"
    assert call("/mcp", {})[0] == 401
    redirect = "https://chatgpt.com/connector_platform_oauth_redirect"
    client = parsed("/oauth/register", {"redirect_uris": [redirect], "token_endpoint_auth_method": "none"})
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    query = urllib.parse.urlencode({"client_id": client["client_id"], "redirect_uri": redirect, "response_type": "code", "code_challenge": challenge, "code_challenge_method": "S256", "state": "acceptance-" + args.run_id, "resource": metadata["resource"]})
    status, headers, _ = call("/oauth/authorize?" + query)
    assert status == 200
    nonce = headers["Set-Cookie"].split(";", 1)[0].split("=", 1)[1]
    status, headers, _ = call("/oauth/approve", {"nonce": nonce, "password": args.password_file.read_text().strip()}, form=True)
    assert status == 303
    result = urllib.parse.parse_qs(urllib.parse.urlsplit(headers["Location"]).query)
    assert result["iss"] == [args.origin]
    token = parsed("/oauth/token", {"grant_type": "authorization_code", "client_id": client["client_id"], "code": result["code"][0], "code_verifier": verifier, "redirect_uri": redirect, "resource": metadata["resource"]}, form=True)
    bearer = token["access_token"]
    # Failed acceptance must not leave its temporary OAuth grant active.
    atexit.register(lambda: call("/oauth/revoke", {"token": token["refresh_token"]}, form=True))
    sequence = 0

    def rpc(method, params):
        nonlocal sequence
        sequence += 1
        result = parsed("/mcp", {"jsonrpc": "2.0", "id": sequence, "method": method, "params": params}, bearer=bearer)
        if "error" in result:
            raise RuntimeError("MCP protocol error: " + str(result["error"].get("code")))
        return result["result"]

    rpc("initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "remote-hosts-live-acceptance", "version": "1"}})
    catalog = rpc("tools/list", {})
    names = [tool['name'] for tool in catalog['tools']]
    expected_tools = expected_tool_names(args.expected_version)
    assert len(names) == len(set(names)) and set(names) == expected_tools, 'Unexpected tool catalog'
    upload_descriptor = next(t for t in catalog["tools"] if t["name"] == "file_upload")
    assert upload_descriptor["_meta"]["openai/fileParams"] == ["file"]
    health = parsed("/healthz")
    assert health["version"] == args.expected_version
    if args.dispatch_protocol is not None:
        assert health.get("dispatch_protocol") == args.dispatch_protocol
    if tuple(map(int, args.expected_version.split('.'))) >= (0, 7, 0):
        assert health.get("transfer_limits_protocol") == 1
        assert health.get("default_file_bytes") == 67108864
        assert health.get("max_file_bytes") == 268435456
        assert health.get("storage_reserve_bytes") == 268435456

    def tool(name, arguments, allow_error=False):
        result = rpc("tools/call", {"name": name, "arguments": arguments})
        value = result.get("structuredContent")
        if value is None:
            value = json.loads(result["content"][0]["text"]) if not result.get("isError") else {"error": "tool_failed", "message": result["content"][0]["text"]}
        deadline = time.monotonic() + 300
        while value.get("pending") and time.monotonic() < deadline:
            time.sleep(0.5)
            polled = rpc("tools/call", {"name": "operation_get", "arguments": {"operation_id": value["operation_id"]}})
            value = polled.get("structuredContent") or {"error": "poll_failed"}
        if value.get("pending"):
            raise RuntimeError("Operation pending: " + value["operation_id"])
        if "error" in value and not allow_error:
            raise RuntimeError(name + ": " + str(value.get("message", value["error"]))[:300])
        return value

    def selected_devices():
        found = tool("devices_list", {})["devices"]
        if args.device_id:
            found = [d for d in found if d["device_id"] in args.device_id]
            assert {d["device_id"] for d in found} == set(args.device_id), "An explicitly selected device is unavailable"
        return found

    devices = selected_devices()
    ready_deadline = time.monotonic() + 120
    def ready():
        return bool(devices) and all(d["online"] and d["capabilities"].get("version") == args.expected_version for d in devices)
    while not ready() and time.monotonic() < ready_deadline:
        time.sleep(3)
        devices = selected_devices()
    assert ready(), "Selected agents must be online and report the expected version"
    report = json.loads(args.report.read_text()) if args.report.exists() else {"origin": args.origin, "version": args.expected_version, "run_id": args.run_id, "selected_device_ids":sorted(d['device_id'] for d in devices), "dispatch_protocol": health.get("dispatch_protocol"), "oauth": "passed", "mcp_tool_count": len(catalog["tools"]), "devices": []}
    validate_report_scope(report, args.origin, args.expected_version, args.run_id, devices)
    report['test_oauth_grant_revoked'] = False
    report['state'] = 'running'
    completed = {d["device_id"] for d in report["devices"]}
    for device in devices:
        if device["device_id"] in completed:
            print(device["name"] + ": reusing completed acceptance receipt", flush=True)
            continue
        root = device["capabilities"]["roots"][0]
        key = args.run_id + "-" + device["device_id"]
        opened = tool("workspace_open", {"device_id": device["device_id"], "root": root, "idempotency_key": key + "-open"})
        ws = opened["workspace"]["id"]
        path = ".remote-hosts-code-acceptance-" + args.run_id + "/probe.py"
        content = "def answer():\n    return 41\n\nprint(answer())\n"
        created = tool("code_apply_edits", {"workspace_id": ws, "idempotency_key": key + "-create", "files": [{"path": path, "expected_version": "absent", "action": "create", "content": content}]})
        version = created["changed"][0]["version"]
        ranges = tool("code_read", {"workspace_id": ws, "requests": [{"path": path, "start_line": 1, "end_line": 2}]})
        assert ranges["ranges"][0]["text"] == "def answer():\n    return 41\n"
        outline = tool("code_symbols", {"workspace_id": ws, "path": path})
        assert outline["kind"] == "syntax_tree" and outline["symbols"][0]["name"] == "answer"
        edit = {"workspace_id": ws, "idempotency_key": key + "-edit", "files": [{"path": path, "expected_version": version, "edits": [{"old_text": "return 41", "new_text": "return 42"}]}]}
        edited = tool("code_apply_edits", edit)
        replayed = tool("code_apply_edits", edit)
        assert replayed["operation_id"] == edited["operation_id"], "Duplicate edit must keep operation identity"
        assert stable_operation_receipt(replayed) == stable_operation_receipt(edited), "Duplicate edit must keep durable receipt"
        stale = dict(edit, idempotency_key=key + "-stale")
        assert "error" in tool("code_apply_edits", stale, allow_error=True)
        found = tool("code_search", {"workspace_id": ws, "query": "return 42", "glob": path, "max_bytes": 4096})
        assert len(found["matches"]) == 1
        terminal = tool("terminal_exec", {"workspace_id": ws, "idempotency_key": key + "-test", "command": "python3 " + path, "timeout_seconds": 30})
        deadline = time.monotonic() + 45
        while True:
            output = tool("terminal_read", {"workspace_id": ws, "terminal_id": terminal["terminal_id"]})
            if output["terminal"]["state"] != "running" or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        assert output["terminal"]["exit_code"] == 0 and "42" in output["output"]
        assert output["cursor_format"] == "sanitized_utf8_v1"
        folder = path.rsplit("/", 1)[0]
        binary_path = folder + "/payload.bin"
        imported_path = folder + "/roundtrip.bin"
        bad_path = folder + "/rejected.bin"
        payload = bytes(range(256)) * 8192
        checksum = hashlib.sha256(payload).hexdigest()

        def run_probe(code, suffix):
            command = "python3 -c " + shlex.quote(code)
            term = tool("terminal_exec", {"workspace_id": ws, "idempotency_key": key + suffix, "command": command, "timeout_seconds": 30})
            until = time.monotonic() + 45
            while True:
                captured = tool("terminal_read", {"workspace_id": ws, "terminal_id": term["terminal_id"]})
                if captured["terminal"]["state"] != "running" or time.monotonic() >= until:
                    break
                time.sleep(0.2)
            assert captured["terminal"]["exit_code"] == 0, "binary probe command failed"

        run_probe("with open(" + repr(binary_path) + ", 'xb') as f: f.write(bytes(range(256))*8192)", "-binary-create")
        exported = tool("file_download", {"workspace_id": ws, "idempotency_key": key + "-export", "path": binary_path, "expected_version": checksum})
        assert exported["sha256"] == checksum and exported["size"] == len(payload)
        # The temporary link stays in memory, never in the saved acceptance report.
        with opener.open(urllib.request.Request(exported["download_url"]), timeout=60) as response:
            assert response.headers["Content-Disposition"].startswith("attachment;")
            assert response.headers["Referrer-Policy"] == "no-referrer"
            assert response.read(len(payload)+1) == payload
        with opener.open(urllib.request.Request(exported["download_url"], headers={"Range": "bytes=103-999"}), timeout=30) as response:
            assert response.status == 206 and response.read() == payload[103:1000]
        imported_args = {"workspace_id": ws, "idempotency_key": key + "-import", "path": imported_path, "sha256": checksum, "file": {"file_id": "probe-" + exported["artifact_id"], "download_url": exported["download_url"], "file_name": "payload.bin", "mime_type": "application/octet-stream"}}
        imported = tool("file_upload", imported_args)
        assert imported["sha256"] == checksum and imported["size"] == len(payload)
        retried_import = tool("file_upload", imported_args)
        assert stable_operation_receipt(retried_import) == stable_operation_receipt(imported), "import retry must reuse original durable receipt"
        assert "error" in tool("file_upload", dict(imported_args, idempotency_key=key+"-no-clobber"), allow_error=True)
        assert "error" in tool("file_upload", dict(imported_args, idempotency_key=key+"-bad-checksum", path=bad_path, sha256="0"*64), allow_error=True)
        run_probe("import pathlib,hashlib; p=pathlib.Path(" + repr(imported_path) + "); assert hashlib.sha256(p.read_bytes()).hexdigest()==" + repr(checksum) + "; assert not pathlib.Path(" + repr(bad_path) + ").exists(); p.unlink(); pathlib.Path(" + repr(binary_path) + ").unlink()", "-binary-cleanup")
        tool("code_apply_edits", {"workspace_id": ws, "idempotency_key": key + "-cleanup", "files": [{"path": path, "expected_version": edited["changed"][0]["version"], "action": "delete"}]})
        report["devices"].append({"device_id": device["device_id"], "name": device["name"], "workspace_id": ws, "range_read": "passed", "syntax_tree": "passed", "precise_edit": "passed", "duplicate_edit": "passed", "stale_version_rejected": True, "search": "passed", "terminal_test_exit_code": 0, "probe_file_removed": True, "agent_version": device["capabilities"]["version"], "binary_transfer_bytes": len(payload), "binary_sha256": checksum, "download": "passed", "upload": "passed", "http_range": "passed", "upload_retry": "passed", "overwrite_rejected": True, "checksum_mismatch_rejected": True})
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(device["name"] + ": code, terminal, 2 MiB binary upload/download, Range, retry and failure checks all passed", flush=True)
    parsed("/oauth/revoke", {"token": token["refresh_token"]}, form=True)
    report["test_oauth_grant_revoked"] = True
    summary = acceptance_summary(report)
    report['state'] = 'passed'
    report['summary'] = summary
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(summary)


if __name__ == "__main__":
    main()

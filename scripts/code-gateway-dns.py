#!/usr/bin/env python3
"""Ensure one DNS-only CNAME using the existing NAS Caddy Cloudflare credential.

Run on the NAS. Credentials are read in memory and never printed or passed as argv.
The script refuses to replace an existing, different DNS record.
"""
import argparse
import json
import pathlib
import re
import subprocess
import urllib.request
import urllib.parse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    config = pathlib.Path("/etc/caddy/Caddyfile").read_text()
    match = re.search(r"(?m)^\s*dns\s+cloudflare\s+(\S+)\s*$", config)
    if not match:
        raise SystemExit("Cannot find existing Cloudflare DNS credential directive")
    credential = match.group(1).strip('"')
    if credential.startswith("{$") and credential.endswith("}"):
        name = credential[2:-1]
        pid = subprocess.check_output(["systemctl", "show", "caddy", "-p", "MainPID"], text=True).strip().split("=", 1)[1]
        env = dict(entry.split(b"=", 1) for entry in pathlib.Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in entry)
        credential = env.get(name.encode(), b"").decode()
    if not credential or credential.startswith("{"):
        raise SystemExit("Could not resolve credential from running Caddy")

    def api(path, data=None):
        request = urllib.request.Request("https://api.cloudflare.com/client/v4/" + path,
                                         data=json.dumps(data).encode() if data else None,
                                         headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)
        except Exception:
            raise SystemExit("Cloudflare request failed; credential value suppressed")
        if not result.get("success"):
            raise SystemExit("Cloudflare API rejected request; codes=" + str([e.get("code") for e in result.get("errors", [])]))
        return result["result"]

    zones = api("zones?name=hackerlife.fun")
    if len(zones) != 1:
        raise SystemExit("Expected one accessible hackerlife.fun zone")
    path = "zones/" + zones[0]["id"] + "/dns_records"
    records = api(path + "?name=mcp.hackerlife.fun")
    desired = {"type": "CNAME", "name": "mcp.hackerlife.fun", "content": "hackerlife.fun", "ttl": 300, "proxied": False}
    if records:
        if len(records) != 1 or any(records[0].get(k) != desired[k] for k in ("type", "name", "content", "proxied")):
            raise SystemExit("Existing MCP DNS record differs; refusing overwrite")
        print("DNS already matches: mcp.hackerlife.fun -> hackerlife.fun (DNS only)")
    elif args.apply:
        api(path, desired)
        print("Created DNS-only CNAME: mcp.hackerlife.fun -> hackerlife.fun")
    else:
        print("Ready to create DNS-only CNAME: mcp.hackerlife.fun -> hackerlife.fun")


if __name__ == "__main__":
    main()

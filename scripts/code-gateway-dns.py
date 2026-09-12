#!/usr/bin/env python3
"""Create or verify one Cloudflare DNS record without embedding deployment identity.

The script can reuse a Cloudflare API token already configured for Caddy. The
credential is read in memory and never printed or passed as an argv value.
"""
import argparse
import json
import pathlib
import re
import subprocess
import urllib.parse
import urllib.request


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zone", required=True, help="Cloudflare zone, for example example.com")
    parser.add_argument("--record", required=True, help="Fully qualified record name")
    parser.add_argument("--target", required=True, help="CNAME target")
    parser.add_argument("--ttl", type=int, default=300)
    parser.add_argument("--proxied", action="store_true", help="Enable Cloudflare proxying")
    parser.add_argument("--caddy-config", type=pathlib.Path, default=pathlib.Path("/etc/caddy/Caddyfile"))
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.ttl < 1:
        raise SystemExit("TTL must be positive")
    if not args.record.endswith(args.zone):
        raise SystemExit("record must belong to the requested zone")

    config = args.caddy_config.read_text()
    match = re.search(r"(?m)^\s*dns\s+cloudflare\s+(\S+)\s*$", config)
    if not match:
        raise SystemExit("Cannot find an existing Cloudflare DNS credential directive")
    credential = match.group(1).strip('"')
    if credential.startswith("{$") and credential.endswith("}"):
        name = credential[2:-1]
        pid = subprocess.check_output(
            ["systemctl", "show", "caddy", "-p", "MainPID"], text=True
        ).strip().split("=", 1)[1]
        env = dict(
            entry.split(b"=", 1)
            for entry in pathlib.Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
            if b"=" in entry
        )
        credential = env.get(name.encode(), b"").decode()
    if not credential or credential.startswith("{"):
        raise SystemExit("Could not resolve credential from running Caddy")

    def api(path, data=None):
        request = urllib.request.Request(
            "https://api.cloudflare.com/client/v4/" + path,
            data=json.dumps(data).encode() if data else None,
            headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.load(response)
        except Exception as error:
            raise SystemExit("Cloudflare request failed; credential value suppressed") from error
        if not result.get("success"):
            codes = [item.get("code") for item in result.get("errors", [])]
            raise SystemExit("Cloudflare API rejected request; codes=" + str(codes))
        return result["result"]

    zone_query = urllib.parse.quote(args.zone, safe="")
    zones = api("zones?name=" + zone_query)
    if len(zones) != 1:
        raise SystemExit(f"Expected exactly one accessible zone named {args.zone}")
    path = "zones/" + zones[0]["id"] + "/dns_records"
    record_query = urllib.parse.quote(args.record, safe="")
    records = api(path + "?name=" + record_query)
    desired = {
        "type": "CNAME",
        "name": args.record,
        "content": args.target,
        "ttl": args.ttl,
        "proxied": args.proxied,
    }
    compared = ("type", "name", "content", "proxied")
    if records:
        if len(records) != 1 or any(records[0].get(key) != desired[key] for key in compared):
            raise SystemExit("Existing DNS record differs; refusing overwrite")
        print(f"DNS already matches: {args.record} -> {args.target}")
    elif args.apply:
        api(path, desired)
        print(f"Created CNAME: {args.record} -> {args.target}")
    else:
        print(f"Ready to create CNAME: {args.record} -> {args.target}")


if __name__ == "__main__":
    main()

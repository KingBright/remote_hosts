#!/usr/bin/env python3
"""Drop to an isolated numeric UID before exec (supports Synology systemd 219)."""
import os

os.setgroups([])
os.setgid(18787)
os.setuid(18787)
os.umask(0o077)
os.execv("/opt/remote-hosts-code/remote-hosts-code", ["remote-hosts-code", "gateway", "--config", "/opt/remote-hosts-code/gateway.json"])

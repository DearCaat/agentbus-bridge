"""Discover user threads on the local managed Codex app-server."""
import json
import os
from pathlib import Path
import socket
import struct
import sys
import time
import uuid

from .codex_control import _process, _remaining, _socket_identity


def loaded_threads(codex_home, dependencies, timeout=6):
    if dependencies not in sys.path:
        sys.path.append(dependencies)
    import websocket

    home = Path(codex_home).resolve()
    endpoint = _socket_identity(home / "app-server-control" / "app-server-control.sock")
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as raw:
        raw.settimeout(_remaining(deadline))
        raw.connect(endpoint)
        pid, uid, _ = struct.unpack("3i", raw.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        process = _process(pid)
        if uid != os.geteuid() or "app-server" not in process["argv"]:
            raise ValueError("unexpected Codex control peer")
        ws = websocket.create_connection("ws://localhost/", socket=raw, timeout=_remaining(deadline))
        try:
            serial = 0

            def request(method, params):
                nonlocal serial
                serial += 1
                ws.settimeout(_remaining(deadline))
                ws.send(json.dumps(dict(id=serial, method=method, params=params)))
                while True:
                    ws.settimeout(_remaining(deadline))
                    reply = json.loads(ws.recv())
                    if reply.get("id") == serial:
                        return reply.get("result")

            info = request("initialize", {
                "clientInfo": {"name": "agentbus_presence", "version": "1"},
                "capabilities": {"experimentalApi": True},
            })
            if not info or Path(info.get("codexHome", "")).resolve() != home:
                raise ValueError("Codex home does not match control socket")
            ws.send(json.dumps({"method": "initialized", "params": {}}))
            ids, seen, cursor = set(), set(), None
            while True:
                params = {"limit": 100}
                if cursor:
                    params["cursor"] = cursor
                page = request("thread/loaded/list", params)
                ids.update(str(uuid.UUID(value)) for value in page["data"])
                cursor = page.get("nextCursor")
                if not cursor:
                    break
                if cursor in seen:
                    raise ValueError("repeated Codex thread cursor")
                seen.add(cursor)

            threads = {}
            for thread_id in sorted(ids):
                result = request("thread/read", {"threadId": thread_id, "includeTurns": False})
                thread = (result or {}).get("thread", {})
                if (thread.get("id") == thread_id and isinstance(thread.get("source"), str)
                        and thread.get("status", {}).get("type") in ("idle", "active", "systemError")):
                    threads[thread_id] = thread.get("cwd")
            return pid, threads
        finally:
            ws.shutdown()

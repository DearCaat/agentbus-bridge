"""Native Codex MCP reload; optional WebSocket transport, never a text command.

The native operation affects every loaded thread in one app-server. Verification
is deliberately narrower: the registered target thread's agentbus connection.
"""
import importlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import struct
import subprocess
import time
import uuid


class _Failure(Exception):
    def __init__(self, code, state="failed"):
        self.code, self.state = code, state


class _RpcError(Exception):
    def __init__(self, code):
        self.code = code if isinstance(code, int) else None


def _remaining(deadline):
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError()
    return left


def _process(pid):
    """Read only the process identity and two non-secret environment fields."""
    p = Path("/proc") / str(pid)
    try:
        fields = (p / "stat").read_text().rsplit(")", 1)[1].split()
        selected = {}
        for item in (p / "environ").read_bytes().split(b"\0"):
            for name in (b"HOME", b"CODEX_HOME"):
                if item.startswith(name + b"="):
                    selected[name.decode()] = item[len(name) + 1:].decode()
        return dict(pid=int(pid), ppid=int(fields[1]), state=fields[0],
                    starttime=fields[19], uid=p.stat().st_uid,
                    argv=(p / "cmdline").read_bytes().decode().split("\0"),
                    **selected)
    except (OSError, ValueError, IndexError, UnicodeError):
        raise _Failure("runtime_identity_unavailable", "unsupported") from None


def _socket_identity(path):
    try:
        resolved = Path(path).resolve(strict=True)
        info, parent = resolved.stat(), resolved.parent.stat()
    except (OSError, ValueError, RuntimeError):
        raise _Failure("control_socket_unavailable", "unsupported") from None
    uid = os.geteuid()
    if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != uid
            or stat.S_IMODE(info.st_mode) & 0o077
            or parent.st_uid not in (0, uid) or stat.S_IMODE(parent.st_mode) & 0o022):
        raise _Failure("unsafe_control_socket")
    return str(resolved)


def _discover(session, codex_bin, deadline):
    pid = session.get("mcp_pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise _Failure("stale_local_registration", "unsupported")
    mcp = _process(pid)
    if mcp["state"] == "Z" or mcp["uid"] != os.geteuid():
        raise _Failure("stale_local_registration", "unsupported")
    ancestor, seen = mcp, set()
    for _ in range(64):
        if ancestor["pid"] in seen:
            break
        seen.add(ancestor["pid"])
        argv = ancestor["argv"]
        if "app-server" in argv and "daemon" not in argv:
            break
        if ancestor["ppid"] <= 1:
            raise _Failure("no_managed_app_server", "unsupported")
        ancestor = _process(ancestor["ppid"])
    else:
        raise _Failure("runtime_identity_unavailable", "unsupported")
    if "app-server" not in ancestor["argv"] or "daemon" in ancestor["argv"]:
        raise _Failure("no_managed_app_server", "unsupported")
    if ancestor["state"] == "Z" or ancestor["uid"] != os.geteuid():
        raise _Failure("runtime_identity_mismatch")
    actual_home = ancestor.get("CODEX_HOME")
    registered_home = session["deliver"].get("codex_home")
    if not actual_home or not registered_home:
        raise _Failure("codex_home_identity_unavailable", "unsupported")
    if Path(actual_home).resolve() != Path(registered_home).resolve():
        raise _Failure("codex_home_identity_mismatch")
    env = dict(os.environ)
    env["CODEX_HOME"] = actual_home
    if ancestor.get("HOME"):
        env["HOME"] = ancestor["HOME"]
    argv = ancestor["argv"]
    listen = argv[argv.index("--listen") + 1] if "--listen" in argv and argv.index("--listen") + 1 < len(argv) else ""
    if listen.startswith("unix://") and listen != "unix://":
        # An explicit endpoint from the actual owning process is authoritative,
        # including standalone isolated app-servers without a managed daemon.
        endpoint_path = listen[len("unix://"):]
        if not Path(endpoint_path).is_absolute():
            raise _Failure("invalid_runtime_socket")
    else:
        try:
            reply = subprocess.run([codex_bin, "app-server", "daemon", "version"],
                                   stdin=subprocess.DEVNULL, capture_output=True,
                                   text=True, env=env, timeout=_remaining(deadline))
            discovered = json.loads(reply.stdout) if reply.returncode == 0 else {}
        except (OSError, ValueError, subprocess.TimeoutExpired):
            raise _Failure("daemon_discovery_failed", "unsupported") from None
        if discovered.get("status") != "running" or not isinstance(discovered.get("socketPath"), str):
            raise _Failure("daemon_not_running", "unsupported")
        endpoint_path = discovered["socketPath"]
    endpoint = _socket_identity(endpoint_path)
    return dict(pid=ancestor["pid"], starttime=ancestor["starttime"], uid=ancestor["uid"],
                mcp_pid=mcp["pid"], mcp_starttime=mcp["starttime"], codex_home=actual_home,
                socket_path=endpoint)


def _unchanged(identity):
    for pid, start in ((identity["pid"], identity["starttime"]),
                       (identity["mcp_pid"], identity["mcp_starttime"])):
        current = _process(pid)
        if current["starttime"] != start or current["state"] == "Z" or current["uid"] != identity["uid"]:
            raise _Failure("runtime_identity_changed")


def _expected_catalog():
    from . import __version__
    from .mcp_server import TOOLS
    return __version__, set(TOOLS)


def run(session, scope, codex_bin="codex", timeout=20):
    """Execute one native reload, then verify only the registered target catalog.

    session must come from the local Store, never from a remote control payload.
    Disconnects after sending a mutation are not retried.
    """
    result = dict(code="not_executed", summary="Native MCP reload was not executed.", harness="codex",
                  effective_scope=None, target_thread_id=None, runtime_identity=None, verification=None,
                  observed={}, affected_servers=[], native_accepted=False, applied=False, verified=False,
                  capabilities={"supported_scopes": ["server"], "transport": "websocket_over_unix_socket"})

    def finish(state, code, summary):
        result.update(code=code, summary=summary)
        return {"state": state, "result": result}

    if scope != "server":
        return finish("unsupported", "unsupported_scope", "Codex only supports server-wide MCP reload; no native action was sent.")
    if not isinstance(session, dict) or session.get("harness") != "codex":
        return finish("unsupported", "unsupported_harness", "A local Codex registration is required.")
    try:
        target = str(uuid.UUID(session["session_id"]))
        deliver = session["deliver"]
        if (not isinstance(deliver, dict) or deliver.get("thread_id") != target
                or not str(session.get("session_key", "")).endswith(":codex:" + target)):
            raise ValueError()
        duration = float(timeout)
        if isinstance(timeout, bool) or not math.isfinite(duration) or duration <= 0:
            raise ValueError()
    except (KeyError, ValueError, TypeError, AttributeError):
        return finish("failed", "invalid_local_registration", "The local Codex thread identity or timeout is invalid.")
    result["target_thread_id"] = target
    deadline = time.monotonic() + duration
    ws = raw = None
    sent = False
    serial = 0
    try:
        try:
            websocket = importlib.import_module("websocket")
        except ImportError:
            return finish("unsupported", "missing_dependency", "Codex control requires the optional requirements-control.txt dependencies.")
        version, required_tools = _expected_catalog()
        identity = _discover(session, codex_bin, deadline)
        result["runtime_identity"] = identity
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.settimeout(_remaining(deadline))
        raw.connect(identity["socket_path"])
        if not hasattr(socket, "SO_PEERCRED"):
            raise _Failure("peer_identity_unavailable", "unsupported")
        pid, uid, _ = struct.unpack("3i", raw.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if pid != identity["pid"] or uid != identity["uid"]:
            raise _Failure("control_socket_peer_mismatch")
        ws = websocket.create_connection("ws://localhost/", socket=raw, timeout=_remaining(deadline))

        def request(method, params):
            nonlocal serial
            serial += 1
            request_id = "agentbus-control-%d" % serial
            ws.settimeout(_remaining(deadline))
            ws.send(json.dumps(dict(id=request_id, method=method, params=params)))
            while True:
                ws.settimeout(_remaining(deadline))
                reply = json.loads(ws.recv())
                if not isinstance(reply, dict):
                    raise ValueError("invalid native response")
                if reply.get("id") != request_id:
                    continue
                if "error" in reply:
                    error = reply["error"]
                    raise _RpcError(error.get("code") if isinstance(error, dict) else None)
                if "result" not in reply:
                    raise ValueError("missing native result")
                return reply["result"]

        initialized = request("initialize", {"clientInfo": {"name": "agentbus_native_control", "version": version},
                                              "capabilities": {"experimentalApi": True}})
        if not isinstance(initialized, dict) or Path(initialized.get("codexHome", "")).resolve() != Path(identity["codex_home"]).resolve():
            raise _Failure("initialized_home_mismatch")
        ws.send(json.dumps({"method": "initialized", "params": {}}))
        loaded, cursor, cursors = set(), None, set()
        while True:
            params = {"limit": 100}
            if cursor is not None:
                params["cursor"] = cursor
            page = request("thread/loaded/list", params)
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise ValueError("invalid loaded-thread response")
            loaded.update(x for x in page["data"] if isinstance(x, str))
            cursor = page.get("nextCursor")
            if not cursor:
                break
            if not isinstance(cursor, str) or cursor in cursors:
                raise ValueError("invalid loaded-thread cursor")
            cursors.add(cursor)
        result["observed"].update(loaded_threads_count=len(loaded), target_loaded=target in loaded)
        if target not in loaded:
            return finish("unsupported", "target_not_loaded", "The registered thread is not loaded in its owning app-server; no reload was sent.")
        def target_status():
            status = request("mcpServerStatus/list", {"threadId": target, "serverName": "agentbus", "detail": "toolsAndAuthOnly"})
            entries = status.get("data", []) if isinstance(status, dict) else []
            return next((s for s in entries if isinstance(s, dict) and s.get("name") == "agentbus"), {})

        def runtime_metadata(status):
            caps = status.get("serverCapabilities") or {}
            experimental = caps.get("experimental") or {} if isinstance(caps, dict) else {}
            metadata = experimental.get("agentbus") or {} if isinstance(experimental, dict) else {}
            return metadata if isinstance(metadata, dict) else {}

        baseline = runtime_metadata(target_status())
        _unchanged(identity)
        result.update(effective_scope="server", affected_servers=["*"])
        sent = True
        generation = uuid.uuid4().hex
        written = request("config/value/write", {
            "keyPath": "mcp_servers.agentbus.env.AGENTBUS_RELOAD_GENERATION",
            "value": generation, "mergeStrategy": "replace"})
        if (not isinstance(written, dict) or written.get("status") not in ("ok", "okOverridden")
                or Path(written.get("filePath", "")).resolve() != (Path(identity["codex_home"]) / "config.toml").resolve()):
            raise ValueError("unverified marker write")
        result["observed"].update(reload_marker_updated=True, marker_write_status=written["status"])
        native = request("config/mcpServer/reload", None)
        if native != {}:
            raise ValueError("invalid native reload response")
        result.update(native_accepted=True, applied=True)
        selected = target_status()
        metadata = runtime_metadata(selected)
        info = selected.get("serverInfo") or {}
        tools = selected.get("tools") or {}
        names = {k for k in tools if isinstance(k, str)} if isinstance(tools, dict) else set()
        result["observed"].update(runtime_status=selected.get("runtimeStatus"), server_version=info.get("version"), tool_names=sorted(names),
                                  tools_error_present=bool(selected.get("toolsError")))
        instance = metadata.get("runtime_id")
        instance_changed = isinstance(instance, str) and bool(instance) and instance != baseline.get("runtime_id")
        generation_matches = metadata.get("reload_generation") == generation
        result["observed"].update(generation_matches=generation_matches, runtime_instance_changed=instance_changed)
        verification = dict(scope="target_thread", thread_id=target, server="agentbus", expected_version=version,
                            required_tools=sorted(required_tools), version_matches=info.get("version") == version,
                            tools_match=required_tools.issubset(names), runtime_connected=selected.get("runtimeStatus") == "connected",
                            generation_matches=generation_matches, runtime_instance_changed=instance_changed)
        result["verification"] = verification
        result["verified"] = all(verification[k] for k in ("version_matches", "tools_match", "runtime_connected", "generation_matches", "runtime_instance_changed")) and not selected.get("toolsError")
        if result["verified"]:
            return finish("verified", "target_catalog_verified", "The plugin-owned reload marker was written and all loaded threads' MCP refreshes were queued; only the target thread's agentbus version and tools were verified.")
        return finish("applied_unverified", "target_catalog_not_verified", "The plugin-owned marker was written and the server accepted refreshes for all loaded threads; the target agentbus version and tools were not verified.")
    except _Failure as error:
        return finish(error.state, error.code, "Local Codex runtime identity could not be verified; no native reload was sent.")
    except _RpcError as error:
        result["observed"]["native_error_code"] = error.code
        if sent and not result["native_accepted"]:
            result["applied"] = None
        state = "applied_unverified" if result["native_accepted"] else ("in_doubt" if sent else "failed")
        return finish(state, "native_request_rejected",
                      "Codex rejected a native request; reload rejection may have partial effects on loaded threads.")
    except Exception:
        # Never reflect native error text, account notifications, headers, or environment values.
        if sent and not result["native_accepted"]:
            result["applied"] = None
        state = "applied_unverified" if result["native_accepted"] else ("in_doubt" if sent else "failed")
        return finish(state, "verification_unavailable" if result["native_accepted"] else ("native_outcome_unknown" if sent else "native_connection_failed"),
                      "Codex accepted the native refresh but target verification is unavailable; no mutation retry was attempted."
                      if result["native_accepted"] else
                      "Native communication failed; no mutation retry was attempted and target reload is unverified.")
    finally:
        if ws is not None:
            try:
                ws.close(timeout=0.1)
            except Exception:
                pass
        if raw is not None:
            raw.close()

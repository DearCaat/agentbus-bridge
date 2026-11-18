"""agentbus MCP: switch to a newer installed plugin version inside a running session.

A plugin update installs into a new cache/<marketplace>/agentbus/<version>/ directory; a session that is
already open keeps running the MCP process from the old directory. After each answered request the MCP checks
whether the installed agentbus is now another version in the same cache parent. If that version passes a
self-test in a subprocess, the process os.execve's into it (same pid, same stdin/stdout, so the session sees
no disconnect), re-registers with the node, and sends notifications/tools/list_changed so the client re-reads
the tool list (Claude Code 2.1.327, docker3, 11-18: re-list within 3 ms; without the notification new tools
stay invisible).

Delivery, injection and reliability live in the node daemon, not here: a newer MCP's `node --ensure` already
replaces an older node (node.py), so this only brings the thin MCP layer up to date.

Never switch when unsure: not running from the plugin cache (--plugin-dir, a dev checkout), no install record,
target not in the same cache parent, self-test failed or timed out, harness is not claude, the node watchdog
is busy (holds SWAP_LOCK), or AGENTBUS_NO_HOTSWAP is set. A target that failed once is not retried.
"""
import base64
import json
import os
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # plugin root (holds the agentbus package)
CHECK_SEC = float(os.environ.get("AGENTBUS_HOTSWAP_SEC", "30"))
SELFTEST_TIMEOUT = float(os.environ.get("AGENTBUS_HOTSWAP_SELFTEST_TIMEOUT", "30"))
LEFTOVER_ENV = "AGENTBUS_HOTSWAP_LEFTOVER"
REEXEC_ARG = "--hotswap-reexec"
SWAP_LOCK = threading.Lock()

_last = [0.0]
_bad = set()


def _log(msg):
    sys.stderr.write("[agentbus-mcp] hotswap: %s\n" % msg)
    sys.stderr.flush()


def plugins_root():
    if os.environ.get("CLAUDE_CODE_PLUGIN_CACHE_DIR"):
        return os.environ["CLAUDE_CODE_PLUGIN_CACHE_DIR"]
    cfg = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.join(os.path.expanduser("~"), ".claude")
    return os.path.join(cfg, "plugins")


def installed_dir(root=ROOT):
    """Installed agentbus directory, only if it shares root's cache parent (cache/<marketplace>/agentbus/)."""
    parent = os.path.dirname(root)
    if os.path.basename(parent) != "agentbus" or os.sep + "cache" + os.sep not in parent + os.sep:
        return None
    try:
        with open(os.path.join(plugins_root(), "installed_plugins.json"), encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError):
        return None
    for key, entries in (d.get("plugins") or {}).items():
        if key.split("@")[0] != "agentbus":
            continue
        for e in entries or []:
            p = e.get("installPath") if isinstance(e, dict) else None
            if not p:
                continue
            p = os.path.realpath(p)
            if os.path.dirname(p) == parent and os.path.isfile(os.path.join(p, "agentbus", "mcp_server.py")):
                return p
    return None


def _env_for(root):
    env = dict(os.environ)
    env["PYTHONPATH"] = root
    return env


def selftest(root):
    try:
        r = subprocess.run([sys.executable, "-m", "agentbus", "selftest"], cwd=root, env=_env_for(root),
                           stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=SELFTEST_TIMEOUT)
    except (OSError, subprocess.SubprocessError) as e:
        return False, str(e)[:200]
    return (r.returncode == 0 and r.stdout.strip().endswith("ok")), (r.stderr or r.stdout)[-200:]


def maybe_swap(reader, harness, now=None):
    """Call after answering a request. Exec's into the newer version (does not return) or returns None."""
    if os.environ.get("AGENTBUS_NO_HOTSWAP") or harness != "claude":
        return None
    now = time.time() if now is None else now
    if now - _last[0] < CHECK_SEC:
        return None
    _last[0] = now
    new = installed_dir()
    if not new or new == ROOT or new in _bad:
        return None
    ok, why = selftest(new)
    if not ok:
        _bad.add(new)
        _log("not switching: %s failed self-test (%r); staying on %s" % (new, why, ROOT))
        return None
    if not SWAP_LOCK.acquire(blocking=False):
        return None
    env = _env_for(new)
    env[LEFTOVER_ENV] = base64.b64encode(reader.leftover()).decode()
    _log("switching %s -> %s (%d buffered bytes handed over)" % (ROOT, new, len(reader.leftover())))
    sys.stdout.flush()
    try:
        os.chdir(new)
        os.execve(sys.executable, [sys.executable, "-m", "agentbus", "mcp", REEXEC_ARG], env)
    except OSError as e:
        SWAP_LOCK.release()
        _bad.add(new)
        _log("not switching: execve failed: %s" % e)
    return None


class LineReader:
    """Read stdin line by line without the sys.stdin read-ahead buffer, so unprocessed bytes can be handed over."""

    def __init__(self, fd=0, initial=b""):
        self.fd, self.buf = fd, initial

    def __iter__(self):
        while True:
            i = self.buf.find(b"\n")
            while i < 0:
                chunk = os.read(self.fd, 65536)
                if not chunk:
                    if self.buf.strip():
                        line, self.buf = self.buf, b""
                        yield line
                    return
                self.buf += chunk
                i = self.buf.find(b"\n")
            line, self.buf = self.buf[:i], self.buf[i + 1:]
            yield line

    def leftover(self):
        return self.buf


def reader_from_env():
    raw = os.environ.pop(LEFTOVER_ENV, "")
    try:
        initial = base64.b64decode(raw) if raw else b""
    except ValueError:
        initial = b""
    return LineReader(0, initial)

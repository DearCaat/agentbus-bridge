"""agentbus node daemon (SPEC.md 'Node', 'Injection', plus the runtime-path addendum).

Layout under the runtime dir (AGENTBUS_HOME, default ~/.agentbus):
  node.lock node.sock node.db node.log  and token (or config key token_file).
Config file: AGENTBUS_CONFIG, default <runtime dir>/config.json.

Stdlib only, Python >= 3.10.
"""

import fcntl
import json
import os
import signal
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time
import uuid

from .hubclient import HubClient, HubError
from .inject import inject_claude, inject_codex, render, same_machine_harness

DEFAULT_HUB_URL = "http://127.0.0.1:37460"
HARNESSES = ("claude", "codex")
MAX_TEXT_BYTES = 64 * 1024
MAX_REQUEST_BYTES = 1024 * 1024
MAX_DETAIL = 2000
HEARTBEAT_INTERVAL = 20.0
POLL_WAIT = 25
BACKOFF_MAX = 30.0
TUNNEL_STABLE_SECS = 30.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_key TEXT PRIMARY KEY,
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    address TEXT,
    name TEXT,
    cwd TEXT,
    deliver TEXT NOT NULL DEFAULT '{}',
    mcp_pid INTEGER NOT NULL DEFAULT 0,
    registered_at REAL,
    last_seen REAL
);
CREATE TABLE IF NOT EXISTS delivered_ids (
    msg_id TEXT PRIMARY KEY,
    to_key TEXT,
    state TEXT NOT NULL,
    detail TEXT,
    reported INTEGER NOT NULL DEFAULT 0,
    created_at REAL,
    updated_at REAL
);
"""


class AlreadyRunning(RuntimeError):
    pass


class ConfigError(RuntimeError):
    pass


def agentbus_home():
    return os.path.expanduser(os.environ.get("AGENTBUS_HOME") or "~/.agentbus")


class Paths:
    def __init__(self, home):
        self.home = home
        env_cfg = os.environ.get("AGENTBUS_CONFIG")
        self.config = os.path.expanduser(env_cfg) if env_cfg else os.path.join(home, "config.json")
        self.sock = os.path.join(home, "node.sock")
        self.db = os.path.join(home, "node.db")
        self.log = os.path.join(home, "node.log")
        self.lock = os.path.join(home, "node.lock")


def pid_alive(pid):
    """True while the process exists and is not a zombie (/proc state != Z)."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if os.path.exists("/proc/self/stat"):
        try:
            with open("/proc/%d/stat" % pid, "rb") as f:
                data = f.read().decode("ascii", "replace")
        except OSError:
            return False
        fields = data.rsplit(")", 1)[-1].split()
        state = fields[0] if fields else ""
        return state not in ("Z", "X", "x")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _argv_list(value, what):
    if not (isinstance(value, list) and value and all(isinstance(x, str) for x in value)):
        raise ConfigError("%s must be a non-empty argv list of strings" % what)
    # Addendum: expand ${ENV} and ~ in every argv element.
    return [os.path.expanduser(os.path.expandvars(x)) for x in value]


def load_config(p):
    try:
        with open(p.config, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        raise ConfigError("missing config file %s" % p.config)
    except ValueError as e:
        raise ConfigError("bad JSON in %s: %s" % (p.config, e))
    if not isinstance(raw, dict):
        raise ConfigError("config must be a JSON object")

    tunnels = []
    if raw.get("tunnel") is not None:
        tunnels.append(_argv_list(raw["tunnel"], "tunnel"))
    extra = raw.get("extra_tunnels") or []
    if not isinstance(extra, list):
        raise ConfigError("extra_tunnels must be a list")
    for i, argv in enumerate(extra):
        tunnels.append(_argv_list(argv, "extra_tunnels[%d]" % i))

    token_file = raw.get("token_file")
    if token_file:
        token_path = os.path.expanduser(str(token_file))
        if not os.path.isabs(token_path):
            token_path = os.path.join(os.path.dirname(p.config), token_path)
    else:
        token_path = os.path.join(p.home, "token")

    machine = os.environ.get("AGENTBUS_MACHINE") or raw.get("machine") or socket.gethostname().split(".")[0]
    return {
        "machine": machine,
        "hub_url": raw.get("hub_url") or DEFAULT_HUB_URL,
        "token_path": token_path,
        "tunnels": tunnels,
        "codex_bin": raw.get("codex_bin") or "codex",
    }


def read_token(path):
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _lock_is_held(p):
    if not os.path.exists(p.lock):
        return False
    fd = os.open(p.lock, os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _pkg_parent():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _stale_node_stopped(p, pkg_parent, wait_seconds=10.0):
    """If the running node runs a different version/code root (e.g. after a plugin update),
    stop it and return True once its lock is free. Returns False if it should be kept."""
    from . import __version__
    try:
        with open(os.path.join(p.home, "node.pid")) as f:
            info = json.load(f)
    except (OSError, ValueError):
        return False
    # Versions only: sessions loading the same version from different paths
    # (--plugin-dir vs installed plugin) must not keep restarting each other's node.
    if info.get("version") == __version__:
        return False
    try:
        os.kill(int(info["pid"]), signal.SIGTERM)
    except (OSError, KeyError, ValueError):
        return False
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        if not _lock_is_held(p):
            return True
        time.sleep(0.2)
    return False


def ensure(home=None, spawn_argv=None):
    """Start a detached node unless one holds the lock. Returns 'running' or 'spawned'."""
    p = Paths(home or agentbus_home())
    os.makedirs(p.home, mode=0o700, exist_ok=True)
    load_config(p)  # fail early on a broken config
    pkg_parent = _pkg_parent()
    if _lock_is_held(p):
        if not _stale_node_stopped(p, pkg_parent):
            return "running"
    argv = list(spawn_argv) if spawn_argv else [sys.executable, "-m", "agentbus", "node", "--foreground"]
    env = dict(os.environ)
    env["AGENTBUS_HOME"] = p.home
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = pkg_parent + os.pathsep + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = pkg_parent
    logf = open(p.log, "ab")
    try:
        subprocess.Popen(
            argv,
            cwd=pkg_parent,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=logf,
            stderr=logf,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        logf.close()
    return "spawned"


def _peer_pid(conn):
    opt = getattr(socket, "SO_PEERCRED", None)
    if opt is None:
        return 0
    try:
        data = conn.getsockopt(socket.SOL_SOCKET, opt, struct.calcsize("3i"))
        return struct.unpack("3i", data)[0]
    except OSError:
        return 0


def _recv_line(conn):
    buf = bytearray()
    while b"\n" not in buf:
        chunk = conn.recv(65536)
        if not chunk:
            break
        buf += chunk
        if len(buf) > MAX_REQUEST_BYTES:
            raise ValueError("request too large")
    return bytes(buf).split(b"\n", 1)[0]


def _fail(code, detail=None):
    out = {"ok": False, "error": code}
    if detail:
        out["detail"] = detail
    return out


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


class Store:
    """node.db: local session registry and at-most-once delivery record."""

    def __init__(self, path):
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, timeout=10, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self._db.close()

    def _query(self, sql, args=()):
        with self._lock:
            return self._db.execute(sql, args).fetchall()

    def _exec(self, sql, args=()):
        with self._lock:
            return self._db.execute(sql, args).rowcount

    @staticmethod
    def _session(row):
        d = dict(row)
        try:
            d["deliver"] = json.loads(d.get("deliver") or "{}")
        except ValueError:
            d["deliver"] = {}
        return d

    def upsert_session(self, session_key, harness, session_id, address, name, cwd, deliver, mcp_pid, now):
        self._exec(
            "INSERT OR REPLACE INTO sessions (session_key, harness, session_id, address, name, cwd, "
            "deliver, mcp_pid, registered_at, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (session_key, harness, session_id, address, name, cwd, json.dumps(deliver), mcp_pid, now, now),
        )

    def get_session(self, session_key):
        rows = self._query("SELECT * FROM sessions WHERE session_key=?", (session_key,))
        return self._session(rows[0]) if rows else None

    def list_sessions(self):
        return [self._session(r) for r in self._query("SELECT * FROM sessions ORDER BY registered_at")]

    def delete_session(self, session_key):
        return self._exec("DELETE FROM sessions WHERE session_key=?", (session_key,)) == 1

    def touch_sessions(self, keys, now):
        for k in keys:
            self._exec("UPDATE sessions SET last_seen=? WHERE session_key=?", (now, k))

    def record_pending(self, msg_id, to_key):
        """Insert before injecting. Returns False if msg_id was already recorded."""
        now = time.time()
        return self._exec(
            "INSERT OR IGNORE INTO delivered_ids (msg_id, to_key, state, detail, reported, created_at, updated_at) "
            "VALUES (?,?,?,?,0,?,?)",
            (msg_id, to_key, "injecting", None, now, now),
        ) == 1

    def get_delivery(self, msg_id):
        rows = self._query("SELECT msg_id, state, detail, reported FROM delivered_ids WHERE msg_id=?", (msg_id,))
        return dict(rows[0]) if rows else None

    def set_result(self, msg_id, state, detail):
        self._exec(
            "UPDATE delivered_ids SET state=?, detail=?, reported=0, updated_at=? WHERE msg_id=?",
            (state, detail, time.time(), msg_id),
        )

    def mark_reported(self, msg_id):
        self._exec("UPDATE delivered_ids SET reported=1, updated_at=? WHERE msg_id=?", (time.time(), msg_id))

    def unreported(self):
        return [
            dict(r)
            for r in self._query(
                "SELECT msg_id, state, detail FROM delivered_ids WHERE reported=0 AND state!='injecting' "
                "ORDER BY created_at"
            )
        ]

    def mark_in_doubt_on_startup(self):
        """Recorded but no result: the crash may have happened mid-injection."""
        return self._exec(
            "UPDATE delivered_ids SET state='in_doubt', detail=?, reported=0, updated_at=? WHERE state='injecting'",
            ("node restarted before the injection result was recorded", time.time()),
        )


def _deliver_fields(harness, deliver):
    keys = ("socket", "token") if harness == "claude" else ("thread_id", "codex_home")
    return {k: deliver[k] for k in keys if isinstance(deliver.get(k), str) and deliver.get(k)}


class Node:
    def __init__(self, home=None, poll_wait=POLL_WAIT, heartbeat_interval=HEARTBEAT_INTERVAL):
        self.p = Paths(home or agentbus_home())
        self.poll_wait = poll_wait
        self.heartbeat_interval = heartbeat_interval
        self.machine = None
        self.hub = None
        self.store = None
        self.codex_bin = "codex"
        self.tunnels = []
        self._stop = threading.Event()
        self._running = False
        self._threads = []
        self._children = set()
        self._children_lock = threading.Lock()
        self._lock_fd = None
        self._srv = None
        self._logf = None
        self._log_lock = threading.Lock()

    # ----- lifecycle -------------------------------------------------------

    def start(self, retry_seconds=0.0):
        if self._running:
            return
        p = self.p
        os.makedirs(p.home, mode=0o700, exist_ok=True)
        cfg = load_config(p)  # ConfigError before any resource is taken
        self.machine = cfg["machine"]
        self.codex_bin = cfg["codex_bin"]
        self.tunnels = cfg["tunnels"]
        self.hub = HubClient(cfg["hub_url"], read_token(cfg["token_path"]))

        self._acquire_lock(retry_seconds)
        self._running = True
        try:
            self._stop = threading.Event()
            self._threads = []
            self._logf = open(p.log, "a", encoding="utf-8", buffering=1)
            self.store = Store(p.db)
            n = self.store.mark_in_doubt_on_startup()
            self._log("node start pid=%d machine=%s hub=%s in_doubt=%d"
                      % (os.getpid(), self.machine, cfg["hub_url"], n))

            self._bind_socket()
            self._spawn(self._serve_loop)
            for argv in self.tunnels:
                self._spawn(self._tunnel_loop, argv)
            self.flush_reports()
            self._spawn(self._heartbeat_loop)
            self._spawn(self._pull_loop)
        except BaseException:
            self.stop()
            raise

    def _acquire_lock(self, retry_seconds):
        fd = os.open(self.p.lock, os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + retry_seconds
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise AlreadyRunning("another node holds %s" % self.p.lock)
                time.sleep(0.1)
        self._lock_fd = fd
        try:
            from . import __version__
            info = {"pid": os.getpid(), "version": __version__, "root": _pkg_parent()}
            with open(os.path.join(self.p.home, "node.pid"), "w") as f:
                json.dump(info, f)
        except OSError:
            pass

    def _bind_socket(self):
        try:
            os.unlink(self.p.sock)
        except FileNotFoundError:
            pass
        old = os.umask(0o177)
        try:
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(self.p.sock)
        finally:
            os.umask(old)
        os.chmod(self.p.sock, 0o600)
        srv.listen(16)
        srv.settimeout(1.0)
        self._srv = srv

    def _spawn(self, target, *args):
        t = threading.Thread(target=target, args=args, daemon=True)
        self._threads.append(t)
        t.start()
        return t

    def wait(self):
        while not self._stop.wait(0.5):
            pass

    def stop(self):
        if not self._running:
            return
        self._running = False
        self._stop.set()
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass
            self._srv = None
        with self._children_lock:
            children = list(self._children)
        for proc in children:
            _terminate(proc)
        me = threading.current_thread()
        for t in self._threads:
            if t is not me:
                t.join(timeout=3)
        try:
            os.unlink(self.p.sock)
        except OSError:
            pass
        if self._lock_fd is not None:
            os.close(self._lock_fd)  # releases the flock
            self._lock_fd = None
        if self.store is not None:
            self.store.close()
        self._log("node stopped")
        if self._logf is not None:
            self._logf.close()
            self._logf = None

    def _log(self, msg):
        with self._log_lock:
            if self._logf is None:
                return
            self._logf.write("%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%S"), msg))

    # ----- local API (unix socket) -----------------------------------------

    def _serve_loop(self):
        srv = self._srv
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self._spawn_conn(conn)

    def _spawn_conn(self, conn):
        t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
        t.start()

    def _handle_conn(self, conn):
        try:
            conn.settimeout(30)
            pid = _peer_pid(conn)
            try:
                req = json.loads(_recv_line(conn).decode("utf-8"))
                resp = self.dispatch(req, pid)
            except (ValueError, UnicodeDecodeError) as e:
                resp = _fail("bad_request", str(e))
            except Exception as e:  # never kill the server thread
                self._log("local op failed: %r" % (e,))
                resp = _fail("internal", "%s: %s" % (type(e).__name__, e))
            conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
        except OSError:
            pass
        finally:
            conn.close()

    def dispatch(self, req, peer_pid=0):
        if not isinstance(req, dict):
            return _fail("bad_request", "request must be a JSON object")
        handlers = {
            "register": self._op_register,
            "send": self._op_send,
            "peers": self._op_peers,
            "status": self._op_status,
            "unregister": self._op_unregister,
        }
        handler = handlers.get(req.get("op"))
        if handler is None:
            return _fail("unknown_op", str(req.get("op")))
        return handler(req, peer_pid)

    def _session_key(self, req):
        key = req.get("session_key")
        if isinstance(key, str) and key:
            return key
        harness, sid = req.get("harness"), req.get("session_id")
        if harness in HARNESSES and isinstance(sid, str) and sid:
            return "%s:%s:%s" % (self.machine, harness, sid)
        return None

    def _hub_fail(self, e):
        if e.status == 0:
            return _fail("hub_unreachable", e.message)
        return _fail(e.code or "hub_error", e.message)

    def _op_register(self, req, peer_pid):
        harness = req.get("harness")
        session_id = req.get("session_id")
        if harness not in HARNESSES:
            return _fail("bad_harness", "harness must be claude or codex")
        if not isinstance(session_id, str) or not session_id:
            return _fail("bad_request", "session_id required")
        deliver = req.get("deliver") or {}
        if not isinstance(deliver, dict):
            return _fail("bad_request", "deliver must be an object")

        body = {"machine": self.machine, "harness": harness, "session_id": session_id}
        name = req.get("name") or None
        cwd = req.get("cwd") or None
        if name:
            body["name"] = name
        if cwd:
            body["cwd"] = cwd
        mcp_pid = _int(req.get("mcp_pid")) or peer_pid
        try:
            r = self.hub.register(body)
        except HubError as e:
            return self._hub_fail(e)

        key = r["session_key"]
        address = r["address"]
        self.store.upsert_session(
            key,
            harness,
            session_id,
            address,
            r.get("name") or name or session_id[:8],
            cwd,
            _deliver_fields(harness, deliver),  # local only: the Claude token never goes to the hub
            mcp_pid,
            time.time(),
        )
        self._log("registered %s address=%s mcp_pid=%d" % (key, address, mcp_pid))
        return {"ok": True, "session_key": key, "address": address}

    def _op_send(self, req, peer_pid):
        key = self._session_key(req)
        sess = self.store.get_session(key) if key else None
        if sess is None:
            return _fail("not_registered", "register this session before sending")
        to, text = req.get("to"), req.get("text")
        if not isinstance(to, str) or not to:
            return _fail("bad_request", "to required")
        if not isinstance(text, str):
            return _fail("bad_request", "text required")
        if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            return _fail("too_large", "text exceeds 64 KiB")
        cid = req.get("client_msg_id") or str(uuid.uuid4())
        try:
            try:
                r = self.hub.send(key, to, text, cid)
            except HubError as e:
                if e.code != "unknown_sender":
                    raise
                # Hub lost its state: re-register this session once and retry.
                self._log("hub does not know %s; re-registering and retrying send" % key)
                self._hub_register_session(sess)
                r = self.hub.send(key, to, text, cid)
        except HubError as e:
            return self._hub_fail(e)
        out = dict(r)
        out["ok"] = True
        return out

    def _op_peers(self, req, peer_pid):
        try:
            r = self.hub.peers()
        except HubError as e:
            return self._hub_fail(e)
        return {"ok": True, "peers": r.get("peers", [])}

    def _op_status(self, req, peer_pid):
        if req.get("msg_id"):
            try:
                out = dict(self.hub.status(str(req["msg_id"])))
            except HubError as e:
                return self._hub_fail(e)
            out["ok"] = True
            return out
        hub_ok, hub_version = True, None
        try:
            hub_version = self.hub.health().get("version")
        except HubError:
            hub_ok = False
        sessions = [
            {"session_key": s["session_key"], "address": s["address"], "live": pid_alive(s["mcp_pid"])}
            for s in self.store.list_sessions()
        ]
        return {
            "ok": True,
            "pid": os.getpid(),
            "machine": self.machine,
            "hub_url": self.hub.base_url,
            "hub_ok": hub_ok,
            "hub_version": hub_version,
            "sessions": sessions,
        }

    def _op_unregister(self, req, peer_pid):
        key = self._session_key(req)
        if key is None:
            return _fail("bad_request", "session_key or harness+session_id required")
        return {"ok": True, "removed": self.store.delete_session(key)}

    # ----- heartbeat and report flushing ------------------------------------

    def _hub_register_session(self, sess):
        """Idempotent (re-)registration of a locally known session. The hub DB may be lost."""
        body = {
            "machine": self.machine,
            "harness": sess["harness"],
            "session_id": sess["session_id"],
        }
        if sess.get("name"):
            body["name"] = sess["name"]
        if sess.get("cwd"):
            body["cwd"] = sess["cwd"]
        r = self.hub.register(body)
        if r.get("session_key") != sess["session_key"]:
            self._log("hub returned key %s for local %s" % (r.get("session_key"), sess["session_key"]))
        return r

    def heartbeat_once(self):
        """Re-register every live local session, then heartbeat the live list.

        Re-registering on every cycle is what restores sessions after a hub restart with a
        fresh DB; the hub treats register as idempotent.
        """
        live_sessions = [s for s in self.store.list_sessions() if pid_alive(s["mcp_pid"])]
        for s in live_sessions:
            try:
                self._hub_register_session(s)
            except HubError as e:
                self._log("re-register %s failed: %s" % (s["session_key"], e))
        live = [s["session_key"] for s in live_sessions]
        self.hub.heartbeat(self.machine, live)
        self.store.touch_sessions(live, time.time())
        return live

    def _heartbeat_loop(self):
        while not self._stop.is_set():
            try:
                self.heartbeat_once()
            except HubError as e:
                self._log("heartbeat failed: %s" % e)
            self.flush_reports()
            self._stop.wait(self.heartbeat_interval)

    def _send_report(self, msg_id, state, detail):
        """True when the hub has the result or will never accept it (stop retrying).

        4xx other than 401/429 is final: e.g. 409 terminal_state (hub already holds a different
        final state) or 404 unknown_message (hub DB lost). Transport and 5xx errors defer.
        """
        try:
            self.hub.report(msg_id, state, (detail or "")[:MAX_DETAIL] or None)
        except HubError as e:
            if 400 <= e.status < 500 and e.status not in (401, 429):
                self._log("report %s %s rejected by hub: %s" % (msg_id, state, e))
                return True
            self._log("report %s %s deferred: %s" % (msg_id, state, e))
            return False
        return True

    def flush_reports(self):
        """Send results that were recorded but not yet reported. Returns count reported."""
        done = 0
        for row in self.store.unreported():
            if not self._send_report(row["msg_id"], row["state"], row["detail"]):
                break
            self.store.mark_reported(row["msg_id"])
            done += 1
        return done

    def _try_report(self, msg_id, state, detail):
        if self._send_report(msg_id, state, detail):
            self.store.mark_reported(msg_id)

    # ----- pull loop and injection -----------------------------------------

    def _pull_loop(self):
        delay = 1.0
        while not self._stop.is_set():
            try:
                msgs = self.hub.pull(self.machine, self.poll_wait)
            except HubError as e:
                self._log("pull failed: %s; retry in %.0fs" % (e, delay))
                self._stop.wait(delay)
                delay = min(delay * 2, BACKOFF_MAX)
                continue
            delay = 1.0
            for m in (msgs.get("messages") or []):
                if self._stop.is_set():
                    break
                if not isinstance(m, dict):
                    continue
                try:
                    self._handle_message(m)
                except Exception as e:
                    self._log("message handling failed: %r" % (e,))

    def _handle_message(self, m):
        msg_id = m.get("msg_id")
        if not isinstance(msg_id, str) or not msg_id:
            self._log("pulled message without msg_id; skipped")
            return
        prev = self.store.get_delivery(msg_id)
        if prev is not None:
            # At-most-once: never inject twice. Re-report what was recorded.
            self._log("duplicate pull of %s (recorded %s); not injecting" % (msg_id, prev["state"]))
            if prev["state"] != "injecting":
                self._try_report(msg_id, prev["state"], prev["detail"])
            return
        # Record before injecting; the insert is committed (autocommit) before inject runs.
        if not self.store.record_pending(msg_id, m.get("to_key") or ""):
            return
        state, detail = self._inject(m)
        self.store.set_result(msg_id, state, detail)
        self._log("%s %s: %s" % (msg_id, state, detail))
        self._try_report(msg_id, state, detail)

    def _inject(self, m):
        try:
            to_key = m.get("to_key") or ""
            text = m.get("text")
            if not isinstance(text, str):
                return "failed", "bad message text"
            sess = self.store.get_session(to_key)
            if sess is None:
                return "failed", "no local session for %s" % to_key
            if not pid_alive(sess["mcp_pid"]):
                return "failed", "target session not live (mcp_pid %s)" % sess["mcp_pid"]
            from_address = m.get("from_address") or ""
            local = same_machine_harness(from_address, self.machine, sess["harness"])
            rendered = render(m["msg_id"], from_address, text, local_harness=sess["harness"] if local else None)
            d = sess["deliver"]
            if sess["harness"] == "claude":
                if not d.get("socket"):
                    return "failed", "no claude deliver socket registered"
                ok, detail = inject_claude(d["socket"], d.get("token") or "", rendered)
            elif sess["harness"] == "codex":
                if not d.get("thread_id"):
                    return "failed", "no codex thread_id registered"
                ok, detail = inject_codex(d["thread_id"], d.get("codex_home") or "", rendered, codex_bin=self.codex_bin)
            else:
                return "failed", "unsupported harness %s" % sess["harness"]
            return ("delivered" if ok else "failed"), detail
        except Exception as e:
            return "failed", "internal: %s: %s" % (type(e).__name__, e)

    # ----- tunnels ----------------------------------------------------------

    def _tunnel_loop(self, argv):
        delay = 1.0
        while not self._stop.is_set():
            started = time.monotonic()
            proc = None
            try:
                proc = subprocess.Popen(
                    argv, stdin=subprocess.DEVNULL, stdout=self._logf, stderr=self._logf, close_fds=True
                )
            except OSError as e:
                self._log("tunnel %s failed to start: %s" % (argv[0], e))
            if proc is not None:
                with self._children_lock:
                    self._children.add(proc)
                try:
                    while not self._stop.is_set():
                        try:
                            proc.wait(timeout=1.0)
                            break
                        except subprocess.TimeoutExpired:
                            continue
                finally:
                    with self._children_lock:
                        self._children.discard(proc)
                    if proc.poll() is None:
                        _terminate(proc)
                if proc.poll() is not None:
                    self._log("tunnel %s exited rc=%s" % (argv[0], proc.returncode))
            if self._stop.is_set():
                return
            if time.monotonic() - started > TUNNEL_STABLE_SECS:
                delay = 1.0
            self._log("restarting tunnel in %.0fs" % delay)
            self._stop.wait(delay)
            delay = min(delay * 2, BACKOFF_MAX)


def _terminate(proc):
    try:
        proc.terminate()
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
    except OSError:
        pass


def run_foreground(home=None):
    node = Node(home)
    try:
        node.start(retry_seconds=2.0)
    except AlreadyRunning:
        return 0
    except ConfigError as e:
        sys.stderr.write("agentbus node: %s\n" % e)
        return 1

    def _on_signal(signum, frame):
        node.stop()

    try:
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
    except ValueError:
        pass  # not the main thread
    node.wait()
    node.stop()
    return 0


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if "--ensure" in args:
        try:
            ensure()
        except ConfigError as e:
            sys.stderr.write("agentbus node: %s\n" % e)
            return 1
        return 0
    if "--foreground" in args:
        return run_foreground()
    sys.stderr.write("usage: agentbus node [--ensure|--foreground]\n")
    return 2

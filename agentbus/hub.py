"""agentbus hub: HTTP + SQLite message broker (stdlib only, Python >= 3.10).

Implements the "Hub HTTP API" section of SPEC.md.
"""

import argparse
import hmac
import json
import math
import re
import sqlite3
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

VERSION = "0.1.0"
DEFAULT_PORT = 37460
LEASE_SECONDS = 120.0
QUEUE_TTL_SECONDS = 7 * 24 * 3600.0
MAX_TEXT_BYTES = 64 * 1024
MAX_BODY_BYTES = 1024 * 1024  # request-level guard; text limit is checked separately
MAX_WAIT_SECONDS = 25.0
PULL_BATCH = 100
SWEEP_INTERVAL = 1.0  # long-poll re-checks lease expiry at least this often
HARNESSES = ("claude", "codex")
REPORT_STATES = ("delivered", "failed", "held", "in_doubt")
TERMINAL_STATES = ("delivered", "failed", "held", "in_doubt", "expired")
LOOPBACK_HOSTS = ("127.0.0.1", "localhost")

_MACHINE_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")
_NAME_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")
_SESSION_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_key TEXT PRIMARY KEY,
    machine     TEXT NOT NULL,
    harness     TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    name        TEXT NOT NULL,
    address     TEXT NOT NULL UNIQUE,
    cwd         TEXT,
    online      INTEGER NOT NULL DEFAULT 0,
    last_seen   REAL,
    UNIQUE (machine, harness, name)
);
CREATE INDEX IF NOT EXISTS sessions_machine ON sessions (machine);
CREATE TABLE IF NOT EXISTS messages (
    msg_id        TEXT PRIMARY KEY,
    from_key      TEXT NOT NULL,
    from_address  TEXT NOT NULL,
    to_key        TEXT NOT NULL,
    to_address    TEXT NOT NULL,
    text          TEXT NOT NULL,
    client_msg_id TEXT NOT NULL,
    state         TEXT NOT NULL,
    detail        TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    lease_until   REAL,
    UNIQUE (from_key, client_msg_id)
);
CREATE INDEX IF NOT EXISTS messages_state ON messages (state, created_at);
"""


class ApiError(Exception):
    def __init__(self, status, code, message):
        Exception.__init__(self, message)
        self.status = status
        self.code = code
        self.message = message


def _iso(ts):
    if ts is None:
        return None
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (dt.microsecond // 1000)


def _bad(message):
    return ApiError(400, "bad_request", message)


def _need(value, key, pattern=None, maxlen=512):
    if not isinstance(value, str) or value == "" or len(value) > maxlen:
        raise _bad("%s is required (non-empty string, max %d chars)" % (key, maxlen))
    if pattern is not None and not pattern.fullmatch(value):
        raise _bad("%s has an invalid format" % key)
    return value


def _req_str(body, key, pattern=None, maxlen=512):
    return _need(body.get(key), key, pattern, maxlen)


def _opt_str(body, key, pattern=None, maxlen=512):
    if body.get(key) is None:
        return None
    return _req_str(body, key, pattern, maxlen)


class Hub(object):
    """All state and business rules. Every DB access is serialized by self.lock."""

    def __init__(self, db_path, lease_seconds=LEASE_SECONDS, queue_ttl=QUEUE_TTL_SECONDS):
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.lease_seconds = float(lease_seconds)
        self.queue_ttl = float(queue_ttl)
        self.conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)

    def close(self):
        with self.lock:
            self.conn.close()

    # -- transaction helpers -------------------------------------------------

    def _tx(self, fn, args):
        # caller holds self.lock
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            result = fn(*args)
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")
        return result

    def _write(self, fn, args=(), notify=False):
        with self.cond:
            result = self._tx(fn, args)
            if notify:
                self.cond.notify_all()
            return result

    def _read(self, fn, args=()):
        with self.lock:
            return fn(*args)

    # -- sessions ------------------------------------------------------------

    def register(self, body):
        machine = _req_str(body, "machine", _MACHINE_RE, 64)
        harness = _req_str(body, "harness", None, 16)
        if harness not in HARNESSES:
            raise _bad("harness must be one of: claude, codex")
        session_id = _req_str(body, "session_id", _SESSION_ID_RE, 128)
        name = _opt_str(body, "name", _NAME_RE, 64)
        cwd = body.get("cwd")
        if cwd is not None and (not isinstance(cwd, str) or len(cwd) > 4096):
            raise _bad("cwd must be a string (max 4096 chars)")
        return self._write(self._register_op, (machine, harness, session_id, name, cwd),
                           notify=True)

    def _register_op(self, machine, harness, session_id, name, cwd):
        key = "%s:%s:%s" % (machine, harness, session_id)
        now = time.time()
        row = self.conn.execute("SELECT name FROM sessions WHERE session_key=?",
                                (key,)).fetchone()
        if name is not None:
            use_name = name
        elif row is not None:
            use_name = row["name"]  # re-register keeps the existing name
        else:
            use_name = session_id[:8]
        clash = self.conn.execute(
            "SELECT session_key FROM sessions WHERE machine=? AND harness=? AND name=? "
            "AND session_key<>?", (machine, harness, use_name, key)).fetchone()
        if clash is not None:
            raise ApiError(409, "name_taken",
                           "name %r is already used on %s/%s" % (use_name, machine, harness))
        address = "%s/%s/%s" % (machine, harness, use_name)
        if row is None:
            self.conn.execute(
                "INSERT INTO sessions (session_key, machine, harness, session_id, name, address, "
                "cwd, online, last_seen) VALUES (?,?,?,?,?,?,?,1,?)",
                (key, machine, harness, session_id, use_name, address, cwd, now))
        else:
            self.conn.execute(
                "UPDATE sessions SET name=?, address=?, cwd=COALESCE(?, cwd), online=1, "
                "last_seen=? WHERE session_key=?",
                (use_name, address, cwd, now, key))
        return {"session_key": key, "address": address}

    def heartbeat(self, body):
        machine = _req_str(body, "machine", _MACHINE_RE, 64)
        live = body.get("live")
        if not isinstance(live, list) or not all(isinstance(k, str) for k in live):
            raise _bad("live must be a list of session_key strings")
        return self._write(self._heartbeat_op, (machine, set(live)), notify=True)

    def _heartbeat_op(self, machine, live):
        now = time.time()
        rows = self.conn.execute(
            "SELECT session_key, online FROM sessions WHERE machine=?", (machine,)).fetchall()
        for r in rows:
            key = r["session_key"]
            if key in live:
                self.conn.execute("UPDATE sessions SET online=1, last_seen=? WHERE session_key=?",
                                  (now, key))
            elif r["online"]:
                self.conn.execute("UPDATE sessions SET online=0 WHERE session_key=?", (key,))
        return {"ok": True}

    def peers(self):
        return self._read(self._peers_op)

    def _peers_op(self):
        rows = self.conn.execute("SELECT * FROM sessions ORDER BY address").fetchall()
        peers = []
        for r in rows:
            peers.append({
                "address": r["address"],
                "session_key": r["session_key"],
                "machine": r["machine"],
                "harness": r["harness"],
                "name": r["name"],
                "cwd": r["cwd"],
                "online": bool(r["online"]),
                "last_seen": _iso(r["last_seen"]),
            })
        return {"peers": peers}

    def _resolve(self, to):
        row = self.conn.execute("SELECT * FROM sessions WHERE address=?", (to,)).fetchone()
        if row is None:
            row = self.conn.execute("SELECT * FROM sessions WHERE session_key=?",
                                    (to,)).fetchone()
        return row

    # -- messages ------------------------------------------------------------

    def send(self, from_key, to, text, client_msg_id):
        if not isinstance(text, str) or text == "":
            raise _bad("text is required (non-empty string)")
        try:
            size = len(text.encode("utf-8"))
        except UnicodeEncodeError:
            raise _bad("text is not valid UTF-8 text")
        if size > MAX_TEXT_BYTES:
            raise ApiError(413, "too_large", "text is %d bytes; limit is %d"
                           % (size, MAX_TEXT_BYTES))
        from_key = _need(from_key, "from_key", None, 512)
        to = _need(to, "to", None, 512)
        client_msg_id = _need(client_msg_id, "client_msg_id", None, 200)
        return self._write(self._send_op, (from_key, to, text, client_msg_id), notify=True)

    def _send_op(self, from_key, to, text, client_msg_id):
        sender = self.conn.execute("SELECT address FROM sessions WHERE session_key=?",
                                   (from_key,)).fetchone()
        if sender is None:
            raise ApiError(404, "unknown_sender", "from_key is not a registered session")
        dup = self.conn.execute(
            "SELECT m.msg_id, m.to_address, s.online FROM messages m "
            "LEFT JOIN sessions s ON s.session_key = m.to_key "
            "WHERE m.from_key=? AND m.client_msg_id=?", (from_key, client_msg_id)).fetchone()
        if dup is not None:
            return {"msg_id": dup["msg_id"], "target": dup["to_address"],
                    "target_online": bool(dup["online"])}
        target = self._resolve(to)
        if target is None:
            raise ApiError(404, "unknown_target", "no session with address or session_key %r"
                           % to)
        now = time.time()
        msg_id = uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO messages (msg_id, from_key, from_address, to_key, to_address, text, "
            "client_msg_id, state, detail, created_at, updated_at, lease_until) "
            "VALUES (?,?,?,?,?,?,?,?,NULL,?,?,NULL)",
            (msg_id, from_key, sender["address"], target["session_key"], target["address"],
             text, client_msg_id, "queued", now, now))
        return {"msg_id": msg_id, "target": target["address"],
                "target_online": bool(target["online"])}

    def _sweep(self, now):
        # leases past their deadline return to queued
        self.conn.execute(
            "UPDATE messages SET state=?, lease_until=NULL, updated_at=? "
            "WHERE state=? AND lease_until < ?", ("queued", now, "leased", now))
        # queued too long -> expired
        self.conn.execute(
            "UPDATE messages SET state=?, detail=?, updated_at=? WHERE state=? AND created_at < ?",
            ("expired", "queued for more than 7 days", now, "queued", now - self.queue_ttl))

    def pull(self, machine, wait):
        """Long-poll: lease deliverable messages for sessions on `machine`."""
        deadline = time.monotonic() + wait
        with self.cond:
            while True:
                msgs = self._tx(self._lease_op, (machine,))
                remaining = deadline - time.monotonic()
                if msgs or remaining <= 0:
                    return msgs
                self.cond.wait(min(remaining, SWEEP_INTERVAL))

    def _lease_op(self, machine):
        now = time.time()
        self._sweep(now)
        rows = self.conn.execute(
            "SELECT m.msg_id, m.from_address, m.to_key, m.text, m.created_at "
            "FROM messages m JOIN sessions s ON s.session_key = m.to_key "
            "WHERE m.state=? AND s.machine=? AND s.online=1 "
            "ORDER BY m.created_at, m.msg_id LIMIT ?",
            ("queued", machine, PULL_BATCH)).fetchall()
        out = []
        for r in rows:
            self.conn.execute(
                "UPDATE messages SET state=?, lease_until=?, updated_at=? WHERE msg_id=?",
                ("leased", now + self.lease_seconds, now, r["msg_id"]))
            out.append({
                "msg_id": r["msg_id"],
                "from_address": r["from_address"],
                "to_key": r["to_key"],
                "text": r["text"],
                "created_at": _iso(r["created_at"]),
            })
        return out

    def report(self, msg_id, state, detail):
        msg_id = _need(msg_id, "msg_id", None, 128)
        if state not in REPORT_STATES:
            raise _bad("state must be one of: %s" % ", ".join(REPORT_STATES))
        if detail is not None and not isinstance(detail, str):
            raise _bad("detail must be a string")
        return self._write(self._report_op, (msg_id, state, detail))

    def _report_op(self, msg_id, state, detail):
        row = self.conn.execute("SELECT state FROM messages WHERE msg_id=?",
                                (msg_id,)).fetchone()
        if row is None:
            raise ApiError(404, "unknown_message", "no such msg_id")
        current = row["state"]
        if current in TERMINAL_STATES:
            if current == state:
                return {"ok": True, "state": current}
            raise ApiError(409, "terminal_state",
                           "message is already in terminal state %s" % current)
        now = time.time()
        self.conn.execute(
            "UPDATE messages SET state=?, detail=?, lease_until=NULL, updated_at=? "
            "WHERE msg_id=?", (state, detail, now, msg_id))
        return {"ok": True, "state": state}

    def status(self, msg_id):
        msg_id = _need(msg_id, "msg_id", None, 128)
        return self._write(self._status_op, (msg_id,))

    def _status_op(self, msg_id):
        self._sweep(time.time())
        r = self.conn.execute("SELECT * FROM messages WHERE msg_id=?", (msg_id,)).fetchone()
        if r is None:
            raise ApiError(404, "unknown_message", "no such msg_id")
        return {
            "msg_id": r["msg_id"],
            "state": r["state"],
            "detail": r["detail"],
            "target": r["to_address"],
            "created_at": _iso(r["created_at"]),
            "updated_at": _iso(r["updated_at"]),
        }


class HubServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, addr, hub, token):
        self.hub = hub
        self.token_bytes = token.encode("utf-8")
        ThreadingHTTPServer.__init__(self, addr, Handler)


_ROUTES = {
    ("POST", "/v1/register"),
    ("POST", "/v1/heartbeat"),
    ("GET", "/v1/peers"),
    ("POST", "/v1/send"),
    ("GET", "/v1/pull"),
    ("POST", "/v1/report"),
    ("GET", "/v1/status"),
}
_PATHS = set(p for _, p in _ROUTES) | {"/v1/health"}


def _qs(query, name):
    values = query.get(name)
    return values[0] if values else None


class Handler(BaseHTTPRequestHandler):
    server_version = "agentbus-hub/" + VERSION
    sys_version = ""

    def log_request(self, code="-", size="-"):
        # one line per request: no bodies, no headers, no tokens
        started = getattr(self, "_started", None)
        elapsed_ms = (time.monotonic() - started) * 1000.0 if started else 0.0
        path = (getattr(self, "path", None) or "-").split("?", 1)[0]
        method = getattr(self, "command", None) or "-"
        sys.stderr.write("%s %s %s %s %s %.1fms\n" % (
            time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()), method, path, code,
            self.client_address[0] if self.client_address else "-", elapsed_ms))
        sys.stderr.flush()

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _authorized(self):
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        presented = header[len("Bearer "):].strip().encode("utf-8")
        return hmac.compare_digest(presented, self.server.token_bytes)

    def _read_body(self):
        raw_len = self.headers.get("Content-Length")
        try:
            length = int(raw_len) if raw_len is not None else 0
        except ValueError:
            raise _bad("invalid Content-Length")
        if length < 0:
            raise _bad("invalid Content-Length")
        if length > MAX_BODY_BYTES:
            raise ApiError(413, "too_large", "request body exceeds limit")
        raw = self.rfile.read(length) if length else b""
        if len(raw) != length:
            raise _bad("truncated request body")
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise _bad("body is not valid JSON")
        if not isinstance(obj, dict):
            raise _bad("body must be a JSON object")
        return obj

    def _send_json(self, status, payload):
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _dispatch(self, method):
        self._started = time.monotonic()
        url = urlsplit(self.path)
        path = url.path
        try:
            if method == "GET" and path == "/v1/health":
                self._send_json(200, {"ok": True, "version": VERSION})
                return
            if path not in _PATHS:
                raise ApiError(404, "not_found", "no such endpoint")
            if (method, path) not in _ROUTES:
                raise ApiError(405, "method_not_allowed", "method not allowed for this endpoint")
            if not self._authorized():
                raise ApiError(401, "unauthorized", "missing or invalid bearer token")
            payload = self._handle(method, path, url.query)
            self._send_json(200, payload)
        except ApiError as e:
            self._send_json(e.status, {"error": e.code, "message": e.message})
        except Exception:
            traceback.print_exc()
            self._send_json(500, {"error": "internal", "message": "internal error"})

    def _handle(self, method, path, query_string):
        hub = self.server.hub
        if method == "GET":
            query = parse_qs(query_string, keep_blank_values=True)
            if path == "/v1/peers":
                return hub.peers()
            if path == "/v1/pull":
                machine = _qs(query, "machine")
                if machine is None or not _MACHINE_RE.fullmatch(machine):
                    raise _bad("machine query parameter is required and must be valid")
                wait = _parse_wait(_qs(query, "wait"))
                return {"messages": hub.pull(machine, wait)}
            if path == "/v1/status":
                msg_id = _qs(query, "msg_id")
                if msg_id is None:
                    raise _bad("msg_id query parameter is required")
                return hub.status(msg_id)
            raise ApiError(405, "method_not_allowed", "method not allowed for this endpoint")

        body = self._read_body()
        if path == "/v1/register":
            return hub.register(body)
        if path == "/v1/heartbeat":
            return hub.heartbeat(body)
        if path == "/v1/send":
            return hub.send(body.get("from_key"), body.get("to"), body.get("text"),
                            body.get("client_msg_id"))
        if path == "/v1/report":
            return hub.report(body.get("msg_id"), body.get("state"), body.get("detail"))
        raise ApiError(405, "method_not_allowed", "method not allowed for this endpoint")


def _parse_wait(raw):
    if raw is None or raw == "":
        return 0.0
    try:
        value = float(raw)
    except ValueError:
        raise _bad("wait must be a number of seconds")
    if not math.isfinite(value):
        raise _bad("wait must be a finite number of seconds")
    return min(max(value, 0.0), MAX_WAIT_SECONDS)


def make_server(db_path, token, host="127.0.0.1", port=DEFAULT_PORT,
                lease_seconds=LEASE_SECONDS, queue_ttl=QUEUE_TTL_SECONDS):
    """Build (but do not run) a hub server. server.hub is the Hub; port 0 = ephemeral."""
    if host not in LOOPBACK_HOSTS:
        raise ValueError("hub must bind a loopback address (127.0.0.1), not %r" % host)
    if not token:
        raise ValueError("token must be non-empty")
    hub = Hub(db_path, lease_seconds=lease_seconds, queue_ttl=queue_ttl)
    try:
        return HubServer((host, port), hub, token)
    except BaseException:
        hub.close()
        raise


def _read_token(token_file):
    with open(token_file, "r", encoding="utf-8") as f:
        token = f.read().strip()
    if not token:
        raise ValueError("token file %s is empty" % token_file)
    return token


def serve(db_path, token_file, host="127.0.0.1", port=DEFAULT_PORT):
    token = _read_token(token_file)
    server = make_server(db_path, token, host=host, port=port)
    sys.stderr.write("agentbus hub %s listening on %s:%d db=%s\n"
                     % (VERSION, host, server.server_address[1], db_path))
    sys.stderr.flush()
    try:
        server.serve_forever()
    finally:
        server.server_close()
        server.hub.close()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="agentbus hub")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--token-file", required=True, help="file containing the bearer token")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args(argv)
    try:
        serve(args.db, args.token_file, host=args.host, port=args.port)
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, sqlite3.Error) as e:
        sys.stderr.write("agentbus hub: %s\n" % e)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

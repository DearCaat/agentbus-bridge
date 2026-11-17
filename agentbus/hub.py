"""agentbus hub: HTTP + SQLite message broker (stdlib only, Python >= 3.10).

Implements the "Hub HTTP API" section of SPEC.md.
"""

import argparse
import hashlib
import hmac
import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from urllib.parse import parse_qs, urlsplit

from . import __version__

VERSION = __version__
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
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3


def _positive_int(value):
    if isinstance(value, str):
        try:
            value = int(value)
        except ValueError:
            raise ValueError("must be a positive integer")
    if type(value) is not int or value <= 0:
        raise ValueError("must be a positive integer")
    return value


class _PrivateRotatingHandler(RotatingFileHandler):
    def _open(self):
        # os.open's creation mode also protects the interval before chmod. An
        # inherited existing log may have been created by shell redirection as 644.
        fd = os.open(self.baseFilename, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.fchmod(fd, 0o600)
            return os.fdopen(fd, self.mode, encoding=self.encoding, errors=self.errors)
        except BaseException:
            os.close(fd)
            raise


class HubLog:
    """Synchronous complete-record logging; rotation is covered by the same lock.

    No queue or stdout/stderr replacement: every write is flushed before return.
    Close shares the write lock, so an in-progress record finishes before close.
    """
    def __init__(self, path=None, max_bytes=LOG_MAX_BYTES, backup_count=LOG_BACKUP_COUNT):
        max_bytes = _positive_int(max_bytes)
        backup_count = _positive_int(backup_count)
        self._lock = threading.RLock()
        self.closed = False
        self.handler = None
        if path is not None:
            path = os.path.abspath(os.fspath(path))
            for index in range(1, backup_count + 1):
                try:
                    os.chmod(path + ".%d" % index, 0o600)
                except FileNotFoundError:
                    pass
            self.handler = _PrivateRotatingHandler(path, maxBytes=max_bytes,
                                                  backupCount=backup_count, encoding="utf-8")
            self.handler.setFormatter(logging.Formatter("%(message)s"))

    def write(self, record):
        message = json.dumps(record, separators=(",", ":"))
        with self._lock:
            if self.closed:
                # Late daemon-worker diagnostics after server_close must neither
                # reopen a file handler nor write to a closed descriptor.
                return
            if self.handler is None:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            else:
                entry = logging.LogRecord("agentbus.hub", logging.INFO, "", 0, message, (), None)
                self.handler.handle(entry)

    def close(self):
        with self._lock:
            if not self.closed:
                if self.handler is not None:
                    self.handler.flush()
                    self.handler.close()
                self.closed = True

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

SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS streams_v2 (
    from_key TEXT NOT NULL, to_key TEXT NOT NULL, last_enqueued INTEGER NOT NULL,
    PRIMARY KEY (from_key, to_key)
);
CREATE TABLE IF NOT EXISTS messages_v2 (
    message_order INTEGER PRIMARY KEY AUTOINCREMENT,
    msg_id TEXT NOT NULL UNIQUE, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
    from_address TEXT NOT NULL, to_address TEXT NOT NULL,
    client_msg_id TEXT NOT NULL, seq INTEGER NOT NULL, reply_to TEXT,
    text TEXT, sha256 TEXT NOT NULL, text_bytes INTEGER NOT NULL,
    fingerprint TEXT NOT NULL, state TEXT NOT NULL, detail TEXT,
    attempt INTEGER NOT NULL DEFAULT 0, lease_until REAL,
    created_at REAL NOT NULL, updated_at REAL NOT NULL, acked_at REAL,
    received_at REAL, injected_at REAL,
    state_revision INTEGER NOT NULL DEFAULT 0,
    history_pruned INTEGER NOT NULL DEFAULT 0,
    UNIQUE (from_key, client_msg_id), UNIQUE (from_key, to_key, seq)
);
CREATE INDEX IF NOT EXISTS messages_v2_head ON messages_v2(from_key,to_key,acked_at,seq);
CREATE INDEX IF NOT EXISTS messages_v2_inbox ON messages_v2(to_key,message_order);
CREATE TABLE IF NOT EXISTS events_v2 (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, msg_id TEXT NOT NULL,
    time REAL NOT NULL, seq INTEGER NOT NULL, attempt INTEGER NOT NULL,
    state TEXT NOT NULL, reason TEXT NOT NULL, detail TEXT
);
CREATE INDEX IF NOT EXISTS events_v2_message ON events_v2(msg_id,event_id);
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


def _positive_days(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("retention days must be finite and positive")
    return value


def _integer(value, name, minimum=0, maximum=2**63 - 1):
    if isinstance(value, str):
        if not value.isascii() or not value.isdigit():
            raise _bad("%s must be an integer" % name)
        try:
            value = int(value)
        except ValueError:
            raise _bad("%s must be an integer" % name)
    if type(value) is not int or not minimum <= value <= maximum:
        raise _bad("%s must be between %d and %d" % (name, minimum, maximum))
    return value


class Hub(object):
    """All state and business rules. Every DB access is serialized by self.lock."""

    def __init__(self, db_path, lease_seconds=LEASE_SECONDS, queue_ttl=QUEUE_TTL_SECONDS,
                 body_retention_days=7, event_retention_days=30):
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.lease_seconds = float(lease_seconds)
        self.queue_ttl = float(queue_ttl)
        self.body_retention = _positive_days(body_retention_days) * 86400
        self.event_retention = _positive_days(event_retention_days) * 86400
        self.conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self.conn.executescript(SCHEMA_V2)
        self._write(self._migrate_receipts_op)

    def _migrate_receipts_op(self):
        columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(messages_v2)")}
        legacy = "received_at" not in columns
        if legacy:
            self.conn.execute("ALTER TABLE messages_v2 ADD COLUMN received_at REAL")
        if "injected_at" not in columns:
            self.conn.execute("ALTER TABLE messages_v2 ADD COLUMN injected_at REAL")
            self.conn.execute("UPDATE messages_v2 SET injected_at=updated_at WHERE state='injected'")
        if legacy:
            # Only old-schema ACKs carry this compatibility meaning. New optional
            # read declarations must never become durable receipts after restart.
            rows = self.conn.execute("SELECT msg_id,acked_at FROM messages_v2 WHERE acked_at IS NOT NULL").fetchall()
            for row in rows:
                self.conn.execute("UPDATE messages_v2 SET received_at=? WHERE msg_id=?", (row["acked_at"], row["msg_id"]))
                self._event2(row["msg_id"], "legacy_ack_receipt_migrated")
        self.conn.execute("CREATE INDEX IF NOT EXISTS messages_v2_receipts_head "
                          "ON messages_v2(from_key,to_key,received_at,seq)")

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


    # -- protocol 2: durable identity, causal streams, explicit receipt --------

    def _row2(self, msg_id):
        return self.conn.execute("SELECT * FROM messages_v2 WHERE msg_id=?", (msg_id,)).fetchone()

    def _event2(self, msg_id, reason, now=None):
        row = self._row2(msg_id)
        now = time.time() if now is None else now
        cur = self.conn.execute(
            "INSERT INTO events_v2(msg_id,time,seq,attempt,state,reason,detail) VALUES(?,?,?,?,?,?,?)",
            (msg_id, now, row["seq"], row["attempt"], row["state"], reason, row["detail"]))
        self.conn.execute("UPDATE messages_v2 SET state_revision=?,updated_at=? WHERE msg_id=?",
                          (cur.lastrowid, now, msg_id))

    def _blocked2(self, row):
        previous = self.conn.execute(
            "SELECT msg_id FROM messages_v2 WHERE from_key=? AND to_key=? "
            "AND seq<? AND received_at IS NULL ORDER BY seq LIMIT 1",
            (row["from_key"], row["to_key"], row["seq"])).fetchone()
        return previous["msg_id"] if previous else None

    def _snapshot2(self, row, full=False):
        out = {k: row[k] for k in (
            "msg_id", "client_msg_id", "from_key", "to_key", "from_address", "to_address",
            "seq", "reply_to", "state", "attempt", "detail", "sha256", "text_bytes",
            "message_order", "state_revision")}
        injection = row["state"] if row["state"] in ("injected", "failed", "in_doubt") else "unknown"
        out.update(submission="accepted", receiver_acknowledged=row["acked_at"] is not None,
                   receiver_persisted=row["received_at"] is not None, received_at=_iso(row["received_at"]),
                   injected_at=_iso(row["injected_at"]), injection_state=injection,
                   agent_read=True if row["acked_at"] is not None else None, task_completion=None,
                   blocked_by=self._blocked2(row), body_retained=row["text"] is not None,
                   history_pruned=bool(row["history_pruned"]), observed_at=_iso(time.time()),
                   ordering_scope="sender_to_receiver", created_at=_iso(row["created_at"]),
                   updated_at=_iso(row["updated_at"]), acknowledged_at=_iso(row["acked_at"]),
                   summary=(("Recipient plugin durably stored the full message. " if row["received_at"] is not None
                             else "Hub accepted; recipient durable storage is not confirmed. ") +
                            "Native presentation: %s. Agent read: %s. Task completion: unknown." %
                            (injection, "explicitly declared" if row["acked_at"] is not None else "unknown")))
        if full:
            out["text"] = row["text"]
        return out

    def _participant2(self, msg_id, session_key):
        row = self._row2(msg_id)
        if row is not None and session_key not in (row["from_key"], row["to_key"]):
            raise ApiError(403, "not_participant", "session is not a message participant")
        return row

    def _unknown2(self, msg_id):
        return dict(msg_id=msg_id, state="unknown_or_pruned", submission="unknown",
                    receiver_acknowledged=None, body_retained=False, text=None,
                    receiver_persisted=None, received_at=None, injected_at=None,
                    injection_state="unknown", agent_read=None, task_completion=None,
                    client_msg_id=None, from_key=None, to_key=None, seq=None,
                    blocked_by=None, attempt=None, state_revision=None,
                    observed_at=_iso(time.time()), ordering_scope="sender_to_receiver",
                    summary="No retained record; this does not establish whether a send occurred.")

    def send2(self, body):
        from_key = _req_str(body, "from_key")
        to = _req_str(body, "to")
        text = body.get("text")
        if not isinstance(text, str) or not text:
            raise _bad("text is required (non-empty string)")
        try:
            data = text.encode("utf-8")
        except UnicodeEncodeError:
            raise _bad("text is not valid UTF-8")
        if len(data) > MAX_TEXT_BYTES:
            raise ApiError(413, "too_large", "text exceeds 64 KiB")
        cid = _req_str(body, "client_msg_id", maxlen=200)
        seq = _integer(body.get("client_seq"), "client_seq", 1)
        reply = _opt_str(body, "reply_to", maxlen=128)
        return self._write(self._send2_op, (from_key, to, text, cid, seq, reply, data), notify=True)

    def _send2_op(self, from_key, to, text, cid, seq, reply, data):
        sender, target = self._resolve(from_key), self._resolve(to)
        if sender is None or sender["session_key"] != from_key:
            raise ApiError(404, "unknown_sender", "sender is not registered")
        if target is None:
            raise ApiError(404, "unknown_target", "target is not registered")
        to_key = target["session_key"]
        digest = hashlib.sha256(data).hexdigest()
        fingerprint = hashlib.sha256(json.dumps(
            [from_key, to_key, text, seq, reply], ensure_ascii=True, separators=(",", ":")
        ).encode("ascii")).hexdigest()
        duplicate = self.conn.execute(
            "SELECT * FROM messages_v2 WHERE from_key=? AND client_msg_id=?", (from_key, cid)).fetchone()
        if duplicate:
            if duplicate["fingerprint"] != fingerprint:
                raise ApiError(409, "idempotency_conflict", "request ID was used with different payload or sequence")
            return self._snapshot2(duplicate)
        if reply:
            previous = self._row2(reply)
            if previous is None or previous["from_key"] != to_key or previous["to_key"] != from_key:
                raise ApiError(409, "invalid_reply_to", "reply_to must identify the opposite direction between these peers")
            if previous["text"] is None:
                raise ApiError(409, "body_pruned", "reply parent content is no longer retained")
        stream = self.conn.execute("SELECT last_enqueued FROM streams_v2 WHERE from_key=? AND to_key=?",
                                   (from_key, to_key)).fetchone()
        expected = stream["last_enqueued"] + 1 if stream else 1
        if seq != expected:
            raise ApiError(409, "sequence_conflict", "expected client_seq %d" % expected)
        now, msg_id = time.time(), uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO messages_v2(msg_id,from_key,to_key,from_address,to_address,client_msg_id,seq,"
            "reply_to,text,sha256,text_bytes,fingerprint,state,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (msg_id, from_key, to_key, sender["address"], target["address"], cid, seq, reply,
             text, digest, len(data), fingerprint, "queued", now, now))
        self.conn.execute("INSERT INTO streams_v2(from_key,to_key,last_enqueued) VALUES(?,?,?) "
                          "ON CONFLICT(from_key,to_key) DO UPDATE SET last_enqueued=excluded.last_enqueued",
                          (from_key, to_key, seq))
        self._event2(msg_id, "accepted", now)
        return self._snapshot2(self._row2(msg_id))

    def _sweep2_op(self, now):
        rows = self.conn.execute("SELECT * FROM messages_v2 WHERE state='leased' AND lease_until<? "
                                 "AND received_at IS NULL", (now,)).fetchall()
        for row in rows:
            self.conn.execute("UPDATE messages_v2 SET state='queued',lease_until=NULL WHERE msg_id=?",
                              (row["msg_id"],))
            self._event2(row["msg_id"], "lease_expired", now)
        rows = self.conn.execute("SELECT msg_id FROM messages_v2 WHERE state='queued' AND created_at<? "
                                 "AND received_at IS NULL", (now-self.queue_ttl,)).fetchall()
        for row in rows:
            self.conn.execute("UPDATE messages_v2 SET state='expired',lease_until=NULL WHERE msg_id=?",
                              (row["msg_id"],))
            self._event2(row["msg_id"], "queue_expired_without_durable_receipt", now)
        # A successful native write alone is not a recovery copy. Both durable
        # storage and successful injection are required without an explicit ACK.
        eligible_at = ("CASE WHEN acked_at IS NOT NULL THEN acked_at "
                       "WHEN received_at IS NOT NULL AND injected_at IS NOT NULL AND state='injected' "
                       "THEN MAX(received_at,injected_at) END")
        rows = self.conn.execute("SELECT msg_id FROM messages_v2 WHERE (" + eligible_at + ")<? AND text IS NOT NULL",
                                 (now-self.body_retention,)).fetchall()
        for row in rows:
            self.conn.execute("UPDATE messages_v2 SET text=NULL WHERE msg_id=?", (row["msg_id"],))
            self._event2(row["msg_id"], "body_pruned", now)
        rows = self.conn.execute("SELECT msg_id FROM messages_v2 WHERE (" + eligible_at + ")<? AND history_pruned=0",
                                 (now-self.event_retention,)).fetchall()
        for row in rows:
            # Keep the minimal state/tombstone and record the fact of pruning atomically.
            self.conn.execute("DELETE FROM events_v2 WHERE msg_id=?", (row["msg_id"],))
            self.conn.execute("UPDATE messages_v2 SET history_pruned=1 WHERE msg_id=?", (row["msg_id"],))
            self._event2(row["msg_id"], "history_pruned", now)

    def sweep2(self):
        return self._write(self._sweep2_op, (time.time(),), notify=True)

    def pull2(self, machine, wait):
        machine = _need(machine, "machine", _MACHINE_RE, 64)
        wait = _parse_wait(str(wait))
        deadline = time.monotonic() + wait
        with self.cond:
            while True:
                result = self._tx(self._lease2_op, (machine,))
                remaining = deadline - time.monotonic()
                if result or remaining <= 0:
                    return result
                self.cond.wait(min(remaining, SWEEP_INTERVAL))

    def _lease2_op(self, machine):
        now = time.time()
        self._sweep2_op(now)
        rows = self.conn.execute(
            "SELECT m.* FROM messages_v2 m JOIN sessions s ON s.session_key=m.to_key "
            "WHERE s.machine=? AND s.online=1 AND m.state='queued' AND m.received_at IS NULL "
            "AND NOT EXISTS(SELECT 1 FROM messages_v2 p WHERE p.from_key=m.from_key "
            "AND p.to_key=m.to_key AND p.seq<m.seq AND p.received_at IS NULL) "
            "ORDER BY m.message_order LIMIT ?", (machine, PULL_BATCH)).fetchall()
        result = []
        for row in rows:
            self.conn.execute("UPDATE messages_v2 SET state='leased',attempt=attempt+1,lease_until=? "
                              "WHERE msg_id=?", (now+self.lease_seconds, row["msg_id"]))
            self._event2(row["msg_id"], "leased", now)
            result.append(self._snapshot2(self._row2(row["msg_id"]), full=True))
        return result

    def receive2(self, body):
        msg_id = _req_str(body, "msg_id", maxlen=128)
        recipient = _req_str(body, "session_key")
        attempt = _integer(body.get("attempt"), "attempt", 0)
        sha256 = _req_str(body, "sha256", re.compile(r"[0-9a-f]{64}"), 64)
        text_bytes = _integer(body.get("text_bytes"), "text_bytes", 1, MAX_TEXT_BYTES)
        return self._write(self._receive2_op, (msg_id, recipient, attempt, sha256, text_bytes), notify=True)

    def _receive2_op(self, msg_id, recipient, attempt, sha256, text_bytes):
        row = self._row2(msg_id)
        if row is None:
            raise ApiError(404, "unknown_message", "no retained message")
        if row["to_key"] != recipient:
            raise ApiError(403, "wrong_recipient", "only exact recipient may confirm durable storage")
        if row["attempt"] != attempt:
            raise ApiError(409, "stale_attempt", "receipt does not match current delivery attempt")
        if row["sha256"] != sha256 or row["text_bytes"] != text_bytes:
            raise ApiError(409, "integrity_conflict", "receipt does not match immutable message content")
        if row["received_at"] is not None:
            return self._snapshot2(row)
        blocked = self._blocked2(row)
        if blocked:
            raise ApiError(409, "blocked_by", "persist predecessor %s first" % blocked)
        now = time.time()
        self.conn.execute("UPDATE messages_v2 SET received_at=?,lease_until=NULL,"
                          "state=CASE WHEN state IN ('queued','leased') THEN 'received' ELSE state END WHERE msg_id=?",
                          (now, msg_id))
        self._event2(msg_id, "recipient_persisted", now)
        return self._snapshot2(self._row2(msg_id))

    def report2(self, body):
        msg_id = _req_str(body, "msg_id", maxlen=128)
        attempt = _integer(body.get("attempt"), "attempt", 0)
        state = body.get("state")
        if state not in ("injected", "failed", "in_doubt"):
            raise _bad("state must be injected, failed or in_doubt")
        detail = body.get("detail")
        if detail is not None and (not isinstance(detail, str) or len(detail) > 2000):
            raise _bad("detail must be a string of at most 2000 characters")
        return self._write(self._report2_op, (msg_id, attempt, state, detail), notify=True)

    def _report2_op(self, msg_id, attempt, state, detail):
        row = self._row2(msg_id)
        if row is None:
            raise ApiError(404, "unknown_message", "no retained message")
        if row["attempt"] != attempt:
            raise ApiError(409, "stale_attempt", "report does not match current delivery attempt")
        if attempt == 0 and row["received_at"] is None:
            raise ApiError(409, "receipt_required", "attempt zero presentation requires durable recipient storage")
        if row["state"] == state and row["detail"] == detail:
            return self._snapshot2(row)
        if row["state"] not in ("leased", "queued", "received"):
            raise ApiError(409, "report_conflict", "delivery result already recorded or lease expired")
        # Expiry alone does not prove the injection did not occur; current-attempt results
        # remain useful until a new attempt is issued. Fencing is by attempt, never clock.
        self.conn.execute("UPDATE messages_v2 SET state=?,detail=?,lease_until=NULL,"
                          "injected_at=CASE WHEN ?='injected' THEN COALESCE(injected_at,?) ELSE injected_at END WHERE msg_id=?",
                          (state, detail, state, time.time(), msg_id))
        self._event2(msg_id, "node_report")
        return self._snapshot2(self._row2(msg_id))

    def ack2(self, body):
        msg_id = _req_str(body, "msg_id", maxlen=128)
        session_key = _req_str(body, "session_key")
        return self._write(self._ack2_op, (msg_id, session_key), notify=True)

    def _ack2_op(self, msg_id, session_key):
        row = self._row2(msg_id)
        if row is None:
            raise ApiError(404, "unknown_message", "no retained message")
        if row["to_key"] != session_key:
            raise ApiError(403, "wrong_recipient", "only recipient may acknowledge")
        if row["acked_at"] is not None:
            return self._snapshot2(row)
        now = time.time()
        self.conn.execute("UPDATE messages_v2 SET acked_at=? "
                          "WHERE msg_id=?", (now, msg_id))
        self._event2(msg_id, "recipient_ack", now)
        return self._snapshot2(self._row2(msg_id))

    def status2(self, msg_id, session_key):
        msg_id = _need(msg_id, "msg_id", maxlen=128)
        session_key = _need(session_key, "session_key")
        return self._read(self._status2_op, (msg_id, session_key, False))

    def message2(self, msg_id, session_key):
        msg_id = _need(msg_id, "msg_id", maxlen=128)
        session_key = _need(session_key, "session_key")
        return self._read(self._status2_op, (msg_id, session_key, True))

    def _status2_op(self, msg_id, session_key, full):
        row = self._participant2(msg_id, session_key)
        return self._snapshot2(row, full) if row else self._unknown2(msg_id)

    def inbox2(self, session_key, after=0, limit=50):
        return self._read(self._inbox2_op, (_need(session_key, "session_key"),
                          _integer(after, "after"), _integer(limit, "limit", 1, 200)))

    def _inbox2_op(self, session_key, after, limit):
        rows = self.conn.execute("SELECT * FROM messages_v2 WHERE to_key=? AND message_order>? "
                                 "ORDER BY message_order LIMIT ?", (session_key, after, limit+1)).fetchall()
        items = [self._snapshot2(row, True) for row in rows[:limit]]
        return dict(items=items, next_cursor=items[-1]["message_order"] if items else after,
                    has_more=len(rows)>limit, complete=len(rows)<=limit,
                    ordering_scope="sender_to_receiver")

    def history2(self, msg_id, session_key, after=0, limit=50):
        return self._read(self._history2_op, (_need(msg_id, "msg_id", maxlen=128),
                          _need(session_key, "session_key"), _integer(after, "after"),
                          _integer(limit, "limit", 1, 200)))

    def _history2_op(self, msg_id, session_key, after, limit):
        row = self._participant2(msg_id, session_key)
        if row is None:
            return dict(self._unknown2(msg_id), events=[], next_cursor=after, has_more=False,
                        complete=False, history_pruned=False)
        rows = self.conn.execute("SELECT * FROM events_v2 WHERE msg_id=? AND event_id>? "
                                 "ORDER BY event_id LIMIT ?", (msg_id, after, limit+1)).fetchall()
        events = [dict(r) for r in rows[:limit]]
        for event in events:
            event["time"] = _iso(event["time"])
        return dict(msg_id=msg_id, events=events, next_cursor=events[-1]["event_id"] if events else after,
                    has_more=len(rows)>limit, complete=len(rows)<=limit and not bool(row["history_pruned"]),
                    history_pruned=bool(row["history_pruned"]))


class HubServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, addr, hub, token, log=None):
        self.hub = hub
        self.log = log if log is not None else HubLog()
        self.token_bytes = token.encode("utf-8")
        self._next_gc = 0
        ThreadingHTTPServer.__init__(self, addr, Handler)

    def server_close(self):
        try:
            super().server_close()
        finally:
            self.log.close()

    def service_actions(self):
        # Runs in the HTTP serve loop even with no requests; workers never run GC
        # outside the same SQLite transaction discipline as message transitions.
        now = time.monotonic()
        if now >= self._next_gc:
            self._next_gc = now + 30
            try:
                self.hub.sweep2()
            except Exception as exc:
                self.log.write(dict(event="gc_error", error=type(exc).__name__))


_ROUTES = {
    ("POST", "/v1/register"),
    ("POST", "/v1/heartbeat"),
    ("GET", "/v1/peers"),
    ("POST", "/v1/send"),
    ("GET", "/v1/pull"),
    ("POST", "/v1/report"),
    ("GET", "/v1/status"),
    ("POST", "/v2/send"),
    ("GET", "/v2/pull"),
    ("POST", "/v2/report"),
    ("POST", "/v2/receive"),
    ("POST", "/v2/ack"),
    ("GET", "/v2/status"),
    ("GET", "/v2/message"),
    ("GET", "/v2/inbox"),
    ("GET", "/v2/history"),
}
_PATHS = set(p for _, p in _ROUTES) | {"/v1/health"}


def _qs(query, name):
    values = query.get(name)
    return values[0] if values else None


class Handler(BaseHTTPRequestHandler):
    server_version = "agentbus-hub/" + VERSION
    sys_version = ""

    def log_request(self, code="-", size="-"):
        # Approved correlation fields only, never headers, body text or raw query.
        started = getattr(self, "_started", None)
        elapsed_ms = (time.monotonic() - started) * 1000.0 if started else 0.0
        path = (getattr(self, "path", None) or "-").split("?", 1)[0]
        method = getattr(self, "command", None) or "-"
        record = dict(event="http_request", time=_iso(time.time()), method=method,
                      path=path if path in _PATHS else "unknown", status=code,
                      request_id=getattr(self, "_request_id", None),
                      msg_id=getattr(self, "_msg_id", None),
                      client_msg_id=getattr(self, "_client_msg_id", None),
                      elapsed_ms=round(elapsed_ms, 1))
        self.server.log.write(record)

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
        if isinstance(payload, dict) and payload.get("msg_id"):
            self._msg_id = payload["msg_id"]
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        if getattr(self, "_request_id", None):
            self.send_header("X-Request-ID", self._request_id)
        self.end_headers()
        self.wfile.write(data)

    def _dispatch(self, method):
        self._started = time.monotonic()
        self._request_id = uuid.uuid4().hex
        self._msg_id = self._client_msg_id = None
        url = urlsplit(self.path)
        path = url.path
        try:
            if method == "GET" and path == "/v1/health":
                self._send_json(200, {"ok": True, "version": VERSION, "protocols": [1, 2],
                                      "capabilities": ["durable_receipts"]})
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
        except Exception as exc:
            self.server.log.write(dict(event="request_error", request_id=self._request_id,
                                       error=type(exc).__name__))
            self._send_json(500, {"error": "internal", "message": "internal error"})

    def _handle(self, method, path, query_string):
        hub = self.server.hub
        if method == "GET":
            query = parse_qs(query_string, keep_blank_values=True)
            self._msg_id = _qs(query, "msg_id")
            if path == "/v2/pull":
                return {"messages": hub.pull2(_qs(query, "machine"), _parse_wait(_qs(query, "wait")))}
            if path in ("/v2/status", "/v2/message"):
                call = hub.status2 if path == "/v2/status" else hub.message2
                return call(_qs(query, "msg_id"), _qs(query, "session_key"))
            if path == "/v2/inbox":
                return hub.inbox2(_qs(query, "session_key"), _qs(query, "after") or 0,
                                  _qs(query, "limit") or 50)
            if path == "/v2/history":
                return hub.history2(_qs(query, "msg_id"), _qs(query, "session_key"),
                                    _qs(query, "after") or 0, _qs(query, "limit") or 50)
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
        self._msg_id = body.get("msg_id") if isinstance(body.get("msg_id"), str) else None
        self._client_msg_id = body.get("client_msg_id") if isinstance(body.get("client_msg_id"), str) else None
        if path == "/v2/send":
            return hub.send2(body)
        if path == "/v2/report":
            return hub.report2(body)
        if path == "/v2/receive":
            return hub.receive2(body)
        if path == "/v2/ack":
            return hub.ack2(body)
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
                lease_seconds=LEASE_SECONDS, queue_ttl=QUEUE_TTL_SECONDS,
                body_retention_days=7, event_retention_days=30,
                log_file=None, log_max_bytes=LOG_MAX_BYTES, log_backup_count=LOG_BACKUP_COUNT):
    """Build (but do not run) a hub server. server.hub is the Hub; port 0 = ephemeral."""
    if host not in LOOPBACK_HOSTS:
        raise ValueError("hub must bind a loopback address (127.0.0.1), not %r" % host)
    if not token:
        raise ValueError("token must be non-empty")
    log = HubLog(log_file, log_max_bytes, log_backup_count)
    hub = None
    try:
        hub = Hub(db_path, lease_seconds=lease_seconds, queue_ttl=queue_ttl,
                  body_retention_days=body_retention_days, event_retention_days=event_retention_days)
        return HubServer((host, port), hub, token, log)
    except BaseException:
        if hub is not None:
            hub.close()
        log.close()
        raise


def _read_token(token_file):
    with open(token_file, "r", encoding="utf-8") as f:
        token = f.read().strip()
    if not token:
        raise ValueError("token file %s is empty" % token_file)
    return token


def serve(db_path, token_file, host="127.0.0.1", port=DEFAULT_PORT,
          body_retention_days=7, event_retention_days=30,
          log_file=None, log_max_bytes=LOG_MAX_BYTES, log_backup_count=LOG_BACKUP_COUNT):
    token = _read_token(token_file)
    server = make_server(db_path, token, host=host, port=port,
                         body_retention_days=body_retention_days, event_retention_days=event_retention_days,
                         log_file=log_file, log_max_bytes=log_max_bytes, log_backup_count=log_backup_count)
    try:
        if log_file is None:
            sys.stderr.write("agentbus hub %s listening on %s:%d db=%s\n"
                             % (VERSION, host, server.server_address[1], db_path))
            sys.stderr.flush()
        else:
            server.log.write(dict(event="hub_start", version=VERSION, host=host,
                                  port=server.server_address[1]))
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
    parser.add_argument("--body-retention-days", type=_positive_days, default=7,
                        help="retain eligible message bodies for this many days (default: 7)")
    parser.add_argument("--event-retention-days", type=_positive_days, default=30,
                        help="retain eligible message history for this many days (default: 30)")
    parser.add_argument("--log-file", help="private rotating JSON log; default: stderr without built-in rotation")
    parser.add_argument("--log-max-bytes", type=_positive_int, default=LOG_MAX_BYTES,
                        help="positive log rotation size in bytes (default: 5242880)")
    parser.add_argument("--log-backup-count", type=_positive_int, default=LOG_BACKUP_COUNT,
                        help="positive number of retained log archives (default: 3)")
    args = parser.parse_args(argv)
    try:
        serve(args.db, args.token_file, host=args.host, port=args.port,
              body_retention_days=args.body_retention_days, event_retention_days=args.event_retention_days,
              log_file=args.log_file, log_max_bytes=args.log_max_bytes, log_backup_count=args.log_backup_count)
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, sqlite3.Error) as e:
        sys.stderr.write("agentbus hub: %s\n" % e)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

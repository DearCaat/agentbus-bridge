"""Tests for agentbus.node: local API, dispatch, dedup, in_doubt, liveness, env addendum.

Uses an in-test fake hub (http.server), a fake Claude messaging socket (AF_UNIX) and a
fake codex binary (script that records argv and CODEX_HOME). No network beyond 127.0.0.1.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from agentbus.inject import render, same_machine_harness  # noqa: E402
from agentbus.node import Node, Paths, Store, agentbus_home, ensure, load_config, pid_alive  # noqa: E402

HUB_TOKEN = "hub-secret"
CLAUDE_TOKEN = "CLAUDE-MSG-TOKEN"
MACHINE = "testbox"


def wait_for(pred, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


def claude_contents(payloads):
    """The user-message content of each recorded Claude socket payload."""
    out = []
    for p in payloads:
        frames = [json.loads(line) for line in p.decode("utf-8").splitlines() if line.strip()]
        out.extend(f["message"]["content"] for f in frames if f.get("type") == "user")
    return out


class _QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # clients (node) may hang up mid long-poll during teardown


class FakeHub:
    """Minimal hub: register/heartbeat/pull/report/send/peers/status/health, bearer auth."""

    def __init__(self):
        self.cond = threading.Condition()
        self.queue = []
        self.registers = []
        self.heartbeats = []
        self.reports = []
        self.sends = []
        self.sessions = {}  # key -> peer dict
        self.names = {}  # (machine, harness, name) -> key
        hub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, obj):
                data = json.dumps(obj).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _route(self, method):
                url = urllib.parse.urlparse(self.path)
                query = dict(urllib.parse.parse_qsl(url.query))
                if method == "GET" and url.path == "/v1/health":
                    return self._send(200, {"ok": True, "version": "fake-0"})
                if self.headers.get("Authorization") != "Bearer " + HUB_TOKEN:
                    return self._send(401, {"error": "unauthorized"})
                body = {}
                if method == "POST":
                    n = int(self.headers.get("Content-Length") or 0)
                    body = json.loads(self.rfile.read(n).decode("utf-8") or "{}") if n else {}
                status, obj = hub.handle(method, url.path, query, body)
                self._send(status, obj)

            def do_GET(self):
                self._route("GET")

            def do_POST(self):
                self._route("POST")

        self.server = _QuietServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def push(self, msg):
        with self.cond:
            self.queue.append(msg)
            self.cond.notify_all()

    def forget(self, key):
        """Simulate the hub losing one session's registration."""
        with self.cond:
            info = self.sessions.pop(key, None)
            if info:
                self.names.pop((info["machine"], info["harness"], info["name"]), None)

    def forget_all(self):
        """Simulate the hub DB being lost."""
        with self.cond:
            self.sessions.clear()
            self.names.clear()

    def reported(self, msg_id, state):
        return any(r[0] == msg_id and r[1] == state for r in list(self.reports))

    def report_count(self, msg_id):
        return sum(1 for r in list(self.reports) if r[0] == msg_id)

    def handle(self, method, path, query, body):
        if method == "POST" and path == "/v1/register":
            machine, harness, sid = body["machine"], body["harness"], body["session_id"]
            name = body.get("name") or sid[:8]
            key = "%s:%s:%s" % (machine, harness, sid)
            taken = self.names.get((machine, harness, name))
            if taken and taken != key:
                return 409, {"error": "name_taken"}
            self.names[(machine, harness, name)] = key
            address = "%s/%s/%s" % (machine, harness, name)
            self.sessions[key] = {
                "address": address, "session_key": key, "machine": machine,
                "harness": harness, "name": name, "cwd": body.get("cwd"), "online": True,
            }
            self.registers.append(body)
            return 200, {"session_key": key, "address": address, "name": name}
        if method == "POST" and path == "/v1/heartbeat":
            self.heartbeats.append((body["machine"], list(body["live"])))
            return 200, {"ok": True}
        if method == "GET" and path == "/v1/pull":
            deadline = time.monotonic() + float(query.get("wait", 0))
            with self.cond:
                while not self.queue:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self.cond.wait(remaining)
                msgs, self.queue = self.queue, []
            return 200, {"messages": msgs}
        if method == "POST" and path == "/v1/report":
            self.reports.append((body["msg_id"], body["state"], body.get("detail")))
            return 200, {"ok": True}
        if method == "POST" and path == "/v1/send":
            if body["from_key"] not in self.sessions:
                return 404, {"error": "unknown_sender"}
            self.sends.append(body)
            target = None
            for s in self.sessions.values():
                if body["to"] in (s["address"], s["session_key"]):
                    target = s
            if target is None:
                return 404, {"error": "unknown_target"}
            return 200, {"msg_id": uuid.uuid4().hex, "target": target["address"], "target_online": True}
        if method == "GET" and path == "/v1/peers":
            return 200, {"peers": list(self.sessions.values())}
        if method == "GET" and path == "/v1/status":
            return 200, {"msg_id": query.get("msg_id"), "state": "delivered"}
        return 404, {"error": "not_found"}


class FakeClaudeSocket:
    """AF_UNIX listener recording every connection's full byte payload (read to EOF)."""

    def __init__(self, path):
        self.path = path
        self.received = []
        self._lock = threading.Lock()
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv.bind(path)
        self._srv.listen(8)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                return
            threading.Thread(target=self._read, args=(conn,), daemon=True).start()

    def _read(self, conn):
        buf = b""
        with conn:
            conn.settimeout(10)
            while True:
                try:
                    chunk = conn.recv(65536)
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
        with self._lock:
            self.received.append(buf)

    def payloads(self):
        with self._lock:
            return list(self.received)

    def close(self):
        self._srv.close()


def write_fake_codex(dirpath, out_path):
    path = os.path.join(dirpath, "fake-codex")
    src = (
        "#!" + sys.executable + "\n"
        "import json, os, sys\n"
        "with open(" + repr(out_path) + ", 'a') as f:\n"
        "    f.write(json.dumps({'argv': sys.argv[1:], 'codex_home': os.environ.get('CODEX_HOME')}) + '\\n')\n"
        "if 'th-bad' in sys.argv:\n"
        "    sys.stderr.write('boom: no such thread\\n')\n"
        "    sys.exit(3)\n"
    )
    with open(path, "w") as f:
        f.write(src)
    os.chmod(path, 0o755)
    return path


class NodeTestCase(unittest.TestCase):
    def setUp(self):
        self._env = mock.patch.dict(os.environ)
        self._env.start()
        self.addCleanup(self._env.stop)
        for k in ("AGENTBUS_HOME", "AGENTBUS_CONFIG", "AGENTBUS_MACHINE"):
            os.environ.pop(k, None)

        self.tmp = tempfile.mkdtemp(prefix="ab-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        with open(os.path.join(self.home, "token"), "w") as f:
            f.write(HUB_TOKEN + "\n")

        self.hub = FakeHub()
        self.addCleanup(self.hub.close)
        self.claude = FakeClaudeSocket(os.path.join(self.tmp, "claude.sock"))
        self.addCleanup(self.claude.close)
        self.codex_out = os.path.join(self.tmp, "codex.jsonl")
        self.codex_bin = write_fake_codex(self.tmp, self.codex_out)
        self.codex_home = os.path.join(self.tmp, "codex-home")
        self.write_config(os.path.join(self.home, "config.json"))

    def write_config(self, path, **extra):
        cfg = {"machine": MACHINE, "hub_url": self.hub.url, "codex_bin": self.codex_bin}
        cfg.update(extra)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(cfg, f)

    def start_node(self, home=None):
        node = Node(home or self.home, poll_wait=1, heartbeat_interval=3600)
        node.start()
        self.addCleanup(node.stop)
        return node

    def local(self, req, home=None, timeout=20):
        sock_path = os.path.join(home or self.home, "node.sock")
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect(sock_path)
            s.sendall(json.dumps(req).encode("utf-8") + b"\n")
            buf = b""
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
        finally:
            s.close()
        return json.loads(buf.decode("utf-8"))

    def register_claude(self, sid="claude-sess-000001", pid=None, name=None):
        req = {
            "op": "register", "harness": "claude", "session_id": sid,
            "mcp_pid": pid or os.getpid(),
            "deliver": {"socket": self.claude.path, "token": CLAUDE_TOKEN},
        }
        if name:
            req["name"] = name
        return self.local(req)

    def register_codex(self, thread_id, sid="codex-sess-0001"):
        return self.local({
            "op": "register", "harness": "codex", "session_id": sid,
            "mcp_pid": os.getpid(),
            "deliver": {"thread_id": thread_id, "codex_home": self.codex_home},
        })

    def claude_message(self, msg_id, to_key, text="hello", from_address="wsl/codex/abcd1234"):
        return {"msg_id": msg_id, "from_address": from_address, "to_key": to_key,
                "text": text, "created_at": "2026-11-10T00:00:00Z"}


class TestRegisterAndSend(NodeTestCase):
    def test_register_keeps_claude_token_local(self):
        node = self.start_node()
        r = self.register_claude()
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["session_key"], MACHINE + ":claude:claude-sess-000001")
        self.assertEqual(r["address"], MACHINE + "/claude/claude-s")

        body = self.hub.registers[0]
        self.assertEqual(set(body), {"machine", "harness", "session_id"})
        self.assertNotIn(CLAUDE_TOKEN, json.dumps(body))
        self.assertNotIn(CLAUDE_TOKEN, json.dumps(self.hub.registers))
        sess = node.store.get_session(r["session_key"])
        self.assertEqual(sess["deliver"], {"socket": self.claude.path, "token": CLAUDE_TOKEN})

        dup = self.local({"op": "register", "harness": "claude", "session_id": "other-sess-0002",
                          "name": "claude-s", "deliver": {}})
        self.assertFalse(dup["ok"])
        self.assertEqual(dup["error"], "name_taken")

    def test_send_is_proxied_to_hub(self):
        self.start_node()
        me = self.register_claude()
        self.register_codex("th-1")
        r = self.local({"op": "send", "session_key": me["session_key"],
                        "to": MACHINE + "/codex/codex-se", "text": "hi there"})
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["target"], MACHINE + "/codex/codex-se")
        self.assertTrue(r["target_online"])
        self.assertTrue(r["msg_id"])
        sent = self.hub.sends[-1]
        self.assertEqual(sent["from_key"], me["session_key"])
        self.assertEqual(sent["text"], "hi there")
        self.assertTrue(sent["client_msg_id"])

        bad = self.local({"op": "send", "session_key": me["session_key"],
                          "to": MACHINE + "/codex/nope", "text": "x"})
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["error"], "unknown_target")


class TestInjection(NodeTestCase):
    def test_pull_injects_claude_frame_exactly(self):
        self.start_node()
        r = self.register_claude()
        self.hub.push(self.claude_message("m-frame-1", r["session_key"]))
        self.assertTrue(wait_for(lambda: self.claude.payloads()))
        expected = (
            '{"type":"auth","token":"CLAUDE-MSG-TOKEN"}\n'
            '{"type":"user","message":{"role":"user","content":"[agentbus] message m-frame-1 '
            'from wsl/codex/abcd1234:\\nhello\\n\\n(Reply with the agentbus send tool, '
            'to=\\"wsl/codex/abcd1234\\".)"}}\n'
        ).encode("utf-8")
        self.assertEqual(self.claude.payloads(), [expected])
        self.assertTrue(wait_for(lambda: self.hub.reported("m-frame-1", "delivered")))

    def test_pull_injects_codex_argv(self):
        self.start_node()
        r = self.register_codex("th-1")
        self.hub.push(self.claude_message("m-codex-1", r["session_key"], text="ping",
                                          from_address="wsl/claude/abcdef12"))
        self.assertTrue(wait_for(lambda: self.hub.reported("m-codex-1", "delivered")))
        with open(self.codex_out) as f:
            recs = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["argv"], [
            "queue", "--thread", "th-1", "--message",
            render("m-codex-1", "wsl/claude/abcdef12", "ping"),
        ])
        self.assertEqual(recs[0]["codex_home"], self.codex_home)

    def test_codex_failure_reported_failed(self):
        self.start_node()
        r = self.register_codex("th-bad")
        self.hub.push(self.claude_message("m-codex-bad", r["session_key"]))
        self.assertTrue(wait_for(lambda: self.hub.reported("m-codex-bad", "failed")))
        detail = [x[2] for x in self.hub.reports if x[0] == "m-codex-bad"][0]
        self.assertTrue(detail.startswith("in_doubt:"))
        self.assertIn("exit 3", detail)
        self.assertNotIn("boom", detail)

    def test_duplicate_msg_id_injected_once(self):
        self.start_node()
        r = self.register_claude()
        self.hub.push(self.claude_message("m-dup-1", r["session_key"]))
        self.assertTrue(wait_for(lambda: self.hub.reported("m-dup-1", "delivered")))
        # Same msg_id returned again (e.g. lease expiry): must not inject again.
        self.hub.push(self.claude_message("m-dup-1", r["session_key"]))
        self.assertTrue(wait_for(lambda: self.hub.report_count("m-dup-1") >= 2))
        time.sleep(0.2)
        self.assertEqual(len(self.claude.payloads()), 1)

    def test_reply_tail_depends_on_sender_machine_and_harness(self):
        self.start_node()
        r = self.register_claude()
        cases = [
            ("m-tail-same", MACHINE + "/claude/abcdef12", "claude"),  # same machine, same harness
            ("m-tail-harness", MACHINE + "/codex/abcdef12", None),    # same machine, other harness
            ("m-tail-machine", "wsl/claude/abcdef12", None),          # other machine, same harness
        ]
        for msg_id, frm, _ in cases:
            self.hub.push(self.claude_message(msg_id, r["session_key"], text="hello", from_address=frm))
        self.assertTrue(wait_for(lambda: len(self.claude.payloads()) == len(cases)))
        got = sorted(claude_contents(self.claude.payloads()))
        want = sorted(render(msg_id, frm, "hello", local_harness=local) for msg_id, frm, local in cases)
        self.assertEqual(got, want)
        self.assertEqual(sum("Reply with the agentbus send tool, to=" in c for c in got), 2)
        self.assertEqual(sum("SendMessage" in c and "ListAgents" in c for c in got), 1)

    def test_same_machine_codex_reply_tail_names_own_messaging(self):
        self.start_node()
        r = self.register_codex("th-1")
        self.hub.push(self.claude_message("m-codex-local", r["session_key"], text="ping",
                                          from_address=MACHINE + "/codex/abcdef12"))
        self.assertTrue(wait_for(lambda: self.hub.reported("m-codex-local", "delivered")))
        with open(self.codex_out) as f:
            recs = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(recs[0]["argv"][-1],
                         render("m-codex-local", MACHINE + "/codex/abcdef12", "ping", local_harness="codex"))
        self.assertNotIn("Reply with the agentbus send tool", recs[0]["argv"][-1])


class TestReplyTail(unittest.TestCase):
    def test_default_tail_is_unchanged(self):
        self.assertEqual(
            render("m1", "wsl/codex/abcd1234", "hi"),
            '[agentbus] message m1 from wsl/codex/abcd1234:\nhi\n\n'
            '(Reply with the agentbus send tool, to="wsl/codex/abcd1234".)')

    def test_same_machine_same_harness_tails(self):
        claude = render("m2", "testbox/claude/bob", "hi", local_harness="claude")
        self.assertIn("(Same machine and harness: reply with SendMessage; find the sender in ListAgents.", claude)
        self.assertNotIn("Reply with the agentbus send tool", claude)
        other = render("m3", "testbox/codex/bob", "hi", local_harness="codex")
        self.assertIn("reply with this harness's own messaging", other)
        self.assertNotIn("Reply with the agentbus send tool", other)

    def test_same_machine_harness_match(self):
        self.assertTrue(same_machine_harness("testbox/claude/bob", "testbox", "claude"))
        self.assertFalse(same_machine_harness("testbox/codex/bob", "testbox", "claude"))
        self.assertFalse(same_machine_harness("wsl/claude/bob", "testbox", "claude"))
        self.assertFalse(same_machine_harness("testbox/claude", "testbox", "claude"))
        self.assertFalse(same_machine_harness("", "testbox", "claude"))


class TestHubStateLoss(NodeTestCase):
    def test_send_recovers_from_unknown_sender(self):
        self.start_node()
        me = self.register_claude()
        target = self.register_codex("th-1")
        self.hub.forget(me["session_key"])
        r = self.local({"op": "send", "session_key": me["session_key"],
                        "to": target["address"], "text": "after loss"})
        self.assertTrue(r["ok"], r)
        self.assertIn(me["session_key"], self.hub.sessions)
        self.assertEqual(self.hub.sends[-1]["text"], "after loss")

    def test_heartbeat_reregisters_live_sessions_after_hub_state_loss(self):
        node = self.start_node()
        a = self.register_claude()
        b = self.register_codex("th-1")
        n_before = len(self.hub.registers)
        self.hub.forget_all()
        live = node.heartbeat_once()
        self.assertEqual(set(live), {a["session_key"], b["session_key"]})
        self.assertIn(a["session_key"], self.hub.sessions)
        self.assertIn(b["session_key"], self.hub.sessions)
        self.assertEqual(len(self.hub.registers), n_before + 2)
        self.assertEqual(set(self.hub.heartbeats[-1][1]), {a["session_key"], b["session_key"]})
        self.assertNotIn(CLAUDE_TOKEN, json.dumps(self.hub.registers))


class TestRealHubRestart(NodeTestCase):
    """Node against the real hub.py; the hub is restarted with a fresh DB."""

    def start_real_hub(self, port=0):
        import agentbus.hub as hubmod

        db = os.path.join(self.tmp, "hub.db")
        if os.path.exists(db):
            os.unlink(db)
        server = hubmod.make_server(db, HUB_TOKEN, port=port, lease_seconds=30)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server

    @staticmethod
    def stop_real_hub(server):
        server.shutdown()
        server.server_close()
        time.sleep(1.2)  # let any in-flight long-poll (node wait=1) finish before closing the DB
        server.hub.close()

    def test_hub_restart_with_fresh_db_then_send_is_delivered(self):
        from agentbus.hubclient import HubClient

        server = self.start_real_hub()
        port = server.server_address[1]
        holder = {"server": server}
        self.addCleanup(lambda: self.stop_real_hub(holder["server"]))
        self.write_config(os.path.join(self.home, "config.json"),
                          hub_url="http://127.0.0.1:%d" % port)
        node = self.start_node()

        target = self.register_claude(sid="target-sess-01")
        sender = self.register_codex("th-1", sid="sender-sess-01")
        self.assertTrue(target["ok"] and sender["ok"], (target, sender))

        # Hub loses its DB and comes back on the same port.
        self.stop_real_hub(holder["server"])
        holder["server"] = self.start_real_hub(port=port)

        node.heartbeat_once()  # re-registers both live sessions, then heartbeats
        r = self.local({"op": "send", "session_key": sender["session_key"],
                        "to": target["address"], "text": "after restart"})
        self.assertTrue(r["ok"], r)

        self.assertTrue(wait_for(lambda: self.claude.payloads(), timeout=20))
        self.assertIn(b"after restart", self.claude.payloads()[0])
        client = HubClient("http://127.0.0.1:%d" % port, HUB_TOKEN)
        self.assertTrue(wait_for(lambda: client.status(r["msg_id"])["state"] == "delivered", timeout=20))


class TestRestartAndLiveness(NodeTestCase):
    def test_in_doubt_reported_on_restart(self):
        node = self.start_node()
        r = self.register_claude()
        node.stop()
        # Simulate a crash after the at-most-once record was committed but before any result.
        store = Store(os.path.join(self.home, "node.db"))
        store.record_pending("m-crash-1", r["session_key"])
        store.close()

        self.start_node()
        self.assertTrue(wait_for(lambda: self.hub.reported("m-crash-1", "in_doubt")))
        # Redelivery after restart must not inject; the recorded state is re-reported.
        self.hub.push(self.claude_message("m-crash-1", r["session_key"]))
        self.assertTrue(wait_for(lambda: self.hub.report_count("m-crash-1") >= 2))
        time.sleep(0.2)
        self.assertEqual(self.claude.payloads(), [])

    def test_dead_mcp_pid_leaves_live_list(self):
        node = self.start_node()
        dead = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        ra = self.register_claude(sid="dead-sess-0001", pid=dead.pid)
        rb = self.register_claude(sid="live-sess-0002", pid=os.getpid())
        live1 = node.heartbeat_once()
        self.assertIn(ra["session_key"], live1)
        self.assertIn(rb["session_key"], live1)

        dead.kill()
        dead.wait()
        live2 = node.heartbeat_once()
        self.assertNotIn(ra["session_key"], live2)
        self.assertIn(rb["session_key"], live2)
        self.assertEqual(self.hub.heartbeats[-1], (MACHINE, live2))


class TestRuntimePathAddendum(NodeTestCase):
    def test_agentbus_home_env_override(self):
        with mock.patch.dict(os.environ, {"AGENTBUS_HOME": self.home}):
            self.assertEqual(agentbus_home(), self.home)
            self.assertEqual(Node().p.home, self.home)
            self.assertEqual(Paths(self.home).config, os.path.join(self.home, "config.json"))

    def test_agentbus_home_default_is_dot_agentbus_under_home(self):
        with mock.patch.dict(os.environ, {"HOME": self.tmp}):
            os.environ.pop("AGENTBUS_HOME", None)
            self.assertEqual(agentbus_home(), os.path.join(self.tmp, ".agentbus"))

    def test_agentbus_config_env_and_token_file(self):
        cfg_dir = os.path.join(self.tmp, "shared-cfg")
        os.makedirs(cfg_dir)
        with open(os.path.join(cfg_dir, "hub.token"), "w") as f:
            f.write(HUB_TOKEN)
        cfg_path = os.path.join(cfg_dir, "agentbus.json")
        # Relative token_file resolves against the config file's directory.
        self.write_config(cfg_path, token_file="hub.token")
        # Runtime home has no config.json and no token: both must come from the env/config.
        home2 = os.path.join(self.tmp, "home2")
        os.environ["AGENTBUS_CONFIG"] = cfg_path

        node = self.start_node(home=home2)
        self.assertEqual(node.p.config, cfg_path)
        r = self.register_claude_at(home2)
        self.assertTrue(r["ok"], r)  # would be 401 without the token from token_file

    def register_claude_at(self, home):
        return self.local({
            "op": "register", "harness": "claude", "session_id": "cfg-sess-0001",
            "deliver": {"socket": self.claude.path, "token": CLAUDE_TOKEN},
        }, home=home)

    def test_agentbus_machine_env_overrides_config(self):
        os.environ["AGENTBUS_MACHINE"] = "envbox"
        self.start_node()
        r = self.register_claude()
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["session_key"], "envbox:claude:claude-sess-000001")
        self.assertEqual(self.hub.registers[-1]["machine"], "envbox")

    def test_tunnel_argv_expanded(self):
        os.environ["AB_TEST_HOST"] = "example.test"
        self.write_config(os.path.join(self.home, "config.json"),
                          tunnel=["ssh", "-N", "-L", "1:127.0.0.1:2", "${AB_TEST_HOST}"],
                          extra_tunnels=[["ssh", "~/x"]])
        cfg = load_config(Paths(self.home))
        self.assertEqual(cfg["tunnels"][0], ["ssh", "-N", "-L", "1:127.0.0.1:2", "example.test"])
        self.assertEqual(cfg["tunnels"][1], ["ssh", os.path.expanduser("~/x")])


class TestEnsure(NodeTestCase):
    def test_ensure_spawns_detached_daemon_once(self):
        argv = [sys.executable, "-c",
                "import sys; sys.path.insert(0, %r); from agentbus.node import main; "
                "sys.exit(main(['--foreground']))" % REPO]
        self.assertEqual(ensure(home=self.home, spawn_argv=argv), "spawned")

        def status():
            try:
                return self.local({"op": "status"}, timeout=2)
            except (OSError, ValueError):
                return None

        self.assertTrue(wait_for(lambda: status() is not None, timeout=20))
        pid = status()["pid"]
        self.addCleanup(self._kill_daemon, pid)
        self.assertEqual(ensure(home=self.home, spawn_argv=argv), "running")
        self.assertEqual(status()["pid"], pid)

    def _kill_daemon(self, pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            return
        wait_for(lambda: not pid_alive(pid), timeout=10)


if __name__ == "__main__":
    unittest.main()

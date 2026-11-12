"""Tests for agentbus/mcp_server.py.

The MCP runs as a real subprocess over stdio. The node is a fake AF_UNIX server
under a temporary AGENTBUS_HOME. AGENTBUS_NO_ENSURE=1 is set for every subprocess test.
Run from the repo root: python3 -m unittest tests.test_mcp -v
"""

import json
import os
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from agentbus import mcp_server  # noqa: E402

CLAUDE_SID = "11111111-2222-3333-4444-555555555555"
CODEX_TID = "22222222-3333-4444-8555-666666666666"
CLAUDE_TOKEN = "tok-test-SECRET-value-123"

STRIP_ENV = ("CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN",
             "CODEX_HOME", "AGENTBUS_HOME", "AGENTBUS_NO_ENSURE")


def default_node_handler(req):
    op = req.get("op")
    if op == "register":
        sid = req.get("session_id") or ""
        name = req.get("name") or sid[:8]
        addr = "testbox/%s/%s" % (req.get("harness"), name)
        return {"ok": True, "session_key": "testbox:%s:%s" % (req.get("harness"), sid), "address": addr}
    if op == "peers":
        return {"ok": True, "peers": [
            {"address": "testbox/codex/22222222", "session_key": "k1", "machine": "testbox",
             "harness": "codex", "name": "22222222", "cwd": "/w/a", "online": True,
             "last_seen": "2026-11-10T16:00:00Z"},
            {"address": "testbox/claude/bob", "session_key": "k2", "machine": "testbox",
             "harness": "claude", "name": "bob", "cwd": "/w/b", "online": False,
             "last_seen": "2026-11-09T10:00:00Z"},
        ]}
    if op == "send":
        if req.get("to") == "nobody/x/y":
            return {"ok": False, "error": "unknown_target", "detail": "no such address"}
        return {"ok": True, "msg_id": "msg-0001", "target": req.get("to"), "target_online": True}
    if op == "status":
        return {"ok": True, "msg_id": req.get("msg_id"), "state": "delivered", "detail": None,
                "target": "testbox/codex/22222222"}
    return {"ok": False, "error": "unknown_op"}


class FakeNode:
    def __init__(self, home):
        self.path = os.path.join(home, "node.sock")
        self.requests = []
        self.handler = default_node_handler
        self._lock = threading.Lock()
        self._stop = False
        self._sock = None

    def start(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(self.path)
        s.listen(8)
        s.settimeout(0.2)
        self._sock = s
        threading.Thread(target=self._serve, daemon=True).start()

    def stop(self):
        self._stop = True
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        if os.path.exists(self.path):
            os.unlink(self.path)

    def ops(self, op=None):
        with self._lock:
            return [r for r in self.requests if op is None or r.get("op") == op]

    def _serve(self):
        while not self._stop:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._one, args=(conn,), daemon=True).start()

    def _one(self, conn):
        with conn:
            conn.settimeout(5)
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                buf += chunk
            req = json.loads(buf.split(b"\n", 1)[0].decode("utf-8"))
            with self._lock:
                self.requests.append(req)
            resp = self.handler(req)
            conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))


class MCP:
    """Drives the MCP server subprocess over pipes."""

    def __init__(self, home, extra_env=None, unset=()):
        env = {k: v for k, v in os.environ.items() if k not in STRIP_ENV and k not in unset}
        env.update({"AGENTBUS_HOME": home, "AGENTBUS_NO_ENSURE": "1", "PYTHONPATH": REPO_ROOT})
        env.update(extra_env or {})
        self.stderr = tempfile.TemporaryFile()
        self.p = subprocess.Popen([sys.executable, "-m", "agentbus.mcp_server"], cwd=REPO_ROOT, env=env,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr)
        self.q = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()
        self._id = 0

    def _read(self):
        for line in self.p.stdout:
            self.q.put(line.decode("utf-8"))
        self.q.put(None)

    def send(self, obj):
        self.p.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
        self.p.stdin.flush()

    def send_raw(self, text):
        self.p.stdin.write(text.encode("utf-8") + b"\n")
        self.p.stdin.flush()

    def recv(self, timeout=20):
        line = self.q.get(timeout=timeout)
        if line is None:
            raise AssertionError("mcp closed stdout")
        return json.loads(line)

    def rpc(self, method, params=None):
        self._id += 1
        self.send({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}})
        msg = self.recv()
        return msg

    def tool(self, name, arguments=None, meta=None):
        params = {"name": name, "arguments": arguments or {}}
        if meta is not None:
            params["_meta"] = meta
        msg = self.rpc("tools/call", params)
        if "error" in msg:
            raise AssertionError("protocol error: %r" % (msg,))
        result = msg["result"]
        return result["isError"], result["content"][0]["text"]

    def stderr_text(self):
        self.stderr.seek(0)
        return self.stderr.read().decode("utf-8", "replace")

    def close(self):
        try:
            self.p.stdin.close()
        except OSError:
            pass
        try:
            self.p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
        self.p.stdout.close()
        self.stderr.close()


class McpTestBase(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="ab-", dir="/tmp")  # short path: unix socket limit
        self.node = FakeNode(self.home)
        self.mcp = None

    def tearDown(self):
        if self.mcp is not None:
            self.mcp.close()
        self.node.stop()
        shutil.rmtree(self.home, ignore_errors=True)

    def start_mcp(self, **kw):
        self.mcp = MCP(self.home, **kw)
        return self.mcp

    def start_node(self):
        self.node.start()


class TestProtocol(McpTestBase):
    def test_initialize_echoes_client_protocol_version(self):
        self.start_node()
        m = self.start_mcp(extra_env={"CLAUDE_CODE_SESSION_ID": CLAUDE_SID})
        msg = m.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                    "clientInfo": {"name": "t", "version": "0"}})
        res = msg["result"]
        self.assertEqual(res["protocolVersion"], "2025-06-18")
        self.assertEqual(res["serverInfo"], {"name": "agentbus", "version": "0.1.0"})
        self.assertIn("tools", res["capabilities"])

    def test_tools_list_has_five_tools_with_schemas(self):
        self.start_node()
        m = self.start_mcp(extra_env={"CLAUDE_CODE_SESSION_ID": CLAUDE_SID})
        tools = {t["name"]: t for t in m.rpc("tools/list")["result"]["tools"]}
        self.assertEqual(sorted(tools), ["agentbus_peers", "agentbus_rename", "agentbus_send",
                                         "agentbus_status", "agentbus_whoami"])
        for t in tools.values():
            self.assertEqual(t["inputSchema"]["type"], "object")
            self.assertTrue(t["description"])
        self.assertEqual(tools["agentbus_send"]["inputSchema"]["required"], ["to", "text"])
        for name in ("agentbus_send", "agentbus_peers"):
            self.assertIn("SendMessage", tools[name]["description"])  # same-harness pointer to the harness

    def test_ping_and_notifications_and_unknown_method(self):
        self.start_node()
        m = self.start_mcp(extra_env={"CLAUDE_CODE_SESSION_ID": CLAUDE_SID})
        m.send({"jsonrpc": "2.0", "method": "notifications/initialized"})  # no reply expected
        self.assertEqual(m.rpc("ping")["result"], {})
        msg = m.rpc("bogus/method")
        self.assertEqual(msg["error"]["code"], -32601)

    def test_parse_error_and_unknown_tool(self):
        self.start_node()
        m = self.start_mcp(extra_env={"CLAUDE_CODE_SESSION_ID": CLAUDE_SID})
        m.send_raw("{not json")
        self.assertEqual(m.recv()["error"]["code"], -32700)
        is_err, text = m.tool("agentbus_nope")
        self.assertTrue(is_err)
        self.assertIn("unknown tool", text)


class TestClaudeFlow(McpTestBase):
    def claude_env(self):
        return {"CLAUDE_CODE_SESSION_ID": CLAUDE_SID,
                "CLAUDE_CODE_MESSAGING_SOCKET": "/tmp/claude-msg.sock",
                "CLAUDE_CODE_MESSAGING_TOKEN": CLAUDE_TOKEN}

    def test_register_at_start_with_claude_deliver_info(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())
        m.rpc("ping")
        regs = self.node.ops("register")
        self.assertEqual(len(regs), 1)
        r = regs[0]
        self.assertEqual(r["harness"], "claude")
        self.assertEqual(r["session_id"], CLAUDE_SID)
        self.assertEqual(r["mcp_pid"], m.p.pid)
        self.assertEqual(r["deliver"]["socket"], "/tmp/claude-msg.sock")
        self.assertEqual(r["deliver"]["token"], CLAUDE_TOKEN)  # node only

    def test_whoami_peers_send_status(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())

        err, text = m.tool("agentbus_whoami")
        self.assertFalse(err)
        who = json.loads(text)
        self.assertTrue(who["registered"])
        self.assertEqual(who["address"], "testbox/claude/" + CLAUDE_SID[:8])
        self.assertEqual(who["session_key"], "testbox:claude:" + CLAUDE_SID)
        self.assertEqual(who["node"], "ok")
        self.assertNotIn(CLAUDE_TOKEN, text)

        err, text = m.tool("agentbus_peers")
        self.assertFalse(err)
        self.assertIn("testbox/codex/22222222", text)
        self.assertIn("address", text.splitlines()[0])
        self.assertIn("no", [c for c in text.splitlines()[2].split()])

        err, text = m.tool("agentbus_send", {"to": "testbox/codex/22222222", "text": "hi there"})
        self.assertFalse(err)
        sent = json.loads(text)
        self.assertEqual(sent["msg_id"], "msg-0001")
        self.assertTrue(sent["target_online"])
        s = self.node.ops("send")[0]
        self.assertEqual(s["session_key"], "testbox:claude:" + CLAUDE_SID)
        self.assertEqual(s["to"], "testbox/codex/22222222")
        self.assertEqual(s["text"], "hi there")
        uuid_text = s["client_msg_id"]
        self.assertEqual(len(uuid_text), 36)
        self.assertEqual(uuid_text.count("-"), 4)

        err, text = m.tool("agentbus_status", {"msg_id": "msg-0001"})
        self.assertFalse(err)
        st = json.loads(text)
        self.assertEqual(st["state"], "delivered")
        self.assertEqual(st["msg_id"], "msg-0001")

    def test_same_machine_same_harness_send_is_refused_without_calling_node_send(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())
        err, text = m.tool("agentbus_send", {"to": "testbox/claude/bob", "text": "hi"})
        self.assertTrue(err)
        self.assertIn("same_machine_same_harness", text)
        self.assertIn("SendMessage", text)
        self.assertEqual(self.node.ops("send"), [])

    def test_send_by_session_key_is_refused_only_when_peer_list_says_same_machine_harness(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())
        err, text = m.tool("agentbus_send", {"to": "k2", "text": "hi"})  # peer k2: testbox/claude
        self.assertTrue(err)
        self.assertIn("same_machine_same_harness", text)
        err, text = m.tool("agentbus_send", {"to": "k1", "text": "hi"})  # peer k1: testbox/codex
        self.assertFalse(err)
        err, text = m.tool("agentbus_send", {"to": "not-in-peers", "text": "hi"})  # undetermined: sent as usual
        self.assertFalse(err)
        self.assertEqual([s["to"] for s in self.node.ops("send")], ["k1", "not-in-peers"])

    def test_other_machine_same_harness_and_same_machine_other_harness_are_sent(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())
        for to in ("wsl/claude/bob", "testbox/codex/22222222"):
            err, text = m.tool("agentbus_send", {"to": to, "text": "hi"})
            self.assertFalse(err, to)
        self.assertEqual([s["to"] for s in self.node.ops("send")], ["wsl/claude/bob", "testbox/codex/22222222"])

    def test_rename_sends_register_with_name_and_updates_address(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.claude_env())
        err, text = m.tool("agentbus_rename", {"name": "reviewer-1"})
        self.assertFalse(err)
        self.assertEqual(json.loads(text)["address"], "testbox/claude/reviewer-1")
        self.assertEqual(self.node.ops("register")[-1]["name"], "reviewer-1")
        err, text = m.tool("agentbus_whoami")
        self.assertEqual(json.loads(text)["address"], "testbox/claude/reviewer-1")

    def test_lazy_registration_after_node_comes_up_late(self):
        m = self.start_mcp(extra_env=self.claude_env())  # node not running yet
        m.rpc("ping")
        err, text = m.tool("agentbus_whoami")  # node unreachable: still answers
        self.assertFalse(err)
        who = json.loads(text)
        self.assertFalse(who["registered"])
        self.assertIn("node_unreachable", who["last_error"])
        self.assertIn("node_unreachable", who["node"])

        self.start_node()
        err, text = m.tool("agentbus_whoami")
        who = json.loads(text)
        self.assertTrue(who["registered"])
        self.assertEqual(len(self.node.ops("register")), 1)


class TestCodexFlow(McpTestBase):
    def codex_env(self):
        return {"CODEX_HOME": "/tmp/codex-test-home"}

    def test_no_register_before_thread_id_then_register_from_tools_call_meta(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.codex_env())
        time.sleep(1.0)
        self.assertEqual(self.node.ops("register"), [])  # no thread id yet, test parent has no rollout fd

        meta = {"threadId": CODEX_TID, "sessionId": CODEX_TID,
                "x-codex-turn-metadata": {"thread_id": CODEX_TID}}
        err, text = m.tool("agentbus_whoami", meta=meta)
        self.assertFalse(err)
        who = json.loads(text)
        self.assertTrue(who["registered"])
        regs = self.node.ops("register")
        self.assertEqual(len(regs), 1)
        r = regs[0]
        self.assertEqual(r["harness"], "codex")
        self.assertEqual(r["session_id"], CODEX_TID)
        self.assertEqual(r["deliver"], {"thread_id": CODEX_TID, "codex_home": "/tmp/codex-test-home"})
        self.assertEqual(r["mcp_pid"], m.p.pid)
        self.assertEqual(who["address"], "testbox/codex/" + CODEX_TID[:8])

    def test_thread_id_from_turn_metadata_when_threadId_missing(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.codex_env())
        m.tool("agentbus_whoami", meta={"x-codex-turn-metadata": {"thread_id": CODEX_TID}})
        self.assertEqual(self.node.ops("register")[0]["session_id"], CODEX_TID)

    def test_send_before_any_thread_id_reports_not_registered(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.codex_env())
        err, text = m.tool("agentbus_send", {"to": "testbox/claude/bob", "text": "x"})
        self.assertTrue(err)
        self.assertIn("not_registered", text)
        self.assertEqual(self.node.ops("send"), [])

    def test_codex_same_machine_same_harness_send_is_refused(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.codex_env())
        meta = {"threadId": CODEX_TID}
        err, text = m.tool("agentbus_send", {"to": "testbox/codex/22222222", "text": "x"}, meta=meta)
        self.assertTrue(err)
        self.assertIn("same_machine_same_harness", text)
        self.assertEqual(self.node.ops("send"), [])
        err, text = m.tool("agentbus_send", {"to": "testbox/claude/bob", "text": "x"}, meta=meta)
        self.assertFalse(err)
        self.assertEqual([s["to"] for s in self.node.ops("send")], ["testbox/claude/bob"])


class TestErrors(McpTestBase):
    def setUp(self):
        super().setUp()
        self.env = {"CLAUDE_CODE_SESSION_ID": CLAUDE_SID}

    def test_node_error_becomes_tool_error(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.env)
        err, text = m.tool("agentbus_send", {"to": "nobody/x/y", "text": "hello"})
        self.assertTrue(err)
        self.assertIn("unknown_target", text)

    def test_send_missing_or_bad_arguments(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.env)
        err, text = m.tool("agentbus_send", {"to": "testbox/codex/22222222"})
        self.assertTrue(err)
        self.assertIn("invalid_arguments", text)
        err, text = m.tool("agentbus_status", {})
        self.assertTrue(err)
        self.assertIn("msg_id", text)
        self.assertEqual(self.node.ops("send"), [])

    def test_text_over_64_kib_rejected_locally(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.env)
        err, text = m.tool("agentbus_send", {"to": "testbox/codex/22222222", "text": "x" * (64 * 1024 + 1)})
        self.assertTrue(err)
        self.assertIn("too_large", text)
        self.assertEqual(self.node.ops("send"), [])

    def test_rename_rejects_bad_name_without_calling_node(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.env)
        m.rpc("ping")  # startup registration has finished once the reply arrives
        before = len(self.node.ops("register"))
        err, text = m.tool("agentbus_rename", {"name": "bad/name"})
        self.assertTrue(err)
        self.assertIn("invalid_name", text)
        self.assertEqual(len(self.node.ops("register")), before)

    def test_rename_name_taken_is_tool_error(self):
        self.start_node()
        self.node.handler = lambda req: ({"ok": False, "error": "name_taken"} if req.get("op") == "register"
                                          and req.get("name") else default_node_handler(req))
        m = self.start_mcp(extra_env=self.env)
        err, text = m.tool("agentbus_rename", {"name": "taken"})
        self.assertTrue(err)
        self.assertIn("name_taken", text)

    def test_node_down_tools_return_errors_not_crash(self):
        m = self.start_mcp(extra_env=self.env)
        for name, args in [("agentbus_peers", {}), ("agentbus_status", {"msg_id": "m"}),
                           ("agentbus_send", {"to": "a/b/c", "text": "t"})]:
            err, text = m.tool(name, args)
            self.assertTrue(err, name)
            self.assertIn("node_unreachable", text, name)
        m.rpc("ping")  # still alive

    def test_stdout_is_json_only(self):
        self.start_node()
        m = self.start_mcp(extra_env=self.env)
        m.tool("agentbus_peers")
        self.assertNotIn(CLAUDE_TOKEN, m.stderr_text())


class TestEnsureSwitch(unittest.TestCase):
    def test_ensure_skipped_when_no_ensure_set(self):
        with mock.patch.dict(os.environ, {"AGENTBUS_NO_ENSURE": "1"}), \
                mock.patch.object(mcp_server.subprocess, "call") as call:
            mcp_server.ensure_node()
            call.assert_not_called()

    def test_ensure_runs_node_with_repo_pythonpath_and_never_raises(self):
        env = {k: v for k, v in os.environ.items() if k != "AGENTBUS_NO_ENSURE"}
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(mcp_server.subprocess, "call", side_effect=OSError("boom")) as call:
            mcp_server.ensure_node()  # must not raise
            argv = call.call_args[0][0]
            self.assertEqual(argv[1:], ["-m", "agentbus", "node", "--ensure"])
            self.assertEqual(call.call_args[1]["env"]["PYTHONPATH"], REPO_ROOT)
            self.assertEqual(call.call_args[1]["cwd"], REPO_ROOT)


class TestCodexProcDiscovery(unittest.TestCase):
    def test_rollout_regex_extracts_thread_id(self):
        name = "rollout-2026-11-10T16-40-49-%s.jsonl" % CODEX_TID
        m = mcp_server.ROLLOUT_RE.search(name)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1), CODEX_TID)
        self.assertIsNone(mcp_server.ROLLOUT_RE.search("notes-%s.jsonl" % CODEX_TID))


if __name__ == "__main__":
    unittest.main()

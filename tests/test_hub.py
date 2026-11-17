"""Tests for agentbus.hub (SPEC.md "Hub HTTP API").

Each test starts a real hub on an ephemeral 127.0.0.1 port in a thread,
backed by a temp SQLite DB and a temp token file.
"""

import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agentbus import hub  # noqa: E402

TOKEN = "test-token-0123456789"
MAX_TEXT = 64 * 1024


class HubTestCase(unittest.TestCase):
    lease_seconds = 120.0

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="agentbus-test-")
        self.db_path = os.path.join(self.tmp, "hub.db")
        token_path = os.path.join(self.tmp, "token")
        with open(token_path, "w") as f:
            f.write(TOKEN + "\n")
        self.server = hub.make_server(self.db_path, TOKEN, "127.0.0.1", 0,
                                      lease_seconds=self.lease_seconds)
        self.hub = self.server.hub
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self._old_stderr = sys.stderr
        sys.stderr = io.StringIO()  # keep request log lines out of test output

    def tearDown(self):
        sys.stderr = self._old_stderr
        self.server.shutdown()
        self.server.server_close()
        self.hub.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers -------------------------------------------------------------

    def call(self, method, path, body=None, token=TOKEN, headers=None, raw=None, timeout=40):
        data = None
        hdrs = dict(headers or {})
        if raw is not None:
            data = raw
            hdrs.setdefault("Content-Type", "application/json")
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        if token is not None:
            hdrs["Authorization"] = "Bearer " + token
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            with e:
                return e.code, json.loads(e.read().decode("utf-8"))

    def post(self, path, body, **kw):
        return self.call("POST", path, body=body, **kw)

    def get(self, path, **kw):
        return self.call("GET", path, **kw)

    def register(self, machine, harness, sid, name=None, cwd=None):
        body = {"machine": machine, "harness": harness, "session_id": sid}
        if name is not None:
            body["name"] = name
        if cwd is not None:
            body["cwd"] = cwd
        status, out = self.post("/v1/register", body)
        self.assertEqual(status, 200, out)
        return out

    def send(self, from_key, to, text, cid):
        return self.post("/v1/send", {"from_key": from_key, "to": to, "text": text,
                                      "client_msg_id": cid})

    def peers(self):
        status, out = self.get("/v1/peers")
        self.assertEqual(status, 200, out)
        return {p["session_key"]: p for p in out["peers"]}

    def heartbeat(self, machine, live):
        status, out = self.post("/v1/heartbeat", {"machine": machine, "live": live})
        self.assertEqual(status, 200, out)
        return out

    def pull(self, machine, wait=0):
        status, out = self.get("/v1/pull?machine=%s&wait=%s" % (machine, wait))
        self.assertEqual(status, 200, out)
        return out["messages"]

    def assertError(self, resp, status, code):
        got_status, out = resp
        self.assertEqual(got_status, status, out)
        self.assertEqual(out.get("error"), code, out)
        self.assertIsInstance(out.get("message"), str)


class TestAuthAndHealth(HubTestCase):
    def test_health_needs_no_auth(self):
        status, out = self.get("/v1/health", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(out, {"ok": True, "version": "0.3.0", "protocols": [1, 2],
                               "capabilities": ["durable_receipts"]})

    def test_missing_or_wrong_token_is_401(self):
        self.assertError(self.get("/v1/peers", token=None), 401, "unauthorized")
        self.assertError(self.get("/v1/peers", token="wrong"), 401, "unauthorized")
        self.assertError(self.get("/v1/peers", token=TOKEN + "x"), 401, "unauthorized")
        self.assertError(self.call("GET", "/v1/peers", token=None,
                                   headers={"Authorization": "Basic " + TOKEN}),
                         401, "unauthorized")

    def test_unauthorized_post_does_not_register(self):
        self.assertError(self.post("/v1/register", {"machine": "wsl", "harness": "claude",
                                                    "session_id": "abcdef123456"}, token="x"),
                         401, "unauthorized")
        self.assertEqual(self.peers(), {})

    def test_unknown_path_and_wrong_method(self):
        self.assertError(self.get("/v1/nope"), 404, "not_found")
        self.assertError(self.get("/v1/register"), 405, "method_not_allowed")

    def test_logs_one_line_without_token_or_body(self):
        buf = io.StringIO()
        old = sys.stderr
        sys.stderr = buf
        try:
            self.post("/v1/register", {"machine": "wsl", "harness": "claude",
                                       "session_id": "secretsess1"})
            self.get("/v1/peers")
        finally:
            sys.stderr = old
        # the request-log lines are the only stderr output
        lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
        self.assertEqual(len(lines), 2, lines)
        self.assertEqual((json.loads(lines[0])["method"], json.loads(lines[0])["path"], json.loads(lines[0])["status"]), ("POST", "/v1/register", 200))
        self.assertEqual((json.loads(lines[1])["method"], json.loads(lines[1])["path"], json.loads(lines[1])["status"]), ("GET", "/v1/peers", 200))
        self.assertNotIn(TOKEN, buf.getvalue())
        self.assertNotIn("secretsess1", buf.getvalue())

    def test_non_loopback_bind_refused(self):
        with self.assertRaises(ValueError):
            hub.make_server(os.path.join(self.tmp, "x.db"), TOKEN, "0.0.0.0", 0)


class TestRegisterPeers(HubTestCase):
    def test_register_default_and_custom_name(self):
        out = self.register("wsl", "claude", "abcdef123456")
        self.assertEqual(out, {"session_key": "wsl:claude:abcdef123456",
                               "address": "wsl/claude/abcdef12"})
        out = self.register("wsl", "claude", "other-session", name="mine")
        self.assertEqual(out["address"], "wsl/claude/mine")

    def test_name_taken_and_scope(self):
        self.register("wsl", "claude", "s1", name="mine")
        self.assertError(self.post("/v1/register", {"machine": "wsl", "harness": "claude",
                                                    "session_id": "s2", "name": "mine"}),
                         409, "name_taken")
        # same name on another machine or harness is fine
        self.register("h100", "claude", "s2", name="mine")
        self.register("wsl", "codex", "s3", name="mine")
        # re-registering the same session is not a conflict
        out = self.register("wsl", "claude", "s1", name="mine")
        self.assertEqual(out["address"], "wsl/claude/mine")

    def test_default_name_collision_is_name_taken(self):
        self.register("wsl", "claude", "s1", name="abcdef12")
        self.assertError(self.post("/v1/register", {"machine": "wsl", "harness": "claude",
                                                    "session_id": "abcdef12xyz"}),
                         409, "name_taken")

    def test_reregister_keeps_name_and_updates_cwd(self):
        self.register("wsl", "claude", "s1", name="mine", cwd="/a")
        self.register("wsl", "claude", "s1")
        p = self.peers()["wsl:claude:s1"]
        self.assertEqual(p["name"], "mine")
        self.assertEqual(p["cwd"], "/a")
        self.register("wsl", "claude", "s1", cwd="/b")
        self.assertEqual(self.peers()["wsl:claude:s1"]["cwd"], "/b")

    def test_bad_harness_and_bad_fields(self):
        self.assertError(self.post("/v1/register", {"machine": "wsl", "harness": "vim",
                                                    "session_id": "s1"}), 400, "bad_request")
        self.assertError(self.post("/v1/register", {"machine": "wsl", "harness": "claude"}),
                         400, "bad_request")
        self.assertError(self.post("/v1/register", {"machine": "a:b", "harness": "claude",
                                                    "session_id": "s1"}), 400, "bad_request")
        self.assertError(self.call("POST", "/v1/register", raw=b"{not json"),
                         400, "bad_request")

    def test_peers_fields(self):
        self.register("wsl", "claude", "s1", name="one", cwd="/w")
        self.register("h100", "codex", "t1")
        p = self.peers()
        self.assertEqual(set(p), {"wsl:claude:s1", "h100:codex:t1"})
        one = p["wsl:claude:s1"]
        self.assertEqual(one["address"], "wsl/claude/one")
        self.assertEqual(one["machine"], "wsl")
        self.assertEqual(one["harness"], "claude")
        self.assertEqual(one["name"], "one")
        self.assertEqual(one["cwd"], "/w")
        self.assertIs(one["online"], True)
        self.assertTrue(one["last_seen"].endswith("Z"))


class TestHeartbeat(HubTestCase):
    def test_heartbeat_online_offline_per_machine(self):
        self.register("wsl", "claude", "s1")
        self.register("wsl", "claude", "s2")
        self.register("h100", "claude", "s3")
        self.heartbeat("wsl", ["wsl:claude:s1"])
        p = self.peers()
        self.assertIs(p["wsl:claude:s1"]["online"], True)
        self.assertIs(p["wsl:claude:s2"]["online"], False)
        self.assertIs(p["h100:claude:s3"]["online"], True)  # other machine untouched

        self.heartbeat("wsl", [])
        p = self.peers()
        self.assertIs(p["wsl:claude:s1"]["online"], False)
        self.assertIs(p["h100:claude:s3"]["online"], True)

    def test_heartbeat_updates_last_seen(self):
        self.register("wsl", "claude", "s1")
        self.heartbeat("wsl", [])
        before = self.peers()["wsl:claude:s1"]["last_seen"]
        time.sleep(0.02)
        self.heartbeat("wsl", ["wsl:claude:s1"])
        p = self.peers()["wsl:claude:s1"]
        self.assertIs(p["online"], True)
        self.assertGreater(p["last_seen"], before)

    def test_heartbeat_validation(self):
        self.assertError(self.post("/v1/heartbeat", {"machine": "wsl"}), 400, "bad_request")
        self.assertError(self.post("/v1/heartbeat", {"machine": "wsl", "live": "x"}),
                         400, "bad_request")


class TestSend(HubTestCase):
    def setUp(self):
        HubTestCase.setUp(self)
        self.a = self.register("wsl", "claude", "aaaaaaaa1", name="alice")["session_key"]
        self.b = self.register("h100", "codex", "bbbbbbbb2", name="bob")["session_key"]
        self.heartbeat("wsl", [self.a])
        self.heartbeat("h100", [self.b])

    def test_send_to_address(self):
        status, out = self.send(self.a, "h100/codex/bob", "hi", "c1")
        self.assertEqual(status, 200, out)
        self.assertEqual(out["target"], "h100/codex/bob")
        self.assertIs(out["target_online"], True)
        self.assertTrue(out["msg_id"])

    def test_send_to_session_key(self):
        status, out = self.send(self.a, self.b, "hi", "c2")
        self.assertEqual(status, 200, out)
        self.assertEqual(out["target"], "h100/codex/bob")

    def test_unknown_target_404(self):
        self.assertError(self.send(self.a, "wsl/claude/nobody", "x", "c3"),
                         404, "unknown_target")
        self.assertError(self.send(self.a, "wsl/claude/aaaa", "x", "c4"),
                         404, "unknown_target")  # no prefix / fuzzy match
        self.assertError(self.send(self.a, "alice", "x", "c5"), 404, "unknown_target")

    def test_unknown_sender_404(self):
        self.assertError(self.send("wsl:claude:ghost", "wsl/claude/alice", "x", "c6"),
                         404, "unknown_sender")

    def test_offline_target_is_queued(self):
        status, out = self.send(self.a, self.b, "later", "c7")
        self.assertEqual(status, 200)
        self.heartbeat("h100", [])
        status, out = self.send(self.a, self.b, "later2", "c8")
        self.assertEqual(status, 200)
        self.assertIs(out["target_online"], False)
        status, st = self.get("/v1/status?msg_id=" + out["msg_id"])
        self.assertEqual(st["state"], "queued")
        self.assertEqual(st["target"], "h100/codex/bob")
        # not deliverable while offline
        self.assertEqual(self.pull("h100"), [])
        # comes online -> deliverable
        self.heartbeat("h100", [self.b])
        msgs = self.pull("h100")
        self.assertEqual([m["text"] for m in msgs], ["later", "later2"])

    def test_idempotent_send(self):
        s1, o1 = self.send(self.a, self.b, "one", "dup-1")
        s2, o2 = self.send(self.a, self.b, "one", "dup-1")
        self.assertEqual((s1, s2), (200, 200))
        self.assertEqual(o1["msg_id"], o2["msg_id"])
        self.assertEqual(o2["target"], "h100/codex/bob")
        # same client_msg_id from a different sender is a different message
        self.register("wsl", "claude", "cccccccc3", name="carol")
        s3, o3 = self.send("wsl:claude:cccccccc3", self.b, "two", "dup-1")
        self.assertNotEqual(o3["msg_id"], o1["msg_id"])
        self.assertEqual(len(self.pull("h100")), 2)

    def test_text_limits(self):
        ok = "x" * MAX_TEXT
        self.assertEqual(self.send(self.a, self.b, ok, "big-ok")[0], 200)
        too_big = "x" * (MAX_TEXT + 1)
        self.assertError(self.send(self.a, self.b, too_big, "big-1"), 413, "too_large")
        # multibyte: 32769 * 2 bytes = 65538 > 64 KiB even though only 32769 chars
        self.assertError(self.send(self.a, self.b, "é" * 32769, "big-2"),
                         413, "too_large")

    def test_empty_text_rejected(self):
        self.assertError(self.send(self.a, self.b, "", "e1"), 400, "bad_request")


class TestPullLongPoll(HubTestCase):
    def setUp(self):
        HubTestCase.setUp(self)
        self.a = self.register("wsl", "claude", "aaaaaaaa1", name="alice")["session_key"]
        self.b = self.register("wsl", "claude", "bbbbbbbb2", name="bob")["session_key"]
        self.heartbeat("wsl", [self.a, self.b])

    def test_pull_requires_machine_and_valid_wait(self):
        self.assertError(self.get("/v1/pull"), 400, "bad_request")
        self.assertError(self.get("/v1/pull?machine=wsl&wait=abc"), 400, "bad_request")

    def test_pull_wait_zero_returns_immediately(self):
        t0 = time.monotonic()
        self.assertEqual(self.pull("wsl", 0), [])
        self.assertLess(time.monotonic() - t0, 2.0)

    def test_pull_times_out_after_wait(self):
        t0 = time.monotonic()
        self.assertEqual(self.pull("wsl", 1), [])
        elapsed = time.monotonic() - t0
        self.assertGreaterEqual(elapsed, 0.9)
        self.assertLess(elapsed, 5.0)

    def test_long_poll_wakes_on_send(self):
        result = {}

        def puller():
            t0 = time.monotonic()
            result["msgs"] = self.pull("wsl", 10)
            result["elapsed"] = time.monotonic() - t0

        th = threading.Thread(target=puller)
        th.start()
        time.sleep(0.3)
        status, out = self.send(self.a, self.b, "wake up", "w1")
        self.assertEqual(status, 200, out)
        th.join(15)
        self.assertFalse(th.is_alive())
        self.assertLess(result["elapsed"], 5.0, "pull did not wake on send")
        self.assertGreaterEqual(result["elapsed"], 0.25)
        msgs = result["msgs"]
        self.assertEqual(len(msgs), 1)
        m = msgs[0]
        self.assertEqual(m["msg_id"], out["msg_id"])
        self.assertEqual(m["from_address"], "wsl/claude/alice")
        self.assertEqual(m["to_key"], self.b)
        self.assertEqual(m["text"], "wake up")
        self.assertTrue(m["created_at"].endswith("Z"))

    def test_wait_is_capped_at_25(self):
        # wait=999 is capped to 25s, not rejected; a queued message returns at once
        status, out = self.send(self.a, self.b, "now", "cap1")
        self.assertEqual(status, 200)
        t0 = time.monotonic()
        msgs = self.pull("wsl", 999)
        self.assertEqual(len(msgs), 1)
        self.assertLess(time.monotonic() - t0, 5.0)

    def test_leased_message_not_returned_twice(self):
        self.send(self.a, self.b, "once", "l1")
        self.assertEqual(len(self.pull("wsl")), 1)
        self.assertEqual(self.pull("wsl"), [])


class TestLease(HubTestCase):
    lease_seconds = 120.0

    def setUp(self):
        HubTestCase.setUp(self)
        # injectable lease length for the test
        self.hub.lease_seconds = 0.5
        self.a = self.register("wsl", "claude", "aaaaaaaa1", name="alice")["session_key"]
        self.b = self.register("wsl", "claude", "bbbbbbbb2", name="bob")["session_key"]
        self.heartbeat("wsl", [self.a, self.b])

    def test_expired_lease_requeues(self):
        status, out = self.send(self.a, self.b, "lease me", "lease-1")
        msg_id = out["msg_id"]
        first = self.pull("wsl")
        self.assertEqual([m["msg_id"] for m in first], [msg_id])
        self.assertEqual(self.pull("wsl"), [])  # still leased
        time.sleep(0.8)
        again = self.pull("wsl")
        self.assertEqual([m["msg_id"] for m in again], [msg_id])
        self.assertEqual(again[0]["text"], "lease me")

    def test_reported_message_not_requeued(self):
        status, out = self.send(self.a, self.b, "done", "lease-2")
        msg_id = out["msg_id"]
        self.pull("wsl")
        status, rep = self.post("/v1/report", {"msg_id": msg_id, "state": "delivered"})
        self.assertEqual(status, 200, rep)
        time.sleep(0.8)
        self.assertEqual(self.pull("wsl"), [])
        status, st = self.get("/v1/status?msg_id=" + msg_id)
        self.assertEqual(st["state"], "delivered")


class TestReportStatus(HubTestCase):
    def setUp(self):
        HubTestCase.setUp(self)
        self.a = self.register("wsl", "claude", "aaaaaaaa1", name="alice")["session_key"]
        self.b = self.register("wsl", "claude", "bbbbbbbb2", name="bob")["session_key"]
        self.heartbeat("wsl", [self.a, self.b])

    def new_msg(self, cid):
        return self.send(self.a, self.b, "payload " + cid, cid)[1]["msg_id"]

    def report(self, msg_id, state, detail=None):
        body = {"msg_id": msg_id, "state": state}
        if detail is not None:
            body["detail"] = detail
        return self.post("/v1/report", body)

    def test_status_shape(self):
        msg_id = self.new_msg("st-1")
        status, st = self.get("/v1/status?msg_id=" + msg_id)
        self.assertEqual(status, 200)
        self.assertEqual(set(st), {"msg_id", "state", "detail", "target", "created_at",
                                   "updated_at"})
        self.assertEqual(st["state"], "queued")
        self.assertIsNone(st["detail"])
        self.assertEqual(st["target"], "wsl/claude/bob")

    def test_status_unknown_404(self):
        self.assertError(self.get("/v1/status?msg_id=nope"), 404, "unknown_message")
        self.assertError(self.get("/v1/status"), 400, "bad_request")

    def test_report_delivered_with_detail(self):
        msg_id = self.new_msg("r-1")
        self.pull("wsl")
        status, out = self.report(msg_id, "delivered", "written to socket")
        self.assertEqual(status, 200, out)
        status, st = self.get("/v1/status?msg_id=" + msg_id)
        self.assertEqual(st["state"], "delivered")
        self.assertEqual(st["detail"], "written to socket")

    def test_terminal_states_are_final(self):
        msg_id = self.new_msg("r-2")
        self.pull("wsl")
        self.assertEqual(self.report(msg_id, "failed", "boom")[0], 200)
        self.assertError(self.report(msg_id, "delivered"), 409, "terminal_state")
        self.assertError(self.report(msg_id, "in_doubt"), 409, "terminal_state")
        status, st = self.get("/v1/status?msg_id=" + msg_id)
        self.assertEqual(st["state"], "failed")
        self.assertEqual(st["detail"], "boom")
        # repeating the same terminal report is harmless
        self.assertEqual(self.report(msg_id, "failed", "boom")[0], 200)

    def test_held_is_terminal(self):
        msg_id = self.new_msg("r-3")
        self.assertEqual(self.report(msg_id, "held")[0], 200)
        self.assertError(self.report(msg_id, "delivered"), 409, "terminal_state")

    def test_report_invalid_state_and_unknown_message(self):
        msg_id = self.new_msg("r-4")
        self.assertError(self.report(msg_id, "queued"), 400, "bad_request")
        self.assertError(self.report(msg_id, "bogus"), 400, "bad_request")
        self.assertError(self.report("nope", "delivered"), 404, "unknown_message")

    def test_expired_after_seven_days(self):
        msg_id = self.new_msg("x-1")
        db = sqlite3.connect(self.db_path)
        try:
            with db:
                db.execute("UPDATE messages SET created_at = created_at - ? WHERE msg_id = ?",
                           (8 * 24 * 3600, msg_id))
        finally:
            db.close()
        self.assertEqual(self.pull("wsl"), [])
        status, st = self.get("/v1/status?msg_id=" + msg_id)
        self.assertEqual(st["state"], "expired")
        self.assertEqual(self.report(msg_id, "delivered")[1]["error"], "terminal_state")


if __name__ == "__main__":
    unittest.main()

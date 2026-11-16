"""Protocol 2 fault and persistence tests; all traffic is isolated."""
import hashlib
import http.client
import io
import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from agentbus.hub import ApiError, Hub, make_server


class HubV2Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "hub.db")
        self.hub = Hub(self.path)
        self.a = self.hub.register(dict(machine="a", harness="claude", session_id="a"))
        self.b = self.hub.register(dict(machine="b", harness="codex", session_id="b"))
        self.c = self.hub.register(dict(machine="c", harness="claude", session_id="c"))

    def tearDown(self):
        self.hub.close()
        self.tmp.cleanup()

    def send(self, seq=1, key=None, text="hello", **extra):
        body = dict(from_key=self.a["session_key"], to=self.b["session_key"],
                    text=text, client_msg_id=key or "request-%d" % seq, client_seq=seq)
        body.update(extra)
        return self.hub.send2(body)

    def ack(self, msg):
        return self.hub.ack2(dict(msg_id=msg["msg_id"], session_key=self.b["session_key"]))

    def test_lost_send_response_retries_same_id_and_revision(self):
        first = self.send()
        again = self.send()
        self.assertEqual(first["msg_id"], again["msg_id"])
        self.assertEqual(first["state_revision"], again["state_revision"])
        for changed in (dict(text="different"), dict(client_seq=2), dict(to=self.c["session_key"])):
            with self.assertRaises(ApiError) as error:
                self.send(**changed)
            self.assertEqual(error.exception.status, 409)

    def test_gap_rejected_without_consuming_sequence(self):
        with self.assertRaises(ApiError):
            self.send(2)
        self.assertEqual(self.send()["seq"], 1)

    def test_ack_cannot_skip_predecessor_and_other_stream_is_not_blocked(self):
        first = self.send()
        second = self.send(2)
        other = self.send(from_key=self.c["session_key"], key="other")
        with self.assertRaises(ApiError) as error:
            self.ack(second)
        self.assertEqual(error.exception.code, "blocked_by")
        self.assertEqual({m["msg_id"] for m in self.hub.pull2("b", 0)},
                         {first["msg_id"], other["msg_id"]})
        self.ack(first)
        self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [second["msg_id"]])

    def test_v1_and_v2_cannot_consume_each_others_messages(self):
        modern = self.send()
        old = self.hub.send(self.a["session_key"], self.b["session_key"], "legacy", "legacy")
        self.assertEqual([m["msg_id"] for m in self.hub.pull("b", 0)], [old["msg_id"]])
        self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [modern["msg_id"]])

    def test_tied_and_backwards_clocks_never_reorder_or_bypass_ack(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            a = self.send()
            b = self.send(2)
        with patch("agentbus.hub.time.time", return_value=900):
            self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [a["msg_id"]])
            self.assertEqual(self.hub.pull2("b", 0), [])
            self.ack(a)
            self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [b["msg_id"]])

    def test_lost_pull_response_releases_only_same_head_after_lease(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            a = self.send()
            b = self.send(2)
            first = self.hub.pull2("b", 0)[0]
        with patch("agentbus.hub.time.time", return_value=1001):
            self.assertEqual(self.hub.pull2("b", 0), [])
        with patch("agentbus.hub.time.time", return_value=1121):
            again = self.hub.pull2("b", 0)[0]
        self.assertEqual(first["msg_id"], again["msg_id"])
        self.assertEqual(again["attempt"], first["attempt"] + 1)
        with self.assertRaises(ApiError) as error:
            self.hub.report2(dict(msg_id=a["msg_id"], attempt=first["attempt"], state="injected"))
        self.assertEqual(error.exception.code, "stale_attempt")
        self.assertEqual(self.hub.status2(b["msg_id"], self.b["session_key"])["blocked_by"], a["msg_id"])

    def test_ack_wins_race_and_duplicate_ack_is_idempotent(self):
        msg = self.send()
        lease = self.hub.pull2("b", 0)[0]
        acked = self.ack(msg)
        self.assertTrue(acked["receiver_acknowledged"])
        self.assertEqual(self.ack(msg)["state_revision"], acked["state_revision"])
        with self.assertRaises(ApiError):
            self.hub.report2(dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state="failed"))
        self.assertEqual(self.hub.status2(msg["msg_id"], self.a["session_key"])["state"], "acknowledged")

    def test_report_before_ack_and_current_attempt_after_sweep(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.send()
            lease = self.hub.pull2("b", 0)[0]
        with patch("agentbus.hub.time.time", return_value=1121):
            self.hub.sweep2()
            # A lease timeout is not proof of failure: an outcome for this exact
            # attempt remains useful if no newer consumer has leased it.
            self.hub.report2(dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state="in_doubt"))
            self.assertEqual(self.hub.pull2("b", 0), [])
        self.assertEqual(self.ack(msg)["state"], "acknowledged")

    def test_concurrent_send_retry_has_one_committed_identity(self):
        barrier = threading.Barrier(5)
        results, errors = [], []
        def worker():
            barrier.wait()
            try:
                results.append(self.send())
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertFalse(errors)
        self.assertEqual(len(results), 5)
        self.assertEqual(len({r["msg_id"] for r in results}), 1)
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM events_v2").fetchone()[0], 1)

    def test_unknown_injection_and_failure_block_without_reinjection(self):
        for state in ("injected", "in_doubt", "failed"):
            with self.subTest(state=state):
                msg = self.send(key=state, from_key=self.c["session_key"], seq=1)
                lease = next(m for m in self.hub.pull2("b", 0) if m["msg_id"] == msg["msg_id"])
                report = dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state=state, detail="result")
                result = self.hub.report2(report)
                self.assertEqual(self.hub.report2(report)["state_revision"], result["state_revision"])
                self.assertEqual(self.hub.pull2("b", 0), [])
                self.ack(msg)
                # Start the next independent subtest stream by reopening a new sender identity.
                self.c = self.hub.register(dict(machine="c", harness="claude", session_id=state, name=state))

    def test_wrong_recipient_and_nonparticipant_cannot_ack_or_read(self):
        msg = self.send()
        with self.assertRaises(ApiError):
            self.hub.ack2(dict(msg_id=msg["msg_id"], session_key=self.a["session_key"]))
        for call in (self.hub.status2, self.hub.message2, self.hub.history2):
            with self.assertRaises(ApiError):
                call(msg["msg_id"], self.c["session_key"])

    def test_reply_must_be_opposite_direction_same_peers(self):
        msg = self.send()
        with self.assertRaises(ApiError):
            self.send(2, reply_to=msg["msg_id"])
        reply = self.hub.send2(dict(from_key=self.b["session_key"], to=self.a["session_key"],
                                  text="answer", client_msg_id="reply", client_seq=1, reply_to=msg["msg_id"]))
        self.assertEqual(reply["reply_to"], msg["msg_id"])

    def test_full_unicode_body_hash_and_inbox_cursor(self):
        text = "病理🌸\n" * 2000
        msg = self.send(text=text)
        self.send(2)
        page = self.hub.inbox2(self.b["session_key"], limit=1)
        self.assertTrue(page["has_more"])
        self.assertFalse(page["complete"])
        full = page["items"][0]
        self.assertEqual(full["text"], text)
        self.assertEqual(full["text_bytes"], len(text.encode()))
        self.assertEqual(full["sha256"], hashlib.sha256(text.encode()).hexdigest())
        page2 = self.hub.inbox2(self.b["session_key"], after=page["next_cursor"], limit=1)
        self.assertFalse(page2["has_more"])
        self.assertNotEqual(page2["items"][0]["msg_id"], msg["msg_id"])

    def test_pagination_limits_match_public_tool_contract(self):
        msg = self.send()
        for limit in (1, 200, "200"):
            self.assertEqual(len(self.hub.inbox2(self.b["session_key"], limit=limit)["items"]), 1)
            self.assertEqual(len(self.hub.history2(msg["msg_id"], self.b["session_key"], limit=limit)["events"]), 1)
        for limit in (0, 201, -1, True, 1.5, "nan"):
            with self.subTest(limit=limit):
                with self.assertRaises(ApiError):
                    self.hub.inbox2(self.b["session_key"], limit=limit)
                with self.assertRaises(ApiError):
                    self.hub.history2(msg["msg_id"], self.b["session_key"], limit=limit)

    def test_history_transaction_rolls_back_message_and_event(self):
        original = self.hub._event2
        def failing(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError("simulated persistence failure")
        with patch.object(self.hub, "_event2", side_effect=failing):
            with self.assertRaises(RuntimeError):
                self.send()
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM messages_v2").fetchone()[0], 0)
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM events_v2").fetchone()[0], 0)
        self.assertEqual(self.send()["seq"], 1)

    def test_history_pagination_and_body_only_retention_phase(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.send()
            self.hub.pull2("b", 0)
            self.ack(msg)
        first = self.hub.history2(msg["msg_id"], self.a["session_key"], limit=1)
        self.assertTrue(first["has_more"])
        following = self.hub.history2(msg["msg_id"], self.a["session_key"], after=first["next_cursor"])
        self.assertEqual([e["reason"] for e in following["events"]], ["leased", "recipient_ack"])
        with patch("agentbus.hub.time.time", return_value=1000 + 8 * 86400):
            self.hub.sweep2()
        self.assertFalse(self.hub.status2(msg["msg_id"], self.a["session_key"])["body_retained"])
        history = self.hub.history2(msg["msg_id"], self.a["session_key"])
        self.assertFalse(history["history_pruned"])
        self.assertEqual(history["events"][-1]["reason"], "body_pruned")

    def test_expired_unacknowledged_head_keeps_full_body_and_blocks(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            first = self.send()
            self.send(2)
        with patch("agentbus.hub.time.time", return_value=1000 + 31 * 86400):
            self.hub.sweep2()
            self.assertEqual(self.hub.pull2("b", 0), [])
            status = self.hub.message2(first["msg_id"], self.b["session_key"])
            self.assertEqual(status["state"], "expired")
            self.assertEqual(status["text"], "hello")
            self.assertFalse(status["history_pruned"])
            self.ack(first)

    def test_gc_keeps_tombstones_active_and_idempotency_across_restart(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.send()
            self.ack(msg)
            active = self.send(2, text="active")
        with patch("agentbus.hub.time.time", return_value=1000 + 31 * 86400):
            self.hub.sweep2()
            pruned = self.hub.message2(msg["msg_id"], self.a["session_key"])
            self.assertFalse(pruned["body_retained"])
            self.assertIsNone(pruned["text"])
            self.assertTrue(self.hub.history2(msg["msg_id"], self.a["session_key"])["history_pruned"])
            self.assertEqual(self.hub.message2(active["msg_id"], self.a["session_key"])["text"], "active")
        self.hub.close()
        self.hub = Hub(self.path)
        self.assertEqual(self.send()["msg_id"], msg["msg_id"])
        self.assertEqual(self.send(3)["seq"], 3)
        self.assertEqual(self.hub.status2("absent", self.a["session_key"])["state"], "unknown_or_pruned")


class HubV2HTTPTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.log = io.StringIO()
        self.capture = patch("sys.stderr", self.log)
        self.capture.start()
        self.server = make_server(os.path.join(self.tmp.name, "http.db"), "isolated-test-token", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()
        self.server.hub.close()
        self.capture.stop()
        self.tmp.cleanup()

    def call(self, method, path, body=None, authorized=True):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        headers = {"Content-Type": "application/json"}
        if authorized:
            headers["Authorization"] = "Bearer isolated-test-token"
        connection.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
        response = connection.getresponse()
        result = response.status, json.loads(response.read()), response.getheader("X-Request-ID")
        connection.close()
        return result

    def test_http_lost_responses_retrieval_ack_history_and_safe_correlated_logs(self):
        from urllib.parse import urlencode
        _, a, _ = self.call("POST", "/v1/register", dict(machine="a", harness="claude", session_id="a"))
        _, b, _ = self.call("POST", "/v1/register", dict(machine="b", harness="codex", session_id="b"))
        body = dict(from_key=a["session_key"], to=b["session_key"], text="PRIVATE BODY 甲" * 300,
                    client_msg_id="durable-request", client_seq=1)
        self.call("POST", "/v2/send", body)  # Discard a real successful HTTP response.
        status, msg, correlation = self.call("POST", "/v2/send", body)
        self.assertEqual(status, 200)
        self.assertTrue(correlation)
        self.call("GET", "/v2/pull?machine=b&wait=0")  # Discard the first lease response.
        self.assertEqual(self.call("GET", "/v2/pull?machine=b&wait=0")[1]["messages"], [])
        query = urlencode(dict(msg_id=msg["msg_id"], session_key=b["session_key"]))
        self.assertEqual(self.call("GET", "/v2/message?" + query)[1]["text"], body["text"])
        ack = self.call("POST", "/v2/ack", dict(msg_id=msg["msg_id"], session_key=b["session_key"]))[1]
        self.assertTrue(ack["receiver_acknowledged"])
        self.assertEqual(self.call("GET", "/v2/status?" + query)[1]["state"], "acknowledged")
        self.assertEqual(self.call("GET", "/v2/history?" + query)[1]["events"][-1]["reason"], "recipient_ack")
        self.assertEqual(len(self.call("GET", "/v2/inbox?" + urlencode(dict(session_key=b["session_key"])))[1]["items"]), 1)
        self.assertEqual(self.call("GET", "/v2/status?" + query, authorized=False)[0], 401)
        records = [json.loads(line) for line in self.log.getvalue().splitlines()]
        record = next(r for r in records if r.get("request_id") == correlation)
        self.assertEqual(record["msg_id"], msg["msg_id"])
        self.assertEqual(record["client_msg_id"], body["client_msg_id"])
        self.assertNotIn(body["text"], self.log.getvalue())
        self.assertNotIn("isolated-test-token", self.log.getvalue())

    def test_http_validation_health_and_periodic_maintenance_hook(self):
        self.assertEqual(self.call("GET", "/v1/health", authorized=False)[1]["protocols"], [1, 2])
        self.assertEqual(self.call("GET", "/v2/inbox?session_key=x&limit=-1")[0], 400)
        self.assertEqual(self.call("GET", "/v2/history?msg_id=x&session_key=x&after=nan")[0], 400)
        observed = threading.Event()
        with patch.object(self.server.hub, "sweep2", side_effect=observed.set):
            self.server._next_gc = 0
            self.assertTrue(observed.wait(2), "maintenance must run with no new HTTP traffic")


if __name__ == "__main__":
    unittest.main()

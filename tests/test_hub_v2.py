"""Protocol 2 fault and persistence tests; all traffic is isolated."""
import hashlib
import http.client
import io
import json
import os
import sqlite3
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

    def receive(self, msg):
        return self.hub.receive2(dict(msg_id=msg["msg_id"], session_key=self.b["session_key"],
                                      attempt=msg["attempt"], sha256=msg["sha256"], text_bytes=msg["text_bytes"]))

    def test_durable_receipts_release_three_messages_without_manual_ack(self):
        expected = [self.send(i)["msg_id"] for i in (1, 2, 3)]
        observed = []
        for msg_id in expected:
            lease = self.hub.pull2("b", 0)[0]
            self.assertEqual(lease["msg_id"], msg_id)
            receipt = self.receive(lease)
            self.assertTrue(receipt["receiver_persisted"])
            self.assertFalse(receipt["receiver_acknowledged"])
            self.assertEqual(self.receive(lease)["received_at"], receipt["received_at"])
            self.hub.report2(dict(msg_id=msg_id, attempt=lease["attempt"], state="injected"))
            observed.append(msg_id)
        self.assertEqual(observed, expected)
        self.assertEqual(self.hub.pull2("b", 0), [])

    def test_receive_validates_recipient_attempt_integrity_and_predecessor(self):
        first, second = self.send(), self.send(2)
        lease = self.hub.pull2("b", 0)[0]
        original = dict(msg_id=first["msg_id"], session_key=self.b["session_key"],
                        attempt=lease["attempt"], sha256=lease["sha256"], text_bytes=lease["text_bytes"])
        for change in (dict(session_key=self.a["session_key"]), dict(attempt=99),
                       dict(sha256="0" * 64), dict(text_bytes=999)):
            with self.subTest(change=change), self.assertRaises(ApiError):
                self.hub.receive2(dict(original, **change))
        with self.assertRaises(ApiError):
            self.receive(second)  # Even a durable copy of seq 2 cannot skip seq 1.
        self.assertFalse(self.hub.status2(first["msg_id"], self.a["session_key"])["receiver_persisted"])
        self.receive(lease)
        self.assertEqual(self.receive(second)["seq"], 2)

    def test_optional_late_ack_does_not_overwrite_injection_truth(self):
        first, second = self.send(), self.send(2)
        lease = self.hub.pull2("b", 0)[0]
        self.receive(lease)
        self.hub.report2(dict(msg_id=first["msg_id"], attempt=lease["attempt"], state="in_doubt", detail="uncertain native presentation"))
        acked = self.ack(first)
        self.assertEqual(acked["state"], "in_doubt")
        self.assertEqual(acked["detail"], "uncertain native presentation")
        self.assertTrue(acked["receiver_acknowledged"])
        self.assertTrue(acked["receiver_persisted"])
        self.assertTrue(self.ack(second)["receiver_acknowledged"])
        self.assertFalse(self.hub.status2(second["msg_id"], self.b["session_key"])["receiver_persisted"])

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

    def test_optional_ack_does_not_release_durable_order_gate(self):
        first = self.send()
        second = self.send(2)
        other = self.send(from_key=self.c["session_key"], key="other")
        self.assertTrue(self.ack(second)["receiver_acknowledged"])
        leased = self.hub.pull2("b", 0)
        self.assertEqual({m["msg_id"] for m in leased},
                         {first["msg_id"], other["msg_id"]})
        self.receive(next(m for m in leased if m["msg_id"] == first["msg_id"]))
        self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [second["msg_id"]])

    def test_v1_and_v2_cannot_consume_each_others_messages(self):
        modern = self.send()
        old = self.hub.send(self.a["session_key"], self.b["session_key"], "legacy", "legacy")
        self.assertEqual([m["msg_id"] for m in self.hub.pull("b", 0)], [old["msg_id"]])
        self.assertEqual([m["msg_id"] for m in self.hub.pull2("b", 0)], [modern["msg_id"]])

    def test_tied_and_backwards_clocks_never_reorder_or_bypass_durable_receipt(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            a = self.send()
            b = self.send(2)
        with patch("agentbus.hub.time.time", return_value=900):
            lease = self.hub.pull2("b", 0)[0]
            self.assertEqual(lease["msg_id"], a["msg_id"])
            self.assertEqual(self.hub.pull2("b", 0), [])
            self.receive(lease)
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

    def test_ack_races_report_without_overwriting_native_truth(self):
        msg = self.send()
        lease = self.hub.pull2("b", 0)[0]
        acked = self.ack(msg)
        self.assertTrue(acked["receiver_acknowledged"])
        self.assertEqual(self.ack(msg)["state_revision"], acked["state_revision"])
        reported = self.hub.report2(dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state="failed"))
        self.assertEqual(reported["state"], "failed")
        self.assertTrue(reported["receiver_acknowledged"])
        self.assertFalse(reported["receiver_persisted"])

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
        self.assertEqual(self.ack(msg)["state"], "in_doubt")

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

    def test_reply_to_later_visible_message_does_not_require_predecessor_read(self):
        first, second = self.send(), self.send(2)
        self.assertEqual(second["blocked_by"], first["msg_id"])
        reply = self.hub.send2(dict(from_key=self.b["session_key"], to=self.a["session_key"],
                                  text="answer to exact later context", client_msg_id="later-reply", client_seq=1,
                                  reply_to=second["msg_id"]))
        self.assertEqual(reply["reply_to"], second["msg_id"])

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

    def test_gc_requires_durable_copy_and_success_or_explicit_read(self):
        messages = {}
        with patch("agentbus.hub.time.time", return_value=1000):
            for index, (name, persisted, outcome, read) in enumerate((
                ("safe", True, "injected", False), ("uncertain", True, "in_doubt", False),
                ("failed", True, "failed", False), ("old_injected", False, "injected", False),
                ("read", False, "failed", True),
            )):
                sender = self.hub.register(dict(machine="gc%d" % index, harness="claude", session_id="sender"))
                msg = self.send(from_key=sender["session_key"], key=name)
                lease = next(m for m in self.hub.pull2("b", 0) if m["msg_id"] == msg["msg_id"])
                if persisted:
                    self.receive(lease)
                self.hub.report2(dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state=outcome))
                if read:
                    self.ack(msg)
                messages[name] = msg
        with patch("agentbus.hub.time.time", return_value=1000 + 31 * 86400):
            self.hub.sweep2()
        for name, msg in messages.items():
            status = self.hub.message2(msg["msg_id"], self.b["session_key"])
            pruned = name in ("safe", "read")
            self.assertEqual(status["body_retained"], not pruned, name)
            self.assertEqual(status["history_pruned"], pruned, name)
            self.assertEqual(status["task_completion"], None)

    def test_receive_is_transactional_and_lost_response_is_idempotent(self):
        msg = self.send()
        lease = self.hub.pull2("b", 0)[0]
        with patch.object(self.hub, "_event2", side_effect=RuntimeError("commit fault")):
            with self.assertRaises(RuntimeError):
                self.receive(lease)
        self.assertFalse(self.hub.status2(msg["msg_id"], self.b["session_key"])["receiver_persisted"])
        first = self.receive(lease)
        with patch("agentbus.hub.time.time", return_value=1):
            retried = self.receive(lease)
        self.assertEqual(first["received_at"], retried["received_at"])
        self.assertEqual(first["state_revision"], retried["state_revision"])
        self.assertIsNone(retried["agent_read"])

    def test_attempt_zero_report_requires_durable_retrieval_receipt(self):
        msg = self.send()
        report = dict(msg_id=msg["msg_id"], attempt=0, state="injected")
        with self.assertRaises(ApiError) as error:
            self.hub.report2(report)
        self.assertEqual(error.exception.code, "receipt_required")
        self.receive(msg)
        result = self.hub.report2(report)
        self.assertEqual(result["state"], "injected")
        self.assertTrue(result["receiver_persisted"])
        self.assertFalse(result["receiver_acknowledged"])

    def test_gc_window_starts_after_both_storage_and_successful_injection(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.send()
            lease = self.hub.pull2("b", 0)[0]
            self.receive(lease)
        with patch("agentbus.hub.time.time", return_value=1000 + 31 * 86400):
            self.hub.sweep2()
            self.assertTrue(self.hub.status2(msg["msg_id"], self.b["session_key"])["body_retained"])
            report = dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state="injected")
            injected = self.hub.report2(report)
            self.hub.sweep2()
            self.assertTrue(self.hub.status2(msg["msg_id"], self.b["session_key"])["body_retained"])
        with patch("agentbus.hub.time.time", return_value=1000 + 39 * 86400):
            self.assertEqual(self.hub.report2(report)["injected_at"], injected["injected_at"])
            self.hub.sweep2()
            retained = self.hub.status2(msg["msg_id"], self.b["session_key"])
            self.assertFalse(retained["body_retained"])
            self.assertFalse(retained["history_pruned"])

    def test_late_optional_ack_after_hub_pruning_preserves_transport_truth(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.send()
            lease = self.hub.pull2("b", 0)[0]
            received = self.receive(lease)
            injected = self.hub.report2(dict(msg_id=msg["msg_id"], attempt=lease["attempt"], state="injected"))
        with patch("agentbus.hub.time.time", return_value=1000 + 8 * 86400):
            self.hub.sweep2()
            acked = self.ack(msg)
        self.assertFalse(acked["body_retained"])
        self.assertTrue(acked["receiver_acknowledged"])
        self.assertTrue(acked["agent_read"])
        self.assertEqual(acked["state"], "injected")
        self.assertEqual(acked["received_at"], received["received_at"])
        self.assertEqual(acked["injected_at"], injected["injected_at"])
        self.assertIsNone(acked["task_completion"])

    def test_new_optional_ack_does_not_turn_into_durable_storage_on_restart(self):
        msg = self.send()
        self.ack(msg)
        self.hub.close()
        self.hub = Hub(self.path)
        result = self.hub.status2(msg["msg_id"], self.b["session_key"])
        self.assertFalse(result["receiver_persisted"])
        self.assertTrue(result["receiver_acknowledged"])

    def test_real_legacy_schema_migration_preserves_read_and_injection_meanings(self):
        path = os.path.join(self.tmp.name, "legacy.db")
        connection = sqlite3.connect(path)
        connection.executescript('''
          CREATE TABLE messages_v2 (
            message_order INTEGER PRIMARY KEY AUTOINCREMENT, msg_id TEXT NOT NULL UNIQUE,
            from_key TEXT NOT NULL, to_key TEXT NOT NULL, from_address TEXT NOT NULL, to_address TEXT NOT NULL,
            client_msg_id TEXT NOT NULL, seq INTEGER NOT NULL, reply_to TEXT, text TEXT,
            sha256 TEXT NOT NULL, text_bytes INTEGER NOT NULL, fingerprint TEXT NOT NULL,
            state TEXT NOT NULL, detail TEXT, attempt INTEGER NOT NULL DEFAULT 0, lease_until REAL,
            created_at REAL NOT NULL, updated_at REAL NOT NULL, acked_at REAL,
            state_revision INTEGER NOT NULL DEFAULT 0, history_pruned INTEGER NOT NULL DEFAULT 0,
            UNIQUE(from_key,client_msg_id), UNIQUE(from_key,to_key,seq));
        ''')
        sha = hashlib.sha256(b"legacy").hexdigest()
        for seq, state in ((1, "acknowledged"), (2, "injected")):
            connection.execute("INSERT INTO messages_v2(msg_id,from_key,to_key,from_address,to_address,client_msg_id,"
                               "seq,text,sha256,text_bytes,fingerprint,state,attempt,created_at,updated_at,acked_at) "
                               "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               ("old%d" % seq, "from", "to", "a/claude/a", "b/codex/b", str(seq), seq,
                                "legacy", sha, 6, "legacy-fingerprint", state, 1, 1000, 1010,
                                1010 if seq == 1 else None))
        connection.commit()
        connection.close()
        legacy = Hub(path)
        try:
            acked = legacy.status2("old1", "to")
            injected = legacy.status2("old2", "to")
            self.assertTrue(acked["receiver_persisted"])
            self.assertTrue(acked["receiver_acknowledged"])
            self.assertEqual(acked["state"], "acknowledged")
            self.assertFalse(injected["receiver_persisted"])
            self.assertFalse(injected["receiver_acknowledged"])
            self.assertEqual(injected["injection_state"], "injected")
            recovered = legacy.receive2(dict(msg_id="old2", session_key="to", attempt=1, sha256=sha, text_bytes=6))
            self.assertTrue(recovered["receiver_persisted"])
            self.assertFalse(recovered["receiver_acknowledged"])
            self.assertEqual(recovered["state"], "injected")
        finally:
            legacy.close()


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
        envelope = self.call("GET", "/v2/message?" + query)[1]
        self.assertEqual(envelope["text"], body["text"])
        receipt_body = dict(msg_id=msg["msg_id"], session_key=b["session_key"], attempt=envelope["attempt"],
                            sha256=envelope["sha256"], text_bytes=envelope["text_bytes"])
        self.call("POST", "/v2/receive", receipt_body)  # Discard committed receipt response.
        receipt = self.call("POST", "/v2/receive", receipt_body)[1]
        self.assertTrue(receipt["receiver_persisted"])
        self.assertFalse(receipt["receiver_acknowledged"])
        self.call("POST", "/v2/report", dict(msg_id=msg["msg_id"], attempt=envelope["attempt"], state="injected"))
        ack = self.call("POST", "/v2/ack", dict(msg_id=msg["msg_id"], session_key=b["session_key"]))[1]
        self.assertTrue(ack["receiver_acknowledged"])
        self.assertEqual(self.call("GET", "/v2/status?" + query)[1]["state"], "injected")
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
        self.assertEqual(self.call("GET", "/v1/health", authorized=False)[1]["capabilities"], ["durable_receipts"])
        self.assertEqual(self.call("GET", "/v2/inbox?session_key=x&limit=-1")[0], 400)
        self.assertEqual(self.call("GET", "/v2/history?msg_id=x&session_key=x&after=nan")[0], 400)
        observed = threading.Event()
        with patch.object(self.server.hub, "sweep2", side_effect=observed.set):
            self.server._next_gc = 0
            self.assertTrue(observed.wait(2), "maintenance must run with no new HTTP traffic")


if __name__ == "__main__":
    unittest.main()

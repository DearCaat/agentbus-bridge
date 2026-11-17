"""Isolated protocol-2 integration: real HTTP Hub, Node socket, MCP stdio, fake harnesses.

All configuration, SQLite files, fake credentials and harness sinks are temporary.
No production address, messaging socket, Codex binary or service is contacted.
"""
import hashlib
import json
import os
import tempfile
import threading
import unittest
import uuid
from unittest import mock

from agentbus.hub import make_server
from agentbus.node import Node
from test_mcp import MCP
from test_node import FakeClaudeSocket, claude_contents, wait_for, write_fake_codex


class TestEndToEndV2(unittest.TestCase):
    def setUp(self):
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        for name in ("AGENTBUS_HOME", "AGENTBUS_CONFIG", "AGENTBUS_MACHINE"):
            os.environ.pop(name, None)
        temp = tempfile.TemporaryDirectory(prefix="ab-e2e-", dir="/tmp")
        self.addCleanup(temp.cleanup)
        self.root = temp.name
        self.home = os.path.join(self.root, "node")
        os.mkdir(self.home)
        token = "isolated-test-" + uuid.uuid4().hex
        with open(os.path.join(self.home, "token"), "w", encoding="utf-8") as handle:
            handle.write(token)
        self.server = make_server(os.path.join(self.root, "hub.sqlite"), token, port=0)
        self.http_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.http_thread.start()
        self.addCleanup(self.close_hub)
        self.claude = FakeClaudeSocket(os.path.join(self.root, "fake-claude.sock"))
        self.addCleanup(self.claude.close)
        self.codex_output = os.path.join(self.root, "fake-codex.jsonl")
        codex_bin = write_fake_codex(self.root, self.codex_output)
        cfg = {"machine": "isolated", "hub_url": "http://127.0.0.1:%d" % self.server.server_address[1],
               "codex_bin": codex_bin}
        with open(os.path.join(self.home, "config.json"), "w", encoding="utf-8") as handle:
            json.dump(cfg, handle)
        self.node = Node(self.home, poll_wait=0.1, heartbeat_interval=3600)
        self.node.start()
        self.addCleanup(self.node.stop)
        self.thread_id = str(uuid.uuid4())
        self.meta = {"threadId": self.thread_id}
        self.sender = MCP(self.home, extra_env={"CODEX_HOME": os.path.join(self.root, "fake-codex-home")})
        self.addCleanup(self.sender.close)
        self.receiver = MCP(self.home, extra_env={
            "CLAUDE_CODE_SESSION_ID": str(uuid.uuid4()),
            "CLAUDE_CODE_MESSAGING_SOCKET": self.claude.path,
            "CLAUDE_CODE_MESSAGING_TOKEN": "isolated-fake-harness-auth",
        })
        self.addCleanup(self.receiver.close)
        self.sender_info = self.call(self.sender, "agentbus_whoami", meta=self.meta)
        self.receiver_info = self.call(self.receiver, "agentbus_whoami")
        self.assertTrue(self.sender_info["registered"])
        self.assertTrue(self.receiver_info["registered"])
        self.assertNotEqual(self.sender_info["harness"], self.receiver_info["harness"])

    def close_hub(self):
        self.server.shutdown()
        self.http_thread.join(timeout=5)
        self.server.server_close()
        self.server.hub.close()

    def call(self, client, name, arguments=None, meta=None):
        error, text = client.tool(name, arguments, meta=meta)
        self.assertFalse(error, "%s: %s" % (name, text))
        return json.loads(text)

    def send_three(self):
        self.assertIn("durable_receipts", self.node.hub.health().get("capabilities", []))
        bodies = ["Message %s 完整正文\n" % i * 1500 + "FINAL %s" % i for i in (1, 2, 3)]
        sent = [self.call(self.sender, "agentbus_send", {
            "to": self.receiver_info["address"], "text": body, "request_id": "stable-%s" % i})
            for i, body in enumerate(bodies, 1)]
        self.assertEqual([x["seq"] for x in sent], [1, 2, 3])
        self.assertEqual([x["request_id"] for x in sent], ["stable-1", "stable-2", "stable-3"])
        self.assertTrue(all(x["submission"] == "accepted" for x in sent))
        return sent, bodies

    def sender_statuses(self, sent):
        return [self.call(self.sender, "agentbus_status", {"msg_id": item["msg_id"]}) for item in sent]

    def assert_full_inbox(self, sent, bodies):
        items, after = [], 0
        while True:
            page = self.call(self.receiver, "agentbus_inbox", {"after": after, "limit": 1})
            items.extend(page["items"])
            if not page["has_more"]:
                break
            self.assertGreater(page["next_cursor"], after)
            after = page["next_cursor"]
        self.assertEqual([x["msg_id"] for x in items], [x["msg_id"] for x in sent])
        self.assertEqual([x["text"] for x in items], bodies)
        self.assertEqual(self.node.store._query("PRAGMA synchronous")[0][0], 2)
        for message, body in zip(sent, bodies):
            row = self.node.store._query("SELECT envelope,received_at,receipt_sent FROM incoming2 WHERE msg_id=?",
                                         (message["msg_id"],))[0]
            saved = json.loads(row["envelope"])
            self.assertEqual(saved["text"], body)
            self.assertEqual(saved["sha256"], hashlib.sha256(body.encode("utf-8")).hexdigest())
            self.assertEqual(saved["text_bytes"], len(body.encode("utf-8")))
            self.assertIsNotNone(row["received_at"])
            self.assertEqual(row["receipt_sent"], 1)
        for message, body in zip(sent, bodies):
            fetched = self.call(self.receiver, "agentbus_message", {"msg_id": message["msg_id"]})
            self.assertEqual(fetched["text"], body)
        return items

    def test_three_messages_persist_and_inject_in_order_without_manual_ack(self):
        sent, bodies = self.send_three()
        self.assertTrue(wait_for(lambda: len(self.claude.payloads()) == 3), "native presentation requires no manual ACK")
        observed = claude_contents(self.claude.payloads())
        self.assertEqual(len(observed), 3)
        for index, (envelope, body) in enumerate(zip(observed, bodies), 1):
            self.assertIn(body, envelope)
            self.assertIn("seq=%s" % index, envelope)
            self.assertIn("Optional explicit read/receipt declaration", envelope)
            self.assertNotIn("After reading the full envelope, explicitly call", envelope)
        self.assert_full_inbox(sent, bodies)
        statuses = self.sender_statuses(sent)
        for status in statuses:
            self.assertTrue(status["receiver_persisted"])
            self.assertIsNotNone(status["received_at"])
            self.assertFalse(status["receiver_acknowledged"], "automatic receipt must never invent agent reading")
            self.assertEqual(status["injection_state"], "injected")
            self.assertIn("unknown", status["summary"])
            self.assertIsNone(status["agent_read"], "automatic storage must not invent a read declaration")
            self.assertIsNone(status["task_completion"])

        reply_body = "Reply to exactly message 1; no read declaration was needed."
        reply = self.call(self.receiver, "agentbus_send", {
            "to": self.sender_info["address"], "text": reply_body,
            "request_id": "stable-answer-1", "reply_to": sent[0]["msg_id"]})
        self.assertEqual(reply["reply_to"], sent[0]["msg_id"])
        self.assertEqual(reply["seq"], 1)
        retrieved = self.call(self.sender, "agentbus_message", {"msg_id": reply["msg_id"]})
        self.assertEqual(retrieved["reply_to"], sent[0]["msg_id"])
        self.assertEqual(retrieved["text"], reply_body)
        self.assertTrue(wait_for(lambda: os.path.exists(self.codex_output) and os.path.getsize(self.codex_output) > 0))
        with open(self.codex_output, encoding="utf-8") as handle:
            native_reply = json.loads(handle.readline())["argv"][-1]
        self.assertIn(reply_body, native_reply)
        self.assertIn("reply_to=" + sent[0]["msg_id"], native_reply)
        self.assertTrue(all(not x["receiver_acknowledged"] for x in self.sender_statuses(sent)))

        # A late optional declaration must not roll native success back or hold successors.
        ack = self.call(self.receiver, "agentbus_ack", {"msg_id": sent[0]["msg_id"]})
        self.assertTrue(ack["receiver_acknowledged"])
        after_ack = self.sender_statuses(sent)
        self.assertTrue(after_ack[0]["receiver_acknowledged"])
        self.assertEqual([x["injection_state"] for x in after_ack], ["injected"] * 3)
        self.assertEqual(len(self.claude.payloads()), 3)

    def test_uncertain_native_predecessor_keeps_complete_durable_inbox(self):
        real_inject = self.node._inject
        def lose_native_result(message):
            outcome, detail = real_inject(message)
            self.assertEqual(outcome, "delivered", detail)
            return "failed", "in_doubt: synthetic lost native socket result"
        with mock.patch.object(self.node, "_inject", side_effect=lose_native_result):
            sent, bodies = self.send_three()
            self.assertTrue(wait_for(lambda: all(x.get("receiver_persisted") for x in self.sender_statuses(sent))))
            items = self.assert_full_inbox(sent, bodies)
            self.assertTrue(wait_for(lambda: self.sender_statuses(sent)[0].get("injection_state") == "in_doubt"))
            self.assertFalse(wait_for(lambda: len(self.claude.payloads()) > 1, timeout=0.5),
                             "uncertain native input must never be blindly repeated or skipped")
            self.assertEqual(len(self.claude.payloads()), 1)
            self.assertIn(bodies[0], claude_contents(self.claude.payloads())[0])
            for later in items[1:]:
                self.assertEqual(later["blocked_by_delivery"], sent[0]["msg_id"])
                self.assertIn("text", later)
            statuses = self.sender_statuses(sent)
            self.assertTrue(all(x["receiver_persisted"] for x in statuses))
            self.assertTrue(all(not x["receiver_acknowledged"] for x in statuses))
            self.call(self.receiver, "agentbus_ack", {"msg_id": sent[0]["msg_id"]})
            self.node.reliable().recover(force=True)
            self.assertFalse(wait_for(lambda: len(self.claude.payloads()) > 1, timeout=0.5),
                             "optional declaration must not skip an uncertain native predecessor")
            first = self.sender_statuses(sent)[0]
            self.assertEqual(first["injection_state"], "in_doubt")
            self.assertTrue(first["receiver_acknowledged"])


if __name__ == "__main__":
    unittest.main()

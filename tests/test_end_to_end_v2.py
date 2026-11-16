"""Isolated protocol-2 integration: real HTTP Hub, Node socket, MCP stdio, fake harnesses.

All configuration, SQLite files, fake credentials and harness sinks are temporary.
No production address, messaging socket, Codex binary or service is contacted.
"""
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

    def test_ack_controls_native_delivery_and_retrieval_and_reply_association(self):
        body_a = "A 完整正文\n" * 2000 + "A FINAL LINE"
        body_b = "B 暂不可见正文\n" * 2000 + "B FINAL LINE"
        target = self.receiver_info["address"]
        sent_a = self.call(self.sender, "agentbus_send", {
            "to": target, "text": body_a, "request_id": "stable-a"})
        sent_b = self.call(self.sender, "agentbus_send", {
            "to": target, "text": body_b, "request_id": "stable-b"})
        self.assertEqual(sent_a["submission"], "accepted")
        self.assertEqual(sent_b["submission"], "accepted")
        self.assertEqual(sent_a["request_id"], "stable-a")
        self.assertEqual(sent_b["request_id"], "stable-b")
        self.assertEqual((sent_a["seq"], sent_b["seq"]), (1, 2))
        self.assertEqual(sent_b["blocked_by"], sent_a["msg_id"])
        self.assertTrue(wait_for(lambda: len(self.claude.payloads()) == 1))
        observed_a = claude_contents(self.claude.payloads())
        self.assertEqual(len(observed_a), 1)
        self.assertIn(body_a, observed_a[0])
        self.assertNotIn(body_b, observed_a[0])
        self.assertIn('agentbus_ack(msg_id="%s")' % sent_a["msg_id"], observed_a[0])

        message_a = self.call(self.receiver, "agentbus_message", {"msg_id": sent_a["msg_id"]})
        self.assertEqual(message_a["text"], body_a)
        message_b = self.call(self.receiver, "agentbus_message", {"msg_id": sent_b["msg_id"]})
        self.assertFalse(message_b["content_visible"])
        self.assertNotIn("text", message_b)
        self.assertEqual(message_b["blocked_by"], sent_a["msg_id"])
        inbox = self.call(self.receiver, "agentbus_inbox", {"limit": 1})
        self.assertTrue(inbox["has_more"])
        self.assertEqual(inbox["items"][0]["text"], body_a)
        second_page = self.call(self.receiver, "agentbus_inbox", {"after": inbox["next_cursor"], "limit": 1})
        self.assertEqual(second_page["items"][0]["msg_id"], sent_b["msg_id"])
        self.assertFalse(second_page["items"][0]["content_visible"])
        self.assertNotIn("text", second_page["items"][0])
        status_a = self.call(self.sender, "agentbus_status", {"request_id": "stable-a"})
        self.assertFalse(status_a["receiver_acknowledged"])
        # Several actual polling cycles must not inject B after either retrieval path.
        self.assertFalse(wait_for(lambda: len(self.claude.payloads()) > 1, timeout=0.5))

        ack = self.call(self.receiver, "agentbus_ack", {"msg_id": sent_a["msg_id"]})
        self.assertTrue(ack["receiver_acknowledged"])
        self.assertTrue(wait_for(lambda: len(self.claude.payloads()) == 2))
        observed_b = claude_contents(self.claude.payloads())[1]
        self.assertIn(body_b, observed_b)
        self.assertIn("seq=2", observed_b)
        visible_b = self.call(self.receiver, "agentbus_message", {"msg_id": sent_b["msg_id"]})
        self.assertEqual(visible_b["text"], body_b)
        status_a = self.call(self.sender, "agentbus_status", {"msg_id": sent_a["msg_id"]})
        self.assertEqual(status_a["state"], "acknowledged")
        self.assertTrue(status_a["receiver_acknowledged"])
        self.assertIn("task completion", status_a["summary"])
        self.assertIn("unknown", status_a["summary"])

        # Opposite-direction reply does not need to wait for B's ACK.
        reply_body = "Reply to exactly A, while B remains unacknowledged."
        reply = self.call(self.receiver, "agentbus_send", {
            "to": self.sender_info["address"], "text": reply_body,
            "request_id": "stable-answer-a", "reply_to": sent_a["msg_id"]})
        self.assertEqual(reply["reply_to"], sent_a["msg_id"])
        self.assertEqual(reply["seq"], 1)
        retrieved = self.call(self.sender, "agentbus_message", {"msg_id": reply["msg_id"]})
        self.assertEqual(retrieved["reply_to"], sent_a["msg_id"])
        self.assertEqual(retrieved["text"], reply_body)
        self.assertTrue(wait_for(lambda: os.path.exists(self.codex_output) and os.path.getsize(self.codex_output) > 0))
        with open(self.codex_output, encoding="utf-8") as handle:
            native_reply = json.loads(handle.readline())["argv"][-1]
        self.assertIn(reply_body, native_reply)
        self.assertIn("reply_to=" + sent_a["msg_id"], native_reply)
        self.assertFalse(self.call(self.sender, "agentbus_status", {"msg_id": sent_b["msg_id"]})["receiver_acknowledged"])


if __name__ == "__main__":
    unittest.main()

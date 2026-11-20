import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from agentbus import mcp_server
from agentbus.hub import Hub
from agentbus.node import Node, Store


class SessionMetadataTest(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def test_node_refreshes_codex_metadata_and_exposes_it_through_mcp(self):
        state = sqlite3.connect(self.directory / "state_5.sqlite")
        self.addCleanup(state.close)
        state.execute("CREATE TABLE threads (id TEXT, model TEXT, reasoning_effort TEXT)")
        state.executemany("INSERT INTO threads VALUES (?,?,?)", [
            ("thread-1", "gpt/example", "xhigh"), ("other", "wrong-model", "low"),
        ])
        state.commit()
        hub = Hub(str(self.directory / "hub.db"))
        self.addCleanup(hub.close)
        node = Node(home=str(self.directory))
        node.codex_home = str(self.directory)
        node.machine = "testbox"
        node.store = Store(str(self.directory / "node.db"))
        self.addCleanup(node.store.close)
        node.hub = Mock(wraps=hub)
        node.hub.heartbeat.side_effect = lambda machine, live: hub.heartbeat({"machine": machine, "live": live})
        pid = os.getpid()
        response = node._op_register({
            "session_id": "thread-1", "harness": "codex",
            "deliver": {"codex_home": str(self.directory), "thread_id": "thread-1"},
        }, pid)
        session = mcp_server.Session()
        session.reg = response
        with patch.object(mcp_server, "node_call", side_effect=lambda req: node._op_peers(req, pid)):
            self.assertEqual(mcp_server.tool_whoami(session, {})["model_name"], "gpt/example")
            self.assertEqual(mcp_server.tool_whoami(session, {})["effort"], "xhigh")
            self.assertIn("gpt/example", mcp_server.tool_peers(session, {}))
        state.execute("UPDATE threads SET model='new-model', reasoning_effort=NULL WHERE id='thread-1'")
        state.commit()
        node.heartbeat_once()
        [peer] = hub.peers()["peers"]
        self.assertEqual(peer["model_name"], "new-model")
        self.assertIsNone(peer["effort"])

    def test_claude_uses_observed_model_and_per_turn_effort(self):
        from agentbus.session_metadata import session_metadata

        path = self.directory / "projects" / "-some-project" / "exact.jsonl"
        path.parent.mkdir(parents=True)
        last = {
            "type": "assistant", "sessionId": "exact", "requestedModel": "alias",
            "message": {"model": "claude-example"}, "effort": "low", "perTurnEffort": "high",
        }
        path.write_text("\n".join(json.dumps(row) for row in (
            last, {**last, "sessionId": "other", "message": {"model": "wrong"}},
            {**last, "message": {"model": "<synthetic>"}},
        )) + "\n" + '{"partial":')
        deliver = {"claude_config_dir": str(self.directory)}
        self.assertEqual(session_metadata("claude", "exact", "/some/project", deliver), {
            "model_name": "claude-example", "effort": "high",
        })
        self.assertEqual(session_metadata("claude", "unknown", "/some/project", deliver), {})

    def test_existing_hub_database_and_old_node_registration(self):
        database = str(self.directory / "hub.db")
        legacy = Hub(database)
        body = {"machine": "testbox", "harness": "claude", "session_id": "existing"}
        legacy.register(body)
        legacy.conn.execute("ALTER TABLE sessions DROP COLUMN effort")
        legacy.conn.execute("ALTER TABLE sessions DROP COLUMN model_name")
        legacy.close()
        hub = Hub(database)
        self.addCleanup(hub.close)
        self.assertIsNone(hub.peers()["peers"][0]["model_name"])
        hub.register({**body, "model_name": "claude-example", "effort": "max"})
        hub.register(body)
        self.assertEqual(hub.peers()["peers"][0]["effort"], "max")

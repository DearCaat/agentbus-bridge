import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from agentbus.node import Node, Store


class CodexPresenceTests(TestCase):
    def test_heartbeat_registers_and_retires_loaded_thread(self):
        with TemporaryDirectory() as directory:
            node = Node(home=directory)
            node.codex_home = directory
            node.machine = "docker3"
            node.store = Store(str(Path(directory) / "node.db"))
            self.addCleanup(node.store.close)
            node.hub = mock.Mock()
            thread = "01a1ed60-9329-74a2-96f0-1b2915d60590"
            key = f"docker3:codex:{thread}"
            node.hub.register.return_value = {"session_key": key, "address": "docker3/codex/01a1ed60"}
            socket = Path(directory) / "app-server-control" / "app-server-control.sock"
            socket.parent.mkdir()
            socket.touch()
            with mock.patch("agentbus.codex_presence.loaded_threads", side_effect=[
                (os.getpid(), {thread: "/app"}), (os.getpid(), {}), (os.getpid(), {thread: "/app"}),
            ]):
                self.assertEqual(node.heartbeat_once(), [key])
                self.assertEqual(node.store.get_session(key)["deliver"]["thread_id"], thread)
                self.assertEqual(node.heartbeat_once(), [])
                self.assertEqual(node.heartbeat_once(), [key])

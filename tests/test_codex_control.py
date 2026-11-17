"""Control admission/outcome tests; no live daemon reload, tasks, or LLM calls.

The native WebSocket framing is the optional library's responsibility. These
tests use a real private Unix socket/peer identity and a fake JSON-RPC peer.
"""
import json
import os
from pathlib import Path
import socket
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from agentbus import codex_control as control


TID = "01a1ced1-883d-7e63-93d8-38d82303106c"
OTHER = "01a1ced1-883d-7e63-93d8-38d82303106d"
TOOLS = {"agentbus_send", "agentbus_control", "agentbus_control_status"}


class NativePeer:
    def __init__(self, case):
        self.case, self.calls, self.queue = case, [], []
        self.reloaded, self.marker = False, "baseline-generation"

    def settimeout(self, _):
        pass

    def close(self, **_):
        pass

    def send(self, raw):
        msg = json.loads(raw)
        self.calls.append(msg)
        method = msg["method"]
        if method == self.case.disconnect_method and (method != 'mcpServerStatus/list' or self.reloaded):
            raise TimeoutError("private native failure must not leak")
        if method == 'mcpServerStatus/list' and self.case.disconnect_baseline and not self.reloaded:
            raise TimeoutError('baseline private failure')
        if "id" not in msg:
            return
        if method == self.case.rpc_error_method:
            self.queue.append({"id": msg["id"], "error": {"code": -32603, "message": "native-secret"}})
            return
        if method == "initialize":
            value = {"codexHome": str(self.case.home)}
            self.queue.append({"method": "account/updated", "params": {"token": "notification-secret"}})
        elif method == "thread/loaded/list":
            if self.case.reuse_pid:
                self.case.reused = True
            if self.case.paginated and "cursor" not in msg["params"]:
                value = {"data": [OTHER], "nextCursor": "next"}
            else:
                value = {"data": [TID] if self.case.loaded else [OTHER], "nextCursor": None}
        elif method == "config/value/write":
            if self.case.marker_status == 'ok' and not self.case.ignore_marker:
                self.marker = msg['params']['value']
            value = {"filePath": str(self.case.home / 'config.toml'), "status": self.case.marker_status, "version": "fixture-version"}
        elif method == "config/mcpServer/reload":
            if self.case.native_error:
                self.queue.append({"id": msg["id"], "error": {"code": -32603, "message": "native-secret"}})
                return
            self.reloaded = True
            value = {}
        elif method == "mcpServerStatus/list":
            changed = self.reloaded and not self.case.ignore_marker and not self.case.same_instance and self.case.marker_status == 'ok'
            metadata = {'runtime_id': 'fresh-instance' if changed else 'baseline-instance', 'reload_generation': self.marker}
            capabilities = {'experimental': {'agentbus': metadata}} if self.case.runtime_metadata and (self.reloaded or self.case.baseline_metadata) else {}
            value = {"data": [{"name": "agentbus", "runtimeStatus": self.case.runtime_status,
                              "serverInfo": {"name": "agentbus", "version": self.case.version},
                              "serverCapabilities": capabilities,
                              "tools": {name: {"name": name, "inputSchema": {}} for name in self.case.tools},
                              "toolsError": None}], "nextCursor": None}
        else:
            raise AssertionError(method)
        self.queue.append({"id": msg["id"], "result": value})

    def recv(self):
        return json.dumps(self.queue.pop(0))


class CodexControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="agentbus-codex-control-")
        self.root = Path(self.temp.name)
        self.home = self.root / "codex-home"
        self.home.mkdir(mode=0o700)
        self.endpoint = self.root / "control.sock"
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.endpoint))
        self.listener.listen(4)
        self.endpoint.chmod(0o600)
        self.pid = os.getpid()
        self.mcp_pid = 987654321
        self.version, self.tools, self.runtime_status = "0.4.0", set(TOOLS), "connected"
        self.loaded, self.paginated, self.native_error = True, False, False
        self.disconnect_method = None
        self.rpc_error_method = None
        self.marker_status = "ok"
        self.ignore_marker = self.same_instance = self.disconnect_baseline = False
        self.runtime_metadata = self.baseline_metadata = True
        self.reuse_pid, self.reused = False, False
        self.peer = NativePeer(self)
        self.session = dict(session_key="test:codex:" + TID, harness="codex", session_id=TID,
                            deliver={"thread_id": TID, "codex_home": str(self.home)}, mcp_pid=self.mcp_pid)
        self.patches = [
            patch.object(control, "_process", side_effect=self.process),
            patch.object(control, "_expected_catalog", return_value=("0.4.0", set(TOOLS))),
            patch.object(control.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=json.dumps({"status": "running", "socketPath": str(self.endpoint)}))),
            patch.object(control.importlib, "import_module", return_value=SimpleNamespace(create_connection=lambda *a, **kw: self.peer)),
        ]
        self.started = [p.start() for p in self.patches]

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.listener.close()
        self.temp.cleanup()

    def process(self, pid):
        if pid == self.mcp_pid:
            return dict(pid=pid, ppid=self.pid, state="S", starttime="changed" if self.reused else "mcp-start",
                        uid=os.geteuid(), argv=["python3", "-m", "agentbus", "mcp"])
        if pid == self.pid:
            return dict(pid=pid, ppid=1, state="S", starttime="server-start", uid=os.geteuid(),
                        argv=["codex", "app-server", "--listen", "unix://"], CODEX_HOME=str(self.home), HOME=str(self.root))
        raise AssertionError(pid)

    def run_control(self, scope="server"):
        return control.run(self.session, scope, timeout=2)

    def mutations(self):
        return [c for c in self.peer.calls if c["method"] == "config/mcpServer/reload"]

    def writes(self):
        return [c for c in self.peer.calls if c["method"] == "config/value/write"]

    def test_verified_is_target_catalog_only_after_exact_global_reload(self):
        out = self.run_control()
        self.assertEqual(out["state"], "verified")
        r = out["result"]
        self.assertEqual(r["effective_scope"], "server")
        self.assertEqual(r["affected_servers"], ["*"])
        self.assertTrue(r["native_accepted"] and r["applied"] and r["verified"])
        self.assertEqual(r["verification"]["scope"], "target_thread")
        self.assertEqual(r["verification"]["thread_id"], TID)
        self.assertEqual(r["verification"]["server"], "agentbus")
        self.assertEqual(len(self.mutations()), 1)
        self.assertIsNone(self.mutations()[0]["params"])
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(set(self.writes()[0]["params"]), {"keyPath", "value", "mergeStrategy"})
        self.assertEqual(self.writes()[0]["params"]["keyPath"], "mcp_servers.agentbus.env.AGENTBUS_RELOAD_GENERATION")
        self.assertEqual(self.writes()[0]["params"]["mergeStrategy"], "replace")
        self.assertNotIn(self.writes()[0]["params"]["value"], json.dumps(out))
        status = next(c for c in self.peer.calls if c["method"] == "mcpServerStatus/list")
        self.assertEqual(status["params"], {"threadId": TID, "serverName": "agentbus", "detail": "toolsAndAuthOnly"})
        self.assertIn("only the target thread", r["summary"])
        self.assertNotIn("notification-secret", json.dumps(out))
        self.assertNotIn('fresh-instance', json.dumps(out))
        self.assertTrue(r['verification']['generation_matches'])
        self.assertTrue(r['verification']['runtime_instance_changed'])

    def test_session_scope_has_zero_discovery_or_native_effects(self):
        self.assertEqual(self.run_control("session")["state"], "unsupported")
        self.started[0].assert_not_called()
        self.started[2].assert_not_called()
        self.started[3].assert_not_called()
        self.assertEqual(self.peer.calls, [])

    def test_wrong_harness_and_thread_identity_are_rejected(self):
        self.session["harness"] = "claude"
        self.assertEqual(self.run_control()["state"], "unsupported")
        self.session["harness"] = "codex"
        self.session["deliver"]["thread_id"] = OTHER
        self.assertEqual(self.run_control()["result"]["code"], "invalid_local_registration")
        self.assertEqual(self.peer.calls, [])

    def test_missing_optional_dependency_does_not_execute(self):
        self.started[3].side_effect = ImportError("private library path")
        r = self.run_control()
        self.assertEqual(r["result"]["code"], "missing_dependency")
        self.assertEqual(r["state"], "unsupported")
        self.started[0].assert_not_called()
        self.assertNotIn("private library path", json.dumps(r))

    def test_private_socket_permissions_are_required(self):
        self.endpoint.chmod(0o666)
        self.assertEqual(self.run_control()["result"]["code"], "unsafe_control_socket")
        self.assertEqual(self.peer.calls, [])

    def test_explicit_private_socket_from_runtime_needs_no_daemon_command(self):
        original = self.process
        self.started[0].side_effect = lambda pid: dict(original(pid), argv=["codex", "app-server", "--listen", "unix://" + str(self.endpoint)]) if pid == self.pid else original(pid)
        self.assertEqual(self.run_control()["state"], "verified")
        self.started[2].assert_not_called()

    def test_mismatched_registered_home_is_not_used_as_authority(self):
        self.session["deliver"]["codex_home"] = "/another-space"
        self.assertEqual(self.run_control()["result"]["code"], "codex_home_identity_mismatch")
        self.started[2].assert_not_called()

    def test_zombie_registration_is_not_a_live_runtime(self):
        original = self.process
        self.started[0].side_effect = lambda pid: dict(original(pid), state="Z") if pid == self.mcp_pid else original(pid)
        self.assertEqual(self.run_control()["result"]["code"], "stale_local_registration")
        self.started[2].assert_not_called()

    def test_standalone_without_managed_server_is_unsupported(self):
        self.started[0].side_effect = lambda pid: dict(pid=pid, ppid=1, state="S", starttime="x", uid=os.geteuid(), argv=["codex"])
        self.assertEqual(self.run_control()["result"]["code"], "no_managed_app_server")
        self.assertEqual(self.peer.calls, [])

    def test_connected_peer_must_be_the_registered_ancestor(self):
        self.pid = 987654322
        self.assertEqual(self.run_control()["result"]["code"], "control_socket_peer_mismatch")
        self.assertEqual(self.peer.calls, [])

    def test_process_reuse_between_loaded_check_and_mutation_is_rejected(self):
        self.reuse_pid = True
        self.assertEqual(self.run_control()["result"]["code"], "runtime_identity_changed")
        self.assertEqual(self.mutations(), [])

    def test_exact_target_must_already_be_loaded(self):
        self.loaded = False
        r = self.run_control()
        self.assertEqual(r["result"]["code"], "target_not_loaded")
        self.assertEqual(r["state"], "unsupported")
        self.assertEqual(self.mutations(), [])

    def test_loaded_list_pagination_is_followed(self):
        self.paginated = True
        self.assertEqual(self.run_control()["state"], "verified")
        pages = [c for c in self.peer.calls if c["method"] == "thread/loaded/list"]
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[1]["params"]["cursor"], "next")

    def test_sender_socket_and_command_fields_are_ignored(self):
        self.session["deliver"].update(socket="/untrusted/socket", command="untrusted-command")
        self.assertEqual(self.run_control()["state"], "verified")
        args, kw = self.started[2].call_args
        self.assertEqual(args[0], ["codex", "app-server", "daemon", "version"])
        self.assertEqual(kw["env"]["CODEX_HOME"], str(self.home))

    def test_empty_native_result_is_not_sufficient_for_version(self):
        self.version = "0.1.0"
        r = self.run_control()
        self.assertEqual(r["state"], "applied_unverified")
        self.assertTrue(r["result"]["native_accepted"])
        self.assertFalse(r["result"]["verified"])
        self.assertEqual(len(self.mutations()), 1)

    def test_control_tools_must_be_in_the_target_runtime(self):
        self.tools = {"agentbus_send"}
        self.assertEqual(self.run_control()["state"], "applied_unverified")

    def test_starting_runtime_is_not_verified(self):
        self.runtime_status = "starting"
        self.assertEqual(self.run_control()["state"], "applied_unverified")

    def test_overridden_marker_never_edits_the_overriding_layer(self):
        self.marker_status = "okOverridden"
        r = self.run_control()
        self.assertEqual(r["state"], "applied_unverified")
        self.assertEqual(r["result"]["observed"]["marker_write_status"], "okOverridden")
        self.assertEqual(len(self.writes()), 1)
        self.assertNotIn("filePath", self.writes()[0]["params"])
        self.assertEqual(len(self.mutations()), 1)

    def test_project_override_cannot_verify_even_when_version_and_tools_match(self):
        self.ignore_marker = True
        r = self.run_control()
        self.assertEqual(r['state'], 'applied_unverified')
        self.assertFalse(r['result']['verification']['generation_matches'])
        self.assertFalse(r['result']['verification']['runtime_instance_changed'])
        self.assertEqual(len(self.writes()), 1)

    def test_matching_generation_without_new_instance_is_not_verified(self):
        self.same_instance = True
        r = self.run_control()
        self.assertEqual(r['state'], 'applied_unverified')
        self.assertTrue(r['result']['verification']['generation_matches'])
        self.assertFalse(r['result']['verification']['runtime_instance_changed'])

    def test_absent_runtime_metadata_does_not_downgrade_verification(self):
        self.runtime_metadata = False
        self.assertEqual(self.run_control()['state'], 'applied_unverified')

    def test_legacy_baseline_without_metadata_can_be_replaced(self):
        self.baseline_metadata = False
        self.assertEqual(self.run_control()['state'], 'verified')

    def test_baseline_failure_never_writes_or_reloads(self):
        self.disconnect_baseline = True
        self.assertEqual(self.run_control()['state'], 'failed')
        self.assertEqual(self.writes(), [])
        self.assertEqual(self.mutations(), [])

    def test_marker_write_lost_response_is_not_retried_and_never_reloads(self):
        self.disconnect_method = "config/value/write"
        r = self.run_control()
        self.assertEqual(r["state"], "in_doubt")
        self.assertIsNone(r["result"]["applied"])
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.mutations(), [])

    def test_marker_write_rejection_is_uncertain_and_never_reloads(self):
        self.rpc_error_method = "config/value/write"
        r = self.run_control()
        self.assertEqual(r["state"], "in_doubt")
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.mutations(), [])
        self.assertNotIn("native-secret", json.dumps(r))

    def test_disconnect_during_mutation_is_in_doubt_without_retry(self):
        self.disconnect_method = "config/mcpServer/reload"
        r = self.run_control()
        self.assertEqual(r["state"], "in_doubt")
        self.assertIsNone(r["result"]["applied"])
        self.assertFalse(r["result"]["native_accepted"])
        self.assertEqual(len(self.mutations()), 1)

    def test_verification_disconnect_preserves_native_acceptance(self):
        self.disconnect_method = "mcpServerStatus/list"
        r = self.run_control()
        self.assertEqual(r["state"], "applied_unverified")
        self.assertTrue(r["result"]["native_accepted"] and r["result"]["applied"])
        self.assertFalse(r["result"]["verified"])
        self.assertEqual(len(self.mutations()), 1)

    def test_native_rejection_can_have_partial_effects_and_no_error_leak(self):
        self.native_error = True
        r = self.run_control()
        self.assertEqual(r["state"], "in_doubt")
        self.assertIsNone(r["result"]["applied"])
        self.assertEqual(r["result"]["observed"]["native_error_code"], -32603)
        self.assertNotIn("native-secret", json.dumps(r))
        self.assertEqual(len(self.mutations()), 1)


if __name__ == "__main__":
    unittest.main()

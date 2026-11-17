"""Independent control jobs: no native actions, real durable state/fault checks."""
import json
import http.client
import io
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from agentbus.hub import ApiError, Hub, HubServer


class HubControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="agentbus-control-hub-")
        self.path = os.path.join(self.temp.name, "hub.db")
        self.hub = Hub(self.path)
        self.a = self.hub.register(dict(machine="sender", harness="claude", session_id="caller"))
        self.b = self.hub.register(dict(machine="target", harness="codex", session_id="target"))
        self.c = self.hub.register(dict(machine="other", harness="claude", session_id="other"))

    def tearDown(self):
        self.hub.close()
        self.temp.cleanup()

    def submit(self, **changes):
        body = dict(from_key=self.a["session_key"], to=self.b["session_key"], request_id="stable-request",
                    action="reload_mcp", scope="server")
        body.update(changes)
        return self.hub.control_submit2(body)

    def claim(self, owner="node-instance"):
        return self.hub.control_pull2("target", owner)[0]

    def begin(self, claimed):
        return self.hub.control_begin2({k: claimed[k] for k in ("control_id", "owner", "attempt")})

    def report(self, claimed, state, result=None):
        return self.hub.control_report2(dict(control_id=claimed["control_id"], owner=claimed["owner"],
                                             attempt=claimed["attempt"], state=state,
                                             result={"code": state} if result is None else result))

    def test_lost_submit_response_idempotency_and_conflict(self):
        first, retried = self.submit(), self.submit()
        self.assertEqual(first["control_id"], retried["control_id"])
        self.assertEqual(first["state"], "queued")
        self.assertFalse(first["execution_confirmed"])
        for changes in (dict(scope="session"), dict(action="reload_plugins"), dict(to=self.c["session_key"])):
            with self.subTest(changes=changes), self.assertRaises(ApiError) as error:
                self.submit(**changes)
            self.assertEqual(error.exception.status, 409)

    def test_executing_job_never_reoffered_after_timeout_or_restart(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            self.submit()
            claimed = self.claim()
            started = self.begin(claimed)
            self.assertEqual(started["state"], "executing")
            self.assertEqual(self.begin(claimed)["state_revision"], started["state_revision"])
        self.hub.close()
        self.hub = Hub(self.path)
        with patch("agentbus.hub.time.time", return_value=1000 + 40 * 86400):
            self.hub.sweep2()
            self.assertEqual(self.hub.control_pull2("target", "new-node"), [])
            result = self.report(claimed, "in_doubt")
        self.assertIsNone(result["execution_confirmed"])
        self.assertFalse(result["verification_confirmed"])

    def test_scope_and_offline_preflight_can_report_without_execution(self):
        job = self.submit(scope="session")
        self.hub.heartbeat(dict(machine="target", live=[]))
        claimed = self.claim()
        result = self.report(claimed, "unsupported", {"code": "scope_not_supported", "effective_scope": "server"})
        self.assertEqual(job["control_id"], result["control_id"])
        self.assertFalse(result["execution_confirmed"])
        self.assertFalse(result["verification_confirmed"])

    def test_fixed_allowlist_and_no_freeform_native_arguments(self):
        for change in (dict(action="shell"), dict(scope="machine"), dict(command="reload"), dict(args=[])):
            with self.subTest(change=change), self.assertRaises(ApiError):
                self.submit(**change)
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM control_jobs").fetchone()[0], 0)

    def test_canonical_target_retry_survives_name_change(self):
        first = self.submit(to=self.b["address"])
        self.hub.register(dict(machine="target", harness="codex", session_id="target", name="renamed"))
        self.assertEqual(self.submit()["control_id"], first["control_id"])
        self.assertEqual(self.submit(to="target/codex/renamed")["control_id"], first["control_id"])

    def test_lost_claim_response_reoffers_with_fencing_only_before_begin(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            self.submit()
            old = self.claim("old-node")
            self.assertEqual(self.hub.control_pull2("target", "other-node"), [])
        with patch("agentbus.hub.time.time", return_value=1121):
            new = self.claim("new-node")
            self.assertEqual(new["attempt"], old["attempt"] + 1)
            for call in (lambda: self.begin(old), lambda: self.report(old, "failed")):
                with self.assertRaises(ApiError) as error:
                    call()
                self.assertEqual(error.exception.code, "stale_attempt")
            self.begin(new)
        with patch("agentbus.hub.time.time", return_value=2000):
            self.assertEqual(self.hub.control_pull2("target", "yet-another-node"), [])
            self.assertEqual(self.begin(new)["state"], "executing")

    def test_queued_ttl_expires_without_execution(self):
        with patch("agentbus.hub.time.time", return_value=1000):
            msg = self.submit()
        with patch("agentbus.hub.time.time", return_value=1301):
            self.assertEqual(self.hub.control_pull2("target", "node"), [])
            status = self.hub.control_status2(msg["control_id"], self.a["session_key"])
        self.assertEqual(status["state"], "expired")
        self.assertFalse(status["execution_confirmed"])
        self.assertIsNone(status["started_at"])

    def test_native_outcomes_require_begin_and_consistent_verification(self):
        self.submit()
        claimed = self.claim()
        with self.assertRaises(ApiError):
            self.report(claimed, "verified", {"verified": True})
        self.begin(claimed)
        with self.assertRaises(ApiError):
            self.report(claimed, "unsupported")
        with self.assertRaises(ApiError):
            self.report(claimed, "verified", {"code": "only_socket_write"})
        with self.assertRaises(ApiError):
            self.report(claimed, "verified", {"verified": True, "native_accepted": False})
        with self.assertRaises(ApiError):
            self.report(claimed, "applied_unverified", {"native_accepted": False})
        result = self.report(claimed, "applied_unverified", {"native_accepted": True, "verified": False,
                             "effective_scope": "server", "affected_servers": ["agentbus"]})
        self.assertTrue(result["execution_confirmed"])
        self.assertFalse(result["verification_confirmed"])

    def test_adapter_can_declare_unsupported_after_begin_only_before_native_acceptance(self):
        self.submit()
        claimed = self.claim()
        self.begin(claimed)
        for result in ({"native_accepted": True, "verified": False}, {"native_accepted": False},
                       {"native_accepted": None, "verified": False}):
            with self.assertRaises(ApiError):
                self.report(claimed, "unsupported", result)
        result = self.report(claimed, "unsupported", {"code": "missing_dependency", "native_accepted": False, "verified": False})
        self.assertEqual(result["state"], "unsupported")
        self.assertFalse(result["execution_confirmed"])

    def test_terminal_result_replay_survives_broken_mcp_connection_and_gc(self):
        proof = {"native_accepted": True, "verified": True, "verification": {"code": "fresh_native_observation"}}
        with patch("agentbus.hub.time.time", return_value=1000):
            submitted = self.submit()
            claimed = self.claim()
            self.begin(claimed)
            self.hub.heartbeat(dict(machine="target", live=[]))
            first = self.report(claimed, "verified", proof)
        self.hub.close()
        self.hub = Hub(self.path)
        retried = self.report(claimed, "verified", dict(reversed(list(proof.items()))))
        self.assertEqual(retried["state_revision"], first["state_revision"])
        with self.assertRaises(ApiError):
            self.report(claimed, "failed")
        with patch("agentbus.hub.time.time", return_value=1000+31*86400):
            self.hub.sweep2()
            pruned = self.hub.control_status2(submitted["control_id"], self.a["session_key"])
            self.assertTrue(pruned["history_pruned"])
            self.assertTrue(pruned["result_pruned"])
            self.assertIsNone(pruned["result"])
            self.assertEqual(pruned["result_fingerprint"], first["result_fingerprint"])
            self.assertEqual(self.report(claimed, "verified", proof)["state_revision"], pruned["state_revision"])
            self.assertEqual(self.submit()["control_id"], submitted["control_id"])

    def test_control_mutations_and_events_are_atomic(self):
        real = self.hub._control_event
        def fail_after_event(*args):
            real(*args)
            raise RuntimeError("synthetic transaction failure")
        with patch.object(self.hub, "_control_event", side_effect=fail_after_event):
            with self.assertRaises(RuntimeError):
                self.submit()
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM control_jobs").fetchone()[0], 0)
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM control_events").fetchone()[0], 0)
        self.submit()
        claimed = self.claim()
        with patch.object(self.hub, "_control_event", side_effect=fail_after_event):
            with self.assertRaises(RuntimeError):
                self.begin(claimed)
        self.assertEqual(self.hub.control_status2(claimed["control_id"], self.a["session_key"])["state"], "leased")
        self.begin(claimed)
        with patch.object(self.hub, "_control_event", side_effect=fail_after_event):
            with self.assertRaises(RuntimeError):
                self.report(claimed, "verified", {"verified": True})
        self.assertEqual(self.hub.control_status2(claimed["control_id"], self.a["session_key"])["state"], "executing")
        self.assertEqual(self.report(claimed, "verified", {"verified": True})["state"], "verified")

    def test_status_is_participant_only_unknown_is_not_failed(self):
        job = self.submit()
        for key in (self.a["session_key"], self.b["session_key"]):
            self.assertEqual(self.hub.control_status2(job["control_id"], key)["state"], "queued")
        with self.assertRaises(ApiError) as error:
            self.hub.control_status2(job["control_id"], self.c["session_key"])
        self.assertEqual(error.exception.status, 403)
        unknown = self.hub.control_status2("no-such-control", self.a["session_key"])
        self.assertEqual(unknown["state"], "unknown_or_pruned")
        self.assertIsNone(unknown["execution_confirmed"])
        self.assertIsNone(unknown["verification_confirmed"])

    def test_result_is_bounded_safe_json_not_commands_or_raw_configs(self):
        self.submit()
        claimed = self.claim()
        for result in ("command", {"command": "reload"}, {"observed": {"auth_token": "never-store"}},
                       {"observed": {"raw": {}}}, {"observed": list(range(129))},
                       {"summary": "x" * 2049}, {"observed": float("inf")},
                       {"verified": "true"}, {"native_accepted": 1}, {"applied": True},
                       {"observed": {str(i): "x" * 2048 for i in range(10)}}):
            with self.subTest(result_type=type(result).__name__), self.assertRaises(ApiError):
                self.report(claimed, "failed", result)
        self.assertEqual(self.report(claimed, "failed", {"code": "target_offline", "harness": "codex"})["state"], "failed")

    def test_control_jobs_do_not_consume_or_unblock_message_fifo(self):
        msg_body = dict(from_key=self.a["session_key"], to=self.b["session_key"], text="ordinary message")
        first = self.hub.send2(dict(msg_body, client_msg_id="one", client_seq=1))
        second = self.hub.send2(dict(msg_body, client_msg_id="two", client_seq=2))
        self.submit()
        claimed = self.claim()
        self.begin(claimed)
        self.report(claimed, "verified", {"verified": True})
        self.assertEqual(self.hub.status2(second["msg_id"], self.b["session_key"])["blocked_by"], first["msg_id"])
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM messages_v2").fetchone()[0], 2)

    def test_real_http_control_routes_capability_and_safe_log_correlation(self):
        from urllib.parse import urlencode
        log = io.StringIO()
        server = HubServer(("127.0.0.1", 0), self.hub, "isolated-control-auth")
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        def call(method, path, body=None, auth=True):
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            headers = {"Content-Type": "application/json"}
            if auth:
                headers["Authorization"] = "Bearer isolated-control-auth"
            connection.request(method, path, json.dumps(body) if body is not None else None, headers)
            response = connection.getresponse()
            result = response.status, json.loads(response.read()), response.getheader("X-Request-ID")
            connection.close()
            return result
        with patch("sys.stderr", log):
            thread.start()
            try:
                capabilities = call("GET", "/v1/health", auth=False)[1]["capabilities"]
                self.assertEqual(capabilities, ["durable_receipts", "native_controls"])
                body = dict(from_key=self.a["session_key"], to=self.b["session_key"], request_id="http-request",
                            action="reload_mcp", scope="server")
                status, submitted, request_id = call("POST", "/v2/control/submit", body)
                self.assertEqual(status, 200)
                self.assertEqual(call("POST", "/v2/control/submit", body)[1]["control_id"], submitted["control_id"])
                claimed = call("GET", "/v2/control/pull?machine=target&owner=worker")[1]["controls"][0]
                phase = {key: claimed[key] for key in ("control_id", "attempt", "owner")}
                self.assertEqual(call("POST", "/v2/control/begin", phase)[1]["state"], "executing")
                proof = dict(native_accepted=True, verified=True, effective_scope="server", observed={"tools": ["agentbus_control"]})
                self.assertEqual(call("POST", "/v2/control/report", dict(phase, state="verified", result=proof))[1]["state"], "verified")
                query = urlencode(dict(control_id=claimed["control_id"], session_key=self.a["session_key"]))
                self.assertTrue(call("GET", "/v2/control/status?" + query)[1]["verification_confirmed"])
                self.assertEqual(call("GET", "/v2/control/status?" + query, auth=False)[0], 401)
                forbidden = dict(phase, state="failed", result={"observed": {"token": "never-log-this"}})
                self.assertEqual(call("POST", "/v2/control/report", forbidden)[0], 400)
            finally:
                server.shutdown()
                thread.join(2)
                server.server_close()
        records = [json.loads(line) for line in log.getvalue().splitlines()]
        correlated = next(row for row in records if row["request_id"] == request_id)
        self.assertEqual(correlated["control_id"], submitted["control_id"])
        for secret in ("isolated-control-auth", "never-log-this", "agentbus_control"):
            self.assertNotIn(secret, log.getvalue())


if __name__ == "__main__":
    unittest.main()

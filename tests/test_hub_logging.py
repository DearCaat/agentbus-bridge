"""Real Hub log rotation, concurrency, privacy and CLI lifecycle checks."""
import contextlib
import http.client
import io
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from agentbus import hub


class HubLoggingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="agentbus-hub-logs-")
        self.path = os.path.join(self.temp.name, "hub.log")

    def tearDown(self):
        self.temp.cleanup()

    def test_existing_oversize_file_preserved_before_first_new_line_and_private(self):
        old = (b"legacy log line\n" * 150)
        with open(self.path, "wb") as stream:
            stream.write(old)
        os.chmod(self.path, 0o644)
        log = hub.HubLog(self.path, max_bytes=128, backup_count=3)
        log.write(dict(event="new"))
        log.close()
        with open(self.path + ".1", "rb") as archive:
            self.assertEqual(archive.read(), old)
        with open(self.path, encoding="utf-8") as active:
            self.assertEqual(json.loads(active.read()), {"event": "new"})
        for path in (self.path, self.path + ".1"):
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)

    def test_concurrent_rotation_preserves_whole_json_records_and_exact_ids(self):
        log = hub.HubLog(self.path, max_bytes=512, backup_count=100)
        barrier = threading.Barrier(8)
        def writer(worker):
            barrier.wait()
            for sequence in range(25):
                log.write(dict(event="concurrent", request_id="%d:%d" % (worker, sequence)))
        threads = [threading.Thread(target=writer, args=(worker,)) for worker in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        log.close()
        records = []
        for name in os.listdir(self.temp.name):
            with open(os.path.join(self.temp.name, name), "rb") as stream:
                data = stream.read()
            self.assertTrue(data.endswith(b"\n"))
            records.extend(json.loads(line) for line in data.splitlines())
        self.assertEqual(len(records), 200)
        self.assertEqual({r["request_id"] for r in records},
                         {"%d:%d" % (w, s) for w in range(8) for s in range(25)})

    def test_archive_count_bound_and_inherited_backup_permissions(self):
        for suffix in ("", ".1", ".2", ".3"):
            with open(self.path + suffix, "w") as stream:
                stream.write("existing\n")
            os.chmod(self.path + suffix, 0o644)
        log = hub.HubLog(self.path, max_bytes=128, backup_count=3)
        for sequence in range(100):
            log.write(dict(event="bounded", request_id=str(sequence)))
        log.close()
        self.assertEqual(set(os.listdir(self.temp.name)), {"hub.log", "hub.log.1", "hub.log.2", "hub.log.3"})
        for name in os.listdir(self.temp.name):
            path = os.path.join(self.temp.name, name)
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
            self.assertLessEqual(os.stat(path).st_size, 128)

    def test_default_stderr_is_not_closed_and_configuration_validated(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            log = hub.HubLog()
            log.write(dict(event="stderr"))
            log.close()
            output.write("still open\n")
        self.assertEqual(json.loads(output.getvalue().splitlines()[0]), {"event": "stderr"})
        for field in ("max_bytes", "backup_count"):
            for value in (0, -1, True, 1.5):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        hub.HubLog(self.path, **{field: value})

    def test_cli_forwards_logging_options_and_default_stderr(self):
        common = ["--db", "/var/tmp/synthetic.db", "--token-file", "/var/tmp/synthetic-token"]
        with patch.object(hub, "serve") as serve:
            self.assertEqual(hub.main(common), 0)
            self.assertIsNone(serve.call_args.kwargs["log_file"])
            self.assertEqual(serve.call_args.kwargs["log_max_bytes"], 5 * 1024 * 1024)
            self.assertEqual(serve.call_args.kwargs["log_backup_count"], 3)
            self.assertEqual(hub.main(common + ["--log-file", self.path, "--log-max-bytes", "256", "--log-backup-count", "2"]), 0)
            self.assertEqual(serve.call_args.kwargs["log_file"], self.path)
            self.assertEqual(serve.call_args.kwargs["log_backup_count"], 2)
        with contextlib.redirect_stderr(io.StringIO()):
            for option in ("--log-max-bytes", "--log-backup-count"):
                with self.assertRaises(SystemExit) as error:
                    hub.main(common + [option, "0"])
                self.assertEqual(error.exception.code, 2)

    def test_real_http_access_gc_error_and_shutdown_flush_to_file(self):
        server = hub.make_server(os.path.join(self.temp.name, "hub.db"), "private-test-auth", port=0,
                                 log_file=self.path, log_max_bytes=4096, log_backup_count=3)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        def request():
            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request("POST", "/v1/register", body=json.dumps(dict(machine="x", harness="claude", session_id="private-body")),
                               headers={"Authorization": "Bearer private-test-auth"})
            response = connection.getresponse()
            response.read()
            connection.close()
        try:
            request()
            with patch.object(server.hub, "sweep2", side_effect=RuntimeError("PRIVATE exception detail")):
                server._next_gc = 0
                server.service_actions()
        finally:
            server.shutdown()
            thread.join(3)
            server.server_close()
            server.hub.close()
        with open(self.path, encoding="utf-8") as stream:
            text = stream.read()
        records = [json.loads(line) for line in text.splitlines()]
        self.assertTrue(any(record["event"] == "http_request" for record in records))
        self.assertTrue(any(record["event"] == "gc_error" for record in records))
        for secret in ("private-test-auth", "private-body", "PRIVATE exception detail"):
            self.assertNotIn(secret, text)
        self.assertTrue(server.log.closed)

    def test_close_waits_for_whole_in_progress_record_and_does_not_reopen(self):
        log = hub.HubLog(self.path)
        entered, release = threading.Event(), threading.Event()
        original = log.handler.emit
        def paused(record):
            entered.set()
            self.assertTrue(release.wait(3))
            original(record)
        with patch.object(log.handler, "emit", side_effect=paused):
            writer = threading.Thread(target=log.write, args=({"event": "last_complete_record"},))
            writer.start()
            self.assertTrue(entered.wait(2))
            closer = threading.Thread(target=log.close)
            closer.start()
            closer.join(0.05)
            self.assertTrue(closer.is_alive(), "close must wait for the active record")
            release.set()
            writer.join(2)
            closer.join(2)
        self.assertFalse(writer.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertIsNone(log.handler.stream)
        log.write({"event": "late_daemon_worker"})
        self.assertIsNone(log.handler.stream)
        with open(self.path) as stream:
            self.assertEqual(json.loads(stream.read()), {"event": "last_complete_record"})

    def test_unfinished_http_connection_does_not_hold_server_close(self):
        server = hub.make_server(os.path.join(self.temp.name, "slow.db"), "isolated", port=0, log_file=self.path)
        entered = threading.Event()
        finish_request = server.finish_request
        def accepted(*args):
            entered.set()
            return finish_request(*args)
        observing = patch.object(server, "finish_request", side_effect=accepted)
        observing.start()
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        connection = socket.create_connection(server.server_address, timeout=2)
        try:
            self.assertTrue(entered.wait(2), "prove the worker is waiting for client input")
            server.shutdown()
            thread.join(2)
            closed = threading.Event()
            closer = threading.Thread(target=lambda: (server.server_close(), closed.set()), daemon=True)
            closer.start()
            self.assertTrue(closed.wait(1), "logging must not introduce slow-client shutdown hangs")
            self.assertTrue(server.log.closed)
        finally:
            connection.close()
            server.server_close()
            server.hub.close()
            observing.stop()

    def test_real_cli_file_logging_and_clean_interrupt(self):
        token_path = os.path.join(self.temp.name, "synthetic-token")
        with open(token_path, "w") as stream:
            stream.write("isolated-cli-auth")
        process = subprocess.Popen([sys.executable, "-m", "agentbus", "hub",
                                    "--db", os.path.join(self.temp.name, "cli.db"),
                                    "--token-file", token_path, "--port", "0", "--log-file", self.path],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            started = None
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if os.path.exists(self.path):
                    with open(self.path) as stream:
                        lines = stream.readlines()
                    if lines:
                        started = json.loads(lines[0])
                        break
                if process.poll() is not None:
                    self.fail("CLI exited before startup")
                time.sleep(0.02)
            self.assertIsNotNone(started)
            connection = http.client.HTTPConnection("127.0.0.1", started["port"], timeout=2)
            connection.request("GET", "/v1/health")
            self.assertEqual(json.loads(connection.getresponse().read())["protocols"], [1, 2])
            connection.close()
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(stdout, b"")
            self.assertEqual(stderr, b"")
            with open(self.path) as stream:
                records = [json.loads(line) for line in stream]
            self.assertEqual(records[0]["event"], "hub_start")
            self.assertEqual(records[-1]["event"], "http_request")
            self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()

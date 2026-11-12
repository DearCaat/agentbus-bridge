"""Native injection routes (SPEC.md 'Injection').

`text` passed to inject_claude / inject_codex is the already-rendered message
(see render()). Both functions return (ok, detail); they never raise.
"""

import json
import os
import socket
import subprocess

CLAUDE_TIMEOUT = 5.0
CODEX_TIMEOUT = 60.0
DETAIL_TAIL = 500


LOCAL_REPLY_TAIL = {
    "claude": "(Same machine and harness: reply with SendMessage; find the sender in ListAgents. "
              "Do not use the agentbus send tool.)",
}
LOCAL_REPLY_TAIL_OTHER = ("(Same machine and harness: reply with this harness's own messaging. "
                          "Do not use the agentbus send tool.)")


def same_machine_harness(from_address, machine, harness):
    """True when from_address (machine/harness/name) is on `machine` and uses `harness`."""
    parts = from_address.split("/")
    return len(parts) == 3 and parts[0] == machine and parts[1] == harness


def render(msg_id, from_address, text, local_harness=None):
    """local_harness: the receiving session's harness when the sender is on the same machine with the
    same harness. The reply tail then names that harness's own messaging instead of agentbus."""
    if local_harness:
        tail = LOCAL_REPLY_TAIL.get(local_harness, LOCAL_REPLY_TAIL_OTHER)
    else:
        tail = '(Reply with the agentbus send tool, to="%s".)' % from_address
    return "[agentbus] message %s from %s:\n%s\n\n%s" % (msg_id, from_address, text, tail)


def _frame(obj):
    # Compact JSON, one frame per line. ensure_ascii left at default (pure ASCII wire).
    return (json.dumps(obj, separators=(",", ":")) + "\n").encode("ascii")


def inject_claude(socket_path, token, text):
    frames = []
    if token:
        frames.append({"type": "auth", "token": token})
    frames.append({"type": "user", "message": {"role": "user", "content": text}})
    payload = b"".join(_frame(f) for f in frames)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(CLAUDE_TIMEOUT)
        sock.connect(socket_path)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
    except OSError as e:
        return False, "%s: %s" % (type(e).__name__, e)
    finally:
        sock.close()
    # Write success only: it does not prove the model read the message.
    return True, "written to claude messaging socket"


def inject_codex(thread_id, codex_home, text, codex_bin="codex"):
    env = dict(os.environ)
    if codex_home:
        env["CODEX_HOME"] = codex_home
    argv = [codex_bin, "queue", "--thread", thread_id, "--message", text]
    try:
        proc = subprocess.run(
            argv,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=CODEX_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout after %ds" % int(CODEX_TIMEOUT)
    except OSError as e:
        return False, "exec failed: %s" % e
    if proc.returncode == 0:
        return True, "queued via codex queue"
    err = proc.stderr.decode("utf-8", "replace").strip()
    return False, "exit %d: %s" % (proc.returncode, err[-DETAIL_TAIL:])

"""agentbus session MCP server.

Minimal MCP over stdio: newline-delimited JSON-RPC 2.0. Stdlib only, Python >= 3.10.
Talks only to the local node over the unix socket $AGENTBUS_HOME/node.sock
(default ~/.agentbus/node.sock), one newline-delimited JSON request per connection.
See SPEC.md, "MCP server".

Harness detection:
- claude: CLAUDE_CODE_SESSION_ID is set. Session id, messaging socket and token come from env.
  The token is sent to the node only.
- codex: otherwise. The thread id arrives on the first tools/call (params._meta.threadId),
  or is found from the rollout file that the codex parent process holds open.
  See docs/codex-thread-id.md.

Env switches:
- AGENTBUS_HOME: config dir (default ~/.agentbus).
- AGENTBUS_NO_ENSURE=1: do not run `node --ensure`.
"""

import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid

from . import hotswap

SERVER_NAME = "agentbus"
SERVER_VERSION = "0.4.0"
RUNTIME_ID = uuid.uuid4().hex
RELOAD_GENERATION = os.environ.get('AGENTBUS_RELOAD_GENERATION')
DEFAULT_PROTOCOL_VERSION = "2024-11-05"
MAX_TEXT_BYTES = 64 * 1024
NODE_TIMEOUT = 10.0
ENSURE_TIMEOUT = 30.0
WATCH_INTERVAL = 2.0
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
ROLLOUT_RE = re.compile(r"rollout-.*-([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\.jsonl$")


def log(msg):
    sys.stderr.write("[agentbus-mcp] %s\n" % msg)
    sys.stderr.flush()


def home_dir():
    return os.environ.get("AGENTBUS_HOME") or os.path.join(os.path.expanduser("~"), ".agentbus")


class ToolError(Exception):
    """Reported to the client as a tool result with isError=true."""


class NodeError(ToolError):
    def __init__(self, code, message=""):
        ToolError.__init__(self, "%s: %s" % (code, message) if message else code)
        self.code = code


class RpcError(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, message)
        self.code = code


def node_call(req, timeout=NODE_TIMEOUT):
    """Send one request to the local node and return its reply dict (ok=true), or raise NodeError."""
    path = os.path.join(home_dir(), "node.sock")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(path)
        sock.sendall((json.dumps(req) + "\n").encode("utf-8"))
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
    except OSError as e:
        raise NodeError("node_unreachable", "cannot reach node at %s (%s)" % (path, e))
    finally:
        sock.close()
    line = buf.split(b"\n", 1)[0]
    try:
        resp = json.loads(line.decode("utf-8")) if line else None
    except ValueError:
        resp = None
    if not isinstance(resp, dict):
        raise NodeError("node_protocol", "empty or invalid reply from node")
    if not resp.get("ok"):
        raise NodeError(str(resp.get("error") or "node_error"), str(resp.get("detail") or ""))
    return resp


def ensure_node():
    """Run `node --ensure`. Never raises: the MCP must start even if the node is down."""
    if os.environ.get("AGENTBUS_NO_ENSURE") == "1":
        return
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT
    try:
        # stdout is DEVNULL and stderr is a file, not a pipe: the node daemon it starts
        # must not keep a pipe open and block this call.
        with tempfile.TemporaryFile() as err:
            rc = subprocess.call(
                [sys.executable, "-m", "agentbus", "node", "--ensure"],
                cwd=REPO_ROOT, env=env, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=err, timeout=ENSURE_TIMEOUT)
            if rc != 0:
                err.seek(0)
                log("node --ensure exited %d: %s" % (rc, err.read()[-300:].decode("utf-8", "replace")))
    except Exception as e:  # noqa: BLE001 - any failure here must not stop the MCP
        log("node --ensure failed: %s" % e)


def codex_home_for(ppid):
    """CODEX_HOME: from our env if present, else read only that key from the codex parent's environ."""
    val = os.environ.get("CODEX_HOME")
    if val:
        return val
    try:
        with open("/proc/%d/environ" % ppid, "rb") as f:
            data = f.read()
    except OSError:
        return None
    prefix = b"CODEX_HOME="
    for item in data.split(b"\0"):
        if item.startswith(prefix):
            return item[len(prefix):].decode("utf-8", "replace")
    return None


def codex_thread_from_proc(ppid):
    """Thread id from the rollout JSONL that the codex parent process holds open."""
    fd_dir = "/proc/%d/fd" % ppid
    try:
        names = os.listdir(fd_dir)
    except OSError:
        return None
    found = set()
    for n in names:
        try:
            target = os.readlink(os.path.join(fd_dir, n))
        except OSError:
            continue
        m = ROLLOUT_RE.search(os.path.basename(target))
        if m:
            found.add(m.group(1).lower())
    # Several open rollouts (e.g. sub-agent threads in one process) are ambiguous: wait for _meta.
    return found.pop() if len(found) == 1 else None


class Session:
    def __init__(self):
        self.lock = threading.RLock()
        self.ppid = os.getppid()
        self.harness = None
        self.session_id = None
        self.deliver = {}
        self.name = None
        self.reg = None          # {"session_key", "address"} once the node accepted registration
        self.node_down = False
        self.last_error = "not registered yet"

    def detect(self):
        sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
        if sid:
            self.harness = "claude"
            self.session_id = sid
            self.deliver = {
                "socket": os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET"),
                "token": os.environ.get("CLAUDE_CODE_MESSAGING_TOKEN"),
            }
        else:
            self.harness = "codex"

    def adopt_codex_thread(self, tid, source):
        with self.lock:
            tid = tid.lower()
            if self.session_id == tid:
                if source.startswith("tools/call"):
                    self.id_from_meta = True
                return
            if self.session_id is not None:
                # tools/call _meta is authoritative; a fd-derived id may be replaced by it, never the reverse.
                if not source.startswith("tools/call") or getattr(self, "id_from_meta", False):
                    log("ignoring codex thread id %s from %s: already %s" % (tid, source, self.session_id))
                    return
                log("codex thread id %s (fd) replaced by %s from %s" % (self.session_id, tid, source))
                self.reg = None
            self.id_from_meta = source.startswith("tools/call")
            self.session_id = tid
            self.deliver = {"thread_id": tid, "codex_home": codex_home_for(self.ppid)}
        log("codex thread id from %s" % source)

    def register_body(self, name):
        body = {
            "op": "register",
            "harness": self.harness,
            "session_id": self.session_id,
            "cwd": os.getcwd(),
            "mcp_pid": os.getpid(),
            "deliver": self.deliver,
        }
        if name:
            body["name"] = name
        return body


def try_register(sess):
    """Register with the node if not yet registered. Returns True when registered."""
    with sess.lock:
        if sess.reg is not None:
            return True
        if sess.session_id is None:
            sess.last_error = "codex thread id not known yet (no tool call or rollout seen)"
            return False
        if sess.node_down:
            ensure_node()
        try:
            resp = node_call(sess.register_body(sess.name))
        except NodeError as e:
            sess.node_down = e.code == "node_unreachable"
            sess.last_error = str(e)
            return False
        sess.node_down = False
        sess.reg = {"session_key": resp.get("session_key"), "address": resp.get("address")}
        sess.last_error = None
        return True


def require_registered(sess):
    if sess.reg is None and not try_register(sess):
        raise ToolError("not_registered: %s" % sess.last_error)
    return sess.reg


def codex_watch(sess):
    """Codex only: poll the parent's rollout fd until the thread id shows up, then register."""
    while sess.reg is None:
        if sess.session_id is None:
            tid = codex_thread_from_proc(sess.ppid)
            if tid:
                sess.adopt_codex_thread(tid, "parent rollout fd")
        try_register(sess)
        time.sleep(WATCH_INTERVAL)


# ---- tools -------------------------------------------------------------

def _str_arg(args, key):
    val = args.get(key)
    if not isinstance(val, str) or not val:
        raise ToolError("invalid_arguments: %s must be a non-empty string" % key)
    return val


def split_address(addr):
    """(machine, harness, name) of an address `machine/harness/name`, or None if it is not one."""
    parts = addr.split("/")
    if len(parts) != 3 or not all(parts):
        return None
    return tuple(parts)


def target_machine_harness(to):
    """(machine, harness) of a send target when it can be told: from an exact address, or from the
    peer list for a session key. None when unknown (the send then goes ahead as before)."""
    parts = split_address(to)
    if parts is not None:
        return parts[0], parts[1]
    try:
        peers = node_call({"op": "peers"}).get("peers")
    except NodeError:
        return None
    for p in peers if isinstance(peers, list) else []:
        if isinstance(p, dict) and p.get("session_key") == to:
            machine, harness = p.get("machine"), p.get("harness")
            if isinstance(machine, str) and isinstance(harness, str):
                return machine, harness
    return None


def tool_whoami(sess, args):
    try:
        node_call({"op": "peers"})
        node_status = "ok"
    except NodeError as e:
        node_status = str(e)
    reg = sess.reg or {}
    return {
        "harness": sess.harness,
        "session_id": sess.session_id,
        "registered": sess.reg is not None,
        "address": reg.get("address"),
        "session_key": reg.get("session_key"),
        "mcp_pid": os.getpid(),
        "node": node_status,
        "last_error": None if sess.reg is not None else sess.last_error,
    }


def tool_peers(sess, args):
    resp = node_call({"op": "peers"})
    peers = resp.get("peers") or []
    if not peers:
        return "no peers"
    rows = [("address", "online", "cwd", "last_seen")]
    for p in peers:
        rows.append((str(p.get("address", "")), "yes" if p.get("online") else "no",
                     str(p.get("cwd", "")), str(p.get("last_seen", ""))))
    widths = [max(len(r[i]) for r in rows) for i in range(4)]
    return "\n".join("  ".join(c.ljust(widths[i]) for i, c in enumerate(r)).rstrip() for r in rows)


def _receipt(resp, operation, request_id=None):
    result = {k: v for k, v in resp.items() if k != "ok"}
    if request_id is not None:
        result["request_id"] = request_id
    if not result.get("summary"):
        if result.get("state") == "unknown_or_pruned":
            summary = "Unknown or pruned message; this does not prove it was never sent or received."
        elif result.get("receiver_persisted"):
            summary = "Recipient plugin durably saved the message; model reading and task completion remain unknown."
            if result.get("injection_state") in ("in_doubt", "failed"):
                summary += " Native injection is uncertain or failed; the retained inbox remains available."
            if result.get("blocked_by_delivery"):
                summary += " Native presentation is blocked by an earlier injection outcome, not by unread messages."
            if result.get("receiver_acknowledged"):
                summary += " Recipient also made an optional explicit read/receipt declaration."
        elif result.get("receiver_acknowledged"):
            summary = "Recipient explicitly declared read/receipt; this does not confirm understanding or task completion."
        elif operation == "send2":
            summary = ("Hub accepted the message; recipient plugin persistence remains unconfirmed; model reading is unknown."
                       if result.get("submission") == "accepted" else
                       "Submission pending or unknown; retry only with the same request_id and payload.")
        elif result.get("state") in ("unknown", "in_doubt", "injecting"):
            summary = "Transport or native injection outcome remains uncertain; model reading is unknown. Do not infer no delivery."
        elif operation == "ack2":
            summary = "Optional read/receipt declaration is unconfirmed; later delivery never requires this tool."
        elif operation == "message2" and result.get("body_retained") is False:
            summary = "Message body is not retained; metadata retrieval is not a read declaration."
        elif operation in ("message2", "inbox2"):
            summary = "Retrieved retained message data, independent of earlier unread messages; no read declaration was made."
        elif operation == "history2":
            summary = "Retrieved available history; inspect pagination and pruning fields."
        else:
            summary = "Observed transport/native injection status; model reading and task completion remain unknown."
        result["summary"] = summary
    return result


def tool_send(sess, args):
    to = _str_arg(args, "to")
    text = _str_arg(args, "text")
    request_id = _str_arg(args, "request_id")
    reply_to = _str_arg(args, "reply_to") if "reply_to" in args else None
    if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
        raise ToolError("too_large: text exceeds %d bytes" % MAX_TEXT_BYTES)
    reg = require_registered(sess)
    own = split_address(reg.get("address") or "")
    target = target_machine_harness(to)
    if own is not None and target is not None and target == own[:2]:
        raise ToolError(
            "same_machine_same_harness: %s is on this machine with the same harness (%s). agentbus is for "
            "other machines or other harnesses; use the harness's own messaging for it (Claude Code: "
            "ListAgents to find the session, then SendMessage)." % (to, own[1]))
    try:
        resp = node_call({"op": "send2", "session_key": reg["session_key"], "to": to,
                          "text": text, "client_msg_id": request_id, "reply_to": reply_to})
    except NodeError as e:
        raise NodeError(e.code, "%s; request_id=%s. Submission may be unknown. Retry with this SAME "
                        "request_id and identical to/text/reply_to; do not allocate a new ID." % (e, request_id))
    return _receipt(resp, "send2", request_id)


def tool_status(sess, args):
    if ("msg_id" in args) == ("request_id" in args):
        raise ToolError("invalid_arguments: provide exactly one of msg_id or request_id")
    reg = require_registered(sess)
    key = "msg_id" if "msg_id" in args else "request_id"
    value = _str_arg(args, key)
    resp = node_call({"op": "status2", "session_key": reg["session_key"],
                      "msg_id" if key == "msg_id" else "client_msg_id": value})
    return _receipt(resp, "status2", value if key == "request_id" else None)


def _page_args(args):
    after, limit = args.get("after", 0), args.get("limit", 50)
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 200:
        raise ToolError("invalid_arguments: after must be a nonnegative integer; limit must be 1-200")
    return {"after": after, "limit": limit}


def _message_op(sess, args, op, paged=False):
    req = {"op": op}
    if op != "inbox2":
        req["msg_id"] = _str_arg(args, "msg_id")
    if paged:
        req.update(_page_args(args))
    req["session_key"] = require_registered(sess)["session_key"]
    return _receipt(node_call(req), op)


def tool_ack(sess, args):
    return _message_op(sess, args, "ack2")


def tool_message(sess, args):
    return _message_op(sess, args, "message2")


def tool_inbox(sess, args):
    return _message_op(sess, args, "inbox2", paged=True)


def tool_history(sess, args):
    return _message_op(sess, args, "history2", paged=True)


def tool_control(sess, args):
    reg = require_registered(sess)
    req = {k:_str_arg(args,k) for k in ('to','action','scope','request_id')}
    req.update(op='control_submit2',session_key=reg['session_key'])
    reply = node_call(req)
    if not reply.get('ok'):
        raise ToolError(reply.get('error','control_failed'))
    return {k:v for k,v in reply.items() if k!='ok'}


def tool_control_status(sess, args):
    if ('control_id' in args)==('request_id' in args):
        raise ToolError('invalid_arguments: provide exactly one control_id or request_id')
    key = 'control_id' if 'control_id' in args else 'request_id'
    req = {key:_str_arg(args,key), 'op':'control_status2',
           'session_key':require_registered(sess)['session_key']}
    reply = node_call(req)
    if not reply.get('ok'):
        raise ToolError(reply.get('error','control_status_failed'))
    return {k:v for k,v in reply.items() if k!='ok'}


def tool_rename(sess, args):
    name = _str_arg(args, "name")
    if not NAME_RE.match(name):
        raise ToolError("invalid_name: use 1-64 chars of [A-Za-z0-9._-], starting with a letter or digit")
    if sess.session_id is None:
        raise ToolError("not_registered: session id unknown (%s)" % sess.last_error)
    with sess.lock:
        resp = node_call(sess.register_body(name))
        sess.name = name
        sess.reg = {"session_key": resp.get("session_key"), "address": resp.get("address")}
        sess.node_down = False
        sess.last_error = None
    return {"address": sess.reg["address"], "session_key": sess.reg["session_key"]}


SCHEMA_EMPTY = {"type": "object", "properties": {}, "additionalProperties": False}

TOOLS = {
    'agentbus_control': (tool_control, {
        'description': 'Request a native reload with a stable request_id. This is a control action, not a text message. Codex reload_mcp requires explicit server scope and refreshes loaded threads on that same app server; verification identifies the exact target thread. Existing Claude interactive sessions do not expose native reload_plugins and return unsupported without execution. Hub acceptance is not native execution; query the control result after a disconnected or reloaded MCP call.',
        'inputSchema': {'type':'object','properties':{
            'to':{'type':'string'},'action':{'type':'string','enum':['reload_plugins','reload_mcp']},
            'scope':{'type':'string','enum':['session','server']},'request_id':{'type':'string','minLength':1,'maxLength':200}},
            'required':['to','action','scope','request_id'],'additionalProperties':False}}),
    'agentbus_control_status': (tool_control_status, {
        'description': 'Query a native control by exactly one control_id or original request_id. Accepted, executing, verified, applied_unverified, unsupported, failed and in_doubt are distinct. Retry transport failures using the same request_id, never a new ID to blindly repeat an uncertain native operation.',
        'inputSchema':{'type':'object','properties':{'control_id':{'type':'string'},'request_id':{'type':'string'}},
            'oneOf':[{'required':['control_id']},{'required':['request_id']}],'additionalProperties':False}}),
    "agentbus_whoami": (tool_whoami, {
        "description": "Show this session's agentbus address, session key, and node status.",
        "inputSchema": SCHEMA_EMPTY}),
    "agentbus_peers": (tool_peers, {
        "description": "List agentbus peers (address, online, cwd, last_seen). agentbus is only for peers on "
                       "another machine or with another harness: a peer on this machine with the same harness "
                       "is reached through the harness's own messaging (Claude Code: ListAgents, SendMessage).",
        "inputSchema": SCHEMA_EMPTY}),
    "agentbus_send": (tool_send, {
        "description": "Send a text message to a peer by exact address or session key. Only for a peer on another "
                       "machine or with another harness: a peer on this machine with the same harness is refused "
                       "(same_machine_same_harness); use the harness's own messaging for it (Claude Code: "
                       "ListAgents to find the session, SendMessage to message it). Requires a stable caller-generated request_id; "
                       "retry with the SAME ID and identical payload after uncertainty. reply_to associates a reply "
                       "with the exact received msg_id. The recipient plugin automatically confirms durable storage; "
                       "manual ACK is optional. Hub acceptance, recipient persistence, native injection, model reading "
                       "and task completion are separate facts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Exact address (machine/harness/name) or session_key."},
                "text": {"type": "string", "description": "Full message text, at most 64 KiB."},
                "request_id": {"type": "string", "minLength": 1, "description": "Stable caller-generated retry key."},
                "reply_to": {"type": "string", "minLength": 1, "description": "Exact received msg_id being answered."},
            },
            "required": ["to", "text", "request_id"], "additionalProperties": False}}),
    "agentbus_status": (tool_status, {
        "description": "Observe hub submission, receiver_persisted, injection_state and optional explicit read declaration "
                       "by msg_id OR request_id. Durable plugin storage and native injection do not prove model reading "
                       "or task completion. Unknown is not proof of no delivery.",
        "inputSchema": {"type": "object", "properties": {
            "msg_id": {"type": "string", "minLength": 1},
            "request_id": {"type": "string", "minLength": 1}},
            "oneOf": [{"required": ["msg_id"]}, {"required": ["request_id"]}],
            "additionalProperties": False}}),
    "agentbus_rename": (tool_rename, {
        "description": "Set a friendlier name for this session (changes its address).",
        "inputSchema": {
            "type": "object",
            "properties": {"name": {"type": "string", "pattern": NAME_RE.pattern}},
            "required": ["name"], "additionalProperties": False}}),
}


for _name, _fn, _description, _has_id, _paged in [
    ("ack", tool_ack, "Optionally declare that you read/received this full message. Not required for delivery or replies; "
     "the plugin automatically confirms durable storage. This declaration does not prove understanding or task completion.", True, False),
    ("message", tool_message, "Retrieve full retained message and envelope, including injection uncertainty. Earlier unread messages do not hide text. "
     "Does not make an explicit read declaration.", True, False),
    ("inbox", tool_inbox, "Retrieve the complete retained inbox with cursor pagination, independent of earlier unread messages. "
     "Inspect blocked_by_delivery for native presentation uncertainty; this does not make a read declaration.", False, True),
    ("history", tool_history, "Retrieve message events with cursor pagination and explicit pruning information.", True, True),
]:
    _properties = {"msg_id": {"type": "string", "minLength": 1}} if _has_id else {}
    if _paged:
        _properties.update({"after": {"type": "integer", "minimum": 0, "default": 0},
                            "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}})
    TOOLS["agentbus_" + _name] = (_fn, {"description": _description, "inputSchema": {
        "type": "object", "properties": _properties, "required": ["msg_id"] if _has_id else [],
        "additionalProperties": False}})


def tool_result(value, is_error):
    result = {"content": [{"type": "text", "text": _result_text(value)}], "isError": is_error}
    if isinstance(value, dict):
        result["structuredContent"] = value
    return result


def _result_text(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2, sort_keys=True)


def handle_tools_call(sess, params):
    name = params.get("name")
    if not isinstance(name, str):
        raise RpcError(-32602, "tools/call requires a string name")
    args = params.get("arguments") or {}
    if not isinstance(args, dict):
        raise RpcError(-32602, "tools/call arguments must be an object")
    if name not in TOOLS:
        return tool_result("unknown tool: %s" % name, True)

    if sess.harness == "codex" and sess.session_id is None:
        meta = params.get("_meta") if isinstance(params.get("_meta"), dict) else {}
        tid = meta.get("threadId")
        if not (isinstance(tid, str) and UUID_RE.match(tid)):
            tid = None
            ts = meta.get("x-codex-turn-metadata")
            if isinstance(ts, dict) and isinstance(ts.get("thread_id"), str) and UUID_RE.match(ts["thread_id"]):
                tid = ts["thread_id"]
        if tid:
            sess.adopt_codex_thread(tid, "tools/call _meta")
        else:
            found = codex_thread_from_proc(sess.ppid)
            if found:
                sess.adopt_codex_thread(found, "parent rollout fd")

    # Lazy retry: a registration that failed at startup (node down, thread id unknown) is retried on each call.
    if sess.reg is None and sess.session_id is not None:
        try_register(sess)

    fn = TOOLS[name][0]
    try:
        value = fn(sess, args)
        failed_control = (name == 'agentbus_control' and isinstance(value,dict) and
                          value.get('state') in ('unsupported','failed','in_doubt','expired'))
        return tool_result(value, failed_control)
    except ToolError as e:
        error = {"error": getattr(e, "code", str(e).split(":", 1)[0]), "summary": "agentbus error: %s" % e}
        if name in ("agentbus_send",'agentbus_control','agentbus_control_status') and isinstance(args.get("request_id"), str):
            error["request_id"] = args["request_id"]
        return tool_result(error, True)
    except Exception as e:  # noqa: BLE001 - report, do not kill the MCP
        log("tool %s failed: %s" % (name, traceback.format_exc()))
        return tool_result("agentbus internal error: %s" % e, True)


def handle_request(sess, method, params):
    if method == "initialize":
        return {
            "protocolVersion": params.get("protocolVersion") or DEFAULT_PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": True}, "experimental": {"agentbus": {
                "runtime_id": RUNTIME_ID, "reload_generation": RELOAD_GENERATION}}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        }
    if method == "tools/list":
        return {"tools": [dict(name=n, **spec) for n, (_, spec) in TOOLS.items()]}
    if method == "tools/call":
        return handle_tools_call(sess, params)
    if method == "ping":
        return {}
    raise RpcError(-32601, "method not found: %s" % method)


def write_msg(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def handle_line(sess, line):
    try:
        msg = json.loads(line)
    except ValueError:
        write_msg({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
        return
    if not isinstance(msg, dict):
        write_msg({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "invalid request"}})
        return
    method = msg.get("method")
    if method is None or "id" not in msg:
        return  # notification (e.g. notifications/initialized) or a response from the client
    params = msg.get("params")
    if not isinstance(params, dict):
        params = {}
    try:
        result = handle_request(sess, method, params)
    except RpcError as e:
        write_msg({"jsonrpc": "2.0", "id": msg["id"], "error": {"code": e.code, "message": str(e)}})
        return
    except Exception as e:  # noqa: BLE001
        log("request %s failed: %s" % (method, traceback.format_exc()))
        write_msg({"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32603, "message": str(e)}})
        return
    write_msg({"jsonrpc": "2.0", "id": msg["id"], "result": result})


def node_watchdog(sess, interval=60.0):
    """Keep this machine's node alive while any session is open: a dead node means the machine
    receives nothing until someone restarts it. Re-registers after restarting it."""
    while True:
        time.sleep(interval)
        if os.environ.get("AGENTBUS_NO_ENSURE") == "1":
            continue
        try:
            c = socket.socket(socket.AF_UNIX)
            c.settimeout(5)
            c.connect(os.path.join(home_dir(), "node.sock"))
            c.close()
            continue
        except OSError:
            pass
        log("node not reachable; running node --ensure")
        with hotswap.SWAP_LOCK:  # no hot swap while restarting the node and re-registering
            ensure_node()
            with sess.lock:
                sess.reg = None
            try_register(sess)


def main(argv=None):
    """Entry point for `python3 -m agentbus mcp`. argv may carry hotswap.REEXEC_ARG (see hotswap.py)."""
    reexec = hotswap.REEXEC_ARG in (argv or [])
    sess = Session()
    sess.detect()
    ensure_node()
    if not try_register(sess):
        log("registration deferred: %s" % sess.last_error)
        if sess.harness == "codex":
            threading.Thread(target=codex_watch, args=(sess,), daemon=True).start()
    threading.Thread(target=node_watchdog, args=(sess,), daemon=True).start()
    reader = hotswap.reader_from_env()
    if reexec:  # just switched from an older version: ask the client to re-read the tool list
        write_msg({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
    for raw in reader:
        line = raw.decode("utf-8", "replace").strip()
        if line:
            handle_line(sess, line)
            hotswap.maybe_swap(reader, sess.harness)


if __name__ == "__main__":
    main()

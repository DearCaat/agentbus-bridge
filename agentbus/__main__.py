"""python3 -m agentbus {hub|node|mcp|send|peers|status} ..."""
import json, os, socket, sys


def _node_call(req):
    home = os.environ.get("AGENTBUS_HOME") or os.path.expanduser("~/.agentbus")
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(30)
    s.connect(os.path.join(home, "node.sock"))
    s.sendall((json.dumps(req) + "\n").encode())
    s.shutdown(socket.SHUT_WR)
    buf = b""
    while True:
        b = s.recv(65536)
        if not b:
            break
        buf += b
    return json.loads(buf.decode() or "{}")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "hub":
        from . import hub
        return hub.main(rest)
    if cmd == "node":
        from . import node
        return node.main(rest)
    if cmd == "mcp":
        from . import mcp_server
        return mcp_server.main(rest)
    if cmd == "peers":
        print(json.dumps(_node_call({"op": "peers"}), ensure_ascii=False, indent=1))
        return 0
    if cmd == "status":
        print(json.dumps(_node_call({"op": "status", "msg_id": rest[0]}), ensure_ascii=False))
        return 0
    if cmd == "send":
        import argparse, uuid
        ap = argparse.ArgumentParser(prog="agentbus send")
        ap.add_argument("--to", required=True)
        ap.add_argument("--text", required=True)
        ap.add_argument("--from-key", default="cli")
        a = ap.parse_args(rest)
        print(json.dumps(_node_call({"op": "send", "session_key": a.from_key, "from_key": a.from_key, "to": a.to,
                                     "text": a.text, "client_msg_id": str(uuid.uuid4())}),
                         ensure_ascii=False))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main() or 0)

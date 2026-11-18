"""python3 -m agentbus {hub|node|mcp|send|peers|status|control|control-status} ..."""
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
    if cmd == "selftest":  # hot-swap check run by an older MCP before it switches to this version
        from . import mcp_server
        json.dumps([dict(name=n, **spec) for n, (_, spec) in mcp_server.TOOLS.items()])
        print("ok")
        return 0
    if cmd == "mcp":
        from . import mcp_server
        return mcp_server.main(rest)
    if cmd == "peers":
        print(json.dumps(_node_call({"op": "peers"}), ensure_ascii=False, indent=1))
        return 0
    if cmd in ('control','control-status'):
        import argparse
        ap=argparse.ArgumentParser(prog='agentbus '+cmd)
        ap.add_argument('--from-session',required=True,help='Exact locally registered sender session key')
        if cmd=='control':
            ap.add_argument('--to',required=True)
            ap.add_argument('--action',required=True,choices=['reload_plugins','reload_mcp'])
            ap.add_argument('--scope',required=True,choices=['session','server'])
            ap.add_argument('--request-id',required=True)
            ap.add_argument('--wait',action='store_true',help='Wait for the native result; exit zero only when verified')
            ap.add_argument('--timeout',type=float,default=30,help='Maximum wait seconds; never retries native execution')
        else:
            group=ap.add_mutually_exclusive_group(required=True)
            group.add_argument('--control-id')
            group.add_argument('--request-id')
        a=ap.parse_args(rest)
        if cmd=='control' and a.wait:
            import math
            if not math.isfinite(a.timeout) or a.timeout<=0:
                ap.error('--timeout must be positive and finite')
        req={'op':'control_submit2' if cmd=='control' else 'control_status2',
             'session_key':a.from_session}
        for k in ('to','action','scope','request_id','control_id'):
            value=getattr(a,k,None)
            if value is not None:req[k]=value
        reply=_node_call(req)
        if cmd=='control' and a.wait and reply.get('ok'):
            import time
            deadline=time.monotonic()+a.timeout
            terminal=('verified','applied_unverified','unsupported','failed','in_doubt','expired')
            while reply.get('state') not in terminal and time.monotonic()<deadline:
                time.sleep(min(.2,max(0,deadline-time.monotonic())))
                reply=_node_call({'op':'control_status2','session_key':a.from_session,'request_id':a.request_id})
                if not reply.get('ok'):break
            print(json.dumps(reply,ensure_ascii=False,indent=2))
            return 0 if reply.get('state')=='verified' and reply.get('verification_confirmed') is True else 1
        print(json.dumps(reply,ensure_ascii=False,indent=2))
        return 0 if reply.get('ok') else 1
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

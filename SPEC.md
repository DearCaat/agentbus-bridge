# agentbus SPEC v0.1

Cross-harness (Claude Code, Codex) and cross-machine messaging between live agent sessions.
Same-machine same-harness traffic is out of scope (use the harness's native messaging).

Constraints: Python >= 3.10, **stdlib only**. Linux and WSL are the initial target platforms.
No third-party services, no public listeners. Everything binds 127.0.0.1 or a unix socket.

## Topology

```
session MCP (stdio, one per agent session)  --unix socket-->  node (one per machine)
node  --HTTP over ssh -L tunnel-->  hub (any reachable Linux host, 127.0.0.1:37460, SQLite)
node  --native injection-->  receiving session
```

## Addresses

- `session_key` = `<machine>:<harness>:<session_id>` (unique, stable).
- `address` = `<machine>/<harness>/<name>`; `name` defaults to the first 8 chars of session_id;
  a session may set a friendlier name (unique per machine+harness; conflict -> error).
- `send.to` accepts an exact address, or an exact session_key. Nothing else. No "most recent
  session" fallback, no fuzzy match. Unknown -> error `unknown_target`. Offline target -> message is
  queued and status says `queued` (target_online=false).
- harness ∈ {`claude`, `codex`}.

## Hub HTTP API (JSON bodies, header `Authorization: Bearer <token>`; 401 otherwise)

Bind 127.0.0.1:37460 (configurable). DB: SQLite file (WAL). Token: read from a file at startup.

| Method/path | Body / query | Response |
|---|---|---|
| POST /v1/register | {machine, harness, session_id, name?, cwd?} | {session_key, address} ; 409 `name_taken` |
| POST /v1/heartbeat | {machine, live: [session_key...]} | {ok:true} ; sessions of that machine not in `live` become offline; listed ones online, last_seen=now |
| GET /v1/peers | - | {peers:[{address, session_key, machine, harness, name, cwd, online, last_seen}]} |
| POST /v1/send | {from_key, to, text, client_msg_id} | {msg_id, target: address, target_online} ; 404 `unknown_target`; same (from_key, client_msg_id) returns the original msg_id (idempotent) |
| GET /v1/pull | ?machine=M&wait=S (S<=25) | long-poll; {messages:[{msg_id, from_address, to_key, text, created_at}]} ; returned msgs move queued->leased (lease 120s; expired leases return to queued) |
| POST /v1/report | {msg_id, state, detail?} | state ∈ delivered, failed, held, in_doubt ; terminal states are final |
| GET /v1/status | ?msg_id=X | {msg_id, state, detail, target, created_at, updated_at} |
| GET /v1/health | - (no auth) | {ok:true, version} |

Text limit 64 KiB (413 `too_large`). Messages older than 7 days in queued state -> `expired`.

## Node (one per machine)

Paths/env (runtime files must live on local disk; each node needs its own runtime directory):
- runtime dir = $AGENTBUS_HOME (default ~/.agentbus): node.sock, node.db, node.lock, node.log.
- config = $AGENTBUS_CONFIG (default $AGENTBUS_HOME/config.json); token = config `token_file` or $AGENTBUS_HOME/token.
- $AGENTBUS_MACHINE overrides config `machine`. Tunnel argv elements get expandvars + expanduser.
- If HOME is on a shared or FUSE filesystem, set `AGENTBUS_HOME` to a local directory.
  Set `AGENTBUS_CONFIG` separately when configuration is stored elsewhere. Configure the
  same values for both harnesses on a machine, and a distinct `AGENTBUS_MACHINE` per machine.

- Config dir `~/.agentbus/` : `config.json` {machine, hub_url (default http://127.0.0.1:37460),
  tunnel: [argv of ssh -N -L ...] or null}, `token` (mode 600), `node.sock`, `node.db`, `node.log`.
- Keeps the ssh tunnel alive (restart with backoff) when `tunnel` is set.
- `extra_tunnels`: optional additional SSH argv lists kept alive by the node. This can
  support a relay topology when a machine cannot connect directly to the hub. The
  deployment owner supplies the SSH hosts and keys; none are bundled.

- Singleton via `fcntl.flock` on `~/.agentbus/node.lock`. `agentbus node --ensure` starts it
  detached (setsid) if not running, returns immediately.
- Local API on unix socket `node.sock` (mode 600): newline-delimited JSON request/response, one
  request per connection: {op: register|send|peers|status|unregister, ...} -> {ok, ...} or {ok:false, error}.
- Local registry (node.db): session_key -> {harness, deliver info, mcp_pid, last_seen}.
  deliver info: claude -> {socket: CLAUDE_CODE_MESSAGING_SOCKET}; codex -> {thread_id, codex_home}.
  A session is live while its mcp_pid is alive (check /proc state != Z) — heartbeat to hub every 20s.
- Pull loop: long-poll hub; for each message:
  1. if msg_id already in local `delivered_ids` -> report previous result, skip (never re-inject);
  2. insert msg_id into `delivered_ids` (commit) BEFORE injecting (at-most-once);
  3. inject; 4. report delivered/failed with detail.
  On startup, ids recorded but never reported -> report `in_doubt`.
- Rendered text injected into the receiving session:
  ```
  [agentbus] message <msg_id> from <from_address>:
  <text>

  (Reply with the agentbus send tool, to="<from_address>".)
  ```
  Exception (sender on the same machine with the same harness as the receiver, i.e. the `machine` and
  `harness` parts of `from_address` equal the receiver's): the last line is instead
  `(Same machine and harness: reply with SendMessage; find the sender in ListAgents. Do not use the agentbus send tool.)`
  for a Claude receiver, and `(Same machine and harness: reply with this harness's own messaging. Do not use the agentbus send tool.)`
  for any other harness. The node compares against its own `machine` (config / `AGENTBUS_MACHINE`).

## Injection (version-dependent harness adapters)

Development observations used Claude Code 2.1.321 and codex-cli 0.167.0. These are
observations of those versions, not guarantees for other builds. See the
compatibility section in README.md and docs/codex-thread-id.md.

- Claude: connect AF_UNIX to the session's `CLAUDE_CODE_MESSAGING_SOCKET`; send
  `{"type":"auth","token":"<CLAUDE_CODE_MESSAGING_TOKEN>"}\n` (if token known) then
  `{"type":"user","message":{"role":"user","content":"<rendered>"}}\n`; shutdown write; close.
  Idle session starts a new turn; busy session gets it between tool calls.
  Requires `crossSessionInbound: "accept"` in the receiver's settings when it runs bypassPermissions
  (otherwise held for approval). Socket write success != model read; report `delivered` means
  "written to socket".
- Codex: `codex queue --thread <thread_id> --message <rendered>` with env CODEX_HOME=<codex_home>;
  exit 0 -> delivered, else failed(detail=stderr tail). Timeout 60s.

## MCP server (stdio, per session; shipped in the plugin)

Minimal MCP over stdio (JSON-RPC 2.0, newline-delimited): initialize, tools/list, tools/call,
notifications/initialized, ping. On start: `node --ensure`, detect harness and session id, register.
- Claude: harness=claude, session_id=$CLAUDE_CODE_SESSION_ID, socket=$CLAUDE_CODE_MESSAGING_SOCKET,
  token=$CLAUDE_CODE_MESSAGING_TOKEN.
- Codex: harness=codex, thread id discovery: see `docs/codex-thread-id.md` (spike result).
Tools:
- `agentbus_whoami()` -> own address, session_key, node/hub health.
- `agentbus_peers()` -> table of peers (address, online, cwd, last_seen).
- `agentbus_send(to, text)` -> msg_id, target, target_online. Generates client_msg_id (uuid4).
  Refused with `same_machine_same_harness` (tool error, nothing sent) when the target's machine and harness
  both equal this session's (own address `machine/harness/name`). The target's machine/harness come from an
  exact address, or for a session_key from the node's `peers` list; a session_key not found there is sent as usual.
  Same-machine same-harness peers are reached with the harness's own messaging.
- `agentbus_status(msg_id)` -> state.
- `agentbus_rename(name)` -> new address.
Errors are returned as tool results with isError=true and a clear message.

## CLI

`python3 -m agentbus hub --db PATH --token-file PATH [--port 37460]`
`python3 -m agentbus node [--ensure|--foreground]`
`python3 -m agentbus mcp`
`python3 -m agentbus send --from-key SESSION_KEY --to ADDR --text T`
(`SESSION_KEY` must be registered; discover it with `agentbus_whoami`).
`python3 -m agentbus peers` / `python3 -m agentbus status ID` (debug via node.sock).

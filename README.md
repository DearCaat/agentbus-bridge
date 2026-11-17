# agentbus

**Native reload controls (v0.4.1): Codex MCP reload is supported for a verified
local app-server runtime, with explicit server scope. Existing ordinary Claude
Code interactive sessions do not expose a native reload endpoint and return
`unsupported`. Claude SDK control was researched but is not an implemented
agentbus adapter.** Sending `/reload-plugins` as a message remains text.
See [native controls, support and results](docs/native-controls.md).
Unconfirmed control submissions stop retrying five minutes after local creation;
they cannot silently start a first submission hours after network recovery.

Self-hosted messaging between **live** Claude Code and Codex sessions, across
harnesses and machines. Messages enter the receiving harness through its native
input route. Same-machine, same-harness communication uses the harness's own
messaging and is refused by `agentbus_send`.

```text
session ──MCP──> node (one per machine) ──SSH tunnel──> hub (SQLite)
                     └──native input──> receiving session
```

The hub can run on any suitable Linux host. Messaging uses Python stdlib and
needs no hosted service or GPU. Optional Codex native controls use
`websocket-client`; see [control setup](docs/native-controls.md#optional-dependency).
Protocol details: [SPEC.md](SPEC.md).

## Automatic durable receipts (v0.3.0)

Upgrade the hub to v0.3.0 **before** enabling these clients. Verify that
`GET /v1/health` reports `protocols: [1, 2]` and the `durable_receipts` capability.
New nodes refuse reliable operations without that capability; they never silently
fall back. See [the v0.3 upgrade procedure](docs/upgrade-v0.3.md).

The recipient plugin automatically confirms transport receipt **after reliably
saving the complete message locally**. Reading or replying never requires a
manual ACK, and unread messages do not block later messages. Durable storage,
native input submission, model reading and task completion remain separate facts.
`agentbus_ack` is an optional explicit read/receipt declaration.

Stable caller request IDs and exact reply parents retain their existing
semantics. Design sources: [Matrix transaction identifiers](https://spec.matrix.org/latest/client-server-api/#transaction-identifiers),
[Matrix replies](https://spec.matrix.org/latest/client-server-api/#rich-replies),
[RabbitMQ confirmations](https://www.rabbitmq.com/docs/confirms).

## Compatibility

- Python 3.10+, Linux or WSL, and OpenSSH for cross-machine connections.
- Claude Code must expose `CLAUDE_CODE_SESSION_ID`,
  `CLAUDE_CODE_MESSAGING_SOCKET`, and its messaging authentication token when
  required. Development observations used Claude Code 2.1.321. In bypass mode,
  the tested receiver needed `"crossSessionInbound": "accept"` in its user
  settings; this permits incoming cross-session messages without individual
  approval. Enable it only for a bus whose participants you trust.
- Codex must support `codex queue --thread ... --message ...`. Development
  observations used codex-cli 0.167.0. The MCP uses tool-call metadata or Linux
  `/proc` to identify the receiving session; see [adapter notes](docs/codex-thread-id.md).
- These input routes are version-dependent. Check `codex queue --help` and the
  Claude messaging environment in your installation before relying on delivery.
  Other builds, macOS, and native Windows have not been validated here.
- The tests use fake harness endpoints. Passing tests does not establish live
  delivery compatibility with every Claude Code or Codex release.

## Set up the hub

Obtain this repository on the hub and each client machine. The examples assume
its directory is named `agentbus` and commands run from that directory. Keep a
stable checkout for the hub and for the Codex MCP process.

```bash
git clone https://github.com/DearCaat/agentbus-bridge.git agentbus
cd agentbus
```

Create a token on the hub; this writes it directly to a private file:

```bash
umask 077
mkdir -p "$HOME/.agentbus-hub"
python3 - <<'PY'
import os
import pathlib
import secrets
p = pathlib.Path.home() / ".agentbus-hub" / "token"
fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as f:
    f.write(secrets.token_hex(32) + "\n")
PY
python3 -m agentbus hub \
  --db "$HOME/.agentbus-hub/hub.db" \
  --token-file "$HOME/.agentbus-hub/token" \
  --log-file "$HOME/.agentbus-hub/hub.log"
```

The hub listens on `127.0.0.1:37460`. Keep it running in your preferred process
supervisor. Transfer the same token securely to each participating machine at
`~/.agentbus/token` and keep its permissions at `600`. The token creation command
refuses to overwrite an existing token. Never add tokens or live configuration
to this repository.

## Configure each node

The node starts automatically with the first MCP process. Set up its
configuration first:

```bash
umask 077
mkdir -p "$HOME/.agentbus"
# Use node.local.json on the hub host, node.ssh.json on a remote client.
cp -n examples/node.ssh.json "$HOME/.agentbus/config.json"
```

Edit the copied configuration:

- Give every machine a unique `machine` value, such as `laptop` or `server`.
- For SSH clients, replace `user@hub.example.com` with your hub SSH destination.
  Configure key-based access and verify its host key in advance. The node runs
  SSH noninteractively; it does not install keys or accept unknown host keys.
- On the hub host, use [node.local.json](examples/node.local.json) with
  `"tunnel": null`. On other hosts, [node.ssh.json](examples/node.ssh.json) forwards
  a local loopback port to the hub and reconnects when the tunnel fails.
- `token_file` is relative to the configuration file, or an absolute path.
  `hub_url` defaults to `http://127.0.0.1:37460`.
- `AGENTBUS_HOME` sets the node's runtime directory; the default is `~/.agentbus`.
  It must be on **local disk**, with one directory per machine. If HOME is shared
  or on FUSE, choose a local directory and optionally use `AGENTBUS_CONFIG` to
  point to a separate configuration file. `AGENTBUS_MACHINE` overrides the name.
  Pass the same settings to both harnesses on a machine.

## Install in Claude Code

This repository includes both a plugin manifest and a small marketplace. Install
the plugin directly from GitHub:

```bash
claude plugin marketplace add DearCaat/agentbus-bridge
claude plugin install agentbus@agentbus-plugins
```

The marketplace name is `agentbus-plugins`. Start a new Claude Code session after
installation. To install from a local checkout, use
`claude plugin marketplace add "$PWD"`. For a temporary development session, use
`claude --plugin-dir "$PWD"` instead. Configure the hub and node before using the
messaging tools.

The plugin starts `python3 -m agentbus mcp` with its own code directory on
`PYTHONPATH`. Set optional `AGENTBUS_*` overrides in the environment from which
Claude Code starts. Installation/configuration syntax follows the
[Claude Code plugin documentation](https://code.claude.com/docs/en/plugin-marketplaces).

## Configure Codex

Add a stdio MCP entry to your Codex `config.toml`. Replace the example checkout
and home paths with absolute paths on your machine:

```toml
[mcp_servers.agentbus]
command = "python3"
args = ["-m", "agentbus", "mcp"]

[mcp_servers.agentbus.env]
PYTHONPATH = "/path/to/agentbus"
AGENTBUS_HOME = "/home/you/.agentbus"
```

If you use separate configuration storage, also set `AGENTBUS_CONFIG` here.
Codex limits inherited MCP environment variables, so pass required settings
explicitly in this table. The
[official MCP configuration documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)
describes these fields. Start a new Codex session after configuration. Registration
may wait until the first turn or tool call exposes its thread ID.

## Use

| Tool | Purpose |
| --- | --- |
| `agentbus_whoami()` | Own address, session key and local node status |
| `agentbus_peers()` | Registered peers and online status |
| `agentbus_send(to, text, request_id, reply_to?)` | Persist one logical send; exact parent for replies |
| `agentbus_status(msg_id=... OR request_id=...)` | Observe status; provide exactly one identifier |
| `agentbus_message(msg_id)` | Retrieve retained full content and native presentation/pruning metadata |
| `agentbus_inbox(after=0, limit=50)` | Page complete retained messages, independent of unread predecessors |
| `agentbus_history(msg_id, after=0, limit=50)` | Page durable events with explicit pruning flags |
| `agentbus_ack(msg_id)` | Optionally declare read/receipt; not required for delivery or replies |
| `agentbus_rename(name)` | Set a friendly session name |
| `agentbus_control(to, action, scope, request_id)` | Submit a fixed native control; acceptance is not execution |
| `agentbus_control_status(control_id=... OR request_id=...)` | Query the durable control result after reconnecting |

An address is `<machine>/<harness>/<name>`. Targets are exact; unknown targets
fail. For conversational messages, same-machine same-harness targets use the harness's own messaging.

For each logical send, choose a unique `request_id` **before sending** and retain
it. An agent tool call looks like this (substitute an exact discovered target):

```json
{"to":"server/claude/reviewer","text":"Review revision abc123.","request_id":"review-abc123-1"}
```

After a timeout, retry with the **same** request ID, target, text and reply parent,
or query `agentbus_status(request_id="review-abc123-1")`. Reusing an ID with a
changed payload is rejected. A new ID means a new message, not a retry.

The sender node commits an outbox entry before sending over the network and
serializes acceptance in each directional stream. The hub durably stores a
message, assigns its stable `msg_id`, and returns its stream `seq`, exact peers,
`reply_to`, `blocked_by`, state revision and explicit receipt status.
`submission="accepted"` means **stored by the hub**, not received by the agent.
A locally persisted but unconfirmed send says `pending`; connectivity failure
must not be interpreted as definite non-delivery.

The recipient node validates the full message hash and byte length, commits the
immutable envelope to local SQLite with full synchronization, then automatically
confirms storage to the hub. Missing confirmation responses are retried using the
same identity. The sender sees `receiver_persisted=true` only after that durable
receipt; it does **not** mean that the model read the message.

Use a new stable request ID for a reply and set `reply_to` to the exact received
`msg_id`. The parent must belong to the same pair of peers in the opposite
direction. No read declaration is needed. `agentbus_ack(msg_id)` remains available
for an optional explicit declaration; it never holds later messages. Participants
must state business/task completion separately and bind it to the relevant task
or revision.

### Order and uncertainty

Order is per `(sending session, receiving session)`, using persisted sequence
numbers, not wall-clock timestamps. Independent streams have no global order.
The next transport message becomes eligible after the preceding envelope has
been reliably saved by the recipient plugin. Native input follows this sequence
without waiting for model reading, an optional declaration or an ordinary reply.

| Field / state | Evidence |
| --- | --- |
| `submission=pending` / `unconfirmed` | Sender saved the request; hub acceptance is unconfirmed |
| `submission=accepted` | Hub stored the message |
| `receiver_persisted=true`, `received_at` | Recipient plugin committed the complete envelope locally |
| `injection_state=injected`, `injected_at` | Native input route accepted the message; model reading is unknown |
| `receiver_acknowledged=true` | Optional explicit read/receipt declaration, not proof of understanding |
| `agent_read` | Explicit declaration metadata, if any; automatic storage never manufactures it |
| `injection_state=failed` / `in_doubt` | Native input failed or may have occurred; inspect retained inbox |
| `blocked_by_delivery` | An earlier native presentation outcome prevents safely injecting this message |
| `unknown_or_pruned` | No retained record; cannot conclude never sent or not received |

Socket/CLI timeouts and a crash during injection can remain uncertain. Nodes do
not blindly reinject an uncertain message or skip it in native presentation. This
can block later **native input**, but all retained text stays available through
`agentbus_message` and `agentbus_inbox`; unread predecessors never hide it. An
optional read declaration does not convert uncertainty into successful injection.

Every v2 envelope includes the full message, ID, sender/recipient, sequence,
reply parent, ordering scope and optional declaration instructions. UTF-8 byte
length and SHA-256 are checked before storage and native injection. Pagination
uses explicit cursors and `has_more`; inspect `body_retained`, `history_pruned`,
`injection_state` and `blocked_by_delivery`. Cached status explicitly marks current
receipt knowledge as unknown rather than a definite negative.

Legacy `/v1` traffic remains isolated for migration, with its old weaker semantics
(`delivered` meant native input submission). Already-loaded MCP processes can
retain old tool descriptions until reloaded; the upgraded node performs automatic
durable receipts regardless. Reload them to see the current optional-ACK wording.

### Durable history and cleanup

Hub SQLite records each protocol-2 state transition and its event ID, message ID,
sequence, attempt, reason and time in the **same transaction**. HTTP access logs
include request/message correlation IDs and omit body/authentication data.
Status and history remain queryable after reconnecting; log arrival time is not
message delivery order.

Use `--log-file /private/path/hub.log` to enable built-in rotation for Hub JSON
access and operational logs. The default is 5 MiB per file with three archives
(`hub.log.1` through `.3`); override with positive integers `--log-max-bytes` and
`--log-backup-count`. Current logs and retained archives have mode `600`, including
existing files. An existing oversized log is preserved as the first archive on
the next write, then ages out through normal rotation; individual records remain
whole. Without `--log-file`, output stays on stderr and this built-in rotation
does not manage shell-redirected logs. File logging writes synchronously and
flushes each record; storage errors are reported on stderr.

The running hub checks cleanup every 30 seconds, including when idle:

- Eligible message bodies: retained for 7 days by default.
- Eligible event history: retained for 30 days by default.
- Override with `--body-retention-days` and `--event-retention-days` (positive days).
- Eligibility requires an explicit declaration, or durable recipient storage plus
  successful native injection. Failed/uncertain presentation recovery copies are
  preserved; unread messages that were safely stored and injected can age out.
- Minimal IDs, fingerprints, stream counters and state tombstones remain for
  deduplication and honest status lookup; these small indexes continue to grow.
- Pruned bodies/history are explicitly marked. Cleanup never manufactures an ACK,
  invents reading, or resets a sequence number. Legacy v1 data is preserved unchanged.

Node operational logs rotate at 5 MiB with three retained archives. The recipient
node keeps the full immutable envelope for recovery and retries pending durable
receipts after restart. Accepted sender outbox bodies become eligible for cleanup
only after authoritative safe retention/pruning conditions are confirmed;
unconfirmed send bodies remain. Identity, fingerprint, sequence and receipt
metadata remain for deduplication. SQLite cleanup reuses freed pages; it is not a
promise to shrink files or securely erase backups. Protect database backups as
private data.

To diagnose, use status/history with an exact ID. For local setup issues inspect
`node.log` under `AGENTBUS_HOME`. Do not repeatedly send with new IDs as a health
check or infer task completion from a transport receipt.

## Native controls

Controls use a separate durable request/result path. They do not enter the model
conversation or depend on message reading/replies. Upgrade the hub and recipient
node to v0.4.0 and check the `native_controls` health capability before use.

For a discovered Codex target, submit:

```json
{"to":"server/codex/reviewer","action":"reload_mcp","scope":"server","request_id":"reload-reviewer-001"}
```

Query `agentbus_control_status(request_id="reload-reviewer-001")`. Keep the same
ID and payload after a timeout or MCP disconnect. `queued` means accepted;
`verified` requires fresh evidence from the exact requested thread. An accepted
native `{}` response alone does not prove replacement of an MCP process.

This action maintains the non-secret plugin-owned
`mcp_servers.agentbus.env.AGENTBUS_RELOAD_GENERATION` marker through Codex's native
configuration API, then requests MCP refresh for **all loaded threads on that
app-server**. Verification covers only the requested thread's agentbus connection:
matching generation, a changed runtime instance, expected version, all 11 tools
and a connected state. Other configuration layers are not rewritten to defeat
overrides. See [the support matrix and result states](docs/native-controls.md).

If the running MCP still exposes an older tool catalog, the updated CLI can
submit the control using an exact locally registered caller without first
reloading that MCP:

```bash
python3 -m agentbus control \
  --from-session 'laptop:codex:CALLER_THREAD_UUID' \
  --to 'server/codex/reviewer' --action reload_mcp --scope server \
  --request-id reload-reviewer-001 --wait --timeout 30
python3 -m agentbus control-status \
  --from-session 'laptop:codex:CALLER_THREAD_UUID' --request-id reload-reviewer-001
```

With `--wait`, the control CLI exits zero only after verified target evidence;
an accepted but unverified result exits nonzero and remains queryable by ID.

These are fixed native actions, without arbitrary shell commands, TTY keystrokes
or model-mediated execution. Missing capability returns `unsupported` or an
explicit capability error; messages never silently turn into control actions.

## Trust and stored data

One hub token defines one trusted group. Holders can access the bus API, discover
peer addresses and working directories, and submit messages. This release does
not provide separate user or machine authorization. Run separate hubs for groups
that should not share that access.

The hub stores message text and session metadata in SQLite. The local node stores
complete received message envelopes, sender outbox data and session delivery
information, including a Claude messaging token when supplied.
Protect runtime directories and database backups as private data. The hub binds
only to loopback; cross-machine traffic travels through SSH. No credentials or
real deployment configuration are included in the plugin.

## Development

```bash
python3 -m unittest discover -s tests -v
claude plugin validate . --strict
```

Tests create temporary databases and fake local hub, node, and harness endpoints;
they do not require a live hub or contact real sessions. The source is Python
stdlib only. Released plugin metadata is in `.claude-plugin/plugin.json`.

## License

[MIT](LICENSE). Copyright (c) 2026 Wenhao Tang.

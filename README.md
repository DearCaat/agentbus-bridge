# agentbus

Self-hosted messaging between **live** Claude Code and Codex sessions, across
harnesses and machines. Messages enter the receiving harness through its native
input route. Same-machine, same-harness communication uses the harness's own
messaging and is refused by `agentbus_send`.

```text
session ──MCP──> node (one per machine) ──SSH tunnel──> hub (SQLite)
                     └──native input──> receiving session
```

The hub can run on any suitable Linux host. No hosted service, GPU, or Python
third-party package is required. Protocol details: [SPEC.md](SPEC.md).

## Reliability protocol 2 (v0.2.0)

Upgrade the hub to v0.2.0 **before** enabling these clients. Verify that
`GET /v1/health` reports `protocols: [1, 2]`. A new client refuses reliable
operations on an old hub; it never silently sends through the old protocol.
See [the upgrade procedure](docs/upgrade-v0.2.md).

Like Matrix transaction IDs and reply relations, each send has a stable caller
request ID and each reply names its parent message. Like publisher confirms and
consumer acknowledgements in message brokers, hub acceptance and recipient ACK
are separate facts. Native injection does not prove understanding or task
completion. Design sources: [Matrix client/server specification](https://spec.matrix.org/latest/client-server-api/#transaction-identifiers),
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
| `agentbus_message(msg_id)` | Retrieve retained full content or an explicit blocked/pruned result |
| `agentbus_inbox(after=0, limit=50)` | Page received messages; later blocked content is withheld |
| `agentbus_history(msg_id, after=0, limit=50)` | Page durable events with explicit pruning flags |
| `agentbus_ack(msg_id)` | Explicitly declare receipt after reading this message |
| `agentbus_rename(name)` | Set a friendly session name |

An address is `<machine>/<harness>/<name>`. Targets are exact; unknown targets
fail, and same-machine same-harness targets use the harness's own messaging.

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

On receiving a full message, read it and explicitly call `agentbus_ack(msg_id)`.
Fetching content does not automatically ACK. Reply using a new stable request ID
and `reply_to` equal to the received `msg_id`; the parent must belong to the same
pair of peers in the opposite direction. Replies carry their exact association,
not a guess based on arrival time. Task/review completion must be stated and
bound to the relevant task/revision by the participating agents.

### Order and uncertainty

Order is per `(sending session, receiving session)`, using persisted sequence
numbers, not wall-clock timestamps. Concurrent independent streams have no
promised global order. A later message stays blocked until the preceding message
has an explicit recipient ACK; this also applies to `message` and `inbox`, which
withhold later text. Other directional streams continue independently.

| State | Evidence |
| --- | --- |
| `pending` (local) | Stored by sender; hub acceptance unconfirmed |
| `queued` | Hub accepted; awaiting recipient-side delivery |
| `leased` | A node claimed an attempt; receipt unconfirmed |
| `injected` | Native input route accepted; agent has not acknowledged |
| `acknowledged` | Recipient agent explicitly declared receipt; task completion unknown |
| `failed` / `expired` | Attempt failed or queue expired; the stream remains blocked |
| `in_doubt` | Injection may have occurred; do not blindly resend |
| `unknown_or_pruned` | No retained record; cannot conclude never sent or not received |

A missing ACK, failed injection or expired head is a visible block, not permission
to skip it. A recipient can query its inbox, retrieve the head's retained content,
and acknowledge it explicitly. An ACK is a recipient declaration, not proof of
understanding. Socket/CLI timeouts and a crash during injection remain uncertain;
existing node dedup records prevent blind reinjection. Delayed reports from an
old lease cannot overwrite a newer attempt or recipient ACK.

Every delivered envelope includes ID, sender/recipient, sequence, reply parent,
ordering scope and exact ACK instructions, followed by the entire message text.
UTF-8 byte length and SHA-256 are verified before native injection. Pagination
uses explicit cursors and `has_more`; inspect `content_visible`, `body_retained`
and `history_pruned` rather than interpreting missing content as an empty reply.
Cached sender status is labeled stale and its current ACK field is unknown.

Legacy `/v1` traffic stays isolated for migration and retains its old weaker
semantics (`delivered` only meant native input submission). Old live MCP processes
keep that interface until reloaded; installing new files alone does not upgrade
an already-running session.

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

- ACKed message bodies: retained for 7 days by default.
- ACKed event history: retained for 30 days by default.
- Override with `--body-retention-days` and `--event-retention-days` (positive days).
- Active, uncertain, failed and unacknowledged heads are not pruned by this job.
- Minimal IDs, fingerprints, stream counters and state tombstones remain for
  deduplication and honest status lookup; these small indexes continue to grow.
- Pruned bodies/history are explicitly marked. Cleanup never manufactures an ACK,
  skips a head, or resets a sequence number. Legacy data is preserved unchanged.

Node operational logs rotate at 5 MiB with three retained archives. Accepted
outbox text is cleared only after its recipient ACK and authoritative Hub body
pruning have been confirmed, at least 7 days after local creation; its identity,
fingerprint, sequence and receipt remain. Unconfirmed outbox text is kept. Hub body/event cleanup is logical
SQLite deletion: freed pages are reused; it is not a promise to shrink the DB
file or securely erase disk backups. Protect database backups as private data.

To diagnose, use status/history with an exact ID. For local setup issues inspect
`node.log` under `AGENTBUS_HOME`. Do not repeatedly send with new IDs as a health
check or infer task completion from a transport receipt.

## Trust and stored data

One hub token defines one trusted group. Holders can access the bus API, discover
peer addresses and working directories, and submit messages. This release does
not provide separate user or machine authorization. Run separate hubs for groups
that should not share that access.

The hub stores message text and session metadata in SQLite. The local node stores
session delivery information, including a Claude messaging token when supplied.
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

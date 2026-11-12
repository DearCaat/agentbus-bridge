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
  --token-file "$HOME/.agentbus-hub/token"
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
| `agentbus_whoami()` | Own address, session key, and local node status |
| `agentbus_peers()` | Registered sessions and online status |
| `agentbus_send(to, text)` | Send to an exact address or session key |
| `agentbus_status(msg_id)` | Delivery state |
| `agentbus_rename(name)` | Change the local session's friendly name |

An address is `<machine>/<harness>/<name>`; the default name is the first eight
characters of the session ID. Discover a peer, send a short message, and check
its status. Unknown targets fail with `unknown_target`; same-machine,
same-harness targets fail with `same_machine_same_harness`.

`delivered` means the input route accepted the message: a Claude socket write
succeeded or `codex queue` exited successfully. It does **not** confirm that the
model read or acted on it. A live session can be awakened or receive input while
busy, depending on its harness. Offline messages remain queued; a node crash
mid-injection can produce `in_doubt` rather than silently retrying an ambiguous
injection.

To diagnose setup, run `python3 -m agentbus peers` from the checkout and inspect
`node.log` under `AGENTBUS_HOME`. `agentbus_whoami` reports registration problems.
If `codex queue` is unavailable or Claude exposes no messaging socket, the
corresponding receiving adapter cannot work in that installation.

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

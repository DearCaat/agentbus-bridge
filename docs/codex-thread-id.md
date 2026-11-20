# Codex receiving adapter

The node needs the exact receiving thread ID and its `CODEX_HOME` to run
`codex queue --thread <thread_id> --message <text>`. This document describes the
implemented discovery strategy, without deployment-specific paths or session
identifiers.

## Automatic registration on managed app servers

The node discovers loaded user threads through the local Codex app-server control
socket. It registers them during its heartbeat and marks an automatically
registered thread offline after Codex unloads it. Sub-agents are excluded from
this discovery. Existing tool-based registrations continue to work.

This path needs the node's `CODEX_HOME` (default `~/.codex`) to match the managed
Codex app-server, and the WebSocket dependency in `requirements-control.txt`.
The node also accepts the dependency from its existing `control-deps` runtime
directory. Other Codex sessions use the MCP discovery below.

## Discovery order

1. Tool-call metadata: until the thread ID is confirmed by a tool call, the MCP
   reads `params._meta.threadId`, falling back to
   `params._meta["x-codex-turn-metadata"].thread_id` when present. This ID
   supersedes any earlier registration inferred from an open rollout.
2. Linux process file descriptors: before the first tool call, the MCP checks
   `/proc/<parent-pid>/fd/` for open files whose names match
   `rollout-<timestamp>-<thread-uuid>.jsonl`. It registers only when exactly one
   distinct matching thread ID exists. Multiple IDs are ambiguous, so it waits
   for tool-call metadata.

Neither source may exist at MCP initialization. A background watcher retries;
registration can therefore be deferred until the first turn or tool call. This
adapter does not choose a session based on its working directory or recency.
Once tool-call metadata confirms a thread ID, later calls reuse it.

## Receiving environment

The MCP uses an explicitly configured `CODEX_HOME` if present. Otherwise it reads
that key from the parent process's `/proc/<parent-pid>/environ`; other values are
not forwarded to the hub. Process inspection can be restricted by the host.

Configure required MCP environment variables explicitly through
`mcp_servers.agentbus.env`, including `PYTHONPATH` and any `AGENTBUS_*` overrides.
The receiver's `codex` executable must be reachable through the node's `PATH`;
the node configuration also accepts `codex_bin` to select its path.

## Compatibility and verification

Development observations used codex-cli 0.167.0. The metadata fields, rollout
file naming convention, process-parent relationship, and `queue` command are
version-dependent integration assumptions. The official MCP configuration
interface does not itself guarantee these receiving behaviors.

Before using a new build, check `codex queue --help`, call `agentbus_whoami` in
the target session, and send a short message from a supported peer. Verify that
it appears in the intended live session. `agentbus_status` alone confirms only
the adapter's submission result, not that the model consumed it.

The repository's MCP tests exercise metadata and rollout discovery with fake
node endpoints. Node tests use a fake `codex` binary to check argv and environment.
They do not establish compatibility with an arbitrary installed Codex build.

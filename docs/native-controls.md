# Native controls (v0.4.0)

**Existing ordinary Claude Code interactive sessions are unsupported for native
remote plugin reload.** Their messaging socket intentionally skips slash
commands. Agentbus does not implement a Claude SDK adapter: successful SDK
research probes do not make an already-running interactive session controllable.
Sending `/reload-plugins` with `agentbus_send` is still an ordinary text message.

Codex MCP reload uses a real native control API, with explicit server scope and
separate target-thread verification. It is not a conversational instruction.

## Support matrix

| Target / request | Support and actual scope |
| --- | --- |
| Registered Codex thread in its verified owning app-server, `reload_mcp`, `server` | Supported native adapter; requests refresh for all loaded threads on that app-server |
| Codex `reload_mcp`, `session` | `unsupported`; no silent widening to server scope |
| Ordinary live Claude Code interactive session, `reload_plugins` | `unsupported`; no reload is executed |
| Claude SDK-managed session | No agentbus adapter in this release; SDK research is not shipped support |
| Missing dependency, no native endpoint, wrong runtime/thread or unavailable capability | Explicit unsupported/failure/capability result; never success by text fallback |

Codex discovery verifies the registered MCP process and its app-server ancestry,
process ownership and identity, the matching `CODEX_HOME`, the private Unix socket
and its actual peer credentials. The exact registered thread must be loaded in
that server. An address alone is not proof that a runtime can be controlled.
The adapter is version-dependent; other native API versions/platforms are not
implicitly supported.

## Optional dependency

Ordinary messaging, durable receipts and the hub remain Python stdlib-only.
Install the optional native transport on each **recipient node** that will run
Codex controls, from the checked-out release:

```bash
python3 -m pip install \
  --target "${AGENTBUS_HOME:-$HOME/.agentbus}/control-deps" \
  -r requirements-control.txt
```

The requirement is `websocket-client>=1.9,<2`. The node executes `control-deps`
from its local runtime home. Use the actual configured
`AGENTBUS_HOME` when the default differs. This directory stays independent of
versioned plugin copies. Agentbus does not download dependencies during a
control request; a missing dependency returns `unsupported`.

For containers whose local runtime is restored at startup, a predownloaded
websocket-client wheel may be cached as `control-websocket-client.whl` beside the
node configuration. On startup the node validates this single known archive and
restores it under the local `control-deps` directory. It does not scan persistent
storage, execute modules from the persistent mount, or download packages. Existing
dependency directories are preserved. This cache is optional and contains no
credentials.

Upgrade the hub and recipient node first. Health must advertise both the usual
`durable_receipts` capability and `native_controls`. Installing new files alone
does not change an already-loaded MCP tool catalog.

## Submit and query

Discover exact addresses with peers. Choose a stable request ID before the first
attempt and retain its target, action and scope. With the v0.4 tool catalog:

```json
{
  "to": "server/codex/reviewer",
  "action": "reload_mcp",
  "scope": "server",
  "request_id": "reload-reviewer-001"
}
```

Call `agentbus_control` with that object. Query the result using exactly one of:

```json
{"request_id":"reload-reviewer-001"}
```

```json
{"control_id":"RETURNED_CONTROL_ID"}
```

Those query objects go to `agentbus_control_status`. A repeated submission with
the same ID and payload returns the same logical control. Reusing it for a
changed target/action/scope is an idempotency conflict. After uncertain transport,
reuse that ID; allocating another ID requests a new native mutation.

The fixed action vocabulary is `reload_mcp` / `reload_plugins` and explicit scope
`server` / `session`; only the combinations in the support matrix execute.
There is no arbitrary native-method, shell, prompt or argument passthrough.
Controls are independent from the same-machine/same-harness restriction on
ordinary message sending; they still require an exact registered local caller
and an eligible exact target.

### Bootstrap an older loaded MCP

Use the updated CLI against the running node and an exact locally registered
caller key. This does not require the caller's old MCP catalog to expose the new
tools:

```bash
python3 -m agentbus control \
  --from-session 'laptop:codex:CALLER_THREAD_UUID' \
  --to 'server/codex/reviewer' \
  --action reload_mcp --scope server --request-id reload-reviewer-001

python3 -m agentbus control-status \
  --from-session 'laptop:codex:CALLER_THREAD_UUID' \
  --request-id reload-reviewer-001
```

Set the same runtime/config environment used by that caller's node. Without `--wait`, a zero CLI exit means the local request was accepted, not that
native execution or verification succeeded. Add `--wait --timeout 30` to the
`control` command to wait: this form exits zero only for `verified` with
`verification_confirmed=true`; unsupported, uncertain, unverified or timed-out
results exit nonzero. Timeout must be positive and finite and does not re-execute
the native action. The same request ID remains queryable afterward. The old
conversational `send` CLI is not a native control substitute.

## What Codex reload changes

Codex's reload can reuse an old MCP connection when configuration is unchanged.
An empty native reload response therefore cannot prove that new agentbus code
was loaded. Agentbus uses the official `config/value/write` API to maintain only
this non-secret plugin-owned setting in the owning `CODEX_HOME` configuration:

```text
mcp_servers.agentbus.env.AGENTBUS_RELOAD_GENERATION
```

It then calls `config/mcpServer/reload` with null parameters. This action changes
the marker and requests MCP refresh across all loaded threads in the same
app-server; other configured MCP servers may also be refreshed. It is not a
session-only reload. The marker is deliberate configuration maintenance, not a
credential. Agentbus does not rewrite higher/lower user configuration layers to
force the marker through an override, change permissions or fix an unsupported
runtime by guessing an endpoint.

Verification reads native MCP status for only the requested thread and the
`agentbus` server. A `verified` result requires all of:

- The expected agentbus package version and all 11 required tools are present.
- The native MCP runtime is connected and has no tool-discovery error.
- Initialization metadata under `experimental.agentbus.reload_generation`
  matches this action's marker.
- `experimental.agentbus.runtime_id` differs from the prior connection's runtime
  ID. These identifiers are captured at MCP startup, not reread from a mutated
  environment.

The result reports `effective_scope="server"` but
`verification.scope="target_thread"`. It does not assert that every other loaded
thread has completed its refresh. Native acceptance without this evidence is
`applied_unverified`; a transport interruption after a mutation can be `in_doubt`.
None of these states implies model reading or task completion.

## Durable execution and result states

The caller's node persists a control outbox before submission. The hub stores a
separate control job/event history, with exact caller/recipient identity and a
stable `control_id`. The recipient node journals intent before the native
mutation and crosses an explicit hub execution barrier. This path does not
consume a message sequence, acknowledge a conversation or insert a user prompt.

| State | Meaning |
| --- | --- |
| `submission_unconfirmed` | Caller node saved the request; hub acceptance and native execution are unknown |
| `queued` | Hub stored the control; no native execution is confirmed |
| `leased` | A recipient worker claimed it; execution has not been confirmed |
| `executing` | Execution barrier committed; native outcome is not yet established |
| `verified` | Native action accepted and the exact target's required runtime evidence matched |
| `applied_unverified` | Native refresh accepted; target verification did not establish the required evidence |
| `unsupported` | Capability/action/scope unsupported; no native action executed |
| `failed` | A definite failure was reported; inspect the result code and evidence |
| `in_doubt` | Native outcome unknown; no automatic native retry |
| `expired` | Queued request expired before execution |
| `unknown` / `unknown_or_pruned` | No current evidence; cannot conclude that no action ran |

Read `execution_confirmed`, `verification_confirmed`, `result.code`,
`result.native_accepted`, `result.applied` and the scoped verification metadata.
Null/unknown is not false. `native_accepted` reports the native protocol, while
`verified` reports observed target runtime evidence. Cached results mark current
knowledge unknown and retain their last observation separately.

A reload may close the MCP process that submitted the control. The action and
result belong to the independent node/hub, so query the same ID after reconnecting
or from the CLI with the same registered caller identity. Replacing a caller
session with a different session key does not transfer that identity's request
lookup; use an authorized participant and the exact control ID where appropriate.

A lost submit response can safely retry the same logical request. A pre-execution
lease can be reoffered after expiry and is fenced by owner/attempt. Once execution
begins, an uncertain outcome is never automatically run again. Recipient recovery
retains/reports a journaled result or marks interrupted execution `in_doubt`.
Controls with no result should be investigated by ID, not blindly resubmitted with
a new ID.

Queued jobs expire after five minutes without execution. Terminal detailed
results/events age out after 30 days, with identity/fingerprint/state tombstones
and explicit pruning metadata retained. This logical cleanup does not promise
physical file shrinking or erase private backups.

## Trust and limits

Native controls use the existing trusted hub group, local Unix-socket permissions
and registered runtime checks. They do not add separate per-user authorization.
Approved metadata is bounded and excludes credentials, raw configurations,
headers and command bodies. Keep node/hub databases and backups private.

The Claude SDK exposes a real `reloadPlugins()` control for its owning host, but
agentbus does not hold a generic SDK control connection to an ordinary already
running Claude terminal. Spawning a new SDK session or resuming a stored
transcript would not reload that existing process, so this release reports
`unsupported` instead. It does not simulate TTY input, overwrite a prompt draft,
interrupt a task or ask a model to pretend it executed the reload.

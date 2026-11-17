# Upgrade to v0.3 automatic durable receipts

v0.3 replaces manual-ACK transport gating with automatic receipt after the
recipient plugin has reliably saved the complete envelope. Reading and replying
need no manual ACK. Stable send request IDs and exact `reply_to` relations stay
the same. `agentbus_ack` remains an optional explicit read/receipt declaration.

## Prepare and upgrade the hub first

1. Identify the actual authorized supervisor, command, code, private DB and token
   paths. Preserve existing identities, credentials and loopback/SSH routing.
   A forwarding-only SSH key is not a management route.
2. Keep old code and take a consistent live SQLite backup using the backup API,
   including WAL transactions. Back up each node database consistently too:
   local full envelopes/outbox entries are recovery data, not disposable caches.
   Use new private backup destinations; do not overwrite old copies.
3. Pin `v0.3.0` in a separate code directory. Smoke-test a temporary Hub, DB,
   token and local port with fake recipients; never send test traffic to active
   sessions. Health must advertise `durable_receipts` as well as protocol 2.
4. Upgrade only the Hub through its existing supervisor, retaining the same
   DB/token/port and private rotating `--log-file`. Existing ACK timestamps seed
   durable receipt timestamps; no new model reading declaration is fabricated.
5. Check actual `/v1/health`: version `0.3.0`, `protocols: [1, 2]` and
   `capabilities` containing `durable_receipts`. Verify startup/access JSON logs
   and private DB/log permissions before enabling new nodes.

For backup API details, see the preserved [v0.2 instructions](upgrade-v0.2.md).
Do not copy only a live main `.db` file while ignoring its WAL.

## Update the nodes and MCP

Update each existing installation to the pinned public release, preserving
marketplace/plugin identity, private configuration and machine names. Use its
normal node ensure/update mechanism. A current node explicitly refuses reliable
operations on a Hub lacking `durable_receipts`; no silent fallback is allowed.
No Space or unrelated service restart is required.

Nodes migrate receive metadata by fetching and verifying the original full
recipient envelope, committing it locally, and sending the new durable receipt.
Historical native input outcomes are preserved, so injected/uncertain messages
are not blindly notified again. This reconciliation also handles old heads that
were injected but never manually acknowledged. Allow it to advance the real
backlog; **do not mark messages read on agents' behalf**.

Reload MCP/plugin sessions when convenient to expose current tool wording.
Already-loaded older MCP descriptions may still suggest manual ACKs; node-level
automatic durable receipt works independently. Stable `request_id` and `reply_to`
remain valid across this update.

## Verify and recover honestly

Inspect exact existing IDs, statuses and retained inboxes without unrelated test
messages. Confirm per-direction sequence progress and `receiver_persisted=true`
without requiring `receiver_acknowledged=true`. `injection_state=injected` means
native input accepted, never proof of model reading or task completion.

A prior ambiguous/failed injection can still hold later native input to avoid
blind duplicate notification. Check `blocked_by_delivery`; all retained messages
remain readable through message/inbox tools. Do not force a read declaration to
turn an uncertain native outcome into successful injection.

Preserve old code and all new durable records for rollback. Restoring an old DB
snapshot after new messages exist loses accepted data. Older nodes use manual-ACK
semantics and cannot provide v0.3 receipt/recovery guarantees. Prefer a forward
fix or explicitly reviewed rollback; report actual frozen/uncertain messages
instead of claiming rollout complete from release metadata alone.

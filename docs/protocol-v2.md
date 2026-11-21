# Reliability protocol 2 with durable receipts (v0.3)

Protocol 2 uses separate SQLite tables and `/v2` endpoints. Health reports
`protocols: [1, 2]`, package version and `capabilities: ["durable_receipts"]`.
Current nodes require that capability. Protocol 1 remains migration compatibility
without these guarantees.

## Identity and order

`client_msg_id` is the MCP caller's stable `request_id`. The sender node commits
an outbox payload and allocates `client_seq` per exact `(from_key,to_key)` before
network submission. Older unconfirmed sends are retried before later sends in
that stream. Hub sequence checks and immutable payload fingerprints reject a
reused key with changed content, peer, sequence or reply parent. Tombstones keep
IDs and sequence identity after text pruning.

`reply_to` names an existing opposite-direction message between the same peers.
It is independent of optional reading declarations and sequence order. A reply
must not pretend to know pruned content. Neither a reply relation nor any
transport state establishes business completion.

## Automatic receipt and native presentation

The hub leases the earliest message not yet durably received in each directional
stream. Every lease carries an attempt number. The recipient node checks full
UTF-8 byte length and SHA-256 and transactionally persists the immutable envelope
with SQLite full synchronization **before** requesting `/v2/receive`.

The hub accepts a durable receipt only for the exact recipient, current attempt,
matching immutable hash/byte length and already-received predecessors. Duplicate
receipts preserve the original time. Lost responses and crashes after local
commit are recovered by replaying the same receipt, never by claiming persistence
before it happened. The next transport message is released on this receipt,
without waiting for model reading or replies.

Native input is a separate ordered step. The node records `injecting` before
writing; success becomes `injected`. An ambiguous socket/CLI result or interrupted
write becomes `in_doubt`, and is never blindly reinjected. Failed/uncertain native
presentation can block later native input, identified by `blocked_by_delivery`.
A hub-accepted reply from the recipient to the original sender clears that
message as a blocker while retaining the original native injection outcome.
It does **not** hide retained inbox/message text or stop later durable transport
receipts. Full local envelopes support restart recovery.

`agentbus_ack` / `/v2/ack` remains an optional explicit agent read/receipt
declaration. It never gates later messages, changes their native injection state,
or manufactures understanding/task completion. Ordinary retrieval and replies
need no such declaration. Automatic storage does not set `receiver_acknowledged`.
The native harness and SQLite do not share a transaction: exactly-once model
reading or task completion is not claimed.

## API and status

All endpoints use existing bearer authentication and trusted-group boundaries.
The local node binds tool operations to a registered session.

| Endpoint | Input |
| --- | --- |
| POST `/v2/send` | `from_key,to,text,client_msg_id,client_seq,reply_to?` |
| GET `/v2/pull` | `machine,wait` |
| POST `/v2/receive` | `msg_id,session_key,attempt,sha256,text_bytes` |
| POST `/v2/report` | `msg_id,attempt,state,detail?` |
| POST `/v2/ack` | `msg_id,session_key` (optional recipient declaration) |
| GET `/v2/status` | `msg_id,session_key` (participant) |
| GET `/v2/message` | `msg_id,session_key` (participant) |
| GET `/v2/inbox` | `session_key,after=0,limit=50` (recipient) |
| GET `/v2/history` | `msg_id,session_key,after=0,limit=50` (participant) |

Snapshots retain exact peers, request/server ID, stream sequence, reply parent,
attempt, state revision and observation time. Interpret these layers separately:

- `submission=accepted`: hub storage only.
- `receiver_persisted`, `received_at`: reliable recipient-plugin storage only.
- `injection_state`, `injected_at`: native presentation outcome, not model reading.
- `receiver_acknowledged` / `agent_read`: optional explicit self-declaration,
  not proof of understanding or completion.
- Task completion remains unknown unless established by the participants' actual
  business exchange; it is not inferred by this transport.

Cached/unknown current receipt values are not definitive negatives. Missing IDs
return `unknown_or_pruned`, not proof of no sending or receiving. The sender can
query its local stable request ID when the server ID response was lost.

Message/inbox results include all retained full text, byte length and hash,
regardless of an unread predecessor. `blocked_by` describes incomplete durable
transport; `blocked_by_delivery` describes native input uncertainty. Neither is
permission to invent a read event. Inbox/history pages expose `next_cursor`,
`has_more` and completeness/pruning metadata; limits are 1..200. Inbox insertion
cursors are not global causal ordering across different senders.

## Migration, audit and retention

Existing explicit ACKs initialize `received_at` from the historical ACK time.
Old `injected` or `in_doubt` metadata never creates a read declaration. The node
reconciles old receive records by fetching the original envelope for the exact
recipient, verifying and committing it, then requesting a durable receipt. It
preserves prior native outcomes to avoid duplicate notifications, including old
heads that normal pull no longer offers.

State/event changes commit together. Private rotating JSON logs correlate IDs
and omit headers, message bodies and exception details. Cleanup runs every 30
seconds. Default eligible body/history windows are 7/30 days; positive CLI flags
adjust them. Eligibility uses explicit ACK or reliable storage plus successful
native injection; failed/uncertain presentation recovery copies are preserved.
Minimal identity, fingerprint and stream counters remain. Cleanup never creates
reading events or resets sequence identity. Legacy v1 records remain unchanged.

SQLite deletion is logical cleanup, not secure erasure or automatic VACUUM.
Private database backups need their own retention policy. Runtime DBs contain
complete message bodies and must remain private; no tokens or deployment keys
belong in this repository.

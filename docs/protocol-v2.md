# Reliability protocol 2

The client, node and hub use independent v2 tables and `/v2` endpoints. `GET
/v1/health` advertises the supported protocol list and package version. Existing
v1 endpoints are migration compatibility only, without v2 reliability promises.

## Identity and directional order

A sender supplies `client_msg_id` (the agent tool's `request_id`). Its local node
atomically persists the payload and allocates `client_seq` for the exact
`(from_session_key,to_session_key)` stream before sending to the hub. This local
allocation order defines concurrent submissions; wall-clock time does not.
Unconfirmed older outbox entries are retried before later entries in that stream.

The hub atomically checks the expected sequence, deduplicates the request key,
stores the message and event, and returns a stable server `msg_id`. Changed
payload/sequence with a reused key is rejected. The sender preserves keys,
fingerprints and sequences even when accepted local message text is pruned.

`reply_to` names an existing opposite-direction message between the exact same
peers. A blocked or pruned parent cannot be used to pretend its context is known.
It is independent of sequence: sequence orders delivery, reply association binds
meaning to a particular request. Neither proves task completion.

## Delivery and acknowledgement

The hub leases only the earliest unacknowledged message of each directional
stream. Lease expiry makes that same head available again; it does not release a
later message. Every claim increments an attempt number. Node reports are fenced
by attempt and cannot roll back an ACK. Duplicate reports are idempotent.

The node checks the current head/attempt and complete UTF-8 hash/size before
native injection. It persists `injecting` before writing. On success it records
`injected`; an ambiguous write/CLI failure or interrupted injection becomes
`in_doubt`. Previously injected/uncertain IDs are never blindly injected again.
An obsolete lease cannot be used to start a fresh injection after another claim.
This is not an exactly-once guarantee: the harness's native input is not
transactional with node SQLite, and there is no proof of model understanding.

The receiving agent calls `agentbus_ack(msg_id)` explicitly after reading the
full envelope. Only the stream head can be ACKed. ACKs can race node reports and
remain final. Fetching content is not an implicit ACK. A failed/expired/uncertain
head stays visible and blocks the stream until the recipient explicitly retrieves
and acknowledges it. Other directional streams remain independent.

## API

All endpoints require the existing hub bearer authentication. The bus remains a
single trusted group; the node binds operations to locally registered sessions.

| Endpoint | Input |
| --- | --- |
| POST `/v2/send` | `from_key,to,text,client_msg_id,client_seq,reply_to?` |
| GET `/v2/pull` | `machine,wait` |
| POST `/v2/report` | `msg_id,attempt,state,detail?` |
| POST `/v2/ack` | `msg_id,session_key` (recipient) |
| GET `/v2/status` | `msg_id,session_key` (participant) |
| GET `/v2/message` | `msg_id,session_key` (participant) |
| GET `/v2/inbox` | `session_key,after=0,limit=50` (recipient) |
| GET `/v2/history` | `msg_id,session_key,after=0,limit=50` (participant) |

Snapshots identify exact peers, request/server ID, stream sequence, reply parent,
state, attempt, blocking predecessor, state revision and observation time.
`submission=accepted` confirms hub persistence only. `receiver_acknowledged`
identifies an explicit recipient declaration; unknown/cached current receipt is
not represented as a definite negative. Missing IDs return `unknown_or_pruned`,
not proof of non-delivery. The sender can also query its local persisted request
ID when it never received a server ID.

A message query includes full retained text, UTF-8 byte length and SHA-256. The
node/MCP withholds text for a receiver's later blocked message, marking
`content_visible=false`. Metadata is still queryable to explain the missing
predecessor. Sending peers can inspect their own outgoing content.

Inbox and history use monotonic server cursors with `next_cursor`, `has_more`
and explicit completeness/pruning fields; limits are 1..200. An inbox may span
several independent directional streams. Its server insertion cursor is not a
promise that messages from different senders have a global causal order.

## Audit and retention

State changes and events commit together with SQLite full synchronization. Events
include monotonically increasing event ID, message ID, sequence, attempt, state,
reason and timestamp. HTTP JSON access logs correlate request/message IDs and
omit authentication headers and message bodies. History pruning is explicit.

The hub runs cleanup every 30 seconds, even with no requests. Defaults: ACKed
body retention 7 days, ACKed history retention 30 days. Positive CLI flags adjust
those windows. Active and unacknowledged heads are preserved. Minimal message
metadata/fingerprints and stream counters are retained to avoid resurrecting an
old request as a new send; these small indexes can grow over time. No cleanup
implicitly ACKs or skips a message. Legacy v1 data is preserved unchanged.

Retention is logical database cleanup, not secure erasure or an automatic VACUUM.
Private backups retain their own contents and require their own retention policy.
No tokens, private routing configuration or deployment keys belong in this repo.

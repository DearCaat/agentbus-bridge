# Upgrade to protocol 2

v0.2.0 changes the agent interface: `agentbus_send` requires a stable caller-chosen
`request_id`; replies can name `reply_to`; recipients explicitly acknowledge each
message. It adds ordered directional streams and durable queryable history.

## Prepare the hub first

1. Identify the actual hub supervisor, command, code checkout, DB and token-file
   paths using your existing authorized management route. A forwarding-only SSH
   key is not a management route. Keep the existing loopback/SSH topology.
2. Record the old version/command and retain its code. Use SQLite's backup API
   for a consistent copy of a live DB, including its WAL transactions; copying
   only the main `.db` file is not a valid live backup.
3. Check out `v0.2.1` in a separate directory. Verify it with a temporary database
   and token on a separate local port, then stop that temporary process. Do not
   use live sessions as a smoke-test destination.
4. Stop only the old hub through its existing supervisor and start the new code
   with the **same** DB/token/loopback port. Set `--log-file` to a private service
   log path to enable built-in bounded access-log rotation. The new code adds independent v2
   tables; legacy v1 tables and messages are preserved. Optional retention flags
   are documented in README. No external broker or new public listener is needed.
5. Verify `/v1/health` returns `version: "0.2.1"` and `protocols: [1, 2]` before
   activating new clients. Inspect correlated JSON access logs and DB permissions.

A live backup can be made on the hub using this example (replace paths):

```python
import sqlite3
source = sqlite3.connect("/path/to/existing-hub.db")
backup = sqlite3.connect("/path/to/private-backups/pre-v0.2.db")
try:
    source.backup(backup)
finally:
    backup.close()
    source.close()
```

Do not overwrite an existing backup. The hub stores message content and metadata;
backup location/permissions must preserve the existing private deployment policy.

## Update clients

Update each installation's existing marketplace entry to this public repository
at `v0.2.1`, preserving its installed identity and private configuration. Existing
processes keep their loaded code. Reload the plugin or start a new session when
ready; its node ensure mechanism adopts the new package version. A new client
refuses reliable operations if the hub is not protocol 2, with an explicit error.
It never silently falls back to the old delivery behavior.

Check whoami/peers and the inbox without sending to unrelated live sessions. An
agent should retrieve and read the current head, explicitly ACK its message ID,
and put that ID in `reply_to` when replying. Reuse request IDs for retries only.

## Rollback and retained data

Before any real v2 message is accepted, rollback can restore the old code and
consistent pre-upgrade DB through the original supervisor. Preserve the failed
new DB for diagnosis; do not delete or overwrite it without operator approval.

After real v2 messages exist, restoring an old snapshot would discard those
records. Preserve the current DB. Older code can use its v1 tables but cannot
serve v2 records; v2 callers must be told the protocol is unavailable. Plan a
forward fix or an explicitly reviewed rollback rather than reporting success
while messages are frozen.

The plugin does not restart a Space or unrelated services. Deployment completion
means the actual hub and installed clients are checked, not merely a new release
or marketplace version appearing on GitHub.

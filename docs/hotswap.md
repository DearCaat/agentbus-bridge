# In-session hot swap (0.4.2)

agentbus MCP: switch to a newer installed plugin version inside a running session.

A plugin update installs into a new cache/<marketplace>/agentbus/<version>/ directory; a session that is
already open keeps running the MCP process from the old directory. After each answered request the MCP checks
whether the installed agentbus is now another version in the same cache parent. If that version passes a
self-test in a subprocess, the process os.execve's into it (same pid, same stdin/stdout, so the session sees
no disconnect), re-registers with the node, and sends notifications/tools/list_changed so the client re-reads
the tool list (Claude Code 2.1.327, docker3, 11-18: re-list within 3 ms; without the notification new tools
stay invisible).

Delivery, injection and reliability live in the node daemon, not here: a newer MCP's `node --ensure` already
replaces an older node (node.py), so this only brings the thin MCP layer up to date.

Never switch when unsure: not running from the plugin cache (--plugin-dir, a dev checkout), no install record,
target not in the same cache parent, self-test failed or timed out, harness is not claude, the node watchdog
is busy (holds SWAP_LOCK), or AGENTBUS_NO_HOTSWAP is set. A target that failed once is not retried.

Set AGENTBUS_NO_HOTSWAP=1 to turn it off. Only processes that already run 0.4.2 or later switch; older sessions need one restart.

"""Read model and effort from the harness record for one exact session."""

import json
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path


MODEL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/+@-]{0,127}")
EFFORT = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}")
CODEX_STATE_DB = re.compile(r"state_(\d+)\.sqlite")
TRANSCRIPT_TAIL = 2 * 1024 * 1024


def model_metadata(model, effort):
    if not isinstance(model, str) or not MODEL_NAME.fullmatch(model):
        return {}
    return {
        "model_name": model,
        "effort": effort if isinstance(effort, str) and EFFORT.fullmatch(effort) else None,
    }


def _codex_metadata(home, session_id):
    def schema_version(path):
        match = CODEX_STATE_DB.fullmatch(path.name)
        return int(match[1]) if match else -1

    try:
        databases = sorted(Path(home).glob("state_*.sqlite"), key=schema_version, reverse=True)
        database = next((path for path in databases if schema_version(path) >= 0), None)
        if database is None:
            return {}
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)) as conn:
            row = conn.execute(
                "SELECT model, reasoning_effort FROM threads WHERE id=?", (session_id,),
            ).fetchone()
        return model_metadata(*row) if row else {}
    except (OSError, sqlite3.Error):
        return {}


def _claude_metadata(config, cwd, session_id):
    if not cwd or not re.fullmatch(r"[A-Za-z0-9-]{1,128}", session_id):
        return {}
    project = re.sub(r"[^A-Za-z0-9-]", "-", cwd)
    transcript = Path(config) / "projects" / project / (session_id + ".jsonl")
    try:
        with transcript.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            start = max(0, size - TRANSCRIPT_TAIL)
            stream.seek(start)
            if start:
                stream.readline(TRANSCRIPT_TAIL)
            lines = stream.read(TRANSCRIPT_TAIL).splitlines()
    except OSError:
        return {}
    for line in reversed(lines):
        try:
            record = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        if (not isinstance(record, dict) or record.get("type") != "assistant"
                or record.get("sessionId") != session_id or record.get("isSidechain")):
            continue
        message = record.get("message")
        metadata = model_metadata(
            message.get("model") if isinstance(message, dict) else None,
            record.get("perTurnEffort") or record.get("effort"),
        )
        if metadata:
            return metadata
    return {}


def session_metadata(harness, session_id, cwd, deliver):
    if harness == "codex" and deliver.get("codex_home"):
        return _codex_metadata(deliver["codex_home"], session_id)
    if harness == "claude":
        config = deliver.get("claude_config_dir") or os.path.expanduser("~/.claude")
        return _claude_metadata(config, cwd, session_id)
    return {}

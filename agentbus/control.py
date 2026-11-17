"""Durable native reload jobs, independent from conversational messages."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid
import zipfile

from .hubclient import HubError

DDL = '''
CREATE TABLE IF NOT EXISTS control_outbox (
 from_key TEXT NOT NULL, request_id TEXT NOT NULL, target TEXT NOT NULL,
 payload TEXT NOT NULL, fingerprint TEXT NOT NULL, response TEXT, error TEXT,
 PRIMARY KEY(from_key,request_id)
);
CREATE TABLE IF NOT EXISTS control_journal (
 control_id TEXT PRIMARY KEY, job TEXT NOT NULL, state TEXT NOT NULL,
 result TEXT, reported INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL
);
'''
ACTIONS = ('reload_plugins', 'reload_mcp')
TERMINAL = ('verified', 'applied_unverified', 'unsupported', 'failed', 'in_doubt', 'expired')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def no_action(state, code, summary, harness=None):
    return {'state': state, 'result': dict(code=code, summary=summary, harness=harness,
            effective_scope='none', native_accepted=False, applied=False, verified=False)}


def process_identity(session):
    """Bind an old registration to the same live, owned process, including PID reuse."""
    pid = int(session.get('mcp_pid') or 0)
    if pid <= 0 or os.stat('/proc/%d' % pid).st_uid != os.getuid():
        raise ValueError('target_not_owned')
    with open('/proc/%d/stat' % pid) as f:
        fields = f.read().rsplit(')', 1)[1].split()
    if fields[0] in ('Z', 'X', 'x'):
        raise ValueError('target_offline')
    ticks = int(fields[19])
    with open('/proc/stat') as f:
        boot = next(int(line.split()[1]) for line in f if line.startswith('btime '))
    began = boot + ticks / os.sysconf('SC_CLK_TCK')
    if began > float(session.get('registered_at') or 0) + 2:
        raise ValueError('target_pid_reused')
    return {'pid': pid, 'start_ticks': ticks}


def restore_transport(node):
    """Restore the optional pure-Python wheel to local runtime storage.

    A persistent mount is read only as one known archive, never scanned or used
    as the executable package directory. No network or package installer runs.
    """
    destination=Path(node.p.home)/'control-deps'
    if (destination/'websocket/__init__.py').is_file():
        return
    archive=Path(node.p.config).parent/'control-websocket-client.whl'
    if not archive.is_file():
        return
    with zipfile.ZipFile(archive) as wheel:
        entries=wheel.infolist()
        if len(entries)>256 or sum(e.file_size for e in entries)>16*1024*1024:
            raise ValueError('invalid_control_dependency_archive')
        for e in entries:
            p=Path(e.filename)
            if p.is_absolute() or '..' in p.parts or not p.parts or not (p.parts[0]=='websocket' or p.parts[0].startswith('websocket_client-') and p.parts[0].endswith('.dist-info')):
                raise ValueError('invalid_control_dependency_archive')
            if (e.external_attr>>16)&0o170000==0o120000:
                raise ValueError('invalid_control_dependency_archive')
        metadata=next((e for e in entries if e.filename.endswith('.dist-info/METADATA')),None)
        if metadata is None or b'Name: websocket-client\n' not in wheel.read(metadata):
            raise ValueError('invalid_control_dependency_archive')
        os.makedirs(node.p.home,mode=0o700,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.control-restore-',dir=node.p.home) as staging:
            wheel.extractall(staging)
            if not (Path(staging)/'websocket/__init__.py').is_file():
                raise ValueError('invalid_control_dependency_archive')
            if not destination.exists():
                os.rename(staging,destination)


class Controller:
    def __init__(self, node):
        self.node, self.store = node, node.store
        self.lock = threading.RLock()
        self.owner = uuid.uuid4().hex
        self.check_at = 0.0
        self.supported = False
        with self.store._lock:
            self.store._db.executescript(DDL)
        try:
            restore_transport(node)
        except (OSError,ValueError,zipfile.BadZipFile):
            self.node._log('optional native transport cache was not restored')

    def rows(self, sql, args=()):
        return [dict(r) for r in self.store._query(sql, args)]

    def row(self, sql, args=()):
        rows = self.rows(sql, args)
        return rows[0] if rows else None

    def available(self):
        if self.supported:
            return True
        if time.monotonic() < self.check_at:
            return False
        self.check_at = time.monotonic() + 2
        try:
            self.supported = 'native_controls' in self.node.hub.health().get('capabilities', [])
        except HubError:
            pass
        return self.supported

    def dispatch(self, req):
        key = self.node._session_key(req)
        if not key or self.store.get_session(key) is None:
            return {'ok': False, 'error': 'not_registered'}
        try:
            if req.get('op') == 'control_status2':
                return self.status(key, req)
            if not self.available():
                return {'ok': False, 'error': 'control_capability_unconfirmed',
                        'summary': 'Hub native control support is not confirmed. No new attempt was started by this call; an earlier request outcome may still be unknown. Query/retry with the SAME request_id.'}
            return self.submit(key, req)
        except (KeyError, TypeError, ValueError) as e:
            return {'ok': False, 'error': 'invalid_control_request', 'summary': str(e)}
        except HubError as e:
            return {'ok': False, 'error': e.code, 'summary': 'Native action status is unknown; retry/query using the same request_id.'}

    def submit(self, key, req):
        rid, target, action, scope = (req.get(k) for k in ('request_id', 'to', 'action', 'scope'))
        if not isinstance(rid, str) or not 1 <= len(rid) <= 200:
            raise ValueError('Stable request_id of 1..200 characters is required.')
        if action not in ACTIONS or scope not in ('session', 'server'):
            raise ValueError('Choose an allowed native action and explicit session or server scope.')
        saved = self.row('SELECT * FROM control_outbox WHERE from_key=? AND request_id=?', (key,rid))
        if saved and target == saved['target']:
            canonical = json.loads(saved['payload'])['to']
        else:
            canonical = next((p['session_key'] for p in self.node.hub.peers().get('peers', [])
                              if target in (p.get('session_key'), p.get('address'))), None)
        if not canonical:
            raise ValueError('Exact registered target was not found; no native action was queued.')
        body = dict(from_key=key, to=canonical, request_id=rid, action=action, scope=scope)
        fingerprint = hashlib.sha256(encoded(body).encode()).hexdigest()
        self.store._exec('INSERT OR IGNORE INTO control_outbox(from_key,request_id,target,payload,fingerprint) VALUES (?,?,?,?,?)',
                         (key,rid,target,encoded(body),fingerprint))
        saved = self.row('SELECT * FROM control_outbox WHERE from_key=? AND request_id=?', (key,rid))
        if saved['fingerprint'] != fingerprint:
            return {'ok': False, 'error': 'idempotency_conflict', 'request_id': rid}
        self.flush()
        return self.status(key, {'request_id': rid})

    def flush(self):
        for row in self.rows('SELECT * FROM control_outbox WHERE response IS NULL'):
            try:
                reply = self.node.hub.control_submit2(json.loads(row['payload']))
                self.store._exec('UPDATE control_outbox SET response=?,error=NULL WHERE from_key=? AND request_id=?',
                                 (encoded(reply),row['from_key'],row['request_id']))
            except HubError as e:
                self.store._exec('UPDATE control_outbox SET error=? WHERE from_key=? AND request_id=?', (e.code,row['from_key'],row['request_id']))

    def status(self, key, req):
        cid, rid = req.get('control_id'), req.get('request_id')
        if bool(cid) == bool(rid):
            raise ValueError('Provide exactly one control_id or request_id.')
        cached = None
        if rid:
            row = self.row('SELECT * FROM control_outbox WHERE from_key=? AND request_id=?',(key,rid))
            if row is None:
                return dict(ok=True,request_id=rid,state='unknown',execution_confirmed=None,verification_confirmed=None)
            if not row['response']:
                return dict(ok=True,request_id=rid,state='submission_unconfirmed',error=row['error'],
                            execution_confirmed=None,verification_confirmed=None,
                            summary='Persisted locally; hub acceptance and native execution are unknown. Retry with the SAME request_id.')
            cached = json.loads(row['response'])
            cid = cached['control_id']
        try:
            return dict(self.node.hub.control_status2(cid,key),ok=True)
        except HubError:
            if cached:
                return dict(cached,ok=True,last_observed_state=cached['state'],state='unknown',
                            freshness='cached',current_status_unknown=True,execution_confirmed=None,
                            verification_confirmed=None,summary='Hub accepted earlier; current native execution status is unknown.')
            raise

    def finish(self, job, outcome):
        self.store._exec('UPDATE control_journal SET state=?,result=?,reported=0,updated_at=? WHERE control_id=?',
                         (outcome['state'],encoded(outcome['result']),time.time(),job['control_id']))
        self.report(job)

    def invoke(self, session, scope):
        # Execute optional dependencies only from the local node runtime.
        import sys
        deps = os.path.join(self.node.p.home,'control-deps')
        if os.path.isdir(deps) and deps not in sys.path:
            sys.path.append(deps)
        from .codex_control import run
        return run(session,scope,codex_bin=self.node.codex_bin)

    def report(self, job):
        row = self.row('SELECT * FROM control_journal WHERE control_id=?',(job['control_id'],))
        try:
            state, result = row['state'], json.loads(row['result'])
            current = self.node.hub.control_status2(job['control_id'], job['to_key'])
            if state == 'in_doubt' and current['state'] == 'leased':
                state, result = 'failed', dict(code='interrupted_before_native',summary='No native call began before the execution barrier.',native_accepted=False,verified=False)
            self.node.hub.control_report2(dict(control_id=job['control_id'],owner=job['owner'],attempt=job['attempt'],state=state,result=result))
            self.store._exec('UPDATE control_journal SET state=?,result=?,reported=1 WHERE control_id=?',(state,encoded(result),job['control_id']))
        except HubError as e:
            self.node._log('control report %s deferred: %s' % (job['control_id'],e.code))

    def cycle(self):
        if not self.available():
            return
        with self.lock:
            # There is no native call in flight between cycles. An interrupted
            # execution intent must never be converted into an automatic retry.
            for row in self.rows("SELECT * FROM control_journal WHERE state='executing'"):
                result = dict(code='native_outcome_unknown',summary='Controller interrupted during native action; it will not be executed again automatically.',native_accepted=None,verified=False)
                self.store._exec("UPDATE control_journal SET state='in_doubt',result=?,reported=0 WHERE control_id=?",(encoded(result),row['control_id']))
            self.flush()
            for row in self.rows('SELECT * FROM control_journal WHERE reported=0 AND result IS NOT NULL'):
                self.report(json.loads(row['job']))
            reply = self.node.hub.control_pull2(self.node.machine,self.owner)
            for job in reply.get('controls',[]) if isinstance(reply,dict) else reply:
                self.execute(job)

    def execute(self, job):
        cid = job['control_id']
        old = self.row('SELECT * FROM control_journal WHERE control_id=?',(cid,))
        if old:
            if old['state'] in TERMINAL:
                self.store._exec('UPDATE control_journal SET job=?,reported=0 WHERE control_id=?',(encoded(job),cid))
                self.report(job)
                return
            if old['state'] != 'prepared':
                return
            self.store._exec('UPDATE control_journal SET job=? WHERE control_id=?',(encoded(job),cid))
        else:
            self.store._exec("INSERT INTO control_journal(control_id,job,state,updated_at) VALUES (?,?,'prepared',?)",(cid,encoded(job),time.time()))
        session = self.store.get_session(job['to_key'])
        if session is None:
            self.finish(job,no_action('failed','target_offline','The exact target has no local session; no native action was executed.'))
            return
        if job['action']=='reload_plugins':
            self.finish(job,no_action('unsupported','claude_interactive_control_unavailable','Existing Claude interactive messaging explicitly skips slash commands; no native reload endpoint is available.',session['harness']))
            return
        if session['harness']!='codex' or job['action']!='reload_mcp' or job['scope']!='server':
            self.finish(job,no_action('unsupported','native_scope_unsupported','Codex native MCP refresh requires explicit server scope and affects loaded threads on that server.',session['harness']))
            return
        try:
            identity = process_identity(session)
        except (OSError,ValueError):
            self.finish(job,no_action('failed','target_runtime_unavailable','The exact registered target is offline or its process identity changed.',session['harness']))
            return
        self.store._exec("UPDATE control_journal SET state='executing',updated_at=? WHERE control_id=? AND state='prepared'",(time.time(),cid))
        try:
            self.node.hub.control_begin2(dict(control_id=cid,owner=job['owner'],attempt=job['attempt']))
        except HubError:
            self.finish(job,no_action('failed','begin_unconfirmed_no_native_call','Native execution barrier was not confirmed; no native call was made.',session['harness']))
            return
        try:
            if process_identity(session)!=identity:
                outcome = no_action('failed','target_identity_changed','Target runtime changed before native execution.',session['harness'])
            else:
                outcome = self.invoke(session,job['scope'])
            if outcome.get('state') not in ('verified','applied_unverified','unsupported','failed','in_doubt'):
                raise ValueError('invalid_native_result')
        except Exception:
            outcome = dict(state='in_doubt',result=dict(code='native_result_unknown',summary='Native operation outcome could not be established; no automatic retry.',native_accepted=None,verified=False))
        self.finish(job,outcome)

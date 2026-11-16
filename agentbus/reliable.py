"""Durable protocol-2 sender and recipient gate for the per-machine node.

Only metadata is logged. A persisted request key survives lost RPC/HTTP replies.
Unacknowledged heads block their own directional stream, not other streams.
"""
import hashlib
import json
import threading
import time

from .hubclient import HubError

DDL = '''
CREATE TABLE IF NOT EXISTS outbox2 (
 from_key TEXT NOT NULL, request_id TEXT NOT NULL, to_key TEXT NOT NULL,
 text TEXT, original_target TEXT NOT NULL, reply_to TEXT, client_seq INTEGER NOT NULL, fingerprint TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', response TEXT, error TEXT,
 created_at REAL NOT NULL, updated_at REAL NOT NULL,
 PRIMARY KEY(from_key, request_id), UNIQUE(from_key,to_key,client_seq)
);
CREATE TABLE IF NOT EXISTS incoming2 (
 msg_id TEXT PRIMARY KEY, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
 seq INTEGER NOT NULL, state TEXT NOT NULL, detail TEXT, attempt INTEGER NOT NULL,
 updated_at REAL NOT NULL
);
'''


def digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


class Reliable:
    def __init__(self, node):
        self.node = node
        self.store = node.store
        self.send_lock = threading.Lock()
        self.protocol_ok = False
        self.protocol_state = "unknown"
        self.health_check_at = 0.0
        self.prune_at = 0.0
        with self.store._lock:
            self.store._db.executescript(DDL)
            self.store._db.execute("UPDATE incoming2 SET state='in_doubt',detail='node restarted during injection' WHERE state='injecting'")

    def available(self):
        if self.protocol_ok:
            return True
        now = time.monotonic()
        if now < self.health_check_at:
            return False
        self.health_check_at = now + 2
        try:
            self.protocol_ok = 2 in self.node.hub.health().get('protocols', [])
            self.protocol_state = 'supported' if self.protocol_ok else 'legacy'
        except (HubError, AttributeError):
            self.protocol_state = 'unknown'
            return False
        return self.protocol_ok

    def _rows(self, sql, args=()):
        return [dict(x) for x in self.store._query(sql, args)]

    def _row(self, sql, args=()):
        rows = self._rows(sql, args)
        return rows[0] if rows else None

    def dispatch(self, req, peer_pid=0):
        key = self.node._session_key(req)
        if not key or self.store.get_session(key) is None:
            return {'ok': False, 'error': 'not_registered', 'detail': 'Register this exact session first.'}
        if not self.available():
            if req.get('op') == 'status2' and req.get('client_msg_id'):
                return self.status(key, req)
            return {'ok': False, 'error': 'protocol_upgrade_required' if self.protocol_state == 'legacy' else 'hub_unreachable',
                    'detail': 'Hub protocol 2 is not confirmed. No new reliable send was attempted; delivery is unknown. Retry with the SAME request_id.'}
        try:
            op = req['op']
            if op == 'send2':
                return self.send(key, req)
            if op == 'status2':
                return self.status(key, req)
            if op == 'ack2':
                return self.ack(key, req)
            if op == 'message2':
                return dict(self.visible(self.node.hub.message2(req['msg_id'], key), key), ok=True)
            if op == 'history2':
                return dict(self.node.hub.history2(req['msg_id'], key, req.get('after', 0), req.get('limit', 50)), ok=True)
            if op == 'inbox2':
                result = self.node.hub.inbox2(key, req.get('after', 0), req.get('limit', 50))
                result['items'] = [self.visible(x, key) for x in result.get('items', [])]
                unavailable = [x['msg_id'] for x in result['items'] if not x.get('content_visible')]
                if unavailable:
                    result['complete'] = False
                    result['content_unavailable_ids'] = unavailable
                    result['summary'] = 'Some content is blocked or pruned. Inspect each item; missing content is not an empty reply.'
                return dict(result, ok=True)
            return {'ok': False, 'error': 'unknown_op'}
        except HubError as e:
            return {'ok': False, 'error': e.code, 'detail': e.message or 'Hub query failed; this is not evidence of non-delivery.'}
        except (KeyError, TypeError, ValueError) as e:
            return {'ok': False, 'error': 'bad_request', 'detail': str(e)}

    def send(self, key, req):
        request_id, to, text = req.get('client_msg_id'), req.get('to'), req.get('text')
        reply_to = req.get('reply_to') or None
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200:
            raise ValueError('A stable request_id (1..200 characters) is required; reuse it only for the same send.')
        if not isinstance(to, str) or not to or not isinstance(text, str) or not text or len(text.encode('utf-8')) > 65536:
            raise ValueError('Exact target and nonempty text up to 64 KiB are required.')
        existing = self._row('SELECT * FROM outbox2 WHERE from_key=? AND request_id=?', (key, request_id))
        # Payload equality includes the canonical target; an address may be renamed,
        # but a caller cannot reuse a key to change the original text or reply parent.
        if existing:
            if digest(json.dumps([existing['to_key'], text, reply_to], ensure_ascii=False)) != existing['fingerprint']:
                return {'ok': False, 'error': 'idempotency_conflict', 'detail': 'Same request_id with changed text or reply_to.'}
            if to not in (existing['to_key'], existing['original_target']):
                peer = self._resolve(to)
                if peer != existing['to_key']:
                    return {'ok': False, 'error': 'idempotency_conflict', 'detail': 'Same request_id with changed target.'}
        else:
            target = self._resolve(to)
            if key.split(':', 2)[:2] == target.split(':', 2)[:2]:
                raise ValueError('same_machine_same_harness: use the harness messaging tools')
            if reply_to:
                parent = self.node.hub.message2(reply_to, key)
                if parent.get('from_key') != target or parent.get('to_key') != key:
                    raise ValueError('reply_to must identify a received message from this exact peer.')
                if parent.get('blocked_by'):
                    raise ValueError('reply_to is blocked by an unacknowledged predecessor; do not reply to unseen later content.')
                if not parent.get('body_retained', True):
                    raise ValueError('reply_to body was pruned; retrieve original context before replying.')
            fingerprint = digest(json.dumps([target, text, reply_to], ensure_ascii=False))
            with self.store._lock:
                db = self.store._db
                db.execute('BEGIN IMMEDIATE')
                try:
                    found = db.execute('SELECT * FROM outbox2 WHERE from_key=? AND request_id=?', (key, request_id)).fetchone()
                    if found:
                        if found['fingerprint'] != fingerprint:
                            raise ValueError('idempotency_conflict: concurrent changed payload')
                    else:
                        seq = db.execute('SELECT COALESCE(MAX(client_seq),0)+1 FROM outbox2 WHERE from_key=? AND to_key=?', (key, target)).fetchone()[0]
                        now = time.time()
                        db.execute('INSERT INTO outbox2(from_key,request_id,to_key,text,original_target,reply_to,client_seq,fingerprint,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)', (key, request_id, target, text, to, reply_to, seq, fingerprint, now, now))
                    db.execute('COMMIT')
                except BaseException:
                    db.execute('ROLLBACK')
                    raise
        self.flush()
        row = self._row('SELECT * FROM outbox2 WHERE from_key=? AND request_id=?', (key, request_id))
        return self._receipt(row, refresh=True)

    def _resolve(self, address):
        for peer in self.node.hub.peers().get('peers', []):
            if address in (peer.get('session_key'), peer.get('address')):
                return peer['session_key']
        raise ValueError('unknown_target: exact registered peer not found; nothing was queued locally')

    def _receipt(self, row, refresh=False):
        response = json.loads(row['response']) if row.get('response') else None
        if response and refresh:
            try:
                response = self.node.hub.status2(response['msg_id'], row['from_key'])
            except HubError:
                response = dict(response, freshness='cached', current_status_unknown=True, receiver_acknowledged=None,
                                summary='Hub accepted earlier; current delivery/ACK status cannot be checked.')
        if response:
            return dict(response, ok=True, request_id=row['request_id'], submission='accepted')
        predecessor = self._row("SELECT request_id FROM outbox2 WHERE from_key=? AND to_key=? AND client_seq<? AND state!='accepted' ORDER BY client_seq LIMIT 1", (row['from_key'], row['to_key'], row['client_seq']))
        return {'ok': True, 'blocked_by_request_id': predecessor['request_id'] if predecessor else None, 'request_id': row['request_id'], 'client_msg_id': row['request_id'],
                'from_key': row['from_key'], 'to_key': row['to_key'], 'seq': row['client_seq'],
                'state': 'rejected' if row['state'] == 'rejected' else 'pending',
                'submission': 'rejected' if row['state'] == 'rejected' else 'unconfirmed',
                'receiver_acknowledged': None, 'last_error': row['error'],
                'summary': 'Persisted locally. Hub acceptance is not confirmed; retry/query with this same request_id and identical payload.' if row['state'] != 'rejected' else 'Hub rejected this request; subsequent sends in this stream are blocked.'}

    def flush(self):
        if not self.available() or not self.send_lock.acquire(blocking=False):
            return
        try:
            rows = self._rows("SELECT * FROM outbox2 WHERE state IN ('pending','rejected') ORDER BY from_key,to_key,client_seq")
            blocked = set()
            for row in rows:
                stream = (row['from_key'], row['to_key'])
                if stream in blocked or row['state'] == 'rejected':
                    blocked.add(stream)
                    continue
                body = {'from_key': row['from_key'], 'to': row['to_key'], 'text': row['text'],
                        'reply_to': row['reply_to'], 'client_msg_id': row['request_id'], 'client_seq': row['client_seq']}
                try:
                    try:
                        result = self.node.hub.send2(body)
                    except HubError as failure:
                        if failure.code != 'unknown_sender':
                            raise
                        session = self.store.get_session(row['from_key'])
                        if session is None:
                            raise
                        self.node._hub_register_session(session)
                        result = self.node.hub.send2(body)
                    if not result.get('msg_id'):
                        raise HubError(0, 'bad_response', 'Hub did not confirm a message ID')
                    self.store._exec("UPDATE outbox2 SET state='accepted',response=?,error=NULL,updated_at=? WHERE from_key=? AND request_id=?", (json.dumps(result), time.time(), row['from_key'], row['request_id']))
                except HubError as e:
                    rejected = 400 <= e.status < 500 and e.status not in (401, 408, 429) and e.code not in ('unknown_sender', 'unknown_target', 'not_found')
                    self.store._exec('UPDATE outbox2 SET state=?,error=?,updated_at=? WHERE from_key=? AND request_id=?', ('rejected' if rejected else 'pending', e.code, time.time(), row['from_key'], row['request_id']))
                    blocked.add(stream)
        finally:
            self.send_lock.release()

    def maintenance(self):
        """Only clear local accepted bodies after authoritative ACK+Hub pruning.

        A bounded round-robin check runs in the heartbeat thread, not the delivery
        path. Network uncertainty keeps local content; minimal identity stays.
        """
        now = time.time()
        if now < self.prune_at or not self.available():
            return
        self.prune_at = now + 60
        rows = self._rows("SELECT * FROM outbox2 WHERE state='accepted' AND text IS NOT NULL AND created_at<? ORDER BY updated_at LIMIT 32", (now - 7 * 86400,))
        for row in rows:
            saved = json.loads(row['response'])
            try:
                state = self.node.hub.status2(saved['msg_id'], row['from_key'])
            except HubError:
                break
            prunable = state.get('receiver_acknowledged') is True and state.get('body_retained') is False
            if prunable:
                self.store._exec('UPDATE outbox2 SET text=NULL,response=?,updated_at=? WHERE from_key=? AND request_id=?', (json.dumps(state), now, row['from_key'], row['request_id']))
            else:
                self.store._exec('UPDATE outbox2 SET response=?,updated_at=? WHERE from_key=? AND request_id=?', (json.dumps(state), now, row['from_key'], row['request_id']))

    def status(self, key, req):
        mid, cid = req.get('msg_id'), req.get('client_msg_id')
        if bool(mid) == bool(cid):
            raise ValueError('Provide exactly one msg_id or request_id.')
        if mid:
            return dict(self.node.hub.status2(mid, key), ok=True)
        row = self._row('SELECT * FROM outbox2 WHERE from_key=? AND request_id=?', (key, cid))
        if row:
            return self._receipt(row, refresh=True)
        return {'ok': True, 'request_id': cid, 'state': 'unknown', 'submission': 'unknown',
                'summary': 'No local record for this request ID. This is not proof it was never sent or received.'}

    def ack(self, key, req):
        result = self.node.hub.ack2({'msg_id': req['msg_id'], 'session_key': key})
        self.store._exec("UPDATE incoming2 SET state='acknowledged',updated_at=? WHERE msg_id=?", (time.time(), req['msg_id']))
        return dict(result, ok=True)

    @staticmethod
    def visible(message, key):
        out = dict(message)
        if out.get('to_key') == key and out.get('state') != 'acknowledged' and out.get('blocked_by'):
            out.pop('text', None)
            out['content_visible'] = False
            out['summary'] = 'Blocked by earlier message %s. Later content is withheld; acknowledge the predecessor first.' % out['blocked_by']
        else:
            out['content_visible'] = out.get('text') is not None
        return out

    def handle(self, message):
        mid, attempt = message.get('msg_id'), message.get('attempt')
        if not mid or not isinstance(attempt, int):
            return
        previous = self._row('SELECT * FROM incoming2 WHERE msg_id=?', (mid,))
        if previous:
            if previous['state'] != 'acknowledged':
                self._report(mid, attempt, 'in_doubt' if previous['state'] == 'injecting' else previous['state'], previous['detail'])
            return
        # Ask the authoritative head immediately before exposing content; do not
        # consume a later message merely because a stale pull response arrived.
        try:
            current = self.node.hub.status2(mid, message['to_key'])
        except HubError:
            return
        if current.get('state') != 'leased' or current.get('blocked_by') or current.get('attempt') != attempt:
            return
        text = message.get('text')
        valid = (isinstance(text, str) and message.get('text_bytes') == len(text.encode('utf-8'))
                 and message.get('sha256') == digest(text))
        if not valid:
            self._report(mid, attempt, 'failed', 'Content integrity mismatch; no injection attempted')
            return
        self.store._exec('INSERT OR IGNORE INTO incoming2(msg_id,from_key,to_key,seq,state,detail,attempt,updated_at) VALUES (?,?,?,?,?,?,?,?)', (mid, message['from_key'], message['to_key'], message['seq'], 'injecting', None, attempt, time.time()))
        outcome, detail = self.node._inject(message)
        state = 'injected' if outcome == 'delivered' else ('in_doubt' if str(detail).startswith('in_doubt:') else 'failed')
        self.store._exec("UPDATE incoming2 SET state=?,detail=?,attempt=?,updated_at=? WHERE msg_id=? AND state='injecting'", (state, detail, attempt, time.time(), mid))
        self._report(mid, attempt, state, detail)

    def _report(self, mid, attempt, state, detail):
        try:
            self.node.hub.report2({'msg_id': mid, 'attempt': attempt, 'state': state, 'detail': detail})
        except HubError as e:
            self.node._log('v2 report %s deferred: %s' % (mid, e.code))

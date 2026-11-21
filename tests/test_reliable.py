"""Real durable Hub/Node stores with fault injection; no live agent messages."""
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from agentbus.hub import Hub, ApiError
from agentbus.hubclient import HubError
from agentbus.node import Node, Store


class Link:
    def __init__(self, hub):
        self.hub = hub
        self.lose_send = False
        self.lose_receive = False
        self.offline = False

    def __getattr__(self, key):
        call = getattr(self.hub, key)
        def wrapped(*args, **kw):
            if self.offline:
                raise HubError(0, 'unreachable', 'synthetic disconnection')
            try:
                result = call(*args, **kw)
            except ApiError as e:
                raise HubError(e.status, e.code, e.message)
            if key == 'send2' and self.lose_send:
                self.lose_send = False
                raise HubError(0, 'unreachable', 'synthetic lost response after COMMIT')
            if key == 'receive2' and self.lose_receive:
                self.lose_receive = False
                raise HubError(0, 'unreachable', 'synthetic lost receipt response after COMMIT')
            return result
        return wrapped

    def health(self):
        if self.offline:
            raise HubError(0, 'unreachable')
        return {'ok': True, 'protocols': [1, 2], 'capabilities': ['durable_receipts']}


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.hub = Hub(':memory:')
        self.nodes = []
        self.sender, self.a = self.node('source', 'codex')
        self.receiver, self.b = self.node('target', 'claude')
        self.observed = []
        self.receiver._inject = self.inject

    def tearDown(self):
        for n in self.nodes:
            n.store.close()
        self.hub.close()

    def node(self, machine, harness):
        n = Node('/var/tmp/synthetic-unused-node')
        n.store = Store(':memory:')
        n.machine = machine
        n.hub = Link(self.hub)
        reg = self.hub.register({'machine': machine, 'harness': harness, 'session_id': machine + '-synthetic'})
        key = reg['session_key']
        n.store.upsert_session(key, harness, machine+'-synthetic', reg['address'], None, None, {}, os.getpid(), 1)
        self.nodes.append(n)
        return n, reg

    def inject(self, message):
        self.observed.append(message['msg_id'])
        return 'delivered', 'synthetic harness accepted'

    def send(self, key='request-1', text='body', **kw):
        return self.sender.dispatch(dict(op='send2', session_key=self.a['session_key'],
            to=self.b['address'], text=text, client_msg_id=key, **kw))

    def incoming(self, op, **kw):
        return self.receiver.dispatch(dict(op=op, session_key=self.b['session_key'], **kw))

    def test_lost_send_response_retries_same_id_without_duplicate(self):
        self.sender.hub.lose_send = True
        first = self.send()
        self.assertEqual(first['submission'], 'unconfirmed')
        second = self.send()
        self.assertEqual(second['submission'], 'accepted')
        self.assertEqual(second['seq'], 1)
        self.assertEqual(self.hub.conn.execute('SELECT COUNT(*) FROM messages_v2').fetchone()[0], 1)
        self.assertEqual(self.send(text='changed')['error'], 'idempotency_conflict')

    def test_retry_alias_works_offline_after_lost_response(self):
        self.sender.hub.lose_send = True
        first = self.send()
        self.sender.hub.offline = True
        second = self.send()
        self.assertTrue(second['ok'])
        self.assertEqual(second['request_id'], first['request_id'])
        self.assertEqual(second['submission'], 'unconfirmed')

    def test_durable_receipt_gates_transport_but_unread_inbox_is_complete(self):
        a = self.send(text='eleven')
        b = self.send('request-2', 'twelve')
        self.assertEqual(b['blocked_by'], a['msg_id'])
        peek = self.incoming('message2', msg_id=b['msg_id'])
        self.assertTrue(peek['content_visible'])
        self.assertEqual(peek['text'], 'twelve')
        first = self.hub.pull2('target', 0)[0]
        self.receiver.reliable().handle(first)
        self.assertEqual(self.observed, [a['msg_id']])
        second = self.hub.pull2('target', 0)[0]
        self.receiver.reliable().handle(second)
        self.assertEqual(self.observed, [a['msg_id'], b['msg_id']])
        for item in (a,b):
            status=self.hub.status2(item['msg_id'],self.a['session_key'])
            self.assertTrue(status['receiver_persisted'])
            self.assertFalse(status['receiver_acknowledged'])

    def test_reply_parent_must_be_same_exact_peers(self):
        first = self.send()
        bad = self.send('bad-reply', reply_to=first['msg_id'])
        self.assertEqual(bad['error'], 'bad_request')
        reply = self.receiver.dispatch(dict(op='send2', session_key=self.b['session_key'],
                    to=self.a['address'], text='answer', client_msg_id='answer-1', reply_to=first['msg_id']))
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['reply_to'], first['msg_id'])

    def test_three_messages_progress_without_agent_ack(self):
        sent = [self.send('auto-%d' % i, 'body-%d' % i) for i in range(3)]
        for i in range(3):
            incoming = self.hub.pull2('target', 0)
            self.assertEqual(len(incoming), 1, 'human ACK must not gate next delivery')
            self.receiver.reliable().handle(incoming[0])
        self.assertEqual(self.observed, [x['msg_id'] for x in sent])
        for item in sent:
            state = self.hub.status2(item['msg_id'], self.a['session_key'])
            self.assertTrue(state['receiver_persisted'])
            self.assertFalse(state['receiver_acknowledged'])

    def test_lost_receipt_response_replays_without_duplicate_injection(self):
        sent = self.send()
        self.receiver.hub.lose_receive = True
        self.receiver.reliable().handle(self.hub.pull2('target', 0)[0])
        self.assertTrue(self.hub.status2(sent['msg_id'],self.a['session_key'])['receiver_persisted'])
        self.assertEqual(self.observed, [])
        self.receiver.reliable().recover(force=True)
        self.receiver.reliable().recover(force=True)
        self.assertEqual(self.observed,[sent['msg_id']])
        self.assertEqual(self.hub.conn.execute("SELECT count(*) FROM events_v2 WHERE msg_id=? AND reason='recipient_persisted'",(sent['msg_id'],)).fetchone()[0],1)

    def test_storage_failure_never_sends_receipt(self):
        sent = self.send()
        message = self.hub.pull2('target',0)[0]
        with patch.object(self.receiver.reliable(),'_persist',side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(OSError):
                self.receiver.reliable().handle(message)
        self.assertFalse(self.hub.status2(sent['msg_id'],self.a['session_key'])['receiver_persisted'])
        self.assertEqual(self.observed,[])

    def test_committed_full_envelope_survives_restart_before_receipt(self):
        with tempfile.TemporaryDirectory(prefix='agentbus-receive-restart-') as temp:
            self.receiver.store.close()
            self.receiver.store=Store(temp+'/node.db')
            self.receiver._reliable=None
            sent=self.send()
            message=self.hub.pull2('target',0)[0]
            self.receiver.reliable()._persist(message)
            self.receiver.store.close()
            self.receiver.store=Store(temp+'/node.db')
            self.receiver._reliable=None
            self.receiver.reliable().recover(force=True)
            row=self.receiver.reliable()._row('SELECT * FROM incoming2 WHERE msg_id=?',(sent['msg_id'],))
            self.assertEqual(json.loads(row['envelope'])['text'],'body')
            self.assertEqual(self.receiver.store._query('PRAGMA synchronous')[0][0],2)
            self.assertEqual(self.observed,[sent['msg_id']])
            self.assertTrue(self.hub.status2(sent['msg_id'],self.a['session_key'])['receiver_persisted'])

    def test_legacy_injected_head_backfills_without_duplicate_notification(self):
        first=self.send()
        second=self.send('later','next')
        message=self.hub.pull2('target',0)[0]
        reliable=self.receiver.reliable()
        self.receiver.store._exec("INSERT INTO incoming2(msg_id,from_key,to_key,seq,state,detail,attempt,updated_at) VALUES (?,?,?,?,'injected',NULL,?,1)",(message['msg_id'],message['from_key'],message['to_key'],message['seq'],message['attempt']))
        self.hub.report2(dict(msg_id=first['msg_id'],attempt=message['attempt'],state='injected'))
        reliable.recover(force=True)
        self.assertEqual(self.observed,[])
        self.assertTrue(self.hub.status2(first['msg_id'],self.a['session_key'])['receiver_persisted'])
        reliable.handle(self.hub.pull2('target',0)[0])
        self.assertEqual(self.observed,[second['msg_id']])

    def test_legacy_read_during_rolling_upgrade_also_gets_durable_receipt(self):
        first=self.send()
        second=self.send('later','next')
        message=self.hub.pull2('target',0)[0]
        r=self.receiver.reliable()
        self.receiver.store._exec("INSERT INTO incoming2(msg_id,from_key,to_key,seq,state,detail,attempt,updated_at) VALUES (?,?,?,?,'acknowledged',NULL,?,1)",(message['msg_id'],message['from_key'],message['to_key'],message['seq'],message['attempt']))
        self.hub.report2(dict(msg_id=first['msg_id'],attempt=message['attempt'],state='injected'))
        self.hub.ack2(dict(msg_id=first['msg_id'],session_key=self.b['session_key']))
        self.assertFalse(self.hub.status2(first['msg_id'],self.a['session_key'])['receiver_persisted'])
        r.recover(force=True)
        self.assertEqual(self.observed,[])
        self.assertTrue(self.hub.status2(first['msg_id'],self.a['session_key'])['receiver_persisted'])
        r.handle(self.hub.pull2('target',0)[0])
        self.assertEqual(self.observed,[second['msg_id']])

    def test_optional_read_does_not_erase_uncertain_native_injection(self):
        first=self.send()
        second=self.send('later','next')
        self.receiver._inject=lambda m:('failed','in_doubt: synthetic uncertain native result')
        r=self.receiver.reliable()
        r.handle(self.hub.pull2('target',0)[0])
        self.assertTrue(self.incoming('ack2',msg_id=first['msg_id'])['receiver_acknowledged'])
        r.handle(self.hub.pull2('target',0)[0])
        self.receiver._inject=lambda m:self.fail('read ACK must not silently resolve native uncertainty')
        r.recover(force=True)
        item=self.incoming('message2',msg_id=second['msg_id'])
        self.assertEqual(item['text'],'next')
        self.assertEqual(item['blocked_by_delivery'],first['msg_id'])

    def test_confirmed_reply_releases_later_native_input(self):
        first = self.send()
        second = self.send('later', 'next')
        receiver = self.receiver.reliable()
        self.receiver._inject = lambda m: ('failed', 'in_doubt: synthetic uncertain result')
        receiver.handle(self.hub.pull2('target', 0)[0])
        self.receiver._inject = self.inject
        receiver.handle(self.hub.pull2('target', 0)[0])
        self.assertEqual(self.observed, [])

        reply = dict(client_msg_id='reply', to=self.a['address'],
                     text='processed', reply_to=first['msg_id'])
        self.receiver.hub.lose_send = True
        self.assertEqual(self.incoming('send2', **reply)['submission'], 'unconfirmed')
        receiver.recover(force=True)
        self.assertEqual(self.observed, [])
        self.assertEqual(self.incoming('message2', msg_id=second['msg_id'])['blocked_by_delivery'],
                         first['msg_id'])

        self.assertEqual(self.incoming('send2', **reply)['submission'], 'accepted')
        self.assertNotIn('blocked_by_delivery', self.incoming('message2', msg_id=second['msg_id']))
        receiver.recover(force=True)
        receiver.recover(force=True)
        self.assertEqual(self.observed, [second['msg_id']])
        original = self.incoming('status2', msg_id=first['msg_id'])
        self.assertEqual(original['injection_state'], 'in_doubt')
        self.assertFalse(original['receiver_acknowledged'])

    def test_transient_recovery_storage_failure_does_not_kill_pull_loop(self):
        r=self.receiver.reliable()
        called=[]
        def recover():
            called.append(True)
            if len(called)==1:
                raise sqlite3.OperationalError('synthetic transient spool failure')
            self.receiver._stop.set()
        self.receiver.hub.pull2=lambda *a:{'messages':[]}
        self.receiver.hub.pull=lambda *a:{'messages':[]}
        with patch.object(r,'recover',side_effect=recover):
            self.receiver._pull_loop()
        self.assertEqual(len(called),2)

    def test_local_full_body_cleanup_only_after_safe_hub_pruning(self):
        sent=self.send()
        r=self.receiver.reliable()
        r.handle(self.hub.pull2('target',0)[0])
        self.receiver.store._exec('UPDATE incoming2 SET received_at=0')
        r.maintenance()
        self.assertIsNotNone(r._row('SELECT envelope FROM incoming2 WHERE msg_id=?',(sent['msg_id'],))['envelope'])
        self.hub.conn.execute('UPDATE messages_v2 SET received_at=0,injected_at=0')
        self.hub.sweep2()
        r.prune_at=0
        r.maintenance()
        row=r._row('SELECT * FROM incoming2 WHERE msg_id=?',(sent['msg_id'],))
        self.assertIsNone(row['envelope'])
        self.assertEqual(row['body_pruned'],1)
        r.recover(force=True)
        self.assertEqual(self.observed,[sent['msg_id']])

    def test_ack_races_report_without_rolling_back_local_or_hub_state(self):
        sent = self.send()
        def early_ack(m):
            self.assertTrue(self.incoming('ack2', msg_id=m['msg_id'])['receiver_acknowledged'])
            return 'delivered', 'fake native input'
        self.receiver._inject = early_ack
        self.receiver.reliable().handle(self.hub.pull2('target', 0)[0])
        row = self.receiver.reliable()._row('SELECT * FROM incoming2 WHERE msg_id=?', (sent['msg_id'],))
        self.assertEqual(row['state'], 'injected')
        self.assertEqual(self.hub.status2(sent['msg_id'], self.a['session_key'])['state'], 'injected')
        self.assertTrue(self.hub.status2(sent['msg_id'], self.a['session_key'])['receiver_acknowledged'])

    def test_ambiguous_injection_is_not_blindly_retried(self):
        sent = self.send()
        self.receiver._inject = lambda m: ('failed', 'in_doubt: synthetic socket timeout')
        m = self.hub.pull2('target', 0)[0]
        self.receiver.reliable().handle(m)
        self.assertEqual(self.hub.status2(sent['msg_id'], self.a['session_key'])['state'], 'in_doubt')
        self.receiver._inject = lambda m: self.fail('must not inject again')
        self.receiver.reliable().handle(m)
        self.assertEqual(self.hub.pull2('target', 0), [])

    def test_body_integrity_failure_never_injects(self):
        sent = self.send()
        message = self.hub.pull2('target', 0)[0]
        message['text'] = 'corrupted'
        self.receiver.reliable().handle(message)
        self.assertFalse(self.observed)
        self.assertEqual(self.hub.status2(sent['msg_id'], self.a['session_key'])['state'], 'failed')

    def test_stale_lease_response_cannot_inject(self):
        self.send()
        first = self.hub.pull2('target', 0)[0]
        self.hub.conn.execute('UPDATE messages_v2 SET lease_until=0')
        second = self.hub.pull2('target', 0)[0]
        self.assertGreater(second['attempt'], first['attempt'])
        self.receiver.reliable().handle(first)
        self.assertFalse(self.observed)
        self.receiver.reliable().handle(second)
        self.assertEqual(len(self.observed), 1)

    def test_cached_status_never_reports_current_ack_false_as_fact(self):
        sent = self.send()
        self.sender.hub.offline = True
        status = self.sender.dispatch(dict(op='status2', session_key=self.a['session_key'],client_msg_id='request-1'))
        self.assertTrue(status['current_status_unknown'])
        self.assertIsNone(status['receiver_acknowledged'])
        self.assertIsNone(status['receiver_persisted'])
        self.assertEqual(status['injection_state'],'unknown')
        self.assertEqual(status['state'],'unknown')

    def test_old_hub_rejected_without_sending(self):
        self.sender.hub.health = lambda: {'ok': True, 'version': 'legacy'}
        result = self.send()
        self.assertEqual(result['error'], 'protocol_upgrade_required')
        self.assertEqual(self.hub.conn.execute('SELECT COUNT(*) FROM messages_v2').fetchone()[0], 0)

    def test_outbox_survives_reopen(self):
        with tempfile.TemporaryDirectory(prefix='agentbus-outbox-test-') as temp:
            self.sender.store.close()
            self.sender.store = Store(temp+'/node.db')
            self.sender.store.upsert_session(self.a['session_key'], 'codex', 'source-synthetic', self.a['address'],None,None,{},os.getpid(),1)
            self.sender.hub.lose_send=True
            self.send()
            self.sender.store.close()
            self.sender.store=Store(temp+'/node.db')
            self.sender._reliable=None
            retried=self.send()
            self.assertEqual(retried['seq'],1)
            self.assertEqual(retried['submission'],'accepted')
            self.assertEqual(self.hub.conn.execute('SELECT COUNT(*) FROM messages_v2').fetchone()[0],1)

    def test_missing_sender_registration_repairs_same_id_and_sequence(self):
        self.hub.conn.execute('DELETE FROM sessions WHERE session_key=?', (self.a['session_key'],))
        sent = self.send()
        self.assertEqual(sent['submission'], 'accepted')
        self.assertEqual(sent['seq'], 1)
        self.assertEqual(self.send()['msg_id'], sent['msg_id'])
        self.assertEqual(self.send('request-2')['seq'], 2)

    def test_local_cleanup_preserves_unacknowledged_body(self):
        sent = self.send()
        self.sender.store._exec('UPDATE outbox2 SET created_at=0')
        reliable = self.sender.reliable()
        reliable.maintenance()
        row = reliable._row('SELECT * FROM outbox2 WHERE request_id=?', ('request-1',))
        self.assertEqual(row['text'], 'body')
        self.assertFalse(self.hub.status2(sent['msg_id'], self.a['session_key'])['receiver_acknowledged'])
        self.incoming('ack2', msg_id=sent['msg_id'])
        self.hub.conn.execute('UPDATE messages_v2 SET acked_at=0')
        self.hub.sweep2()
        reliable.prune_at = 0
        reliable.maintenance()
        row = reliable._row('SELECT * FROM outbox2 WHERE request_id=?', ('request-1',))
        self.assertIsNone(row['text'])
        self.assertEqual(self.send()['msg_id'], sent['msg_id'])

    def test_unknown_status_ack_is_unknown_not_definite_false(self):
        result = self.incoming('status2', msg_id='synthetic-never-observed')
        self.assertEqual(result['state'], 'unknown_or_pruned')
        self.assertIsNone(result['receiver_acknowledged'])

    def test_network_failure_during_protocol_check_is_not_legacy_proof(self):
        self.sender.hub.offline = True
        result = self.send()
        self.assertEqual(result['error'], 'hub_unreachable')
        self.assertNotEqual(result['error'], 'protocol_upgrade_required')


class OperationalLogTests(unittest.TestCase):
    def test_rotation_preserves_inherited_append_fd_and_limits_archives(self):
        with tempfile.TemporaryDirectory(prefix='agentbus-log-test-') as temp:
            n = Node(temp)
            path = n.p.log
            with open(path, 'wb') as initial:
                initial.write(b'x' * (5 * 1024 * 1024 + 10))
            n._logf = open(path, 'a', buffering=1)
            try:
                with open(path, 'ab') as inherited:
                    n._log('new structured metadata')
                    inherited.write(b'child append stays live\n')
                    inherited.flush()
                with open(path, 'rb') as current:
                    content = current.read()
                self.assertIn(b'new structured metadata', content)
                self.assertIn(b'child append stays live', content)
                self.assertLessEqual(os.path.getsize(path+'.1'), 5*1024*1024)
                self.assertEqual(os.stat(path+'.1').st_mode & 0o777, 0o600)
            finally:
                n._logf.close()

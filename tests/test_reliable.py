"""Real durable Hub/Node stores with fault injection; no live agent messages."""
import hashlib
import json
import os
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
            return result
        return wrapped

    def health(self):
        if self.offline:
            raise HubError(0, 'unreachable')
        return {'ok': True, 'protocols': [1, 2]}


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

    def test_ack_gates_next_and_inbox_never_exposes_later_body(self):
        a = self.send(text='eleven')
        b = self.send('request-2', 'twelve')
        self.assertEqual(b['blocked_by'], a['msg_id'])
        peek = self.incoming('message2', msg_id=b['msg_id'])
        self.assertFalse(peek['content_visible'])
        self.assertNotIn('text', peek)
        later = next(x for x in self.incoming('inbox2')['items'] if x['msg_id']==b['msg_id'])
        self.assertNotIn('text', later)
        first = self.hub.pull2('target', 0)[0]
        self.receiver.reliable().handle(first)
        self.assertEqual(self.observed, [a['msg_id']])
        self.assertEqual(self.hub.pull2('target', 0), [])
        denied = self.incoming('ack2', msg_id=b['msg_id'])
        self.assertEqual(denied['error'], 'blocked_by')
        self.incoming('ack2', msg_id=a['msg_id'])
        second = self.hub.pull2('target', 0)[0]
        self.receiver.reliable().handle(second)
        self.assertEqual(self.observed, [a['msg_id'], b['msg_id']])

    def test_reply_parent_must_be_same_exact_peers(self):
        first = self.send()
        bad = self.send('bad-reply', reply_to=first['msg_id'])
        self.assertEqual(bad['error'], 'bad_request')
        reply = self.receiver.dispatch(dict(op='send2', session_key=self.b['session_key'],
                    to=self.a['address'], text='answer', client_msg_id='answer-1', reply_to=first['msg_id']))
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['reply_to'], first['msg_id'])

    def test_ack_races_report_without_rolling_back_local_or_hub_state(self):
        sent = self.send()
        def early_ack(m):
            self.assertTrue(self.incoming('ack2', msg_id=m['msg_id'])['receiver_acknowledged'])
            return 'delivered', 'fake native input'
        self.receiver._inject = early_ack
        self.receiver.reliable().handle(self.hub.pull2('target', 0)[0])
        row = self.receiver.reliable()._row('SELECT * FROM incoming2 WHERE msg_id=?', (sent['msg_id'],))
        self.assertEqual(row['state'], 'acknowledged')
        self.assertEqual(self.hub.status2(sent['msg_id'], self.a['session_key'])['state'], 'acknowledged')

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

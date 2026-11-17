"""Persistent controller fault tests; never reload a real running harness."""
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import time
import unittest
import zipfile
from unittest.mock import patch

from agentbus.hub import Hub, ApiError
from agentbus.hubclient import HubError
from agentbus.node import Node, Store


class Link:
    def __init__(self,hub):
        self.hub=hub
        self.lose_submit=False
        self.lose_begin=False
        self.lose_report=False
        self.offline=False

    def health(self):
        return {'capabilities':['native_controls','durable_receipts'],'protocols':[1,2]}

    def __getattr__(self,name):
        def call(*args,**kw):
            if self.offline:raise HubError(0,'unreachable')
            try:r=getattr(self.hub,name)(*args,**kw)
            except ApiError as e:raise HubError(e.status,e.code,e.message)
            flag={'control_submit2':'lose_submit','control_begin2':'lose_begin','control_report2':'lose_report'}.get(name)
            if flag and getattr(self,flag):
                setattr(self,flag,False)
                raise HubError(0,'unreachable','synthetic lost reply after COMMIT')
            return r
        return call


class ControlNodeTests(unittest.TestCase):
    def setUp(self):
        self.hub=Hub(':memory:')
        self.nodes=[]
        self.sender,self.a=self.node('a','codex')
        self.receiver,self.b=self.node('b','codex')
        self.calls=[]
        self.receiver.controller().invoke=self.native

    def tearDown(self):
        for n in self.nodes:n.store.close()
        self.hub.close()

    def node(self,machine,harness):
        n=Node('/var/tmp/agentbus-controller-fixture')
        n.machine=machine;n.store=Store(':memory:');n.hub=Link(self.hub)
        r=self.hub.register(dict(machine=machine,harness=harness,session_id=machine+'-control'))
        n.store.upsert_session(r['session_key'],harness,machine+'-control',r['address'],None,None,{},os.getpid(),time.time())
        self.nodes.append(n);return n,r

    def native(self,session,scope):
        self.calls.append((session['session_key'],scope))
        return dict(state='verified',result=dict(code='native_verified',summary='Isolated fake adapter verified',native_accepted=True,applied=True,verified=True,effective_scope='server',verification={'scope':'target_thread'}))

    def submit(self,rid='reload-1',**kw):
        req=dict(op='control_submit2',session_key=self.a['session_key'],to=self.b['address'],action='reload_mcp',scope='server',request_id=rid)
        req.update(kw);return self.sender.dispatch(req)

    def status(self,rid='reload-1'):
        return self.sender.dispatch(dict(op='control_status2',session_key=self.a['session_key'],request_id=rid))

    def test_reload_result_is_queryable_without_conversational_messages(self):
        r=self.submit();self.assertTrue(r['ok']);self.assertEqual(r['state'],'queued')
        self.assertFalse(r['execution_confirmed'])
        self.receiver.controller().cycle()
        r=self.status();self.assertEqual(r['state'],'verified');self.assertTrue(r['verification_confirmed'])
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM messages_v2').fetchone()[0],0)

    def test_slash_text_remains_an_ordinary_message(self):
        response=self.sender.dispatch(dict(op='send2',session_key=self.a['session_key'],
            to=self.b['address'],client_msg_id='plain-slash',text='/reload-plugins'))
        self.assertTrue(response['ok'])
        self.receiver.controller().cycle()
        self.assertEqual(self.calls,[])
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM control_jobs').fetchone()[0],0)

    def test_lost_submit_response_reuses_same_request_without_duplicate(self):
        self.sender.hub.lose_submit=True
        self.assertEqual(self.submit()['state'],'submission_unconfirmed')
        second=self.submit();self.assertTrue(second['ok'])
        self.receiver.controller().cycle();self.submit();self.receiver.controller().cycle()
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.submit(scope='session')['error'],'idempotency_conflict')

    def test_never_accepted_control_does_not_execute_hours_after_network_recovery(self):
        with patch.object(self.sender.hub,'control_submit2',side_effect=HubError(0,'unreachable')):
            first=self.submit()
        self.assertEqual(first['state'],'submission_unconfirmed')
        self.assertFalse(first['retry_stopped'])
        deadline=first['retry_until']
        with patch('agentbus.control.time.time',return_value=deadline+7200):
            self.sender.controller().cycle()
            # Explicit retry and controller restart cannot reopen the window.
            again=self.submit()
            from agentbus.control import Controller
            recovered=Controller(self.sender)
            recovered.flush()
            self.receiver.controller().cycle()
        self.assertEqual(again['retry_until'],deadline)
        self.assertTrue(again['retry_stopped'])
        self.assertEqual(again['state'],'submission_unconfirmed')
        self.assertIsNone(again['execution_confirmed'])
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM control_jobs').fetchone()[0],0)
        self.assertEqual(self.calls,[])

    def test_lost_accepted_response_after_retry_deadline_is_still_unknown(self):
        self.sender.hub.lose_submit=True
        first=self.submit()
        self.receiver.controller().cycle()
        with patch('agentbus.control.time.time',return_value=first['retry_until']+1):
            self.sender.controller().flush()
            result=self.status()
        self.assertEqual(result['state'],'submission_unconfirmed')
        self.assertTrue(result['retry_stopped'])
        self.assertIsNone(result['execution_confirmed'])
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM control_jobs').fetchone()[0],1)

    def test_known_control_id_remains_queryable_after_submission_deadline(self):
        first=self.submit()
        row=self.sender.controller().row('SELECT * FROM control_outbox')
        self.receiver.controller().cycle()
        with patch('agentbus.control.time.time',return_value=row['retry_until']+1):
            result=self.status()
        self.assertEqual(result['control_id'],first['control_id'])
        self.assertEqual(result['state'],'verified')

    def test_observed_retry_stop_survives_clock_rollback_and_restart(self):
        with patch.object(self.sender.hub,'control_submit2',side_effect=HubError(0,'unreachable')):
            first=self.submit()
        with patch('agentbus.control.time.time',return_value=first['retry_until']+1):
            self.assertTrue(self.status()['retry_stopped'])
        from agentbus.control import Controller
        recovered=Controller(self.sender)
        with patch('agentbus.control.time.time',return_value=first['retry_until']-200):
            recovered.flush()
            again=recovered.status(self.a['session_key'],{'request_id':'reload-1'})
        self.assertTrue(again['retry_stopped'])
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM control_jobs').fetchone()[0],0)

    def test_legacy_pending_migration_does_not_grant_new_retry_window(self):
        with patch.object(self.sender.hub,'control_submit2',side_effect=HubError(0,'unreachable')):
            self.submit()
        db=self.sender.store._db
        db.execute('ALTER TABLE control_outbox DROP COLUMN retry_until');db.commit()
        from agentbus.control import Controller
        recovered=Controller(self.sender)
        recovered.flush()
        result=recovered.status(self.a['session_key'],{'request_id':'reload-1'})
        self.assertTrue(result['retry_stopped'])
        self.assertEqual(result['retry_until'],0)
        self.assertIsNone(result['execution_confirmed'])
        self.assertEqual(self.hub.conn.execute('SELECT count(*) FROM control_jobs').fetchone()[0],0)

    def test_session_scope_does_not_silently_reload_shared_server(self):
        self.submit(scope='session');self.receiver.controller().cycle()
        self.assertEqual(self.status()['state'],'unsupported');self.assertEqual(self.calls,[])

    def test_claude_tui_control_never_fakes_sdk_execution(self):
        claude,reg=self.node('c','claude')
        self.submit(to=reg['address'],action='reload_plugins',scope='session')
        claude.controller().cycle();r=self.status()
        self.assertEqual(r['state'],'unsupported');self.assertFalse(r['execution_confirmed'])
        self.assertEqual(r['result']['code'],'claude_interactive_control_unavailable')

    def test_missing_target_fails_without_native_call(self):
        self.submit();self.receiver.store.delete_session(self.b['session_key'])
        self.receiver.controller().cycle();self.assertEqual(self.status()['state'],'failed');self.assertEqual(self.calls,[])

    def test_reused_pid_is_rejected_before_execution(self):
        self.submit();self.receiver.store._exec('UPDATE sessions SET registered_at=1')
        self.receiver.controller().cycle();self.assertEqual(self.status()['state'],'failed');self.assertEqual(self.calls,[])

    def test_begin_reply_lost_never_blindly_retries_native(self):
        self.submit();self.receiver.hub.lose_begin=True
        self.receiver.controller().cycle();self.receiver.controller().cycle()
        self.assertEqual(self.calls,[]);self.assertEqual(self.status()['state'],'failed')

    def test_report_reply_lost_does_not_repeat_native_action(self):
        self.submit();self.receiver.hub.lose_report=True
        self.receiver.controller().cycle();self.receiver.controller().cycle()
        self.assertEqual(len(self.calls),1);self.assertEqual(self.status()['state'],'verified')

    def test_crash_after_native_before_result_commit_is_in_doubt(self):
        self.submit();c=self.receiver.controller();original=self.receiver.store._exec
        def fail(sql,args=()):
            if sql.startswith('UPDATE control_journal SET state=?,result='):raise sqlite3.OperationalError('synthetic commit failure')
            return original(sql,args)
        with patch.object(self.receiver.store,'_exec',side_effect=fail):
            with self.assertRaises(sqlite3.OperationalError):c.cycle()
        c.cycle();self.assertEqual(len(self.calls),1);self.assertEqual(self.status()['state'],'in_doubt')

    def test_missing_native_evidence_is_not_verified(self):
        self.submit()
        self.receiver.controller().invoke=lambda *a,**kw:dict(state='applied_unverified',result=dict(code='refresh_accepted',summary='Native refresh accepted; target not observed',native_accepted=True,verified=False))
        self.receiver.controller().cycle();r=self.status()
        self.assertEqual(r['state'],'applied_unverified');self.assertFalse(r['verification_confirmed'])

    def test_cached_control_state_is_not_a_fresh_execution_claim(self):
        self.submit();self.sender.hub.offline=True;r=self.status()
        self.assertEqual(r['state'],'unknown');self.assertIsNone(r['execution_confirmed']);self.assertTrue(r['current_status_unknown'])

    def test_bootstrap_cli_works_without_new_mcp_catalog(self):
        from agentbus import __main__ as cli
        with patch.object(cli,'_node_call',return_value={'ok':True,'state':'queued'}) as call,contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['control','--from-session',self.a['session_key'],'--to',self.b['address'],'--action','reload_mcp','--scope','server','--request-id','cli-1']),0)
        self.assertEqual(call.call_args.args[0]['request_id'],'cli-1')
        self.assertEqual(call.call_args.args[0]['op'],'control_submit2')

    def test_waiting_cli_never_treats_unverified_native_acceptance_as_success(self):
        from agentbus import __main__ as cli
        args=['control','--from-session',self.a['session_key'],'--to',self.b['address'],'--action','reload_mcp','--scope','server','--request-id','wait-1','--wait']
        with patch.object(cli,'_node_call',return_value={'ok':True,'state':'applied_unverified','verification_confirmed':False}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(args),1)
        with patch.object(cli,'_node_call',return_value={'ok':True,'state':'verified','verification_confirmed':True}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(args),0)

    def test_persistent_wheel_restores_only_to_local_runtime(self):
        from agentbus.control import restore_transport
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as root:
            config=os.path.join(root,'persistent');local=os.path.join(root,'runtime')
            os.mkdir(config)
            with zipfile.ZipFile(os.path.join(config,'control-websocket-client.whl'),'w') as wheel:
                wheel.writestr('websocket/__init__.py','__version__="synthetic"\n')
                wheel.writestr('websocket_client-1.9.2.dist-info/METADATA','Name: websocket-client\nVersion: 1.9.2\n')
            restore_transport(SimpleNamespace(p=SimpleNamespace(config=config+'/config.json',home=local)))
            self.assertTrue(os.path.isfile(local+'/control-deps/websocket/__init__.py'))
            self.assertFalse(os.path.exists(config+'/control-deps'))

    def test_invalid_dependency_archive_cannot_escape_runtime_cache(self):
        from agentbus.control import restore_transport
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as root:
            with zipfile.ZipFile(root+'/control-websocket-client.whl','w') as wheel:
                wheel.writestr('../outside.py','never execute')
            with self.assertRaises(ValueError):
                restore_transport(SimpleNamespace(p=SimpleNamespace(config=root+'/config.json',home=root+'/runtime')))
            self.assertFalse(os.path.exists(root+'/outside.py'))


if __name__=='__main__':unittest.main()

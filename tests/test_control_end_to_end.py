"""Actual HTTP Hub + Unix Node + MCP stdio; isolated native adapter only."""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from agentbus.hub import make_server
from agentbus.node import Node


class ControlEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='agentbus-control-e2e-')
        self.home=Path(self.temp.name)
        self.server=make_server(str(self.home/'hub.db'),'synthetic-control-token',port=0)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.nodes=[];self.mcps=[];self.release=threading.Event();self.calls=[]
        for name in ('source','target'):
            p=self.home/name;p.mkdir(mode=0o700)
            (p/'token').write_text('synthetic-control-token')
            (p/'config.json').write_text(json.dumps(dict(machine=name,hub_url='http://127.0.0.1:%d'%self.server.server_address[1],token_file='token')))
            n=Node(str(p),poll_wait=.1,heartbeat_interval=60);n.start();self.nodes.append(n)
        self.nodes[1].controller().invoke=self.native
        self.target=self.nodes[1].dispatch(dict(op='register',harness='codex',session_id=str(uuid.uuid4()),name='reviewer',mcp_pid=os.getpid(),deliver={}))
        self.sid=str(uuid.uuid4())

    def tearDown(self):
        self.release.set()
        for p in self.mcps:
            if p.poll() is None:p.terminate()
            p.communicate(timeout=5)
        for n in self.nodes:n.stop()
        self.server.shutdown();self.thread.join(3);self.server.server_close();self.server.hub.close();self.temp.cleanup()

    def native(self,session,scope):
        self.calls.append(session['session_key'])
        self.release.wait(5)
        return dict(state='verified',result=dict(code='isolated_native_verified',summary='Isolated native test verified',native_accepted=True,applied=True,verified=True,effective_scope='server',verification={'scope':'target_thread'}))

    def mcp(self):
        env=dict(os.environ);env.update(AGENTBUS_HOME=str(self.home/'source'),AGENTBUS_CONFIG=str(self.home/'source/config.json'),CLAUDE_CODE_SESSION_ID=self.sid,PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        env.pop('AGENTBUS_MACHINE',None)
        p=subprocess.Popen([sys.executable,'-m','agentbus','mcp'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env)
        self.mcps.append(p);return p

    def rpc(self,p,name,args):
        msg=dict(jsonrpc='2.0',id=uuid.uuid4().hex,method='tools/call',params={'name':name,'arguments':args})
        p.stdin.write((json.dumps(msg)+'\n').encode());p.stdin.flush()
        if not select.select([p.stdout],[],[],10)[0]:self.fail('MCP reply timed out')
        reply=json.loads(p.stdout.readline())
        result=reply['result'];self.assertFalse(result.get('isError'),result)
        return json.loads(result['content'][0]['text'])

    def test_control_survives_calling_mcp_exit_and_is_not_a_message(self):
        first=self.mcp()
        r=self.rpc(first,'agentbus_control',dict(to=self.target['address'],action='reload_mcp',scope='server',request_id='caller-reload'))
        cid=r['control_id']
        first.terminate();first.communicate(timeout=5)
        self.release.set()
        replacement=self.mcp();deadline=time.monotonic()+8
        while True:
            state=self.rpc(replacement,'agentbus_control_status',{'request_id':'caller-reload'})
            if state['state']=='verified':break
            self.assertLess(time.monotonic(),deadline);time.sleep(.1)
        self.assertEqual(state['control_id'],cid)
        self.assertTrue(state['execution_confirmed']);self.assertTrue(state['verification_confirmed'])
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.server.hub.conn.execute('SELECT count(*) FROM messages_v2').fetchone()[0],0)
        duplicate=self.rpc(replacement,'agentbus_control',dict(to=self.target['address'],action='reload_mcp',scope='server',request_id='caller-reload'))
        self.assertEqual(duplicate['control_id'],cid);self.assertEqual(len(self.calls),1)

    def test_session_scope_is_reported_unsupported_without_native_call(self):
        p=self.mcp();self.release.set()
        self.rpc(p,'agentbus_control',dict(to=self.target['address'],action='reload_mcp',scope='session',request_id='narrow-scope'))
        deadline=time.monotonic()+8
        while True:
            state=self.rpc(p,'agentbus_control_status',{'request_id':'narrow-scope'})
            if state['state']=='unsupported':break
            self.assertLess(time.monotonic(),deadline);time.sleep(.1)
        self.assertEqual(self.calls,[]);self.assertFalse(state['execution_confirmed'])


if __name__=='__main__':unittest.main()

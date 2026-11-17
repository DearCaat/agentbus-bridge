import json
import unittest
from unittest.mock import patch
from agentbus import mcp_server as m


class McpControlTests(unittest.TestCase):
    def setUp(self):
        self.s=m.Session();self.s.harness='claude';self.s.session_id='fixture'
        self.s.reg={'session_key':'source:claude:fixture'}

    def call(self,name,args,reply):
        with patch.object(m,'node_call',return_value=reply) as node:
            r=m.handle_tools_call(self.s,{'name':name,'arguments':args})
        return r,json.loads(r['content'][0]['text']),node

    def test_native_request_has_explicit_scope_and_no_generic_success(self):
        args=dict(to='target/codex/reviewer',action='reload_mcp',scope='server',request_id='one-reload')
        r,value,node=self.call('agentbus_control',args,{'ok':True,'state':'queued','control_id':'c1','execution_confirmed':False})
        self.assertFalse(r.get('isError'));self.assertNotIn('ok',value)
        self.assertEqual(node.call_args.args[0],dict(args,op='control_submit2',session_key=self.s.reg['session_key']))

    def test_unsupported_action_is_not_presented_as_execution_success(self):
        r,value,_=self.call('agentbus_control',dict(to='target/claude/reviewer',action='reload_plugins',scope='session',request_id='unsupported'),{'ok':True,'state':'unsupported','execution_confirmed':False})
        self.assertTrue(r['isError']);self.assertFalse(value['execution_confirmed'])

    def test_status_query_preserves_native_failure_without_generic_ok(self):
        r,value,_=self.call('agentbus_control_status',{'request_id':'unknown-native'},dict(ok=True,state='in_doubt',execution_confirmed=None,verification_confirmed=False))
        self.assertFalse(r.get('isError'));self.assertNotIn('ok',value)
        self.assertIsNone(value['execution_confirmed'])

    def test_status_requires_exactly_one_identifier(self):
        with patch.object(m,'node_call') as node:
            r=m.handle_tools_call(self.s,{'name':'agentbus_control_status','arguments':{'control_id':'c1','request_id':'r1'}})
        self.assertTrue(r['isError']);node.assert_not_called()


if __name__=='__main__':unittest.main()

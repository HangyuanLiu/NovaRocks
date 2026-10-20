#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Invocation boundaries and cleanup results are orthogonal to test exits."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
import invocation_record as record
import identity_snapshot


class Records(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)

    def test_started_final_original_exit_and_atomic_persistence(self):
        path=record.start(self.root,self.root,'stage','fixture',['stage:fixture'],['false'],{'inputs':[]})
        self.assertTrue(path.is_file())
        final=path.with_name(path.name.replace('.started.json','.json'))
        self.assertFalse(final.exists())
        with mock.patch.object(identity_snapshot.os,'fsync',wraps=identity_snapshot.os.fsync) as sync:
            record.finish(path,143,'log')
            self.assertGreaterEqual(sync.call_count,2)
        value=json.loads(final.read_text())
        self.assertEqual(value['exit'],{'code':143,'signal_inferred':'SIGTERM'})
        self.assertTrue(value['inputs_stable'])
        self.assertEqual(value['cleanup']['summary'],'ok')
        self.assertFalse(list(self.root.rglob('*.tmp-*')))
        second=record.start(self.root,self.root,'stage','fixture',[],[],{'inputs':[]})
        self.assertNotEqual(path,second)

    def write(self,name,value):
        path=self.root/name
        path.write_text(json.dumps(value)+"\n")
        return str(path)

    def sql(self,values):
        path=self.root/'results.jsonl'
        path.write_text(''.join(json.dumps(v)+'\n' for v in values))
        return record.cleanup_facts('sql-suite',{'sql_results':str(path)})

    def test_sql_cleanup_states_and_failed_launch(self):
        fixture={'kind':'fixture','start':'failed','shutdown':{'status':'not_called'},'resources':[{'state':'released','registration':'registration.json'}]}
        self.assertEqual(self.sql([fixture])['summary'],'ok')
        fixture['resources'][0]['state']='cleanup_failed'
        self.assertEqual(self.sql([fixture])['summary'],'violation')
        fixture['resources'][0]['state']='unsettled'
        self.assertEqual(self.sql([fixture])['summary'],'unknown')
        self.assertEqual(self.sql([])['summary'],'unknown')
        fixture['resources']=[]
        self.assertEqual(self.sql([{'kind':'selection'},fixture])['summary'],'unknown')
        for status in ['not_launched','external','ok']:
            self.assertEqual(self.sql([{'kind':'selection'},fixture,{'kind':'server','cleanup':{'status':status}}])['summary'],'ok')

    def test_sql_selection_uses_native_cases_and_actual_environment(self):
        runner=self.root/'runner'
        declaration={'schema':1,'suites':[{'name':'fixture','cases':[{'case':'a'},{'case':'b'}],
            'reuse':{'env_inputs':[{'name':'FLAG','kind':'value'}]}}]}
        runner.write_text('#!/usr/bin/env python3\nimport json\nprint('+repr(json.dumps(declaration))+')\n')
        runner.chmod(0o755)
        argv=['env','FLAG=actual',str(runner),'--skip','b','--suite','fixture']
        units,spec=record.sql_selection(self.root,'fixture',argv,record.command_inputs(argv))
        self.assertEqual(units,['sql-case:fixture/a'])
        path=record.start(self.root,self.root,'sql-suite','fixture',units,argv,spec,{'FLAG':'outer'})
        final=record.finish(path,0,environment={'FLAG':'outer'})
        value=json.loads(final.read_text())
        self.assertTrue(value['inputs_stable'])
        self.assertEqual(value['env']['FLAG'],identity_snapshot.env_inputs(spec['env_inputs'],{'FLAG':'actual'})['FLAG'])

    def test_system_cleanup_uses_owner_facts_and_monitor(self):
        self.assertEqual(record.cleanup_facts('system-scenario',{})['summary'],'unknown')
        value={'fixture':{'launch_config':'failed','teardown':{'status':'ok'},'resources':[{'state':'cleanup_failed'}]},'cluster':{'launch':'not_attempted','shutdown':'not_applicable','partial_cleanup':None}}
        path=self.write('teardown.json',value)
        self.assertEqual(record.cleanup_facts('system-scenario',{'teardown':path})['summary'],'violation')
        value['fixture']['resources'][0]['state']='unsettled'
        self.write('teardown.json',value)
        self.assertEqual(record.cleanup_facts('system-scenario',{'teardown':path})['summary'],'unknown')
        value['fixture']['resources'][0]['state']='released'
        value['cluster'].update(launch='failed',partial_cleanup='monitor')
        self.write('teardown.json',value)
        self.assertEqual(record.cleanup_facts('system-scenario',{'teardown':path})['summary'],'unknown')
        monitor=self.root/'monitor.jsonl'
        pending={'schema':1,'event':'Pending'}
        final={'schema':1,'event':'Final','status_raw':0,'startup_error':None,'owner_lost':False,'errors':[],'cleanup_complete':True}
        def write_monitor():
            monitor.write_text(json.dumps(pending)+'\n'+json.dumps(final)+'\n')
        write_monitor()
        self.assertEqual(record.cleanup_facts('system-scenario',{'teardown':path,'monitors':[str(monitor)]})['summary'],'ok')
        final['errors']=['actual cleanup failed'];write_monitor()
        self.assertEqual(record.monitor_cleanup(monitor),'violation')
        final['errors']=[];final['schema']=2;write_monitor()
        self.assertEqual(record.monitor_cleanup(monitor),'unknown')
        final['schema']=1;final['cleanup_complete']=False;write_monitor()
        self.assertEqual(record.monitor_cleanup(monitor),'violation')

    def test_pretty_native_monitor_summary_requires_confirmed_cleanup(self):
        path=self.root/'cleanup-summary.json'
        value={'schema':1,'registration_closed':True,'complete':True,'ok':True,'records':[],'errors':[],'late_registrations':[]}
        identity_snapshot.atomic_json(path,value)
        self.assertEqual(record.monitor_cleanup(path),'ok')
        value['schema']=99
        identity_snapshot.atomic_json(path,value)
        self.assertEqual(record.monitor_cleanup(path),'unknown')
        value['schema']=1;value['records']=[{'violations':['actual cleanup failed']}]
        identity_snapshot.atomic_json(path,value)
        self.assertEqual(record.monitor_cleanup(path),'violation')

    def test_native_platform_is_recorded_and_changes_system_and_stage_identity(self):
        import platform
        expected={'system':platform.system(),'machine':platform.machine(),'release':platform.release()}
        for kind in ('stage','system-scenario'):
            argv=['runner','--only','scenario']
            path=record.start(self.root,self.root,kind,'platform',[],argv,{'inputs':[]})
            first=json.loads(path.read_text())['inputs_start']
            self.assertEqual(first['platform'],expected)
            with mock.patch.object(record,'execution_platform',return_value=dict(expected,machine='other-machine')):
                second=record.snapshot(self.root,record.execution_spec(kind,argv,{'inputs':[]}),{})
            self.assertNotEqual(first,second)
            final=record.finish(path,0)
            self.assertEqual(json.loads(final.read_text())['inputs_end']['platform'],expected)

    def test_system_topology_changes_identity_but_output_directory_does_not(self):
        def capture(size,output):
            argv=['runner','--cluster-size',size,'--timeout-secs','30','--artifact-root',output,'--only','scenario']
            path=record.start(self.root,self.root,'system-scenario','scenario',[],argv,{'inputs':[]})
            return json.loads(path.read_text())['inputs_start']
        self.assertEqual(capture('3','first'),capture('3','second'))
        self.assertNotEqual(capture('3','first'),capture('2','first'))



if __name__=='__main__':unittest.main()

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
"""Boundaries of development orchestration; native outputs remain the oracle."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from verification import run
from verification.model import Attempt, Observation, Scope, UnitKey


def put(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+'\n')
    return path


def rust_batch(root,binary='p',name='regular',identity=None):
    identity=identity or {'executable_sha256':'binary','nextest_profile':'default','nextest_config_sha256':'config'}
    put(root/'inputs/prepared.json',{'schema':1,'profile':'default','config_sha256':identity['nextest_config_sha256']})
    put(root/'inputs/test-artifacts.json',{'schema':1,'binaries':{binary:{'sha256':identity['executable_sha256']}}})
    put(root/'inputs/selected-tests.json',{'rust-suites':{binary:{'testcases':{name:{'filter-match':{'status':'matches'}}}}}})
    (root/'inputs/tool-config.toml').write_text('[store]\ndir = "/private/materials"\n')
    key=binary+'::'+name
    put(root/'identity/source-start.json',{'schema':1,'source':{'commit':'a'*40,'dirty':False,'worktree_tree':'tree'}})
    put(root/'identity/nextest.json',{'schema':1,'domain':'nextest','scopes':{key:{'declared':True,'inputs_start':identity,'inputs_end':identity,'inputs_stable':True}}})
    put(root/'rust-run/cleanup-summary.json',{'schema':1,'registration_closed':True,'complete':True,'ok':True,'records':[],'errors':[]})
    junit=root/'nextest/default/junit.xml';junit.parent.mkdir(parents=True)
    junit.write_text('<testsuites><testsuite name="'+binary+'"><testcase name="'+name+'" classname="'+binary+'"/></testsuite></testsuites>')
    put(root/'execution-index.json',{'schema':1,'complete':False,'stages':{'nextest':{'started':True,'returncode':0,'errors':[]}},
        'materials':{'inputs':'inputs','selection':'inputs/selected-tests.json','test_bindings':'inputs/test-artifacts.json',
        'tool_config':'inputs/tool-config.toml','junit':{'path':'nextest/default/junit.xml','complete':True},'rust_cleanup':'rust-run/cleanup-summary.json'}})
    return identity


class Run(unittest.TestCase):
    def test_cli_direct_module_and_shell_entrypoints_start_from_another_directory(self):
        ci=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='verification-cli-') as directory:
            commands=[([sys.executable,str(ci/'verification/cli.py'),'--help'],directory),
                      ([sys.executable,'-m','verification.cli','--help'],str(ci)),
                      (['bash',str(ci/'dev-verify.sh'),'--help'],directory)]
            for argv,cwd in commands:
                with self.subTest(argv=argv):
                    result=subprocess.run(argv,cwd=cwd,text=True,capture_output=True,timeout=15)
                    self.assertEqual(result.returncode,0,result.stderr)
                    self.assertIn('check-declarations',result.stdout)
                    self.assertEqual(result.stderr,'')

    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(prefix='verification-run-')
        self.addCleanup(self.temporary.cleanup)
        self.base=Path(self.temporary.name)
        self.repo=self.base/'repo';self.repo.mkdir()
        for argv in (['git','init','-q'],['git','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','fixture']):
            subprocess.run(argv,cwd=self.repo,check=True)
        patch=mock.patch.object(run.identity_snapshot,'tools',return_value={'fixture':'toolchain'})
        patch.start();self.addCleanup(patch.stop)
        self.inputs=self.base/'prepared/inputs';self.inputs.mkdir(parents=True)
        self.metadata={'version':1,'workspace_members':['p'], 'packages':[{
            'id':'p','name':'pkg','manifest_path':str(self.repo/'Cargo.toml'),
            'metadata':{'novarocks':{'test':{'runtime-inputs':[],'env-inputs':[],'ignored':[]}}}}]}
        self.native={'repo':str(self.repo),'metadata':self.metadata,'sql':{'schema':1,'suites':[]},'system':{'schema':1,'scenarios':[]},
            'stages':{'schema':1,'stages':[]},'inputs':str(self.inputs),'receipt':{'novarocks':{'path':sys.executable,'sha256':run.identity_snapshot.digest(sys.executable)}}}
        put(self.inputs/'test-artifacts.json',{'schema':1,'binaries':{'p':{'path':sys.executable,'sha256':run.identity_snapshot.digest(sys.executable),'package-id':'p','binary-name':'pkg'}}})
        put(self.inputs/'cargo-metadata.json',self.metadata)
        put(self.inputs/'prepared.json',{'schema':1,'profile':'default','nextest_version':'fixture'})
        (self.inputs/'nextest.toml').write_text('[profile.default]\n')
        self.chosen={'rust_filterset':None,'doctest':False,'sql_suites':[],'system_scenarios':[],'stages':[]}

    def scope(self,kind,native,tier='regular',selected=True):
        unit=UnitKey(kind,native)
        return Scope(str(unit),(unit,),{'build':'fixture'},'isolated',True,selected,tier)

    def test_exclusive_attempts_and_native_preparation_has_no_positional_filter(self):
        self.assertNotEqual(run.exclusive(self.base),run.exclusive(self.base))
        with mock.patch.object(run.subprocess,'run',return_value=subprocess.CompletedProcess([],0)) as command:
            run.prepare(self.repo,self.base)
        argv=command.call_args[0][0]
        self.assertNotIn('list',argv)

    def test_native_enumeration_removes_inherited_names_partitions_and_filters(self):
        put(self.inputs/'selected-tests.command.json',{'cwd':str(self.repo),'argv':['native-nextest','nextest','list','--cargo-metadata','meta','--message-format','json','narrow-name','--partition','hash:1/3','-E','old']})
        with mock.patch.object(run.subprocess,'check_output',return_value='{"rust-suites":{}}') as command:
            run.native_list(self.inputs,'rdeps(=pkg)','all')
        argv=command.call_args[0][0]
        self.assertNotIn('narrow-name',argv);self.assertNotIn('--partition',argv);self.assertNotIn('old',argv)
        self.assertEqual(argv[-4:],['-E','rdeps(=pkg)','--run-ignored','all'])
        self.assertEqual(command.call_args[1]['cwd'],str(self.repo))

    def test_native_ignored_rules_add_heavy_and_remove_helpers(self):
        declaration=self.metadata['packages'][0]['metadata']['novarocks']['test']
        declaration['ignored']=[{'filter':'test(=heavy)','class':'heavy','requires':['binary:novarocks'],'reason':'server'},
                                {'filter':'test(=helper)','class':'helper','reason':'nested helper'},
                                {'filter':'test(=manual)','class':'manual','reason':'recorder'}]
        put(self.inputs/'cargo-metadata.json',self.metadata)
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':name!='regular','filter-match':{'status':'matches'}} for name in ('regular','heavy','helper','manual')}}}}
        def native(inputs,expression=None,ignored=None):
            value=copy.deepcopy(listing)
            if expression and expression!='rdeps(=pkg)':
                name=next(name for name in ('heavy','helper','manual') if 'test(='+name+')' in expression)
                for key,case in value['rust-suites']['p']['testcases'].items():case['filter-match']['status']='matches' if key==name else 'mismatch'
            return value
        chosen=dict(self.chosen,rust_filterset='rdeps(=pkg)',doctest=True)
        with mock.patch.object(run,'native_list',side_effect=native):scopes=run.scopes(self.repo,self.native,chosen)
        by_key={scope.key:scope for scope in scopes}
        self.assertIn('rust-test:p::regular',by_key);self.assertIn('rust-test:p::heavy',by_key)
        self.assertNotIn('rust-test:p::helper',by_key);self.assertNotIn('rust-test:p::manual',by_key)
        self.assertEqual(by_key['rust-test:p::heavy'].tier,'heavy')
        self.assertTrue(by_key['rust-test:p::heavy'].selected)
        self.assertEqual(chosen['resolved_heavy_ignored'][0]['unit'],'rust-test:p::heavy')

    def test_resume_only_executes_unmet_regular_scopes_and_retains_inventory(self):
        regular=self.scope('rust-test','p::regular');satisfied=self.scope('rust-test','p::done')
        unselected=self.scope('stage','unaffected',selected=False)
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,path,attempt=run.execute(self.repo,self.base/'verification',self.native,self.chosen,[regular,satisfied,unselected],{regular.key})
        expression=batch.call_args[0][3]
        self.assertIn('test(=regular)',expression);self.assertNotIn('test(=done)',expression)
        selection=json.loads((attempt/'selection.json').read_text())
        self.assertEqual(len(selection['scopes']),3);self.assertEqual(selection['executed_scope_keys'],[regular.key])
        self.assertEqual(selection['unselected_obligations'][0]['backstop'],'admission verification')
        self.assertTrue((attempt/'identity/source-start.json').is_file());self.assertTrue((attempt/'identity/source-end.json').is_file())
        self.assertFalse(value['layer_ok'],'a mocked command exit without native evidence is never positive coverage')

    def test_heavy_uses_ignored_only_and_missing_prerequisite_remains_not_run(self):
        heavy=self.scope('rust-test','p::heavy',tier='heavy')
        self.native['ignored_units']={heavy.key:{'class':'heavy','requires':['binary:novarocks']}}
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,_,attempt=run.execute(self.repo,self.base/'available',self.native,self.chosen,[heavy])
        self.assertTrue(batch.call_args[1]['ignored']);self.assertIn('test(=heavy)',batch.call_args[0][3])
        self.assertTrue(list(attempt.glob('prerequisites/*.json')));self.assertFalse(value['layer_ok'])
        self.native['ignored_units'][heavy.key]['requires']=['unsupported:external']
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,_,attempt=run.execute(self.repo,self.base/'missing',self.native,self.chosen,[heavy])
        batch.assert_not_called();self.assertFalse(value['layer_ok'])
        self.assertTrue(json.loads((attempt/'execution.json').read_text())['errors'])

    def test_workspace_stage_runs_full_native_command_and_imports_its_batch(self):
        stage=self.scope('stage','workspace nextest and doctests')
        regular=self.scope('rust-test','p::regular')
        item={'name':'workspace nextest and doctests','tier':'admission'}
        chosen=dict(self.chosen,stages=[item])
        def invoked(root,repo,kind,name,units,argv,spec,materials,environment):
            self.assertIn('--run-stage',argv)
            self.assertEqual(argv[argv.index('--run-stage')+1],item['name'])
            self.assertIn('execution_index',materials)
            rust_batch(Path(materials['execution_index']).parent)
            put(Path(root)/'invocations/0001-stage.json',{'schema':1,'kind':'stage','name':name,'units':[],'materials':materials,
                'inputs_start':{},'inputs_end':{},'inputs_stable':True,'exit':{'code':0},'ended_at':'now','cleanup':{'source':'none','summary':'ok','refs':[]}})
            return 0
        with mock.patch.object(run,'invoke',side_effect=invoked),mock.patch.object(run,'stage_spec',return_value={'env_inputs':[]}),mock.patch.object(run,'_rust_batch') as narrowed:
            _,_,attempt=run.execute(self.repo,self.base/'workspace',self.native,chosen,[stage,regular])
        narrowed.assert_not_called()
        native=run.owned_attempt(attempt,[regular])
        self.assertTrue(any(obs.unit==regular.units[0] and obs.result=='pass' for obs in native.observations))

    def test_doctest_only_gap_does_not_secretly_rerun_satisfied_tests(self):
        doc=self.scope('rust-doctest','workspace')
        with mock.patch.object(run,'_rust_batch') as batch:
            result,_,attempt=run.execute(self.repo,self.base/'docs',self.native,self.chosen,[doc])
        batch.assert_not_called();self.assertFalse(result['layer_ok'])
        gap=json.loads((attempt/'doctest-only-gap.json').read_text())
        self.assertEqual(gap['status'],'not_run')
        self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])

    def test_regular_filter_compresses_full_native_binary_and_checks_membership(self):
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':False,'filter-match':{'status':'matches'}}
                    for name in ('regular','done')}}}}
        listing['rust-suites']['p']['testcases']['helper']={'ignored':True,'filter-match':{'status':'mismatch'}}
        bindings=json.loads((self.inputs/'test-artifacts.json').read_text())['binaries']
        metadata={'p':self.metadata['packages'][0]}
        all_scopes=[self.scope('rust-test','p::'+name) for name in ('regular','done')]
        with mock.patch.object(run,'native_list',return_value=listing):
            expression,actual=run._regular_expression(all_scopes,bindings,metadata,self.inputs,listing)
        self.assertEqual(expression,'(package(=pkg) & binary(=pkg))')
        self.assertEqual(set(actual),{scope.key for scope in all_scopes})
        narrowed=json.loads(json.dumps(listing))
        narrowed['rust-suites']['p']['testcases']['done']['filter-match']['status']='mismatch'
        with mock.patch.object(run,'native_list',return_value=narrowed):
            expression,_=run._regular_expression(all_scopes[:1],bindings,metadata,self.inputs,listing)
        self.assertIn('test(=regular)',expression);self.assertNotIn('test(=done)',expression)
        with mock.patch.object(run,'native_list',return_value=listing):
            with self.assertRaisesRegex(ValueError,'changes requested'):
                run._regular_expression(all_scopes[:1],bindings,metadata,self.inputs,listing)

    def test_native_lib_module_prefix_is_not_guessed_as_an_integration_binary(self):
        bindings={'p':{'package-id':'p','binary-name':'lib'},'p::exec':{'package-id':'p','binary-name':'exec'}}
        listing={'rust-suites':{'p':{'testcases':{'exec::module::check':{'ignored':False}}},
                               'p::exec':{'testcases':{'different_check':{'ignored':False}}}}}
        value=self.scope('rust-test','p::exec::module::check')
        expression=run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)
        self.assertIn('binary(=lib)',expression)
        self.assertIn('test(=exec::module::check)',expression)
        with self.assertRaisesRegex(ValueError,'no unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]})
        listing['rust-suites']['p::exec']['testcases']['module::check']={'ignored':False}
        with self.assertRaisesRegex(ValueError,'no unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)

    def test_repeated_or_heavy_unit_requires_unique_native_selector(self):
        bindings={key:{'package-id':'p','binary-name':'same'} for key in ('p::one','p::two')}
        listing={'rust-suites':{key:{'testcases':{'check':{'ignored':False}}} for key in bindings}}
        value=self.scope('rust-test','p::one::check')
        self.assertEqual(run._rust_binding(value,bindings,listing),('p::one','check'))
        with self.assertRaisesRegex(ValueError,'selector is not unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)

    def test_large_complete_regular_binary_filter_has_bounded_argv(self):
        names=['parser::session::case_%05d'%n for n in range(13228)]
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':False,'filter-match':{'status':'matches'}} for name in names}}}}
        values=[self.scope('rust-test','p::'+name) for name in names]
        bindings=json.loads((self.inputs/'test-artifacts.json').read_text())['binaries']
        with mock.patch.object(run,'native_list',return_value=listing):
            expression,actual=run._regular_expression(values,bindings,{'p':self.metadata['packages'][0]},self.inputs,listing)
        self.assertLess(len(expression.encode()),128)
        self.assertEqual(len(actual),13228)

    def test_native_inventory_keeps_deferred_aggregate_scope_before_results(self):
        item={'name':'workspace nextest and doctests','tier':'admission','domain':['**']}
        self.native['stages']['stages']=[item]
        chosen=dict(self.chosen,stages=[item])
        listing={'rust-suites':{'p':{'testcases':{'regular':{'ignored':False,'filter-match':{'status':'matches'}}}}}}
        with mock.patch.object(run,'native_list',return_value=listing),mock.patch.object(run,'stage_spec',return_value={}),mock.patch.object(run,'stage_argv',return_value=['native-stage']),mock.patch.object(run.invocation_record,'snapshot',return_value={'stage':'identity'}):
            values=run.scopes(self.repo,self.native,chosen)
        stage=next(scope for scope in values if scope.key=='stage:'+item['name'])
        self.assertFalse(stage.selected)
        self.assertEqual(stage.deferred['backstop'],'admission')
        with mock.patch.object(run,'invoke') as invoked,mock.patch.object(run,'_rust_batch') as batch:
            run.execute(self.repo,self.base/'deferred',self.native,chosen,values)
        invoked.assert_not_called()
        batch.assert_not_called()

    def test_aggregate_projection_keeps_admission_full_and_negative_observations(self):
        from verification.scope import required_scopes
        unit=UnitKey('stage','workspace nextest and doctests')
        deferred=Scope(str(unit),(unit,),{'build':'fixture'},'immutable',True,False,
                       deferred=dict(run.RUST_AGGREGATE_DEFERRED))
        self.assertEqual(required_scopes('dev',[deferred]),[])
        self.assertEqual(required_scopes('admission',[deferred]),[deferred])
        self.assertEqual(required_scopes('full',[deferred]),[deferred])
        for result,cleanup in [('fail','ok'),('unknown','unknown')]:
            with self.subTest(result=result,cleanup=cleanup):
                obs=Observation(unit,deferred.key,'old','stage',deferred.identity,result,
                                True,True,True,cleanup,ref='original-stage-record')
                value,_=run.compose_root(self.base/('audit-'+result),[deferred],
                    {'identities':{deferred.key:deferred.identity}},extra_attempts=[Attempt('old',[obs])])
                self.assertFalse(value['layer_ok'])
                self.assertEqual(value['scopes'],[])
                self.assertEqual(value['deferred_scopes'][0]['observations'][0]['ref'],'original-stage-record')
                self.assertEqual(value['unselected_obligations'][0]['backstop'],'admission')
                if cleanup=='unknown':self.assertTrue(value['deferred_scopes'][0]['cleanup_obligations'])
                else:self.assertEqual(value['deferred_scopes'][0]['verdict'],'failed')

    def doc_resume_fixture(self, label):
        root=self.base/label
        old=run.exclusive(root)
        put(old/'selection.json',{'schema':1})
        identity=rust_batch(old/'rust')
        regular=self.scope('rust-test','p::regular');regular.identity=identity
        doc=self.scope('rust-doctest','workspace');doc.identity={'domain':'workspace-docs'}
        return root,old,regular,doc

    def test_doctest_only_exception_is_recorded_before_one_native_batch_and_uses_reader(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-exception')
        before={str(p):p.read_bytes() for p in old.rglob('*') if p.is_file()}
        def batch(repo,attempt,output,expression,environment):
            saved=json.loads((attempt/'selection.json').read_text())
            exception=saved['resume_exception']
            self.assertEqual(exception['repeated_satisfied_scopes'],[regular.key])
            self.assertEqual(exception['pending_scopes'],[doc.key])
            self.assertEqual(exception['actual_filterset'],expression)
            self.assertEqual(exception['batch_output'],str(output))
            self.assertTrue(Path(exception['prior_composition']).is_file())
            self.assertIn('test(=regular)',expression)
            rust_batch(output,identity=regular.identity)
            index=json.loads((output/'execution-index.json').read_text())
            index['stages']['doctest']={'started':True,'returncode':0,'errors':[]}
            index['materials']['doctest_cleanup']='doctest-run/cleanup-summary.json'
            put(output/'execution-index.json',index)
            put(output/'doctest-run/cleanup-summary.json',{'schema':1,'registration_closed':True,'complete':True,'ok':True,'records':[],'errors':[]})
            put(output/'identity/doctest.json',{'schema':1,'domain':'doctest','scopes':{'workspace':{
                'declared':True,'inputs_start':doc.identity,'inputs_end':doc.identity,'inputs_stable':True}}})
            return 0
        with mock.patch.object(run,'_rust_batch',side_effect=batch) as executor:
            value,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
        self.assertEqual(executor.call_count,1)
        doc_result=next(item for item in value['scopes'] if item['key']==doc.key)
        self.assertEqual(doc_result['verdict'],'satisfied')
        self.assertTrue(doc_result['cleanup_obligations'],'an old unknown cleanup is not discharged by new PASS')
        self.assertFalse(value['layer_ok'])
        self.assertTrue(value['resume_exceptions'])
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)

    def test_doctest_exception_new_failure_is_inconsistent_and_not_retried(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-failure')
        def batch(repo,attempt,output,expression,environment):
            rust_batch(output,identity=regular.identity)
            junit=output/'nextest/default/junit.xml'
            junit.write_text('<testsuites><testsuite name="p"><testcase name="regular" classname="p"><failure message="new failure"/></testcase></testsuite></testsuites>')
            index=json.loads((output/'execution-index.json').read_text());index['stages']['nextest']['returncode']=100
            put(output/'execution-index.json',index)
            return 100
        with mock.patch.object(run,'_rust_batch',side_effect=batch) as executor:
            value,_,_=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
        self.assertEqual(executor.call_count,1)
        row=next(item for item in value['scopes'] if item['key']==regular.key)
        self.assertEqual(row['verdict'],'inconsistent')
        self.assertEqual({obs['result'] for obs in row['observations']},{'pass','fail'})
        self.assertFalse(value['layer_ok'])

    def test_doc_exception_refuses_narrow_pending_when_other_target_is_unmet(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-other')
        other=self.scope('sql-case','suite/missing')
        with mock.patch.object(run,'_rust_batch') as executor:
            value,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc,other],{doc.key})
        executor.assert_not_called()
        self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])
        self.assertFalse(value['layer_ok'])

    def test_doc_exception_requires_matching_satisfied_regular_scope(self):
        for mutation in ('identity','heavy','unselected','cleanup','counterexample','binding'):
            root,old,regular,doc=self.doc_resume_fixture('refuse-'+mutation)
            if mutation=='identity':regular.identity=dict(regular.identity,executable_sha256='changed')
            elif mutation=='heavy':regular.tier='heavy'
            elif mutation=='unselected':regular.selected=False
            elif mutation=='cleanup':
                (old/'rust/rust-run/cleanup-summary.json').unlink()
            elif mutation=='binding':
                self.native['regular_inventory']={'rust-suites':{}}
            else:
                junit=old/'rust/nextest/default/junit.xml'
                junit.write_text('<testsuites><testsuite name="p"><testcase name="regular" classname="p"><failure/></testcase></testsuite></testsuites>')
            with self.subTest(mutation=mutation),mock.patch.object(run,'_rust_batch') as executor:
                _,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
                executor.assert_not_called()
                self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])

    def test_heavy_native_binding_refusal_is_recorded_and_composed(self):
        heavy=self.scope('rust-test','missing::heavy',tier='heavy')
        self.native['ignored_units']={heavy.key:{'class':'heavy','requires':['binary:novarocks']}}
        with mock.patch.object(run,'_rust_batch') as batch:
            value,_,attempt=run.execute(self.repo,self.base/'heavy-refusal',self.native,self.chosen,[heavy])
        batch.assert_not_called()
        self.assertFalse(value['layer_ok'])
        errors=json.loads((attempt/'execution.json').read_text())['errors']
        self.assertEqual(errors[0]['kind'],'rust-heavy')
        self.assertIn('no unique binary/test binding',errors[0]['error'])
        self.assertTrue((attempt/'result.json').is_file())

    def test_spawn_failure_has_no_fabricated_native_exit(self):
        attempt=run.exclusive(self.base/'spawn');put(attempt/'selection.json',{'schema':1})
        code=run.invoke(attempt,self.repo,'stage','missing',[UnitKey('stage','missing')],['/missing/native-executor'],{'inputs':[]},{},{})
        self.assertEqual(code,127)
        final=next(path for path in (attempt/'invocations').glob('*.json') if not path.name.endswith('.started.json'))
        value=json.loads(final.read_text());self.assertIsNone(value['exit']['code']);self.assertFalse(value['execution']['started'])
        observation=run.owned_attempt(attempt).observations[0]
        self.assertEqual(observation.result,'not_run');self.assertFalse(observation.terminated)

    def system_attempt(self,extra_missing=False):
        attempt=run.exclusive(self.base/'cleanup');put(attempt/'selection.json',{'schema':1})
        registration=put(attempt/'resource.json',{'schema':1,'owner':'isolated-iceberg-rest','id':'unique','label':'compose-fixture','state':'registered'})
        refs=[str(registration)]
        if extra_missing:refs.append(str(attempt/'missing.json'))
        resources=[{'owner':'isolated-iceberg-rest','id':'unique','label':'compose-fixture','registration':ref,'state':'unsettled'} for ref in refs]
        teardown=put(attempt/'scenario-teardown.json',{'schema':1,'fixture':{'launch_config':'failed','teardown':{'status':'ok'},'resources':resources},
            'cluster':{'launch':'not_attempted','shutdown':'not_applicable','partial_cleanup':None}})
        evidence=put(attempt/'scenario-evidence.json',{'schema_version':5,'scenario':'scene','outcome':'failed'})
        put(attempt/'invocations/0001-scene.json',{'schema':1,'kind':'system-scenario','units':['system-scenario:scene'],'seq':1,
            'inputs_start':{},'inputs_end':{},'inputs_stable':True,'exit':{'code':1},'ended_at':'now',
            'materials':{'evidence':str(evidence),'teardown':str(teardown)},'cleanup':{'source':'producer','summary':'unknown','refs':[str(teardown)]+refs}})
        return attempt

    def test_cleanup_confirmation_queries_only_exact_resources_and_loads_repeated_attempts(self):
        attempt=self.system_attempt()
        calls=[]
        def present(argv,**kwargs):calls.append(argv);return subprocess.CompletedProcess(argv,0,'still-present\n','')
        first,_=run.cleanup_confirmation(attempt,run_command=present)
        self.assertFalse(first['confirmations'][0]['complete'])
        def absent(argv,**kwargs):calls.append(argv);return subprocess.CompletedProcess(argv,0,'','')
        second,path=run.cleanup_confirmation(attempt,run_command=absent)
        self.assertTrue(second['confirmations'][0]['complete']);self.assertNotEqual(path.name,'cleanup-confirmation.json')
        self.assertEqual(len(run.confirmations(attempt)),2)
        self.assertEqual(len(calls),6)
        self.assertTrue(all('label=com.docker.compose.project=compose-fixture' in argv for argv in calls))
        self.assertFalse(any(any(word in argv for word in ('rm','down','stop','kill')) for argv in calls))

    def test_missing_required_cleanup_ref_cannot_be_hidden_by_other_successes(self):
        attempt=self.system_attempt(extra_missing=True)
        value,_=run.cleanup_confirmation(attempt,run_command=lambda argv,**kwargs:subprocess.CompletedProcess(argv,0,'',''))
        self.assertFalse(value['confirmations'][0]['complete'])
        self.assertTrue(any(not check['ok'] for check in value['confirmations'][0]['checks']))
        with self.assertRaises(ValueError):run.cleanup_confirmation(self.base/'does-not-exist')

    def test_direct_native_root_and_admission_rehearsal_keep_local_authority(self):
        root=self.base/'native-root';identity=rust_batch(root)
        scope=Scope('rust-test:p::regular',(UnitKey('rust-test','p::regular'),),identity,'isolated')
        value,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},current=root.name)
        self.assertTrue(value['layer_ok']);self.assertEqual(len(value['attempts']),1)
        admission,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},layer='admission',current=root.name)
        self.assertFalse(admission['layer_ok']);self.assertFalse(admission['attempts'][0]['formal'])
        self.assertFalse(admission['local_rehearsal']['qualifies_for_admission'])
        self.assertTrue(admission['local_rehearsal']['scopes'][0]['observations'])

    def test_local_rehearsal_labels_quarantine_separately_from_raw_membership(self):
        root=self.base/'quarantined-root';identity=rust_batch(root)
        unit=UnitKey('rust-test','p::regular')
        scope=Scope('rust-test:p::regular',(unit,),identity,'isolated')
        disposition=run.Dispositions({'schema':1,'known_failures':[], 'quarantine':[{
            'unit':str(unit),'reason':'unstable','owner':'tests','exit_condition':'fix',
            'review_by':'2099-01-01'}]})
        with mock.patch.object(run.Dispositions,'read',return_value=disposition):
            value,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},
                                     layer='admission',current=root.name)
        rehearsal=value['local_rehearsal']
        self.assertIn('before admission quarantine exclusions',rehearsal['membership'])
        self.assertIn(str(unit),rehearsal['admission_membership_exclusions'])
        self.assertEqual(rehearsal['scopes'][0]['gated_units'],[str(unit)])
        self.assertFalse(rehearsal['qualifies_for_admission'])

    def test_variant_preparation_executes_existing_owner_helper(self):
        library=self.repo/'tools/ci/lib';library.mkdir(parents=True)
        actual=Path(run.__file__).resolve().parents[1]/'lib/identity_snapshot.py'
        (library/'identity_snapshot.py').symlink_to(actual)
        variant=self.base/'compatible-program';variant.write_text('variant')
        calls=self.base/'owner-calls'
        (library/'system_scenarios.sh').write_text('ci_prepare_system_scenario_binaries() {\n  printf \'%s\\n\' "$@" >"$OWNER_CALLS"\n  SYSTEM_SCENARIO_PRIMARY_BINARY="$1"\n  SYSTEM_SCENARIO_COMPATIBLE_BINARY="$COMPATIBLE_FIXTURE_BINARY"\n  SYSTEM_SCENARIO_OTHER_ISLAND_BINARY=""\n}\n')
        put(self.inputs/'artifacts.json',self.native['receipt'])
        self.native['receipt']['novarocks-system-tests']={'path':sys.executable}
        with mock.patch.object(run.subprocess,'check_output',return_value='primary\ncompatible\n'),mock.patch.dict(os.environ,{'OWNER_CALLS':str(calls),'COMPATIBLE_FIXTURE_BINARY':str(variant)}):
            run.prepare_system_variants(self.repo,self.native,dict(self.chosen,system_scenarios=[{'name':'mixed'}]),self.base/'variants')
        self.assertTrue(calls.is_file());self.assertEqual(calls.read_text().splitlines()[0],sys.executable)
        self.assertEqual(self.native['system_variants']['compatible']['path'],str(variant))
        self.assertTrue(self.native['system_variants_prepared'])

    def test_system_variant_arguments_bind_prepared_programs(self):
        variant=self.base/'compatible';variant.write_text('compiled-variant')
        self.native['receipt']['novarocks-system-tests']={'path':sys.executable}
        self.native['system_variants']={'compatible':{'path':str(variant),'sha256':run.identity_snapshot.digest(variant)}}
        argv=run.system_argv(self.native,{'name':'native-compatibility/mixed'},'/materials')
        self.assertEqual(argv[argv.index('--compatible-binary')+1],str(variant))
        variant.write_text('changed')
        with self.assertRaises(ValueError):run.system_argv(self.native,{'name':'native-compatibility/mixed'},'/materials')


if __name__=='__main__':unittest.main()

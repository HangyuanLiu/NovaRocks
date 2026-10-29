# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Local control correctness using exact API facts and real hashed archives.

CI maintainers run this inexpensive suite when local control changes. The
independent native JSON/log bytes are never manufactured into a product pass.
"""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from aliyun_ci.config import validate_config
from aliyun_ci.handoff import pack
from aliyun_ci.local_run import (start_local,status_local,wait_local,cancel_local,collect_local,
    finish_local,result_exit_code,_save)
from aliyun_ci.plan import derive
from aliyun_ci.ports import APIError
from .test_start import ScriptAPI,NOW,EMPTY,CREATED,instance,listed
from .test_cloud_command import row
from .test_config import example


class Store:
    def __init__(self):self.objects={};self.calls=[]
    def get(self,key):
        self.calls.append(('get',key))
        if key not in self.objects:raise APIError('NoSuchKey')
        return self.objects[key]
    def put(self,key,data,**kwargs):self.calls.append(('put',key));self.objects[key]=data
    def presign(self,method,key,expires_at):return 'https://example.invalid/capability-secret'


class LocalTests(unittest.TestCase):
    def setUp(self):
        data=example();data.pop('github');data['execution_kind']='local-rust'
        self.config=validate_config(data)
        self.plan=derive(self.config,'local-20261029T000000Z-abcd',1,NOW)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.record=self.root/'run.json'
        self.store=Store()
        self.store.objects[f"{self.config['oss']['prefix']}/images/m-exact/release.json"]=json.dumps(
            {'image_id':'m-exact','region':self.config['region']}).encode()
        self.source=self.root/'source';self.source.mkdir();(self.source/'source.bundle').write_bytes(b'exact-source')
        self.manifest={'sha':'a'*40,'tree':'b'*40,'bundle_sha256':hashlib.sha256(b'exact-source').hexdigest()}
        (self.source/'source.json').write_text(json.dumps(self.manifest))

    def prepare_record(self,**updates):
        value={'schema':1,'config':self.config.data,'plan':self.plan.to_dict(),
            'image_id':'m-exact','instance_id':'i-exact','source':self.manifest,'phase':'observing',
            'command':{'instance_id':'i-exact','invoke_id':'invoke-original','dispatch_state':'accepted',
                       'deadline':self.plan.deadline}}
        value.update(updates);_save(self.record,value,exclusive=True);return value

    def archive(self,kind='final'):
        attempt=self.root/'attempt';(attempt/'rust').mkdir(parents=True)
        raw_index={'actual_native_exit':7,'unchanged':['nextest','doctest']}
        (attempt/'rust'/'execution-index.json').write_text(json.dumps(raw_index))
        (attempt/'stdout.log').write_bytes(b'original failure\n')
        archive=pack(attempt,kind=kind)
        self.addCleanup(lambda: __import__('shutil').rmtree(archive.parent))
        self.store.objects[self.plan.oss_keys[kind]]=archive.read_bytes()
        return raw_index

    def test_start_persists_source_identity_and_never_writes_capabilities_to_record(self):
        api=ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',CREATED),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',{'InvokeId':'invoke-original'})])
        class OOS:
            def call(inner,action,**parameters):
                self.assertTrue(self.record.exists());return {}
        value=start_local(self.config,self.plan,api,OOS(),self.store,image_id='m-exact',
            source=self.source,record_path=self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('invoke-original',value['command']['invoke_id'])
        self.assertEqual(self.manifest,value['source'])
        self.assertNotIn('capability-secret',self.record.read_text())
        self.assertEqual(1,sum(a=='RunInstances' for a,_ in api.calls))
        with self.assertRaises(FileExistsError):
            start_local(self.config,self.plan,ScriptAPI([]),OOS(),self.store,image_id='m-exact',
                source=self.source,record_path=self.record,now=lambda:NOW)

    def test_status_reconnection_only_observes_same_nonzero_batch(self):
        self.prepare_record()
        task={'identity':{'run_id':self.plan.run_id,'run_attempt':1},'state':'complete','exit_code':7}
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInvocationResults',row(output=json.dumps(task)))])
        value=status_local(self.config,api,self.record)
        self.assertEqual(7,value['task']['exit_code'])
        self.assertEqual(['DescribeInstances','DescribeInvocationResults'],[a for a,_ in api.calls])
        self.assertEqual('invoke-original',api.calls[-1][1]['InvokeId'])

    def test_dropped_output_cannot_supply_batch_result(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',EMPTY),('DescribeInvocationResults',row(dropped=3))])
        self.assertIsNone(status_local(self.config,api,self.record)['task'])

    def test_collect_preserves_native_result_and_log_bytes_with_member_verification(self):
        self.prepare_record();native=self.archive()
        value=collect_local(self.config,self.store,self.record,self.root/'results')
        self.assertTrue(value['archives']['final']['manifest_ok'])
        self.assertEqual(native,value['native']['final']['execution_index']['value'])
        self.assertEqual(b'original failure\n',(self.root/'results/final/attempt/stdout.log').read_bytes())
        self.assertFalse(value['archives']['interrupted']['present'])

    def test_unknown_archive_read_is_not_absent(self):
        self.prepare_record()
        class Denied:
            def get(inner,key):raise APIError('AccessDenied')
        value=collect_local(self.config,Denied(),self.record,self.root/'results')
        self.assertIsNone(value['archives']['final']['present'])

    def test_collect_failure_still_releases_exact_instance_without_github(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('DeleteInstance',{}),('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]})])
        with patch('aliyun_ci.local_run.collect_local',side_effect=OSError('disk full')):
            result=finish_local(self.config,api,oos,self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('released',result['compute_terminal'])
        self.assertEqual('disk full',result['local_collection_error'])
        self.assertEqual(1,result_exit_code(result))

    def test_nonzero_native_result_does_not_erase_successful_handoff_or_release(self):
        self.prepare_record(observation={'task':{'exit_code':7}});native=self.archive()
        api=ScriptAPI([('DescribeInstances',EMPTY),('DescribeInstances',EMPTY)])
        result=finish_local(self.config,api,ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),
            self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('released',result['compute_terminal'])
        self.assertEqual(native,result['rust_result']['value'])
        self.assertEqual(7,result['local_task']['exit_code'])
        self.assertEqual(1,result_exit_code(result))

    def test_cancel_never_uses_stop_invocation_or_repeats_unknown_command(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',TimeoutError())])
        with self.assertRaises(TimeoutError):cancel_local(self.config,api,self.record,now=lambda:NOW)
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan)))])
        result=cancel_local(self.config,api,self.record,now=lambda:NOW)
        self.assertEqual('DispatchUnknown',result['status'])
        self.assertEqual(['DescribeInstances'],[a for a,_ in api.calls])

    def test_failed_release_is_independent_from_valid_materials(self):
        self.prepare_record(observation={'task':{'exit_code':0}});self.archive()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('DeleteInstance',APIError('Forbidden'))])
        result=finish_local(self.config,api,ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),
            self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('unknown',result['compute_terminal'])
        self.assertTrue(result['local_collection']['archives']['final']['manifest_ok'])
        self.assertEqual('compute',result['leftovers'][0]['kind'])
        self.assertEqual(1,result_exit_code(result))

    def test_keyboard_interrupt_dispatch_preserves_unknown_original_identity(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',KeyboardInterrupt())])
        with self.assertRaises(KeyboardInterrupt):
            cancel_local(self.config,api,self.record,now=lambda:NOW)
        value=json.loads(self.record.read_text())
        self.assertEqual('unknown',value['cancellation']['dispatch_state'])
        self.assertEqual('invoke-original',value['command']['invoke_id'])

    def test_observation_merge_retains_concurrent_cancellation_identity(self):
        self.prepare_record()
        class ConcurrentAPI:
            def call(inner,action,**parameters):
                if action == 'DescribeInstances':return EMPTY
                value=json.loads(self.record.read_text())
                value['cancellation']={'invoke_id':'cancel-original','dispatch_state':'accepted'}
                _save(self.record,value)
                return row(status='Running')
        status_local(self.config,ConcurrentAPI(),self.record)
        self.assertEqual('cancel-original',json.loads(self.record.read_text())['cancellation']['invoke_id'])

    def test_credential_expiry_refusal_precedes_allocation(self):
        class Refused(Store):
            def presign(inner,*args):raise ValueError('known credential expiry is too short')
        store=Refused();store.objects.update(self.store.objects)
        with self.assertRaisesRegex(ValueError,'expiry'):
            start_local(self.config,self.plan,ScriptAPI([]),ScriptAPI([]),store,image_id='m-exact',
                source=self.source,record_path=self.record,now=lambda:NOW)
        self.assertFalse(self.record.exists())

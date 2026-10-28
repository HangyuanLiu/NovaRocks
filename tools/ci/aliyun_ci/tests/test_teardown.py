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

"""Exact cleanup outcome tests; the fake service dictates real absence/presence facts."""
from datetime import timedelta
import json
import unittest
from pathlib import Path
import tempfile
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.ports import APIError
from aliyun_ci.teardown import delete_exact, teardown_run
from .test_config import example
from .test_start import ScriptAPI, EMPTY, NOW, instance, listed


class Store:
    def __init__(self):self.objects={}
    def get(self,key):
        if key not in self.objects:raise APIError('NoSuchKey')
        return self.objects[key]
    def put(self,key,data,**kwargs):self.objects[key]=data


class EmptyGitHub:
    def call(self,method,path,body=None):return {'runners':[]}


class TeardownTests(unittest.TestCase):
    def setUp(self):
        self.config=validate_config(example());self.plan=derive(self.config,'42',1,NOW)
    def test_start_output_loss_recovers_full_tags_and_repeated_cleanup_is_idempotent(self):
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
                       ('DescribeInstances',listed(instance(self.config,self.plan))),('DeleteInstance',{}),('DescribeInstances',EMPTY),
                       ('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]}),('ListSecretParameters',{'Parameters':[]})])
        store=Store()
        for _ in range(2):
            summary=teardown_run(self.config,self.plan,api,oos,store,EmptyGitHub(),rust_result={'exit_code':100} if _ == 0 else None,verify=lambda data:{'present':True},now=lambda:NOW,sleep=lambda _:None)
            self.assertEqual('released',summary['compute_terminal'])
            self.assertEqual({'exit_code':100},summary['rust_result'])
            self.assertFalse(summary['handoff']['final']['present'])
        deletes=[params for action,params in api.calls if action=='DeleteInstance']
        self.assertEqual([{'RegionId':'cn-beijing','InstanceId':'i-exact','Force':True}],deletes)
        self.assertEqual(self.plan.tags['run_id'],next(r['Value'] for r in api.calls[0][1]['Tag'] if r['Key']=='run_id'))
    def test_unknown_deletion_does_not_claim_release(self):
        value=listed(instance(self.config,self.plan))
        api=ScriptAPI([('DescribeInstances',value),('DeleteInstance',TimeoutError()),('DescribeInstances',value)])
        clock=[NOW]
        def advance(seconds):clock[0]+=timedelta(minutes=20)
        with self.assertRaisesRegex(APIError,'i-exact'):
            delete_exact(self.config,api,'i-exact',self.plan.tags,now=lambda:clock[0],sleep=advance)
    def test_wrong_resource_is_never_deleted(self):
        value=instance(self.config,self.plan,RegionId='another-region')
        api=ScriptAPI([('DescribeInstances',listed(value))])
        with self.assertRaisesRegex(ValueError,'ownership'):
            delete_exact(self.config,api,'i-exact',self.plan.tags,now=lambda:NOW)
        self.assertEqual(1,len(api.calls))

    def test_verified_native_index_is_preserved_verbatim_with_archive_reference(self):
        from aliyun_ci import handoff
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'attempt';(root/'rust').mkdir(parents=True)
            index={'schema':1,'exit_code':100,'domains':{'nextest':{'status':'FAIL','exit_code':100},'doctests':{'status':'PASS'}}}
            (root/'rust/execution-index.json').write_text(json.dumps(index))
            (root/'tools-check-result.json').write_text('{"exit_code":0}')
            raw=handoff.pack(root,output=Path(directory)/'final.tar.gz').read_bytes()
        store=Store();store.objects[self.plan.oss_keys['final']]=raw
        api=ScriptAPI([('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]})])
        result=teardown_run(self.config,self.plan,api,oos,store,EmptyGitHub(),now=lambda:NOW)
        self.assertEqual(index,result['rust_result']['value'])
        self.assertEqual('attempt/rust/execution-index.json',result['rust_result']['member'])
        self.assertEqual('final',result['rust_result']['archive_kind'])
        self.assertEqual(self.plan.oss_keys['final'],result['rust_result']['archive_key'])
        self.assertTrue(result['tools_check'])

    def test_verified_archive_without_native_members_keeps_results_unknown(self):
        from aliyun_ci import handoff
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'attempt';root.mkdir()
            (root/'stdout.log').write_text('test output is not an execution result')
            raw=handoff.pack(root,output=Path(directory)/'final.tar.gz').read_bytes()
        store=Store();store.objects[self.plan.oss_keys['final']]=raw
        result=teardown_run(self.config,self.plan,ScriptAPI([('DescribeInstances',EMPTY)]),
            ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),store,EmptyGitHub(),now=lambda:NOW)
        self.assertTrue(result['handoff']['final']['manifest_ok'])
        self.assertIsNone(result['rust_result'])
        self.assertIsNone(result['tools_check'])

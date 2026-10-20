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
"""Identity tests use a tiny repository and independent expected content hashes."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import shutil
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
import identity_snapshot as identity


class Identities(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.repo / '.gitignore').write_text('ignored\ninputs/\nidentity/\n')
        (self.repo / 'source').write_text('A')
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')

    def git(self, *argv):
        return subprocess.check_output(['git'] + list(argv), cwd=self.repo).decode().strip()

    def test_temporary_index_covers_dirty_untracked_and_excludes_ignored(self):
        original = (self.repo / '.git/index').read_bytes()
        clean = identity.source_snapshot(self.repo)['source']
        self.assertEqual(clean['worktree_tree'], self.git('rev-parse', 'HEAD^{tree}'))
        (self.repo / 'source').write_text('B')
        dirty = identity.source_snapshot(self.repo)['source']
        self.assertNotEqual(clean['worktree_tree'], dirty['worktree_tree'])
        (self.repo / 'untracked').write_text('new')
        untracked = identity.source_snapshot(self.repo)['source']
        self.assertNotEqual(dirty['worktree_tree'], untracked['worktree_tree'])
        (self.repo / 'ignored').write_text('noise')
        self.assertEqual(untracked, identity.source_snapshot(self.repo)['source'])
        self.assertEqual(original, (self.repo / '.git/index').read_bytes())

    def test_environment_unset_empty_and_file_content(self):
        declared = [{'name':'VALUE','kind':'value'}, {'name':'PROGRAM','kind':'file-content'}]
        unset = identity.env_inputs(declared, {})
        empty = identity.env_inputs(declared, {'VALUE':''})
        self.assertNotEqual(unset['VALUE'], empty['VALUE'])
        value = identity.env_inputs(declared, {'PROGRAM':str(self.repo / 'source')})
        self.assertEqual(value['PROGRAM'], hashlib.sha256(b'A').hexdigest())
        self.assertNotIn('PROGRAM', identity.env_inputs(declared, {}, values_only=True))

    def test_scope_confirmation_is_independent_of_attempt_end(self):
        path = self.repo / 'identity/stage.json'
        def snapshot():
            return {'stage':{'declared':True,'inputs':identity.file_inputs(self.repo,['source'])}}
        record = identity.begin(path, 'stage', snapshot())
        identity.confirm(path, record, snapshot())
        self.assertTrue(json.loads(path.read_text())['scopes']['stage']['inputs_stable'])
        self.assertFalse((self.repo / 'identity/source-end.json').exists())
        (self.repo / 'source').write_text('changed')
        identity.confirm(path, record, snapshot())
        self.assertFalse(json.loads(path.read_text())['scopes']['stage']['inputs_stable'])

    def rust_inputs(self):
        inputs = self.repo / 'inputs'
        inputs.mkdir()
        data = {'cargo-metadata.json':{'workspace_members':['p'], 'packages':[{'id':'p','metadata':{'novarocks':{'test':{'runtime-inputs':['source'],'env-inputs':[{'name':'NOVAROCKS_BIN','kind':'file-content'},{'name':'FLAG','kind':'value'}]}}}}]},
                'prepared.json':{'profile':'default'},'test-artifacts.json':{'binaries':{'p':{'package-id':'p','path':str(self.repo/'source')}}},
                'selected-tests.json':{'rust-suites':{'p':{'testcases':{'case':{'filter-match':{'status':'matches'}}}}}}}
        for name,value in data.items(): (inputs/name).write_text(json.dumps(value))
        (inputs/'nextest.toml').write_text('[profile.default]\n')
        return inputs

    def test_rust_identities_only_change_for_relevant_inputs(self):
        inputs = self.rust_inputs()
        env = {'NOVAROCKS_BIN':str(self.repo/'source')}
        first = identity.rust_snapshot(self.repo, inputs, env, 'nextest')
        (self.repo/'unrelated').write_text('noise')
        self.assertEqual(first,identity.rust_snapshot(self.repo,inputs,env,'nextest'))
        env['FLAG']=''
        self.assertNotEqual(first,identity.rust_snapshot(self.repo,inputs,env,'nextest'))
        before = identity.rust_snapshot(self.repo,inputs,env,'nextest')
        (inputs/'nextest.toml').write_text('[profile.default]\nretries=0\n')
        self.assertNotEqual(before,identity.rust_snapshot(self.repo,inputs,env,'nextest'))

    def test_unselected_suites_do_not_read_runtime_or_environment_inputs(self):
        inputs=self.rust_inputs()
        selected=json.loads((inputs/'selected-tests.json').read_text())
        selected['rust-suites']['unselected']={'testcases':{'skipped':{'filter-match':{'status':'mismatch'}}}}
        (inputs/'selected-tests.json').write_text(json.dumps(selected))
        # No binding exists for the unselected suite: even attempting to inspect
        # its executable or package would fail this independent selection.
        scopes=identity.rust_snapshot(self.repo,inputs,{},'nextest')
        self.assertEqual(list(scopes),['p::case'])

    def test_native_cargo_null_metadata_means_undeclared(self):
        inputs=self.rust_inputs()
        metadata=json.loads((inputs/'cargo-metadata.json').read_text())
        metadata['packages'][0]['metadata']=None
        (inputs/'cargo-metadata.json').write_text(json.dumps(metadata))
        scopes=identity.rust_snapshot(self.repo,inputs,{},'nextest')
        self.assertFalse(scopes['p::case']['declared'])
        self.assertFalse(identity.rust_snapshot(self.repo,inputs,{},'doctest')['workspace']['declared'])

    def test_attempt_material_paths_do_not_enter_execution_identity(self):
        inputs=self.rust_inputs()
        another=self.repo/'identity/other-inputs'
        shutil.copytree(inputs,another)
        (inputs/'tool-config.toml').write_text('[store]\ndir = "first"\n')
        (another/'tool-config.toml').write_text('[store]\ndir = "second"\n')
        self.assertEqual(identity.rust_snapshot(self.repo,inputs,{},'nextest'),
                         identity.rust_snapshot(self.repo,another,{},'nextest'))

    def test_doctest_ignores_binary_content_but_nextest_does_not(self):
        inputs = self.rust_inputs()
        binary = inputs/'server'
        binary.write_text('one')
        env = {'NOVAROCKS_BIN':str(binary)}
        nextest = identity.rust_snapshot(self.repo,inputs,env,'nextest')
        doctest = identity.rust_snapshot(self.repo,inputs,env,'doctest')
        binary.write_text('two')
        self.assertNotEqual(nextest,identity.rust_snapshot(self.repo,inputs,env,'nextest'))
        self.assertEqual(doctest,identity.rust_snapshot(self.repo,inputs,env,'doctest'))


if __name__ == '__main__': unittest.main()

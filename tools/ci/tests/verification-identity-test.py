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
"""Saved identity comparisons require content and independent confirmation."""
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.identity import confirmed, digest, same_identity, store_only_tool_config


class Identity(unittest.TestCase):
    def test_canonical_mapping_order_and_missing_inputs(self):
        self.assertTrue(same_identity({'a': 1, 'b': 2}, {'b': 2, 'a': 1}))
        self.assertFalse(same_identity(None, None))

    def test_each_declared_input_change_invalidates(self):
        before = {'executable_sha256': 'e1', 'runtime_inputs': {'case': 'v1'}, 'env': {'V': 'unset'},
                  'server_sha256': 's1', 'fixture': 'f1', 'topology': [1, 3],
                  'nextest_config_sha256': 'c1', 'source': {'commit': 'same', 'worktree_tree': 'A'}}
        for key in before:
            after = dict(before, **{key: 'changed'})
            self.assertNotEqual(digest(before), digest(after), key)

    def test_nonsemantic_source_does_not_enter_plain_test_identity(self):
        identity = {'executable_sha256': 'same', 'runtime_inputs': {}, 'env': {}}
        source_a, source_b = {'worktree_tree': 'A'}, {'worktree_tree': 'B'}
        self.assertNotEqual(digest(source_a), digest(source_b))
        self.assertEqual(digest(identity), digest(dict(identity)))

    def test_confirmation_requires_equal_start_end(self):
        self.assertTrue(confirmed({'inputs_start': {'x': 1}, 'inputs_end': {'x': 1}, 'inputs_stable': True}))
        self.assertFalse(confirmed({'inputs_start': {'x': 1}, 'inputs_end': {'x': 2}, 'inputs_stable': True}))
        self.assertFalse(confirmed({'inputs_start': {'x': 1}, 'inputs_stable': True}))

    def test_only_store_directory_is_nonsemantic(self):
        self.assertTrue(store_only_tool_config('[store]\ndir = "/run/A"\n'))
        self.assertTrue(store_only_tool_config('[store]\ndir = "/run/B"\n'))
        self.assertFalse(store_only_tool_config('[store]\ndir = "/run/A"\nretries = 3\n'))
        self.assertFalse(store_only_tool_config('[store]\ndir = "/run/A"\n[profile]\n'))


if __name__ == '__main__':
    unittest.main()

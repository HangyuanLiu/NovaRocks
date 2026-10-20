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
"""Membership and lifetime scope precede observed outcomes."""
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.model import Scope, UnitKey
from verification.scope import default_scope, required_scopes


class Scopes(unittest.TestCase):
    def test_case_scope_requires_state_contract(self):
        unit = UnitKey('sql-case', 's/a')
        self.assertEqual(default_scope(unit), 'sql-suite:s')
        self.assertEqual(default_scope(unit, {'scope': 'case'}), 'sql-suite:s')
        self.assertEqual(default_scope(unit, {'scope': 'case', 'initial_state': 'reset'}), 'sql-case:s/a')

    def test_layer_membership_is_declaration_driven(self):
        regular = Scope('stage:fmt', ('stage:fmt',))
        heavy = Scope('stage:heavy', ('stage:heavy',), tier='heavy')
        suspended = Scope('stage:paused', ('stage:paused',), suspended=True)
        unselected = Scope('stage:later', ('stage:later',), selected=False)
        declarations = [regular, heavy, suspended, unselected]
        self.assertEqual([s.key for s in required_scopes('dev', declarations)], ['stage:fmt', 'stage:heavy'])
        self.assertEqual([s.key for s in required_scopes('admission', declarations)], ['stage:fmt', 'stage:later'])
        self.assertEqual([s.key for s in required_scopes('full', declarations)], ['stage:fmt', 'stage:heavy', 'stage:later'])

    def test_modes_are_native_unit_dimension(self):
        normal = UnitKey('stage', 'memory-model:loom')
        loom = UnitKey('stage', 'memory-model:loom', 'loom')
        self.assertNotEqual(normal, loom)
        self.assertEqual(UnitKey.parse(str(loom)), loom)
        self.assertEqual(UnitKey.parse({'kind': 'rust-doctest', 'id': 'workspace'}), UnitKey('rust-doctest', 'workspace'))

    def test_duplicate_units_or_scopes_are_rejected(self):
        with self.assertRaises(ValueError):
            Scope('s', ('stage:x', 'stage:x'))
        with self.assertRaises(ValueError):
            required_scopes('full', [Scope('s', ('stage:x',)), Scope('s', ('stage:y',))])


if __name__ == '__main__':
    unittest.main()

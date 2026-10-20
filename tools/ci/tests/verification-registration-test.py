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
"""A passing observation does not close a full-verification registration."""
import copy
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.registration import derive


def full(verdict, target='new', cleanup=False, quarantine=False):
    counter = verdict in ('failed', 'inconsistent')
    return {'schema': 1, 'layer': 'full', 'target': {'commit': target}, 'scopes': [{
        'key': 'sql-suite:s', 'verdict': verdict, 'excluded': {},
        'quarantined': {'sql-case:s/a': {}} if quarantine else {},
        'observations': [{'unit': 'sql-case:s/a', 'category': 'counterexample' if counter else 'pass'}],
        'accepted_execution': {'attempt': target},
        'cleanup_obligations': [{'scope': 'sql-suite:s', 'summary': 'unknown'}] if cleanup else []}]}


class Registration(unittest.TestCase):
    def test_failure_open_repeat_pass_and_explicit_fix_close(self):
        original = []
        records, delta = derive(original, full('failed'))
        self.assertEqual(delta['new'], ['sql-suite:s'])
        self.assertEqual(original, [])
        records, delta = derive(records, full('inconsistent'))
        self.assertEqual(delta['still_failing'], ['sql-suite:s'])
        records, delta = derive(records, full('satisfied', 'pass'))
        self.assertEqual(delta['last_pass_open'], ['sql-suite:s'])
        self.assertEqual(records[0]['status'], 'open')
        link = {'scope': 'sql-suite:s', 'commit': 'fix-sha', 'verified_after_fix': True, 'target': {'commit': 'fixed'}}
        records, delta = derive(records, full('satisfied', 'fixed'), [link])
        self.assertEqual(delta['closed'], ['sql-suite:s'])
        self.assertEqual(records[0]['status'], 'closed')

    def test_inconsistent_is_never_fixed_and_infra_is_separate(self):
        records, _ = derive([], full('failed'))
        records, delta = derive(records, full('inconsistent', cleanup=True), [{'scope': 'sql-suite:s', 'commit': 'fix', 'verified_after_fix': True}])
        self.assertEqual(records[0]['status'], 'open')
        self.assertEqual(len(delta['infra']), 1)
        records, delta = derive([], full('not_run', cleanup=True))
        self.assertEqual(records, [])
        self.assertEqual(len(delta['infra']), 1)

    def test_quarantined_failure_has_no_new_product_registration(self):
        records, delta = derive([], full('failed', quarantine=True))
        self.assertEqual(records, [])
        self.assertEqual(len(delta['quarantine']), 1)

    def test_different_fix_target_does_not_close(self):
        records, _ = derive([], full('failed'))
        before = copy.deepcopy(records)
        link = {'scope': 'sql-suite:s', 'commit': 'fix', 'verified_after_fix': True, 'target': {'commit': 'other'}}
        updated, delta = derive(records, full('satisfied'), [link])
        self.assertEqual(updated[0]['status'], 'open')
        self.assertEqual(records, before)
        self.assertEqual(delta['closed'], [])


if __name__ == '__main__':
    unittest.main()

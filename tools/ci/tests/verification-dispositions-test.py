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
"""Dispositions cannot hide missing execution, cleanup, or another failure."""
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.compose import compose
from verification.dispositions import Dispositions
from verification.model import Attempt, Observation, Scope, UnitKey


class Disposition(unittest.TestCase):
    def entry(self, failure=None, **changes):
        value = {'unit': 'sql-case:s/a', 'failure': failure or {'class': 'error_code', 'code': 'NR-1'},
                 'reason': 'diagnosed defect', 'owner': 'sql-owner', 'expires': '2026-11-01'}
        value.update(changes)
        return value

    def parse(self, entry):
        return Dispositions({'schema': 1, 'known_failures': [entry], 'quarantine': []}, '2026-10-19')

    def observation(self, result='fail', failure=None):
        return Observation(UnitKey('sql-case', 's/a'), 'sql-suite:s', 'a', 'L', result=result,
                           failure=failure or {'class': 'error_code', 'error_code': 'NR-1'})

    def test_expiry_and_exact_code(self):
        self.assertIsNotNone(self.parse(self.entry()).match(self.observation()))
        self.assertIsNone(self.parse(self.entry(expires='2026-10-18')).match(self.observation()))
        self.assertIsNotNone(self.parse(self.entry(expires='2026-10-19')).match(self.observation()))
        self.assertIsNone(self.parse(self.entry()).match(self.observation(failure={'class': 'error_code', 'error_code': 'NR-2'})))

    def test_exact_mismatch_statement_and_digest(self):
        policy = self.parse(self.entry({'class': 'mismatch', 'statement': 3, 'actual_digest': 'sha256:abc'}))
        self.assertIsNotNone(policy.match(self.observation(failure={'class': 'mismatch', 'statement_index': 3, 'actual_digest': 'sha256:abc'})))
        self.assertIsNone(policy.match(self.observation(failure={'class': 'mismatch', 'statement_index': 3, 'actual_digest': 'sha256:def'})))

    def test_unsupported_classes_and_units_are_rejected(self):
        for failure in ({'class': 'assertion'}, {'class': 'timeout'}, {'class': 'error'}, {'class': 'load'}):
            with self.subTest(failure=failure), self.assertRaises(ValueError):
                self.parse(self.entry(failure))
        with self.assertRaises(ValueError):
            self.parse(self.entry(unit='rust-test:bin::name'))

    def test_required_owner_reason_and_date(self):
        for field in ('owner', 'reason', 'expires'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(self.entry(**{field: ''}))

    def test_no_disposition_for_cancelled_unknown_or_missing(self):
        policy = self.parse(self.entry())
        for status in ('pass', 'cancelled', 'unknown', 'not_run'):
            self.assertIsNone(policy.match(self.observation(result=status)))
        self.assertEqual(len(policy.stale([self.observation(result='pass')])), 1)

    def test_collateral_nonexecuted_failure_does_not_match_statement_failure(self):
        policy = self.parse(self.entry())
        for reason in ('suite_init_failed', 'load_failed', 'aborted'):
            observation = self.observation()
            observation.detail = {'reason': reason}
            self.assertIsNone(policy.match(observation), reason)

    def test_quarantine_expires(self):
        entry = {'unit': 'system-scenario:scenario', 'owner': 'owner', 'reason': 'unstable',
                 'exit_condition': 'deterministic fix', 'review_by': '2026-10-18'}
        policy = Dispositions({'schema': 1, 'quarantine': [entry]}, '2026-10-19')
        self.assertIsNone(policy.excluded('system-scenario:scenario'))
        del entry['exit_condition']
        with self.assertRaises(ValueError):
            Dispositions({'schema': 1, 'quarantine': [entry]}, '2026-10-19')

    def test_primary_code_cannot_dispose_an_actual_failed_oracle(self):
        policy = self.parse(self.entry())
        for kind, step in [('expected_error',1),('be_log',1),('result_set',1),
                           ('text_assertions',2),('query_stats_contract',3)]:
            with self.subTest(kind=kind,step=step):
                observation = self.observation()
                observation.failure['statement_index'] = 1
                observation.detail = {'oracle':{'executed':[{'step':step,'kind':kind,'outcome':'fail'}]}}
                self.assertIsNone(policy.match(observation))

    def test_missing_or_unexecuted_oracle_does_not_invent_a_secondary_failure(self):
        policy = self.parse(self.entry())
        for oracle in ({}, {'required':[{'step':2,'kind':'be_log'}],'executed':[]},
                       {'executed':[{'step':1,'kind':'expected_error','outcome':'pass'}]}):
            with self.subTest(oracle=oracle):
                observation = self.observation()
                observation.detail = {'oracle':oracle}
                self.assertIsNotNone(policy.match(observation))

    def test_only_the_exact_known_mismatch_result_oracle_is_disposed(self):
        policy = self.parse(self.entry({'class':'mismatch','statement':3,'actual_digest':'sha256:abc'}))
        observation = self.observation(failure={'class':'mismatch','statement_index':3,'actual_digest':'sha256:abc'})
        primary = {'step':3,'kind':'result_set','outcome':'fail'}
        observation.detail = {'oracle':{'executed':[primary]}}
        self.assertIsNotNone(policy.match(observation))
        for other in ({'step':4,'kind':'result_set','outcome':'fail'},
                      {'step':3,'kind':'be_log','outcome':'fail'},
                      {'step':3,'kind':'expected_error','outcome':'fail'}):
            with self.subTest(other=other):
                observation.detail = {'oracle':{'executed':[primary,other]}}
                self.assertIsNone(policy.match(observation))

    def test_raw_oracle_facts_also_prevent_matching_nested_diagnostic_codes(self):
        policy = self.parse(self.entry())
        observation = self.observation()
        observation.detail = {'raw':{'oracle':{'executed':[{'step':2,'kind':'be_log','outcome':'fail'}]}}}
        self.assertIsNone(policy.match(observation))
        observation.detail = {}
        observation.failure = {'class':'error','primary_failure':{'class':'error_code','error_code':'NR-1'},
                               'additional_failures':[{'class':'error','phase':'case_cleanup'}]}
        self.assertIsNone(policy.match(observation))

    def test_direct_compose_rejects_failed_oracles_in_dev_and_formal_admission(self):
        policy = self.parse(self.entry())
        observation = self.observation()
        observation.identity = {'suite':'same'}
        observation.identity_confirmed = observation.record_complete = observation.terminated = True
        observation.cleanup = 'ok'
        observation.detail = {'oracle':{'executed':[]}}
        attempt = Attempt('a',[observation],current=True,formal=True,
                          source={'commit':'a'*40,'dirty':False})
        scope = Scope('sql-suite:s',(observation.unit,),observation.identity,'isolated')
        for layer in ('dev','admission'):
            with self.subTest(layer=layer):
                observation.detail['oracle']['executed'] = []
                baseline = compose({},layer,[attempt],[scope],policy)
                self.assertEqual(baseline['scopes'][0]['verdict'],'known_failure')
                self.assertTrue(baseline['layer_ok'])
                observation.detail['oracle']['executed'] = [{'step':2,'kind':'expected_error','outcome':'fail'}]
                actual = compose({},layer,[attempt],[scope],policy)
                self.assertEqual(actual['scopes'][0]['verdict'],'failed')
                self.assertFalse(actual['layer_ok'])
                raw = actual['scopes'][0]['observations'][0]
                self.assertEqual(raw['category'],'counterexample')
                self.assertEqual(raw['failure']['error_code'],'NR-1')


if __name__ == '__main__':
    unittest.main()

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
"""Independent fixtures protect composition against false positive conclusions."""
import copy
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.compose import compose
from verification.model import Attempt, Observation, Scope, UnitKey

A = UnitKey('sql-case', 's/a')
B = UnitKey('sql-case', 's/b')
I = {'server_sha256': 'server-v1', 'suite_inputs_sha256': 'suite-v1'}
SOURCE = {'commit': 'a' * 40, 'dirty': False}


def scope(**changes):
    values = dict(key='sql-suite:s', units=(A, B), identity=I, initial_state='isolated')
    values.update(changes)
    return Scope(**values)


def obs(unit=A, attempt='current', result='pass', **changes):
    values = dict(unit=unit, scope='sql-suite:s', attempt=attempt, lifecycle='L', identity=I,
                  result=result, identity_confirmed=True, record_complete=True,
                  terminated=result in {'pass', 'fail'}, cleanup='ok', ref=attempt + '/' + str(unit))
    values.update(changes)
    return Observation(**values)


def attempt(name='current', results=None, formal=False):
    return Attempt(name, results if results is not None else [obs(A, name), obs(B, name)],
                   formal=formal, current=name == 'current', source=SOURCE)


def dispositions(quarantine=False):
    return {'schema': 1, 'known_failures': [{
        'unit': str(A), 'failure': {'class': 'error_code', 'code': 'NR-1'},
        'reason': 'tracked defect', 'owner': 'sql-owner', 'expires': '2999-01-01'}],
        'quarantine': [{'unit': str(B), 'reason': 'tracked instability', 'owner': 'sql-owner',
                        'exit_condition': 'fix flaky oracle', 'review_by': '2999-01-01'}] if quarantine else []}


class Composition(unittest.TestCase):
    def run_case(self, attempts, scopes=None, layer='dev', baseline=None, confirmations=()):
        value = compose({}, layer, attempts, scopes or [scope()], baseline, confirmations)
        return value, value['scopes'][0]['verdict']

    def test_current_pass_and_failure_have_independent_source_and_result(self):
        value, verdict = self.run_case([attempt(results=[obs(result='fail', cleanup='unknown'), obs(B)])])
        self.assertEqual(verdict, 'failed')
        self.assertFalse(value['layer_ok'])
        record = value['scopes'][0]['observations'][0]
        self.assertEqual((record['source'], record['result']), ('current', 'fail'))
        self.assertEqual(len(value['scopes'][0]['cleanup_obligations']), 1)

    def test_old_pass_and_current_failure_are_inconsistent(self):
        old = attempt('old')
        current = attempt(results=[obs(result='fail'), obs(B)])
        for pool in ([old, current], [current, old]):
            value, verdict = self.run_case(pool)
            self.assertEqual(verdict, 'inconsistent')
            self.assertFalse(value['layer_ok'])

    def test_all_old_counterexamples_remain_visible_after_known_failure(self):
        old = attempt('old', [obs(A, 'old', 'fail', failure={'class': 'error_code', 'error_code': 'NR-2'})])
        current = attempt(results=[obs(result='fail', failure={'class': 'error_code', 'error_code': 'NR-1'}), obs(B)])
        _, verdict = self.run_case([old, current], baseline=dispositions())
        self.assertEqual(verdict, 'failed')

    def test_known_failure_does_not_supply_missing_case_coverage(self):
        current = attempt(results=[obs(result='fail', failure={'class': 'error_code', 'error_code': 'NR-1'}),
                                   obs(B, result='not_run', record_complete=False)])
        _, verdict = self.run_case([current], baseline=dispositions())
        self.assertEqual(verdict, 'not_run')

    def test_complete_known_failure_lifecycle_is_disposed(self):
        current = attempt(results=[obs(result='fail', failure={'class': 'error_code', 'error_code': 'NR-1'}), obs(B)])
        value, verdict = self.run_case([current], baseline=dispositions())
        self.assertEqual(verdict, 'known_failure')
        self.assertTrue(value['layer_ok'])

    def test_informal_known_failure_is_not_admission_evidence(self):
        current = attempt(results=[obs(result='fail', failure={'class': 'error_code', 'error_code': 'NR-1'}), obs(B)])
        _, verdict = self.run_case([current], layer='admission', baseline=dispositions())
        self.assertEqual(verdict, 'not_run')

    def test_formal_attempt_requires_clean_full_commit(self):
        formal = attempt(formal=True)
        formal.source = {'commit': 'a123', 'dirty': False}
        _, verdict = self.run_case([formal], layer='admission')
        self.assertEqual(verdict, 'not_run')

    def test_cancelled_gap_can_be_filled_without_deleting_cancelled_observation(self):
        old = attempt('old', [obs(A, 'old', 'cancelled', record_complete=False), obs(B, 'old', 'not_run')])
        value, verdict = self.run_case([old, attempt()])
        self.assertEqual(verdict, 'satisfied')
        self.assertEqual(sum(o['category'] == 'coverage_gap' for o in value['scopes'][0]['observations']), 2)

    def test_cleanup_unknown_remains_until_specific_confirmation(self):
        old = attempt('old', [obs(A, 'old', 'cancelled', cleanup='unknown')])
        value, verdict = self.run_case([old, attempt()])
        self.assertEqual(verdict, 'satisfied')
        self.assertFalse(value['layer_ok'])
        confirmation = {'schema': 1, 'attempt': 'old', 'scope': 'sql-suite:s', 'lifecycle': 'L',
                        'complete': True, 'summary': 'ok', 'checks': [{'ok': True, 'resource': 'actual owner record'}]}
        value, _ = self.run_case([old, attempt()], confirmations=[confirmation])
        self.assertTrue(value['layer_ok'])
        for changes in ({'attempt': 'other'}, {'checks': []}, {'complete': False}, {'summary': 'unknown'}):
            invalid = dict(confirmation, **changes)
            value, _ = self.run_case([old, attempt()], confirmations=[invalid])
            self.assertFalse(value['layer_ok'])

    def test_pass_with_cleanup_violation_is_diagnostic(self):
        value, verdict = self.run_case([attempt(results=[obs(cleanup='violation'), obs(B, cleanup='violation')])])
        self.assertEqual(verdict, 'diagnostic_only')
        self.assertFalse(value['layer_ok'])

    def test_partial_suite_lifecycles_cannot_be_spliced(self):
        old = attempt('old', [obs(A, 'old')])
        current = attempt(results=[obs(B)])
        _, verdict = self.run_case([old, current])
        self.assertEqual(verdict, 'diagnostic_only')

    def test_suite_identity_change_invalidates_every_case(self):
        changed = scope(identity=dict(I, suite_inputs_sha256='suite-v2'))
        _, verdict = self.run_case([attempt('old')], scopes=[changed])
        self.assertEqual(verdict, 'not_run')

    def test_missing_initial_state_or_declared_inputs_blocks_history(self):
        for changes in ({'initial_state': None}, {'inputs_declared': False}):
            _, verdict = self.run_case([attempt('old')], scopes=[scope(**changes)])
            self.assertEqual(verdict, 'diagnostic_only')
        value, verdict = self.run_case([attempt()], scopes=[scope(initial_state=None, inputs_declared=False)])
        self.assertEqual(verdict, 'satisfied')

    def test_unconfirmed_identity_never_supplies_coverage(self):
        value, verdict = self.run_case([attempt(results=[obs(identity_confirmed=False), obs(B)])])
        self.assertEqual(verdict, 'diagnostic_only')
        self.assertEqual(value['scopes'][0]['observations'][0]['category'], 'coverage_gap')

    def test_per_scope_confirmation_survives_missing_attempt_end(self):
        # No source-end or batch completion appears in the model by design.
        value, verdict = self.run_case([attempt('old')])
        self.assertEqual(verdict, 'satisfied')
        self.assertEqual(value['scopes'][0]['accepted_execution']['attempt'], 'old')

    def test_admission_quarantine_is_per_unit(self):
        current = attempt(results=[obs(A), obs(B, result='fail')], formal=True)
        value, verdict = self.run_case([current], layer='admission', baseline=dispositions(True))
        self.assertEqual(verdict, 'satisfied')
        self.assertTrue(value['layer_ok'])
        current.observations[0].result = 'fail'
        _, verdict = self.run_case([current], layer='admission', baseline=dispositions(True))
        self.assertEqual(verdict, 'failed')

    def test_quarantined_unit_cleanup_violation_still_blocks(self):
        current = attempt(results=[obs(A), obs(B, result='fail', cleanup='violation')], formal=True)
        value, verdict = self.run_case([current], layer='admission', baseline=dispositions(True))
        self.assertEqual(verdict, 'satisfied')
        self.assertFalse(value['layer_ok'])

    def test_empty_quarantined_scope_is_excluded_without_claiming_satisfied(self):
        value, verdict = self.run_case([], scopes=[scope(units=(B,))], layer='admission', baseline=dispositions(True))
        self.assertEqual(verdict, 'excluded')
        self.assertIsNone(value['scopes'][0]['accepted_execution'])

    def test_full_ignores_historical_pass_and_dispositions(self):
        _, verdict = self.run_case([attempt('old')], layer='full')
        self.assertEqual(verdict, 'not_run')
        current = attempt(results=[obs(result='fail', failure={'class': 'error_code', 'error_code': 'NR-1'}), obs(B)])
        _, verdict = self.run_case([current], layer='full', baseline=dispositions())
        self.assertEqual(verdict, 'failed')

    def test_invalid_verification_is_diagnostic(self):
        _, verdict = self.run_case([attempt(results=[obs(applicability='diagnostic'), obs(B)])])
        self.assertEqual(verdict, 'diagnostic_only')

    def test_stale_known_failure_requires_applicable_canonical_pass(self):
        for changes in ({'identity': {'different': 'input'}}, {'identity_confirmed': False},
                        {'applicability': 'diagnostic'}):
            value, _ = self.run_case([attempt(results=[obs(**changes), obs(B)])], baseline=dispositions())
            self.assertEqual(value['scopes'][0]['stale_known_failures'], [])
        value, _ = self.run_case([attempt()], baseline=dispositions())
        self.assertEqual(len(value['scopes'][0]['stale_known_failures']), 1)

    def test_scope_and_observation_inputs_are_not_mutated(self):
        current = attempt()
        before = copy.deepcopy(current)
        compose({}, 'dev', [current], [scope()])
        self.assertEqual(current, before)

    def test_deferred_and_modes_remain_explicit_gaps(self):
        loom = UnitKey('stage', 'memory-model:loom', 'loom')
        declaration = Scope(str(loom), (loom,), identity={'mode': 'loom'})
        value = compose({}, 'dev', [], [declaration])
        self.assertEqual(value['scopes'][0]['verdict'], 'not_run')
        _, verdict = self.run_case([], scopes=[scope(deferred={'reason': 'heavy input missing', 'backstop': 'full run'})])
        self.assertEqual(verdict, 'deferred')


if __name__ == '__main__':
    unittest.main()

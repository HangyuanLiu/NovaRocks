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
"""Local SQL admission membership retains native facts and cleanup obligations."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.classification import classify
from verification.dispositions import Dispositions


class Classification(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'raw.jsonl'
        self.records = [
            {'kind':'run','schema':1,'mode':'verify','verification':{'verify_enabled':True},
             'platform':{'system':'fixture','machine':'fixture','release':'fixture'}},
            {'kind':'selection','suites':[{'suite':'s','inputs_sha256':'A','verify_enabled':True,
              'declaration':{'scope':'suite','env_inputs':[],'initial_state':'isolated'},
              'cases':[{'case':'a'},{'case':'b'}]}]},
            *[{'kind':'case','suite':'s','case':name,'lifecycle_id':'L','status':'pass',
                'oracle':{'required':[{'step':1,'kind':'expected_error'}],
                          'executed':[{'step':1,'kind':'expected_error','outcome':'pass'}]}}
              for name in ['a','b']],
            {'kind':'suite','suite':'s','lifecycle_id':'L','inputs_end_sha256':'A',
             'inputs_stable':True,'executed_cases':['a','b'],'cleanup':{'status':'ok'}},
            {'kind':'server','cleanup':{'status':'ok'}},
            {'kind':'fixture','shutdown':{'status':'ok'},'resources':[]},
            {'kind':'end','completed':True,'exit_code':0}]
        self.dispositions = {'schema':1,'known_failures':[],'quarantine':[]}

    def evaluate(self, admission=False):
        self.path.write_text(''.join(json.dumps(r)+'\n' for r in self.records))
        raw=self.path.read_bytes()
        result=classify(self.path, Dispositions(self.dispositions, '2026-10-19'), admission)
        self.assertEqual(raw,self.path.read_bytes())
        self.assertTrue(all(not a['formal'] for a in result['composition']['attempts']))
        return result

    def fail_a(self):
        self.records[2].update(status='fail',failure={'class':'error_code','error_code':'NR-EXACT'})
        self.records[2]['oracle']={'required':[{'step':1,'kind':'result_set'}],'executed':[]}
        self.records[-1]['exit_code']=1

    def quarantine_a(self):
        self.dispositions['quarantine']=[{'unit':'sql-case:s/a','reason':'bounded test quarantine',
          'owner':'test owner','review_by':'2026-11-01','exit_condition':'fix the native failure'}]

    def test_native_pass_and_unexplained_exit(self):
        self.assertEqual(self.evaluate()['status'],'PASS')
        self.records[-1]['exit_code']=1
        self.assertEqual(self.evaluate()['status'],'FAIL')

    def test_quarantine_changes_only_admission_membership(self):
        self.fail_a(); self.quarantine_a()
        self.assertEqual(self.evaluate()['status'],'FAIL')
        result=self.evaluate(True)
        self.assertEqual(result['status'],'PASS')
        self.assertEqual(result['rows'][0]['status'],'QUARANTINED')
        self.assertEqual(result['rows'][0]['raw_status'],'NEW_FAILURE')
        self.assertEqual(result['composition']['scopes'][0]['verdict'],'failed')

    def test_quarantined_execution_retains_cleanup_obligation(self):
        self.fail_a(); self.quarantine_a()
        self.records[-3]['cleanup']['status']='failed'
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def test_other_case_failure_is_not_hidden_by_quarantine(self):
        self.fail_a(); self.quarantine_a()
        self.records[3].update(status='fail',failure={'class':'error_code','error_code':'NR-OTHER'})
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def test_missing_closure_and_cancelled_exit_are_not_dispositions(self):
        self.fail_a(); self.quarantine_a()
        self.records.pop(4)
        self.assertEqual(self.evaluate(True)['status'],'FAIL')
        self.records[-1]['exit_code']=143
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def test_exact_known_failure_requires_complete_other_case(self):
        self.fail_a()
        self.dispositions['known_failures']=[{'unit':'sql-case:s/a',
          'failure':{'class':'error_code','code':'NR-EXACT'},'reason':'bounded existing failure',
          'owner':'test owner','expires':'2026-11-01'}]
        self.assertEqual(self.evaluate()['status'],'KNOWN_FAIL')
        self.records[3]['status']='not_run'
        self.assertEqual(self.evaluate()['status'],'FAIL')

    def test_expired_quarantine_rejoins_membership(self):
        self.fail_a(); self.quarantine_a()
        self.dispositions['quarantine'][0]['review_by']='2026-10-18'
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def test_quarantine_does_not_explain_a_nonzero_exit_without_a_case_failure(self):
        self.quarantine_a()
        self.records[-1]['exit_code']=1
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def test_entire_quarantined_scope_keeps_raw_failure_without_an_empty_scope(self):
        self.fail_a(); self.quarantine_a()
        other=copy.deepcopy(self.dispositions['quarantine'][0]);other['unit']='sql-case:s/b'
        self.dispositions['quarantine'].append(other)
        result=self.evaluate(True)
        self.assertEqual(result['status'],'PASS')
        self.assertEqual(result['composition']['scopes'][0]['verdict'],'failed')
        self.assertEqual(len(result['membership']['excluded']),2)

    def test_known_failure_survives_another_quarantined_failure(self):
        self.fail_a()
        self.dispositions['known_failures']=[{'unit':'sql-case:s/a',
          'failure':{'class':'error_code','code':'NR-EXACT'},'reason':'bounded existing failure',
          'owner':'test owner','expires':'2026-11-01'}]
        self.quarantine_a();self.dispositions['quarantine'][0]['unit']='sql-case:s/b'
        self.records[3].update(status='fail',failure={'class':'error_code','error_code':'NR-OTHER'})
        self.assertEqual(self.evaluate(True)['status'],'KNOWN_FAIL')

    def test_zero_native_exit_cannot_contradict_a_terminal_case_failure(self):
        self.fail_a(); self.quarantine_a()
        self.records[-1]['exit_code']=0
        self.assertEqual(self.evaluate(True)['status'],'FAIL')

    def known_a(self):
        self.fail_a()
        self.records[2]['failure']['statement_index']=1
        self.records[2]['oracle']={'required':[{'step':1,'kind':'result_set'}],
                                  'executed':[]}
        self.dispositions['known_failures']=[{'unit':'sql-case:s/a',
          'failure':{'class':'error_code','code':'NR-EXACT'},'reason':'direct target refusal',
          'owner':'test owner','expires':'2026-11-01'}]

    def test_expected_error_pass_diagnostic_codes_do_not_become_failures(self):
        self.records[2]['reason']='expected error: NR-EXACT, UnknownEngineCode; diagnostic PASS'
        result=self.evaluate()
        self.assertEqual(result['status'],'PASS')
        self.assertTrue(all(row['status']=='PASS' for row in result['rows']))

    def test_known_refusal_is_exact_and_deduplicates_diagnostic_code_text(self):
        self.known_a()
        self.records[2]['reason']='target refused; quoted NR-OTHER and PASS are diagnostic only'
        result=self.evaluate()
        self.assertEqual(result['status'],'KNOWN_FAIL')
        self.assertEqual(sum(row['status']=='KNOWN_FAIL' for row in result['rows']),1)
        for code in ('NR-EXACT.extra','UnknownEngineCode'):
            with self.subTest(code=code):
                self.records[2]['failure']['error_code']=code
                self.assertEqual(self.evaluate()['status'],'FAIL')

    def test_negative_oracle_failure_cannot_inherit_the_observed_error_code(self):
        self.known_a()
        self.records[2]['oracle']={'required':[{'step':1,'kind':'expected_error'}],
          'executed':[{'step':1,'kind':'expected_error','outcome':'fail'}]}
        result=self.evaluate()
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['rows'][0]['status'],'NEW_FAILURE')
        self.assertEqual(result['composition']['scopes'][0]['verdict'],'failed')
        self.assertFalse(result['composition']['layer_ok'])

    def test_failed_verify_diagnostic_code_cannot_inherit_a_known_refusal(self):
        self.known_a()
        self.records[2]['failure']={'class':'mismatch','error_code':'NR-EXACT',
                                   'statement_index':1,'actual_digest':'actual-result'}
        self.records[2]['oracle']['executed']=[{'step':1,'kind':'result_set','outcome':'fail'}]
        result=self.evaluate()
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['rows'][0]['status'],'NEW_FAILURE')

    def test_known_refusal_does_not_hide_a_failed_later_oracle(self):
        self.known_a()
        self.records[2]['oracle']['required'].append({'step':2,'kind':'expected_error'})
        self.records[2]['oracle']['executed']=[{'step':2,'kind':'expected_error','outcome':'fail'}]
        result=self.evaluate()
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['rows'][0]['status'],'NEW_FAILURE')
        self.assertEqual(result['rows'][0]['failure']['error_code'],'NR-EXACT')
        self.assertEqual(result['composition']['scopes'][0]['verdict'],'failed')
        self.assertFalse(result['composition']['layer_ok'])

    def test_known_refusal_does_not_hide_suite_hook_failure(self):
        self.known_a()
        self.records[4]['cleanup']={'status':'failed','errors':['suite cleanup failed']}
        result=self.evaluate()
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['rows'][0]['status'],'KNOWN_FAIL')
        self.records[4]['cleanup']={'status':'ok'}
        self.records[4]['init']={'status':'failed','error':'suite init failed'}
        self.assertEqual(self.evaluate()['status'],'FAIL')

    def test_exact_mismatch_and_quarantined_assertion_keep_g15_dispositions(self):
        self.known_a()
        self.records[2]['failure']={'class':'mismatch','statement_index':1,'actual_digest':'actual'}
        self.records[2]['oracle']['executed']=[{'step':1,'kind':'result_set','outcome':'fail'}]
        self.dispositions['known_failures'][0]['failure']={'class':'mismatch','statement':1,'actual_digest':'actual'}
        result=self.evaluate()
        self.assertEqual(result['status'],'KNOWN_FAIL')
        self.assertEqual(result['composition']['scopes'][0]['verdict'],'known_failure')
        self.assertTrue(result['composition']['layer_ok'])
        self.dispositions['known_failures']=[]
        self.quarantine_a()
        self.assertEqual(self.evaluate(True)['status'],'PASS')

    def test_aggregate_case_failure_diagnostics_never_supply_a_known_code(self):
        self.known_a()
        primary=copy.deepcopy(self.records[2]['failure'])
        for additional in ([{'class':'error','phase':'case_cleanup','error':'database busy'}],
                           [{'class':'error_code','error_code':'NR-OTHER','statement_index':2}]):
            with self.subTest(additional=additional):
                self.records[2]['failure']={'class':'error','primary_failure':primary,
                                           'additional_failures':additional}
                result=self.evaluate()
                self.assertEqual(result['status'],'FAIL')
                self.assertEqual(result['rows'][0]['status'],'NEW_FAILURE')
                self.assertEqual(result['rows'][0]['failure']['primary_failure'],primary)


if __name__ == '__main__': unittest.main()

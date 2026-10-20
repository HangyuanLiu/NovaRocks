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
"""Manual producer-format fixtures with explicit, independent expected verdicts."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.compose import compose
from verification.identity import digest as identity_digest
from verification.materials import (MaterialError, read_json, read_rust, read_sql,
                                    read_invocation, sql_obligation, sql_scope_identity,
                                    system_cleanup, sql_cleanup, read_monitor_cleanup)
from verification.model import Attempt, Scope, UnitKey
from verification.record import write_record


class Materials(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='verification-materials-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value) + '\n')
        return path

    def sql(self, **changes):
        run = {'kind': 'run', 'schema': 1, 'platform': {'system':'Linux','machine':'x86_64','release':'test'}, 'runner': {'sha256': 'runner'}, 'server': {'sha256': 'server'},
               'config': {'sha256': 'config'}, 'topology': {'cluster_mode': 'cross-process', 'cluster_size': 3},
               'mode': 'verify', 'verification': {'verify_enabled': True, 'float_epsilon': None,
                                                'record_from': 'reference'}}
        run.update(changes)
        selected = {'kind': 'selection', 'fixture': {'env_file_sha256': 'fixture'}, 'suites': [{
            'suite': 's', 'inputs_sha256': 'suite-content', 'env': {},
            'verify_enabled': True, 'query_timeout_secs': 30,
            'declaration': {'scope': 'suite', 'initial_state': 'isolated', 'env_inputs': []},
            'cases': [{'case': 'a', 'source_sha256': 'case-a'}, {'case': 'b', 'source_sha256': 'case-b'}]}]}
        a = {'kind': 'case', 'suite': 's', 'case': 'a', 'lifecycle_id': 'L1', 'status': 'pass',
             'oracle': {'required': [{'step': 1, 'kind': 'expected_error'}],
                        'executed': [{'step': 1, 'kind': 'expected_error', 'outcome': 'pass', 'policy': {}}]}}
        b = dict(a, case='b')
        suite = {'kind': 'suite', 'suite': 's', 'lifecycle_id': 'L1', 'executed_cases': ['a', 'b'],
                 'inputs_end_sha256': 'suite-content', 'inputs_stable': True, 'cleanup': {'status': 'ok'}}
        server = {'kind': 'server', 'cleanup': {'status': 'ok'}}
        fixture = {'kind': 'fixture', 'start': 'ok', 'shutdown': {'status': 'ok'}, 'resources': []}
        end = {'kind': 'end', 'completed': True, 'exit_code': 0}
        return [run, selected, a, b, suite, server, fixture, end]

    def sql_file(self, records, name='sql-results.jsonl'):
        path = self.root / name
        path.write_text(''.join(json.dumps(value) + '\n' for value in records))
        return path

    def sql_verdict(self, records, extra=b''):
        path = self.sql_file(records)
        if extra:
            with path.open('ab') as stream:
                stream.write(extra)
        observations, errors = read_sql(path, 'now')
        identity = sql_scope_identity(records[0], records[1], records[1]['suites'][0])
        scope = Scope('sql-suite:s', ('sql-case:s/a', 'sql-case:s/b'), identity=identity, initial_state='isolated')
        value = compose({}, 'dev', [Attempt('now', observations, current=True, errors=errors)], [scope])
        return value['scopes'][0]['verdict'], value, errors

    def test_sql_pure_expected_error_and_explain_oracles_satisfy(self):
        records = self.sql()
        records[3]['oracle'] = {'required': [{'step': 1, 'kind': 'explain_contains'}],
                                'executed': [{'step': 1, 'kind': 'explain_contains', 'outcome': 'pass', 'policy': {}}]}
        verdict, _, errors = self.sql_verdict(records)
        self.assertEqual(verdict, 'satisfied')
        self.assertEqual(errors, [])

    def test_sql_verification_overrides_are_diagnostic(self):
        for override in ({'verify_enabled': False}, {'float_epsilon': .01}, {'order_sensitive_default': True},
                         {'update_expected': True}, {'target_session_sql': [{'sha256': 'x'}]},
                         {'rewrite_explain_contains_as_not_contains': ['x']}, {'dry_run': True}):
            records = self.sql()
            records[0]['verification'].update(override)
            verdict, _, _ = self.sql_verdict(records)
            self.assertEqual(verdict, 'diagnostic_only', override)
        for mode in ('record', 'diff'):
            verdict, _, _ = self.sql_verdict(self.sql(mode=mode))
            self.assertEqual(verdict, 'diagnostic_only')

    def test_sql_requires_every_pass_oracle_and_rejects_unknown_kind(self):
        records = self.sql()
        records[2]['oracle']['executed'] = []
        self.assertEqual(self.sql_verdict(records)[0], 'diagnostic_only')

    def test_platform_changes_sql_identity_and_legacy_material_stays_diagnostic(self):
        records=self.sql()
        suite=records[1]['suites'][0]
        first=sql_scope_identity(records[0],records[1],suite)
        records[0]['platform']=dict(records[0]['platform'],machine='aarch64')
        self.assertNotEqual(identity_digest(first),identity_digest(sql_scope_identity(records[0],records[1],suite)))
        for missing in (None,{}, {'system':'Linux'}, {'system':'Linux','machine':''}):
            records[0]['platform']=missing
            verdict,_,_=self.sql_verdict(records)
            self.assertEqual(verdict,'diagnostic_only')
            observations,_=read_sql(self.sql_file(records),'historical')
            self.assertTrue(all(value.applicability=='diagnostic' for value in observations))
        del records[0]['platform']
        self.assertIsNone(sql_scope_identity(records[0],records[1],suite)['platform'])
        self.assertEqual(self.sql_verdict(records)[0],'diagnostic_only')

    def test_legacy_system_and_stage_platform_is_never_invented(self):
        path=self.system()
        for kind,units in [('system-scenario',['system-scenario:test']),('stage',['stage:check'])]:
            value=json.loads(path.read_text());value.update(kind=kind,units=units)
            value['inputs_start'].pop('platform',None);value['inputs_end'].pop('platform',None)
            path=self.put('legacy-'+kind+'.json',value)
            observations,_=read_invocation(path,'historical')
            self.assertEqual(observations[0].applicability,'diagnostic')
            self.assertNotIn('platform',observations[0].identity)

    def test_sql_actual_suite_switch_and_fixture_runtime_are_identity_inputs(self):
        records = self.sql()
        suite = records[1]['suites'][0]
        first = sql_scope_identity(records[0], records[1], suite)
        records[1]['fixture']['isolated_runtime'] = {'images': {'rest': 'new-image'}}
        self.assertNotEqual(first, sql_scope_identity(records[0], records[1], suite))
        for switch in (False, None):
            suite['verify_enabled'] = switch
            self.assertEqual(self.sql_verdict(records)[0], 'diagnostic_only')
        records[2]['oracle']['required'][0]['kind'] = 'unknown-assertion'
        self.assertEqual(self.sql_verdict(records)[0], 'diagnostic_only')

    def test_case_pass_without_suite_or_server_cleanup_cannot_be_reused(self):
        records = self.sql()
        verdict, value, _ = self.sql_verdict([value for value in records if value['kind'] != 'suite'])
        self.assertEqual(verdict, 'not_run')
        self.assertFalse(value['layer_ok'])
        verdict, value, _ = self.sql_verdict([value for value in records if value['kind'] != 'server'])
        self.assertEqual(verdict, 'diagnostic_only')
        self.assertFalse(value['layer_ok'])

    def test_sql_interruption_preserves_terminal_case_and_missing_tail(self):
        records = self.sql()[:3]
        path = self.sql_file(records)
        observations, errors = read_sql(path, 'now')
        self.assertEqual([value.result for value in observations], ['pass', 'not_run'])
        self.assertEqual(errors, [])
        self.assertTrue(all(value.cleanup == 'unknown' for value in observations))

    def test_sql_skipped_aborted_maps_to_not_run(self):
        records = self.sql()
        records[3].update(status='not_run', reason='aborted')
        self.assertEqual(self.sql_verdict(records)[0], 'diagnostic_only')

    def test_unresolved_selection_prevents_positive_coverage_and_keeps_failures(self):
        for field in ('unresolved_only', 'unresolved_skip'):
            records = self.sql()
            records[1][field] = ['requested-but-missing']
            verdict, value, errors = self.sql_verdict(records)
            self.assertEqual(verdict, 'diagnostic_only')
            self.assertFalse(value['layer_ok'])
            self.assertTrue(any(field in error for error in errors))
            records[2].update(status='fail', failure={'class': 'error_code', 'error_code': 'NR-NEW'})
            verdict, value, errors = self.sql_verdict(records)
            self.assertEqual(verdict, 'failed')
            self.assertEqual(value['scopes'][0]['observations'][0]['failure']['error_code'], 'NR-NEW')

    def test_suite_init_failure_does_not_establish_case_execution(self):
        records = self.sql()
        records[2].update(status='fail', reason='suite_init_failed', failure={'class': 'error_code', 'error_code': 'NR-INIT'})
        path = self.sql_file(records)
        observations, _ = read_sql(path, 'now')
        self.assertEqual(observations[0].result, 'fail')
        self.assertFalse(observations[0].terminated)
        self.assertFalse(observations[0].record_complete)

    def test_unknown_sql_schema_rejects_positive_but_keeps_readable_negative(self):
        records = self.sql(schema=99)
        records[2]['status'] = 'fail'
        verdict, value, errors = self.sql_verdict(records)
        self.assertEqual(verdict, 'diagnostic_only')
        self.assertTrue(errors)
        self.assertEqual(value['scopes'][0]['observations'][0]['result'], 'fail')

    def test_truncated_record_is_not_silently_a_complete_source(self):
        verdict, _, errors = self.sql_verdict(self.sql(), b'{"kind":')
        self.assertEqual(verdict, 'diagnostic_only')
        self.assertTrue(errors)

    def test_sql_own_cleanup_failure_and_external_resource_unknown_are_distinct(self):
        records = self.sql()
        records[6]['resources'] = [{'state': 'unsettled'}]
        self.assertEqual(sql_cleanup(records), 'unknown')
        records[6]['resources'] = [{'state': 'cleanup_failed'}]
        self.assertEqual(sql_cleanup(records), 'violation')
        records[6] = {'kind': 'fixture', 'start': 'failed', 'shutdown': {'status': 'not_called'}, 'resources': []}
        self.assertEqual(sql_cleanup(records), 'ok')

    def test_sql_case_cleanup_flag_survives_aggregate_failure_and_missing_closure(self):
        for failure in (
            {'class':'error','cleanup':True},
            {'class':'error','primary_failure':{'class':'error','cleanup':True}},
            {'class':'error','additional_failures':[{'class':'error','cleanup':True}]},
            {'class':'error','primary_failure':{'class':'error','additional_failures':[
                {'class':'error','primary_failure':{'class':'error','cleanup':True}}]}}):
            with self.subTest(failure=failure):
                records=self.sql()
                records[2].update(status='fail',failure=failure)
                path=self.sql_file(records)
                before=path.read_bytes()
                observations,errors=read_sql(path,'now')
                self.assertEqual(errors,[])
                self.assertEqual([o.cleanup for o in observations],['violation','ok'])
                self.assertEqual(observations[0].failure,failure)
                self.assertEqual(path.read_bytes(),before)
                incomplete=[r for r in records if r['kind'] not in ('suite','end')]
                observations,_=read_sql(self.sql_file(incomplete),'now')
                self.assertEqual([o.cleanup for o in observations],['violation','unknown'])
                self.assertFalse(observations[0].record_complete)

    def test_sql_generic_errors_and_diagnostic_cleanup_text_are_not_cleanup_facts(self):
        for failure in (
            {'class':'error','error':'cleanup failed'},
            {'class':'error','cleanup':False},
            {'class':'error','cleanup':'true'},
            {'class':'error','cleanup':1},
            {'class':'error','diagnostic':{'cleanup':True}},
            {'class':'error','primary_failure':{'class':'error','error':'cleanup failed'},
             'additional_failures':[{'class':'error','cleanup':False}]}
        ):
            with self.subTest(failure=failure):
                records=self.sql()
                records[2].update(status='fail',failure=failure)
                observations,_=read_sql(self.sql_file(records),'now')
                self.assertEqual([o.cleanup for o in observations],['ok','ok'])
                incomplete=[r for r in records if r['kind'] not in ('suite','end')]
                observations,_=read_sql(self.sql_file(incomplete),'now')
                self.assertEqual([o.cleanup for o in observations],['unknown','unknown'])
                records[4]['cleanup']['status']='failed'
                observations,_=read_sql(self.sql_file(records),'now')
                self.assertTrue(all(o.cleanup=='violation' for o in observations))

    def rust(self, terminated=True, doctest=False, tool='[store]\ndir = "/attempt/A"\n'):
        self.put('inputs/prepared.json', {'schema': 1, 'profile': 'default', 'config_sha256': 'config'})
        self.put('inputs/test-artifacts.json', {'schema': 1, 'binaries': {'pkg::integration': {'sha256': 'binary'}}})
        self.put('inputs/selected-tests.json', {'rust-suites': {'pkg::integration': {'testcases': {
            'test_a': {'filter-match': {'status': 'matches'}}}}}})
        (self.root / 'inputs/tool-config.toml').write_text(tool)
        identity = {'executable_sha256': 'binary', 'nextest_profile': 'default',
                    'nextest_config_sha256': 'config', 'runtime_inputs': {}, 'env': {}}
        self.put('identity/nextest.json', {'schema': 1, 'domain': 'nextest', 'scopes': {
            'pkg::integration::test_a': {'declared': True, 'inputs_start': identity,
                                       'inputs_end': identity, 'inputs_stable': True}}})
        self.put('rust-run/cleanup-summary.json', {'schema': 1, 'complete': True, 'ok': True, 'records': [], 'errors': []})
        junit = self.root / 'nextest/default/junit.xml'
        junit.parent.mkdir(parents=True)
        junit.write_text('<testsuites><testsuite name="pkg::integration"><testcase classname="pkg::integration" name="test_a"/></testsuite></testsuites>')
        index = {'schema': 1, 'complete': False, 'stages': {'nextest': {'returncode': 0, 'errors': []}} if terminated else {},
                 'materials': {'inputs': {'path': 'inputs'}, 'selection': {'path': 'inputs/selected-tests.json'},
                               'test_bindings': {'path': 'inputs/test-artifacts.json'},
                               'tool_config': {'path': 'inputs/tool-config.toml'},
                               'junit': {'path': 'nextest/default/junit.xml', 'complete': True},
                               'rust_cleanup': {'path': 'rust-run/cleanup-summary.json'}}}
        if doctest:
            index['stages']['doctest'] = {'returncode': 0, 'errors': []}
            self.put('doctest-run/cleanup-summary.json', {'schema': 1, 'complete': True, 'ok': True, 'records': []})
            index['materials']['doctest_cleanup'] = {'path': 'doctest-run/cleanup-summary.json'}
            self.put('identity/doctest.json', {'schema': 1, 'domain': 'doctest', 'scopes': {'workspace': {
                'declared': True, 'inputs_start': {'worktree_tree': 'same'}, 'inputs_end': {'worktree_tree': 'same'}, 'inputs_stable': True}}})
        self.put('execution-index.json', index)
        return identity

    def rust_verdicts(self):
        observations, errors = read_rust(self.root, 'now')
        scopes = [Scope(value.scope, (value.unit,), identity=value.identity, initial_state='immutable')
                  for value in observations]
        value = compose({}, 'dev', [Attempt('now', observations, current=True, errors=errors)], scopes)
        return {scope['key']: scope['verdict'] for scope in value['scopes']}, value

    def test_rust_checkpoint_preserves_closed_nextest_after_hard_kill_in_doctest(self):
        self.rust()
        verdicts, _ = self.rust_verdicts()
        self.assertEqual(verdicts['rust-test:pkg::integration::test_a'], 'satisfied')
        self.assertEqual(verdicts['rust-doctest:workspace'], 'not_run')

    def test_rust_unclosed_domain_does_not_trust_existing_junit(self):
        self.rust(terminated=False)
        verdicts, _ = self.rust_verdicts()
        self.assertEqual(verdicts['rust-test:pkg::integration::test_a'], 'not_run')

    def test_rust_tool_extension_blocks_reuse(self):
        self.rust(tool='[store]\ndir = "/attempt"\nretries = 4\n')
        verdicts, _ = self.rust_verdicts()
        self.assertEqual(verdicts['rust-test:pkg::integration::test_a'], 'diagnostic_only')

    def test_rust_output_directory_does_not_enter_identity(self):
        self.rust()
        first, _ = read_rust(self.root, 'one')
        (self.root / 'inputs/tool-config.toml').write_text('[store]\ndir = "/different/output"\n')
        second, _ = read_rust(self.root, 'two')
        self.assertEqual(first[0].identity, second[0].identity)
        self.assertTrue(first[0].record_complete and second[0].record_complete)

    def test_rust_scope_input_confirmation_and_bound_hash_are_required(self):
        self.rust()
        path = self.root / 'identity/nextest.json'
        document = json.loads(path.read_text())
        document['scopes']['pkg::integration::test_a']['inputs_end']['env'] = {'changed': 'value'}
        path.write_text(json.dumps(document))
        verdicts, _ = self.rust_verdicts()
        self.assertEqual(verdicts['rust-test:pkg::integration::test_a'], 'not_run')

    def test_rust_doctest_confirmation_is_domain_local(self):
        self.rust(doctest=True)
        verdicts, _ = self.rust_verdicts()
        self.assertEqual(verdicts['rust-doctest:workspace'], 'satisfied')

    def system(self, code=0, outcome='passed', teardown=None):
        evidence = self.put('scenario-evidence.json', {'schema_version': 5, 'scenario': 'test', 'outcome': outcome, 'exit_code': 0})
        teardown = teardown or {'schema': 1, 'fixture': {'teardown': {'status': 'ok'}, 'resources': []},
                                 'cluster': {'launch': 'ok', 'shutdown': 'ok', 'partial_cleanup': None}}
        shutdown = self.put('scenario-teardown.json', teardown)
        invocation = {'schema': 1, 'seq': 1, 'kind': 'system-scenario', 'name': 'test',
                      'units': ['system-scenario:test'], 'ended_at': 'now', 'exit': {'code': code},
                      'inputs_start': {'binary': 'server', 'platform':{'system':'Linux','machine':'x86_64'}},
                      'inputs_end': {'binary': 'server', 'platform':{'system':'Linux','machine':'x86_64'}}, 'inputs_stable': True,
                      'materials': {'evidence': str(evidence), 'teardown': str(shutdown)},
                      'cleanup': {'source': 'producer', 'summary': 'ok'}}
        path = self.put('0001-test.json', invocation)
        return path

    def test_system_passed_evidence_and_actual_error_exit_is_failure(self):
        observations, errors = read_invocation(self.system(code=1), 'now')
        self.assertEqual(errors, [])
        self.assertEqual(observations[0].result, 'fail')
        self.assertEqual(observations[0].cleanup, 'ok')
        self.assertEqual(observations[0].detail['evidence']['outcome'], 'passed')

    def test_native_lowercase_system_outcome_is_required_for_positive_coverage(self):
        observations, errors = read_invocation(self.system(), 'now')
        self.assertEqual(errors, [])
        self.assertEqual(observations[0].result, 'pass')
        observations, _ = read_invocation(self.system(outcome='Passed'), 'now')
        self.assertEqual(observations[0].result, 'unknown')
        observations, _ = read_invocation(self.system(outcome='failed'), 'now')
        self.assertEqual(observations[0].result, 'fail')

    def test_system_early_launch_failure_is_not_cleanup_violation(self):
        teardown = {'schema': 1, 'fixture': {'launch_config': 'failed', 'teardown': {'status': 'ok'}, 'resources': []},
                    'cluster': {'launch': 'not_attempted', 'shutdown': 'not_applicable', 'partial_cleanup': None}}
        self.assertEqual(system_cleanup(teardown), 'ok')
        teardown['fixture']['resources'] = [{'state': 'unsettled'}]
        self.assertEqual(system_cleanup(teardown), 'unknown')
        teardown['fixture']['resources'] = [{'state': 'cleanup_failed'}]
        self.assertEqual(system_cleanup(teardown), 'violation')
        teardown['fixture']['resources'] = []
        teardown['cluster'] = {'launch': 'failed', 'shutdown': 'not_applicable', 'partial_cleanup': 'monitor'}
        self.assertEqual(system_cleanup(teardown), 'unknown')

    def test_pretty_cleanup_summary_confirms_only_partial_monitor_obligation(self):
        summary = {'schema': 1, 'ok': True, 'complete': True, 'registration_closed': True,
                   'records': [], 'errors': [], 'late_registrations': []}
        path = self.root / 'cleanup-summary.json'
        path.write_text(json.dumps(summary, indent=2) + '\n')
        self.assertEqual(read_monitor_cleanup(path), 'ok')
        teardown = {'schema': 1, 'fixture': {'teardown': {'status': 'ok'}, 'resources': []},
                    'cluster': {'launch': 'failed', 'shutdown': 'not_applicable', 'partial_cleanup': 'monitor'}}
        self.assertEqual(system_cleanup(teardown, [read_monitor_cleanup(path)]), 'ok')
        teardown['fixture']['resources'] = [{'state': 'unsettled'}]
        self.assertEqual(system_cleanup(teardown, ['ok']), 'unknown')
        teardown['fixture']['resources'] = [{'state': 'cleanup_failed'}]
        self.assertEqual(system_cleanup(teardown, ['ok']), 'violation')

    def test_monitor_events_require_terminal_schema_and_real_cleanup_result(self):
        path = self.root / 'record.jsonl'
        pending = {'schema': 1, 'event': 'Pending'}
        final = {'schema': 1, 'event': 'Final', 'status_raw': 0, 'startup_error': None,
                 'owner_lost': False, 'errors': [], 'cleanup_complete': True}
        path.write_text(json.dumps(pending) + '\n' + json.dumps(final) + '\n')
        self.assertEqual(read_monitor_cleanup(path), 'ok')
        final['errors'] = ['actual cleanup action failed']
        path.write_text(json.dumps(pending) + '\n' + json.dumps(final) + '\n')
        self.assertEqual(read_monitor_cleanup(path), 'violation')
        path.write_text(json.dumps(final) + '\n')
        self.assertEqual(read_monitor_cleanup(path), 'unknown')

    def test_unknown_system_schema_rejects_positive(self):
        path = self.system()
        self.put('scenario-evidence.json', {'schema_version': 99, 'outcome': 'Passed'})
        observations, errors = read_invocation(path, 'now')
        self.assertTrue(errors)
        self.assertEqual(observations[0].result, 'unknown')
        self.assertFalse(observations[0].record_complete)

    def test_system_evidence_must_belong_to_the_exact_native_scenario(self):
        path = self.system()
        self.put('scenario-evidence.json', {'schema_version': 5, 'scenario': 'another', 'outcome': 'Passed'})
        observations, errors = read_invocation(path, 'now')
        self.assertTrue(errors)
        self.assertEqual(observations[0].result, 'unknown')

    def test_raw_materials_unchanged_and_derived_output_exclusive(self):
        path = self.system()
        paths = list(self.root.rglob('*'))
        before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths if path.is_file()}
        observations, _ = read_invocation(path, 'now')
        scope = Scope('system-scenario:test', ('system-scenario:test',), identity=observations[0].identity)
        value = compose({}, 'dev', [Attempt('now', observations, current=True)], [scope])
        output = self.root / 'composition/result.json'
        write_record(output, value)
        with self.assertRaises(FileExistsError):
            write_record(output, value)
        after = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in before}
        self.assertEqual(before, after)


if __name__ == '__main__':
    unittest.main()

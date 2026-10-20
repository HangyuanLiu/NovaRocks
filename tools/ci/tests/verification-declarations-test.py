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
"""Native enumeration is the declaration oracle; no source-shape checks."""
import copy
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.declarations import native_selector_requests, validate_declarations


def metadata(ignored=None):
    return {'version': 1, 'workspace_members': ['pkg-id'], 'packages': [{
        'id': 'pkg-id', 'name': 'pkg', 'metadata': {'novarocks': {'test': {
            'runtime-inputs': [], 'env-inputs': [],
            'ignored': ignored if ignored is not None else [{
                'filter': 'test(=heavy)', 'class': 'heavy', 'requires': ['fixture:external'],
                'reason': 'Requires an external fixture'}]}}}}]}


def native(names=None):
    names = names if names is not None else {'heavy': (True, 'matches'), 'regular': (False, 'mismatch')}
    return {'rust-suites': {'native-binary': {'testcases': {
        name: {'ignored': ignored, 'filter-match': {'status': status}}
        for name, (ignored, status) in names.items()}}}}


BINDINGS = {'schema': 1, 'binaries': {'native-binary': {'package-id': 'pkg-id'}}}


class Declarations(unittest.TestCase):
    def validate(self, meta=None, source=None, selected=None):
        meta = meta or metadata()
        source = source or native()
        requests = native_selector_requests(meta)
        results = {request['key']: selected or source for request in requests}
        return validate_declarations(meta, source, BINDINGS, results)

    def test_native_exact_ignored_coverage(self):
        result = self.validate()
        self.assertTrue(result['ok'], result['errors'])
        self.assertEqual(result['native_ignored'], 1)
        self.assertEqual(result['coverage'][0]['unit'], 'rust-test:native-binary::heavy')
        self.assertEqual(result['coverage'][0]['class'], 'heavy')

    def test_filters_are_delegated_to_native_parser(self):
        meta = metadata()
        meta['packages'][0]['metadata']['novarocks']['test']['ignored'][0]['filter'] = 'invalid native expression ('
        request = native_selector_requests(meta)[0]
        result = validate_declarations(meta, native(), BINDINGS, {request['key']: {'error': 'native parse diagnostic'}})
        self.assertFalse(result['ok'])
        self.assertIn('native parse diagnostic', result['errors'][0])
        self.assertIn('invalid native expression (', request['filterset'])

    def test_missing_native_evaluation_stays_pending(self):
        result = validate_declarations(metadata(), native(), BINDINGS)
        self.assertFalse(result['ok'])
        self.assertEqual(result['pending_native_selectors'], ['pkg/0'])
        self.assertEqual(result['coverage'][0]['declarations'], [])

    def test_undeclared_inputs_are_visible_and_not_invented(self):
        meta = metadata([])
        meta['packages'][0]['metadata'] = None
        result = validate_declarations(meta, native({'regular': (False, 'matches')}), BINDINGS, {})
        self.assertTrue(result['ok'])
        self.assertEqual(result['no_reuse'], ['pkg'])

    def test_runtime_and_environment_declarations_are_paired(self):
        meta = metadata()
        del meta['packages'][0]['metadata']['novarocks']['test']['env-inputs']
        result = self.validate(meta)
        self.assertFalse(result['ok'])
        self.assertTrue(any('together' in error for error in result['errors']))

    def test_ignored_native_test_needs_a_declaration(self):
        result = self.validate(metadata([]))
        self.assertFalse(result['ok'])
        self.assertTrue(any('no declaration' in error for error in result['errors']))

    def test_heavy_must_select_one_actual_ignored_case(self):
        result = self.validate(selected=native({'regular': (False, 'matches'), 'heavy': (True, 'mismatch')}))
        self.assertFalse(result['ok'])
        self.assertTrue(any('retained native ignored' in error for error in result['errors']))
        result = self.validate(selected=native({'regular': (False, 'mismatch'), 'heavy': (True, 'mismatch')}))
        self.assertFalse(result['ok'])
        self.assertTrue(any('exactly one' in error for error in result['errors']))

    def test_duplicate_filter_coverage_is_ambiguous(self):
        meta = metadata()
        entries = meta['packages'][0]['metadata']['novarocks']['test']['ignored']
        entries.append(copy.deepcopy(entries[0]))
        result = self.validate(meta)
        self.assertFalse(result['ok'])
        self.assertTrue(any('ambiguous' in error for error in result['errors']))

    def test_inactive_internal_helper_is_not_a_phantom_test(self):
        meta = metadata([{'filter': 'test(=feature_helper)', 'class': 'helper', 'reason': 'Feature-gated internal helper'}])
        result = self.validate(meta, native({'regular': (False, 'mismatch')}))
        self.assertTrue(result['ok'], result['errors'])
        self.assertEqual(result['coverage'], [])
        self.assertEqual(result['inactive'], ['pkg/0'])

    def test_metadata_paths_and_env_kinds_are_checked(self):
        meta = metadata()
        declaration = meta['packages'][0]['metadata']['novarocks']['test']
        declaration['runtime-inputs'] = ['../outside']
        declaration['env-inputs'] = [{'name': 'INPUT', 'kind': 'directory'}]
        result = self.validate(meta)
        self.assertFalse(result['ok'])
        self.assertTrue(any('repository-relative' in error for error in result['errors']))
        self.assertTrue(any('environment input' in error for error in result['errors']))

    def test_binding_is_native_package_authority(self):
        other = copy.deepcopy(BINDINGS)
        other['binaries']['native-binary']['package-id'] = 'another-package'
        result = validate_declarations(metadata(), native(), other, {'pkg/0': native()})
        self.assertFalse(result['ok'])
        self.assertTrue(any('escaped its package' in error for error in result['errors']))


if __name__ == '__main__':
    unittest.main()

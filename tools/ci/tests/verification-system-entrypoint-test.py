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
"""Compare P09 target identities with the actual native shell-owned argv/spec."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification import run

OWNER = Path(__file__).resolve().parents[3]


class SystemIdentity(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        config = self.repo / 'tools/ci/fixtures/system-scenarios-base.toml'
        config.parent.mkdir(parents=True)
        config.write_text('fixture = true\n')
        self.programs = {}
        for role in ('primary', 'compatible', 'other-island'):
            path = self.root / role
            path.write_text(role)
            path.chmod(0o700)
            self.programs[role] = path
        self.declaration = {'impact': {'paths': ['tests/**']}, 'env_inputs': [],
                            'initial_state': 'isolated', 'basis': 'fixture', 'heavy': None}
        self.runner = self.root / 'runner'
        self.runner.write_text('#!' + sys.executable + '\nimport json,sys\n'
            'if "--list-declarations" in sys.argv:\n'
            ' print(json.dumps({"schema":1,"scenarios":[{"name":"fixture/one",'
            '"declaration":' + repr(self.declaration) + '}]}))\n'
            'elif "--list-binary-requirements" in sys.argv: print("primary")\n')
        self.runner.chmod(0o700)
        self.native = {'repo': str(self.repo), 'receipt': {
            'novarocks': {'path': str(self.programs['primary'])},
            'novarocks-system-tests': {'path': str(self.runner)}}}
        self.item = {'name': 'fixture/one', 'declaration': self.declaration}
        self.capture = self.root / 'capture.py'
        self.capture.write_text('import json,sys\n'
            'json.dump({"argv":sys.argv[5:],"spec":json.load(open(sys.argv[2]))},open(sys.argv[1],"w"))\n')
        self.selection = self.root / 'selected'
        self.selection.write_text('fixture/one\n')
        patch = mock.patch.object(run.invocation_record, 'tools', return_value={'fixture': 'toolchain'})
        patch.start()
        self.addCleanup(patch.stop)

    def shell_facts(self, variants=False):
        output = self.root / ('shell-variants' if variants else 'shell-primary')
        output.mkdir()
        capture = output / 'facts.json'
        script = r'''source "$OWNER/tools/ci/lib/system_scenarios.sh"
ci_invocation_start() {
  "$PYTHON" "$CAPTURE_SCRIPT" "$CAPTURE" "$CI_INVOCATION_SPEC" "$@"
  echo captured
}
ci_invocation_finish() { :; }
ci_epoch() { echo 1; }
ci_bound_binary() { :; }
ci_run_logged() { : >"$1"; }
ci_record_system_scenario() { :; }
ci_render_summary() { :; }
ci_run_system_scenarios "$RUNNER" "$PRIMARY" "$COMPATIBLE" "$OTHER" "$CONFIG" "$OUTPUT/artifacts" 3 900 "$SELECTION"
'''
        env = dict(os.environ, OWNER=str(OWNER), PYTHON=sys.executable,
            CAPTURE_SCRIPT=str(self.capture), CAPTURE=str(capture), CI_RUN_DIR=str(output),
            REPO_ROOT=str(self.repo), RUNNER=str(self.runner), PRIMARY=str(self.programs['primary']),
            COMPATIBLE=str(self.programs['compatible']) if variants else '',
            OTHER=str(self.programs['other-island']) if variants else '',
            CONFIG=str(self.repo / 'tools/ci/fixtures/system-scenarios-base.toml'),
            OUTPUT=str(output), SELECTION=str(self.selection))
        subprocess.run(['bash', '-c', script], env=env, check=True, capture_output=True, text=True)
        return json.loads(capture.read_text())

    def identity(self, argv, spec):
        return run.invocation_record.snapshot(self.repo,
            run.invocation_record.execution_spec('system-scenario', argv, spec), {})

    def test_native_owner_and_p09_same_inputs_match_with_and_without_variants(self):
        for variants in (False, True):
            with self.subTest(variants=variants):
                if variants:
                    self.native['system_variants'] = {role: {'path': str(self.programs[role]),
                        'sha256': run.identity_snapshot.digest(self.programs[role])}
                        for role in ('compatible', 'other-island')}
                else:
                    self.native.pop('system_variants', None)
                native = self.shell_facts(variants)
                target_argv = run.system_argv(self.native, self.item, self.root / 'different-output')
                target_spec = run.system_spec(self.native, self.item)
                self.assertEqual(target_spec['declared'], native['spec']['declared'])
                self.assertEqual(self.identity(target_argv, target_spec),
                                 self.identity(native['argv'], native['spec']))
                # The material record keeps the original complete argv.
                started = run.invocation_record.start(self.root / 'records', self.repo,
                    'system-scenario', self.item['name'], ['system-scenario:fixture/one'],
                    native['argv'], native['spec'], {})
                self.assertEqual(json.loads(started.read_text())['argv'], native['argv'])

    def test_real_semantic_or_file_changes_remain_distinct(self):
        native = self.shell_facts(variants=True)
        original = self.identity(native['argv'], native['spec'])
        for flag, value in (('--cluster-size', '4'), ('--timeout-secs', '901'), ('--only', 'fixture/two')):
            argv = list(native['argv'])
            argv[argv.index(flag) + 1] = value
            self.assertNotEqual(original, self.identity(argv, native['spec']))
        for path in (self.runner, *self.programs.values(),
                     self.repo / 'tools/ci/fixtures/system-scenarios-base.toml'):
            before = path.read_bytes()
            path.write_bytes(before + b'changed')
            self.assertNotEqual(original, self.identity(native['argv'], native['spec']))
            path.write_bytes(before)

    def test_missing_env_declaration_is_not_declared(self):
        item = dict(self.item, declaration=dict(self.declaration, env_inputs=None))
        self.assertFalse(run.system_spec(self.native, item)['declared'])


if __name__ == '__main__':
    unittest.main()

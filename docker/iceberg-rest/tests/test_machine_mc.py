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

"""The mc adapter retains caller identity, deadline, generation and failures."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import machine_mc as adapter
from scratch import RunBudget, ScratchIdentity


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = {"policy": {"scratch_ttl_seconds": 2000, "max_run_seconds": 500,
                      "cleanup_grace_seconds": 30}, "protected_benchmark_roots": [],
                      "config": {"benchmark_root": "s3://novarocks/benchmark"}}
        self.identity = ScratchIdentity.create("s3://novarocks/test-worktree")
        self.context = RunBudget.admit(self.state, self.identity, seconds=400).as_dict()
        self.owner = mock.Mock()
        self.owner.read_saved_state.return_value = self.state
        self.calls = []
        @contextmanager
        def snapshot(definition, **kwargs):
            self.calls.append((definition, kwargs))
            yield self.state
        self.owner.ready_snapshot.side_effect = snapshot
        self.owner.backend.run_mc.return_value = 'mc-output\n'
        self.args = ['--root', str(self.root), '--daemon', 'daemon-id',
                     '--expected-version', 'version-id', '--expected-generation', 'generation-id']

    def invoke(self, script='private-secret-payload', extra=(), context=None):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, {'NOVA_FIXTURE_RUN_CONTEXT': json.dumps(context or self.context)}, clear=True), \
             mock.patch.object(adapter.machine, 'MachineOwner', return_value=self.owner) as constructor, \
             mock.patch.object(adapter.runtime, 'install_signal_handlers'), \
             mock.patch.object(sys, 'stdin', io.StringIO(script)), redirect_stdout(out), redirect_stderr(err):
            status = adapter.main(self.args + list(extra))
        constructor.assert_called_once_with(self.root, daemon='daemon-id')
        return status, out.getvalue(), err.getvalue()

    def test_work_uses_original_complete_deadline_and_generation(self):
        before = time.monotonic()
        status, out, err = self.invoke()
        self.assertEqual((status, out, err), (0, 'mc-output\n', ''))
        definition, kwargs = self.calls[0]
        self.assertEqual(definition, {'version': 'version-id'})
        self.assertEqual(kwargs['expected_generation'], 'generation-id')
        self.assertAlmostEqual(kwargs['deadline'] - before, 370, delta=2)
        self.assertEqual(self.owner.backend.run_mc.call_args.args, (self.state, 'private-secret-payload'))
        self.assertEqual(self.owner.backend.run_mc.call_args.kwargs['deadline'], kwargs['deadline'])

    def test_invalid_admission_never_acquires_lock_or_runs_script(self):
        context = dict(self.context, cleanup_grace_seconds=40)
        status, out, err = self.invoke(context=context)
        self.assertEqual(status, 75)
        self.assertEqual(out, '')
        self.assertNotIn('private-secret-payload', err)
        self.owner.ready_snapshot.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_admitted_expiry_is_failure_and_cleanup_uses_original_reserve(self):
        now_ms = int(time.time() * 1000)
        context = dict(self.context, started_unix_ms=now_ms-390000, deadline_unix_ms=now_ms+10000)
        status, _, _ = self.invoke(context=context)
        self.assertEqual(status, 1)
        self.owner.ready_snapshot.assert_not_called()
        before = time.monotonic()
        status, _, _ = self.invoke(extra=['--cleanup'], context=context)
        self.assertEqual(status, 0)
        self.assertAlmostEqual(self.calls[0][1]['deadline']-before, 10, delta=2)

    def test_exact_cleanup_uses_registered_pairs_and_settles_only_success(self):
        pairs = [{'user': self.identity.iam_name('init'), 'policy': self.identity.resource_name('init')}]
        request = json.dumps({'identity': self.identity.as_dict(), 'users': pairs})
        sweeper = mock.Mock()
        sweeper.cleanup_run.return_value = {'complete': True, 'removed': [], 'errors': []}
        with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
            status, out, err = self.invoke(request, ['--cleanup', '--cleanup-run'])
        self.assertEqual((status, err), (0, ''))
        self.assertTrue(json.loads(out)['complete'])
        self.assertEqual(sweeper.cleanup_run.call_args.args, (self.identity, pairs))
        self.owner.backend.run_mc.assert_not_called()
        sweeper.cleanup_run.return_value = {'complete': False, 'errors': ['denied']}
        with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
            status, out, err = self.invoke(request, ['--cleanup', '--cleanup-run'])
        self.assertEqual((status, out), (1, ''))
        self.assertIn('ScratchCleanupIncomplete', err)
        self.assertNotIn('private-secret-payload', err)

    def test_expected_activation_failure_is_not_silently_reattached(self):
        self.owner.ready_snapshot.side_effect = adapter.runtime.RuntimeFailure('MachineVersionMismatch', 'activation changed')
        status, out, err = self.invoke()
        self.assertEqual((status, out), (75, ''))
        self.assertIn('MachineVersionMismatch', err)
        self.owner.backend.run_mc.assert_not_called()


if __name__ == '__main__':
    unittest.main()

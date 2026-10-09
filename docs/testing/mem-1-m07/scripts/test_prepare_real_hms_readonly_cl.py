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
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Pure host component drafts. Synthetic records are not provider acceptance.

No test starts Docker, HTTP, Spark, HMS or NovaRocks. The pinned old helper is
loaded for its real closed JSON and metadata validation functions only.
"""
from pathlib import Path
import copy
import importlib.util
import json
import os
import sys
import threading
import time
import uuid
import unittest
from unittest.mock import patch

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
ROOT = Path(__file__).resolve().parents[4]
M = load(Path(__file__).with_name('prepare_real_hms_readonly_cl.py'), 'm07_hms_bulk_component_v2_draft')
B = load(ROOT / M.BASE_PATH, 'm07_hms_bulk_stock_validation_component')
CAP = B.read_json(ROOT / 'docs/testing/mem-1-m07/inputs/real-hms-capability-preflight-freeze-v1.json')
WAREHOUSE = 's3://warehouse/cat-component/rest/hms'

def row(kind, value, sid='create-n0000-s0'):
    return {'phase': sid, 'kind': kind, 'value': value}

def wire(records):
    return b''.join((M.PREFIX + B.canonical(record) + b'\n' for record in records))

def facts(kind, ordinal):
    obj = f'cl_{kind}_{ordinal:06}'
    location = WAREHOUSE + '/cl_ns_0000/' + obj
    value = {'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, kind + str(ordinal))), 'format_version': 2 if kind == 'table' else 1, 'location': location, 'metadata_location': location + '/metadata/test.metadata.json', 'schema_id': 0, 'schema': copy.deepcopy(CAP['input']['schema']), 'schema_count': 1, 'raw_metadata': {'bytes': 100, 'sha256': 'a' * 64}}
    if kind == 'table':
        value.update(snapshot_id=None, snapshot_count=0, default_spec_id=0, spec_count=1, partition_spec={'spec-id': 0, 'fields': []})
    else:
        value.update(version_id=1, version_count=1, default_catalog=M.CATALOG, default_namespace=['cl_ns_0000'], representations=[{'dialect': 'spark', 'sql': f'SELECT id FROM cl_table_{ordinal:06}'}])
    return {'namespace': 'cl_ns_0000', 'object': obj, 'facts': value}

def oracle_records():
    sid = 'before-n0000-s0'
    ns, tables, views = M.names(0)
    result = [row('namespaces', sorted(['default', *[M.names(i)[0] for i in range(32)]]), sid), row('tables', tables, sid), row('views', views, sid), row('all_objects', sorted(tables + views), sid)]
    for ordinal in range(128):
        result += [row('table', facts('table', ordinal), sid), row('view', facts('view', ordinal), sid)]
    return result + [row('complete', {'status': 'complete'}, sid)]

def validate(records, *, complete=True, receipt=None, kind='before', shard=0):
    return M.validate_records(B, records, kind, 0, shard, WAREHOUSE, CAP, ['default'], [], complete=complete, receipt=receipt)

def bare_owner():
    cls = M.make_owner_class(B)
    owner = cls.__new__(cls)
    owner.native_pending = True
    owner.owner_thread_id = threading.get_ident()
    owner.secrets = []
    owner.children = []
    owner.status = {'resource_retained_whole_failure': False, 'cleanup_errors': [], 'retention_barriers': []}
    return owner

class _Patcher:

    def __init__(self, case):
        self.case = case

    def setattr(self, target, name, value):
        original = patch.object(target, name, value)
        original.start()
        self.case.addCleanup(original.stop)
import tempfile

class _ReceiptFixture:
    """Real bounded local receipts plus a synthetic clock; no provider/native owner."""

    def __init__(self, root, clock, late, reject_failed_write=False):
        self.root = Path(root)
        self.clock = clock
        self.late = late
        self.reject_failed_write = reject_failed_write
        self.children = []
        self.clocks = M.CallerClocks(10, 20, 30)
        self.status = {'errors': [], 'cleanup_errors': [], 'resource_retained_whole_failure': False, 'baseline_restored': True, 'cleanup_complete': False}
        self.saved_states = []
        self.revoke_calls = 0

    def retain_cancellation(self, operation, error):
        if not isinstance(error, Exception):
            self.status['resource_retained_whole_failure'] = True

    def check_cleanup_clock(self):
        B.remaining(self.clocks.cleanup_until)

    def cleanup(self):
        self.status['cleanup_complete'] = True
        if self.late == 'cleanup':
            self.clock[0] = self.clocks.cleanup_until

    def save_status(self):
        state = self.status['status']
        if self.reject_failed_write and state == 'EXTERNAL_BULK_LIFECYCLE_FAILED':
            raise OSError('component failed receipt write')
        B.atomic_json(self.root / 'bulk-status.json', self.status)
        self.saved_states.append(state)
        if self.late == 'final-sync' and state == 'EXTERNAL_BULK_LIFECYCLE_SETTLED':
            self.clock[0] = self.clocks.cleanup_until

    def revoke_final_status(self):
        self.revoke_calls += 1
        (self.root / 'bulk-status.json').unlink(missing_ok=True)

class ReadonlyClHostTests(unittest.TestCase):

    def test_original_full_scale_and_non_overlapping_shards(self):
        self.assertTrue(len(M.SHARDS) == 128 and len(set(M.SHARDS)) == 128)
        self.assertTrue(M.NORMAL == B.read_json(ROOT / M.CL_PATH)['normal'])
        self.assertTrue(M.digest(B.bounded_read(ROOT / M.CL_PATH)) == M.CL_SHA)
        for ns in range(32):
            created = [op for shard in range(4) for op in M.mutations('create', ns, shard)]
            dropped = [op for shard in range(4) for op in M.mutations('drop', ns, shard)]
            self.assertTrue(len(created) == len(dropped) == 1025)
            self.assertTrue(sum((op['operation'] == 'create_table' for op in created)) == 512)
            self.assertTrue(sum((op['operation'] == 'create_view' for op in created)) == 512)
            self.assertTrue(len({(op['operation'], op['object']) for op in created}) == 1025)

    def test_caller_clocks_refuse_expired_non_finite_or_unordered(self):
        for values in [(10, 20, 30), (float('nan'), 20, 30), (float('inf'), 20, 30), (20, 19, 30), (20, 30, 29), (True, 20, 30)]:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    M.CallerClocks(*values).validate(now=10)

    def test_caller_clock_is_immutable_and_not_refreshed(self):
        clocks = M.CallerClocks(11, 20, 30)
        clocks.validate(now=10)
        with self.assertRaises(Exception):
            clocks.preparation_until = 100
        with self.assertRaises(ValueError):
            clocks.validate(now=11)

    def test_frame_and_json_refusals(self):
        for payload in [b'prefix ' + M.PREFIX + b'{}\n', M.PREFIX + b'{}', M.PREFIX + b'{"phase":"x","phase":"x","kind":"complete","value":{}}\n', M.PREFIX + b'{"phase":"create-n0000-s0","kind":"alien","value":{}}\n', M.PREFIX + b'{"phase":"create-n0000-s0","kind":"complete","value":NaN}\n', b'x' * (M.OUTPUT_CAP + 1), M.PREFIX + b'x' * (M.MARKER_CAP + 1) + b'\n']:
            with self.subTest(payload=payload):
                with self.assertRaises((ValueError, B.Refusal)):
                    M.parse_markers(B, payload, 'create-n0000-s0')

    def test_marker_count_1024_exact_1025_refused_and_phase_exact(self):
        record = row('complete', {'status': 'complete'})
        self.assertTrue(len(M.parse_markers(B, wire([record] * 1024), record['phase'])) == 1024)
        with self.assertRaises(ValueError):
            M.parse_markers(B, wire([record] * 1025), record['phase'])
        with self.assertRaises(ValueError):
            M.parse_markers(B, wire([record]), 'create-n0000-s1')

    def test_actual_validator_requires_all_true_view_facts_and_exact_sets(self):
        records = oracle_records()
        self.assertTrue(len(validate(records)) == 261)
        self.assertTrue(len(wire(records)) < M.OUTPUT_CAP)
        self.assertTrue(len({r['value']['facts']['uuid'] for r in records if r['kind'] in ('table', 'view')}) == 256)
        missing = copy.deepcopy(records)
        missing[2]['value'] = []
        with self.assertRaises(ValueError):
            validate(missing)
        extra = copy.deepcopy(records)
        extra[3]['value'].append('foreign')
        with self.assertRaises(ValueError):
            validate(extra)

    def test_view_metadata_contract_refuses_wrong_actual_facts(self):
        for field, value in [('uuid', str(uuid.UUID(int=0))), ('schema_id', False), ('version_id', 2), ('default_namespace', ['foreign']), ('representations', [])]:
            with self.subTest(field=field, value=value):
                records = oracle_records()
                records[5]['value']['facts'][field] = value
                with self.assertRaises((ValueError, B.Refusal)):
                    validate(records)

    def test_partition_spec_is_loaded_fact_and_not_only_create_intent(self):
        records = oracle_records()
        records[4]['value']['facts']['partition_spec']['fields'] = [{'source-id': 1}]
        with self.assertRaises((ValueError, B.Refusal)):
            validate(records)

    def test_complete_missing_duplicated_reordered_or_credential_fact_refused(self):
        records = oracle_records()
        for changed in (records[:-1], records + records[-1:], records[:4] + [records[5], records[4]] + records[6:]):
            with self.assertRaises(ValueError):
                validate(changed)
        with self.assertRaises(ValueError):
            M.validate_records(B, records, 'before', 0, 0, WAREHOUSE, CAP, ['default'], [records[4]['value']['facts']['uuid']], complete=True)

    def test_partial_unknown_mutation_prefix_retained_after_json_error(self):
        operation = M.mutations('create', 0, 0)[0]
        records = [row('mutation', {**operation, 'state': 'attempt'}), row('mutation', {**operation, 'state': 'applied'})]
        payload = wire(records) + M.PREFIX + b'broken-json\n'
        receipt = {}
        with self.assertRaises((ValueError, B.Refusal)):
            validate(M.iter_markers(B, payload, 'create-n0000-s0'), kind='create', complete=False, receipt=receipt)
        self.assertTrue(receipt['records'] == records)
        self.assertTrue(operation not in receipt['unconfirmed_mutations'])
        self.assertTrue(len(receipt['unconfirmed_mutations']) == 256)
        attempt_receipt = {}
        validate(records[:1], kind='create', complete=False, receipt=attempt_receipt)
        self.assertTrue(operation in attempt_receipt['unconfirmed_mutations'])

    def test_applied_without_attempt_duplicate_identity_and_fake_completion_refused(self):
        operation = M.mutations('create', 0, 0)[0]
        for records in ([row('mutation', {**operation, 'state': 'applied'})], [row('mutation', {**operation, 'state': 'attempt'})] * 2, [row('complete', {'status': 'complete'})]):
            with self.assertRaises(ValueError):
                validate(records, kind='create', complete=False)

    def test_safe_failure_is_terminal_and_never_complete_success(self):
        failure = row('failure', {'exception_class': 'java.io.IOException', 'message_sha256': 'a' * 64})
        validate([failure], kind='create', complete=False)
        with self.assertRaises(ValueError):
            validate([failure, row('complete', {'status': 'complete'})], kind='create', complete=False)
        with self.assertRaises(ValueError):
            validate([failure], kind='create', complete=True)

    def test_program_uses_real_api_exact_range_streaming_hash_and_existing_no_retry_binding(self):
        text = M.program(B, 'before', 31, 3, 'before-n0031-s3', WAREHOUSE)
        self.assertTrue('@@' not in text and len(text.encode()) <= 32768)
        self.assertTrue('val ordinalStart = 384' in text and 'val ordinalEnd = 512' in text)
        self.assertTrue('val namespaceName = "cl_ns_0031"' in text)
        self.assertTrue('cat.buildView(vid).withSchema(schema)' in text)
        self.assertTrue('allProperties.put(HiveCatalog.LIST_ALL_TABLES, "true")' in text)
        self.assertTrue('cat.loadView(vid)' in text and 'cat.loadTable(tid)' in text)
        self.assertTrue('hash.update(buffer, 0, count)' in text and 'total == declared' in text)
        self.assertTrue('ByteArrayOutputStream' not in text and 'toByteArray' not in text)
        self.assertTrue('hive.metastore.failure.retries", "0"' in text)
        self.assertTrue('hive.metastore.connect.retries", "1"' in text)
        self.assertTrue('AwsClientProperties.CLIENT_REGION' in text)

    def test_unresolved_native_owner_blocks_pinned_destructive_cleanup(self):
        monkeypatch = _Patcher(self)
        owner = bare_owner()
        monkeypatch.setattr(B.Preflight, 'cleanup', lambda _: self.fail('dependent owner was destroyed'))
        owner.cleanup()
        self.assertTrue(owner.status['resource_retained_whole_failure'] and owner.native_pending)
        self.assertTrue(owner.status['cleanup_errors'][0]['class'] == 'UnconfirmedExactRoleSettlement')

    def test_native_validator_refusal_or_cancel_preserves_dependency(self):
        for failure in [False, KeyboardInterrupt(), SystemExit(1)]:
            with self.subTest(failure=failure):
                owner = bare_owner()

                def validator(_):
                    if isinstance(failure, BaseException):
                        raise failure
                    return failure
                with self.assertRaises(BaseException):
                    owner.settle_native_window({'synthetic': 'component only'}, validator)
                self.assertTrue(owner.native_pending and owner.status['resource_retained_whole_failure'])

    def test_only_exact_true_callback_resolves_gate_without_claiming_native_acceptance(self):
        owner = bare_owner()
        with self.assertRaises(ValueError):
            owner.settle_native_window({}, lambda _: 1)
        owner = bare_owner()
        owner.settle_native_window({'synthetic': 'component only'}, lambda _: True)
        self.assertTrue(not owner.native_pending)
        self.assertTrue('native_acceptance' not in owner.status)
        self.assertTrue(len(owner.status['native_settlement_receipt_sha256']) == 64)

    def test_context_preserves_original_cancel_even_when_cleanup_also_fails(self):
        monkeypatch = _Patcher(self)

        class FakeOwner:

            def __init__(self):
                self.children = []
                self.status = {'errors': [], 'cleanup_errors': [], 'resource_retained_whole_failure': False}
                self.saved = False

            def retain_cancellation(self, _, error):
                if not isinstance(error, Exception):
                    self.status['resource_retained_whole_failure'] = True

            def check_cleanup_clock(self):
                pass

            def revoke_final_status(self):
                pass

            def cleanup(self):
                raise ValueError('secondary cleanup')

            def save_status(self):
                self.saved = True
        owner = FakeOwner()
        primary = KeyboardInterrupt()
        monkeypatch.setattr(M, 'new_owner', lambda *args: owner)
        with self.assertRaises(KeyboardInterrupt) as seen:
            with M.managed_owner(None, None, None, None, None):
                raise primary
        self.assertTrue(seen.exception is primary and owner.saved)
        self.assertTrue(owner.status['resource_retained_whole_failure'])
        self.assertTrue(owner.status['errors'][0]['class'] == 'KeyboardInterrupt')
        self.assertTrue(owner.status['cleanup_errors'][0]['class'] == 'ValueError')

    def test_baseline_and_restored_prove_actual_empty_baseline_objects(self):
        records = [row('namespaces', ['default'], 'baseline'), row('baseline_objects', {'namespace': 'default', 'objects': []}, 'baseline'), row('complete', {'status': 'complete'}, 'baseline')]
        self.assertTrue(len(M.validate_records(B, records, 'baseline', None, None, WAREHOUSE, CAP, None, [], complete=True)) == 3)
        changed = copy.deepcopy(records)
        changed[1]['value']['objects'] = ['unexpected']
        with self.assertRaises(ValueError):
            M.validate_records(B, changed, 'baseline', None, None, WAREHOUSE, CAP, None, [], complete=True)
        with self.assertRaises(ValueError):
            M.validate_records(B, [records[0], records[2]], 'baseline', None, None, WAREHOUSE, CAP, None, [], complete=True)

    def test_cross_thread_owner_cannot_claim_serial_execution(self):
        owner = bare_owner()
        owner.owner_thread_id = -1
        with self.assertRaises(ValueError):
            owner.cleanup()

    def test_pinned_capture_real_host_python_child_is_reaped_and_group_gone(self):
        children = []
        now = time.monotonic()
        code, output, facts = B.capture([sys.executable, '-c', "print('component-only')"], dict(os.environ), now + 5, M.OUTPUT_CAP, owned_children=children, wall_deadline=now + 6, reap_seconds=5, ownership='verifier-wrapper')
        self.assertTrue(code == 0 and output == b'component-only\n' and (not children))
        self.assertTrue(facts['capture_completed'] and facts['leader_reaped'] and facts['group_exit_confirmed'])
        with self.assertRaises(ProcessLookupError):
            os.kill(facts['child_pid'], 0)

    def test_real_host_output_overflow_keeps_bounded_partial_and_unconfirmed_external_scope(self):
        children = []
        now = time.monotonic()
        script = f"import sys; sys.stdout.buffer.write(b'x' * {M.OUTPUT_CAP + 8192}); sys.stdout.flush()"
        with self.assertRaises(B.CaptureFailure) as captured:
            B.capture([sys.executable, '-c', script], dict(os.environ), now + 5, M.OUTPUT_CAP, owned_children=children, wall_deadline=now + 6, reap_seconds=5, ownership='verifier-wrapper')
        error = captured.exception
        self.assertTrue(len(error.output) == M.OUTPUT_CAP and (not children))
        self.assertTrue(error.exit_facts['leader_reaped'] and error.exit_facts['group_exit_confirmed'])
        self.assertTrue(error.exit_facts['resource_retained_whole_failure'])
        self.assertTrue(error.exit_facts['detached_child_exit_confirmed'] is None)
        with self.assertRaises(ProcessLookupError):
            os.kill(error.exit_facts['child_pid'], 0)

    def test_final_cleanup_and_fsync_use_one_original_deadline_and_rewrite_late_success(self):
        for late in ('cleanup', 'final-sync'):
            with self.subTest(late=late), tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
                clock = [1.0]
                owner = _ReceiptFixture(root, clock, late)
                with patch.object(M, 'new_owner', return_value=owner), patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
                    with self.assertRaises(B.Refusal):
                        with M.managed_owner(None, None, None, None, None):
                            pass
                self.assertEqual(owner.clocks.cleanup_until, 30)
                self.assertEqual(owner.status['status'], 'EXTERNAL_BULK_LIFECYCLE_FAILED')
                self.assertTrue(owner.status['cleanup_complete'])
                self.assertFalse(owner.status['resource_retained_whole_failure'])
                self.assertTrue(owner.status['cleanup_errors'])
                saved = B.read_json(Path(root) / 'bulk-status.json')
                self.assertEqual(saved['status'], 'EXTERNAL_BULK_LIFECYCLE_FAILED')
                self.assertEqual(saved['cleanup_errors'][0]['operation'], 'bulk-cleanup' if late == 'cleanup' else 'bulk-final-receipt')
                if late == 'final-sync':
                    self.assertEqual(owner.saved_states, ['EXTERNAL_BULK_LIFECYCLE_SETTLED', 'EXTERNAL_BULK_LIFECYCLE_FAILED'])
                else:
                    self.assertNotIn('EXTERNAL_BULK_LIFECYCLE_SETTLED', owner.saved_states)

    def test_late_final_sync_failed_failure_write_revokes_original_success_artifact(self):
        with tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
            clock = [1.0]
            owner = _ReceiptFixture(root, clock, 'final-sync', reject_failed_write=True)
            with patch.object(M, 'new_owner', return_value=owner), patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
                with self.assertRaises(B.Refusal):
                    with M.managed_owner(None, None, None, None, None):
                        pass
            self.assertEqual(owner.status['status'], 'EXTERNAL_BULK_LIFECYCLE_FAILED')
            self.assertEqual(owner.revoke_calls, 1)
            self.assertFalse((Path(root) / 'bulk-status.json').exists())
            self.assertEqual([row['operation'] for row in owner.status['cleanup_errors']], ['bulk-final-receipt', 'bulk-failed-receipt'])

    def test_original_cancel_survives_late_cleanup_and_final_receipt_secondary_errors(self):
        with tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
            clock = [1.0]
            owner = _ReceiptFixture(root, clock, 'cleanup')
            primary = KeyboardInterrupt()
            with patch.object(M, 'new_owner', return_value=owner), patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
                with self.assertRaises(KeyboardInterrupt) as seen:
                    with M.managed_owner(None, None, None, None, None):
                        raise primary
            self.assertIs(seen.exception, primary)
            self.assertEqual(owner.status['errors'][0]['class'], 'KeyboardInterrupt')
            self.assertEqual([row['operation'] for row in owner.status['cleanup_errors']], ['bulk-cleanup', 'bulk-final-receipt'])
            self.assertEqual(B.read_json(Path(root) / 'bulk-status.json')['status'], 'EXTERNAL_BULK_LIFECYCLE_FAILED')
            self.assertEqual(owner.clocks.cleanup_until, 30)

    def test_timely_final_cleanup_and_fsync_publish_only_confirmed_component_settlement(self):
        with tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
            clock = [1.0]
            owner = _ReceiptFixture(root, clock, 'none')
            with patch.object(M, 'new_owner', return_value=owner), patch.object(M.time, 'monotonic', side_effect=lambda: clock[0]):
                with M.managed_owner(None, None, None, None, None):
                    pass
            self.assertEqual(owner.saved_states, ['EXTERNAL_BULK_LIFECYCLE_SETTLED'])
            self.assertFalse(owner.status['errors'])
            self.assertFalse(owner.status['cleanup_errors'])
            self.assertEqual(owner.revoke_calls, 0)
            self.assertEqual(owner.clocks.cleanup_until, 30)
            self.assertEqual(B.read_json(Path(root) / 'bulk-status.json')['status'], 'EXTERNAL_BULK_LIFECYCLE_SETTLED')

    def test_broken_error_formatter_cannot_replace_original_lifecycle_failure(self):

        class BrokenMessage(Exception):

            def __str__(self):
                raise RuntimeError('component formatter failure')
        with tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
            owner = _ReceiptFixture(root, [1.0], 'none')
            primary = BrokenMessage()
            with patch.object(M, 'new_owner', return_value=owner), patch.object(M.time, 'monotonic', return_value=1.0):
                with self.assertRaises(BrokenMessage) as seen:
                    with M.managed_owner(None, None, None, None, None):
                        raise primary
            self.assertIs(seen.exception, primary)
            projected = owner.status['errors'][0]
            self.assertEqual(projected['class'], 'BrokenMessage')
            self.assertEqual(projected['reason_unavailable_class'], 'RuntimeError')
            self.assertEqual(projected['reason_bytes'], 0)
            self.assertEqual(projected['reason_sha256'], M.digest(b''))
            self.assertEqual(B.read_json(Path(root) / 'bulk-status.json')['status'], 'EXTERNAL_BULK_LIFECYCLE_FAILED')

    def test_real_owner_clock_guard_and_private_marker_revoke_use_original_fields(self):
        with tempfile.TemporaryDirectory(prefix='m07-cl-component-') as root:
            owner = bare_owner()
            owner.root = Path(root)
            owner.clocks = M.CallerClocks(10, 20, 30)
            marker = owner.root / 'bulk-status.json'
            marker.write_bytes(b'component-only-old-success\n')
            with patch.object(M.time, 'monotonic', return_value=30.0):
                with self.assertRaises(B.Refusal):
                    owner.check_cleanup_clock()
            self.assertEqual(owner.clocks.cleanup_until, 30)
            owner.revoke_final_status()
            self.assertFalse(marker.exists())
            self.assertTrue(owner.root.is_dir())
if __name__ == '__main__':
    unittest.main()

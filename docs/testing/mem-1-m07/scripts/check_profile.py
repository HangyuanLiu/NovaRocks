#!/usr/bin/env python3
"""Check the frozen M07 target arithmetic; this does not verify enforcement."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
p = json.loads((ROOT / 'profile-v1.json').read_text())
MIB = 1024 * 1024
MAX = (1 << 63) - 1

def checked_sum(*values):
    result = 0
    for value in values:
        assert isinstance(value, int) and 0 <= value <= MAX
        result += value
        assert result <= MAX
    return result

def checked_product(a, b):
    result = a * b
    assert 0 <= result <= MAX
    return result

c = p['client_rows']
f = p['frontend_capabilities']
t = p['transport_support']
a = p['frontend_additional_owners']
assert c['data_positions'] == 2 and c['terminal_positions'] == 1
assert 5 <= c['segment_bytes'] <= c['row_payload_bytes'] <= 1024 * MIB
assert c['row_payload_bytes'] == c['mysql_max_allowed_packet_bytes']
tail = f['supported_cancel_burst'] + (f['sustained_cancels_per_second'] * t['short_tail_exit_deadline_ms'] + 999) // 1000
assert tail <= f['short_tail_positions']
assert f['client_compute_positions'] + tail <= f['client_window_positions']
root = checked_sum(2 * c['receiver_backing_bytes_per_segment'], 2 * c['compact_body_backing_bytes_per_segment'], c['schema_backing_bytes'], c['frozen_mysql_metadata_bytes'], c['coalescing_bytes'], c['small_row_staging_bytes'], 65536)
assert root <= c['frontend_window_all_objects_bytes']
assert root <= c['closing_all_objects_bytes']
local = p['local_source']
local_peak = checked_sum(*(local[key] for key in ['snapshot_bytes', 'raw_page_bytes', 'decoded_page_bytes', 'collector_bytes', 'workspace_bytes', 'root_window_bytes']))
assert local_peak <= local['all_objects_per_position_bytes']
d = p['internal_domain']
domain_peak = checked_sum(d['maximum_coexisting_fact_sets'] * d['one_fact_set_bytes'], *(d[key] for key in ['transform_workspace_bytes', 'input_bytes', 'assembly_bytes', 'bookkeeping_bytes', 'root_window_bytes']))
assert domain_peak <= d['all_objects_per_position_bytes']
connections = t['maximum_live_backends'] * sum(n + 2 for n in t['connections_per_frontend_backend'].values())
streams = f['client_window_positions'] + t['maximum_live_backends'] * (f['client_window_positions'] + 64 + 64 + 64)
native_peak = checked_sum(connections * t['connection_all_independent_backings_bytes'], streams * (t['stream_bookkeeping_bytes'] + t['idle_decoder_bytes'] + t['header_raw_and_expanded_bytes']), connections * t['tonic_pending_per_connection'] * 4096, t['nonroot_producer_positions'] * t['nonroot_producer_expanded_bytes'], t['nonroot_encode_positions'] * t['nonroot_wire_actual_backing_bytes'], t['nonroot_decode_positions'] * (t['nonroot_wire_actual_backing_bytes'] + t['nonroot_decoded_expanded_bytes']), f['client_window_positions'] * 12288)
assert native_peak <= t['frontend_native_additional_bytes']
components = {
    'client_windows': f['client_window_positions'] * c['frontend_window_all_objects_bytes'],
    'closing': f['closing_positions'] * c['closing_all_objects_bytes'],
    'local': local['positions'] * local['all_objects_per_position_bytes'],
    'internal': d['positions'] * d['all_objects_per_position_bytes'],
    'ordinary_connections': f['ordinary_connections'] * p['mysql_input']['connection_all_objects_bytes'],
    'control_connections': f['control_connections'] * a['control_connection_all_objects_bytes'],
    'control_owner': a['control_owner_bytes'],
    'waiters': a['waiter_owner_bytes'],
    'native': t['frontend_native_additional_bytes'],
}
fe_peak = checked_sum(*components.values())
assert fe_peak <= a['frontend_declared_result_envelope_bytes']
b = p['be_root_result']
be_peak = checked_sum(b['original_input_backing_capacity_bytes'], b['additional_hydrate_backing_capacity_bytes'], b['scratch_capacity_bytes'], b['small_row_staging_bytes'], (b['active_segment_positions'] + b['queued_segment_positions'] + b['live_send_holders'] * b['independent_payload_copies_per_send']) * b['segment_backing_capacity_bytes'], b['fixed_schema_cursor_driver_capacity_bytes'], 4096)
assert be_peak <= b['joint_retained_bytes_per_root'] <= b['joint_retained_bytes_per_process']
report = {'status': 'TARGET_ARITHMETIC_PASS_NOT_PRODUCT_ENFORCEMENT', 'frontend_component_bytes': components, 'frontend_total_bytes': fe_peak, 'native_actual_target_bytes': native_peak, 'native_connections': connections, 'native_streams': streams, 'be_root_target_bytes': be_peak, 'local_target_bytes': local_peak, 'domain_target_bytes': domain_peak, 'tail_positions': tail}
print(json.dumps(report, indent=2))

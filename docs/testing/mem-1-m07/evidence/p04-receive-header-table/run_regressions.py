#!/usr/bin/env python3
"""Run local assertion regressions against actual source and restore every edit."""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, default=Path.cwd())
    p.add_argument('--private', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--start-at', type=int, default=0)
    args = p.parse_args()
    repo, private = args.repo.resolve(), args.private.resolve()
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CARGO_NET_OFFLINE='true')
    public = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--locked', '--offline', '--test', 'native_h2_receive_header_table']
    scratch = ['cargo', 'test', '--manifest-path', str(private/'Cargo.toml'), '--target-dir', str(private/'target'), '--locked', '--offline']
    forward_test = 'hyper_both_directions_and_tonic_fresh_factory_use_actual_typed_table'
    cases = [
        ('hyper-client-table-forward', repo/'vendor/hyper-1.8.1/src/proto/h2/client.rs', '        builder.receive_header_table_buffer(buffer.clone());', '        let _ = buffer;', public, forward_test, 'FAILED'),
        ('hyper-server-table-forward', repo/'vendor/hyper-1.8.1/src/proto/h2/server.rs', '            builder.receive_header_table_buffer(buffer.clone());', '            let _ = buffer;', public, forward_test, 'FAILED'),
        ('tonic-table-forward', repo/'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs', '            builder.receive_header_table_buffer(buffer);', '            let _ = buffer;', public, forward_test, 'FAILED'),
        ('hyper-server-advertisement', repo/'vendor/hyper-1.8.1/src/proto/h2/server.rs', '            builder.header_table_size(size);', '            let _ = size;', public, forward_test, 'actual SETTINGS header table advertisement'),
        ('tonic-table-predial-validation', repo/'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs', '''        if let Some(buffer) = &self.receive_header_table_buffer {
            if self.receive_header_field_pool.is_none()
                || self.header_table_size.unwrap_or(4096) as usize > buffer.max_table_bytes()
            {
                return Err(invalid("per-connection header table buffer requires a decoded field pool and fitting advertised incoming table size"));
            }
        }
''', '', public, 'tonic_typed_table_dependencies_and_advertisement_are_checked_before_dial', 'before creating a dial future'),
        ('fixed-table-selection', private/'h2-source/src/hpack/decoder.rs', '            decoder.table.entries = TableEntries::Fixed(table);', '            drop(table);', scratch, 'actual_fixed_decoder_inserts_without_table_allocation_and_retains_payload_aliases', 'only the two independently funded field wrappers'),
        ('typed-bound-layout', private/'h2-source/src/receive_header_table.rs', '        layout\n            .size()', '        layout\n            .size().min(0)', scratch, 'actual_typed_backing_fits_original_bound_without_ring_growth', 'complete original bound'),
        ('table-credit-before-vec', private/'h2-source/src/receive_header_table.rs', '    slots: Vec<Option<Header>>,\n    _owner: ReceiveHeaderTableBuffer,', '    _owner: ReceiveHeaderTableBuffer,\n    slots: Vec<Option<Header>>,', scratch, 'actual_typed_backing_fits_original_bound_without_ring_growth', 'typed Vec/Core/Arc must physically exit'),
        ('ack-required-minimum', private/'h2-source/src/hpack/decoder.rs', '            self.required_min = Some(self.required_min.map_or(size, |min| min.min(size)));', '            self.required_min = None;', scratch, 'actual_decoder_preserves_required_minimum_and_latest_ack_limit', 'Some(1024)'),
        ('continuation-resize-state', private/'h2-source/src/hpack/decoder.rs', '        let span = tracing::trace_span!("hpack::decode");', '        self.can_resize = true;\n        let span = tracing::trace_span!("hpack::decode");', scratch, 'actual_decoder_partial_integer_keeps_obligation_and_continuation_cannot_resize', 'unwrap_err'),
    ]
    prior = output/'receipt.json'
    receipts = json.loads(prior.read_text()) if prior.exists() else {}
    for name, path, old, new, command, test, oracle in cases[args.start_at:]:
        original = path.read_bytes(); needle = old.encode()
        assert original.count(needle) == 1, (name, original.count(needle))
        changed = original.replace(needle, new.encode(), 1)
        (output/(name+'.diff')).write_text(''.join(difflib.unified_diff(original.decode().splitlines(True), changed.decode().splitlines(True), fromfile=str(path), tofile=name)))
        try:
            path.write_bytes(changed)
            with (output/(name+'.log')).open('w') as log:
                status = subprocess.run(command+[test, '--', '--test-threads=1', '--nocapture'], cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
            text = (output/(name+'.log')).read_text()
            assert status == 101 and 'test result: FAILED. 0 passed; 1 failed;' in text and 'panicked at' in text and oracle in text, (name, status, text[-1800:])
        finally:
            path.write_bytes(original)
            assert path.read_bytes() == original
        receipts[name] = {'runtime_exit': status, 'runtime_assertion_failed': True, 'before_sha256': hashlib.sha256(original).hexdigest(), 'restored_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'byte_exact_restoration': True}
        print(name+': runtime assertion detected; byte-exact restoration', flush=True)
        (output/'receipt.json').write_text(json.dumps(receipts, indent=2)+'\n')
    for name, command in [('restored-public', public), ('restored-private', scratch)]:
        with (output/(name+'.log')).open('w') as log:
            status=subprocess.run(command+['--','--test-threads=1'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
        assert status == 0, name
    print('restored-public and restored-private passed',flush=True)


if __name__ == '__main__':
    main()

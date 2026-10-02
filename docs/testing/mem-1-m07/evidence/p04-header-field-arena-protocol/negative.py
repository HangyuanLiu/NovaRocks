#!/usr/bin/env python3
"""Exercise actual protocol forwarding mutants; restore exact sources finally."""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
import tempfile


def run(args, log):
    with log.open('w') as output:
        output.write('Command: ' + repr(args) + '\n')
        output.flush()
        process = subprocess.run(args, stdout=output, stderr=subprocess.STDOUT)
        output.write('\nExit: ' + str(process.returncode) + '\n')
    return process.returncode


def main():
    repo = Path(__file__).resolve().parents[5]
    output = Path(tempfile.mkdtemp(prefix='m07-field-arena-negative-'))
    cases = [
        ('hyper-client-forward', 'vendor/hyper-1.8.1/src/proto/h2/client.rs',
         'builder.receive_header_field_pool(pool.clone());', 'let _ = pool;',
         'native_h2_header_field_pool',
         'hyper_client_forwards_field_owners_through_config_clone_and_actual_task_join'),
        ('hyper-server-forward', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
         'builder.receive_header_field_pool(pool.clone());', 'let _ = pool;',
         'native_h2_header_field_pool',
         'hyper_server_forwards_field_owners_through_config_clone_and_actual_task_join'),
        ('tonic-field-forward', 'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs',
         'builder.receive_header_field_pool(pool);', 'drop(pool);',
         'native_h2_header_field_pool',
         'tonic_actual_factory_forwards_field_owners_until_last_response_alias_exit'),
        ('tonic-endpoint-cap', 'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs',
         '.or(inherited_max_header_list_size)', '.or(None)',
         'native_tonic_connection_factory',
         'header_field_pool_validates_dependencies_and_effective_endpoint_cap_before_dial'),
    ]
    receipts = {}
    for name, relative, before, after, target, test in cases:
        source = repo / relative
        original = source.read_bytes()
        text = original.decode()
        assert text.count(before) == 1, (name, text.count(before))
        mutant = text.replace(before, after).encode()
        (output / (name + '.diff')).write_text(''.join(difflib.unified_diff(
            original.decode().splitlines(True), mutant.decode().splitlines(True),
            fromfile=relative, tofile=name)))
        try:
            source.write_bytes(mutant)
            log = output / (name + '.log')
            status = run(['cargo', 'test', '-p', 'novarocks-native-adapter',
                          '--test', target, test, '--', '--exact', '--nocapture'], log)
            observed = log.read_text()
            assert status == 101 and 'test result: FAILED' in observed and 'error[E' not in observed, name
            receipts[name] = {'runtime_exit': status, 'runtime_failed': True,
                              'original_sha256': hashlib.sha256(original).hexdigest()}
        finally:
            source.write_bytes(original)
            assert source.read_bytes() == original
    status = run(['cargo', 'test', '-p', 'novarocks-native-adapter', '--test',
                  'native_h2_header_field_pool', '--test', 'native_tonic_connection_factory',
                  '--', '--test-threads=1'], output / 'restored.log')
    assert status == 0
    result = {'negative_runtime': receipts, 'restored_exit': status,
              'byte_exact_restoration': True}
    (output / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'output': str(output), 'receipt': result}, indent=2))


if __name__ == '__main__':
    main()

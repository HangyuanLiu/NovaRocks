#!/usr/bin/env python3
"""Run the scoped protocol/ownership checks against pinned actual sources."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import tempfile

repo = Path.cwd()
receipt = Path(__file__).parent
for filename, base in [('product-sha256.json', repo), ('vendor-source-sha256.json', repo / 'vendor')]:
    for relative, digest in json.loads((receipt / filename).read_text()).items():
        assert hashlib.sha256((base / relative).read_bytes()).hexdigest() == digest, relative
output = Path(tempfile.mkdtemp(prefix='m07-acquisition-verify-'))
targets = [
    'native_http_header_map_pool', 'native_tonic_header_map_capacity',
    'native_tonic_status_field_pool', 'native_tonic_connection_factory',
    'native_tonic_preallocated_status', 'native_tonic_preallocated_trailers',
    'native_tonic_preallocated_unary', 'root_result_reader', 'native_tonic_request_response_headers',
    'native_ingress_response_headers', 'native_initial_settings_kernel',
    'native_h2_resident_store', 'native_h2_stream_store_owner', 'native_tonic_acquisition_owner',
]
test_command = ['cargo', 'test', '--offline', '-p', 'novarocks-native-adapter']
for target in targets:
    test_command += ['--test', target]
commands = [('related', test_command + ['--', '--test-threads=1'])]
for filter_ in ['native_response::tests', 'native_ingress::tests', 'native_server::', 'native_transport_capacity::tests', 'backend_application::', 'native_client::tests']:
    commands.append((filter_.split('::')[0], ['cargo', 'test', '--offline', '-p', 'novarocks-native-adapter', '--lib', filter_, '--', '--test-threads=1']))
commands += [
            ('check', ['cargo', 'check', '--offline', '-p', 'novarocks-native-adapter']),
            ('h2-clippy', ['cargo', 'clippy', '--offline', '-p', 'h2', '--lib', '--', '-D', 'warnings']),
            ('hyper-clippy', ['cargo', 'clippy', '--offline', '-p', 'hyper', '--lib', '--', '-D', 'warnings']),
            ('http-clippy', ['cargo', 'clippy', '--offline', '-p', 'http', '--lib', '--', '-D', 'warnings']),
            ('tonic-clippy', ['cargo', 'clippy', '--offline', '-p', 'tonic', '--lib', '--', '-D', 'warnings']),
            ('native-clippy', ['cargo', 'clippy', '--offline', '-p', 'novarocks-native-adapter',
                              '--test', 'native_tonic_preallocated_status',
                              '--test', 'native_tonic_preallocated_trailers',
                              '--test', 'native_tonic_preallocated_unary',
                              '--test', 'root_result_reader',
                              '--test', 'native_tonic_request_response_headers',
                              '--test', 'native_ingress_response_headers',
                              '--test', 'native_initial_settings_kernel',
                              '--test', 'native_tonic_connection_factory',
                              '--test', 'native_h2_resident_store',
                              '--test', 'native_h2_stream_store_owner', '--test', 'native_tonic_acquisition_owner', '--lib']),
            ('fmt', ['cargo', 'fmt', '--all', '--', '--check']),
            ('vendor-fmt', ['rustfmt', '--edition', '2021', '--config', 'skip_children=true', '--check',
                            'vendor/h2-0.4.12/src/client.rs',
                            'vendor/h2-0.4.12/src/lib.rs',
                            'vendor/h2-0.4.12/src/proto/mod.rs',
                            'vendor/h2-0.4.12/src/proto/streams/mod.rs',
                            'vendor/h2-0.4.12/src/proto/streams/stream_store.rs',
                            'vendor/h2-0.4.12/src/proto/streams/store.rs',
                            'vendor/h2-0.4.12/src/proto/streams/streams.rs',
                            'vendor/h2-0.4.12/src/proto/streams/recv.rs',
                            'vendor/h2-0.4.12/src/server.rs',
                            'vendor/h2-0.4.12/src/proto/connection.rs',
                            'vendor/h2-0.4.12/src/proto/settings.rs',
                            'vendor/h2-0.4.12/src/codec/mod.rs',
                            'vendor/h2-0.4.12/src/codec/framed_read.rs',
                            'vendor/h2-0.4.12/src/codec/framed_write.rs',
                            'vendor/hyper-1.8.1/src/client/conn/http2.rs',
                            'vendor/hyper-1.8.1/src/server/conn/http2.rs',
                            'vendor/hyper-1.8.1/src/proto/h2/client.rs',
                            'vendor/hyper-1.8.1/src/proto/h2/server.rs',
                            'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs',
                            'vendor/tonic-0.12.3/src/transport/channel/service/connection.rs',
                            'vendor/tonic-0.12.3/src/transport/mod.rs',
                            ]),
            ('diff', ['git', 'diff', '--check'])]
results = {}
for label, command in commands:
    log = output / (label + '.log')
    with log.open('w') as handle:
        handle.write('Command: ' + ' '.join(command) + '\n')
        handle.flush()
        result = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT)
    results[label] = result.returncode
    print(label, result.returncode, flush=True)
    if result.returncode:
        print('evidence directory', output, flush=True)
        raise SystemExit(result.returncode)
results['protocol_targets'] = len(targets)
matches = re.findall(r'test result: ok\. (\d+) passed;', (output / 'related.log').read_text())
assert len(matches) == len(targets)
results['protocol_tests'] = sum(map(int, matches))
results['native_lib_tests'] = sum(int(re.search(r'test result: ok\. (\d+) passed;', (output / (label + '.log')).read_text()).group(1)) for label in ['native_response', 'native_ingress', 'native_server', 'native_transport_capacity', 'backend_application', 'native_client'])
(output / 'verification.json').write_text(json.dumps(results, indent=2) + '\n')
print('evidence directory', output, flush=True)

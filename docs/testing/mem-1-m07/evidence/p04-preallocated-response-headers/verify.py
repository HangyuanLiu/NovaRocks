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
output = Path(tempfile.mkdtemp(prefix='m07-preallocated-response-verify-'))
targets = [
    'native_http_header_map_pool', 'native_tonic_header_map_capacity',
    'native_tonic_status_field_pool', 'native_tonic_connection_factory',
    'native_tonic_preallocated_status', 'native_tonic_preallocated_trailers',
    'native_tonic_preallocated_unary', 'root_result_reader',
]
test_command = ['cargo', 'test', '--offline', '-p', 'novarocks-native-adapter']
for target in targets:
    test_command += ['--test', target]
commands = [('related', test_command + ['--', '--test-threads=1']),
            ('check', ['cargo', 'check', '--offline', '-p', 'novarocks-native-adapter']),
            ('http-clippy', ['cargo', 'clippy', '--offline', '-p', 'http', '--lib', '--', '-D', 'warnings']),
            ('tonic-clippy', ['cargo', 'clippy', '--offline', '-p', 'tonic', '--lib', '--', '-D', 'warnings']),
            ('native-clippy', ['cargo', 'clippy', '--offline', '-p', 'novarocks-native-adapter',
                              '--test', 'native_tonic_preallocated_status',
                              '--test', 'native_tonic_preallocated_trailers',
                              '--test', 'native_tonic_preallocated_unary',
                              '--test', 'root_result_reader']),
            ('fmt', ['cargo', 'fmt', '--all', '--', '--check']),
            ('vendor-fmt', ['rustfmt', '--edition', '2021', '--check',
                            'vendor/http-1.4.0/src/header/map.rs',
                            'vendor/tonic-0.12.3/src/status.rs',
                            'vendor/tonic-0.12.3/src/codec/encode.rs',
                            'vendor/tonic-0.12.3/src/server/grpc.rs',
                            'vendor/tonic-0.12.3/src/server/mod.rs']),
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
(output / 'verification.json').write_text(json.dumps(results, indent=2) + '\n')
print('evidence directory', output, flush=True)

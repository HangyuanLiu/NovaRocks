#!/usr/bin/env python3
"""Mutate actual product sources, require runtime rejection, restore exact bytes."""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
output = Path(tempfile.mkdtemp(prefix='m07-request-response-negatives-'))
cases = [
    ('uninstalled-request-capability', 'vendor/tonic-0.12.3/src/server/grpc.rs',
     'if request.headers().field_allocation_pool().is_none() {', 'if true {',
     'native_tonic_request_response_headers', 'all_four_service_shapes'),
    ('unfunded-response-pair', 'vendor/tonic-0.12.3/src/server/grpc.rs',
     'let initial = http::HeaderMap::try_from_allocation_pool(pool).map_err(refused)?;',
     'let initial = http::HeaderMap::new();',
     'native_tonic_request_response_headers', 'all_four_service_shapes'),
    ('unfunded-capacity-refusal', 'vendor/tonic-0.12.3/src/server/grpc.rs',
     'status.into_http_with_headers(parts.headers)',
     'drop(parts.headers); status.into_http()',
     'native_tonic_request_response_headers', 'insufficient_response_positions'),
    ('uninstalled-ingress-fallback', 'novarocks/native-adapter/src/native_response.rs',
     'if request.headers().field_allocation_pool().is_none() {', 'if true {',
     'native_ingress_response_headers', 'preparation'),
    ('unfunded-native-early-refusal', 'novarocks/native-adapter/src/native_response.rs',
     'if parts.headers.field_allocation_pool().is_some() {', 'if false {',
     'LIB', 'native_server::tests::authentication_refusal'),
    ('unfunded-running-deadline', 'novarocks/native-adapter/src/native_ingress.rs',
     'return Ok(error_headers.respond(Status::from_static(\n                        Code::DeadlineExceeded,\n                        "native ingress deadline elapsed",\n                    )));',
     'return Ok(Status::from_static(\n                        Code::DeadlineExceeded,\n                        "native ingress deadline elapsed",\n                    ).into_http());',
     'native_ingress_response_headers', 'timeout_after_inner'),

]


expected = json.loads(Path(__file__).with_name('product-sha256.json').read_text())
for relative, digest in expected.items():
    assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
originals = {path: (repo / path).read_bytes() for _, path, *_ in cases}
for label, path, old, *_ in cases:
    assert originals[path].decode().count(old) == 1, (label, 'unique source anchor')
try:
    for label, path, old, new, target, filter_ in cases:
        original = originals[path]
        source = original.decode()
        mutated = source.replace(old, new)
        (repo / path).write_text(mutated)
        (output / (label + '.diff')).write_text(''.join(difflib.unified_diff(
            source.splitlines(True), mutated.splitlines(True), fromfile=path, tofile=path)))
        command = ['cargo', 'test', '--offline', '-p', 'novarocks-native-adapter']
        command += ['--lib', filter_] if target == 'LIB' else ['--test', target, filter_]
        command += ['--', '--test-threads=1']
        with (output / (label + '.log')).open('w') as log:
            log.write('Command: ' + ' '.join(command) + '\n')
            log.flush()
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        (repo / path).write_bytes(original)
        log = (output / (label + '.log')).read_text()
        assert result.returncode == 101 and 'test result: FAILED.' in log and '0 passed; 0 failed' not in log, (
            label, result.returncode, 'compiled runtime failure required')
        print(label, 'runtime rejected', flush=True)
finally:
    for path, original in originals.items():
        (repo / path).write_bytes(original)
    assert all((repo / path).read_bytes() == original for path, original in originals.items())
    print('exact source restoration', flush=True)
    print('evidence directory', output, flush=True)

#!/usr/bin/env python3
"""Mutate actual product sources, require runtime rejection, restore exact bytes."""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
output = Path(tempfile.mkdtemp(prefix='m07-preallocated-response-negatives-'))
cases = [
    ('owned-static-message', 'vendor/tonic-0.12.3/src/status.rs',
     'message: field::Message::Static(message),',
     'message: field::Message::Owned(message.to_owned()),',
     'native_tonic_preallocated_status', 'static_status'),
    ('fresh-trailer-map', 'vendor/tonic-0.12.3/src/codec/encode.rs',
     'Some(trailers) => status.into_trailers_with_headers(trailers),',
     'Some(trailers) => { drop(trailers); status.to_header_map() },',
     'native_tonic_preallocated_trailers', 'eof_emits'),
    ('partial-trailers-escape', 'vendor/tonic-0.12.3/src/status.rs',
     'headers.clear();\n                error.metadata = MetadataMap::from_headers(headers);',
     'error.metadata = MetadataMap::from_headers(headers);',
     'native_tonic_preallocated_status', 'trailer_field_exhaustion'),
    ('terminal-source-repoll', 'vendor/tonic-0.12.3/src/codec/encode.rs',
     'if self_proj.state.is_end_stream {\n            return Poll::Ready(None);\n        }',
     'if false { return Poll::Ready(None); }',
     'native_tonic_preallocated_trailers', 'encoder_error_discards'),
    ('unrelated-map-family', 'vendor/tonic-0.12.3/src/server/grpc.rs',
     'if first_map.same_pool(last_map) && first.same_pool(last) =>',
     'if first.same_pool(last) =>',
     'native_tonic_preallocated_unary', 'invalid_response_pairs'),
    ('fresh-unary-response-map', 'vendor/tonic-0.12.3/src/server/grpc.rs',
     'parts.headers = initial;',
     'drop(initial); parts.headers = http::HeaderMap::new();',
     'native_tonic_preallocated_unary', 'successful_unary_moves'),
    ('unfunded-root-unary', 'novarocks/native-adapter/src/root_result_unary.rs',
     '.unary_with_response_headers(service, request, headers)',
     '.unary(service, request)',
     'root_result_reader', 'unary_oversized_request'),
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
        command = ['cargo', 'test', '--offline', '-p', 'novarocks-native-adapter',
                   '--test', target, filter_, '--', '--test-threads=1']
        with (output / (label + '.log')).open('w') as log:
            log.write('Command: ' + ' '.join(command) + '\n')
            log.flush()
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        (repo / path).write_bytes(original)
        log = (output / (label + '.log')).read_text()
        assert result.returncode == 101 and 'test result: FAILED.' in log, (
            label, result.returncode, 'compiled runtime failure required')
        print(label, 'runtime rejected', flush=True)
finally:
    for path, original in originals.items():
        (repo / path).write_bytes(original)
    assert all((repo / path).read_bytes() == original for path, original in originals.items())
    print('exact source restoration', flush=True)
    print('evidence directory', output, flush=True)

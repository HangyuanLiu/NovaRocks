#!/usr/bin/env python3
"""Mutate actual product sources, require runtime rejection, restore exact bytes."""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
output = Path(tempfile.mkdtemp(prefix='m07-encoder-diagnostic-negatives-'))
cases = [
    ('uninstalled-diagnostic-fields', 'vendor/tonic-0.12.3/src/codec/encode.rs',
     'body.inner.diagnostic_fields = trailers.field_allocation_pool().cloned();',
     'body.inner.diagnostic_fields = None;',
     'native_tonic_preallocated_trailers', 'encoder_error_discards'),
    ('copied-formatted-message', 'vendor/tonic-0.12.3/src/status.rs',
     'status.message = field::Message::Shared(message);',
     'status.message = field::Message::Owned(String::from_utf8(message.to_vec()).unwrap());',
     'native_tonic_preallocated_trailers', 'encoder_error_discards'),
    ('heap-fallback-on-format-refusal', 'vendor/tonic-0.12.3/src/status.rs',
     'Err(status) => status,',
     'Err(_) => Self::new(code, args.to_string()),',
     'native_tonic_preallocated_trailers', 'formatted_encoder_diagnostic_maximum'),
    ('unfunded-size-diagnostic', 'vendor/tonic-0.12.3/src/codec/encode.rs',
     'max_message_size,\n        diagnostic_fields,\n        &mut buf[offset..],',
     'max_message_size,\n        None,\n        &mut buf[offset..],',
     'native_tonic_preallocated_trailers', 'size_limit_diagnostic'),
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

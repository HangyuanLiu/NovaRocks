#!/usr/bin/env python3
"""Serialized actual-source regressions; require runtime failure and restore exact bytes."""
from pathlib import Path
import argparse
import difflib
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
output = Path(tempfile.mkdtemp(prefix='m07-status-field-negatives-'))
target = 'native_tonic_status_field_pool'
cases = [
    ('lost-client-attachment', 'vendor/h2-0.4.12/src/client.rs',
     'pool.try_bind_connection_with_fields(fields.allocation_pool())',
     'pool.try_bind_connection()', 'native_h2_header_map_pool',
     'h2_both_directions_maps_clones_iterators'),
    ('lost-server-attachment', 'vendor/h2-0.4.12/src/server.rs',
     '.try_bind_connection_with_fields(fields.allocation_pool())',
     '.try_bind_connection()', 'native_h2_header_map_pool',
     'h2_both_directions_maps_clones_iterators'),
    ('unfunded-status-decode', 'vendor/tonic-0.12.3/src/status.rs',
     'if let Some(pool) = header_map.field_allocation_pool() {',
     'if let Some(pool) = header_map.field_allocation_pool().filter(|_| false) {',
     target, 'cloned_status_http_body'),
    ('unfunded-message-encode', 'vendor/tonic-0.12.3/src/status.rs',
     'Some(pool) => field::encode_message(pool, self.message())?,',
     'Some(_pool) => Bytes::copy_from_slice(Cow::from(percent_encode(self.message().as_bytes(), ENCODING_SET)).as_bytes()),',
     target, 'cloned_status_http_body'),
    ('unfunded-details-encode', 'vendor/tonic-0.12.3/src/status.rs',
     'Some(pool) => field::encode_details(pool, &self.details)?,',
     'Some(_pool) => Bytes::from(crate::util::base64::STANDARD_NO_PAD.encode(&self.details[..])),',
     target, 'actual_h2_hyper_tonic_status_attachment'),
    ('early-field-position-retirement', 'vendor/http-1.4.0/src/header/field_pool.rs',
     'Ok(Bytes::from_owner_with_exit_guard(owner, exit))',
     'drop(exit); Ok(Bytes::from_owner(owner))', 'native_h2_header_field_pool',
     'h2_server_plain_huffman_owners'),
    ('allocated-metadata-refusal', 'vendor/tonic-0.12.3/src/metadata/map.rs',
     '''crate::Status::field_error(
        crate::Code::ResourceExhausted,
        "HTTP metadata capacity exhausted",
    )''',
     'crate::Status::resource_exhausted("HTTP metadata capacity exhausted")',
     target, 'exact_decoded_quantum_and_metadata_refusal'),
    ('overestimated-decoded-quantum', 'vendor/tonic-0.12.3/src/status/field.rs',
     '''let padding = input
        .iter()
        .rev()
        .take(2)
        .take_while(|&&b| b == b'=')
        .count();
    let symbols = input.len() - padding;
    let len = (symbols / 4)
        .checked_mul(3)
        .and_then(|complete| complete.checked_add((symbols % 4) * 6 / 8))''',
     '''let len = (input.len() / 4)
        .checked_add(usize::from(input.len() % 4 != 0))
        .and_then(|groups| groups.checked_mul(3))''',
     target, 'exact_decoded_quantum_and_metadata_refusal'),
]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--start-at', choices=[case[0] for case in cases])
args = parser.parse_args()
if args.start_at:
    cases = cases[next(i for i, case in enumerate(cases) if case[0] == args.start_at):]
expected = json.loads(Path(__file__).with_name('vendor-source-sha256.json').read_text())
for relative, digest in expected.items():
    assert hashlib.sha256((repo / 'vendor' / relative).read_bytes()).hexdigest() == digest, relative
originals = {path: (repo / path).read_bytes() for _, path, *_ in cases}
for label, path, old, *_ in cases:
    assert originals[path].decode().count(old) == 1, (label, 'unique source anchor before mutations')
try:
    for label, path, old, new, test_target, filter_ in cases:
        original = originals[path]
        source = original.decode()
        assert source.count(old) == 1, (label, 'unique source anchor')
        mutated = source.replace(old, new)
        (repo / path).write_text(mutated)
        (output / (label + '.diff')).write_text(''.join(difflib.unified_diff(
            source.splitlines(True), mutated.splitlines(True), fromfile=path, tofile=path)))
        command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--offline',
                   '--test', test_target, filter_, '--', '--test-threads=1']
        with (output / (label + '.log')).open('w') as log:
            log.write('Command: ' + ' '.join(command) + '\n')
            log.flush()
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
        (repo / path).write_bytes(original)
        log = (output / (label + '.log')).read_text()
        assert result.returncode == 101 and 'test result: FAILED.' in log, (
            label, result.returncode, 'runtime failure required')
        print(label, 'runtime failed', result.returncode, flush=True)
finally:
    for path, original in originals.items():
        (repo / path).write_bytes(original)
    for path, original in originals.items():
        assert (repo / path).read_bytes() == original
    print('exact source restoration', {p: hashlib.sha256(b).hexdigest()
                                       for p, b in originals.items()}, flush=True)
    print('negative evidence directory', output, flush=True)

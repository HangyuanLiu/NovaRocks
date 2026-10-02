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
output = Path(tempfile.mkdtemp(prefix='m07-generated-field-negatives-'))
wire = 'native_hyper_generated_field_pool'
public = 'native_http_generated_field_owner'
cases = [
    ('copied-generated-decimal', 'vendor/http-1.4.0/src/header/value.rs',
     'Ok(Self {\n            inner: bytes,\n            is_sensitive: false,\n        })\n    }\n\n    /// Convert a static string',
     'Ok(Self {\n            inner: Bytes::copy_from_slice(&bytes),\n            is_sensitive: false,\n        })\n    }\n\n    /// Convert a static string', public,
     'generated_decimal_goldens_fill_all_positions'),
    ('maximum-width-overestimate', 'vendor/http-1.4.0/src/header/value.rs',
     'pool.try_fill(digits.len(), |output|',
     'pool.try_fill(20, |output|', public,
     'exact_digit_limits_refuse_before_allocation'),
    ('unfunded-client-content-length', 'vendor/hyper-1.8.1/src/proto/h2/client.rs',
     'http::HeaderValue::try_from_u64_with_pool(len, &fields)',
     '{ drop(fields); Ok::<_, std::convert::Infallible>(http::HeaderValue::from(len)) }', wire,
     'generated_client_content_length_refusal'),
    ('unfunded-server-content-length', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
     'http::HeaderValue::try_from_u64_with_pool(len, &fields)',
     '{ drop(fields); Ok::<_, std::convert::Infallible>(http::HeaderValue::from(len)) }', wire,
     'generated_server_content_length_uses_original'),
    ('unfunded-server-date', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
     'date::header_value_with_pool(&fields)',
     '{ drop(fields); Ok::<_, std::convert::Infallible>(date::update_and_header_value()) }', wire,
     'generated_server_date_uses_original'),
]

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--only', choices=[case[0] for case in cases])
parser.add_argument('--start-at', choices=[case[0] for case in cases])
args = parser.parse_args()
if args.only:
    cases = [case for case in cases if case[0] == args.only]
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

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
output = Path(tempfile.mkdtemp(prefix='m07-pseudo-field-negatives-'))
wire = 'native_h2_pseudo_field_pool'
public = 'native_http_pseudo_field_owner'
cases = [
    ('unfunded-new-name-method', 'vendor/h2-0.4.12/src/hpack/header.rs',
     'return Ok(Header::Method(Method::from_owned_bytes(value)?));',
     'return Ok(Header::Method(Method::from_bytes(&value)?));', wire,
     'h2_literal_new_name_long_method'),
    ('unfunded-indexed-name-method', 'vendor/h2-0.4.12/src/hpack/header.rs',
     'Name::Method => Ok(Header::Method(Method::from_owned_bytes(value)?)),',
     'Name::Method => Ok(Header::Method(Method::from_bytes(&value)?)),', wire,
     'indexed_name_and_hyper_huffman_long_method'),
    ('unfunded-peer-scheme', 'vendor/h2-0.4.12/src/server.rs',
     'let maybe_scheme = uri::Scheme::from_owned_bytes(scheme.clone().into_inner());',
     'let maybe_scheme = scheme.parse();', wire,
     'custom_scheme_is_original_owned_after_h2'),
    ('copied-owned-method', 'vendor/http-1.4.0/src/method.rs',
     "AllocatedExtension::new_shared(\n            src,\n        )?",
     "AllocatedExtension::new_shared(\n            Bytes::copy_from_slice(&src),\n        )?", public,
     'shared_pseudo_fields_and_request_clones'),
    ('boxed-owned-scheme', 'vendor/http-1.4.0/src/uri/scheme.rs',
     'Ok(Scheme2::Other(SchemeStorage::Shared(value)).into())',
     'Ok(Scheme2::Other(if false { SchemeStorage::Shared(value) } else { SchemeStorage::Owned(Box::new(value)) }).into())', public,
     'shared_pseudo_fields_and_request_clones'),
    ('storage-dependent-method-hash', 'vendor/http-1.4.0/src/method.rs',
     'self.as_bytes().hash(state);',
     'std::mem::discriminant(&self.0).hash(state); self.as_bytes().hash(state);', public,
     'default_eager_clone_is_preserved'),
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

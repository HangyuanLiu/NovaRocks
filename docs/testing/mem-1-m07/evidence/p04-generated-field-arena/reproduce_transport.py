#!/usr/bin/env python3
"""Probe complete normal Hyper sources with explicit forwarding/cache injection only."""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--miri', action='store_true')
parser.add_argument('--mutation', choices=['date-copy', 'cold-cache'])
args = parser.parse_args()
repo = Path.cwd()
evidence = Path(__file__).resolve().parent
pins = json.loads((evidence / 'vendor-source-sha256.json').read_text())
for relative, digest in pins.items():
    assert hashlib.sha256((repo / 'vendor' / relative).read_bytes()).hexdigest() == digest, relative
root = Path(tempfile.mkdtemp(prefix='m07-generated-field-hyper-'))
packages = [('http', '1.4.0'), ('bytes', '1.11.0'), ('h2', '0.4.12'), ('hyper', '1.8.1')]
for name, version in packages:
    shutil.copytree(repo / f'vendor/{name}-{version}', root / f'{name}-{version}')
hyper = root / 'hyper-1.8.1/src'
if args.mutation:
    date = hyper / 'common/date.rs'
    original = date.read_text()
    if args.mutation == 'date-copy':
        old = 'HeaderValue::from_maybe_shared(bytes)'
        new = 'HeaderValue::from_bytes(&bytes)'
    else:
        old = 'cache.check();\n            output.copy_from_slice(cache.buffer());'
        new = '''cache.check();
            if cache.header_value.is_none() {
                cache.header_value = Some(HeaderValue::from_bytes(cache.buffer()).unwrap());
            }
            output.copy_from_slice(cache.buffer());'''
    assert original.count(old) == 1, (args.mutation, 'unique actual-source anchor')
    changed = original.replace(old, new)
    date.write_text(changed)
    (root / 'mutation.diff').write_text(''.join(difflib.unified_diff(
        original.splitlines(True), changed.splitlines(True),
        fromfile='hyper-1.8.1/src/common/date.rs', tofile='hyper-1.8.1/src/common/date.rs')))
    print('Actual normal-source mutation:', args.mutation, flush=True)
with (hyper / 'lib.rs').open('a') as output:
    output.write('''
/// Private receipt forwarding to the actual bounded Date producer.
pub fn __m07_date(pool: &http::header::HeaderFieldAllocationPool)
    -> std::result::Result<http::HeaderValue, http::header::HeaderFieldFillError<std::convert::Infallible>> {
    common::date::header_value_with_pool(pool)
}
/// Private receipt forwarding to the actual ordinary Date producer.
pub fn __m07_ordinary_date() -> http::HeaderValue {
    common::date::update_and_header_value()
}
/// Deterministic cache state injection; does not replace rendering/check logic.
pub fn __m07_seed_date() { common::date::__m07_seed_date(); }
''')
with (hyper / 'common/date.rs').open('a') as output:
    output.write('''
pub(crate) fn __m07_seed_date() {
    CACHED.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache.update(UNIX_EPOCH);
        cache.next_update = SystemTime::now() + Duration::from_secs(86400);
    });
}
''')
(root / 'src').mkdir()
shutil.copyfile(evidence / 'transport-driver.rs', root / 'src/lib.rs')
manifest = '''[package]
name="m07-generated-field-hyper-probe"
version="0.1.0"
edition="2021"
[workspace]
exclude=["http-1.4.0","bytes-1.11.0","h2-0.4.12","hyper-1.8.1"]
[lib]
doctest=false
[dependencies]
hyper={path="hyper-1.8.1",default-features=false,features=["http2","server"]}
http={path="http-1.4.0"}
bytes={path="bytes-1.11.0"}
[patch.crates-io]
'''
for name, version in packages:
    manifest += f'{name}={{path="{name}-{version}"}}\n'
(root / 'Cargo.toml').write_text(manifest)
shutil.copyfile(repo / 'Cargo.lock', root / 'Cargo.lock')
print('Complete normal-source Hyper probe workspace:', root, flush=True)
commands = [['cargo', 'test', '--manifest-path', str(root / 'Cargo.toml'), '--offline', '--lib']]
if args.miri:
    commands.append(['cargo', '+nightly', 'miri', 'test', '--manifest-path', str(root / 'Cargo.toml'), '--offline', '--locked', '--lib'])
for index, command in enumerate(commands):
    print('Command:', ' '.join(command), flush=True)
    env = dict(os.environ)
    if 'miri' in command:
        # The actual Date implementation reads REALTIME; do not substitute a clock.
        env['MIRIFLAGS'] = (env.get('MIRIFLAGS', '') + ' -Zmiri-disable-isolation').strip()
        print('MIRIFLAGS:', env['MIRIFLAGS'], flush=True)
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    (root / f'run-{index}.log').write_text(result.stdout)
    print(result.stdout, end='', flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
production = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in tomllib.loads((repo / 'Cargo.lock').read_text())['package']}
deps = [p for p in tomllib.loads((root / 'Cargo.lock').read_text())['package'] if p['name'] != 'm07-generated-field-hyper-probe']
for p in deps:
    assert (p['name'], p['version'], p.get('source'), p.get('checksum')) in production, p
print('Verified production dependency identities:', len(deps), flush=True)

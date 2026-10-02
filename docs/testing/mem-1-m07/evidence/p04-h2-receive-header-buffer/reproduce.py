#!/usr/bin/env python3
"""Probe exact current fixed encoded input source; no product/workspace edits."""
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


def run(command, root, name, env):
    print('Command: ' + ' '.join(command), flush=True)
    lines = []
    with (root / (name + '.log')).open('w') as log:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True) as child:
            for line in child.stdout:
                lines.append(line); log.write(line); log.flush(); print(line, end='', flush=True)
            status = child.wait()
    print(f'{name} exit: {status}', flush=True)
    return status, ''.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--miri', action='store_true')
    parser.add_argument('--quality', action='store_true')
    parser.add_argument('--negative', action='store_true')
    args = parser.parse_args(); repo = args.repo.resolve(); evidence = Path(__file__).resolve().parent
    root = Path(tempfile.mkdtemp(prefix='m07-h2-header-input-')); (root / 'src').mkdir()
    shutil.copytree(repo / 'vendor/bytes-1.11.0', root / 'bytes-source')
    production = repo / 'vendor/h2-0.4.12/src/receive_header.rs'
    original = production.read_bytes()
    hashes = {'vendor/h2-0.4.12/src/receive_header.rs': hashlib.sha256(original).hexdigest()}
    for path in [repo / 'Cargo.lock', repo / 'vendor/bytes-1.11.0/Cargo.toml', *sorted((repo / 'vendor/bytes-1.11.0/src').rglob('*.rs'))]:
        hashes[path.relative_to(repo).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    (root / 'source-sha256.json').write_text(json.dumps(hashes, indent=2) + '\n')
    path = root / 'src/lib.rs'
    path.write_bytes(b'#![allow(dead_code)]\n' + original + b'\n' + (evidence / 'probe-tests.rs').read_bytes())
    (root / 'Cargo.toml').write_text('[package]\nname="m07-h2-header-input-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\nexclude=["bytes-source"]\n[lib]\ndoctest=false\n[dependencies]\nbytes="=1.11.0"\n[patch.crates-io]\nbytes={path=' + json.dumps(str(root / 'bytes-source')) + '}\n')
    shutil.copyfile(evidence / 'probe-Cargo.lock', root / 'Cargo.lock')
    prod = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in tomllib.loads((repo / 'Cargo.lock').read_text())['package']}
    for p in tomllib.loads((root / 'Cargo.lock').read_text())['package']:
        if p['name'] != 'm07-h2-header-input-probe':
            assert (p['name'], p['version'], p.get('source'), p.get('checksum')) in prod
    print(f'Probe workspace: {root}', flush=True)
    print('Current source SHA256: ' + hashes['vendor/h2-0.4.12/src/receive_header.rs'], flush=True)
    common = ['--manifest-path', str(root / 'Cargo.toml'), '--offline', '--locked']
    env = dict(os.environ, CARGO_NET_OFFLINE='true')
    ordinary = ['cargo', 'test', *common, '--', '--test-threads=1', '--nocapture']
    if run(ordinary, root, 'ordinary', env)[0]: raise SystemExit('Ordinary probe failed')
    stable = path.read_bytes()
    if args.negative:
        mutants = [
            ('append-copy', b'        let start = self.filled;', b'        let copied = input.to_vec();\n        let input = copied.as_slice();\n        let start = self.filled;', 'append_reset_reuses_exact_pointer_and_never_allocates', 'fixed append/decode/reset must not allocate'),
            ('early-credit', b'struct Core {\n    buffer:', b'struct Core {\n    _ownership: Bytes,\n    buffer:', 'constructor_exact_backing_and_last_public_handle_physical_exit', 'original credit exited before physical encoded Vec/Core deallocation'),
        ]
        for name, needle, changed, test, oracle in mutants:
            assert stable.count(needle) == 1
            mutant = stable.replace(needle, changed)
            if name == 'early-credit':
                needle2 = b'    // Core\'s Arc allocation and Vec exit before this original owner.\n    _ownership: Bytes,\n'
                assert mutant.count(needle2) == 1
                mutant = mutant.replace(needle2, b'')
            (root / (name + '.diff')).write_text(''.join(difflib.unified_diff(stable.decode().splitlines(True), mutant.decode().splitlines(True), fromfile='actual-source-plus-tests.rs', tofile=name + '.rs')))
            try:
                path.write_bytes(mutant)
                status, output = run(['cargo', 'test', *common, test, '--', '--test-threads=1', '--nocapture'], root, name, env)
                assert status == 101 and 'test result: FAILED. 0 passed; 1 failed;' in output and oracle in output and 'panicked at' in output
            finally:
                path.write_bytes(stable); assert path.read_bytes() == stable
        (root / 'restore-sha256.json').write_text(json.dumps({'before': hashlib.sha256(stable).hexdigest(), 'restored': hashlib.sha256(path.read_bytes()).hexdigest(), 'byte_exact': True}, indent=2) + '\n')
        if run(ordinary, root, 'restored', env)[0]: raise SystemExit('Restored probe failed')
    # This is a compile-fail API proof, distinct from runtime negative oracles.
    try:
        path.write_bytes(stable + b'\nfn forbidden_alias(lease: &mut BoundHeaderBlockBuffer) -> &[u8] { lease.decode(|input, _| input) }\n')
        status, output = run(['cargo', 'check', *common, '--lib'], root, 'slice-escape-compile-fail', env)
        assert status == 101 and 'lifetime may not live long enough' in output and 'forbidden_alias' in output
    finally:
        path.write_bytes(stable); assert path.read_bytes() == stable
    if args.quality:
        if run(['rustfmt', '--edition', '2021', '--check', str(evidence / 'probe-tests.rs')], root, 'probe-fmt', env)[0]: raise SystemExit('Probe formatting failed')
        if run(['cargo', 'clippy', *common, '--all-targets'], root, 'clippy', env)[0]: raise SystemExit('Clippy failed')
    if args.miri:
        installed = subprocess.check_output(['rustup', 'component', 'list', '--toolchain', 'nightly', '--installed'], text=True)
        assert 'miri-' in installed and 'rust-src' in installed
        if run(['cargo', '+nightly', 'miri', 'test', *common, '--', '--test-threads=1', '--nocapture'], root, 'miri', env)[0]: raise SystemExit('Miri failed')


if __name__ == '__main__':
    main()

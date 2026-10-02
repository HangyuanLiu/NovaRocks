#!/usr/bin/env python3
"""Run the complete actual HTTP/Bytes normal dependencies offline, optionally under Miri."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def hashes(repo):
    result = {}
    for package in ['http-1.4.0', 'bytes-1.11.0']:
        source = repo / 'vendor' / package
        for path in sorted(source.rglob('*.rs')) + [source / 'Cargo.toml']:
            result[package + '/' + path.relative_to(source).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--miri', action='store_true')
    args = parser.parse_args()
    evidence = Path(__file__).resolve().parent
    repo = args.repo.resolve(strict=True)
    expected = json.loads((evidence / 'source-sha256.json').read_text())
    if hashes(repo) != expected:
        raise SystemExit('Product source differs from the recorded snapshot')
    root = Path(tempfile.mkdtemp(prefix='m07-header-map-'))
    for package in ['http-1.4.0', 'bytes-1.11.0']:
        shutil.copytree(repo / 'vendor' / package, root / package)
    (root / 'src').mkdir()
    shutil.copyfile(evidence / 'driver.rs', root / 'src/lib.rs')
    shutil.copyfile(evidence / 'probe-Cargo.lock', root / 'Cargo.lock')
    (root / 'Cargo.toml').write_text('''[package]
name="m07-header-map-probe"
version="0.1.0"
edition="2021"
[workspace]
exclude=["http-1.4.0","bytes-1.11.0"]
[lib]
doctest=false
[dependencies]
http={path="http-1.4.0",version="=1.4.0"}
bytes="=1.11.0"
itoa="=1.0.15"
[patch.crates-io]
bytes={path="bytes-1.11.0"}
''')
    production = {(p['name'], p['version'], p.get('source'), p.get('checksum')) for p in tomllib.loads((repo / 'Cargo.lock').read_text())['package']}
    for package in tomllib.loads((root / 'Cargo.lock').read_text())['package']:
        if package['name'] == 'm07-header-map-probe':
            continue
        identity = (package['name'], package['version'], package.get('source'), package.get('checksum'))
        if identity not in production:
            raise SystemExit(f'Dependency is not production pinned: {identity}')
    print(f'Probe workspace: {root}', flush=True)
    commands = [['cargo', 'test', '--manifest-path', str(root / 'Cargo.toml'), '--offline', '--locked', '--lib']]
    if args.miri:
        commands.append(['cargo', '+nightly', 'miri', 'test', '--manifest-path', str(root / 'Cargo.toml'), '--offline', '--locked', '--lib'])
    for index, command in enumerate(commands):
        print('Command: ' + ' '.join(command), flush=True)
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=os.environ)
        (root / f'run-{index}.log').write_text(result.stdout)
        print(result.stdout, end='', flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()

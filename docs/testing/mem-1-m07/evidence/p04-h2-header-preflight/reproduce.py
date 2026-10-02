#!/usr/bin/env python3
"""Run actual private HPACK sources with focused allocator and raw-alias probes."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--miri', action='store_true')
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    target = Path(tempfile.mkdtemp(prefix='m07-h2-header-probe-'))
    source = repo / 'vendor/h2-0.4.12/src'
    (target / 'src/hpack').mkdir(parents=True)
    for name in ['decoder.rs', 'header.rs']:
        shutil.copyfile(source / 'hpack' / name, target / 'src/hpack' / name)
    shutil.copytree(source / 'hpack/huffman', target / 'src/hpack/huffman')
    shutil.copyfile(source / 'ext.rs', target / 'src/ext.rs')
    # Only the decoder's unused frame::Error conversion needs this neutral stub.
    # No production decoder code is rewritten; real frame/transport behavior is
    # covered separately by workspace protocol tests against the patched h2.
    (target / 'src/lib.rs').write_text('''#![allow(dead_code)]
mod ext;
mod frame { pub enum Error { Hpack(crate::hpack::DecoderError) } }
mod hpack {
    mod decoder;
    mod header;
    pub mod huffman;
    pub use decoder::{Decoder, DecoderError, NeedMore};
    pub use header::{BytesStr, Header};
}
#[cfg(test)]
mod probes;
''')
    shutil.copyfile(evidence / 'probe-tests.rs', target / 'src/probes.rs')
    (target / 'Cargo.toml').write_text(
        '[package]\nname="m07-h2-header-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[workspace]\n[dependencies]\n'
        + 'bytes={path=' + json.dumps(str(repo / 'vendor/bytes-1.11.0')) + '}\n'
        + 'http="=1.4.0"\ntracing={version="=0.1.44",default-features=false,features=["std"]}\n')
    lock = evidence / 'probe-Cargo.lock'
    if lock.exists():
        shutil.copyfile(lock, target / 'Cargo.lock')
    print(f'Probe workspace: {target}', flush=True)
    if not args.prepare_only:
        common = ['--manifest-path', str(target / 'Cargo.toml'), '--offline',
                  *(['--locked'] if lock.exists() else [])]
        subprocess.run(['cargo', 'test', *common, '--', '--test-threads=1'], check=True)
        if args.miri:
            subprocess.run(['cargo', '+nightly', 'miri', 'test', *common,
                            'probes::', '--', '--test-threads=1'], check=True)


if __name__ == '__main__':
    main()

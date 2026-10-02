#!/usr/bin/env python3
"""Snapshot actual normal h2/http/bytes dependencies; no network or repo writes.

Only a cfg-feature wrapper is appended to scratch stream_store.rs. The actual
bind/storage/Drop implementation is not replaced. Production lock identities
are verified before either normal or Miri execution. Default is prepare only.
"""
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

PIN = '181a8f337546a17490d2b1d69f6f48f793d62e2990c72c11d12f3d92e155a33f'
FEATURE = 'm07-stream-store-probe'
HELPER = r'''
#[cfg(feature = "m07-stream-store-probe")]
#[doc(hidden)]
#[derive(Debug)]
pub struct M07FixedStreamStore(FixedStreamStore);
#[cfg(feature = "m07-stream-store-probe")]
impl StreamStoreBuffer {
    /// Scratch-only direct call into the unchanged production bind method.
    #[doc(hidden)]
    pub fn m07_bind(&self) -> io::Result<M07FixedStreamStore> {
        self.bind().map(M07FixedStreamStore)
    }
}
#[cfg(feature = "m07-stream-store-probe")]
impl M07FixedStreamStore {
    /// Scratch-only observation of the actual fixed arrays.
    #[doc(hidden)]
    pub fn m07_empty_geometry(&self) -> (usize, usize, bool) {
        (self.0.slots.len(), self.0.waiters.len(),
            self.0.slots.iter().all(|slot| slot.value.is_none() && !slot.linked)
                && self.0.waiters.iter().all(|waiter| !waiter.leased && !waiter.notify && waiter.waker.is_none()))
    }
    /// Scratch-only installation of a concrete destructor-observed Waker.
    #[doc(hidden)]
    pub fn m07_install_waiter(&mut self, position: usize, waker: std::task::Waker) {
        assert!(self.0.waiters[position].waker.is_none());
        self.0.waiters[position].waker = Some(waker);
    }
}
'''


def identities(repo, lock):
    exact = {(p['name'], p['version'], p.get('source'), p.get('checksum'))
        for p in tomllib.loads((repo / 'Cargo.lock').read_text())['package']}
    for p in tomllib.loads(lock.read_text())['package']:
        if p['name'] == 'm07-stream-store-probe':
            continue
        identity = (p['name'], p['version'], p.get('source'), p.get('checksum'))
        if identity not in exact:
            raise SystemExit(f'Non-production dependency identity: {identity}')


def prepare(repo, probe):
    actual = repo / 'vendor/h2-0.4.12/src/proto/streams/stream_store.rs'
    if hashlib.sha256(actual.read_bytes()).hexdigest() != PIN:
        raise SystemExit('Actual stream_store source does not match frozen primitive pin')
    root = Path(tempfile.mkdtemp(prefix='m07-stream-store-miri-'))
    hashes = {}
    for name, upstream in [('h2-source', 'h2-0.4.12'), ('http-source', 'http-1.4.0'), ('bytes-source', 'bytes-1.11.0')]:
        shutil.copytree(repo / 'vendor' / upstream, root / name)
        for path in sorted((root / name).rglob('*.rs')):
            hashes[name + '/' + path.relative_to(root / name).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
        hashes[name + '/Cargo.toml'] = hashlib.sha256((root / name / 'Cargo.toml').read_bytes()).hexdigest()
    (root / 'source-sha256.json').write_text(json.dumps(hashes, indent=2) + '\n')
    path = root / 'h2-source/src/proto/streams/stream_store.rs'
    original = path.read_text()
    changed = original + '\n' + HELPER
    path.write_text(changed)
    (root / 'stream-store-helper.diff').write_text(''.join(difflib.unified_diff(
        original.splitlines(True), changed.splitlines(True), fromfile='actual/stream_store.rs', tofile='scratch/stream_store.rs')))
    source_manifest = root / 'h2-source/Cargo.toml'
    manifest = source_manifest.read_text()
    assert manifest.count('[features]\n') == 1
    source_manifest.write_text(manifest.replace('[features]\n', '[features]\n' + FEATURE + ' = []\n'))
    (root / 'src').mkdir()
    shutil.copyfile(probe, root / 'src/lib.rs')
    manifest = '[package]\nname="m07-stream-store-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\nexclude=["h2-source","http-source","bytes-source"]\n[lib]\ndoctest=false\n[dependencies]\n'
    manifest += 'h2={path=' + json.dumps(str(root / 'h2-source')) + ',version="=0.4.12",features=["' + FEATURE + '"]}\n'
    manifest += 'bytes="=1.11.0"\ntokio={version="=1.52.3",default-features=false,features=["io-util"]}\n'
    for name, version in {'atomic-waker':'1.1.2', 'fnv':'1.0.7', 'futures-core':'0.3.32', 'futures-sink':'0.3.32', 'http':'1.4.0', 'indexmap':'2.12.1', 'slab':'0.4.11', 'pin-project-lite':'0.2.16', 'itoa':'1.0.15', 'tracing-core':'0.1.35'}.items():
        manifest += f'{name}={{version="={version}",default-features=false}}\n'
    manifest += 'tokio-util={version="=0.7.17",default-features=false,features=["codec","io"]}\ntracing={version="=0.1.43",default-features=false,features=["std"]}\n'
    manifest += '[patch.crates-io]\nbytes={path=' + json.dumps(str(root / 'bytes-source')) + '}\nhttp={path=' + json.dumps(str(root / 'http-source')) + '}\n'
    (root / 'Cargo.toml').write_text(manifest)
    lock = (repo / 'docs/testing/mem-1-m07/evidence/p04-shared-header-field/probe-Cargo.lock').read_text()
    assert lock.count('name = "m07-shared-header-probe"') == 1
    (root / 'Cargo.lock').write_text(lock.replace('name = "m07-shared-header-probe"', 'name = "m07-stream-store-probe"'))
    return root


def run(command, root, name):
    print('Command: ' + ' '.join(command), flush=True)
    env = dict(os.environ, CARGO_NET_OFFLINE='true')
    with (root / (name + '.log')).open('w') as log:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env) as child:
            for line in child.stdout:
                log.write(line); log.flush(); print(line, end='', flush=True)
            status = child.wait()
    print(f'{name} exit: {status}', flush=True)
    if status:
        raise SystemExit(status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--probe', type=Path, default=Path(__file__).with_name('probe.rs'))
    parser.add_argument('--reuse', type=Path)
    parser.add_argument('--ordinary', action='store_true')
    parser.add_argument('--miri', action='store_true')
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    root = args.reuse.resolve(strict=True) if args.reuse else prepare(repo, args.probe.resolve(strict=True))
    identities(repo, root / 'Cargo.lock')
    print('Probe workspace: ' + str(root), flush=True)
    print('Actual stream-store SHA256: ' + PIN, flush=True)
    common = ['--manifest-path', str(root / 'Cargo.toml'), '--target-dir', str(root / 'target'), '--offline', '--locked']
    print('Ordinary: cargo test ' + ' '.join(common) + ' -- --test-threads=1 --nocapture', flush=True)
    print('Miri: cargo +nightly miri test ' + ' '.join(common) + ' -- --test-threads=1 --nocapture', flush=True)
    if args.ordinary:
        run(['cargo', 'test', *common, '--', '--test-threads=1', '--nocapture'], root, 'ordinary')
    if args.miri:
        installed = subprocess.check_output(['rustup', 'component', 'list', '--toolchain', 'nightly', '--installed'], text=True)
        if not any(line.startswith('miri-') for line in installed.splitlines()):
            raise SystemExit('Nightly Miri is unavailable; no installation attempted')
        run(['cargo', '+nightly', 'miri', 'test', *common, '--', '--test-threads=1', '--nocapture'], root, 'miri')


if __name__ == '__main__':
    main()

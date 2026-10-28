# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Archive original attempt files and verify them outside the disposable worker.

CI maintainers own this low-cost functional asset. It never interprets Rust
assertions, retries workloads, or turns archive completeness into test success.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tarfile
import tempfile
import urllib.request

from .ports import MANIFEST_SCHEMA


def _digest_file(path: Path) -> str:
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def _safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and '..' not in path.parts and str(path) == name


class _HashingReader:
    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.source.read(size)
        self.digest.update(data)
        return data


def _files(root: Path, prefix: str):
    if root.is_symlink():
        raise ValueError(f'archive input is a symbolic link: {root}')
    if root.is_file():
        yield root, prefix
        return
    if not root.is_dir():
        raise FileNotFoundError(root)
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError(f'archive input contains a symbolic link: {path}')
        if path.is_file():
            yield path, f'{prefix}/{path.relative_to(root).as_posix()}'
        elif not path.is_dir():
            raise ValueError(f'archive input is not a regular file: {path}')


def pack(attempt_root: str | Path, diagnostics: dict[str, str | Path] | None = None,
         *, kind: str = 'final', output: str | Path | None = None) -> Path:
    """Write one archive exclusively. Hash exactly the bytes placed into tar.

    Diagnostics are explicit non-secret paths. Callers must never pass JIT,
    instance-role material or bootstrap files containing presigned URLs.
    """
    if kind not in ('final', 'interrupted'):
        raise ValueError('archive kind must be final or interrupted')
    if Path(attempt_root).is_symlink():
        raise ValueError('archive input is a symbolic link')
    attempt = Path(attempt_root).resolve(strict=True)
    if output is None:
        directory = Path(tempfile.mkdtemp(prefix='novarocks-ci-handoff-'))
        destination = directory / f'{kind}.tar.gz'
    else:
        destination = Path(output).resolve()
    if destination.is_relative_to(attempt):
        raise ValueError('archive output must be outside the attempt input')
    inputs = list(_files(attempt, 'attempt'))
    for name, path in sorted((diagnostics or {}).items()):
        if not _safe_name(name):
            raise ValueError('invalid diagnostic archive name')
        inputs.extend(_files(Path(path), f'diagnostics/{name}'))
    names = [name for _, name in inputs]
    if len(names) != len(set(names)):
        raise ValueError('archive members are not unique')
    manifest = {'schema': MANIFEST_SCHEMA, 'kind': kind, 'complete': kind == 'final', 'members': []}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('xb') as raw:
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w|') as archive:
                for path, name in inputs:
                    with path.open('rb') as source:
                        size = source.seek(0, 2)
                        source.seek(0)
                        reader = _HashingReader(source)
                        info = tarfile.TarInfo(name)
                        info.size = size
                        info.mode = 0o600
                        archive.addfile(info, reader)
                    manifest['members'].append({'path': name, 'size': size, 'sha256': reader.digest.hexdigest()})
                data = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()
                info = tarfile.TarInfo('MANIFEST.json')
                info.size = len(data)
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(data))
    return destination


def verify(source: bytes | str | Path) -> dict:
    """Validate all member bytes and the gzip trailer without extracting files."""
    fact = {'present': True, 'kind': None, 'complete': False, 'manifest_ok': False,
            'missing_members': [], 'errors': [], 'archive_sha256': None}
    if not isinstance(source, bytes) and not Path(source).exists():
        return dict(fact, present=False, errors=['archive absent'])
    try:
        with ExitStack() as stack:
            raw = io.BytesIO(source) if isinstance(source, bytes) else stack.enter_context(Path(source).open('rb'))
            fact['archive_sha256'] = hashlib.file_digest(raw, 'sha256').hexdigest()
            raw.seek(0)
            compressed = stack.enter_context(gzip.GzipFile(fileobj=raw, mode='rb'))
            archive = stack.enter_context(tarfile.open(fileobj=compressed, mode='r:'))
            actual, manifest = {}, None
            for member in archive:
                if not member.isfile() or not _safe_name(member.name):
                    raise ValueError('archive contains an unsafe or non-file member')
                if member.name in actual or (member.name == 'MANIFEST.json' and manifest is not None):
                    raise ValueError('duplicate archive member')
                extracted = archive.extractfile(member)
                if member.name == 'MANIFEST.json':
                    manifest = json.load(extracted)
                else:
                    actual[member.name] = {'path': member.name, 'size': member.size,
                                           'sha256': hashlib.file_digest(extracted, 'sha256').hexdigest()}
            # Tar EOF can precede the gzip checksum; consume it before accepting.
            while compressed.read(1024 * 1024):
                pass
            if not isinstance(manifest, dict) or manifest.get('schema') != MANIFEST_SCHEMA:
                raise ValueError('missing or unsupported archive manifest')
            if manifest.get('kind') not in ('final', 'interrupted') or type(manifest.get('complete')) is not bool:
                raise ValueError('invalid archive manifest facts')
            fact.update(kind=manifest['kind'], complete=manifest['complete'])
            declared = manifest.get('members')
            if not isinstance(declared, list) or any(not isinstance(row, dict) for row in declared):
                raise ValueError('invalid member manifest')
            expected = {row.get('path'): row for row in declared}
            if len(expected) != len(declared) or any(not isinstance(name, str) or not _safe_name(name) for name in expected):
                raise ValueError('invalid or duplicate manifest path')
            fact['missing_members'] = sorted(set(expected) - set(actual))
            if fact['missing_members']:
                fact['errors'].append('manifest members are absent')
            if set(actual) - set(expected):
                fact['errors'].append('archive has unmanifested members')
            for name in set(expected) & set(actual):
                if expected[name] != actual[name]:
                    fact['errors'].append(f'member size or SHA-256 differs: {name}')
            fact['manifest_ok'] = not fact['errors']
    except (OSError, EOFError, tarfile.TarError, ValueError, KeyError, TypeError) as error:
        fact['errors'].append(str(error))
    return fact


def upload(archive: str | Path, url: str, *, timeout: float = 60) -> dict:
    path = Path(archive)
    digest = _digest_file(path)
    # Empty Content-Type preserves the presigner's empty canonical header and
    # prevents urllib from inserting application/x-www-form-urlencoded.
    with path.open('rb') as source:
        request = urllib.request.Request(url, data=source, method='PUT',
            headers={'Content-Length': str(path.stat().st_size), 'Content-Type': ''})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status not in (200, 201, 204):
                raise OSError(f'archive upload returned HTTP {response.status}')
    return {'uploaded': True, 'kind': verify(path)['kind'], 'size': path.stat().st_size, 'sha256': digest}


def pack_upload(attempt_root, diagnostics=None, *, kind, url, output=None, timeout=60):
    archive = pack(attempt_root, diagnostics, kind=kind, output=output)
    return dict(upload(archive, url, timeout=timeout), archive=str(archive))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('pack-upload', 'verify'))
    parser.add_argument('--attempt', type=Path)
    parser.add_argument('--diagnostic', action='append', default=[], metavar='NAME=PATH')
    parser.add_argument('--kind', choices=('final', 'interrupted'), default='final')
    parser.add_argument('--bootstrap', type=Path, default=Path('/run/novarocks-ci/bootstrap.json'))
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'verify':
        if args.archive is None:
            parser.error('--archive is required for verify')
        result = verify(args.archive)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['manifest_ok'] else 1
    config = json.loads(args.bootstrap.read_text())
    diagnostics = dict(item.split('=', 1) for item in args.diagnostic)
    result = pack_upload(args.attempt or config['attempt_root'], diagnostics,
        kind=args.kind, url=config['upload_urls'][args.kind], output=args.archive)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

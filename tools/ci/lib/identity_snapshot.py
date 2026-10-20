#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Execution input identities. Only declared inputs enter reusable scopes."""
import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import uuid


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    try:
        with temporary.open('x', encoding='utf-8') as stream:
            os.chmod(temporary, 0o600)
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def tools():
    result = {}
    for name, argv in [('rustc', ['rustc', '-vV']), ('cargo', ['cargo', '--version']), ('python', ['python3', '--version'])]:
        try:
            process = subprocess.run(argv, capture_output=True, text=True, check=False)
            result[name] = {'output': process.stdout.strip(), 'exit': process.returncode}
        except OSError as error:
            result[name] = {'error': str(error)}
    return result


def source_snapshot(repo):
    repo = Path(repo).resolve()
    def git(*argv, environment=None):
        return subprocess.check_output(['git'] + list(argv), cwd=repo, env=dict(os.environ, GIT_OPTIONAL_LOCKS='0') if environment is None else environment, text=True, stderr=subprocess.DEVNULL).strip()
    with tempfile.TemporaryDirectory(prefix='novarocks-source-index-') as root:
        environment = dict(os.environ, GIT_INDEX_FILE=str(Path(root) / 'index'))
        git('read-tree', 'HEAD', environment=environment)
        git('add', '-A', '--', '.', environment=environment)
        tree = git('write-tree', environment=environment)
    return {'schema': 1, 'source': {'commit': git('rev-parse', 'HEAD'), 'dirty': bool(git('status', '--porcelain')), 'worktree_tree': tree},
            'toolchain': tools(), 'platform': {'system': platform.system(), 'machine': platform.machine(), 'release': platform.release()}}


def file_inputs(repo, patterns):
    result = {}
    repo = Path(repo).resolve()
    for pattern in patterns:
        if Path(pattern).is_absolute() or '..' in Path(pattern).parts:
            raise ValueError('runtime input must be repository relative: ' + pattern)
        matches = sorted(Path(path) for path in glob.glob(str(repo / pattern), recursive=True) if Path(path).is_file())
        result['glob:' + pattern] = [str(path.relative_to(repo)) for path in matches]
        for path in matches:
            result[str(path.relative_to(repo))] = digest(path)
    return result


def env_inputs(declarations, environment, values_only=False):
    result = {}
    for item in declarations:
        name, kind = item['name'], item['kind']
        if kind not in ('value', 'file-content'):
            raise ValueError('unknown environment input kind: ' + kind)
        if values_only and kind == 'file-content':
            continue
        if name not in environment:
            result[name] = 'unset'
        elif kind == 'value':
            result[name] = hashlib.sha256(environment[name].encode()).hexdigest()
        else:
            result[name] = digest(environment[name])
    return result


def rust_snapshot(repo, inputs, environment, domain):
    inputs = Path(inputs)
    metadata = json.loads((inputs / 'cargo-metadata.json').read_text())
    packages = {p['id']: p for p in metadata['packages'] if p['id'] in metadata.get('workspace_members', [p['id'] for p in metadata['packages']])}
    declarations = {key: (p.get('metadata') or {}).get('novarocks', {}).get('test', {}) for key, p in packages.items()}
    common = {'toolchain': tools(), 'platform': {'system': platform.system(), 'machine': platform.machine(), 'release': platform.release()}}
    if domain == 'doctest':
        declared = all('runtime-inputs' in d and 'env-inputs' in d for d in declarations.values())
        env = {}
        for declaration in declarations.values():
            env.update(env_inputs(declaration.get('env-inputs', []), environment, values_only=True))
        payload = dict(common, source=source_snapshot(repo)['source'], cargo_profile='dev', env=env)
        return {'workspace': {'declared': declared, 'inputs': payload}}
    prepared = json.loads((inputs / 'prepared.json').read_text())
    bindings = json.loads((inputs / 'test-artifacts.json').read_text())['binaries']
    selected = json.loads((inputs / 'selected-tests.json').read_text())['rust-suites']
    result = {}
    for binary_id, suite in selected.items():
        matched = [name for name, case in suite['testcases'].items() if case.get('filter-match', {}).get('status') == 'matches']
        if not matched:
            continue
        binary = bindings[binary_id]
        declaration = declarations.get(binary['package-id'], {})
        payload = dict(common, executable_sha256=digest(binary['path']), nextest_profile=prepared['profile'],
                       nextest_config_sha256=digest(inputs / 'nextest.toml'), nextest_version=prepared.get('nextest_version'),
                       runtime_inputs=file_inputs(repo, declaration.get('runtime-inputs', [])),
                       env=env_inputs(declaration.get('env-inputs', []), environment))
        declared = 'runtime-inputs' in declaration and 'env-inputs' in declaration
        for name in matched:
            result[binary_id + '::' + name] = {'declared': declared, 'inputs': payload}
    return result


def begin(path, domain, scopes):
    record = {'schema': 1, 'domain': domain, 'scopes': {key: {'declared': value['declared'], 'inputs_start': value['inputs'], 'inputs_end': None, 'inputs_stable': False} for key, value in scopes.items()}}
    atomic_json(path, record)
    return record


def confirm(path, record, scopes):
    for key, scope in record['scopes'].items():
        value = scopes.get(key)
        scope['inputs_end'] = value['inputs'] if value else None
        scope['inputs_stable'] = bool(value and scope['inputs_start'] == value['inputs'] and scope['declared'] == value['declared'])
    atomic_json(path, record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['source'])
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    atomic_json(options.output, source_snapshot(options.repo))


if __name__ == '__main__':
    main()

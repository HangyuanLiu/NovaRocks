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
"""Generate the SQL routing table from native suite declarations, without TOML parsing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
START = '<!-- sql-suite-map:start -->'
END = '<!-- sql-suite-map:end -->'


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def declarations(options):
    if options.declarations:
        return json.loads(options.declarations.read_text())
    runner = options.runner
    if runner is None:
        receipt_path = options.receipt or os.environ.get('NOVA_CI_ARTIFACT_RECEIPT')
        if not receipt_path:
            raise ValueError('set NOVA_CI_ARTIFACT_RECEIPT to the joint preparation receipt, or provide --runner')
        binding = json.loads(Path(receipt_path).read_text())['novarocks-sql-test']
        runner = Path(binding['path'])
        if digest(runner) != binding['sha256']:
            raise ValueError('prepared SQL runner changed; refresh the joint preparation receipt')
    result = subprocess.run([str(runner), '--list-declarations'], cwd=REPO,
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def escape(text):
    return str(text).replace('|', '\\|').replace('\n', ' ')


def render(document):
    if document.get('schema') != 1 or not isinstance(document.get('suites'), list):
        raise ValueError('unsupported native SQL declaration schema')
    rows = ['| Suite | Impact paths | Tier and prerequisites | Reuse scope and initial state |',
            '|---|---|---|---|']
    seen = set()
    for suite in sorted(document['suites'], key=lambda value: value['name']):
        name = suite['name']
        if name in seen:
            raise ValueError('duplicate native SQL suite: ' + name)
        seen.add(name)
        paths = suite['impact']['paths']
        if not paths or any(not isinstance(path, str) or not path for path in paths):
            raise ValueError('SQL suite has no impact paths: ' + name)
        requirements = [suite['tier']]
        heavy = suite.get('heavy')
        if suite['explicit_only'] and not heavy:
            raise ValueError('explicit SQL suite has no heavy declaration: ' + name)
        if heavy:
            requirements.append(heavy['reason'] + ': ' + heavy['detail'])
        if suite.get('disabled_reason'):
            requirements.append('DISABLED: ' + suite['disabled_reason'])
        reuse = suite['reuse']
        initial = reuse.get('initial_state')
        state = reuse['scope'] + '; ' + (initial if initial else 'undeclared')
        rows.append('| `{}` | {} | {} | {} |'.format(escape(name),
            '<br>'.join('`' + escape(path) + '`' for path in paths),
            '<br>'.join(escape(value) for value in requirements), escape(state)))
    return START + '\n' + '\n'.join(rows) + '\n' + END


def update(text, table):
    if START in text or END in text:
        if text.count(START) != 1 or text.count(END) != 1:
            raise ValueError('SQL suite-map markers must occur exactly once')
        a, b = text.index(START), text.index(END) + len(END)
        if text.index(START) >= text.index(END):
            raise ValueError('SQL suite-map markers are out of order')
    else:
        a = text.index('| Suite |')
        b = text.index('\n\n', a)
    return text[:a] + table + text[b:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--runner', type=Path)
    parser.add_argument('--receipt', type=Path)
    parser.add_argument('--declarations', type=Path, help='Read an explicitly saved native declaration document')
    parser.add_argument('--readme', type=Path, default=REPO / 'tests/sql/correctness/README.md')
    options = parser.parse_args()
    existing = options.readme.read_text()
    expected = update(existing, render(declarations(options)))
    if options.check:
        if existing != expected:
            raise ValueError('SQL suite map is stale; rerun tools/ci/sql-suite-map.py with the same native runner')
        print('SQL suite map: current')
    else:
        options.readme.write_text(expected)
        print('SQL suite map updated: ' + str(options.readme))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        sys.exit('error: ' + str(error))

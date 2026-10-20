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
"""Durable invocation boundaries and producer-owned cleanup facts."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import subprocess
import sys

from identity_snapshot import atomic_json, digest, env_inputs, file_inputs, source_snapshot, tools


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def execution_platform():
    return {'system': platform.system(), 'machine': platform.machine(), 'release': platform.release()}


def snapshot(repo, spec, environment):
    payload = {'env': env_inputs(spec.get('env_inputs', []), environment), 'toolchain': tools(),
               'platform': execution_platform()}
    if 'inputs' in spec:
        payload['files'] = file_inputs(repo, spec['inputs'])
    elif not spec.get('rust_batch'):
        try:
            payload['source'] = source_snapshot(repo)['source']
        except (OSError, subprocess.SubprocessError) as error:
            payload['source_error'] = str(error)
    payload['files_explicit'] = {key: digest(path) if Path(path).is_file() else None for key, path in spec.get('files', {}).items()}
    if 'execution_args' in spec:
        payload['execution_args'] = spec['execution_args']
    if 'declaration' in spec:
        payload['declaration'] = spec['declaration']
    return payload


def execution_spec(kind, argv, spec=None):
    spec = dict(spec or {})
    native_argv = spec.get('native_command', argv) if kind == 'stage' else argv
    if 'native_command' in spec and (not isinstance(native_argv, list) or not native_argv or any(not isinstance(arg, str) for arg in native_argv)):
        raise ValueError('native execution command must be a nonempty string argv')
    if kind == 'stage':
        outputs = spec.get('output_args', [])
        if not isinstance(outputs, list) or any(type(index) is not int or not 0 < index < len(native_argv) for index in outputs):
            raise ValueError('stage output argument indexes must identify non-executable argv values')
        input_args = spec.get('input_args', [])
        if not isinstance(input_args, list) or any(type(index) is not int or not 0 < index < len(native_argv) or index in outputs for index in input_args):
            raise ValueError('stage input argument indexes must identify non-output argv values')
        inputs = command_inputs([arg for index, arg in enumerate(native_argv) if index not in outputs])
        inputs.update({'input:%d' % index: native_argv[index] for index in input_args})
        runtime = shutil.which(native_argv[0]) if native_argv else None
        if runtime:
            inputs['runtime_executor'] = runtime
        spec['files'] = dict(spec.get('files', {}), **inputs)
        spec['execution_args'] = stage_execution_args(native_argv, spec['files'], outputs)
    else:
        spec['files'] = dict(spec.get('files', {}), **command_inputs(native_argv))
    if kind == 'system-scenario':
        spec['execution_args'] = system_execution_args(argv)
    return spec


def stage_execution_args(argv, files, output_args):
    outputs = set(output_args)
    for index, arg in enumerate(argv[:-1]):
        if arg in ('--output', '--artifact-root', '--results-file', '--junit'):
            outputs.add(index + 1)
    file_values = {str(value): '@file:' + key for key, value in files.items()}
    result = []
    for index, arg in enumerate(argv):
        if index in outputs:
            result.append('@output')
        elif arg.startswith(('--output=', '--artifact-root=', '--results-file=', '--junit=')):
            result.append(arg.split('=', 1)[0] + '=@output')
        elif index == 0 and 'runtime_executor' in files:
            result.append('@file:runtime_executor')
        else:
            result.append(file_values.get(arg, arg))
    return result


def stage_descriptor(name, argv, output_args=None, input_args=None):
    return {'schema': 1, 'name': name, 'argv': argv,
            'spec': execution_spec('stage', argv, {'env_inputs': [], 'native_command': argv,
                                                  'output_args': output_args or [], 'input_args': input_args or []})}


def stage_argument_indexes(name):
    value = os.environ.get(name, '')
    return [int(index) for index in value.split(',')] if value else []


def start(root, repo, kind, name, units, argv, spec=None, environment=None):
    root, repo = Path(root), Path(repo)
    environment = effective_environment(argv, os.environ if environment is None else environment)
    spec = execution_spec(kind, argv, spec)
    directory = root / 'invocations'
    directory.mkdir(parents=True, exist_ok=True)
    (directory / '.reservations').mkdir(exist_ok=True)
    # Exclusive reservation also serializes callers selecting the same next sequence.
    for seq in range(1, 1000000):
        reservation = directory / '.reservations' / ('%04d' % seq)
        try:
            descriptor = os.open(str(reservation), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
            break
        except FileExistsError:
            continue
    else:
        raise ValueError('invocation sequence exhausted')
    slug = re.sub('[^a-zA-Z0-9_.-]+', '-', name).strip('-') or 'invocation'
    path = directory / ('%04d-%s.started.json' % (seq, slug))
    payload = snapshot(repo, spec, environment)
    record = {'schema': 1, 'seq': seq, 'kind': kind, 'name': name, 'units': units, 'argv': argv,
              'cwd': str(repo.resolve()), 'started_at': now(), 'inputs_start': payload, 'env': payload['env'], 'inputs_declared':spec.get('declared',True)}
    atomic_json(directory / '.inputs' / path.name, spec)
    atomic_json(path, record)
    return path


def system_execution_args(argv):
    excluded = {'--artifact-root', '--binary', '--compatible-binary', '--other-island-binary', '--config'}
    result = []
    index = 1
    while index < len(argv):
        name = argv[index].split('=', 1)[0]
        if name in excluded:
            index += 1 if '=' in argv[index] else 2
        else:
            result.append(argv[index])
            index += 1
    return result


def monitor_cleanup(path):
    path = Path(path)
    try:
        raw = path.read_bytes()
        if not raw.endswith(b'\n'):
            return 'unknown'
        try:
            document = json.loads(raw)
        except ValueError:
            document = None
        values = [document] if isinstance(document, dict) else [json.loads(line) for line in raw.splitlines()]
    except (OSError, ValueError):
        return 'unknown'
    if any(not isinstance(value, dict) for value in values):
        return 'unknown'
    if len(values) == 1 and 'ok' in values[0]:
        value = values[0]
        if value.get('schema') != 1:
            return 'unknown'
        if any(item.get('violations') for item in value.get('records', [])):
            return 'violation'
        if value.get('errors') or value.get('late_registrations'):
            return 'unknown'
        return 'ok' if value.get('ok') is True and value.get('complete') is True and value.get('registration_closed') is True else 'unknown'
    if not values or values[0].get('event') != 'Pending' or any(v.get('schema') != 1 for v in values):
        return 'unknown'
    final = values[-1]
    required = {'status_raw', 'startup_error', 'owner_lost', 'errors', 'cleanup_complete'}
    if final.get('event') != 'Final' or not required.issubset(final):
        return 'unknown'
    if not isinstance(final['errors'], list) or not isinstance(final['cleanup_complete'], bool):
        return 'unknown'
    if final['errors'] or final['cleanup_complete'] is False or any(v.get('event') == 'CleanupIncomplete' for v in values):
        return 'violation'
    if path.with_name('caller-drop-error.txt').exists():
        return 'violation'
    return 'ok'


def cleanup_facts(kind, materials):
    refs, statuses = [], []
    def add(status):
        statuses.append(status)
    def resource(items):
        for item in items:
            if item.get('registration'):
                refs.append(item['registration'])
            add('violation' if item.get('state') == 'cleanup_failed' else 'unknown' if item.get('state') != 'released' else 'ok')
    def monitor(path):
        refs.append(str(path))
        return monitor_cleanup(path)
    if kind == 'system-scenario':
        teardown = materials.get('teardown')
        refs += [str(teardown)] if teardown else []
        try:
            value = json.loads(Path(teardown).read_text())
            fixture, cluster = value['fixture'], value['cluster']
            add('violation' if fixture['teardown']['status'] == 'failed' else 'ok')
            resource(fixture.get('resources', []))
            add('violation' if cluster['shutdown'] == 'failed' else 'ok')
            if cluster.get('partial_cleanup') == 'monitor':
                paths = materials.get('monitors', [])
                add('unknown' if not paths else 'ok')
                for path in paths:
                    add(monitor(path))
        except (OSError, TypeError, ValueError, KeyError):
            add('unknown')
    elif kind == 'sql-suite':
        path = materials.get('sql_results')
        refs += [str(path)] if path else []
        try:
            records = [json.loads(line) for line in Path(path).read_text().splitlines()]
            fixtures = [r for r in records if r.get('kind') == 'fixture']
            servers = [r for r in records if r.get('kind') == 'server']
            if not fixtures or (any(r.get('kind') == 'selection' for r in records) and not servers):
                add('unknown')
            for value in fixtures:
                add('violation' if value['shutdown']['status'] == 'failed' else 'ok')
                resource(value.get('resources', []))
            for value in servers:
                add('violation' if value['cleanup']['status'] == 'failed' else 'ok')
        except (OSError, TypeError, ValueError, KeyError):
            add('unknown')
    else:
        return {'source': 'none', 'refs': [], 'summary': 'ok'}
    summary = 'violation' if 'violation' in statuses else 'unknown' if 'unknown' in statuses else 'ok'
    return {'source': 'producer', 'refs': refs, 'summary': summary}


def finish(path, code, log=None, materials=None, binaries=None, environment=None):
    path = Path(path)
    record = json.loads(path.read_text())
    spec = json.loads((path.parent / '.inputs' / path.name).read_text())
    materials = materials or {}
    try:
        end = snapshot(record['cwd'], spec, effective_environment(record['argv'], os.environ if environment is None else environment))
    except (OSError, ValueError) as error:
        end = {'error': str(error)}
    inferred = None
    if 129 <= code <= 192:
        try:
            inferred = signal.Signals(code - 128).name
        except ValueError:
            pass
    record.update(ended_at=now(), exit={'code': code, 'signal_inferred': inferred}, log=log,
                  materials=materials, binaries=binaries or {'primary': None, 'variants': []},
                  inputs_end=end, inputs_stable=record['inputs_start'] == end and not any(k.endswith('_error') for k in end),
                  cleanup=cleanup_facts(record['kind'], materials))
    destination = path.with_name(path.name.replace('.started.json', '.json'))
    atomic_json(destination, record)
    return destination


def command_inputs(argv):
    """Capture actual binary/config inputs, excluding attempt material paths."""
    files = {}
    for arg in argv:
        if Path(arg).is_file() and os.access(arg, os.X_OK):
            files['executor'] = arg
            break
    for index, arg in enumerate(argv[:-1]):
        if arg in ('--binary', '--compatible-binary', '--other-island-binary', '--config'):
            files[arg] = argv[index + 1]
        elif arg.startswith(('NOVAROCKS_BIN=', 'NOVA_ENV_REST_ENV_FILE=')):
            name, value = arg.split('=', 1)
            files[name] = value
    return files


def effective_environment(argv, environment):
    effective = dict(environment)
    if argv and argv[0] == 'env':
        for arg in argv[1:]:
            if '=' not in arg or arg.startswith('-'):
                break
            name, value = arg.split('=', 1)
            effective[name] = value
    return effective


def sql_selection(repo, name, argv, files):
    result = subprocess.run([files['executor'], '--list-declarations'], cwd=repo, capture_output=True, text=True, check=True)
    document = json.loads(result.stdout)
    suites = [value for value in document['suites'] if value['name'] == name]
    if len(suites) != 1:
        raise ValueError('SQL declaration must identify one selected suite')
    suite = suites[0]
    def option(name, default=None):
        return argv[argv.index(name)+1] if name in argv else default
    only = set(filter(None, option('--only', '').split(',')))
    skip = set(filter(None, option('--skip', '').split(',')))
    cases = [case['case'] for case in suite['cases'] if (not only or case['case'] in only) and case['case'] not in skip]
    if '--limit' in argv:
        cases = cases[:int(option('--limit'))]
    reuse = suite['reuse']
    spec = {'inputs':[], 'env_inputs':reuse.get('env_inputs') or [], 'declared':reuse.get('env_inputs') is not None}
    return ['sql-case:'+name+'/'+case for case in cases], spec


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    begin = sub.add_parser('start')
    begin.add_argument('--root', required=True)
    begin.add_argument('--repo', required=True)
    begin.add_argument('--kind', required=True, choices=['stage', 'sql-suite', 'system-scenario'])
    begin.add_argument('--name', required=True)
    begin.add_argument('--unit', action='append', default=[])
    begin.add_argument('--spec')
    begin.add_argument('argv', nargs=argparse.REMAINDER)
    describe = sub.add_parser('describe-stage')
    describe.add_argument('--name', required=True)
    describe.add_argument('argv', nargs=argparse.REMAINDER)
    end = sub.add_parser('finish')
    end.add_argument('--record', required=True)
    end.add_argument('--code', required=True, type=int)
    end.add_argument('--log')
    end.add_argument('--materials', default='{}')
    options = parser.parse_args()
    if options.action == 'describe-stage':
        argv = options.argv[1:] if options.argv[:1] == ['--'] else options.argv
        print(json.dumps(stage_descriptor(options.name, argv, stage_argument_indexes('CI_STAGE_OUTPUT_ARGS'), stage_argument_indexes('CI_STAGE_INPUT_ARGS'))))
        return
    if options.action == 'start':
        argv = options.argv[1:] if options.argv[:1] == ['--'] else options.argv
        spec = json.loads(Path(options.spec).read_text()) if options.spec else {}
        files = command_inputs(argv)
        if options.kind == 'stage':
            spec.setdefault('output_args', stage_argument_indexes('CI_STAGE_OUTPUT_ARGS'))
            spec.setdefault('input_args', stage_argument_indexes('CI_STAGE_INPUT_ARGS'))
        if options.kind == 'sql-suite':
            options.unit, sql_spec = sql_selection(options.repo, options.name, argv, files)
            spec.update(sql_spec)
        if options.kind != 'stage' or 'native_command' not in spec:
            spec.setdefault('files', {}).update(files)
        print(start(options.root, options.repo, options.kind, options.name, options.unit, argv, spec))
    else:
        path = Path(options.record)
        start_record = json.loads(path.read_text())
        files = command_inputs(start_record['argv'])
        primary = files.get('--binary') or files.get('NOVAROCKS_BIN')
        def binary(path):
            return {'path': path, 'sha256': next((value for key,value in start_record['inputs_start']['files_explicit'].items() if files.get(key)==path),None)}
        variants = [dict(binary(path), role=role.removeprefix('--')) for role, path in files.items() if role in ('--compatible-binary', '--other-island-binary')]
        print(finish(path, options.code, options.log, json.loads(options.materials), {'primary': binary(primary) if primary else None, 'variants': variants}))


if __name__ == '__main__':
    main()

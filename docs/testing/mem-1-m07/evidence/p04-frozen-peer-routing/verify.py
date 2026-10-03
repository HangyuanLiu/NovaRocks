#!/usr/bin/env python3
"""Recheck the pinned peer-routing slice and its repository dependency fences."""
from pathlib import Path
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
receipt = Path(__file__).parent
for filename, base in [('product-sha256.json', repo), ('vendor-source-sha256.json', repo / 'vendor')]:
    for relative, digest in json.loads((receipt / filename).read_text()).items():
        assert hashlib.sha256((base / relative).read_bytes()).hexdigest() == digest, relative
output = Path(tempfile.mkdtemp(prefix='m07-peer-routing-verify-'))
commands = []
for label, package, filter_ in [
    ('frontend', 'novarocks-frontend-application', 'runtime_filter::compiler::tests'),
    ('worker', 'novarocks-worker', 'runtime_filter::domain::routing::tests'),
    ('install', 'novarocks-native-adapter', 'runtime_filter_install::tests'),
    ('client', 'novarocks-native-adapter', 'native_client::'),
    ('exchange', 'novarocks-native-adapter', 'exchange_transmitter::tests'),
]:
    commands.append((label, ['cargo', 'test', '--locked', '--offline', '-p', package,
                            '--lib', filter_, '--', '--test-threads=1']))
for label, script in [
    ('wire-fence', 'tools/ci/tests/native-wire-dependency-boundary-test.sh'),
    ('physical-fence', 'tools/ci/tests/physical-plan-dependency-boundary-test.sh'),
    ('local-fence', 'tools/ci/tests/local-program-dependency-boundary-test.py'),
    ('statistics-fence', 'tools/ci/tests/ncp8-statistics-boundary-test.sh'),
]:
    commands.append((label, (['python3'] if script.endswith('.py') else []) + [script]))
commands.append(('fmt', ['cargo', 'fmt', '--all', '--', '--check']))
for label, command in commands:
    with (output / (label + '.log')).open('w') as log:
        log.write('Command: ' + ' '.join(command) + '\n')
        log.flush()
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    print(label, result.returncode, flush=True)
    if result.returncode:
        print('evidence directory', output, flush=True)
        raise SystemExit(result.returncode)
print('evidence directory', output, flush=True)

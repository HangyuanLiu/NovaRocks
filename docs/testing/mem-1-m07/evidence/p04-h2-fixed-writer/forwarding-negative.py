#!/usr/bin/env python3
"""Run only by the root agent after all shared Cargo/source work is idle.

Mutates one pinned forwarding seam at a time; restores the original exact
bytes before checking failure evidence. This is an actual production-lock
protocol test, not an isolated fake credit or reconstructed Hyper test.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PREFIX = Path('/tmp/m07-hyper-fixed-write-forwarding')
def sha(data):
    return hashlib.sha256(data).hexdigest()
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--side', choices=['client', 'server', 'reserve', 'both', 'all'], default='all')
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--prefix', type=Path, default=PREFIX)
    args = parser.parse_args()
    global_prefix = args.prefix
    prepared = json.loads((Path(__file__).resolve().parent / 'forwarding-prepared.json').read_text())
    root = args.repo.resolve(strict=True)
    for test_path, expected in prepared['test_pins'].items():
        if sha((root/test_path).read_bytes()) != expected:
            raise RuntimeError('Pinned actual test source changed: ' + test_path)
    lock = root / 'Cargo.lock'
    if sha(lock.read_bytes()) != prepared['lock_sha256']:
        raise RuntimeError('Pinned production lock changed')
    sides = ['client', 'server', 'reserve'] if args.side == 'all' else (['client', 'server'] if args.side == 'both' else [args.side])
    results = []
    for side in sides:
        entry = prepared['entries'][side]
        path = root / entry['path']
        original = path.read_bytes()
        if sha(original) != entry['original_sha256']:
            raise RuntimeError('Pinned production forwarding source changed: ' + str(path))
        needle = bytes.fromhex(entry['needle_hex'])
        assert original.count(needle) == 1
        mutant = original.replace(needle, bytes.fromhex(entry.get('replacement_hex', '')), 1)
        assert sha(mutant) == entry['mutant_sha256']
        backup = Path(str(global_prefix) + '-' + side + '-original.rs')
        backup.write_bytes(original)
        log_path = Path(str(global_prefix) + '-' + side + '-negative.log')
        if log_path.exists():
            raise RuntimeError('Refusing to overwrite the first negative log: ' + str(log_path))
        command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test', entry['target'], '--offline', '--locked', entry['case'], '--', '--exact']
        path.write_bytes(mutant)
        try:
            run = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
            output = run.stdout.decode(errors='replace')
            log_path.write_text('COMMAND: ' + ' '.join(command) + '\nEXIT: ' + str(run.returncode) + '\n' + output)
        finally:
            if path.read_bytes() != mutant:
                raise RuntimeError('Concurrent source change detected; refusing to overwrite it: ' + str(path))
            path.write_bytes(original)
            if path.read_bytes() != original:
                raise RuntimeError('Byte-exact source restoration failed')
        if sha(lock.read_bytes()) != prepared['lock_sha256']:
            raise RuntimeError('Production lock changed during negative execution')
        passed_oracle = run.returncode == 101 and 'test result: FAILED' in output and any(message in output for message in entry['runtime_messages'])
        results.append(dict(side=side, returncode=run.returncode, runtime_oracle_failed=passed_oracle, source_restored_sha256=sha(path.read_bytes()), log=str(log_path), log_sha256=sha(log_path.read_bytes())))
        Path(str(global_prefix) + '-results.json').write_text(json.dumps(results, indent=2) + '\n')
        if not passed_oracle:
            raise RuntimeError('No genuine DATA frame-size oracle failure; source restored, inspect ' + str(log_path))
    positive = Path(str(global_prefix) + '-restored-positive.log')
    if positive.exists():
        raise RuntimeError('Refusing to overwrite a prior restored positive log')
    command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test', 'native_hyper_fixed_write_buffer', '--test', 'native_h2_fixed_write_buffer', '--offline', '--locked']
    run = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
    positive.write_bytes(('COMMAND: ' + ' '.join(command) + '\nEXIT: ' + str(run.returncode) + '\n').encode() + run.stdout)
    if run.returncode != 0:
        raise RuntimeError('Restored positive failed; inspect ' + str(positive))
    for entry in prepared['entries'].values():
        assert sha((root/entry['path']).read_bytes()) == entry['original_sha256']
    for test_path, expected in prepared['test_pins'].items():
        assert sha((root/test_path).read_bytes()) == expected
    assert sha(lock.read_bytes()) == prepared['lock_sha256']
    print(json.dumps(dict(negative_results=results, restored_positive=str(positive), restored_positive_sha256=sha(positive.read_bytes())), indent=2))
if __name__ == '__main__':
    main()

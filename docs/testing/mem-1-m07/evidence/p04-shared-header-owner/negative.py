#!/usr/bin/env python3
"""Mutate the actual HTTP ownership seam, require runtime failure, restore bytes.

Run only without concurrent workspace Cargo or product edits. Scratch normal
dependencies are independent. Never accept a compile failure as this oracle.
"""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
import tempfile


def run(args, path):
    with path.open('w') as output:
        output.write('Command: ' + repr(args) + '\n')
        output.flush()
        process = subprocess.run(args, stdout=output, stderr=subprocess.STDOUT)
        output.write('\nExit: ' + str(process.returncode) + '\n')
    return process.returncode


def main():
    repo = Path(__file__).resolve().parents[5]
    source = repo / 'vendor/http-1.4.0/src/header/name.rs'
    original = source.read_bytes()
    text = original.decode()
    begin = text.index('    pub fn from_lowercase_bytes(')
    end = text.index('    /// Converts a static string', begin)
    segment = text[begin:end]
    marker = 'let value = unsafe { ByteStr::from_utf8_unchecked(src) };\n        Ok(Custom(value).into())'
    assert segment.count(marker) == 1
    mutated = text[:begin] + segment.replace(marker, 'Self::from_lowercase(&src)') + text[end:]
    output = Path(tempfile.mkdtemp(prefix='m07-shared-header-negative-'))
    (output / 'restore-copy.diff').write_text(''.join(difflib.unified_diff(
        text.splitlines(True), mutated.splitlines(True),
        fromfile='actual-original-header-name', tofile='actual-mutant-header-name')))
    command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test',
               'native_http_header_owner',
               'custom_name_and_value_move_exact_backing_and_clone_without_allocation',
               '--', '--exact', '--nocapture']
    try:
        source.write_text(mutated)
        status = run(command, output / 'restore-copy.log')
        result = (output / 'restore-copy.log').read_text()
        assert status == 101 and 'test result: FAILED' in result
        assert 'HTTP field operation allocated' in result
        assert 'error[E' not in result
    finally:
        source.write_bytes(original)
    assert source.read_bytes() == original
    restored = run(['cargo', 'test', '-p', 'novarocks-native-adapter', '--test',
                    'native_http_header_owner', '--', '--test-threads=1'],
                   output / 'restored.log')
    assert restored == 0
    receipt = {'runtime_exit': status, 'runtime_failed': True,
               'oracle': 'HTTP field operation allocated', 'restored_exit': restored,
               'byte_exact_restore': True,
               'source_sha256': hashlib.sha256(original).hexdigest()}
    (output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({'output': str(output), 'receipt': receipt}, indent=2))


if __name__ == '__main__':
    main()

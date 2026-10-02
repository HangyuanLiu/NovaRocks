#!/usr/bin/env python3
"""Run actual forwarding/dispatch mutants and restore each product byte exactly.

Do not run concurrently with workspace Cargo, product edits, or source-copy
probes. Require actual test failure, never accept a compiler failure as evidence.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument('--output', type=Path, default=Path('/tmp/m07-receive-header-negative'))
    parser.add_argument('--dispatch', action='store_true')
    parser.add_argument('--server-forward-only', action='store_true')
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=True)
    forward = '            builder.receive_header_block_buffer(buffer);'
    hyper = '        builder.receive_header_block_buffer(buffer.clone());'
    cases = [
        ('tonic-forward', 'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs', forward,
         '            drop(buffer);', 'native_tonic_connection_factory',
         'reused_header_workspace_alone_proves_actual_factory_forwarding'),
        ('hyper-client-forward', 'vendor/hyper-1.8.1/src/proto/h2/client.rs', hyper,
         '        let _ = buffer;', 'native_tonic_connection_factory',
         'reused_header_workspace_alone_proves_actual_factory_forwarding'),
    ]
    if args.dispatch:
        cases.append(('client-codec-install', 'vendor/h2-0.4.12/src/client.rs',
                      '            codec.set_receive_header_block_buffer(buffer);',
                      '            drop(buffer);', 'native_h2_receive_header_block',
                      'client_actual_static_decoder_consumption_has_no_independent_full_frame_allocation'))
        cases.append(('lost-malformed-state', 'vendor/h2-0.4.12/src/frame/headers.rs',
                      '        let mut malformed = self.is_malformed;',
                      '        let mut malformed = false;', 'native_h2_receive_header_block',
                      'fragmented_malformed_header_remains_sticky_in_fixed_and_default_modes'))
    if args.server_forward_only:
        cases = [('hyper-server-forward', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
                  '            builder.receive_header_block_buffer(buffer.clone());',
                  '            let _ = buffer;', 'native_h2_receive_header_block',
                  'cloned_hyper_server_installs_original_encoded_buffer_and_decoded_alias_is_independent')]
    receipts = []
    for name, file, anchor, replacement, target, test in cases:
        path = repo / file
        before = path.read_bytes()
        original = before.decode()
        if original.count(anchor) != 1:
            raise SystemExit(f'{name}: expected unique product anchor')
        mutated = original.replace(anchor, replacement)
        (args.output / f'{name}.diff').write_text(''.join(difflib.unified_diff(
            original.splitlines(keepends=True), mutated.splitlines(keepends=True),
            fromfile=file, tofile=file)))
        command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test', target,
                   test, '--', '--exact', '--nocapture']
        try:
            path.write_text(mutated)
            with (args.output / f'{name}.log').open('wb') as log:
                result = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=180, check=False)
        finally:
            path.write_bytes(before)
            assert path.read_bytes() == before
        body = (args.output / f'{name}.log').read_text()
        runtime = result.returncode == 101 and 'test result: FAILED' in body
        receipts.append(dict(name=name, command=command, exit=result.returncode,
                             runtime_failed=runtime, restored_sha256=hashlib.sha256(before).hexdigest()))
        (args.output / ('server-receipt.json' if args.server_forward_only else 'receipt.json')).write_text(json.dumps(receipts, indent=2)+'\n')
        if not runtime:
            raise SystemExit(f'{name}: no executed runtime failure')
        with (args.output / f'{name}-restored.log').open('wb') as log:
            subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT,
                           timeout=180, check=True)
    print(json.dumps(receipts, indent=2))


if __name__ == '__main__':
    main()

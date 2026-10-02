#!/usr/bin/env python3
"""Run serial runtime controls, restoring every product file byte-for-byte.

Requires a quiet workspace: no simultaneous product edits or workspace builds.
Does not install tools or dependencies, commit, publish, or alter locks.
"""
from pathlib import Path
import argparse
import difflib
import hashlib
import json
import subprocess

ROOT = Path(__file__).resolve().parents[5]
CASES = [
    ('protocol-preflight', 'vendor/h2-0.4.12/src/frame/headers.rs',
     '        pseudo!(protocol);', '        // runtime control: omitted protocol check',
     'native_h2_send_header_block', 'extended_connect_protocol_is_counted_before_encoding'),
    ('duplicate-preflight', 'vendor/h2-0.4.12/src/frame/headers.rs',
     'for (name, value) in &self.fields {', 'for (name, value) in self.fields.iter().take(1) {',
     'native_h2_send_header_block', 'exact_decoded_boundary_includes_status_and_each_duplicate'),
    ('prioritize-panic', 'vendor/h2-0.4.12/src/proto/streams/prioritize.rs',
     '''                    dst.buffer(frame)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;''',
     '                    dst.buffer(frame).expect("invalid frame");',
     'native_h2_send_header_block', 'extended_connect_protocol_is_counted_before_encoding'),
    ('hyper-client-forward', 'vendor/hyper-1.8.1/src/proto/h2/client.rs',
     '        builder.send_header_block_pool(pool.clone());',
     '        let _ = pool; // runtime control: omitted actual forwarding',
     'native_h2_send_header_block', 'cloned_hyper_client_retains_original_pool_and_real_peer_decodes_continuations'),
    ('hyper-server-forward', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
     '            builder.send_header_block_pool(pool.clone());',
     '            let _ = pool; // runtime control: omitted actual forwarding',
     'native_h2_send_header_block', 'cloned_hyper_server_retains_original_pool_and_real_peer_decodes_continuations'),
    ('tonic-forward', 'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs',
     '            builder.send_header_block_pool(pool);',
     '            drop(pool); // runtime control: omitted actual forwarding',
     'native_tonic_connection_factory', 'actual_channel_factory_refuses_oversized_block_before_first_headers'),
]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, default=Path('/tmp/m07-send-header-block-negative'))
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for name, relative, before, after, target, test in CASES:
        path = repo / relative
        original = path.read_bytes()
        text = original.decode()
        if text.count(before) != 1:
            raise SystemExit(f'{name}: expected unique current-source anchor')
        mutated = text.replace(before, after).encode()
        (output / f'{name}.diff').write_text(''.join(difflib.unified_diff(
            text.splitlines(keepends=True), mutated.decode().splitlines(keepends=True),
            fromfile=relative, tofile=relative)))
        command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test', target,
                   test, '--', '--exact', '--nocapture']
        try:
            path.write_bytes(mutated)
            with (output / f'{name}.log').open('wb') as log:
                result = subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=180, check=False)
        finally:
            path.write_bytes(original)
            if path.read_bytes() != original:
                raise SystemExit(f'{name}: restoration failed')
        runtime_failure = b'test result: FAILED' in (output / f'{name}.log').read_bytes()
        record = dict(name=name, command=command, exit_code=result.returncode,
                      runtime_failure=runtime_failure, restored_sha256=hashlib.sha256(original).hexdigest())
        results.append(record)
        print(json.dumps(record), flush=True)
        (output / 'results.json').write_text(json.dumps(results, indent=2)+'\n')
        if result.returncode != 101 or not runtime_failure:
            raise SystemExit(f'{name}: expected an executed runtime failure, not a compile failure')
    command = ['cargo', 'test', '-p', 'novarocks-native-adapter', '--test',
               'native_h2_send_header_block', '--test', 'native_tonic_connection_factory']
    with (output / 'restored-positive.log').open('wb') as log:
        subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT, timeout=180, check=True)
    print('All current product bytes restored; actual positive targets passed.', flush=True)

if __name__ == '__main__':
    main()

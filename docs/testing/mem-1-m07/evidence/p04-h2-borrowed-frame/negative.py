#!/usr/bin/env python3
"""Restore an intermediate raw copy in the actual dispatch and prove runtime failure.

Run without concurrent workspace builds, product edits or source-copy probes.
Product bytes are restored in finally, followed by real target replay.
"""
from pathlib import Path
import argparse
import difflib
import hashlib
import json
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument('--output', type=Path, default=Path('/tmp/m07-borrowed-frame-negative'))
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    path = repo / 'vendor/h2-0.4.12/src/codec/framed_read.rs'
    original = path.read_bytes()
    anchor = 'FrameInput::Borrowed(bytes),'
    text = original.decode()
    if text.count(anchor) != 1:
        raise SystemExit('Expected exactly one actual borrowed fixed-reader dispatch')
    mutated = text.replace(anchor, 'FrameInput::Owned(BytesMut::from(bytes)),').encode()
    (output / 'raw-copy.diff').write_text(''.join(difflib.unified_diff(
        text.splitlines(keepends=True), mutated.decode().splitlines(keepends=True),
        fromfile='vendor/h2-0.4.12/src/codec/framed_read.rs',
        tofile='vendor/h2-0.4.12/src/codec/framed_read.rs')))
    command = ['cargo','test','-p','novarocks-native-adapter','--test','native_h2_borrowed_frame',
               'valid_controls_and_large_unknown_frame_are_borrowed_before_body',
               '--','--exact','--nocapture']
    try:
        path.write_bytes(mutated)
        with (output / 'raw-copy.log').open('wb') as log:
            result = subprocess.run(command,cwd=repo,stdout=log,stderr=subprocess.STDOUT,
                                    timeout=180,check=False)
    finally:
        path.write_bytes(original)
        if path.read_bytes() != original:
            raise SystemExit('Product byte-exact restoration failed')
    body = (output / 'raw-copy.log').read_text()
    runtime = result.returncode == 101 and 'test result: FAILED' in body
    print(json.dumps(dict(command=command,exit=result.returncode,runtime_failure=runtime,
                         byte_exact_restored_sha256=hashlib.sha256(original).hexdigest())),flush=True)
    (output / 'receipt.json').write_text(json.dumps(dict(command=command,exit=result.returncode,
        runtime_failure=runtime,restored_sha256=hashlib.sha256(original).hexdigest()),indent=2)+'\n')
    if not runtime:
        raise SystemExit('Expected executed allocator failure; compilation failure is not evidence')
    command = ['cargo','test','-p','novarocks-native-adapter','--test','native_h2_borrowed_frame']
    with (output / 'restored.log').open('wb') as log:
        subprocess.run(command,cwd=repo,stdout=log,stderr=subprocess.STDOUT,timeout=180,check=True)
    print('Restored actual protocol target passed.',flush=True)

if __name__ == '__main__':
    main()

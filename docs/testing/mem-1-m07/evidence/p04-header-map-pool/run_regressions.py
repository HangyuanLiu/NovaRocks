#!/usr/bin/env python3
"""Runtime negatives in the independent complete actual-source probe; restore every mutation."""
import argparse
import difflib
import hashlib
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--probe', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
source = args.probe / 'http-1.4.0/src/header/map.rs'
original = source.read_text()
cases = [
    ('extra-unfunded-growth', 'if self.allocation.is_some() && self.extra_values.len() == self.extra_values.capacity()', 'if false && self.extra_values.len() == self.extra_values.capacity()', 'full_keys_still_accept_duplicates_replace_and_entry'),
    ('clone-unfunded-copy', 'if let Some(claim) = &self.allocation {\n            let mut copy', 'if let Some(claim) = None::<&MapAllocationClaim> {\n            let mut copy', 'eager_copy_claims_and_preserves_sensitive_values'),
    ('iterator-credit-too-early', '_allocation: self.allocation,', '_allocation: None,', 'owning_iterator_holds_original_position'),
    ('missing-panic-cleanup', 'fn drop(&mut self) {\n                for _ in self.0.by_ref() {}\n            }', 'fn drop(&mut self) {}', 'ordinary_cell_clone_and_panicking_iterator_drop'),
    ('premature-multivalue-unlink', 'links = entry.links;', 'links = entry.links.take();', 'extra_value_drain_claims_before_mutation'),
    ('unfunded-value-drain', 'if has_extra {', 'if has_extra && self.allocation.is_none() {', 'extra_value_drain_claims_before_mutation'),
]
try:
    for name, needle, replacement, test in cases:
        if original.count(needle) != 1:
            raise SystemExit(f'Unique mutation failed: {name}: {original.count(needle)}')
        changed = original.replace(needle, replacement, 1)
        # Keep the deliberately unreachable tail compilable without disguising test failures.
        command = ['cargo', 'test', '--manifest-path', str(args.probe / 'Cargo.toml'), '--offline', '--locked', '--lib', test, '--', '--exact']
        source.write_text(changed)
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        (args.output / (name + '.log')).write_text(result.stdout)
        (args.output / (name + '.diff')).write_text(''.join(difflib.unified_diff(original.splitlines(True), changed.splitlines(True), fromfile='actual', tofile=name)))
        if result.returncode != 101 or 'test result: FAILED.' not in result.stdout:
            raise SystemExit(f'Negative did not fail at runtime: {name}: {result.returncode}')
        print(f'{name}: runtime 101', flush=True)
        source.write_text(original)
finally:
    source.write_text(original)
    print('Restored SHA256: ' + hashlib.sha256(source.read_bytes()).hexdigest(), flush=True)

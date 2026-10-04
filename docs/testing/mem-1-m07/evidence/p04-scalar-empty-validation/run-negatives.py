from pathlib import Path
import os, subprocess, json, hashlib, difflib

root = Path.cwd()
work = root / 'logs/mem-1-m07/p04-scalar-empty-validation/negatives'
work.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, RUSTUP_TOOLCHAIN='1.92.0', CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0', CARGO_TARGET_DIR=str(root / 'logs/mem-1-m07/p04-response-port/product-target'))
path = 'novarocks/native-adapter/src/root_scalar_leaf_codec.rs'
cases = [
    ('empty-bypasses-zone-witness', '        let mut examined = 0;\n        let mut work = 0;', '        if self.candidate_encoded_len == 0 { source = 3; }\n        let mut examined = 0;\n        let mut work = 0;', 'empty_zone_mismatch_latches_failure_without_emission_or_credit_return'),
    ('empty-is-one-row', '            completed_rows: 0,\n            status: if empty_complete {', '            completed_rows: u64::from(empty_complete),\n            status: if empty_complete {', 'empty_schema_witness_emits_neither_value_nor_absent_record'),
    ('empty-length-before-witness', '            ScalarLeafPhase::Validating { .. } | ScalarLeafPhase::Failed => None,', '            ScalarLeafPhase::Validating { .. } if self.candidate_encoded_len == 0 => Some(0),\n            ScalarLeafPhase::Validating { .. } | ScalarLeafPhase::Failed => None,', 'empty_schema_witness_emits_neither_value_nor_absent_record'),
]
results = []
for label, old, new, test in cases:
    file = root / path
    original = file.read_bytes()
    source = original.decode()
    assert source.count(old) == 1
    changed = source.replace(old, new)
    (work / (label + '.patch')).write_text(''.join(difflib.unified_diff(source.splitlines(True), changed.splitlines(True), fromfile=path, tofile=path)))
    file.write_text(changed)
    try:
        with (work / (label + '.log')).open('w') as out:
            result = subprocess.run(['cargo', 'test', '--offline', '--locked', '-p', 'novarocks-native-adapter', '--test', 'native_scalar_leaf_validation', test, '--', '--exact', '--nocapture'], env=env, stdout=out, stderr=subprocess.STDOUT)
        log = (work / (label + '.log')).read_text()
        row = dict(label=label, source=path, test=test, exit_code=result.returncode, compiled_runtime_failure='test result: FAILED' in log, source_before_sha256=hashlib.sha256(original).hexdigest(), source_mutated_sha256=hashlib.sha256(changed.encode()).hexdigest())
        results.append(row)
        print(json.dumps(row), flush=True)
    finally:
        file.write_bytes(original)
        assert file.read_bytes() == original
    (work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    assert result.returncode == 101 and row['compiled_runtime_failure']

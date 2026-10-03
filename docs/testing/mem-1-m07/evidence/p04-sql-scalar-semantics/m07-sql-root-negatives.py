from pathlib import Path
import hashlib, json, os, subprocess, time

root = Path('/Users/harbor/.codex/worktrees/b92b/NovaRocks')
run = Path('/tmp/m07-sql-root-negatives')
run.mkdir(exist_ok=True)
def replace(old, new):
    def mutate(src):
        assert src.count(old) == 1, (old, src.count(old))
        return src.replace(old, new, 1)
    return mutate

cases = [
    ('conditional-return-domain', 'novarocks/sql/src/analyzer/logical_output.rs',
     replace('        domain\n    }\n\n    /// Value provenance', '        None\n    }\n\n    /// Value provenance'),
     ['-p', 'novarocks-sql', '--lib', 'analyzer::logical_output::scalar_domain_tests::m07_scalar_bound_conditionals_keep_only_homogeneous_returned_domains']),
    ('complete-nested-declaration', 'novarocks/sql/src/compiler/root_output.rs',
     replace('                factory.borrowed_logical_type(column.column_id),', '                None,'),
     ['-p', 'novarocks-sql', '--lib', 'compiler::root_output::tests::m07_scalar_complete_nested_declaration_is_not_lost_as_plain_storage']),
    ('final-nested-marker-proof', 'novarocks/sql/src/compiler/root_output.rs',
     replace(' || !super::root_scalar_type::nested_domains_match(&field, &ty.data_type)', ''),
     ['-p', 'novarocks-sql', '--lib', 'compiler::root_output::tests::m07_scalar_final_carrier_must_keep_the_captured_nested_domain']),
    ('invalid-leaf-witness', 'novarocks/types/src/coercion.rs',
     replace('if invalid_source || erased_opaque {', 'if false {'),
     ['-p', 'novarocks-sql', '--lib', 'compiler::root_scalar_type::tests::m07_scalar_common_type_cannot_erase_an_invalid_domain_into_a_value']),
    ('invalid-map-container-witness', 'novarocks/types/src/coercion.rs',
     replace('if has_marker {', 'if false {'),
     ['-p', 'novarocks-sql', '--lib', 'compiler::root_scalar_type::tests::m07_scalar_map_container_marker_survives_normalization_as_a_refusal']),
    ('shared-nested-domain', 'novarocks/types/src/coercion.rs',
     replace('            left.filter(|logical| right == Some(*logical))', '            None'),
     ['-p', 'novarocks-types', '--test', 'm07_nested_domain_merge', 'list_same_json_domain_survives_nullability_merge']),
    ('independent-physical-proof', 'novarocks/physical-plan/src/validation/graph.rs',
     replace('result.scalar_schema.as_ref().is_some_and(|frozen| frozen.field() == schema.field())',
             'result.scalar_schema.as_ref().is_none_or(|frozen| frozen.field() == schema.field())'),
     ['-p', 'novarocks-native-adapter', '--lib', 'physical_v1_roundtrip::scalar_physical_root_refuses_missing_independent_semantic_proof']),
]
env = dict(os.environ, CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0')
originals = {path: (root / path).read_bytes() for _, path, _, _ in cases}
records = []
try:
    for name, path, mutate, args in cases:
        source = root / path
        original = originals[path]
        assert source.read_bytes() == original
        modified = mutate(original.decode()).encode()
        assert modified != original
        command = ['cargo', '+1.92.0', 'test', '--locked'] + args + ['--', '--exact', '--test-threads=1']
        log = run / (name + '.log')
        start = time.time()
        try:
            source.write_bytes(modified)
            with log.open('wb') as out:
                result = subprocess.run(command, cwd=root, env=env, stdout=out, stderr=subprocess.STDOUT, timeout=600)
            output = log.read_text(errors='replace')
            record = dict(name=name, source=path, command=command, exit_code=result.returncode,
                compiled_runtime_failed=result.returncode == 101 and 'test result: FAILED.' in output,
                elapsed_seconds=round(time.time()-start, 2), log=str(log))
        finally:
            source.write_bytes(original)
            assert source.read_bytes() == original
        records.append(record)
        (run / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
        print(json.dumps(record), flush=True)
        assert record['compiled_runtime_failed'], record
finally:
    for path, original in originals.items():
        (root / path).write_bytes(original)
        assert (root / path).read_bytes() == original
    (run / 'restored-sources.json').write_text(json.dumps({p:hashlib.sha256(b).hexdigest() for p,b in originals.items()}, indent=2)+'\n')

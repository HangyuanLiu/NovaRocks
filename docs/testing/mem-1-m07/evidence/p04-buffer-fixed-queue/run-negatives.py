from pathlib import Path
import os, subprocess, json, hashlib, difflib

root = Path.cwd()
work = root / 'logs/mem-1-m07/p04-buffer-fixed-queue/negatives'
work.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, RUSTUP_TOOLCHAIN='1.92.0', CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0', CARGO_TARGET_DIR=str(root / 'logs/mem-1-m07/p04-response-port/product-target'))
queue = 'vendor/tower-0.4.13/src/buffer/queue.rs'
service = 'vendor/tower-0.4.13/src/buffer/service.rs'
cases = [
    ('missing-fixed-slots-query', queue, 'core.checked_add(slots.size())', 'core.checked_add(0)', 'actual_fixed_constructor_matches_exact_common_plus_queue_queries'),
    ('missing-fixed-mutex-prewarm', queue, '        drop(handle.state());', '        // Negative: omit the actual final fixed mutex prewarm.', 'actual_fixed_constructor_matches_exact_common_plus_queue_queries'),
    ('original-route-ordinary-mpsc', service, '        let (tx, rx) = queue::pair_original(bound, original.clone())?;', '        let (tx, rx) = queue::pair();', 'actual_ring_wrap_preserves_fifo_and_has_only_prepaid_cell_allocations'),
    ('close-loses-accepted-fifo', queue, '                        state.receiver_closed = true;\n                        state.waker.take()', '                        state.receiver_closed = true;\n                        state.len = 0; // Negative: hide accepted messages before drain.\n                        state.waker.take()', 'actual_service_error_closes_new_sends_and_drains_every_old_reply'),
]
results = []
for label, path, old, new, test in cases:
    file = root / path
    original = file.read_bytes()
    source = original.decode()
    assert source.count(old) == 1
    changed = source.replace(old, new)
    (work / (label + '.patch')).write_text(''.join(difflib.unified_diff(source.splitlines(True), changed.splitlines(True), fromfile=path, tofile=path)))
    file.write_text(changed)
    try:
        with (work / (label + '.log')).open('w') as out:
            result = subprocess.run(['cargo', 'test', '--offline', '--locked', '-p', 'novarocks-native-adapter', '--test', 'native_tower_fixed_queue', test, '--', '--exact', '--nocapture'], env=env, stdout=out, stderr=subprocess.STDOUT)
        log = (work / (label + '.log')).read_text()
        row = dict(label=label, source=path, test=test, exit_code=result.returncode, compiled_runtime_failure='test result: FAILED' in log, source_before_sha256=hashlib.sha256(original).hexdigest(), source_mutated_sha256=hashlib.sha256(changed.encode()).hexdigest())
        results.append(row)
        print(json.dumps(row), flush=True)
    finally:
        file.write_bytes(original)
        assert file.read_bytes() == original
    (work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    assert result.returncode == 101 and row['compiled_runtime_failure']

#!/usr/bin/env python3
"""Require compiled failures from actual field/metrics source regressions."""
from pathlib import Path
import difflib
import json
import subprocess
import tempfile

repo = Path.cwd()
out = Path(tempfile.mkdtemp(prefix="m07-control-concurrency-negatives-"))
field = "vendor/http-1.4.0/src/header/field_pool.rs"
metrics = "novarocks/native-adapter/src/backend_metrics.rs"
originals = {path: (repo / path).read_bytes() for path in (field, metrics)}
old_field = subprocess.check_output(["git", "show", "ce4b32946468190ea2be9ee3965cfd0e17be785d:" + field])
assert b"compare_exchange(false, true" in old_field
cases = [
    ("temporary-checkout-contention-refuses-capacity", field, old_field,
     ["--test", "native_http_field_pool_concurrency", "pending_fill_does_not_refuse_original_date_and_generated_header_consumers"]),
    ("busy-scrape-republishes-stale-preparation", metrics,
     originals[metrics].replace(b'families.retain(|family| family.get_name() != "novarocks_backend_task_preparation");',
                               b'families.retain(|family| family.get_name() != "nonexistent_family");'),
     ["--lib", "preparation_busy_scrape_omits_stale_values_without_blocking_control_metrics"]),
    ("busy-scrape-claims-available-ledger", metrics,
     originals[metrics].replace(b'.set(i64::from(preparation.is_some()));', b'.set(1);'),
     ["--lib", "preparation_busy_scrape_omits_stale_values_without_blocking_control_metrics"]),
]
results = {}
try:
    for label, path, mutated, selection in cases:
        assert mutated != originals[path], (label, "source mutation required")
        (repo / path).write_bytes(mutated)
        (out / (label + ".diff")).write_text("".join(difflib.unified_diff(
            originals[path].decode().splitlines(True), mutated.decode().splitlines(True),
            fromfile=path, tofile=path)))
        command = ["cargo", "test", "--offline", "-p", "novarocks-native-adapter"] + selection + ["--", "--test-threads=1"]
        log = out / (label + ".log")
        with log.open("w") as output:
            output.write("Command: " + " ".join(command) + "\n")
            output.flush()
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=240)
        (repo / path).write_bytes(originals[path])
        assert result.returncode == 101 and "test result: FAILED." in log.read_text(), (
            label, result.returncode, "compiled runtime failure required")
        results[label] = "compiled runtime FAILED"
        print(label, results[label], flush=True)
finally:
    for path, source in originals.items():
        (repo / path).write_bytes(source)
    assert all((repo / path).read_bytes() == source for path, source in originals.items())
    (out / "verification.json").write_text(json.dumps(results, indent=2) + "\n")
    print("exact source restoration; evidence directory", out, flush=True)

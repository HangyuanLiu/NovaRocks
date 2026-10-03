#!/usr/bin/env python3
"""Mutate actual cache/physical admission wiring; require runtime failures."""
from pathlib import Path
import difflib
import json
import subprocess
import sys
import tempfile

repo = Path.cwd()
out = Path(tempfile.mkdtemp(prefix="m07-peer-capacity-negatives-"))
cache = "novarocks/native-adapter/src/native_channel_cache.rs"
client = "novarocks/native-adapter/src/native_client.rs"
capacity = "novarocks/native-adapter/src/native_transport_capacity.rs"
cases = [
    ("ninth-cache-waiter-is-admitted", cache,
     "transport_tonic_pending_per_connection as usize;",
     "transport_tonic_pending_per_connection as usize + 1;",
     "one_leader_eight_waiters_and_ninth_refusal_publish_one_result"),
    ("first-wake-panic-skips-other-waiters", cache,
     "first_panic = Some(payload);", "std::panic::resume_unwind(payload);",
     "first_notification_panic_does_not_skip_second_or_poison_published_result"),
    ("factory-omits-key-on-reconnect", client,
     "Some(key) => factory.try_config_for_key(class, key),",
     "Some(_key) => factory.try_config(class),",
     "actual_same_channel_internal_reconnect_refuses_connecting_gate_before_new_tcp_dial"),
    ("cache-miss-uses-unkeyed-factory", client,
     "let channel = capacity_endpoint_for_key(runtime, &key, TransportClass::Data)?",
     "let channel = capacity_endpoint(runtime, &key.endpoint, TransportClass::Data)?",
     "production_cache_miss_uses_same_key_gate_before_tcp_and_recovers_failed_leader"),
    ("lifecycle-omits-key-live-verdict", capacity,
     "self.factory.core().connection_keys.install(token)",
     "Ok::<(), io::Error>(())",
     "full_closing_preserves_live_and_connecting_original_charges_until_real_alias_exit"),
    ("lifecycle-omits-key-retirement", capacity,
     "self.factory.core().connection_keys.retire(token)?;", "let _ = token;",
     "unbound_key_position_is_held_until_last_actual_original_pool_alias"),
    ("stock-refusal-leaks-key-claim", capacity,
     "self.capacity\n                        .exit(token)\n                        .expect(\"unpublished key claim exits once\");",
     "let _ = token;",
     "stock_refusal_rolls_back_unpublished_key_claim_before_next_attempt"),
]
if len(sys.argv) > 1:
    selected = set(sys.argv[1:])
    assert selected <= {case[0] for case in cases}
    cases = [case for case in cases if case[0] in selected]
originals = {path: (repo / path).read_bytes() for _, path, *_ in cases}
for label, path, old, *_ in cases:
    assert originals[path].decode().count(old) == 1, (label, "unique source anchor")
results = {}
try:
    for label, path, old, new, filter_ in cases:
        source = originals[path].decode()
        mutated = source.replace(old, new, 1)
        (repo / path).write_text(mutated)
        (out / (label + ".diff")).write_text("".join(difflib.unified_diff(
            source.splitlines(True), mutated.splitlines(True), fromfile=path, tofile=path)))
        command = ["cargo", "test", "--offline", "-p", "novarocks-native-adapter",
                   "--lib", filter_, "--", "--test-threads=1"]
        log = out / (label + ".log")
        with log.open("w") as handle:
            handle.write("Command: " + " ".join(command) + "\n")
            handle.flush()
            result = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, timeout=180)
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

#!/usr/bin/env python3
"""Verify exact peer-cache/physical capacity source pins and actual consumers."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import tempfile

repo = Path.cwd()
receipt = Path(__file__).parent
for filename, base in [("product-sha256.json", repo), ("vendor-source-sha256.json", repo / "vendor")]:
    for relative, digest in json.loads((receipt / filename).read_text()).items():
        assert hashlib.sha256((base / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix="m07-peer-capacity-verification-"))
targets = ["native_connection_lifecycle", "native_initial_settings_kernel", "native_tonic_acquisition_owner",
           "native_tonic_connection_factory", "native_h2_stream_store_owner", "native_h2_resident_store",
           "native_http_header_map_pool", "native_tonic_header_map_capacity", "native_tonic_status_field_pool",
           "native_tonic_preallocated_status", "native_tonic_preallocated_trailers", "native_tonic_preallocated_unary",
           "root_result_reader", "native_tonic_request_response_headers", "native_ingress_response_headers"]
protocol = ["cargo", "test", "--offline", "-p", "novarocks-native-adapter"]
for target in targets:
    protocol += ["--test", target]
commands = [
    ("native-lib", ["cargo", "test", "--offline", "-p", "novarocks-native-adapter", "--lib", "--", "--test-threads=1"]),
    ("protocol", protocol + ["--", "--test-threads=1"]),
    ("geometry", ["cargo", "test", "--offline", "-p", "novarocks-native-adapter", "--lib",
                  "checked_stock_receipt_fits_frozen_process_and_connection_envelopes", "--", "--nocapture"]),
    ("native-clippy", ["cargo", "clippy", "--offline", "-p", "novarocks-native-adapter", "--lib", "--tests"]),
    ("fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    ("diff", ["git", "diff", "--check"]),
]
for guard in ["native-wire", "native-trust", "application-domain", "local-program", "memory"]:
    commands.append((guard + "-guard", ["python3", "tools/ci/check-" + guard + "-dependency-boundary.py"]))
results = {}
for label, command in commands:
    with (out / (label + ".log")).open("w") as log:
        log.write("Command: " + " ".join(command) + "\n")
        log.flush()
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    results[label] = result.returncode
    print(label, result.returncode, flush=True)
    if result.returncode:
        print("evidence directory", out, flush=True)
        raise SystemExit(result.returncode)
results["native_lib_tests"] = int(re.search(r"test result: ok\. (\d+) passed;", (out / "native-lib.log").read_text()).group(1))
results["protocol_tests"] = sum(map(int, re.findall(r"test result: ok\. (\d+) passed;", (out / "protocol.log").read_text())))
assert results["native_lib_tests"] >= 648 and results["protocol_tests"] >= 151
(out / "verification.json").write_text(json.dumps(results, indent=2) + "\n")
print("evidence directory", out, flush=True)

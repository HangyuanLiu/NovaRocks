#!/usr/bin/env python3
"""Mutate actual Control routing and original listener admission; require runtime failures."""
from pathlib import Path
import difflib
import json
import subprocess
import sys
import tempfile

repo = Path.cwd()
out = Path(tempfile.mkdtemp(prefix="m07-control-negatives-"))
ingress = "novarocks/native-adapter/src/native_ingress.rs"
server = "novarocks/native-adapter/src/native_server.rs"
backend = "novarocks/native-adapter/src/backend_application.rs"
fe = "novarocks/frontend-application/src/native/transport.rs"
topology = "novarocks/frontend-application/src/topology.rs"
codec = "novarocks/proto-codec/src/membership.rs"
contract = "novarocks/execution-contract/src/membership.rs"
cases = [
    ("wrong-domain-bypasses-predecode-guard", ingress,
     ".filter(|method| method.is_allowed_at(self.domain))",
     ".filter(|method| method.contract().traffic != novarocks_proto_codec::native_rpc::NativeTrafficClass::Retired)",
     "novarocks-native-adapter", "wrong_domain_and_retired_manifest_entries_refuse_before_body_handler_and_gates"),
    ("domain-class-mismatch-is-not-rejected", server,
     "if !compatible {", "if false && !compatible {",
     "novarocks-native-adapter", "mismatched_domain_and_stock_class_refuse_before_address_resolution"),
    ("backend-control-borrows-data-stock", backend,
     "transport_capacity,\n            crate::native_transport_capacity::TransportClass::Control,",
     "transport_capacity,\n            crate::native_transport_capacity::TransportClass::Data,",
     "novarocks-native-adapter", "application_authenticates_complete_native_route_set_before_domain_or_fallback"),
    ("fe-control-rpc-dials-data-endpoint", fe,
     "Ok(descriptor.control_endpoint().native_endpoint())",
     "Ok(descriptor.endpoint().native_endpoint())",
     "novarocks-frontend-application", "frozen_descriptor_routes_ordinary_and_control_runs_to_independent_sockets"),
    ("heartbeat-manager-dials-data-endpoint", topology,
     ".map(|(id, facts)| (*id, facts.descriptor.control_endpoint().clone()))",
     ".map(|(id, facts)| (*id, facts.descriptor.endpoint().clone()))",
     "novarocks-frontend-application", "heartbeat_rows_use_control_without_changing_data_endpoint_ownership"),
    ("missing-control-wire-falls-back-to-data", codec,
     "let control_endpoint = raw.control_endpoint.clone().ok_or_else(|| {",
     "let control_endpoint = raw.control_endpoint.clone().or_else(|| raw.endpoint.clone()).ok_or_else(|| {",
     "novarocks-proto-codec", "descriptor_requires_valid_independent_control_even_without_root_support"),
    ("root-support-control-drift-is-accepted", contract,
     "if support.control_endpoint() != &self.control_endpoint {",
     "if false && support.control_endpoint() != &self.control_endpoint {",
     "novarocks-execution-contract", "mandatory_control_endpoint_is_distinct_and_part_of_exact_process_facts"),
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
    for label, path, old, new, package, filter_ in cases:
        source = originals[path].decode()
        mutated = source.replace(old, new, 1)
        (repo / path).write_text(mutated)
        (out / (label + ".diff")).write_text("".join(difflib.unified_diff(
            source.splitlines(True), mutated.splitlines(True), fromfile=path, tofile=path)))
        command = ["cargo", "test", "--offline", "-p", package,
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

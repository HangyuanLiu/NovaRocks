#!/usr/bin/env python3
"""Verify pinned registration/typed connector source and actual focused tests."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

repo = Path.cwd()
receipt = Path(__file__).parent
for name, digest in json.loads((receipt / "source-sha256.json").read_text()).items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest, name
# Validate the complete copied upstream tree as well as the product source pins.
upstream_root = repo / "vendor/tokio-1.52.3"
upstream = json.loads((upstream_root / "UPSTREAM.json").read_text())["source_files"]
changed = {name for name, digest in upstream.items()
           if hashlib.sha256((upstream_root / name).read_bytes()).hexdigest() != digest}
assert changed == {"src/io/poll_evented.rs", "src/net/tcp/listener.rs", "src/net/tcp/stream.rs",
                   "src/runtime/io/driver.rs", "src/runtime/io/mod.rs", "src/runtime/io/registration.rs",
                   "src/runtime/io/registration_set.rs", "src/runtime/io/scheduled_io.rs"}, changed
out = Path(tempfile.mkdtemp(prefix="m07-original-socket-verify-"))
env = os.environ.copy()
env.update(CARGO_BUILD_JOBS="4", CARGO_INCREMENTAL="0")
targets = ["native_original_socket_registration", "native_tonic_attempt_connector",
           "native_original_io_box", "native_tonic_original_io_owner",
           "native_connection_lifecycle", "native_initial_settings_kernel",
           "native_tonic_acquisition_owner", "native_tonic_connection_factory",
           "native_http_field_pool_concurrency"]
protocol = ["cargo", "test", "--offline", "--locked", "-p", "novarocks-native-adapter"]
for target in targets:
    protocol += ["--test", target]
commands = [
    ("libs", ["cargo", "test", "--offline", "--locked", "-p", "novarocks-native-adapter",
              "-p", "novarocks-native-trust", "--lib", "--", "--test-threads=1"]),
    ("protocol", protocol + ["--", "--test-threads=1"]),
    ("workspace-mutex", ["cargo", "test", "--offline", "--locked", "--workspace",
                         "--test", "native_original_socket_registration", "--", "--test-threads=1"]),
    ("fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    ("diff", ["git", "diff", "--check"]),
]
for label, command in commands:
    with (out / (label + ".log")).open("w") as log:
        log.write("Command: " + " ".join(command) + "\n")
        log.flush()
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    print(label, result.returncode, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
print("evidence directory", out, flush=True)

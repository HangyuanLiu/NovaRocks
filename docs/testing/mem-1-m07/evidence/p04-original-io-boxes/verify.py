#!/usr/bin/env python3
"""Verify pinned actual IO source and focused implementation behavior."""
from pathlib import Path
import hashlib
import json
import subprocess
import tempfile

repo = Path.cwd()
receipt = Path(__file__).parent
for name, digest in json.loads((receipt / "source-sha256.json").read_text()).items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest, name
out = Path(tempfile.mkdtemp(prefix="m07-original-io-verify-"))
protocol = ["cargo", "test", "--offline", "-p", "novarocks-native-adapter"]
for target in ["native_tonic_acquisition_owner", "native_tonic_connection_factory",
               "native_connection_lifecycle", "native_initial_settings_kernel",
               "native_original_io_box", "native_tonic_original_io_owner"]:
    protocol += ["--test", target]
commands = [
    ("libs", ["cargo", "test", "--offline", "-p", "novarocks-native-adapter", "-p",
              "novarocks-native-trust", "--lib", "--", "--test-threads=1"]),
    ("protocol", protocol + ["--", "--test-threads=1"]),
    ("clippy", ["cargo", "clippy", "--offline", "-p", "novarocks-native-adapter", "-p",
                "novarocks-native-trust", "--all-targets"]),
    ("fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    ("diff", ["git", "diff", "--check"]),
]
for label, command in commands:
    with (out / (label + ".log")).open("w") as log:
        log.write("Command: " + " ".join(command) + "\n")
        log.flush()
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    print(label, result.returncode, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
print("evidence directory", out, flush=True)

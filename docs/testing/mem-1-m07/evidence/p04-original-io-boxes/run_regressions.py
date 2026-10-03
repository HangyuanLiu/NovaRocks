#!/usr/bin/env python3
"""Require exact-target runtime rejection of actual IO owner regressions.

Run only with exclusive ownership of these production files and Cargo. This
script temporarily mutates their actual bytes and restores all three in finally.
Its proof is concrete IO Box/carrier ordering, not socket/TLS/task ownership.
"""

import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise AssertionError("exact unique production mutation anchor required")
    return source.replace(old, new, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument(
        "--native-test",
        default="actual_last_original_owner_exits_after_normal_box_deallocation",
        help="exact Native normal single-owner physical deallocation test name",
    )
    parser.add_argument("--case", choices=("native-owner-first", "tonic-owner-first", "tonic-missing-output-owner"))
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    native = "novarocks/native-trust/src/adapter.rs"
    tonic = "vendor/tonic-0.12.3/src/transport/channel/service/io.rs"
    connection = "vendor/tonic-0.12.3/src/transport/channel/service/connection.rs"
    originals = {path: (repo / path).read_bytes() for path in (native, tonic, connection)}
    cases = [
        (
            "native-owner-first", native,
            replace_once(
                originals[native],
                b"        let owner = self.owner.take();\n        drop(self.io.take());\n        drop(owner);",
                b"        let owner = self.owner.take();\n        drop(owner);\n        drop(self.io.take());",
            ),
            "native_original_io_box", args.native_test,
        ),
        (
            "tonic-owner-first", tonic,
            replace_once(
                originals[tonic],
                b"        let io = self.io.take();\n        drop(io);\n        drop(owner);",
                b"        let io = self.io.take();\n        drop(owner);\n        drop(io);",
            ),
            "native_tonic_original_io_owner",
            "actual_private_wrapper_frees_both_boxes_before_original_owner",
        ),
        (
            "tonic-missing-output-owner", connection,
            replace_once(
                originals[connection],
                b"let io = OwnedConnectionIo::new(io, output_io_owner);",
                b"let io = OwnedConnectionIo::new(io, None);",
            ),
            "native_tonic_original_io_owner",
            "actual_channel_keeps_io_owner_after_success_until_connection_exit",
        ),
    ]
    if args.case:
        cases = [case for case in cases if case[0] == args.case]
    # Refuse a typo before modifying any production bytes. In particular, a
    # panic-in-Drop test is not a substitute for the normal last-owner oracle:
    # double panic/SIGABRT is deliberately rejected below.
    for _, _, _, target, test in cases:
        source = (repo / f"novarocks/native-adapter/tests/{target}.rs").read_text()
        if not re.search(r"\b(?:async\s+)?fn\s+" + re.escape(test) + r"\s*\(", source):
            raise AssertionError(f"exact physical-exit oracle missing: {target}::{test}")
    out = Path(tempfile.mkdtemp(prefix="m07-original-io-boxes-negatives-"))
    record = {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "scope": "actual outer IO Box and original carrier; not TLS/socket/task closure",
        "original_sha256": {path: sha256(source) for path, source in originals.items()},
        "tests_sha256": {
            target: sha256((repo / f"novarocks/native-adapter/tests/{target}.rs").read_bytes())
            for _, _, _, target, _ in cases
        },
        "cases": [],
    }
    print("Evidence directory:", out, flush=True)
    try:
        for label, path, mutated, target, test in cases:
            if not all((repo / name).read_bytes() == source for name, source in originals.items()):
                raise AssertionError("exclusive unchanged production files required between cases")
            (out / f"{label}.diff").write_text("".join(difflib.unified_diff(
                originals[path].decode().splitlines(True), mutated.decode().splitlines(True),
                fromfile=path, tofile=path,
            )))
            command = ["cargo", "test", "--offline", "--locked", "-p", "novarocks-native-adapter",
                       "--test", target, test, "--", "--exact", "--test-threads=1"]
            log = out / f"{label}.log"
            case = {
                "name": label, "source": path, "mutation_sha256": sha256(mutated),
                "command": command, "log": str(log), "verdict": "not completed",
            }
            record["cases"].append(case)
            (repo / path).write_bytes(mutated)
            try:
                with log.open("w") as output:
                    output.write("Command: " + " ".join(command) + "\n")
                    output.flush()
                    result = subprocess.run(command, cwd=repo, stdout=output,
                                            stderr=subprocess.STDOUT, timeout=args.timeout)
                case["exit_code"] = result.returncode
            except subprocess.TimeoutExpired:
                case["verdict"] = "rejected: timeout is not a compiled runtime FAILED"
                raise
            finally:
                (repo / path).write_bytes(originals[path])
                case["restored_sha256"] = sha256((repo / path).read_bytes())
                if log.exists():
                    case["log_sha256"] = sha256(log.read_bytes())
            text = log.read_text(errors="replace")
            valid = (
                result.returncode == 101
                and re.search(r"\brunning 1 test\b", text)
                and re.search(r"^test " + re.escape(test) + r" \.\.\. FAILED\s*$", text, re.MULTILINE)
                and re.search(r"test result: FAILED\. 0 passed; 1 failed;", text)
                and not re.search(r"error\[E\d+\]|could not compile|signal:|SIGABRT|running 0 tests", text)
            )
            if not valid:
                case["verdict"] = "rejected: exact compiled runtime FAILED required"
                raise AssertionError((label, result.returncode, str(log), case["verdict"]))
            case["verdict"] = "compiled runtime FAILED; exact one test"
            print(label, case["verdict"], flush=True)
    finally:
        for path, source in originals.items():
            (repo / path).write_bytes(source)
        record["exact_restoration"] = all(
            (repo / path).read_bytes() == source for path, source in originals.items()
        )
        record["final_sha256"] = {
            path: sha256((repo / path).read_bytes()) for path in originals
        }
        (out / "verification.json").write_text(json.dumps(record, indent=2) + "\n")
        print("Exact source restoration:", record["exact_restoration"], "evidence:", out, flush=True)
        if not record["exact_restoration"]:
            raise AssertionError("byte-exact source restoration failed")


if __name__ == "__main__":
    main()

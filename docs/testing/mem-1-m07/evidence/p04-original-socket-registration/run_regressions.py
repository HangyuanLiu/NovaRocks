#!/usr/bin/env python3
"""Require exact runtime failures for actual Tokio registration regressions.

Run only with exclusive ownership of the two production files and Cargo. Each
mutation is byte-exactly restored in finally. The PAL-prewarm oracle requires
the supported macOS pthread implementation; Linux has no heap PAL to omit.
This receipt covers registration Arc/PAL/carrier ordering, not the whole reactor,
socket, TLS, Waker, task or Native connection graph.
"""

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
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
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--case", choices=("ordinary-arc-retirement", "missing-pal-prewarm"))
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    # Refuse an inapplicable PAL oracle before touching source. Its mutation
    # would correctly preserve the Linux inline-futex constructor allocation.
    if args.case != "ordinary-arc-retirement" and sys.platform != "darwin":
        parser.error("missing-pal-prewarm requires the supported macOS pthread allocator; select ordinary-arc-retirement on Linux")
    repo = args.repo.resolve(strict=True)
    scheduled = "vendor/tokio-1.52.3/src/runtime/io/scheduled_io.rs"
    registration = "vendor/tokio-1.52.3/src/runtime/io/registration_set.rs"
    target = "native_original_socket_registration"
    test = "listener_drop_waits_for_actual_reactor_retirement_before_original_credit"
    originals = {path: (repo / path).read_bytes() for path in (scheduled, registration)}
    cases = [
        (
            "ordinary-arc-retirement", scheduled,
            replace_once(
                originals[scheduled],
                b"""            if let Some(mut io) = Arc::into_inner(arc) {
                // No Weak handles exist. The Arc allocation has physically
                // exited before into_inner returns. Keep the original owner
                // outside ScheduledIo so wake/destructor unwinding drops the
                // waiters/PAL before releasing this final capability as well.
                let owner = io.registration_owner.take();
                drop(io);
                drop(owner);
            }
""",
                b"            drop(arc);\n",
            ),
            "actual Arc deallocation must precede original carrier/credit exit",
        ),
        (
            "missing-pal-prewarm", registration,
            replace_once(
                originals[registration],
                b"        ret.prewarm();\n",
                b"        // Negative probe: omit unpublished final-Arc PAL prewarm.\n",
            ),
            "actual constructor must capture the pregranted Darwin PAL allocation",
        ),
    ]
    if args.case:
        cases = [case for case in cases if case[0] == args.case]
    test_source = repo / f"novarocks/native-adapter/tests/{target}.rs"
    if not re.search(r"\bfn\s+" + re.escape(test) + r"\s*\(", test_source.read_text()):
        raise AssertionError(f"exact physical-exit oracle missing: {target}::{test}")
    out = Path(tempfile.mkdtemp(prefix="m07-original-socket-registration-negatives-"))
    env = os.environ.copy()
    env["CARGO_BUILD_JOBS"] = "4"
    env["CARGO_INCREMENTAL"] = "0"
    record = {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "scope": "actual Tokio registration Arc/PAL and original carrier; outer owner graph remains separate",
        "platform": sys.platform,
        "environment_overrides": {"CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0"},
        "original_sha256": {path: sha256(source) for path, source in originals.items()},
        "test_sha256": sha256(test_source.read_bytes()),
        "cases": [],
    }
    print("Evidence directory:", out, flush=True)
    try:
        for label, path, mutated, oracle in cases:
            if not all((repo / name).read_bytes() == source for name, source in originals.items()):
                raise AssertionError("exclusive unchanged production files required between cases")
            if sha256(test_source.read_bytes()) != record["test_sha256"]:
                raise AssertionError("exclusive unchanged exact-target test required")
            (out / f"{label}.diff").write_text("".join(difflib.unified_diff(
                originals[path].decode().splitlines(True), mutated.decode().splitlines(True),
                fromfile=path, tofile=path,
            )))
            command = ["cargo", "test", "--offline", "--locked", "-p", "novarocks-native-adapter",
                       "--test", target, test, "--", "--exact", "--test-threads=1"]
            log = out / f"{label}.log"
            case = {
                "name": label, "source": path, "mutation_sha256": sha256(mutated),
                "command": command, "oracle": oracle, "log": str(log),
                "verdict": "not completed",
            }
            record["cases"].append(case)
            (repo / path).write_bytes(mutated)
            try:
                with log.open("w") as output:
                    output.write("Command: " + " ".join(command) + "\n")
                    output.write("Environment overrides: CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0\n")
                    output.flush()
                    result = subprocess.run(command, cwd=repo, env=env, stdout=output,
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
                and not re.search(r"error\[E\d+\]|could not compile|signal:|SIGABRT|running 0 tests|fixture watchdog|exceeded.*watchdog", text)
            )
            if not valid:
                case["verdict"] = "rejected: exact compiled runtime FAILED required"
                raise AssertionError((label, result.returncode, str(log), case["verdict"]))
            # Require the expected concrete oracle, rather than an unrelated
            # panic in this test. Neither case mutates pending pointer safety.
            expected = (
                "actual Arc/PAL and carrier must exit before original credit release"
                if label == "ordinary-arc-retirement" else
                "all actual registration allocations must fit the original typed bound exactly"
            )
            if expected not in text:
                case["verdict"] = "rejected: expected physical-exit or constructor-layout assertion missing"
                raise AssertionError((label, str(log), expected))
            case["verdict"] = "compiled runtime FAILED; exact one test and expected physical oracle"
            print(label, case["verdict"], flush=True)
    finally:
        for path, source in originals.items():
            (repo / path).write_bytes(source)
        record["exact_restoration"] = all(
            (repo / path).read_bytes() == source for path, source in originals.items()
        )
        record["final_sha256"] = {path: sha256((repo / path).read_bytes()) for path in originals}
        (out / "verification.json").write_text(json.dumps(record, indent=2) + "\n")
        print("Exact source restoration:", record["exact_restoration"], "evidence:", out, flush=True)
        if not record["exact_restoration"]:
            raise AssertionError("byte-exact source restoration failed")


if __name__ == "__main__":
    main()

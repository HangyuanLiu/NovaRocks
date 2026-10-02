#!/usr/bin/env python3
"""Actual current private GOAWAY path and negative mutants in scratch only.

Uses the fixed-writer normal dependency manifest/lock and exact production h2
source copy; never edits product source, installs tools or accesses the network.
This is a private wire/limit oracle, not a public GOAWAY-debug producer contract,
allocator grant, whole-connection or actual deployed deadline proof.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

DRIVER = r'''// Calls actual h2 private-path helper added only in the scratch dependency.
#[cfg(test)]
mod probe {
    fn allowed(debug: usize, length_bytes: [u8; 3]) {
        let (wire, error, payload_too_big) = h2::m07_fixed_goaway_probe(debug);
        assert_eq!(error, None);
        assert!(!payload_too_big);
        assert_eq!(wire.len(), debug + 17);
        assert_eq!(&wire[..3], &length_bytes);
        assert_eq!(&wire[3..9], &[7, 0, 0, 0, 0, 0]);
        assert_eq!(&wire[9..13], &[0, 0, 0, 1]);
        assert_eq!(&wire[13..17], &[0, 0, 0, 1]);
        for (at, byte) in wire[17..].iter().enumerate() {
            assert_eq!(*byte, (at % 251) as u8, "original diagnostic changed at byte {at}");
        }
    }
    #[test]
    fn debug_zero_exact_wire() { allowed(0, [0, 0, 8]); }
    #[test]
    fn debug_16376_exact_wire_at_local_payload_limit_despite_large_peer_maximum() {
        allowed(16376, [0, 64, 0]);
    }
    #[test]
    fn debug_16377_refuses_with_actual_io_and_user_error_and_zero_wire() {
        let (wire, error, payload_too_big) = h2::m07_fixed_goaway_probe(16377);
        assert_eq!(error, Some(std::io::ErrorKind::InvalidInput));
        assert!(payload_too_big, "actual UserError::PayloadTooBig must survive io mapping");
        assert!(wire.is_empty(), "refused diagnostic must emit no partial GOAWAY");
    }
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--mutations", action="store_true")
    parser.add_argument("--log-prefix", type=Path, default=Path("/tmp/m07-fixed-writer-probe-goaway"))
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    # Reuse the already verified manifest/lock identities without importing a
    # module through pycache or changing any previous evidence source file.
    previous = evidence / "reproduce.py"
    helpers = {"__name__": "m07_fixed_writer_helpers"}
    exec(compile(previous.read_text(), str(previous), "exec"), helpers)
    root, lock = helpers["prepare"](repo, evidence, "writer")
    helpers["verify_identities"](repo, lock)
    scratch = root / "h2-source"
    shutil.copytree(repo / "vendor/h2-0.4.12", scratch)
    source_files = sorted((scratch / "src").rglob("*.rs"))
    tree = hashlib.sha256()
    for source in source_files:
        relative = source.relative_to(scratch).as_posix()
        data = source.read_bytes()
        tree.update(relative.encode() + b"\0" + data + b"\0")
        if relative in ["src/send_frame_buffer.rs", "src/codec/mod.rs", "src/codec/framed_write.rs", "src/proto/mod.rs", "src/proto/go_away.rs", "src/lib.rs"]:
            print(f"Exact current source {relative} SHA256: {hashlib.sha256(data).hexdigest()}", flush=True)
    print(f"All {len(source_files)} exact copied Rust source tree SHA256: {tree.hexdigest()}", flush=True)
    manifest = root / "Cargo.toml"
    original_path = 'h2={path=' + json.dumps(str(repo / "vendor/h2-0.4.12"))
    changed_path = 'h2={path=' + json.dumps(str(scratch))
    text = manifest.read_text()
    if text.count(original_path) != 1:
        raise SystemExit("expected exactly one normal h2 dependency path")
    manifest.write_text(text.replace(original_path, changed_path).replace("[workspace]\n", '[workspace]\nexclude=["h2-source"]\n'))
    with (scratch / "src/proto/mod.rs").open("ab") as output:
        output.write(b"\n" + (evidence / "goaway-probe.rs").read_bytes())
    with (scratch / "src/lib.rs").open("a") as output:
        output.write("\n#[doc(hidden)]\npub use crate::proto::m07_fixed_goaway_probe::run_case as m07_fixed_goaway_probe;\n")
    (root / "src/lib.rs").write_text(DRIVER)
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    command = ["cargo", "test", "--manifest-path", str(manifest), "--target-dir", str(root / "target"), "--offline", "--locked", "--", "--test-threads=1", "--nocapture"]

    def execute(name, expect_failure=False):
        log = Path(str(args.log_prefix) + f"-{name}.log")
        with log.open("w") as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, env=env)
        print(f"{name}: exit={result.returncode}; full log={log}", flush=True)
        if expect_failure:
            body = log.read_text()
            if result.returncode != 101 or "test result: FAILED" not in body:
                raise SystemExit(f"{name} did not fail through an executed test; inspect {log}")
        elif result.returncode:
            raise SystemExit(f"{name} failed; inspect {log}")

    execute("baseline")
    if args.mutations:
        writer = scratch / "src/codec/framed_write.rs"
        goaway = scratch / "src/proto/go_away.rs"
        originals = {writer: writer.read_text(), goaway: goaway.read_text()}
        guard = re.compile(r"(Frame::GoAway\(v\) => \{\n)(\s*if self\.local_max_frame_size\.is_some\(\).*?return Err\(PayloadTooBig\);\n\s*\}\n)(\s*v\.encode)", re.S)
        matches = list(guard.finditer(originals[writer]))
        if len(matches) != 1:
            raise SystemExit("cannot identify exactly one current GOAWAY local-cap guard")
        dropped_guard = guard.sub(lambda match: match[1] + match[3], originals[writer], count=1)
        caller = 'dst.buffer(frame.into())\n                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;'
        if originals[goaway].count(caller) != 1:
            raise SystemExit("cannot identify exactly one current propagated GOAWAY buffer error")
        panic_caller = originals[goaway].replace(caller, 'dst.buffer(frame.into()).expect("invalid GOAWAY frame");')
        for name, path, mutated in [("guard-removed", writer, dropped_guard), ("caller-expect-restored", goaway, panic_caller)]:
            diff = "".join(difflib.unified_diff(originals[path].splitlines(True), mutated.splitlines(True), fromfile=str(path.relative_to(scratch)), tofile=str(path.relative_to(scratch))))
            diff_path = Path(str(args.log_prefix) + f"-{name}.diff")
            diff_path.write_text(diff)
            print(f"{name} exact scratch-only diff: {diff_path}", flush=True)
            try:
                path.write_text(mutated)
                execute(name, expect_failure=True)
            finally:
                path.write_text(originals[path])
                if path.read_text() != originals[path]:
                    raise SystemExit("scratch source restoration mismatch")
        execute("restored")
    print("Product sources untouched; default writer-Cargo.lock reused; no upstream dev dependencies", flush=True)


if __name__ == "__main__":
    main()

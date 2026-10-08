"""Independent literal MySQL text-row oracle for the frozen native wire cases."""

import hashlib
import json
from pathlib import Path


def expected_small_rows():
    digest = hashlib.sha256()
    payload_bytes = 0
    for value in range(1, 200001):
        text = str(value).encode("ascii")
        payload = bytes([len(text)]) + text
        digest.update(payload)
        digest.update(len(payload).to_bytes(8, "little"))
        payload_bytes += len(payload)
    return 200000, payload_bytes, 200004, digest.hexdigest()


def expected_large_row():
    length = 16777474
    digest = hashlib.sha256(b"\xfe" + length.to_bytes(8, "little"))
    block = b"q" * 65536
    for start in range(0, length, len(block)):
        digest.update(block[: min(len(block), length - start)])
    digest.update((length + 9).to_bytes(8, "little"))
    return 1, length + 9, 6, digest.hexdigest()


if __name__ == "__main__":
    manifest = json.loads(
        (Path(__file__).parent.parent / "inputs/result-delivery-wire-boundary-v1.json").read_text()
    )
    for case, expected in zip(manifest["cases"], [expected_small_rows(), expected_large_row()], strict=True):
        actual = tuple(case[key] for key in [
            "expected_rows", "expected_row_payload_bytes", "expected_packets", "expected_row_sha256"
        ])
        assert actual == expected, case["name"]
    print("Independent wire oracles match both frozen cases")

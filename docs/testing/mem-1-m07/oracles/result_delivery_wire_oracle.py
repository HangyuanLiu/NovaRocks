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
    length = 1048576
    digest = hashlib.sha256()
    block = b"q" * 65536
    for _ in range(17):
        digest.update(b"\xfd" + length.to_bytes(3, "little"))
        for start in range(0, length, len(block)):
            digest.update(block[: min(len(block), length - start)])
    row_bytes = 17 * (length + 4)
    digest.update(row_bytes.to_bytes(8, "little"))
    return 1, row_bytes, 22, digest.hexdigest()


if __name__ == "__main__":
    manifest = json.loads(
        (Path(__file__).parent.parent / "inputs/result-delivery-wire-boundary-v4.json").read_text()
    )
    for case, expected in zip(manifest["cases"], [expected_small_rows(), expected_large_row()], strict=True):
        actual = tuple(case[key] for key in [
            "expected_rows", "expected_row_payload_bytes", "expected_packets", "expected_row_sha256"
        ])
        assert actual == expected, case["name"]
    assert manifest["cases"][0]["expected_schema"] == [{"name": "generate_series", "mysql_type": 8}]
    assert manifest["cases"][1]["expected_schema"] == [
        {"name": f"c{i}", "mysql_type": 253} for i in range(17)
    ]
    print("Independent row and column-name/type oracles match both frozen cases")

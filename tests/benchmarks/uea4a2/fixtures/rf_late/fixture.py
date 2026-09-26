#!/usr/bin/env python3
"""Generate or verify the immutable multi-row-group Iceberg RF probe input."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_GROUP = 4096
PROBE_GROUPS = 32
PROBE_ROWS = ROWS_PER_GROUP * PROBE_GROUPS
BUILD_ROWS = ((3, "Y"), (3, "N"), (4, "N"), (1005, "N"))
PROBE_FILE = "probe.parquet"
BUILD_FILE = "build.parquet"
ORACLE_FILE = "oracle.json"
MANIFEST_FILE = "manifest.json"
READY_FILE = "READY"
KIND = "uea4a2-iceberg-rf-late-v1"


def sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def field(name: str, data_type: pa.DataType, field_id: int) -> pa.Field:
    return pa.field(
        name,
        data_type,
        nullable=False,
        metadata={b"PARQUET:field_id": str(field_id).encode("ascii")},
    )


PROBE_SCHEMA = pa.schema(
    (field("id", pa.int32(), 1), field("k", pa.int32(), 2), field("payload", pa.int64(), 3))
)
BUILD_SCHEMA = pa.schema((field("k", pa.int32(), 1), field("flag", pa.string(), 2)))


def probe_key(group: int) -> int:
    return 3 if group in (0, PROBE_GROUPS - 1) else 1000 + group


def payload(row_id: int) -> int:
    # Stable, high-entropy values keep the probe data pages materially sized.
    return (row_id * 6364136223846793005 + 1442695040888963407) & ((1 << 63) - 1)


def probe_table() -> pa.Table:
    row_ids = range(PROBE_ROWS)
    return pa.Table.from_arrays(
        (
            pa.array(row_ids, type=pa.int32()),
            pa.array((probe_key(i // ROWS_PER_GROUP) for i in row_ids), type=pa.int32()),
            pa.array((payload(i) for i in row_ids), type=pa.int64()),
        ),
        schema=PROBE_SCHEMA,
    )


def build_table() -> pa.Table:
    return pa.Table.from_arrays(
        (
            pa.array((row[0] for row in BUILD_ROWS), type=pa.int32()),
            pa.array((row[1] for row in BUILD_ROWS), type=pa.string()),
        ),
        schema=BUILD_SCHEMA,
    )


def file_layout(path: Path, expected_schema: pa.Schema) -> dict:
    file = pq.ParquetFile(path)
    metadata = file.metadata
    require(file.schema_arrow.equals(expected_schema, check_metadata=True), f"schema or field IDs differ: {path}")
    groups = []
    key_index = 1 if path.name == PROBE_FILE else 0
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        key = group.column(key_index).statistics
        require(key is not None and key.has_min_max and key.null_count == 0, f"missing key statistics: {path} group {index}")
        groups.append(
            {
                "rows": group.num_rows,
                "compressed_bytes": sum(group.column(i).total_compressed_size for i in range(group.num_columns)),
                "k_min": key.min,
                "k_max": key.max,
            }
        )
    return {
        "file": path.name,
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
        "created_by": metadata.created_by,
        "rows": metadata.num_rows,
        "columns": metadata.num_columns,
        "row_groups": groups,
    }


def expected_oracle() -> dict:
    matched_ids = tuple(range(ROWS_PER_GROUP)) + tuple(
        range((PROBE_GROUPS - 1) * ROWS_PER_GROUP, PROBE_ROWS)
    )
    return {
        "probe_rows": PROBE_ROWS,
        "build_rows": len(BUILD_ROWS),
        "probe_group_count": PROBE_GROUPS,
        "probe_rows_per_group": ROWS_PER_GROUP,
        "join_sql": "SELECT COUNT(*), SUM(p.id), SUM(p.payload % 997) FROM probe p JOIN build b ON p.k = b.k WHERE b.flag = 'Y'",
        "join_count": len(matched_ids),
        "join_id_sum": sum(matched_ids),
        "join_payload_mod_997_sum": sum(payload(i) % 997 for i in matched_ids),
        "matching_probe_groups": [0, PROBE_GROUPS - 1],
        "nonmatching_probe_groups": list(range(1, PROBE_GROUPS - 1)),
    }


def verify(directory: Path) -> None:
    manifest_path = directory / MANIFEST_FILE
    manifest = json.loads(manifest_path.read_text())
    require((directory / READY_FILE).read_text().strip() == "sha256:" + sha256(manifest_path), "READY does not bind the manifest")
    require(manifest.get("schema_version") == 1 and manifest.get("fixture_kind") == KIND, "fixture identity differs")
    require(manifest.get("writer") == {"engine": "PyArrow", "version": "23.0.1"}, "writer version differs")
    require(pa.__version__ == "23.0.1", "verification requires the pinned PyArrow 23.0.1 reader")
    require(manifest.get("generator_sha256") == sha256(Path(__file__)), "generator source differs")
    oracle_path = directory / ORACLE_FILE
    require(manifest.get("oracle_sha256") == sha256(oracle_path), "oracle hash differs")
    require(json.loads(oracle_path.read_text()) == expected_oracle(), "fixed join oracle differs")
    files = manifest.get("files")
    require(isinstance(files, dict) and set(files) == {PROBE_FILE, BUILD_FILE}, "file inventory differs")
    for name, schema, count in (
        (PROBE_FILE, PROBE_SCHEMA, PROBE_GROUPS),
        (BUILD_FILE, BUILD_SCHEMA, 1),
    ):
        path = directory / name
        require(path.is_file(), f"missing {name}")
        actual = file_layout(path, schema)
        require(actual == files[name], f"Parquet layout or hash differs: {name}")
        require("parquet-cpp-arrow version 23.0.1" in actual["created_by"], f"wrong physical writer: {name}")
        require(len(actual["row_groups"]) == count, f"wrong row-group count: {name}")
        if name == PROBE_FILE:
            require(all(group["rows"] == ROWS_PER_GROUP for group in actual["row_groups"]), "probe row-group size differs")
            require(
                all(group["k_min"] == group["k_max"] == probe_key(index) for index, group in enumerate(actual["row_groups"])),
                "probe group key ranges differ",
            )
            table = pq.read_table(path)
            require(table.equals(probe_table(), check_metadata=True), "probe values differ")
        else:
            require(actual["row_groups"][0]["rows"] == len(BUILD_ROWS), "build row-group size differs")
            require(pq.read_table(path).equals(build_table(), check_metadata=True), "build values differ")
    print(f"verified {KIND}: {PROBE_GROUPS} probe row groups, {PROBE_ROWS} probe rows, {len(BUILD_ROWS)} build rows")


def generate(directory: Path) -> None:
    require(pa.__version__ == "23.0.1", "generation requires PyArrow 23.0.1")
    require(not directory.exists(), f"refusing to replace fixture directory: {directory}")
    directory.mkdir(parents=True)
    pq.write_table(
        probe_table(),
        directory / PROBE_FILE,
        version="2.6",
        compression="snappy",
        row_group_size=ROWS_PER_GROUP,
        data_page_size=4096,
        use_dictionary=["k"],
        write_page_index=True,
    )
    pq.write_table(
        build_table(),
        directory / BUILD_FILE,
        version="2.6",
        compression="snappy",
        row_group_size=len(BUILD_ROWS),
        data_page_size=4096,
        use_dictionary=["k", "flag"],
        write_page_index=True,
    )
    write_json(directory / ORACLE_FILE, expected_oracle())
    write_json(
        directory / MANIFEST_FILE,
        {
            "schema_version": 1,
            "fixture_kind": KIND,
            "writer": {"engine": "PyArrow", "version": pa.__version__},
            "generator_sha256": sha256(Path(__file__)),
            "writer_options": {
                "parquet_version": "2.6",
                "compression": "snappy",
                "data_page_size": 4096,
                "write_page_index": True,
                "probe_row_group_size": ROWS_PER_GROUP,
                "build_row_group_size": len(BUILD_ROWS),
            },
            "oracle_sha256": sha256(directory / ORACLE_FILE),
            "files": {
                PROBE_FILE: file_layout(directory / PROBE_FILE, PROBE_SCHEMA),
                BUILD_FILE: file_layout(directory / BUILD_FILE, BUILD_SCHEMA),
            },
        },
    )
    (directory / READY_FILE).write_text("sha256:" + sha256(directory / MANIFEST_FILE) + "\n")
    verify(directory)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("generate", "verify"))
    parser.add_argument("--directory", type=Path, default=Path(__file__).parent)
    arguments = parser.parse_args()
    if arguments.mode == "generate":
        generate(arguments.directory)
    else:
        verify(arguments.directory)


if __name__ == "__main__":
    main()

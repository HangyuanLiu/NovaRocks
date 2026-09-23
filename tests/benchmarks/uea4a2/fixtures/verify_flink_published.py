#!/usr/bin/env python3
"""Verify the checked-in Flink consumer corpus without producer binaries.

The producer receipt records hashes for the original Flink Parquet and Hadoop
JARs as provenance. Reproducing the write requires those JARs separately;
checking the published Parquet input does not download or execute them.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
CORPUS = ROOT / "corpus/flink"
MANIFEST = ROOT / "manifest.json"
RECEIPT = CORPUS / "flink-local-manifest.json"
DATA = CORPUS / "flink-local-1.20.5.parquet"
POSITIONS = CORPUS / "physical_ids.txt"
SQL = CORPUS / "flink-local.sql"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def bound_entry(entry: dict, relative: str) -> None:
    require(entry.get("path") == relative, f"wrong Flink inventory path: {relative}")
    path = ROOT / relative
    require(path.is_file(), f"missing published Flink attachment: {relative}")
    require(path.stat().st_size == entry.get("bytes"), f"Flink size mismatch: {relative}")
    require(digest(path) == entry.get("sha256"), f"Flink hash mismatch: {relative}")


def main() -> None:
    inventory = json.loads(MANIFEST.read_text())
    require(
        inventory.get("schema_version") == 4
        and inventory.get("fixture_kind") == "uea4a2-frozen-input-inventory-v4",
        "wrong frozen corpus inventory",
    )
    published = inventory["writers"]["flink"]
    require(published.get("writer_version") == "Flink 1.20.5", "wrong Flink inventory version")
    require(published.get("rows") == 4096, "wrong Flink inventory row count")
    bound_entry(published["receipt"], "corpus/flink/flink-local-manifest.json")
    bound_entry(published["data"], "corpus/flink/flink-local-1.20.5.parquet")
    bound_entry(published["physical_positions"], "corpus/flink/physical_ids.txt")

    receipt = json.loads(RECEIPT.read_text())
    require(
        receipt.get("schema_version") == 1
        and receipt.get("writer") == "Flink"
        and receipt.get("writer_version") == "1.20.5"
        and receipt.get("provision_method") == "official-archive-local",
        "wrong original Flink writer receipt",
    )
    require(
        receipt.get("file") == DATA.name
        and receipt.get("file_bytes") == DATA.stat().st_size
        and receipt.get("sha256") == digest(DATA)
        and receipt.get("sql_file") == SQL.name
        and receipt.get("sql_sha256") == digest(SQL),
        "Flink data or SQL differs from its original writer receipt",
    )
    require(
        receipt["distribution_archive"]["official_url"]
        == "https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz"
        and receipt["distribution_archive"]["sha512"]
        == "c846ecbcc4a1705724832acdbad27538c31f17aa494e8b69ff5f92d724574be83d1dff0b363aa0570e5a448f529224705d967d0d14df3267143eec5064a52a1f"
        and receipt["parquet_bundle"]["sha256"]
        == "6b98bbb43f6e4a721343621ec29850e0b0f19e55d9434624ffccf4cb1236f9f5"
        and receipt["hadoop_runtime"]["sha256"]
        == "492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127"
        and receipt["java"]["major_version"] == 17,
        "Flink producer provenance differs from the recorded binary versions",
    )
    require(
        receipt["parameters"]
        == {
            "runtime_mode": "batch",
            "parallelism": 1,
            "filesystem_sink": "local",
            "compression": "SNAPPY",
            "utc_timezone": True,
        },
        "Flink producer parameters changed",
    )

    metadata = pq.read_metadata(DATA)
    table = pq.read_table(DATA)
    require(
        metadata.created_by == receipt["created_by"]
        and metadata.num_row_groups == receipt["row_groups"] == 1
        and table.num_rows == receipt["rows"] == 4096
        and table.column_names == receipt["columns"],
        "Flink Parquet footer, schema, or row count changed",
    )
    require(
        table.schema.types
        == [
            pa.int64(),
            pa.int32(),
            pa.string(),
            pa.decimal128(12, 2),
            pa.timestamp("ns"),
            pa.list_(pa.int32()),
        ],
        "Flink published column types changed",
    )
    physical_ids = [int(line) for line in POSITIONS.read_text().splitlines()]
    require(
        len(physical_ids) == 4096
        and sorted(physical_ids) == list(range(4096))
        and table.column("id").to_pylist() == physical_ids,
        "Flink physical-position oracle does not match the published file",
    )
    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    for row in rows:
        value = row["id"]
        require(row["category"] == value % 17, f"Flink category mismatch at {value}")
        require(
            row["label"] == (None if value % 17 == 0 else f"label-{value % 11}"),
            f"Flink label mismatch at {value}",
        )
        require(row["amount"] == Decimal(value) / Decimal(100), f"Flink amount mismatch at {value}")
        require(row["event_time"] == datetime(2024, 1, 1), f"Flink time mismatch at {value}")
        require(row["nested"] == [value % 5, value % 7], f"Flink nested mismatch at {value}")
    oracle = hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()
    require(oracle == receipt["oracle_sha256"], "Flink row oracle differs from the writer receipt")
    print(
        f"verified published Flink consumer corpus rows=4096 sha256:{receipt['sha256']}; "
        "producer JAR hashes are provenance only"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Provision one real PyArrow Parquet writer fixture and verify corpus receipts.

Provisioning is explicit. The verifier only reads existing files and never
downloads or substitutes a writer when a corpus member is absent.
"""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

SPARK_READY_SHA256 = "bb539e37a04db8d2818d55aff784c27ed67dfdf68d28c2abe0985245c1577867"
SPARK_FILE_SHA256 = "38b09121cb7ce425fe69d627cc6d45a130066fa79192d8286265990f65dc65d6"


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def pyarrow_table():
    import pyarrow as pa

    # Multiple pages, nullable values, dictionary candidates, nested values,
    # timestamps and decimals exercise independent decoder paths.
    count = 4096
    return pa.table(
        {
            "id": pa.array(range(count), type=pa.int64()),
            "label": pa.array(
                [None if index % 17 == 0 else f"label-{index % 11}" for index in range(count)],
                type=pa.string(),
            ),
            "nested": pa.array(
                [[index % 5, index % 7] if index % 13 else None for index in range(count)],
                type=pa.list_(pa.int32()),
            ),
            "event_time": pa.array(
                [1_700_000_000_000_000 + index * 1_000 for index in range(count)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "amount": pa.array(
                [Decimal(f"{index // 100}.{index % 100:02d}") for index in range(count)],
                type=pa.decimal128(12, 2),
            ),
        }
    )


def row_oracle(path: Path) -> str:
    import pyarrow.parquet as pq

    hasher = hashlib.sha256()
    for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
        for row in batch.to_pylist():
            hasher.update(json.dumps(row, sort_keys=True, default=str).encode())
            hasher.update(b"\n")
    return hasher.hexdigest()


def register_spark(directory: Path, source_object: str) -> None:
    import pyarrow.parquet as pq

    file = directory / "spark-parquet-mr-1.17.1.parquet"
    ready = directory / "READY.json"
    manifest = directory / "spark-manifest.json"
    if manifest.exists():
        raise ValueError("Spark receipt already exists")
    if digest(ready) != SPARK_READY_SHA256 or digest(file) != SPARK_FILE_SHA256:
        raise ValueError("Spark READY or Parquet input differs from frozen source")
    published = json.loads(ready.read_text())
    if published.get("state") != "ReadyValid":
        raise ValueError("Spark source is not READY-published")
    warehouse = published["exact_warehouse"].rstrip("/")
    if not source_object.startswith(warehouse + "/ssb/lineorder/data/"):
        raise ValueError("Spark file is outside the published lineorder data prefix")
    producer = published["producer_fingerprint"]["spark_runtime"]
    if producer.get("spark_version") != "3.5.5-java17":
        raise ValueError("unexpected Spark producer version")
    metadata = pq.ParquetFile(file).metadata
    if metadata.num_rows != 832000 or "parquet-mr version 1.17.1" not in metadata.created_by:
        raise ValueError("Spark file layout differs from the audited source")
    receipt = {
        "schema_version": 1,
        "writer": "Spark/parquet-mr",
        "writer_version": "Spark 3.5.5-java17 / parquet-mr 1.17.1",
        "generator": "READY-published SSB scale 1 lineorder fixture",
        "ready_sha256": SPARK_READY_SHA256,
        "producer_fingerprint": published["producer_fingerprint"],
        "source_object": source_object,
        "file": file.name,
        "sha256": SPARK_FILE_SHA256,
        "file_bytes": file.stat().st_size,
        "rows": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "columns": metadata.num_columns,
        "created_by": metadata.created_by,
        "oracle_sha256": row_oracle(file),
    }
    manifest.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    verify(manifest)


def provision_pyarrow(output: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    output.mkdir(parents=True, exist_ok=True)
    target = output / f"pyarrow-{pa.__version__}.parquet"
    manifest = output / "pyarrow-manifest.json"
    if target.exists() or manifest.exists():
        raise ValueError("writer fixture already exists; use a fresh output directory")
    table = pyarrow_table()
    pq.write_table(
        table,
        target,
        version="2.6",
        compression="snappy",
        use_dictionary=["label"],
        data_page_size=1024,
        row_group_size=1024,
        write_page_index=True,
    )
    metadata = pq.ParquetFile(target).metadata
    receipt = {
        "schema_version": 1,
        "writer": "PyArrow",
        "writer_version": pa.__version__,
        "generator": "writer_corpus.py provision-pyarrow",
        "parameters": {
            "parquet_version": "2.6",
            "compression": "snappy",
            "dictionary_columns": ["label"],
            "data_page_size": 1024,
            "row_group_size": 1024,
            "write_page_index": True,
        },
        "file": target.name,
        "sha256": digest(target),
        "file_bytes": target.stat().st_size,
        "rows": table.num_rows,
        "row_groups": metadata.num_row_groups,
        "columns": table.column_names,
        "created_by": metadata.created_by,
        "oracle_sha256": hashlib.sha256(
            json.dumps(table.to_pylist(), sort_keys=True, default=str).encode()
        ).hexdigest(),
    }
    manifest.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    verify(manifest)


def verify(manifest: Path) -> None:
    import pyarrow.parquet as pq

    receipt = json.loads(manifest.read_text())
    if receipt.get("schema_version") != 1 or receipt.get("writer") not in {"PyArrow", "Trino", "Spark/parquet-mr"}:
        raise ValueError("unsupported writer receipt")
    if receipt["writer"] == "Trino" and (
        receipt.get("writer_version") != "483"
        or receipt.get("image_digest")
        != "sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
        or "parquet-mr-trino version 483" not in receipt.get("created_by", "")
    ):
        raise ValueError("Trino receipt does not name the pinned writer")
    if receipt["writer"] == "PyArrow" and (
        receipt.get("writer_version") != "23.0.1"
        or "parquet-cpp-arrow version 23.0.1" not in receipt.get("created_by", "")
    ):
        raise ValueError("PyArrow receipt does not name the pinned writer")
    if receipt["writer"] == "Spark/parquet-mr":
        if (
            receipt.get("ready_sha256") != SPARK_READY_SHA256
            or receipt.get("sha256") != SPARK_FILE_SHA256
            or digest(manifest.parent / "READY.json") != SPARK_READY_SHA256
        ):
            raise ValueError("Spark receipt does not match the published source")
    filename = receipt.get("file")
    if not isinstance(filename, str) or Path(filename).name != filename:
        raise ValueError("writer receipt file must be one local basename")
    target = manifest.parent / filename
    if not target.is_file() or digest(target) != receipt.get("sha256"):
        raise ValueError("writer fixture missing or hash mismatch")
    metadata = pq.ParquetFile(target).metadata
    table = None if receipt["writer"] == "Spark/parquet-mr" else pq.read_table(target)
    if (
        metadata.created_by != receipt.get("created_by")
        or metadata.num_row_groups != receipt.get("row_groups")
        or metadata.num_rows != receipt.get("rows")
        or (table is not None and table.column_names != receipt.get("columns"))
        or (table is None and metadata.num_columns != receipt.get("columns"))
    ):
        raise ValueError("writer fixture layout mismatch")
    oracle = row_oracle(target) if table is None else hashlib.sha256(
        json.dumps(table.to_pylist(), sort_keys=True, default=str).encode()
    ).hexdigest()
    if oracle != receipt.get("oracle_sha256"):
        raise ValueError("writer fixture row oracle mismatch")
    print(f"verified {target} sha256:{receipt['sha256']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    provision = commands.add_parser("provision-pyarrow")
    provision.add_argument("--output-dir", type=Path, required=True)
    checked = commands.add_parser("verify")
    checked.add_argument("--manifest", type=Path, required=True)
    spark = commands.add_parser("register-spark")
    spark.add_argument("--input-dir", type=Path, required=True)
    spark.add_argument("--source-object", required=True)
    args = parser.parse_args()
    if args.command == "provision-pyarrow":
        provision_pyarrow(args.output_dir)
    elif args.command == "register-spark":
        register_spark(args.input_dir, args.source_object)
    else:
        verify(args.manifest)


if __name__ == "__main__":
    main()

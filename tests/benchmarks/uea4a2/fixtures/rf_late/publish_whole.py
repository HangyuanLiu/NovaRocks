#!/usr/bin/env python3
"""Publish the frozen RF probe as one Iceberg whole-file split, then verify it."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

import pyarrow.parquet as pq
from pyiceberg.manifest import DataFile
from pyiceberg.table import TableProperties, _parquet_files_to_data_files

from fixture import MANIFEST_FILE, PROBE_FILE, READY_FILE, sha256, verify
from publish import DEFAULT_NAMESPACE, TABLES, catalog_from_environment

TABLE_NAME = "probe_whole_v1"
SOURCE_TABLE_NAME = "probe_v1"
EXPECTED_ROWS = 131072
EXPECTED_ROW_GROUPS = 32


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def remote_sha256(table, object_uri: str) -> str:
    digest = hashlib.sha256()
    with table.io.new_input(object_uri).open() as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def physical_layout(table, object_uri: str) -> tuple[int, int]:
    with table.io.new_input(object_uri).open() as source:
        metadata = pq.read_metadata(source)
    return metadata.num_rows, metadata.num_row_groups


def source_record(catalog, namespace: str, local_file: Path) -> dict:
    table = catalog.load_table((namespace, SOURCE_TABLE_NAME))
    rows = table.inspect.files().to_pylist()
    require(len(rows) == 1, "source probe table must have exactly one current DataFile")
    item = rows[0]
    require(item["file_path"].startswith(table.location().rstrip("/") + "/data/"), "source DataFile escaped its table")
    require(item["record_count"] == EXPECTED_ROWS, "source probe row count differs")
    require(item["file_size_in_bytes"] == local_file.stat().st_size, "source probe file size differs")
    require(len(item["split_offsets"] or []) == EXPECTED_ROW_GROUPS, "source probe split offsets differ")
    require(remote_sha256(table, item["file_path"]) == sha256(local_file), "source probe object hash differs")
    require(physical_layout(table, item["file_path"]) == (EXPECTED_ROWS, EXPECTED_ROW_GROUPS), "source probe physical layout differs")
    return {
        "table": SOURCE_TABLE_NAME,
        "snapshot_id": table.metadata.current_snapshot_id,
        "object_uri": item["file_path"],
        "object_sha256": sha256(local_file),
        "split_offset_count": EXPECTED_ROW_GROUPS,
    }


def clone_without_split_offsets(source: DataFile) -> DataFile:
    require(len(source.split_offsets or []) == EXPECTED_ROW_GROUPS, "Parquet importer did not find 32 row groups")
    clone = DataFile.from_args(
        content=source.content,
        file_path=source.file_path,
        file_format=source.file_format,
        partition=source.partition,
        record_count=source.record_count,
        file_size_in_bytes=source.file_size_in_bytes,
        column_sizes=source.column_sizes,
        value_counts=source.value_counts,
        null_value_counts=source.null_value_counts,
        nan_value_counts=source.nan_value_counts,
        lower_bounds=source.lower_bounds,
        upper_bounds=source.upper_bounds,
        key_metadata=source.key_metadata,
        split_offsets=None,
        equality_ids=source.equality_ids,
        sort_order_id=source.sort_order_id,
    )
    require(clone.split_offsets is None, "whole-file DataFile still has split offsets")
    require(clone.record_count == EXPECTED_ROWS, "whole-file DataFile row count differs")
    return clone


def whole_record(catalog, namespace: str, local_file: Path) -> dict:
    table = catalog.load_table((namespace, TABLE_NAME))
    rows = table.inspect.files().to_pylist()
    require(len(rows) == 1, "whole-file table must have exactly one current DataFile")
    item = rows[0]
    object_uri = table.location().rstrip("/") + "/data/" + PROBE_FILE
    require(item["file_path"] == object_uri, "whole-file DataFile path differs")
    require(item["split_offsets"] is None, "Iceberg manifest retained split offsets")
    require(item["record_count"] == EXPECTED_ROWS, "whole-file Iceberg row count differs")
    require(item["file_size_in_bytes"] == local_file.stat().st_size, "whole-file Iceberg byte count differs")
    require(remote_sha256(table, object_uri) == sha256(local_file), "whole-file object hash differs")
    require(physical_layout(table, object_uri) == (EXPECTED_ROWS, EXPECTED_ROW_GROUPS), "whole-file Parquet layout differs")
    require(table.metadata.current_snapshot_id is not None, "whole-file table has no current snapshot")
    return {
        "table": TABLE_NAME,
        "location": table.location(),
        "snapshot_id": table.metadata.current_snapshot_id,
        "object_uri": object_uri,
        "object_sha256": sha256(local_file),
        "object_bytes": local_file.stat().st_size,
        "record_count": EXPECTED_ROWS,
        "physical_row_groups": EXPECTED_ROW_GROUPS,
        "iceberg_split_offsets": None,
    }


def append_whole_file(table, object_uri: str) -> None:
    with table.transaction() as transaction:
        if transaction.table_metadata.name_mapping() is None:
            transaction.set_properties(
                **{TableProperties.DEFAULT_NAME_MAPPING: transaction.table_metadata.schema().name_mapping.model_dump_json()}
            )
        files = list(
            _parquet_files_to_data_files(
                table_metadata=transaction.table_metadata,
                file_paths=[object_uri],
                io=table.io,
            )
        )
        require(len(files) == 1, "Parquet importer returned an unexpected DataFile count")
        whole = clone_without_split_offsets(files[0])
        with transaction.update_snapshot(snapshot_properties={"uea4a2-fixture": "rf-late-whole-v1"}).fast_append() as append:
            append.append_data_file(whole)


def write_receipt_once(path: Path, receipt: dict) -> None:
    require(not path.exists(), f"refusing to replace publication receipt: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".rf-whole-", delete=False) as temporary:
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    try:
        os.link(temporary_path, path)
    finally:
        temporary_path.unlink()


def publish(directory: Path, receipt_path: Path, namespace: str) -> None:
    verify(directory)
    require(not receipt_path.exists(), f"refusing to replace publication receipt: {receipt_path}")
    local_file = directory / PROBE_FILE
    catalog = catalog_from_environment()
    require(catalog.namespace_exists(namespace), "RF fixture namespace is absent")
    require(not catalog.table_exists((namespace, TABLE_NAME)), f"refusing to mutate existing {namespace}.{TABLE_NAME}")
    source = source_record(catalog, namespace, local_file)
    table = catalog.create_table(
        (namespace, TABLE_NAME), schema=TABLES[SOURCE_TABLE_NAME][1], properties={"format-version": "2"}
    )
    object_uri = table.location().rstrip("/") + "/data/" + PROBE_FILE
    with local_file.open("rb") as source_file, table.io.new_output(object_uri).create(overwrite=False) as destination:
        shutil.copyfileobj(source_file, destination, length=1024 * 1024)
    require(remote_sha256(table, object_uri) == sha256(local_file), "uploaded object hash differs")
    append_whole_file(table, object_uri)
    whole = whole_record(catalog, namespace, local_file)
    receipt = {
        "schema_version": 1,
        "namespace": namespace,
        "rest_uri": os.environ["NOVAROCKS_ICEBERG_REST_URI"],
        "warehouse": os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"],
        "local_manifest_sha256": sha256(directory / MANIFEST_FILE),
        "local_ready": (directory / READY_FILE).read_text().strip(),
        "source": source,
        "whole": whole,
    }
    write_receipt_once(receipt_path, receipt)
    print(f"published {namespace}.{TABLE_NAME} snapshot={whole['snapshot_id']} with no split offsets")


def verify_published(directory: Path, receipt_path: Path) -> None:
    verify(directory)
    receipt = json.loads(receipt_path.read_text())
    require(receipt.get("schema_version") == 1, "publication receipt version differs")
    require(receipt.get("rest_uri") == os.environ["NOVAROCKS_ICEBERG_REST_URI"], "REST endpoint differs")
    require(receipt.get("warehouse") == os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"], "REST warehouse differs")
    require(receipt.get("local_manifest_sha256") == sha256(directory / MANIFEST_FILE), "local manifest differs")
    require(receipt.get("local_ready") == (directory / READY_FILE).read_text().strip(), "local READY differs")
    catalog = catalog_from_environment()
    namespace = receipt["namespace"]
    local_file = directory / PROBE_FILE
    require(source_record(catalog, namespace, local_file) == receipt.get("source"), "source Iceberg snapshot differs")
    require(whole_record(catalog, namespace, local_file) == receipt.get("whole"), "whole-file Iceberg snapshot differs")
    print(f"verified {namespace}.{TABLE_NAME}: 32 physical row groups, one Iceberg DataFile, no split offsets")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("publish", "verify-published"))
    parser.add_argument("--directory", type=Path, default=Path(__file__).parent)
    parser.add_argument("--receipt", type=Path, default=Path(__file__).parent / "whole_published.json")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    arguments = parser.parse_args()
    if arguments.mode == "publish":
        publish(arguments.directory, arguments.receipt, arguments.namespace)
    else:
        verify_published(arguments.directory, arguments.receipt)


if __name__ == "__main__":
    main()

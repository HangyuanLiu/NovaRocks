#!/usr/bin/env python3
"""Publish a checked, immutable short Iceberg fixture from the task's Spark table."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

IDENTITY = ("uea4a2_short_20260923", "short_v1")
KIND = "uea4a2-iceberg-short-v1"


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def command(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()


def provision(output: Path, spark_image_id: str) -> None:
    if output.exists():
        raise ValueError(f"refusing to replace existing fixture: {output}")
    endpoint = os.environ["AWS_S3_ENDPOINT"]
    access = os.environ["AWS_S3_ACCESS_KEY_ID"]
    secret = os.environ["AWS_S3_SECRET_ACCESS_KEY"]
    rest = os.environ["NOVAROCKS_ICEBERG_REST_URI"]
    warehouse = os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"]
    catalog = RestCatalog("task", uri=rest, warehouse=warehouse, **{
        "s3.endpoint": endpoint, "s3.access-key-id": access,
        "s3.secret-access-key": secret, "s3.region": "us-east-1",
    })
    table = catalog.load_table(IDENTITY)
    suffix = "/" + "/".join(IDENTITY)
    if not table.location().endswith(suffix):
        raise ValueError("short Iceberg table location does not match its namespace and name")
    actual_warehouse = table.location().removesuffix(suffix)
    current = table.inspect.files().to_pylist()
    if len(current) != 1 or current[0]["record_count"] != 4096:
        raise ValueError("short Iceberg table must have exactly one current 4096-row data file")
    object_uri = current[0]["file_path"]
    if not object_uri.startswith(table.location() + "/data/") or not object_uri.endswith(".parquet"):
        raise ValueError("current data file is outside the exact Iceberg table")
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="uea4a2-mc-") as config:
        mc = ["mc", "--config-dir", config]
        command(*mc, "alias", "set", "task", endpoint, access, secret)
        remote = "task/" + object_uri.removeprefix("s3://")
        object_stat = json.loads(command(*mc, "stat", "--json", remote))
        command(*mc, "cp", remote, str(output / "data.parquet"))
    rows = pq.read_table(output / "data.parquet").to_pylist()
    if len(rows) != 4096 or sorted(rows, key=lambda row: row["id"]) != [
        {"id": number, "value": number * 3} for number in range(4096)
    ]:
        raise ValueError("Spark short Iceberg rows differ from their exact oracle")
    oracle = {
        "row_count": 4096, "min_id": 0, "max_id": 4095,
        "id_sum": sum(range(4096)), "value_sum": 3 * sum(range(4096)),
    }
    write_json(output / "oracle.json", oracle)
    write_json(output / "objects.json", [{
        "key": object_uri.removeprefix(table.location() + "/"),
        "size": int(object_stat["size"]),
        "etag": object_stat["etag"],
        "sha256": digest(output / "data.parquet"),
    }])
    source = Path(__file__).parent
    for name in ("provision_short_iceberg.sql", "compact_short_iceberg.sql"):
        shutil.copyfile(source / name, output / name)
    artifacts = {name: digest(output / name) for name in (
        "data.parquet", "oracle.json", "objects.json",
        "provision_short_iceberg.sql", "compact_short_iceberg.sql",
    )}
    manifest = {
        "schema_version": 1, "fixture_kind": KIND,
        "database": IDENTITY[0], "table": IDENTITY[1],
        "table_location": table.location(), "warehouse_uri": actual_warehouse,
        "rest_uri": rest, "s3_endpoint": endpoint, "region": "us-east-1",
        "credential_name": "iceberg-test-data", "credential_generation": "v1",
        "snapshot_id": table.metadata.current_snapshot_id,
        "data_file_count": 1, "row_count": 4096,
        "objects_sha256": artifacts["objects.json"],
        "writer": {"engine": "Spark", "version": "3.5.5", "image_id": spark_image_id},
        "artifacts": artifacts,
    }
    write_json(output / "manifest.json", manifest)
    (output / "READY").write_text("sha256:" + digest(output / "manifest.json") + "\n")
    verify(output)


def verify(output: Path) -> None:
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if ((output / "READY").read_text().strip() != "sha256:" + digest(manifest_path)
            or manifest.get("schema_version") != 1 or manifest.get("fixture_kind") != KIND
            or manifest.get("data_file_count") != 1 or manifest.get("row_count") != 4096):
        raise ValueError("short Iceberg fixture READY or identity mismatch")
    for name, expected in manifest["artifacts"].items():
        if Path(name).name != name or digest(output / name) != expected:
            raise ValueError(f"short Iceberg artifact hash mismatch: {name}")
    oracle = json.loads((output / "oracle.json").read_text())
    rows = pq.read_table(output / "data.parquet").to_pylist()
    if (oracle != {"row_count": 4096, "min_id": 0, "max_id": 4095,
                   "id_sum": 8386560, "value_sum": 25159680}
            or sorted(rows, key=lambda row: row["id"]) != [
                {"id": number, "value": number * 3} for number in range(4096)
            ]):
        raise ValueError("short Iceberg row oracle mismatch")
    inventory = json.loads((output / "objects.json").read_text())
    if (len(inventory) != 1 or inventory[0]["size"] != (output / "data.parquet").stat().st_size
            or inventory[0]["sha256"] != digest(output / "data.parquet")):
        raise ValueError("short Iceberg object inventory mismatch")
    print(f"verified {KIND} rows=4096 file={inventory[0]['key']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("provision", "verify"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--spark-image-id", help="required for provision")
    args = parser.parse_args()
    if args.mode == "provision":
        if not args.spark_image_id or not args.spark_image_id.startswith("sha256:"):
            parser.error("provision requires --spark-image-id sha256:...")
        provision(args.output, args.spark_image_id)
    else:
        verify(args.output)


if __name__ == "__main__":
    main()

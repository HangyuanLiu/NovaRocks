#!/usr/bin/env python3
"""Publish a checked, immutable short Iceberg fixture from the task's Spark table."""

from __future__ import annotations

import argparse
import importlib.util
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import sys
from urllib.parse import quote, urlencode
from urllib.request import urlopen

import pyarrow.parquet as pq

KIND = "uea4a2-iceberg-short-v1"


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def fixture_support():
    path = Path(__file__).resolve().parents[4] / "docker/paimon-read/fixture.py"
    spec = importlib.util.spec_from_file_location("short_fixture_support", path)
    if spec is None or spec.loader is None:
        raise ValueError("the existing fixture support is unavailable")
    support = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = support
    spec.loader.exec_module(support)
    return support


def rest_json(url: str):
    with urlopen(url, timeout=30) as response:
        return json.load(response)


def provision(output: Path, spark_image_id: str, database: str, table_name: str, spark_output: Path) -> None:
    if output.exists():
        raise ValueError(f"refusing to replace existing fixture: {output}")
    identity = (database, table_name)
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in identity):
        raise ValueError("short fixture database and table must be explicit SQL identifiers")
    endpoint = os.environ["AWS_S3_ENDPOINT"]
    rest = os.environ["NOVAROCKS_ICEBERG_REST_URI"]
    warehouse = os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"]
    support = fixture_support()
    env_file = Path(os.environ["NOVA_ENV_REST_ENV_FILE"]).resolve(strict=True)
    runtime = support.load_runtime(env_file)
    if runtime.minio_endpoint_host != endpoint:
        raise ValueError("short publisher endpoint differs from its fixed environment publication")
    config = rest_json(rest.rstrip("/") + "/v1/config?" + urlencode({"warehouse": warehouse}))
    properties = dict(config.get("defaults", {}))
    properties.update(config.get("overrides", {}))
    prefix = properties.get("prefix", "")
    prefix_path = "/" + quote(prefix, safe="") if prefix else ""
    loaded = rest_json(rest.rstrip("/") + "/v1" + prefix_path +
                       "/namespaces/" + quote(database, safe="") +
                       "/tables/" + quote(table_name, safe=""))
    metadata = loaded["metadata"]
    location = metadata["location"]
    suffix = "/" + "/".join(identity)
    if not location.endswith(suffix):
        raise ValueError("short Iceberg table location does not match its namespace and name")
    actual_warehouse = location.removesuffix(suffix)
    current_snapshot = metadata["current-snapshot-id"]
    if not any(snapshot["snapshot-id"] == current_snapshot for snapshot in metadata["snapshots"]):
        raise ValueError("REST metadata does not contain the current short snapshot")
    text = spark_output.read_text()
    records = [json.loads(line.removeprefix("NR_SHORT_FILE|"))
               for line in text.splitlines() if line.startswith("NR_SHORT_FILE|")]
    if len(records) != 1 or records[0]["record_count"] != 4096:
        raise ValueError("official Spark files receipt must contain one current 4096-row data file")
    object_uri = records[0]["file_path"]
    if not object_uri.startswith(location + "/data/") or not object_uri.endswith(".parquet"):
        raise ValueError("current data file is outside the exact Iceberg table")
    object_stat = json.loads(support.compose_mc(runtime,
        'set -eu\n/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null\n/usr/bin/mc stat --json "$1"',
        "minio/" + object_uri.removeprefix("s3://")))
    data = support.compose_mc_bytes(runtime,
        'set -eu\n/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null\n/usr/bin/mc cat "$1"',
        "minio/" + object_uri.removeprefix("s3://"))
    if len(data) != object_stat["size"] or len(data) != records[0]["file_size_in_bytes"]:
        raise ValueError("short data bytes differ from their official files receipt and object stat")
    output.mkdir(parents=True)
    (output / "data.parquet").write_bytes(data)
    shutil.copyfile(spark_output, output / "writer.log")
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
        "key": object_uri.removeprefix(location + "/"),
        "size": int(object_stat["size"]),
        "etag": object_stat["etag"],
        "sha256": digest(output / "data.parquet"),
    }])
    source = Path(__file__).resolve().parents[4] / "novarocks/fs/tests/data/writer-corpus/spark"
    for name in ("provision_short_iceberg.sql", "compact_short_iceberg.sql"):
        shutil.copyfile(source / name, output / name)
    artifacts = {name: digest(output / name) for name in (
        "data.parquet", "oracle.json", "objects.json",
        "provision_short_iceberg.sql", "compact_short_iceberg.sql", "writer.log",
    )}
    manifest = {
        "schema_version": 1, "fixture_kind": KIND,
        "database": database, "table": table_name,
        "table_location": location, "warehouse_uri": actual_warehouse,
        "rest_uri": rest, "s3_endpoint": endpoint, "region": "us-east-1",
        "credential_name": "iceberg-test-data", "credential_generation": "v1",
        "snapshot_id": current_snapshot,
        "run_context": json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"]) if os.environ.get("NOVA_FIXTURE_RUN_CONTEXT") not in (None, "null") else None,
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
    parser.add_argument("--database", help="explicit prepared namespace, required for provision")
    parser.add_argument("--table", help="explicit prepared table, required for provision")
    parser.add_argument("--spark-output", type=Path, help="official Spark files output, required for provision")
    args = parser.parse_args()
    if args.mode == "provision":
        if not args.spark_image_id or not args.spark_image_id.startswith("sha256:"):
            parser.error("provision requires --spark-image-id sha256:...")
        if not args.database or not args.table or not args.spark_output:
            parser.error("provision requires explicit --database, --table and --spark-output")
        provision(args.output, args.spark_image_id, args.database, args.table, args.spark_output)
    else:
        verify(args.output)


if __name__ == "__main__":
    main()

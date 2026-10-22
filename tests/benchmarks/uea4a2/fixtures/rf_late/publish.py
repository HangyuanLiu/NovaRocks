#!/usr/bin/env python3
"""Explicitly publish the verified RF files as two immutable Iceberg tables."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import sys

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

from fixture import BUILD_FILE, MANIFEST_FILE, PROBE_FILE, sha256, verify, write_json

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO / "docker/iceberg-rest"))
import fixture_run
from scratch import namespace_run

RUN = None

DEFAULT_NAMESPACE = "uea4a2_rf_late_20260923"
TABLES = {
    "probe_v1": (
        PROBE_FILE,
        Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="k", field_type=IntegerType(), required=True),
            NestedField(field_id=3, name="payload", field_type=LongType(), required=True),
        ),
    ),
    "build_v1": (
        BUILD_FILE,
        Schema(
            NestedField(field_id=1, name="k", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="flag", field_type=StringType(), required=True),
        ),
    ),
}


def catalog_from_environment() -> RestCatalog:
    return RestCatalog(
        "uea4a2-rf-late",
        uri=os.environ["NOVAROCKS_ICEBERG_REST_URI"],
        warehouse=os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"],
        **{
            "s3.endpoint": os.environ["AWS_S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_S3_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            "s3.region": "us-east-1",
        },
    )


def published_table_record(catalog: RestCatalog, namespace: str, table_name: str, local_file: Path) -> dict:
    table = catalog.load_table((namespace, table_name))
    files = table.inspect.files().to_pylist()
    if len(files) != 1:
        raise ValueError(f"{table_name} must have exactly one current Iceberg data file")
    data = files[0]
    uri = table.location().rstrip("/") + "/data/" + local_file.name
    if data["file_path"] != uri or int(data["record_count"]) != (131072 if table_name == "probe_v1" else 4):
        raise ValueError(f"{table_name} current snapshot differs from fixed RF fixture")
    if int(data["file_size_in_bytes"]) != local_file.stat().st_size:
        raise ValueError(f"{table_name} Iceberg file size differs from local fixture")
    remote = table.io.new_input(uri)
    import hashlib

    digest = hashlib.sha256()
    with remote.open() as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != sha256(local_file):
        raise ValueError(f"{table_name} published object hash differs from local fixture")
    return {
        "table": table_name,
        "location": table.location(),
        "snapshot_id": table.metadata.current_snapshot_id,
        "object_uri": uri,
        "object_sha256": digest.hexdigest(),
        "object_bytes": local_file.stat().st_size,
        "record_count": int(data["record_count"]),
    }


def publish(directory: Path, receipt_path: Path, namespace: str) -> None:
    verify(directory)
    if receipt_path.exists():
        raise ValueError(f"refusing to replace publication receipt: {receipt_path}")
    if RUN:
        fixture_run.startup_sweep(Path(os.environ["NOVA_ENV_REST_ENV_FILE"]), budget=RUN)
    catalog = catalog_from_environment()
    if catalog.namespace_exists(namespace):
        for table_name in TABLES:
            if catalog.table_exists((namespace, table_name)):
                raise ValueError(f"refusing to mutate existing table: {namespace}.{table_name}")
    else:
        catalog.create_namespace(namespace, properties={})
    records = []
    for table_name, (filename, schema) in TABLES.items():
        location = RUN.identity.scope_root + "/rest/" + namespace + "/" + table_name if RUN else None
        table = catalog.create_table((namespace, table_name), schema=schema, location=location, properties={"format-version": "2"})
        if RUN and table.location() != location:
            raise ValueError("actual RF table location differs from the original scratch run")
        local_file = directory / filename
        object_uri = table.location().rstrip("/") + "/data/" + filename
        with local_file.open("rb") as source, table.io.new_output(object_uri).create(overwrite=False) as destination:
            shutil.copyfileobj(source, destination, length=1024 * 1024)
        table.add_files([object_uri])
        records.append(published_table_record(catalog, namespace, table_name, local_file))
    write_json(
        receipt_path,
        {
            "schema_version": 1,
            "namespace": namespace,
            "run_context": RUN.as_dict() if RUN else None,
            "run_storage": json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"]) if RUN else None,
            "local_manifest_sha256": sha256(directory / MANIFEST_FILE),
            "rest_uri": os.environ["NOVAROCKS_ICEBERG_REST_URI"],
            "warehouse": os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"],
            "tables": records,
        },
    )
    print(f"published {namespace}: " + ", ".join(f"{item['table']}@{item['snapshot_id']}" for item in records))


def verify_published(directory: Path, receipt_path: Path) -> None:
    verify(directory)
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("schema_version") != 1 or receipt.get("local_manifest_sha256") != sha256(directory / MANIFEST_FILE):
        raise ValueError("publication receipt does not bind the current local fixture")
    if receipt.get("rest_uri") != os.environ["NOVAROCKS_ICEBERG_REST_URI"]:
        raise ValueError("REST endpoint differs from publication receipt")
    if receipt.get("warehouse") != os.environ["NOVAROCKS_ICEBERG_REST_WAREHOUSE"]:
        raise ValueError("REST warehouse differs from publication receipt")
    catalog = catalog_from_environment()
    namespace = receipt["namespace"]
    actual = [
        published_table_record(catalog, namespace, table_name, directory / filename)
        for table_name, (filename, _) in TABLES.items()
    ]
    if actual != receipt.get("tables"):
        raise ValueError("published snapshot or object differs from publication receipt")
    print(f"verified published Iceberg RF fixture {namespace}")


def admit_run(seconds: int = 3600):
    global RUN
    _, _, RUN = fixture_run.admit(Path(os.environ["NOVA_ENV_REST_ENV_FILE"]), seconds)
    if RUN:
        os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(RUN.as_dict(), sort_keys=True)
        def expired(number, frame):
            raise TimeoutError("original fixture work deadline expired")
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(0.001, RUN.remaining_seconds(work=True)))
    return RUN


def writer_namespace(namespace: str | None) -> str:
    if RUN:
        namespace = namespace or RUN.identity.resource_name("rf")
        if namespace_run(namespace) != RUN.identity.run_id:
            raise ValueError("shared writer namespace must belong to the original scratch run")
    return namespace or DEFAULT_NAMESPACE


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("publish", "verify-published"))
    parser.add_argument("--directory", type=Path, default=Path(__file__).parent)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--namespace")
    parser.add_argument("--fixture-budget-secs", type=int, default=3600)
    arguments = parser.parse_args()
    admit_run(arguments.fixture_budget_secs)
    if arguments.mode == "publish":
        arguments.namespace = writer_namespace(arguments.namespace)
    arguments.namespace = arguments.namespace or DEFAULT_NAMESPACE
    if arguments.mode == "publish":
        publish(arguments.directory, arguments.receipt, arguments.namespace)
    else:
        verify_published(arguments.directory, arguments.receipt)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Verify copied A4 and SSB baseline receipts without changing their sources.

Offline mode checks checked-in provenance and oracles. Online mode additionally
reads the exact MinIO keys and current Iceberg manifests; it never writes S3.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
A4_MANIFEST_SHA256 = "827bfc2ab08d16bb648d1229cadb519d0453b9d0aab591709b20d95fd42bfe39"
SSB_READY_SHA256 = "bb539e37a04db8d2818d55aff784c27ed67dfdf68d28c2abe0985245c1577867"
SSB_PUBLISHED_MANIFEST_SHA256 = "b9c59f30669a17c458d17eb74b48521acd8a13e5464da603c4ce55bedf1ea06c"
SSB_SELECTED_FILE_SHA256 = "38b09121cb7ce425fe69d627cc6d45a130066fa79192d8286265990f65dc65d6"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def verify_offline(ssb_file: Path | None) -> dict:
    receipt = read_json(ROOT / "baseline.json")
    require(receipt["schema_version"] == 1, "baseline schema version changed")
    require(receipt["fixture_kind"] == "uea4a2-existing-iceberg-baselines-v1", "baseline kind changed")
    for relative, pin in receipt["files"].items():
        path = ROOT / relative
        require(path.is_file(), f"baseline attachment missing: {relative}")
        require(path.stat().st_size == pin["bytes"], f"baseline attachment size changed: {relative}")
        require(file_sha(path) == pin["sha256"], f"baseline attachment hash changed: {relative}")

    a4 = read_json(ROOT / "a4/manifest.json")
    a4_receipt = receipt["a4_small_files"]
    require(file_sha(ROOT / "a4/manifest.json") == A4_MANIFEST_SHA256, "A4 published manifest changed")
    require((ROOT / "a4/READY").read_text().strip() == f"sha256:{A4_MANIFEST_SHA256}", "A4 READY changed")
    require(a4["table_location"] == a4_receipt["table_location"], "A4 table location changed")
    require(a4["snapshot_id"] == a4_receipt["snapshot_id"], "A4 snapshot changed")
    require(a4["schema_id"] == a4_receipt["schema_id"], "A4 schema changed")
    require(a4["objects_sha256"] == file_sha(ROOT / "a4/objects.json"), "A4 inventory hash changed")
    for relative, digest in a4["artifacts"].items():
        require(file_sha(ROOT / "a4" / relative) == digest, f"A4 published artifact changed: {relative}")
    objects = read_json(ROOT / "a4/objects.json")
    data = [item for item in objects if item["key"].startswith("data/") and item["key"].endswith(".parquet")]
    require(len(objects) == a4_receipt["object_count"] == 313, "A4 object count changed")
    require(len(data) == a4_receipt["data_file_count"] == 240, "A4 data file count changed")
    require(sum(item["size"] for item in data) == a4_receipt["data_file_bytes"] == 176234, "A4 data bytes changed")
    require(min(item["size"] for item in data) == 733 and max(item["size"] for item in data) == 737, "A4 small file sizes changed")
    oracle = read_json(ROOT / "a4/oracle.json")
    require({key: oracle[key] for key in a4_receipt["oracle"]} == a4_receipt["oracle"], "A4 oracle changed")
    require(a4_receipt["oracle_sql"] == "SELECT COUNT(*) AS row_count, SUM(id) AS id_sum, SUM(value) AS value_sum FROM ${table}", "A4 SQL oracle changed")

    ssb = receipt["ssb_wide_row_group"]
    require(file_sha(ROOT / "ssb/READY.json") == SSB_READY_SHA256, "SSB READY changed")
    require(file_sha(ROOT / "ssb/published-manifest.txt") == SSB_PUBLISHED_MANIFEST_SHA256, "SSB published manifest changed")
    ready = read_json(ROOT / "ssb/READY.json")
    published = read_json(ROOT / "ssb/published-manifest.txt")
    selected = read_json(ROOT / "ssb/spark-manifest.json")
    entries = [item for item in published["tables"] if item["name"] == "lineorder"]
    require(len(entries) == 1, "SSB published lineorder table is ambiguous")
    line = entries[0]
    require(ready["state"] == "ReadyValid" and ready["publication"]["ready_uri"] == ssb["ready_uri"], "SSB publication changed")
    require(published["warehouse"] == ssb["warehouse"], "SSB warehouse changed")
    require(line["metadata_uri"] == ssb["table_metadata_uri"], "SSB metadata pointer changed")
    require(int(line["snapshot_id"]) == ssb["snapshot_id"], "SSB snapshot changed")
    require(int(line["rows"]) == ssb["table_count_oracle"] == 6001171, "SSB table count changed")
    require(selected["source_object"] == ssb["selected_data_object"], "SSB selected file changed")
    require(selected["sha256"] == ssb["selected_data_sha256"] == SSB_SELECTED_FILE_SHA256, "SSB selected file hash changed")
    require(selected["rows"] == ssb["selected_data_rows"] == 832000, "SSB selected file rows changed")
    require(selected["row_groups"] == ssb["selected_data_row_groups"] == 1, "SSB selected file row groups changed")
    require(selected["columns"] == ssb["selected_data_columns"] == 17, "SSB selected file columns changed")
    require((ROOT / "ssb/q1.1.result").read_text().splitlines() == ["revenue", "219159726134"], "SSB Q1.1 oracle changed")
    require(ssb["published_ssb_query_revenue_oracle"] == 219159726134, "SSB published SQL oracle changed")
    if ssb_file is not None:
        import pyarrow.parquet as pq

        require(file_sha(ssb_file) == SSB_SELECTED_FILE_SHA256, "local SSB file hash changed")
        metadata = pq.read_metadata(ssb_file)
        require(metadata.num_rows == 832000 and metadata.num_row_groups == 1 and metadata.num_columns == 17, "local SSB Parquet layout changed")
        largest = max(metadata.row_group(0).column(index).total_compressed_size for index in range(17))
        require(largest == ssb["selected_max_column_chunk_compressed_bytes"] == 2954257, "local SSB column chunk changed")
    return receipt


def verify_online(receipt: dict) -> None:
    import fastavro
    from minio import Minio

    endpoint = urlsplit(os.environ["AWS_S3_ENDPOINT"])
    client = Minio(
        endpoint.netloc,
        access_key=os.environ["AWS_S3_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
        secure=endpoint.scheme == "https",
    )

    def object_bytes(uri: str) -> bytes:
        parsed = urlsplit(uri.replace("s3a://", "s3://"))
        response = client.get_object(parsed.netloc, parsed.path.lstrip("/"))
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    a4 = receipt["a4_small_files"]
    prefix = a4["table_location"].removeprefix("s3://warehouse/").rstrip("/") + "/"
    actual = sorted(
        (
            {"key": item.object_name[len(prefix):], "size": item.size, "etag": item.etag}
            for item in client.list_objects("warehouse", prefix=prefix, recursive=True)
        ),
        key=lambda item: item["key"],
    )
    require(actual == read_json(ROOT / "a4/objects.json"), "A4 current S3 inventory differs")
    metadata_bytes = object_bytes("s3://warehouse/" + prefix + a4["current_metadata_key"])
    require(sha(metadata_bytes) == a4["current_metadata_sha256"], "A4 current metadata changed")
    metadata = json.loads(metadata_bytes)
    require(metadata["current-snapshot-id"] == a4["snapshot_id"], "A4 current snapshot changed")
    require(metadata["current-schema-id"] == a4["schema_id"], "A4 current schema changed")
    snapshot = next(item for item in metadata["snapshots"] if item["snapshot-id"] == metadata["current-snapshot-id"])
    manifest_list = object_bytes(snapshot["manifest-list"])
    require(sha(manifest_list) == a4["manifest_list_sha256"], "A4 current manifest list changed")
    data_files = []
    for entry in fastavro.reader(io.BytesIO(manifest_list)):
        if entry.get("content", 0) == 0:
            manifest = object_bytes(entry["manifest_path"])
            data_files.extend(
                row["data_file"]
                for row in fastavro.reader(io.BytesIO(manifest))
                if row["status"] != 2
            )
    require(len(data_files) == a4["current_snapshot_data_file_count"], "A4 current data file count changed")
    require(sum(item["record_count"] for item in data_files) == a4["current_snapshot_records"], "A4 current records changed")
    current_keys = {
        urlsplit(item["file_path"].replace("s3a://", "s3://")).path.lstrip("/").removeprefix(prefix)
        for item in data_files
    }
    inventory_keys = {item["key"] for item in actual if item["key"].startswith("data/") and item["key"].endswith(".parquet")}
    require(current_keys == inventory_keys, "A4 current files differ from published inventory")

    ssb = receipt["ssb_wide_row_group"]
    require(object_bytes(ssb["ready_uri"]) == (ROOT / "ssb/READY.json").read_bytes(), "SSB remote READY changed")
    require(object_bytes(ssb["manifest_part_uri"]) == (ROOT / "ssb/published-manifest.txt").read_bytes(), "SSB remote published manifest changed")
    table_metadata_bytes = object_bytes(ssb["table_metadata_uri"])
    require(sha(table_metadata_bytes) == ssb["table_metadata_sha256"], "SSB table metadata changed")
    table_metadata = json.loads(table_metadata_bytes)
    require(table_metadata["current-snapshot-id"] == ssb["snapshot_id"], "SSB current snapshot changed")
    require(table_metadata["location"].replace("s3a://", "s3://") == ssb["table_location"], "SSB table location changed")
    snapshot = next(item for item in table_metadata["snapshots"] if item["snapshot-id"] == ssb["snapshot_id"])
    manifest_list = object_bytes(snapshot["manifest-list"])
    require(sha(manifest_list) == ssb["manifest_list_sha256"], "SSB current manifest list changed")
    manifests = [item for item in fastavro.reader(io.BytesIO(manifest_list)) if item.get("content", 0) == 0]
    require(len(manifests) == 1, "SSB current data manifest count changed")
    manifest_bytes = object_bytes(manifests[0]["manifest_path"])
    require(sha(manifest_bytes) == ssb["data_manifest_sha256"], "SSB current data manifest changed")
    data_files = [row["data_file"] for row in fastavro.reader(io.BytesIO(manifest_bytes)) if row["status"] != 2]
    require(len(data_files) == ssb["current_snapshot_data_file_count"], "SSB current file count changed")
    selected = [item for item in data_files if item["file_path"].replace("s3a://", "s3://") == ssb["selected_data_object"]]
    require(len(selected) == 1, "SSB selected file is absent from current snapshot")
    require(selected[0]["record_count"] == ssb["selected_data_rows"], "SSB selected file records changed")
    uri = urlsplit(ssb["selected_data_object"])
    stat = client.stat_object(uri.netloc, uri.path.lstrip("/"))
    require(stat.size == ssb["selected_data_bytes"] and stat.etag == ssb["selected_data_etag"], "SSB selected file S3 identity changed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--online", action="store_true", help="read current MinIO object and Iceberg snapshot identities")
    parser.add_argument("--ssb-file", type=Path, help="optional local copy of the exact selected SSB Parquet file")
    args = parser.parse_args()
    receipt = verify_offline(args.ssb_file)
    if args.online:
        verify_online(receipt)
    print("verified A4 and SSB baseline receipts" + (" and current S3 snapshots" if args.online else ""))


if __name__ == "__main__":
    main()

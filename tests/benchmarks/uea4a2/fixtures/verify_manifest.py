#!/usr/bin/env python3
"""Publish or verify the immutable UEA-4A-2 local writer corpus manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "manifest.json"
SOURCES = {
    "spark": ("Spark 3.5.5 / parquet-mr 1.17.1", "short_iceberg", "data.parquet", None),
    "pyarrow": ("PyArrow 23.0.1", "corpus/pyarrow", "pyarrow-23.0.1.parquet", "physical_ids.txt"),
    "trino": ("Trino 483", "corpus/trino", "trino-483.parquet", "physical_ids.txt"),
    "flink": ("Flink 1.20.5", "corpus/flink", "flink-local-1.20.5.parquet", "physical_ids.txt"),
}
RECEIPTS = {
    "spark": "short_iceberg/manifest.json",
    "pyarrow": "corpus/pyarrow/pyarrow-manifest.json",
    "trino": "corpus/trino/trino-manifest.json",
    "flink": "corpus/flink/flink-local-manifest.json",
}


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def entry(relative: str) -> dict:
    path = ROOT / relative
    if not path.is_file():
        raise ValueError(f"missing corpus attachment: {relative}")
    return {"path": relative, "sha256": digest(path), "bytes": path.stat().st_size}


def expected() -> dict:
    writers = {}
    for key, (version, directory, file, positions) in SOURCES.items():
        receipt = entry(RECEIPTS[key])
        source = json.loads((ROOT / receipt["path"]).read_text())
        data = entry(f"{directory}/{file}")
        expected_hash = source["artifacts"]["data.parquet"] if key == "spark" else source["sha256"]
        if data["sha256"] != expected_hash:
            raise ValueError(f"{key} file disagrees with its original writer receipt")
        if key != "spark" and (source["rows"] != 4096 or source["file"] != file):
            raise ValueError(f"{key} original writer receipt has different rows or file")
        record = {"writer_version": version, "receipt": receipt, "data": data, "rows": 4096}
        if positions:
            record["physical_positions"] = entry(f"{directory}/{positions}")
            if len((ROOT / record["physical_positions"]["path"]).read_text().splitlines()) != 4096:
                raise ValueError(f"{key} position oracle has an unexpected row count")
        else:
            record["physical_positions"] = "identity from short_iceberg/oracle.json"
        writers[key] = record
    ready = (ROOT / "short_iceberg/READY").read_text().strip()
    if ready != "sha256:" + writers["spark"]["receipt"]["sha256"]:
        raise ValueError("short Iceberg READY does not bind its published writer receipt")
    oracle = entry("short_iceberg/oracle.json")
    spark_receipt = json.loads((ROOT / RECEIPTS["spark"]).read_text())
    if oracle["sha256"] != spark_receipt["artifacts"]["oracle.json"]:
        raise ValueError("short Iceberg oracle differs from its published writer receipt")
    rf_manifest = entry("rf_late/manifest.json")
    rf_ready = entry("rf_late/READY")
    if (ROOT / rf_ready["path"]).read_text().strip() != "sha256:" + rf_manifest["sha256"]:
        raise ValueError("late-RF READY does not bind its manifest")
    rf_source = json.loads((ROOT / rf_manifest["path"]).read_text())
    if rf_source.get("fixture_kind") != "uea4a2-iceberg-rf-late-v1":
        raise ValueError("wrong late-RF fixture kind")
    rf_receipt = entry("rf_late/published.json")
    published = json.loads((ROOT / rf_receipt["path"]).read_text())
    if (published.get("schema_version") != 1
            or published.get("local_manifest_sha256") != rf_manifest["sha256"]
            or {table.get("table") for table in published.get("tables", [])}
            != {"probe_v1", "build_v1"}):
        raise ValueError("late-RF publication receipt does not bind both tables")
    rf_files = {}
    for table in published["tables"]:
        source = rf_source["files"][table["table"].split("_")[0] + ".parquet"]
        if (table["object_sha256"] != source["sha256"]
                or table["object_bytes"] != source["bytes"]
                or not isinstance(table.get("snapshot_id"), int)):
            raise ValueError("late-RF published object differs from local source")
        rf_files[table["table"]] = entry("rf_late/" + source["file"])
    whole_receipt = entry("rf_late/whole_published.json")
    whole_published = json.loads((ROOT / whole_receipt["path"]).read_text())
    original_probe = next(table for table in published["tables"] if table["table"] == "probe_v1")
    source_probe = whole_published.get("source", {})
    whole_probe = whole_published.get("whole", {})
    probe_file = rf_source["files"]["probe.parquet"]
    row_groups = len(probe_file["row_groups"])
    if (whole_published.get("schema_version") != 1
            or whole_published.get("namespace") != published.get("namespace")
            or whole_published.get("rest_uri") != published.get("rest_uri")
            or whole_published.get("warehouse") != published.get("warehouse")
            or whole_published.get("local_manifest_sha256") != rf_manifest["sha256"]
            or whole_published.get("local_ready") != (ROOT / rf_ready["path"]).read_text().strip()
            or source_probe.get("table") != "probe_v1"
            or source_probe.get("snapshot_id") != original_probe["snapshot_id"]
            or source_probe.get("object_uri") != original_probe["object_uri"]
            or source_probe.get("object_sha256") != probe_file["sha256"]
            or source_probe.get("split_offset_count") != row_groups
            or whole_probe.get("table") != "probe_whole_v1"
            or not isinstance(whole_probe.get("snapshot_id"), int)
            or whole_probe["snapshot_id"] <= 0
            or whole_probe["snapshot_id"] == source_probe["snapshot_id"]
            or whole_probe.get("object_uri") != whole_probe.get("location", "").rstrip("/") + "/data/probe.parquet"
            or whole_probe.get("object_uri") == source_probe.get("object_uri")
            or whole_probe.get("object_sha256") != probe_file["sha256"]
            or whole_probe.get("object_bytes") != probe_file["bytes"]
            or whole_probe.get("record_count") != probe_file["rows"]
            or row_groups != 32
            or whole_probe.get("physical_row_groups") != row_groups
            or "iceberg_split_offsets" not in whole_probe
            or whole_probe["iceberg_split_offsets"] is not None):
        raise ValueError("late-RF whole-file publication receipt differs from frozen probe and original snapshot")
    return {
        "schema_version": 4,
        "fixture_kind": "uea4a2-frozen-input-inventory-v4",
        "writers": writers,
        "short_iceberg_ready": entry("short_iceberg/READY"),
        "short_iceberg_oracle": oracle,
        "published_iceberg_baselines": entry("baselines/baseline.json"),
        "late_rf": {
            "manifest": rf_manifest,
            "ready": rf_ready,
            "oracle": entry("rf_late/oracle.json"),
            "publication_receipt": rf_receipt,
            "whole_publication": {
                "receipt": whole_receipt,
                "source_snapshot_id": source_probe["snapshot_id"],
                "snapshot_id": whole_probe["snapshot_id"],
                "iceberg_split_offsets": whole_probe["iceberg_split_offsets"],
                "physical_row_groups": whole_probe["physical_row_groups"],
                "record_count": whole_probe["record_count"],
                "object_uri": whole_probe["object_uri"],
                "object_sha256": whole_probe["object_sha256"],
                "object_bytes": whole_probe["object_bytes"],
            },
            "files": rf_files,
        },
        "scope": "four-writer FS conformance and published Iceberg short, small-file, wide, and whole-file late-RF inputs",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--publish", action="store_true")
    group.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    candidate = expected()
    if args.publish:
        if MANIFEST.exists():
            raise ValueError("refusing to replace an existing corpus manifest")
        MANIFEST.write_text(json.dumps(candidate, indent=2, sort_keys=True) + "\n")
    elif json.loads(MANIFEST.read_text()) != candidate:
        raise ValueError("corpus manifest differs from the checked original writer attachments")
    print(f"UEA-4A-2 corpus manifest verified: {digest(MANIFEST)}")


if __name__ == "__main__":
    main()

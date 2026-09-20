#!/usr/bin/env python3
"""Network-enabled, atomic fixture input provisioner. Never called by verify CI."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from fixture_inputs import (
    FixtureInputError,
    artifact_entry,
    definition_sha256,
    fixture_store,
    inspect_image,
    load_lock,
    require_relative,
    run,
    validate_artifact,
    verify_image,
)


SCRIPT_DIR = Path(__file__).resolve().parent


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def download(url: str, output: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "NovaRocks-fixture-provision/1"})
    try:
        with urllib.request.urlopen(request, timeout=90) as response, output.open("wb") as destination:
            shutil.copyfileobj(response, destination)
    except OSError as error:
        raise FixtureInputError(f"fixture artifact download failed: {url}") from error


def prepare_images(lock: dict[str, Any]) -> dict[str, dict[str, str]]:
    receipts: dict[str, dict[str, str]] = {}
    for name, item in lock["images"].items():
        reference = f"{item['source']}@{item['manifest_digest']}"
        run(["docker", "pull", "--platform", item["platform"], reference])
        info = inspect_image(reference)
        verify_image(info, item)
        run(["docker", "tag", reference, item["alias"]])
        receipts[name] = {"alias": item["alias"], "manifest_digest": item["manifest_digest"], "platform": item["platform"]}
    return receipts


def prepare_artifacts(lock: dict[str, Any], staging: Path) -> dict[str, dict[str, Any]]:
    artifacts_dir = staging / "artifacts"
    artifacts_dir.mkdir(parents=True)
    receipts: dict[str, dict[str, Any]] = {}
    for name, item in lock["artifacts"].items():
        output = artifacts_dir / name
        download(item["url"], output)
        receipts[name] = validate_artifact(output, item)
    return receipts


def build_derived_images(lock: dict[str, Any], repo_root: Path, staging: Path, lock_sha: str) -> dict[str, dict[str, str]]:
    receipts: dict[str, dict[str, str]] = {}
    for name, item in lock["derived_images"].items():
        context = staging / "build-contexts" / name
        (context / "artifacts").mkdir(parents=True)
        dockerfile = repo_root / require_relative(item["dockerfile"])
        shutil.copy2(dockerfile, context / "Dockerfile")
        for artifact in item["artifacts"]:
            shutil.copy2(staging / "artifacts" / artifact, context / "artifacts" / artifact)
        definition = definition_sha256(repo_root, item["definition_files"])
        base = lock["images"][item["base"]]["alias"]
        run([
            "docker", "build", "--platform", item["platform"],
            "--build-arg", f"SPARK_BASE={base}",
            "--label", f"novarocks.fixture.lock.sha256={lock_sha}",
            "--label", f"novarocks.fixture.definition.sha256={definition}",
            "-t", item["alias"], str(context),
        ])
        info = inspect_image(item["alias"])
        verify_image(info, item, derived=True)
        receipts[name] = {"alias": item["alias"], "platform": item["platform"], "definition_sha256": definition, "image_id": str(info.get("Id", ""))}
    return receipts


def provision(store: Path, repo_root: Path, lock_path: Path) -> Path:
    lock, lock_sha = load_lock(lock_path)
    store.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=store))
    try:
        images = prepare_images(lock)
        artifacts = prepare_artifacts(lock, staging)
        derived = build_derived_images(lock, repo_root, staging, lock_sha)
        generation = f"generation-{uuid.uuid4().hex}"
        generations = store / "generations"
        generations.mkdir(exist_ok=True)
        staging.replace(generations / generation)
        bom = {
            "schema": 1,
            "lock_sha256": lock_sha,
            "artifact_dir": f"generations/{generation}/artifacts",
            "images": images,
            "artifacts": artifacts,
            "derived_images": derived,
        }
        atomic_json(store / "bom.json", bom)
        (store / "READY").write_text(f"sha256:{lock_sha}\n")
    except Exception:
        # A failed staging directory has no READY/BOM reachability. Retain it
        # for diagnosis instead of deleting a store another provision may use.
        raise
    print(store / "bom.json")
    return store / "bom.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store")
    parser.add_argument("--repo-root", default=SCRIPT_DIR.parents[1])
    parser.add_argument("--lock", default=SCRIPT_DIR / "lock.json")
    args = parser.parse_args()
    try:
        provision(fixture_store(args.store), Path(args.repo_root).resolve(), Path(args.lock).resolve())
    except FixtureInputError as error:
        print(f"PROVISION FAILED: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

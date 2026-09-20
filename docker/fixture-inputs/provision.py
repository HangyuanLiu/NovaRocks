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
    fixture_store_lock,
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


def parse_image_sources(specs: list[str], image_names: set[str]) -> dict[str, str]:
    """Parse explicit transport-only source overrides.

    The lock remains the identity authority: an override can choose where the
    daemon fetches bytes, but never alter a logical item, digest, or platform.
    """
    overrides: dict[str, str] = {}
    for spec in specs:
        name, separator, source = spec.partition("=")
        if not separator or not name or not source:
            raise FixtureInputError(
                "--image-source must use logical-name=transport-repository"
            )
        if name not in image_names:
            raise FixtureInputError(f"unknown fixture image override: {name}")
        if any(character.isspace() for character in source) or "@" in source:
            raise FixtureInputError(
                "fixture image transport repository must not contain whitespace or a digest"
            )
        if name in overrides:
            raise FixtureInputError(f"duplicate fixture image override: {name}")
        overrides[name] = source
    return overrides


def image_reference(
    name: str, item: dict[str, Any], overrides: dict[str, str]
) -> tuple[str, str]:
    transport_source = overrides.get(name, item["source"])
    return transport_source, f"{transport_source}@{item['manifest_digest']}"


def prepare_images(
    lock: dict[str, Any], overrides: dict[str, str], pull_timeout_seconds: int
) -> dict[str, dict[str, str]]:
    receipts: dict[str, dict[str, str]] = {}
    for name, item in lock["images"].items():
        transport_source, reference = image_reference(name, item, overrides)
        try:
            info = inspect_image(reference)
            verify_image(info, item)
        except FixtureInputError:
            run(
                ["docker", "pull", "--platform", item["platform"], reference],
                timeout_seconds=pull_timeout_seconds,
            )
            info = inspect_image(reference)
        verify_image(info, item)
        run(["docker", "tag", reference, item["alias"]])
        receipts[name] = {
            "alias": item["alias"],
            "manifest_digest": item["manifest_digest"],
            "platform": item["platform"],
            "transport_source": transport_source,
        }
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


def provision(
    store: Path,
    repo_root: Path,
    lock_path: Path,
    image_source_specs: list[str],
    pull_timeout_seconds: int,
) -> Path:
    if pull_timeout_seconds < 1:
        raise FixtureInputError("--docker-pull-timeout-seconds must be a positive integer")
    lock, lock_sha = load_lock(lock_path)
    image_sources = parse_image_sources(image_source_specs, set(lock["images"]))
    store.mkdir(parents=True, exist_ok=True)
    with fixture_store_lock(store, exclusive=True, create=True):
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=store))
        try:
            images = prepare_images(lock, image_sources, pull_timeout_seconds)
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
    parser.add_argument(
        "--image-source",
        action="append",
        default=[],
        metavar="LOGICAL_NAME=TRANSPORT_REPOSITORY",
        help="fetch one locked image through an explicit mirror without changing its digest",
    )
    parser.add_argument(
        "--docker-pull-timeout-seconds",
        type=int,
        default=int(os.environ.get("NOVA_FIXTURE_DOCKER_PULL_TIMEOUT_SECONDS", "180")),
        help="bound one explicit Docker image transfer (default: 180)",
    )
    args = parser.parse_args()
    try:
        provision(
            fixture_store(args.store),
            Path(args.repo_root).resolve(),
            Path(args.lock).resolve(),
            args.image_source,
            args.docker_pull_timeout_seconds,
        )
    except FixtureInputError as error:
        print(f"PROVISION FAILED: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

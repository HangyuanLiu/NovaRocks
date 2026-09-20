from __future__ import annotations

import hashlib
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
for module_name in ("fixture_inputs", "provision", "verify"):
    spec = importlib.util.spec_from_file_location(module_name, ROOT / f"{module_name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

fixture_inputs = sys.modules["fixture_inputs"]
verify_module = sys.modules["verify"]
provision_module = sys.modules["provision"]


class FixtureInputsTest(unittest.TestCase):
    def test_lock_is_immutable_and_covers_all_current_inputs(self) -> None:
        lock, digest = fixture_inputs.load_lock(ROOT / "lock.json")
        self.assertEqual(lock["schema"], 1)
        self.assertEqual(
            lock["images"]["paimon-spark-base"]["platform"], "linux/amd64"
        )
        self.assertIn("paimon-writer", lock["derived_images"])
        self.assertIn("iceberg-spark", lock["derived_images"])
        self.assertEqual(len(digest), 64)
        self.assertNotIn("latest", str(lock))

    def test_artifact_validation_requires_size_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "artifact.jar"
            artifact.write_bytes(b"fixture")
            item = {"bytes": 7, "sha1": hashlib.sha1(b"fixture").hexdigest()}
            receipt = fixture_inputs.validate_artifact(artifact, item)
            self.assertEqual(receipt["sha256"], hashlib.sha256(b"fixture").hexdigest())
            artifact.write_bytes(b"changed")
            with self.assertRaises(fixture_inputs.FixtureInputError):
                fixture_inputs.validate_artifact(artifact, item)

    def test_definition_rejects_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(fixture_inputs.FixtureInputError):
                fixture_inputs.definition_sha256(Path(temporary), ["../outside"])

    def test_missing_bom_blocks_without_docker_or_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(verify_module, "inspect_image") as inspect:
                with self.assertRaises(fixture_inputs.FixtureInputError) as raised:
                    verify_module.verify(
                        Path(temporary), ROOT.parents[1], ROOT / "lock.json"
                    )
            inspect.assert_not_called()
            self.assertIn("BOM is missing", str(raised.exception))

    def test_image_source_override_changes_transport_but_not_identity(self) -> None:
        lock, _ = fixture_inputs.load_lock(ROOT / "lock.json")
        item = lock["images"]["paimon-spark-base"]
        overrides = provision_module.parse_image_sources(
            ["paimon-spark-base=dockerproxy.net/apache/spark"], set(lock["images"])
        )
        source, reference = provision_module.image_reference(
            "paimon-spark-base", item, overrides
        )
        self.assertEqual(source, "dockerproxy.net/apache/spark")
        self.assertEqual(reference, f"{source}@{item['manifest_digest']}")
        with self.assertRaises(fixture_inputs.FixtureInputError):
            provision_module.parse_image_sources(
                ["unknown=example.invalid/spark"], set(lock["images"])
            )
        with self.assertRaises(fixture_inputs.FixtureInputError):
            provision_module.parse_image_sources(
                ["paimon-spark-base=example.invalid/spark@sha256:override"],
                set(lock["images"]),
            )

    def test_provision_reuses_a_verified_local_image_before_pulling(self) -> None:
        item = {
            "source": "example.invalid/spark",
            "platform": "linux/amd64",
            "manifest_digest": "sha256:fixture",
            "alias": "novarocks/fixture:test",
        }
        with mock.patch.object(provision_module, "inspect_image", return_value={}), mock.patch.object(
            provision_module, "verify_image"
        ), mock.patch.object(provision_module, "run") as run:
            provision_module.prepare_images({"images": {"spark": item}}, {}, 30)
        self.assertEqual(
            run.call_args_list,
            [mock.call(["docker", "tag", "example.invalid/spark@sha256:fixture", item["alias"]])],
        )


if __name__ == "__main__":
    unittest.main()

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Writer startup hooks are explicit and preserve the admitted budget."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "docker/iceberg-rest"))
import fixture_run
from scratch import ScratchIdentity


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, REPO / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


iceberg = load("startup_uea4a4_iceberg", "tests/benchmarks/uea4a4/iceberg_fixture.py")
hms = load("startup_hms_fixture", "tests/sql/fixtures/iceberg-hms/fixture.py")


class WriterStartupTests(unittest.TestCase):
    def test_iceberg_prepare_passes_original_budget_before_output_creation(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            output = root / "output"
            budget = object()
            runtime = SimpleNamespace(env_id="worktree", run_budget=budget)
            args = SimpleNamespace(output_dir=str(output), env_file=str(root / "env.sh"),
                files_per_table=24, run_id="new-run", dry_run=False)
            def sweep(env_file, *, budget):
                self.assertIs(budget, runtime.run_budget)
                self.assertEqual(env_file, (root / "env.sh").resolve())
                self.assertFalse(output.exists())
                raise RuntimeError("native sweep failed")
            with patch.object(iceberg.support, "parse_env_file", return_value={
                    "NOVAROCKS_ICEBERG_REST_URI":"saved", "NOVAROCKS_ICEBERG_REST_WAREHOUSE":"saved"}), \
                    patch.object(iceberg.support, "load_runtime", return_value=runtime), \
                    patch.object(iceberg.support.governed_run, "startup_sweep", side_effect=sweep) as called:
                with self.assertRaisesRegex(RuntimeError, "native sweep failed"):
                    iceberg.prepare(args)
                called.assert_called_once()
            self.assertFalse(output.exists())

    def test_iceberg_existing_ready_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "READY").touch()
            args = SimpleNamespace(output_dir=str(root), env_file=str(root / "env.sh"),
                files_per_table=24, run_id="same-run", dry_run=False)
            with patch.object(iceberg.support, "parse_env_file", return_value={}), \
                    patch.object(iceberg.support, "load_runtime", return_value=object()), \
                    patch.object(iceberg, "verify_ready", return_value={"run_id":"same-run",
                        "definition_sha256":iceberg.definition_sha256(24)}), \
                    patch.object(iceberg.support.governed_run, "startup_sweep") as sweep:
                self.assertEqual(iceberg.prepare(args), 0)
                sweep.assert_not_called()

    def test_hms_prepare_sweeps_original_budget_before_evidence_or_spark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = ScratchIdentity.create("s3://novarocks/worktree")
            budget = SimpleNamespace(identity=identity, as_dict=lambda: {"identity":identity.as_dict()},
                                     remaining_seconds=lambda **kwargs: 3600)
            environment = {"NOVA_ENV_REST_ENV_FILE":str(root / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT":str(root), "NOVAROCKS_ICEBERG_HMS_WAREHOUSE":"s3://warehouse/old"}
            def sweep(env_file, *, budget):
                self.assertIs(budget.identity, identity)
                self.assertEqual(env_file, root / "env.sh")
                self.assertFalse((root / "tests/sql/.runtime/hms-evidence").exists())
                raise RuntimeError("native sweep failed")
            with patch.dict(os.environ, environment, clear=True), \
                    patch.object(sys, "argv", ["fixture.py", "prepare", identity.resource_name("hms")]), \
                    patch.object(fixture_run, "admit", return_value=({}, {}, budget)), \
                    patch.object(fixture_run, "startup_sweep", side_effect=sweep) as called, \
                    patch.object(hms.signal, "signal"), patch.object(hms.signal, "setitimer"), \
                    patch.object(hms, "run_spark") as spark:
                with self.assertRaisesRegex(RuntimeError, "native sweep failed"):
                    hms.main()
                called.assert_called_once()
                spark.assert_not_called()

    def test_hms_snapshot_does_not_sweep(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "NOVA_ENV_REST_ENV_FILE":str(Path(directory) / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT":directory}, clear=True), \
                patch.object(sys, "argv", ["fixture.py", "before", "iru7_readonly"]), \
                patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                patch.object(fixture_run, "startup_sweep") as sweep, \
                patch.object(hms, "run_spark", return_value={"metadata_location":["v1", "v2", "v3"]}), \
                patch.object(hms, "inventory", return_value=(["a", "b", "c"], 6)):
            hms.main()
            sweep.assert_not_called()


if __name__ == "__main__":
    unittest.main()

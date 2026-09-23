#!/usr/bin/env python3
"""Focused checks for the coarse UEA-4A-4 regression screen."""

from __future__ import annotations

import copy
import unittest

from compare import ComparisonError, compare, same_configuration


def run(start: int, *, throughput: float = 10.0, p95: float = 100_000.0,
        binary: str = "a" * 64) -> dict:
    windows = [
        {"provider": provider, "mode": mode, "repetition": 0,
         "throughput_per_second": throughput, "p95_micros": p95}
        for provider in ("iceberg", "paimon") for mode in ("normal", "slow-remote")
    ]
    return {
        "report": {"workload_sha256": "w", "iceberg_fixture_sha256": "i",
                   "paimon_fixture_sha256": "p", "effective_config_sha256": "c",
                   "server_binary_sha256": binary, "windows": windows,
                   "peak_rss_bytes_by_role": {"fe": 1, "be_0": 2, "be_1": 3, "be_2": 4}},
        "evidence": {"base_config_sha256": "c", "cargo_lock_sha256": "l", "platform": {"os": "macos"},
                     "runner_native_build_identity": "r", "source_revision": "s",
                     "source_tree_sha256": "t", "started_unix_millis": start,
                     "ended_unix_millis": start + 10},
    }


class CompareTests(unittest.TestCase):
    def test_equal_short_runs_pass(self) -> None:
        self.assertTrue(compare(run(0), run(20, binary="b" * 64))["passed"])

    def test_only_large_regressions_fail(self) -> None:
        baseline = run(0)
        self.assertTrue(compare(baseline, run(20, throughput=5.0, p95=200_000,
                                               binary="b" * 64))["passed"])
        self.assertFalse(compare(baseline, run(20, throughput=4.9,
                                                binary="b" * 64))["passed"])
        self.assertFalse(compare(baseline, run(20, p95=200_001,
                                                binary="b" * 64))["passed"])

    def test_all_provider_modes_are_checked(self) -> None:
        candidate = run(20, binary="b" * 64)
        candidate["report"]["windows"][3]["throughput_per_second"] = 1.0
        result = compare(run(0), candidate)
        self.assertFalse(result["passed"])
        self.assertEqual(sum(not row["passed"] for row in result["metrics"]), 1)

    def test_identity_and_order_fail_closed(self) -> None:
        baseline, candidate = run(0), run(20, binary="b" * 64)
        changed = copy.deepcopy(candidate)
        changed["report"]["iceberg_fixture_sha256"] = "other"
        with self.assertRaises(ComparisonError):
            same_configuration(baseline, changed)
        changed = copy.deepcopy(candidate)
        changed["evidence"]["base_config_sha256"] = "other"
        with self.assertRaises(ComparisonError):
            same_configuration(baseline, changed)
        with self.assertRaises(ComparisonError):
            same_configuration(baseline, run(20))
        with self.assertRaises(ComparisonError):
            same_configuration(baseline, run(5, binary="b" * 64))


if __name__ == "__main__":
    unittest.main()

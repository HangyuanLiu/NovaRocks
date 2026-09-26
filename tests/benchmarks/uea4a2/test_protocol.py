"""Offline negative and statistical checks for the frozen benchmark driver."""

import json
import hashlib
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from compare import compare, percentile  # noqa: E402
from run import (PreflightError, validate_frozen_manifest, validate_manifest,
                 validate_pilot_manifest, validate_pilot_rss_trace,
                 validate_pilot_runner_proxy_trace)  # noqa: E402


class ProtocolTests(unittest.TestCase):
    def test_v3_runner_proxy_samples_require_real_process_identity(self):
        trace = {"processes": {"runner-proxy": {"pid": 42, "process_start_token": "birth"}},
                 "samples": [{"role": "runner-proxy", "pid": 42,
                              "process_start_token": "birth", "elapsed_millis": 100,
                              "rss_bytes": 4096, "cpu_user_nanos": 100,
                              "cpu_system_nanos": 10, "unavailable_reason": None}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resources.json"
            path.write_text(json.dumps(trace))
            validate_pilot_runner_proxy_trace("g0-a", path)
            trace["samples"][0]["pid"] = 43
            path.write_text(json.dumps(trace))
            with self.assertRaisesRegex(PreflightError, "runner-proxy resource samples"):
                validate_pilot_runner_proxy_trace("g0-a", path)

    def test_v2_rss_summary_is_bound_to_raw_process_samples(self):
        timing = {"idle_start": 0, "idle_end": 1000, "measure_start": 4000,
                  "measure_end": 124000, "post_start": 124100, "post_end": 126100}
        window = {"workload": "short", "repetition": 0, "started_ms": 4000, "duration_ms": 120000,
                  "rss_timing": timing}
        roles = ("be-0", "be-1", "be-2")
        times = (0, 500, 1000, 4000, 64000, 124000, 124100, 126100)
        trace = {"schema_version": 3, "processes": {}, "samples": []}
        rss = {"short": {}}
        for index, role in enumerate(roles):
            identity = {"pid": index + 1, "process_start_token": f"sample-{index}"}
            trace["processes"][role] = identity
            for number, instant in enumerate(times):
                trace["samples"].append({"role": role, **identity,
                                          "elapsed_millis": instant,
                                          "rss_bytes": 10 * (number + 1) + index * 100,
                                          "unavailable_reason": None})
            rss["short"][role] = [{"idle_bytes": 20 + index * 100,
                                    "peak_bytes": 80 + index * 100,
                                    "steady_bytes": 60 + index * 100,
                                    "post_drain_bytes": 80 + index * 100,
                                    "observed_post_drain_seconds": 2.0}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resources.json"
            path.write_text(json.dumps(trace))
            validate_pilot_rss_trace("g0-a", [window], rss, path)
            rss["short"]["be-1"][0]["peak_bytes"] += 1
            with self.assertRaisesRegex(PreflightError, "differs from raw"):
                validate_pilot_rss_trace("g0-a", [window], rss, path)

    def test_nearest_rank_uses_ceiling_not_interpolation(self):
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(percentile([1] * 19 + [100], 95), 1)
        self.assertEqual(percentile([1] * 19 + [100], 99), 100)

    def test_checked_in_manifest_cannot_run_unfrozen(self):
        path = Path(__file__).with_name("workload.json")
        with self.assertRaisesRegex(PreflightError, "sample_extension_rule"):
            validate_frozen_manifest(path)

    def test_pilot_accepts_explicit_query_with_formal_fields_unfrozen(self):
        path = Path(__file__).with_name("workload.json")
        self.assertEqual(validate_pilot_manifest(path)["pilot"]["queries"][0]["expected_row_count"], 10)

    def test_changed_threshold_is_rejected_even_if_other_fields_missing(self):
        original = json.loads(Path(__file__).with_name("workload.json").read_text())
        original["measurement"]["aa_throughput_spread_limit"] = 0.25
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workload.json"
            path.write_text(json.dumps(original))
            with self.assertRaisesRegex(PreflightError, "aa_throughput_spread_limit"):
                validate_manifest(path)

    def test_pilot_collects_two_g0_receipts_without_formal_pass(self):
        document = json.loads(Path(__file__).with_name("workload.json").read_text())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "workload.json"
            manifest.write_text(json.dumps(document))
            g0 = root / "g0"
            g0.write_bytes(b"release checkpoint")
            fixture = root / "fixture.json"
            artifact = root / "oracle.json"
            artifact.write_text('{"row_count":4096}')
            fixture.write_text(json.dumps({"fixture_kind": "uea4a2-iceberg-short-v1",
                                           "schema_version": 1, "row_count": 4096,
                                           "data_file_count": 1,
                                           "artifacts": {"oracle.json": hashlib.sha256(artifact.read_bytes()).hexdigest()}}))
            (root / "READY").write_text("sha256:" + hashlib.sha256(fixture.read_bytes()).hexdigest())
            config = root / "config.toml"
            config.write_text("[server]\n")
            runner = root / "runner"
            runner.write_text("#!/usr/bin/env python3\n"
                              "import hashlib, json, os, pathlib, sys\n"
                              "if '--list' in sys.argv: print('uea4/iceberg-range-performance'); sys.exit()\n"
                              "h=lambda p: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()\n"
                              "root=pathlib.Path(sys.argv[sys.argv.index('--artifact-root')+1])\n"
                              "p=root/'uea4-iceberg-range-performance'/'uea4a2-performance.json'\n"
                              "p.parent.mkdir(parents=True)\n"
                              "m=os.environ['NOVAROCKS_UEA4A2_WORKLOAD_MANIFEST']\n"
                              "f=os.environ['NOVAROCKS_UEA4A2_FIXTURE_MANIFEST']\n"
                              "b=sys.argv[sys.argv.index('--binary')+1]\n"
                              "c=sys.argv[sys.argv.index('--config')+1]\n"
                              "name=os.environ['NOVAROCKS_UEA4A2_GROUP']\n"
                              "mode=os.getenv('UEA4A2_FAKE_MODE','good')\n"
                              "q=[{'started_ms':i,'ended_ms':i+(3 if mode=='unstable' and name=='g0-b' else 1),'status':'success'} for i in range(1300 if mode=='throughput' and name=='g0-b' else 1000)]\n"
                              "if mode=='late' and name=='g0-b': q[-1]['ended_ms']=120001\n"
                              "w=[{'workload':'uea4a2-iceberg-short-count','repetition':i,'duration_ms':120000,'started_ms':0,'warmup_drained':True,'tail_drained':True,'tail_drain_ms':0,'queries':q} for i in range(3)]\n"
                              "if mode=='wobble' and name=='g0-a': w[0]['queries']=[{'started_ms':i,'ended_ms':i+1,'status':'success'} for i in range(1300)]\n"
                              "s={'idle_bytes':10,'peak_bytes':20,'steady_bytes':15,'post_drain_bytes':11}\n"
                              "rss={'uea4a2-iceberg-short-count':{r:[s,s,s] for r in ('be-0','be-1','be-2')}}\n"
                              "(p.parent/'queries.json').write_text(json.dumps(w)); (p.parent/'resources.json').write_text('[]')\n"
                              "(p.parent/'proxy-events.jsonl').write_text(json.dumps({'kind':'Arrived','request_id':1,'connection_id':1,'protocol':'HTTP/1.1','method':'GET','object_id':'fixture','read_class':'Data','range':'bytes=0-1','bytes':2})+'\\n')\n"
                              "(p.parent/'proxy-connections.jsonl').write_text(json.dumps({'kind':'Accepted','connection_id':1})+'\\n')\n"
                              "a={k:{'path':v,'sha256':h(p.parent/v)} for k,v in [('query_trace','queries.json'),('resource_trace','resources.json'),('proxy_event_trace','proxy-events.jsonl'),('proxy_connection_trace','proxy-connections.jsonl')]}\n"
                              "effective='2'*64 if mode=='config-cross' and name=='g0-b' else h(c)\n"
                              "report={'schema_version':1,'group':name,'topology':{'fe':1,'be':3},'workload_sha256':h(m),'fixture_manifest_sha256':h(f),'base_config_sha256':h(c),'binary_sha256':h(b),'runner_sha256':('0'*64 if mode=='runner-mismatch' and name=='g0-b' else h(sys.argv[0])),'effective_config_sha256':effective,'windows':w,'rss':rss,'attachments':a}\n"
                              "p.write_text(json.dumps(report))\n"
                              "evidence_effective='1'*64 if mode=='config-self' and name=='g0-b' else effective\n"
                              "(p.parent/'scenario-evidence.json').write_text(json.dumps({'scenario':'uea4/iceberg-range-performance','outcome':'passed','cluster_size':3,'launch_profile':'performance','effective_launch_config_semantics_sha256':evidence_effective}))\n")
            runner.chmod(0o755)
            for mode, expected_code in (("good", 0), ("late", 2), ("unstable", 1),
                                        ("throughput", 1), ("wobble", 1),
                                        ("runner-mismatch", 2), ("config-self", 2),
                                        ("config-cross", 2)):
                with self.subTest(mode=mode):
                    output = root / mode
                    result = subprocess.run([sys.executable, str(Path(__file__).with_name("run.py")),
                                             "--baseline-pilot", "--g0", str(g0), "--runner", str(runner),
                                             "--manifest", str(manifest), "--fixture-manifest", str(fixture),
                                             "--config", str(config), "--output", str(output)],
                                            capture_output=True, text=True,
                                            env={**os.environ, "UEA4A2_FAKE_MODE": mode})
                    self.assertEqual(result.returncode, expected_code, result.stderr)
                    if mode == "good":
                        self.assertIn("no candidate gate", result.stdout)
                        self.assertTrue(json.loads((output / "pilot-aa.json").read_text())["passed"])
                        self.assertFalse((output / "input-hashes.json").exists())
                        self.assertTrue((output / "pilot-input-hashes.json").exists())
                    elif mode == "late":
                        self.assertIn("only 999 in-window successful", result.stderr)
                        self.assertFalse((output / "pilot-aa.json").exists())
                    elif mode in ("runner-mismatch", "config-self", "config-cross"):
                        expected = ("input/provenance mismatch" if mode == "runner-mismatch" else
                                    "effective config semantics mismatch" if mode == "config-self" else
                                    "G0 A/A effective config semantics differ")
                        self.assertIn(expected, result.stderr)
                        self.assertFalse((output / "pilot-aa.json").exists())
                    else:
                        aa = json.loads((output / "pilot-aa.json").read_text())
                        self.assertFalse(aa["passed"])
                        self.assertFalse(aa["candidate_evaluated"])
                        metric = "throughput_per_second" if mode in ("throughput", "wobble") else "p95_ms"
                        if mode == "wobble":
                            self.assertEqual(aa["checks"][0]["medians"]["g0-a"],
                                             aa["checks"][0]["medians"]["g0-b"])
                        self.assertTrue(any(not row["passed"] and row["metric"] == metric
                                            for row in aa["checks"]))

    def test_missing_receipts_never_pass(self):
        # The checked-in manifest is deliberately unfrozen: this must fail before
        # a comparison can infer anything from an empty output directory.
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(PreflightError):
                compare(Path(directory), Path(__file__).with_name("workload.json"))


if __name__ == "__main__":
    unittest.main()

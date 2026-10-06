// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Each variant must execute the frozen matrix and produce honest schema facts.
use novarocks_memory_attribution_bench::manifest::{self, MANIFEST_SHA256};
use serde_json::Value;
use std::{fs, process::Command};
#[test]
fn all_allocator_entrypoints_execute_the_frozen_smoke_matrix() {
    let manifest = manifest::load().unwrap();
    let bins = [
        ("raw", env!("CARGO_BIN_EXE_attribution-bench-raw")),
        ("counting", env!("CARGO_BIN_EXE_attribution-bench-counting")),
        (
            "attributing",
            env!("CARGO_BIN_EXE_attribution-bench-attributing"),
        ),
        (
            "attributing-system",
            env!("CARGO_BIN_EXE_attribution-bench-attributing-system"),
        ),
    ];
    let base = std::env::temp_dir().join(format!(
        "novarocks-attribution-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    for (variant, bin) in bins {
        let out = base.join(variant);
        let result = Command::new(bin)
            .args(["--mode", "smoke", "--out"])
            .arg(&out)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{variant}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let v: Value = serde_json::from_slice(&fs::read(out.join("report.json")).unwrap()).unwrap();
        assert_eq!(v["schema_version"], 1);
        assert_eq!(v["variant"], variant);
        assert_eq!(v["mode"], "smoke");
        assert_eq!(v["formal_acceptance"], false);
        assert_eq!(v["manifest_sha256"], MANIFEST_SHA256);
        assert_eq!(v["manifest"]["attribution_threshold_bytes"], 512);
        assert_eq!(v["manifest"]["token_bytes"], 8);
        assert!(v["metadata"]["git_sha"].as_str().unwrap().len() >= 40);
        assert_eq!(
            v["metadata"]["cargo_lock_sha256"].as_str().unwrap().len(),
            64
        );
        for field in ["host", "kernel", "rustc"] {
            assert!(!v["metadata"][field].as_str().unwrap().is_empty());
        }
        let rows = v["results"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            manifest.workloads.w1.sizes_bytes.len() * manifest.alignment_bytes.len() + 11
        );
        for workload in ["W1", "W2", "W3", "W4", "W5"] {
            assert!(rows.iter().any(|r| r["workload"] == workload));
        }
        for workload in ["W3", "W5"] {
            for workers in [1, 4, 8, 16] {
                assert!(
                    rows.iter()
                        .any(|r| r["workload"] == workload && r["worker_count"] == workers)
                );
            }
        }
        for row in rows {
            assert!(row["latency"]["p50_ns_per_operation"].as_u64().is_some());
            assert!(
                row["latency"]["p99_ns_per_operation"].as_u64().unwrap()
                    >= row["latency"]["p50_ns_per_operation"].as_u64().unwrap()
            );
            assert!(
                row["throughput"]["ops_per_second"]
                    .as_f64()
                    .unwrap()
                    .is_finite()
            );
            assert!(row["throughput"]["ops_per_second"].as_f64().unwrap() > 0.0);
            assert_eq!(
                row["latency"]["samples"].as_u64().unwrap(),
                20 * row["worker_count"].as_u64().unwrap()
            );
            if row["workload"] == "W3" {
                assert_eq!(row["nominal_remote_release_fraction"], 0.2);
                assert_eq!(row["release_thread_count"], 1);
            }
            let every = match row["workload"].as_str().unwrap() {
                "W2" => Some(1_u64),
                "W3" => Some(5_u64),
                _ => None,
            };
            for pass in ["throughput", "latency"] {
                let workers = row["worker_count"].as_u64().unwrap();
                let operations = row[pass]["operations"].as_u64().unwrap();
                let expected_remote = every.map_or(0, |n| (operations / workers / n) * workers);
                assert_eq!(row[pass]["remote_operation_count"], expected_remote);
                assert_eq!(
                    row[pass]["remote_release_fraction"].as_f64().unwrap(),
                    expected_remote as f64 / operations as f64
                );
            }
            if row["workload"] == "W5" {
                assert_eq!(row["ambient_scope_installed"], false);
            }
            for stage in ["baseline", "held", "released"] {
                assert_eq!(
                    row["space"][stage]["jemalloc"].is_null(),
                    variant == "attributing-system"
                );
            }
            for bin in row["size_histogram"].as_array().unwrap() {
                assert_eq!(
                    bin["jemalloc_usable_bytes"].is_null(),
                    variant == "attributing-system"
                );
                if let Some(usable) = bin["jemalloc_usable_bytes"].as_u64() {
                    assert!(usable >= bin["underlying_requested_bytes"].as_u64().unwrap());
                }
            }
        }
        if variant == "attributing-system" {
            assert!(v["metadata"]["jemalloc_runtime_version"].is_null());
            assert!(!v["metadata"]["jemalloc_unavailable_reason"].is_null());
        } else {
            assert!(v["metadata"]["jemalloc_runtime_version"].is_string());
        }
    }
    fs::remove_dir_all(base).unwrap();
}

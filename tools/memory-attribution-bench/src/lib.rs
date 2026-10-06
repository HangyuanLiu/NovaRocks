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

//! Frozen allocator comparisons; formal mode never grants formal acceptance.
pub mod manifest;
pub mod report;
pub mod workloads;
use serde::Serialize;
use std::{fs, path::PathBuf};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Variant {
    Raw,
    Counting,
    Attributing,
    AttributingSystem,
}
impl Variant {
    pub fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Counting => "counting",
            Self::Attributing => "attributing",
            Self::AttributingSystem => "attributing-system",
        }
    }
    pub fn is_attributing(self) -> bool {
        matches!(self, Self::Attributing | Self::AttributingSystem)
    }
    pub fn has_jemalloc(self) -> bool {
        !matches!(self, Self::AttributingSystem)
    }
}
#[derive(Serialize)]
struct Report {
    schema_version: u32,
    variant: Variant,
    mode: String,
    formal_acceptance: bool,
    manifest_sha256: &'static str,
    manifest: serde_json::Value,
    metadata: report::HostMetadata,
    parameters: serde_json::Value,
    measurement_limitations: Vec<&'static str>,
    results: Vec<workloads::ResultRow>,
}
pub fn run(variant: Variant) -> Result<(), String> {
    let mut mode = "smoke".to_owned();
    let mut out = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = args.next().ok_or("--mode requires smoke or formal")?,
            "--out" => {
                out = Some(PathBuf::from(
                    args.next().ok_or("--out requires a directory")?,
                ))
            }
            "--help" | "-h" => {
                println!(
                    "Usage: attribution-bench-{} --mode smoke|formal --out DIRECTORY",
                    variant.name()
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let out = out.ok_or("--out is required")?;
    if mode != "smoke" && mode != "formal" {
        return Err("--mode requires smoke or formal".into());
    }
    if mode == "formal" && !cfg!(target_os = "linux") {
        return Err("formal mode requires Linux; smoke is the portability check".into());
    }
    if mode == "formal" && cfg!(debug_assertions) {
        return Err("formal mode requires a release build without debug assertions".into());
    }
    let target = out.join("report.json");
    if target.exists() {
        return Err(format!(
            "refusing to overwrite existing report: {}",
            target.display()
        ));
    }
    let manifest = manifest::load()?;
    let parameters = if mode == "formal" {
        manifest.modes.formal
    } else {
        manifest.modes.smoke
    };
    let metadata = report::host_metadata(variant)?;
    let context = workloads::Context::new()?;
    let mut results = Vec::new();
    for case in workloads::cases(&manifest) {
        results.push(workloads::run_case(&case, &context, variant, parameters)?);
    }
    let report = Report {
        schema_version: 1,
        variant,
        mode,
        formal_acceptance: false,
        manifest_sha256: manifest::MANIFEST_SHA256,
        manifest: serde_json::from_str(manifest::MANIFEST_JSON).map_err(|e| e.to_string())?,
        metadata,
        parameters: serde_json::json!({"warmup_iterations":parameters.warmup_iterations,"throughput_iterations_per_worker":parameters.throughput_iterations_per_worker,"latency_iterations_per_worker":parameters.latency_iterations_per_worker,"live_hold_iterations":parameters.live_hold_iterations}),
        measurement_limitations: vec![
            "Smoke validates entrypoints and schema; Linux formal acceptance is a manual user conclusion.",
            "Latency samples time individual complete operations including timer overhead; no timer subtraction or batch-average tail claim.",
            "Remote latency waits for actual free acknowledgement; channel and synchronization cost is included identically in all variants.",
            "Throughput and latency run separate passes. Fixed warmup runs on each measurement worker before CPU/wall sampling; their thread-local caches remain live. Throughput wall time includes start synchronization and join/drain; it is not hook-only CPU cost.",
            "Held-space is a separate pass with sequential construction and retained objects; no transient resize peak or allocator/OS instantaneous snapshot claim.",
            "Remote held-space releases all retained objects remotely; timing uses the frozen every-N rule. Nominal proportions and actual acknowledged remote operation counts/fractions are reported separately; warmup is excluded.",
            "RSS and jemalloc resident/active/allocated are distinct. Non-Linux current RSS is unavailable; peak RSS is separately labeled.",
            "Histograms describe the size cycle or resize chain, not a sampled production census. Chunk Vec metadata and transport overhead are included in timing.",
            "Attribution scope/helper overhead is included only in attributing variants; the underlying operation stream is shared by all variants.",
        ],
        results,
    };
    fs::create_dir_all(&out).map_err(|e| e.to_string())?;
    fs::write(
        &target,
        serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    println!(
        "report={} manifest_sha256={} formal_acceptance=false",
        target.display(),
        manifest::MANIFEST_SHA256
    );
    Ok(())
}

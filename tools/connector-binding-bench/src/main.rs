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

use std::env;
use std::fs;
use std::path::PathBuf;

use serde::Serialize;
use sha2::{Digest, Sha256};

const WORKLOAD: &str = include_str!("../workloads.toml");

#[derive(Serialize)]
struct Output {
    schema_version: u8,
    git_head: String,
    profile: String,
    workload_sha256: String,
    samples: u32,
    measurements: Vec<Measurement<'static>>,
}

#[derive(Serialize)]
struct Measurement<'a> {
    case: &'a str,
    status: &'a str,
    reason: &'a str,
}

fn main() -> Result<(), String> {
    let mut output = None;
    let mut profile = "dev-opt".to_owned();
    let mut samples = 5_u32;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => output = args.next().map(PathBuf::from),
            "--profile" => profile = args.next().ok_or("--profile requires a value")?,
            "--samples" => {
                samples = args
                    .next()
                    .ok_or("--samples requires a value")?
                    .parse()
                    .map_err(|_| "--samples must be an unsigned integer")?
            }
            "--help" | "-h" => {
                println!(
                    "usage: connector-binding-bench --output <path> [--profile <name>] [--samples <n>]"
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let output = output.ok_or("--output is required")?;
    let digest = format!("{:x}", Sha256::digest(WORKLOAD.as_bytes()));
    let result = Output {
        schema_version: 1,
        git_head: git_head(),
        profile,
        workload_sha256: digest,
        samples,
        measurements: workload_cases()
            .iter()
            .map(|case| Measurement {
                case,
                status: "unsupported",
                reason: "no product adapter is installed; this result is intentionally not a synthetic timing",
            })
            .collect(),
    };
    let encoded = serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(output, encoded).map_err(|error| error.to_string())?;
    Ok(())
}

fn git_head() -> String {
    option_env!("NOVAROCKS_BENCH_GIT_HEAD")
        .unwrap_or("unknown")
        .to_owned()
}

fn workload_cases() -> [&'static str; 6] {
    [
        "fe-bootstrap-no-io",
        "fe-63-healthy-plus-1-hang",
        "be-cold-and-retained",
        "be-failure-storm",
        "replacement-retire-churn",
        "planning-and-task-update-encoding",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_identity_covers_every_required_case() {
        for case in workload_cases() {
            assert!(WORKLOAD.contains(case), "workload omits {case}");
        }
        assert!(WORKLOAD.contains("1024"));
        assert!(WORKLOAD.contains("63"));
        assert!(WORKLOAD.contains("64"));
        assert!(WORKLOAD.contains("256"));
    }

    #[test]
    fn output_schema_is_stable_and_explicit_about_missing_adapters() {
        let output = Output {
            schema_version: 1,
            git_head: "test".to_owned(),
            profile: "dev-opt".to_owned(),
            workload_sha256: format!("{:x}", Sha256::digest(WORKLOAD.as_bytes())),
            samples: 5,
            measurements: workload_cases()
                .iter()
                .map(|case| Measurement {
                    case,
                    status: "unsupported",
                    reason: "no product adapter is installed; this result is intentionally not a synthetic timing",
                })
                .collect(),
        };
        let value = serde_json::to_value(output).expect("serialize output");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["measurements"].as_array().unwrap().len(), 6);
        assert_eq!(value["measurements"][0]["status"], "unsupported");
    }
}

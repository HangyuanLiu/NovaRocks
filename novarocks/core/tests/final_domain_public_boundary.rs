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

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::OnceLock;

fn current_target_dir() -> PathBuf {
    std::env::current_exe()
        .expect("test executable path")
        .parent()
        .expect("test executable lives in target/deps")
        .parent()
        .expect("target profile directory")
        .parent()
        .expect("Cargo target directory")
        .to_path_buf()
}

struct CurrentRlibs {
    novarocks: PathBuf,
    execution: PathBuf,
}

fn current_rlibs() -> &'static CurrentRlibs {
    static RLIBS: OnceLock<CurrentRlibs> = OnceLock::new();
    RLIBS.get_or_init(|| {
        let mut command = Command::new(env!("CARGO"));
        command
            .arg("build")
            .arg("--locked")
            .arg("--offline")
            .arg("--manifest-path")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .and_then(|path| path.parent())
                    .expect("NovaRocks workspace root")
                    .join("Cargo.toml"),
            )
            .arg("-p")
            .arg("novarocks")
            .arg("--lib")
            .arg("--no-default-features")
            .arg("--message-format=json-render-diagnostics")
            .env("CARGO_TARGET_DIR", current_target_dir());
        let mut features = Vec::new();
        if cfg!(feature = "foundationdb-provider") {
            features.push("foundationdb-provider");
        }
        if cfg!(feature = "mysql-state-store-provider") {
            features.push("mysql-state-store-provider");
        }
        if cfg!(feature = "state-store-test-hooks") {
            features.push("state-store-test-hooks");
        }
        if cfg!(feature = "ssb") {
            features.push("ssb");
        }
        if !features.is_empty() {
            command.arg("--features").arg(features.join(","));
        }
        let output = command.output().expect("build current novarocks library");
        assert!(
            output.status.success(),
            "current novarocks library build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let messages = String::from_utf8(output.stdout)
            .expect("Cargo JSON output is UTF-8")
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect::<Vec<_>>();
        CurrentRlibs {
            novarocks: artifact_rlib(&messages, "novarocks"),
            execution: artifact_rlib(&messages, "novarocks_execution"),
        }
    })
}

fn artifact_rlib(messages: &[serde_json::Value], target_name: &str) -> PathBuf {
    let artifacts = messages
        .iter()
        .filter(|message| message["reason"] == "compiler-artifact")
        .filter(|message| message["target"]["name"] == target_name)
        .filter(|message| {
            message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "lib"))
        })
        .flat_map(|message| message["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|filename| filename.as_str().map(PathBuf::from))
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "rlib")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        artifacts.len(),
        1,
        "Cargo must report exactly one current {target_name} rlib, got {artifacts:?}"
    );
    artifacts.into_iter().next().unwrap()
}

fn compile_external(source: &str) -> Output {
    let workspace = tempfile::tempdir().expect("temporary external caller workspace");
    let source_path = workspace.path().join("external.rs");
    fs::write(&source_path, source).expect("external caller source");
    let rlibs = current_rlibs();
    let deps = rlibs
        .execution
        .parent()
        .expect("execution rlib dependency directory");
    let metadata_path = workspace.path().join("external.rmeta");

    Command::new("rustc")
        .arg("--edition=2024")
        .arg("--crate-name=external_final_domain_boundary")
        .arg("--emit=metadata")
        .arg("-o")
        .arg(metadata_path)
        .arg(&source_path)
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--extern")
        .arg(format!("novarocks={}", rlibs.novarocks.display()))
        .arg("--extern")
        .arg(format!("novarocks_execution={}", rlibs.execution.display()))
        .output()
        .expect("run rustc for external caller")
}

fn assert_inaccessible(output: Output, capability: &str, diagnostic_anchor: &str) {
    assert!(
        !output.status.success(),
        "external caller unexpectedly reached {capability}"
    );
    let diagnostics = String::from_utf8_lossy(&output.stderr);
    assert!(
        (diagnostics.contains("private")
            || diagnostics.contains("unresolved import")
            || diagnostics.contains("could not find"))
            && diagnostics.contains(diagnostic_anchor),
        "external caller failed for an unexpected reason: {diagnostics}"
    );
}

#[test]
fn external_callers_cannot_reach_final_domain_issuance_authority() {
    let output = compile_external(
        "use novarocks::runtime_filter::port::final_domain::CompletionFenceAuthority;\n\
         fn main() { let _ = core::mem::size_of::<CompletionFenceAuthority>(); }\n",
    );

    assert_inaccessible(
        output,
        "runtime_filter final-domain authority",
        "runtime_filter",
    );
}

#[test]
fn external_callers_cannot_open_aggregate_final_domain_sessions() {
    let output = compile_external(
        "use novarocks::exec::operators::AggregateFinalDomainSessionBuilder;\n\
         fn main() { let _ = core::mem::size_of::<AggregateFinalDomainSessionBuilder>(); }\n",
    );

    assert_inaccessible(output, "aggregate final-domain session builder", "exec");
}

#[test]
fn fragment_kernel_exposes_only_the_canonical_construction_and_runtime_paths() {
    let output = compile_external(
        "use novarocks_execution::exec::fragment::FragmentProgramBuilder;\n\
         use novarocks_execution::exec::node::ExecPlanBuilder;\n\
         use novarocks_execution::runtime::fragment::{FragmentInstanceSpec, FragmentSubmission};\n\
         fn main() {\n\
             let _ = core::mem::size_of::<ExecPlanBuilder>();\n\
             let _ = core::mem::size_of::<FragmentProgramBuilder>();\n\
             let _ = core::mem::size_of::<FragmentInstanceSpec>();\n\
             let _ = core::mem::size_of::<FragmentSubmission>();\n\
         }\n",
    );
    assert!(
        output.status.success(),
        "canonical fragment-kernel API must be externally usable: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn external_callers_cannot_reach_fragment_kernel_legacy_paths() {
    for (source, capability, diagnostic_anchor) in [
        (
            "use novarocks::runtime::fragment::instance::FragmentInstanceSpec;\n\
             fn main() { let _ = core::mem::size_of::<FragmentInstanceSpec>(); }\n",
            "instance",
            "runtime",
        ),
        (
            "use novarocks::runtime::fragment::io::FragmentEventSink;\n\
             fn main() { let _ = core::mem::size_of::<&dyn FragmentEventSink>(); }\n",
            "io",
            "runtime",
        ),
        (
            "use novarocks::exec::operators::DataStreamSinkFactory;\n\
             fn main() { let _ = core::mem::size_of::<DataStreamSinkFactory>(); }\n",
            "operators",
            "exec",
        ),
        (
            "use novarocks::exec::pipeline::fragment_context::FragmentContext;\n\
             fn main() { let _ = core::mem::size_of::<FragmentContext>(); }\n",
            "pipeline",
            "exec",
        ),
        (
            "use novarocks::runtime::query_context::QueryContextManager;\n\
             fn main() { let _ = core::mem::size_of::<QueryContextManager>(); }\n",
            "query_context",
            "runtime",
        ),
    ] {
        assert_inaccessible(compile_external(source), capability, diagnostic_anchor);
    }
}

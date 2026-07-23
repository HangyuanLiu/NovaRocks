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

fn novarocks_rlib() -> PathBuf {
    let deps = std::env::current_exe()
        .expect("test executable path")
        .parent()
        .expect("test executable lives in target/deps")
        .to_path_buf();
    fs::read_dir(&deps)
        .expect("target dependency directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("libnovarocks-") && name.ends_with(".rlib"))
        })
        .expect("novarocks rlib for external-public-API check")
}

fn compile_external(source: &str) -> Output {
    let workspace = tempfile::tempdir().expect("temporary external caller workspace");
    let source_path = workspace.path().join("external.rs");
    fs::write(&source_path, source).expect("external caller source");
    let rlib = novarocks_rlib();
    let deps = rlib.parent().expect("rlib dependency directory");

    Command::new("rustc")
        .arg("--edition=2024")
        .arg("--crate-name=external_final_domain_boundary")
        .arg("--emit=metadata")
        .arg(&source_path)
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--extern")
        .arg(format!("novarocks={}", rlib.display()))
        .output()
        .expect("run rustc for external caller")
}

fn assert_private(output: Output, capability: &str) {
    assert!(
        !output.status.success(),
        "external caller unexpectedly reached {capability}"
    );
    let diagnostics = String::from_utf8_lossy(&output.stderr);
    assert!(
        diagnostics.contains("private") && diagnostics.contains(capability),
        "external caller failed for an unexpected reason: {diagnostics}"
    );
}

#[test]
fn external_callers_cannot_reach_final_domain_issuance_authority() {
    let output = compile_external(
        "use novarocks::runtime_filter::port::final_domain::CompletionFenceAuthority;\n\
         fn main() { let _ = core::mem::size_of::<CompletionFenceAuthority>(); }\n",
    );

    assert_private(output, "runtime_filter");
}

#[test]
fn external_callers_cannot_open_aggregate_final_domain_sessions() {
    let output = compile_external(
        "use novarocks::exec::operators::AggregateFinalDomainSessionBuilder;\n\
         fn main() { let _ = core::mem::size_of::<AggregateFinalDomainSessionBuilder>(); }\n",
    );

    assert_private(output, "AggregateFinalDomainSessionBuilder");
}

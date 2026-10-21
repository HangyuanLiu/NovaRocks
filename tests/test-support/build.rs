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

use std::path::PathBuf;
use std::process::Command;

fn main() {
    let target = std::env::var("TARGET").expect("Cargo target");
    let host = std::env::var("HOST").expect("Cargo host");
    assert_eq!(
        host, target,
        "test monitor delivery supports native targets only"
    );
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo OUT_DIR"))
        .join("novarocks-test-monitor");
    for path in [
        "build.rs",
        "src/monitor/main.rs",
        "src/unix/mod.rs",
        "src/unix/process_group.rs",
        "src/run_retention.rs",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    let mut command = Command::new(std::env::var_os("RUSTC").expect("Cargo rustc"));
    command
        .env("NOVAROCKS_TEST_MONITOR", &output)
        .args([
            "src/monitor/main.rs",
            "--edition=2024",
            "--crate-name=novarocks_test_monitor",
            "--target",
            &target,
            "-o",
        ])
        .arg(&output)
        .arg(format!(
            "-Copt-level={}",
            std::env::var("OPT_LEVEL").expect("Cargo optimization")
        ))
        .arg(format!(
            "-Cdebug-assertions={}",
            std::env::var_os("CARGO_CFG_DEBUG_ASSERTIONS").is_some()
        ))
        .arg(format!(
            "-Cpanic={}",
            std::env::var("CARGO_CFG_PANIC").expect("Cargo panic strategy")
        ));
    if let Some(linker) = std::env::var_os("RUSTC_LINKER") {
        command.arg("-C").arg({
            let mut value = std::ffi::OsString::from("linker=");
            value.push(linker);
            value
        });
    }
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        command.args(flags.split('\u{1f}').filter(|flag| !flag.is_empty()));
    }
    let result = command
        .status()
        .expect("execute rustc for std-only test monitor");
    assert!(
        result.success(),
        "std-only test monitor compilation failed: {result}"
    );
    println!(
        "cargo:rustc-env=NOVAROCKS_TEST_MONITOR={}",
        output.display()
    );
}

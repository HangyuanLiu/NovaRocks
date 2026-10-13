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
use std::process::Command;

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Emit only current-package facts. Profile declarations do not certify final flags.
pub fn emit() {
    println!("cargo:rerun-if-changed={}", file!());
    for key in [
        "NOVAROCKS_BUILD_PROFILE",
        "NOVAROCKS_NATIVE_BUILD_IDENTITY",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTC",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    for path in ["HEAD", "refs", "packed-refs"] {
        if let Some(path) = command_output("git", &["rev-parse", "--git-path", path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let declared = env::var("NOVAROCKS_BUILD_PROFILE").ok().filter(|value| {
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    });
    let rustc = env::var("RUSTC")
        .ok()
        .and_then(|program| command_output(&program, &["--version"]));
    let commit = command_output("git", &["rev-parse", "--verify", "HEAD"])
        .filter(|value| value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit()));
    for (name, value) in [
        ("SOURCE_COMMIT", commit),
        (
            "EXPLICIT_IDENTITY",
            env::var("NOVAROCKS_NATIVE_BUILD_IDENTITY").ok(),
        ),
        ("DECLARED_PROFILE", declared),
        ("PACKAGE_OPT_LEVEL", env::var("OPT_LEVEL").ok()),
        ("PACKAGE_DEBUG_INFO", env::var("DEBUG").ok()),
        ("TARGET", env::var("TARGET").ok()),
        ("RUSTC_IDENTITY", rustc),
    ] {
        let value = value.unwrap_or_default();
        assert!(
            !value.contains(['\r', '\n']),
            "build information must be a single line"
        );
        println!("cargo:rustc-env=NOVAROCKS_BUILDINFO_{name}={value}");
    }
}

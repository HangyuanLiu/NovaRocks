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
use std::path::Path;
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

fn full_commit(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Freeze source only at executable edges. HEAD's own reflog covers loose,
/// packed and detached references without observing other worktrees' refs.
pub fn emit(diagnostic_identity_env: &str) {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR");
    let helper = Path::new(&manifest)
        .ancestors()
        .map(|directory| directory.join("build-support/provenance.rs"))
        .find(|candidate| candidate.is_file())
        .expect("source build helper must be present in the workspace");
    println!("cargo:rerun-if-changed={}", helper.display());
    for key in [
        diagnostic_identity_env,
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTC",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    let head = command_output("git", &["rev-parse", "--git-path", "HEAD"]);
    let log = command_output("git", &["rev-parse", "--git-path", "logs/HEAD"]);
    for path in head.iter().chain(log.iter()) {
        println!("cargo:rerun-if-changed={path}");
    }
    let git_commit = command_output("git", &["rev-parse", "--verify", "HEAD"])
        .filter(|value| full_commit(value));
    // An existing nonempty HEAD log is the actual signal. logAllRefUpdates=false
    // prevents automatic creation, but does not stop an existing log appending.
    let usable_log = log.as_deref().is_some_and(|path| {
        Path::new(path)
            .metadata()
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    });
    let normal_source = usable_log && git_commit.is_some();
    let fallback_identity = if normal_source {
        None
    } else {
        // Fallback inputs are shared only when source notifications are unavailable.
        for key in ["NOVAROCKS_NATIVE_BUILD_IDENTITY", "NOVAROCKS_SOURCE_COMMIT"] {
            println!("cargo:rerun-if-env-changed={key}");
        }
        Some(env::var("NOVAROCKS_NATIVE_BUILD_IDENTITY")
            .expect("source identity requires a usable worktree HEAD reflog or NOVAROCKS_NATIVE_BUILD_IDENTITY"))
    };
    let explicit = env::var(diagnostic_identity_env).ok().or(fallback_identity);
    let native_identity = explicit
        .clone()
        .or_else(|| git_commit.clone())
        .expect("the executable requires an explicit source identity");
    assert!(
        (1..=128).contains(&native_identity.len())
            && native_identity != "unknown"
            && native_identity
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')),
        "{diagnostic_identity_env} or fallback NOVAROCKS_NATIVE_BUILD_IDENTITY must contain 1-128 ASCII letters, digits, '.', '_' or '-', and must not be 'unknown'"
    );
    let source = if normal_source {
        git_commit
    } else {
        git_commit.or_else(|| env::var("NOVAROCKS_SOURCE_COMMIT").ok())
    };
    if let Some(source) = &source {
        assert!(
            full_commit(source),
            "NOVAROCKS_SOURCE_COMMIT must be a full 40-character Git commit"
        );
    }
    let short = source
        .as_deref()
        .map(|source| &source[..8])
        .unwrap_or(&native_identity);
    let short_version = format!("novarocks-{short}");
    let commit_time = command_output("git", &["log", "-1", "--format=%ci"]).unwrap_or_default();
    let full_version = if commit_time.is_empty() {
        short_version.clone()
    } else {
        format!("{short_version} ({commit_time})")
    };
    let rustc = env::var("RUSTC")
        .ok()
        .and_then(|program| command_output(&program, &["--version"]));
    for (key, value) in [
        ("SOURCE_COMMIT", source),
        ("EXPLICIT_IDENTITY", explicit),
        ("NATIVE_IDENTITY", Some(native_identity)),
        ("SHORT_VERSION", Some(short_version)),
        ("FULL_VERSION", Some(full_version)),
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
        println!("cargo:rustc-env=NOVAROCKS_BUILDINFO_{key}={value}");
    }
}

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

const NATIVE_BUILD_IDENTITY_ENV: &str = "NOVAROCKS_NATIVE_BUILD_IDENTITY";

fn git_output(args: &[&str]) -> Option<String> {
    std::process::Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|output| !output.is_empty())
}

fn is_valid_native_build_identity(identity: &str) -> bool {
    let valid_char = |character: char| {
        character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
    };
    (1..=128).contains(&identity.len())
        && identity != "unknown"
        && identity.chars().all(valid_char)
}

fn is_full_git_commit(commit: &str) -> bool {
    commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn main() {
    println!("cargo:rerun-if-env-changed={NATIVE_BUILD_IDENTITY_ENV}");
    if let Some(head_path) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }
    for git_path in ["refs", "packed-refs"] {
        if let Some(path) = git_output(&["rev-parse", "--git-path", git_path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }

    let explicit_identity = std::env::var(NATIVE_BUILD_IDENTITY_ENV).ok();
    let full_commit = git_output(&["rev-parse", "--verify", "HEAD"])
        .filter(|commit| is_full_git_commit(commit));
    let native_build_identity = explicit_identity
        .or_else(|| full_commit.clone())
        .expect("novarocks-version requires NOVAROCKS_NATIVE_BUILD_IDENTITY or a full Git commit");
    assert!(
        is_valid_native_build_identity(&native_build_identity),
        "{NATIVE_BUILD_IDENTITY_ENV} must contain 1-128 ASCII letters, digits, '.', '_' or '-', and must not be 'unknown'"
    );
    let short_commit = full_commit
        .as_deref()
        .map(|commit| &commit[..8])
        .unwrap_or(&native_build_identity)
        .to_owned();
    let commit_time = git_output(&["log", "-1", "--format=%ci"]).unwrap_or_default();

    println!("cargo:rustc-env=NOVAROCKS_GIT_HASH={short_commit}");
    println!("cargo:rustc-env=NOVAROCKS_GIT_TIME={commit_time}");
    println!("cargo:rustc-env=NOVAROCKS_NATIVE_BUILD_IDENTITY={native_build_identity}");
}

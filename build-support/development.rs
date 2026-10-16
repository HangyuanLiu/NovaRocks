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

/// Development seams follow the effective Cargo debug-assertions setting.
pub fn emit() {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("package manifest directory"),
    );
    let helper = manifest
        .ancestors()
        .map(|directory| directory.join("build-support/development.rs"))
        .find(|path| path.is_file())
        .expect("workspace development cfg helper");
    println!("cargo:rerun-if-changed={}", helper.display());
    println!("cargo:rustc-check-cfg=cfg(novarocks_dev_controls)");
    if std::env::var_os("CARGO_CFG_DEBUG_ASSERTIONS").is_some() {
        println!("cargo:rustc-cfg=novarocks_dev_controls");
    }
}

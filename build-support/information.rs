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

/// Facts frozen by the executable package, never rediscovered by a library.
#[derive(Clone, Copy, Debug)]
pub struct BuildInformation {
    pub package: &'static str,
    pub version: &'static str,
    pub source_commit: &'static str,
    pub explicit_identity: &'static str,
    pub native_build_identity: &'static str,
    pub short_version: &'static str,
    pub full_version: &'static str,
    pub opt_level: &'static str,
    pub debug_info: &'static str,
    pub target: &'static str,
    pub rustc_identity: &'static str,
    pub debug_assertions: bool,
    pub panic_unwind: bool,
}

#[cfg(test)]
impl BuildInformation {
    pub const fn fixture() -> Self {
        Self {
            package: "fixture-cli",
            version: "0.1.0",
            source_commit: "",
            explicit_identity: "fixture-identity",
            native_build_identity: "fixture-identity",
            short_version: "novarocks-fixture",
            full_version: "novarocks-fixture",
            opt_level: "0",
            debug_info: "line-tables-only",
            target: "fixture-target",
            rustc_identity: "fixture-rustc",
            debug_assertions: false,
            panic_unwind: true,
        }
    }
}

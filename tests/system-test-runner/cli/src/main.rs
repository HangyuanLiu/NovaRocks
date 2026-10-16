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

fn main() {
    novarocks_system_test_runner::run(
        novarocks_system_test_runner::build_information::BuildInformation {
            package: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            source_commit: env!("NOVAROCKS_BUILDINFO_SOURCE_COMMIT"),
            explicit_identity: env!("NOVAROCKS_BUILDINFO_EXPLICIT_IDENTITY"),
            native_build_identity: env!("NOVAROCKS_BUILDINFO_NATIVE_IDENTITY"),
            short_version: env!("NOVAROCKS_BUILDINFO_SHORT_VERSION"),
            full_version: env!("NOVAROCKS_BUILDINFO_FULL_VERSION"),
            opt_level: env!("NOVAROCKS_BUILDINFO_PACKAGE_OPT_LEVEL"),
            debug_info: env!("NOVAROCKS_BUILDINFO_PACKAGE_DEBUG_INFO"),
            target: env!("NOVAROCKS_BUILDINFO_TARGET"),
            rustc_identity: env!("NOVAROCKS_BUILDINFO_RUSTC_IDENTITY"),
            debug_assertions: cfg!(debug_assertions),
            panic_unwind: cfg!(panic = "unwind"),
        },
    )
}

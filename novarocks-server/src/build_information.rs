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

#[cfg(feature = "normal-build")]
const BUILD_KIND: &str = "normal-build";
#[cfg(all(feature = "test-controls", not(feature = "normal-build")))]
const BUILD_KIND: &str = "test-controls";
#[cfg(not(any(feature = "normal-build", feature = "test-controls")))]
const BUILD_KIND: &str = "unknown";

fn owners() -> Vec<(&'static str, &'static [&'static str])> {
    vec![
        crate::compiled_test_capabilities(),
        novarocks_failpoint::compiled_test_capabilities(),
        novarocks_frontend_application::compiled_test_capabilities(),
        novarocks_mysql_adapter::compiled_test_capabilities(),
        novarocks_query_application::compiled_test_capabilities(),
        novarocks_native_adapter::compiled_test_capabilities(),
        novarocks_worker::compiled_test_capabilities(),
        novarocks_execution::compiled_test_capabilities(),
        novarocks_fs::compiled_test_capabilities(),
        novarocks_connector_iceberg::compiled_test_capabilities(),
        novarocks_mv_application::compiled_test_capabilities(),
        novarocks_version::compiled_test_capabilities(),
    ]
}

fn known(value: &str) -> serde_json::Value {
    if value.is_empty() {
        serde_json::Value::Null
    } else {
        value.into()
    }
}

// Design: ADR-0174 (docs/adr/ADR-0174-test-capabilities-and-finite-role-authorization.md)
pub fn value(debug_assertions: bool, panic_unwind: bool) -> serde_json::Value {
    let mut capabilities = owners().into_iter().map(|(owner, compiled)|
        serde_json::json!({"owner": owner, "compiled_capabilities": compiled}))
        .collect::<Vec<_>>();
    capabilities.sort_by(|a, b| a["owner"].as_str().cmp(&b["owner"].as_str()));
    serde_json::json!({
        "schema_version": 1,
        "package": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "source_commit": known(env!("NOVAROCKS_BUILDINFO_SOURCE_COMMIT")),
        "explicit_build_identity": known(env!("NOVAROCKS_BUILDINFO_EXPLICIT_IDENTITY")),
        "build_kind": BUILD_KIND,
        "capabilities": capabilities,
        "declared_profile": known(env!("NOVAROCKS_BUILDINFO_DECLARED_PROFILE")),
        "package_profile": {
            "opt_level": known(env!("NOVAROCKS_BUILDINFO_PACKAGE_OPT_LEVEL")),
            "debug_info": known(env!("NOVAROCKS_BUILDINFO_PACKAGE_DEBUG_INFO"))
        },
        "actual_cfg": {
            "target": known(env!("NOVAROCKS_BUILDINFO_TARGET")),
            "debug_assertions": debug_assertions,
            "panic": if panic_unwind { "unwind" } else { "abort" },
            "pointer_width": usize::BITS,
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH
        },
        "rustc_identity": known(env!("NOVAROCKS_BUILDINFO_RUSTC_IDENTITY")),
        "explicitly_unknown_settings": ["final_lto", "final_codegen_units", "dependency_profile_overrides", "additional_compiler_flag_effects"],
        "release_certification": "not-certified-by-self-report"
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn local_cfg_and_unknown_facts_remain_separate() {
        let value = super::value(false, true);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["actual_cfg"]["debug_assertions"], false);
        assert_eq!(value["actual_cfg"]["panic"], "unwind");
        assert_eq!(super::known(""), serde_json::Value::Null);
        assert!(
            !value["explicitly_unknown_settings"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

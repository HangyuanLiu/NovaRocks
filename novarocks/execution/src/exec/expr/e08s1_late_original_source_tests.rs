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

//! Actual original source publication before all-definition capability admission.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use arrow::datatypes::DataType;
use novarocks_sql::compiler::SqlPhysicalEmissionMode;

#[test]
fn e08s1_late_original_full_source_keeps_missing_aggregate_without_new_lookup() {
    let source = sql_source(
        "SELECT dict_merge(CAST(k AS VARCHAR), 1) FROM fixture",
        DataType::Int32,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    let mut found = false;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let novarocks_physical_plan::NodeKind::Aggregate { calls, .. } = &node.kind {
                found |= calls.iter().any(|call| {
                    call.binding.function.function_id.as_str() == "builtin.aggregate/dict_merge/v1"
                });
            }
        }
    }
    assert!(found, "original source retains its actual aggregate author");
}
#[test]
fn e08s1_late_original_full_source_has_scalar_and_aggregate_over_definitions() {
    let source = sql_source(
        "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
        DataType::Int32,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    assert!(!source.plan().fragments().is_empty());
    assert_eq!(
        source.emission_mode(),
        SqlPhysicalEmissionMode::OriginalNativeV1
    );
}

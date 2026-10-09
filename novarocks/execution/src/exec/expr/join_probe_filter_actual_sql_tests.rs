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
//! Permanent original SQL dependency witness for probe-key runtime filters.
use super::filter_conjunction_actual_sql_compiler_tests::compiler_results;
use super::ndv_filter_actual_sql_source_tests::sql_source_with_columns;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use arrow::datatypes::DataType;
use novarocks_physical_plan::RuntimeFilterConsumerTarget;
use novarocks_sql::compiler::SqlPhysicalEmissionMode;

fn sources() -> Vec<novarocks_sql::compiler::SqlAuthoredPhysicalPlan> {
    let original = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/sql/correctness/filter/sql/filter_multiple_subqueries.sql"));
    let selects = &original[original.find("-- query 2\n").unwrap()..];
    let queries = selects.split(';').map(|statement| {
        statement.lines().filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>().join("\n").trim().to_owned()
    }).filter(|statement| !statement.is_empty()).collect::<Vec<_>>();
    assert_eq!(queries.len(), 5);
    queries.into_iter().map(|statement| {
        assert!(statement.starts_with("SELECT"));
        sql_source_with_columns(
            &statement.replace("${case_db}", "fixture"),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            &[("id", DataType::Int64), ("v", DataType::Int64)],
        )
    }).collect()
}

#[test]
fn join_probe_filter_actual_original_sql_full_target_contract() {
    let mut probes = 0;
    for (query, source) in sources().into_iter().enumerate() {
        for (id, filter) in source.plan().runtime_filters() {
            for consumer in &filter.consumers {
                eprintln!("original subquery query={} filter={id:?} consumer={consumer:?} contract={filter:?}", query + 2);
                if matches!(consumer.target, RuntimeFilterConsumerTarget::JoinProbeKey { .. }) {
                    probes += 1;
                }
            }
        }
    }
    assert!(probes > 0, "original required SQL must exercise the missing probe-key consumer");
}

#[test]
fn join_probe_filter_actual_original_sql_complete_compilation() {
    let functions = installed_builtin_owner_catalogue();
    for source in sources() {
        for (fragment, result) in compiler_results(&source, &functions) {
            result.unwrap_or_else(|error| panic!("original subquery fragment {fragment:?}: {error}"));
        }
    }
}

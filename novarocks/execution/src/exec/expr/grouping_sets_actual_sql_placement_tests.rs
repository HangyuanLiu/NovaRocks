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

//! Original grouping-set SQL placement source and permanent full-fragment lowering.
use super::filter_conjunction_actual_sql_compiler_tests::{compiler_results, source_packages};
use super::ndv_filter_actual_sql_source_tests::sql_source_with_columns;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use arrow::datatypes::DataType;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};

const ORIGINAL_INPUT: &str = "SELECT GROUPING(a) AS g, a, count(*) AS rows_seen,\n       count(a) AS nonnull_a, count(DISTINCT a) AS distinct_a,\n       sum(a) AS sum_a, sum(b) AS sum_b,\n       array_agg(a ORDER BY b) AS ordered_a\nFROM fixture.t_grouping_input\nGROUP BY ROLLUP(a)\nORDER BY g, a NULLS FIRST;";
const GROUPING_SET: &str =
    "select v1, sum(v2), min(v2 + v3) from fixture.t0 group by grouping sets((v1, v2));";
fn sources(mode: SqlPhysicalEmissionMode) -> [(&'static str, SqlAuthoredPhysicalPlan); 2] {
    [
        (
            "grouping_sets_original_aggregate_input/query2",
            sql_source_with_columns(
                ORIGINAL_INPUT,
                mode,
                &[("a", DataType::Int32), ("b", DataType::Int32)],
            ),
        ),
        (
            "agg_test_grouping_set/query4",
            sql_source_with_columns(
                GROUPING_SET,
                mode,
                &[
                    ("v1", DataType::Int64),
                    ("v2", DataType::Int64),
                    ("v3", DataType::Int64),
                    ("v4", DataType::Utf8),
                ],
            ),
        ),
    ]
}
fn source_probe(mode: SqlPhysicalEmissionMode) {
    for (case, source) in sources(mode) {
        assert!(!source.plan().fragments().is_empty());
        eprintln!(
            "grouping actual source case={case} mode={mode:?} result={:?} edges={:?}",
            source.plan().result_port(),
            source.plan().edges()
        );
        for (fragment_id, fragment) in source.plan().fragments() {
            for node in fragment.nodes().values() {
                eprintln!(
                    "grouping actual node case={case} mode={mode:?} fragment={fragment_id:?} node={node:?}"
                );
                for input in &node.inputs {
                    let child = fragment
                        .nodes()
                        .get(input)
                        .expect("original child belongs to fragment");
                    eprintln!(
                        "grouping actual placement input={:?} output={:?} requirements={:?}",
                        child.id, child.output_properties, node.required_inputs
                    );
                }
            }
            for (id, value) in fragment.values() {
                eprintln!(
                    "grouping actual value case={case} fragment={fragment_id:?} id={id:?} value={value:?}"
                );
            }
        }
        // This uses the existing full package owner with same-source semantics,
        // uses/calls and frozen provider facts; no constructed physical fragment.
        let packages = source_packages(&source);
        assert_eq!(packages.len(), source.plan().fragments().len());
        for (id, package) in packages {
            eprintln!(
                "grouping actual package case={case} fragment={id:?} root={:?}",
                package.fragment().root()
            );
        }
    }
}
fn permanent_compilation(mode: SqlPhysicalEmissionMode) {
    let functions = installed_builtin_owner_catalogue();
    for (case, source) in sources(mode) {
        let programs = compiler_results(&source, &functions);
        assert_eq!(programs.len(), source.plan().fragments().len());
        for (fragment, result) in programs {
            let program = result.unwrap_or_else(|error| panic!("complete original grouping SQL must compile: case={case} mode={mode:?} fragment={fragment:?}: {error:?}"));
            assert!(!program.graph().nodes().is_empty());
        }
    }
}
#[test]
fn grouping_sets_actual_original_source_and_packages() {
    source_probe(SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn grouping_sets_actual_candidate_source_and_packages() {
    source_probe(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration);
}
#[test]
fn grouping_sets_actual_original_complete_compilation() {
    permanent_compilation(SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn grouping_sets_actual_candidate_complete_compilation() {
    permanent_compilation(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration);
}

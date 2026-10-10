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
//! Original corpus emission and authenticated journal loans; no replacement expression graph.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, PureCallLifecycle, ResolvedFunctionBinding,
};
use novarocks_physical_plan::ExprKind;
use novarocks_sql::compiler::{
    SqlCallDependencySite, SqlCompileControl, SqlPhysicalEmissionMode, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
const LENGTH_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_length_limit_semantics.sql"
));
const INTEGER_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
));
// These two members share the complete existing measurement family. They are
// explicit source probes, not additional owners or inferred corpus coverage.
const REMAINING_MEASUREMENT: &str = "SELECT ascii('A') AS a, char_length('é') AS n";
pub(super) const NAMES: [&str; 7] = [
    "ascii",
    "length",
    "char_length",
    "repeat",
    "space",
    "lpad",
    "rpad",
];
pub(super) fn calls(
    index: usize,
    mode: SqlPhysicalEmissionMode,
) -> Vec<(String, ResolvedFunctionBinding)> {
    let sql = [LENGTH_SQL, INTEGER_SQL, REMAINING_MEASUREMENT][index];
    let owner = sql_source(sql, DataType::Int64, mode);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut out = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for (_, source) in fragment.expressions().iter() {
            let ExprKind::FunctionCall { function, .. } = &source.kind else {
                continue;
            };
            // Source selection is test-only. Production admission borrows the installed owner.
            let Some(name) = NAMES
                .iter()
                .find(|name| function.function_id.as_str() == format!("builtin.scalar/{name}/v1"))
            else {
                continue;
            };
            let loan = owner
                .borrow_call_dependency_observed(
                    SqlCallDependencySite::Expression { fragment, source },
                    &mut work,
                )
                .unwrap();
            let binding = loan.original_binding().resolved();
            assert_eq!(
                binding.logical_argument_count,
                binding.selected.argument_types.len()
            );
            assert!(
                binding
                    .selected
                    .argument_types
                    .iter()
                    .all(|a| matches!(a, FunctionArgumentType::Value(_)))
            );
            assert!(matches!(
                binding.selected.result_type,
                FunctionResultType::Scalar(_)
            ));
            eprintln!(
                "string static original source case={index} mode={mode:?} fragment={:?} source={:?} binding={binding:?}",
                fragment.id(),
                source.id
            );
            out.push(((*name).to_string(), binding.clone()));
        }
    }
    work.finish().unwrap();
    assert!(
        !out.is_empty(),
        "actual original emitted selected calls are required"
    );
    out
}
fn original(index: usize) {
    let catalog = builtin_sql_function_catalog().snapshot();
    let control = SqlCompileControl::unbounded();
    for (_, binding) in calls(index, SqlPhysicalEmissionMode::OriginalNativeV1) {
        catalog
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
            .unwrap();
        let mut stale = binding.clone();
        let FunctionResultType::Scalar(target) = &mut stale.selected.result_type else {
            unreachable!()
        };
        target.data_type = DataType::Binary;
        catalog
            .admit_bound_lifecycle_observed(&stale, PureCallLifecycle::Scalar, &control)
            .unwrap();
    }
}
#[test]
fn e08s1_string_batch_original_length_caps_source_keeps_original_loans_without_data() {
    original(0)
}
#[test]
fn e08s1_string_batch_original_integer_arguments_and_measurement_members_keep_original_loans() {
    original(1);
    original(2)
}

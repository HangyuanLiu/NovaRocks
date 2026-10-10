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
//! Actual SQL compiler source receipts before the intrinsic consumer is installed.
//! The reused fixture uses its explicitly supplied no-fold evaluator, so these
//! tests prove analyzed/published FVT, not production FE folding/case closure.
use super::*;
use crate::exec::expr::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use novarocks_physical_plan::{ExprKind, UnaryOperator};
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
fn original_source(sql: &str, dtype: DataType) -> SqlAuthoredPhysicalPlan {
    sql_source(sql, dtype, SqlPhysicalEmissionMode::OriginalNativeV1)
}
fn bitnot_types(owner: &SqlAuthoredPhysicalPlan) -> Vec<FunctionValueType> {
    let mut observed = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for (_, source) in fragment.expressions().iter() {
            if let ExprKind::Unary {
                op: UnaryOperator::BitwiseNot,
                expr,
            } = &source.kind
            {
                let child = fragment.expressions().get(*expr).unwrap();
                // Borrow the actual sole SQL author; do not resolve a function
                // named bitnot or infer source facts from a runtime array.
                assert_eq!(source.ty, child.ty);
                observed.push(source.ty.clone());
            }
        }
    }
    observed
}
const PROJECT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_bitnot_operator_semantics.sql"
));
const STRICT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/function/sql/strict_unary_nullable_argument.sql"
));
#[test]
fn native_bitnot_sql_before_actual_project_file_uses_only_signed_bigint_source() {
    let owner = original_source(PROJECT, DataType::Int64);
    let observed = bitnot_types(&owner);
    assert_eq!(observed.len(), 4);
    assert!(
        observed
            .iter()
            .all(|ty| ty.logical_type == ValueLogicalType::Physical
                && ty.data_type == DataType::Int64)
    );
    let input: ArrayRef = Arc::new(Int64Array::from(vec![-1, 0, 1, 1024]));
    assert_eq!(
        original(input, false).unwrap().to_data(),
        Int64Array::from(vec![0, -1, -2, -1025]).to_data()
    );
}
#[test]
fn native_bitnot_sql_before_actual_nullable_values_and_integer_literal_are_signed_bigint() {
    for (begin, end, nullable) in [
        ("-- query 1", "-- query 2", true),
        ("-- query 4", "", false),
    ] {
        let tail = STRICT.split_once(begin).unwrap().1;
        let statement = if end.is_empty() {
            tail
        } else {
            tail.split_once(end).unwrap().0
        };
        let owner = original_source(statement.trim(), DataType::Int64);
        let observed = bitnot_types(&owner);
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0],
            FunctionValueType::new(DataType::Int64, nullable)
        );
    }
}
#[test]
fn native_bitnot_sql_before_original_provider_unsigned_source_has_explicit_provider_refusal() {
    // The original fixture's admitted provider has no exact unsigned value
    // type. This records its existing source refusal, not an intrinsic result
    // or a claim that an unsigned source was admitted by the SQL compiler.
    for dtype in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        let refused = std::panic::catch_unwind(|| {
            original_source("SELECT ~k FROM fixture.source", dtype.clone())
        })
        .expect_err("the original provider cannot author an unsigned scan output");
        let diagnostic = refused
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| refused.downcast_ref::<&str>().copied())
            .expect("the existing fixture unwrap preserves the compilation diagnostic");
        assert!(
            diagnostic.contains("scan output 'k' has no exact provider value type"),
            "original provider refusal: {diagnostic}"
        );
    }
}
#[test]
fn native_bitnot_sql_before_original_strict_null_source_stays_explicit_physical_null() {
    let owner = original_source("SELECT ~NULL", DataType::Int64);
    assert_eq!(
        bitnot_types(&owner),
        vec![FunctionValueType::new(DataType::Null, true)]
    );
}

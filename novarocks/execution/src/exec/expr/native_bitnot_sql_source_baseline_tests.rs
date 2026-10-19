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
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
fn original_source(sql: &str, dtype: DataType) -> SqlAuthoredPhysicalPlan {
    sql_source(sql, dtype)
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
            .all(|ty| ty.logical_type() == ValueLogicalType::Physical
                && *ty.data_type() == DataType::Int64)
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
fn native_bitnot_sql_original_unsigned_source_has_typed_intrinsic_refusal_before_provider() {
    use crate::exec::expr::numeric_unary_original_nonnull_sql_baseline_tests::try_sql_source_with_declared_fields;
    use arrow::datatypes::Field;
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };

    // Frozen intrinsic admission refuses each unsigned source before provider
    // negotiation. No completed source or unsigned intrinsic value is claimed.
    for dtype in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        let source = FunctionValueType::new(dtype.clone(), false);
        assert!(!novarocks_functions::native_bitnot_source_supported(&source));
        let failure = try_sql_source_with_declared_fields(
            "SELECT ~k FROM fixture.source",
            &[Field::new("k", dtype.clone(), false)],
        )
        .err()
        .expect("unsigned source must be refused before publication");
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("expected typed intrinsic source refusal for {dtype:?}: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
        assert_eq!(error.code(), AnalyzeErrorKind::TypeMismatch.descriptor().code);
        assert_eq!(error.span(), Some(novarocks_parser::Span::new(7, 9)));
        assert!(error.control_error().is_none());
        assert!(
            error.message().starts_with(
                "invalid function binding: invalid kernel program: native BitwiseNot unsupported source shape: "
            ),
            "intrinsic source refusal for {dtype:?}: {}",
            error.message()
        );
        assert!(
            error.message().contains(&format!("kind: Scalar({dtype:?})")),
            "intrinsic must retain the exact unsigned source kind: {}",
            error.message()
        );
        assert!(
            error.message().contains(&format!("data_type: {dtype:?}")),
            "intrinsic must retain the exact unsigned carrier: {}",
            error.message()
        );
    }
}

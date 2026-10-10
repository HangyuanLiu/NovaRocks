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
//! The actual SQL case with no-op folding retains the original emitted call and request authors.
//! Prior native PASS alone does not establish that a call survived real constant folding.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
const SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
));
fn check(mode: SqlPhysicalEmissionMode) {
    let owner = sql_source(SQL, DataType::Int64, mode);
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for (id, node) in fragment.expressions().iter() {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if !["builtin.scalar/left/v1", "builtin.scalar/right/v1"]
                .contains(&function.function_id.as_str())
            {
                continue;
            }
            let request = fragment
                .call_requests()
                .get(PhysicalCallDefinition::Expression(*id))
                .unwrap();
            assert_eq!(args.len(), 2);
            assert_eq!(request.logical_argument_count, 2);
            assert_eq!(request.arguments.len(), 2);
            assert_eq!(function.argument_types.len(), 2);
            for (index, ((argument, selected), child)) in request
                .arguments
                .iter()
                .zip(function.argument_types.iter())
                .zip(args.iter())
                .enumerate()
            {
                let StaticFunctionArgument::Value { value_type, .. } = argument else {
                    panic!("real source value argument")
                };
                let FunctionArgumentType::Value(bound) = selected else {
                    panic!("real selected value argument")
                };
                assert_eq!(value_type, bound);
                assert_eq!(bound.logical_type, ValueLogicalType::Physical);
                assert_eq!(
                    bound.data_type,
                    if index == 0 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    }
                );
                assert_eq!(&fragment.expressions().get(*child).unwrap().ty, bound);
            }
            assert_eq!(node.ty, function.result_type);
            assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
            eprintln!(
                "left_right actual retained source mode={mode:?} fragment={:?} definition={id:?} signature={function:?} original_request={request:?} ordered_children={args:?}",
                fragment.id()
            );
            count += 1;
        }
    }
    assert!(
        count == 4,
        "actual corpus SQL keeps a real function call with no-op fold"
    );
    eprintln!("left_right actual retained call count mode={mode:?} count={count}");
}
#[test]
fn left_right_actual_original_sql_kept_call_source() {
    check(SqlPhysicalEmissionMode::OriginalNativeV1)
}
#[test]
fn left_right_actual_candidate_sql_kept_call_source() {
    check(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration)
}

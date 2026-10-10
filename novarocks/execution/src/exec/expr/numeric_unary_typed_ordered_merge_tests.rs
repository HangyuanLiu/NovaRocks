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

//! Typed physical ordered merge support; explicitly not optimizer SQL plans.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    NodeKind, PhysicalCallDefinition, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_sql::compiler::{
    SqlCompileControl, SqlPhysicalEmissionMode, ordered_array_state_source_for_test,
    ordinary_state_observe_for_test,
};
fn check(intermediate: bool, mode: SqlPhysicalEmissionMode) {
    let control = SqlCompileControl::unbounded();
    let source = ordered_array_state_source_for_test(intermediate, mode, &control).unwrap();
    let actual = ordinary_state_observe_for_test(&source, &control).unwrap();
    assert_eq!(
        (actual.partials, actual.intermediates, actual.finals),
        (1, usize::from(intermediate), 1)
    );
    assert_eq!(actual.final_consumer_constant, Some(7));
    assert_eq!(
        actual.final_producer_constants,
        vec![Some(7); 1 + usize::from(intermediate)]
    );
    assert_eq!(
        actual.final_independent_lineages, 0,
        "same original source phase clones"
    );
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, _) in calls.iter().enumerate() {
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Relational(
                        PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: u32::try_from(ordinal).unwrap(),
                        },
                    ))
                    .unwrap();
                assert_eq!(request.logical_argument_count, 1);
                assert_eq!(request.arguments.len(), 2);
                let StaticFunctionArgument::Value {
                    constant: None,
                    value_type,
                } = &request.arguments[1]
                else {
                    panic!("actual own nonconstant ORDER")
                };
                assert_eq!(value_type.data_type, DataType::Int8);
                assert_eq!(
                    value_type.nullable,
                    mode == SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration
                );
            }
        }
    }
}
#[test]
fn numeric_unary_original_typed_ordered_merge_partial_final() {
    check(false, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_exact_typed_ordered_merge_partial_final() {
    check(
        false,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
#[test]
fn numeric_unary_original_typed_ordered_merge_intermediate() {
    check(true, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_exact_typed_ordered_merge_intermediate() {
    check(
        true,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}

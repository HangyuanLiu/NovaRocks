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

use super::*;
use crate::exec::expr::ExprNode;
use arrow::array::{Int8Array, Int16Array, Int32Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    ArithmeticRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedArithmeticRecipe,
};
use novarocks_type_contract::{
    ArithmeticOperator, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType, arithmetic_result_value_type_with_op,
};
use novarocks_types::SlotId;
use std::time::Duration;

struct OriginalControl;
impl PureCompileControl for OriginalControl {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for OriginalControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("decimal arithmetic has no waiting operation")
    }
}
fn value_type(array: &ArrayRef, logical: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical).unwrap()
}
fn decimal(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
fn wide_decimal(values: Vec<Option<i256>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
fn signed_array(values: &[Option<i64>], ty: &DataType) -> ArrayRef {
    match ty {
        DataType::Int8 => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("fixture must author a signed source"),
    }
}
fn compare_legacy_rows(
    operator: ArithmeticOperator,
    left: ArrayRef,
    left_logical: ValueLogicalType,
    right: ArrayRef,
    right_logical: ValueLogicalType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (Result<ArrayRef, String>, Vec<ArithmeticRowResult>) {
    let left_type = value_type(&left, left_logical);
    let right_type = value_type(&right, right_logical);
    let result_type =
        arithmetic_result_value_type_with_op(&left_type, &right_type, operator).unwrap();
    let recipe = PreparedArithmeticRecipe::try_new(
        operator,
        &left_type,
        &right_type,
        &result_type,
        policy,
        allow,
        &OriginalControl,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        left_type.try_to_field("left").unwrap(),
        right_type.try_to_field("right").unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![left.clone(), right.clone()]).unwrap();
    let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1), SlotId::new(2)],
    )
    .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let lhs = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), left_type.data_type);
    let rhs = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), right_type.data_type);
    let expression = match operator {
        ArithmeticOperator::Add => ExprNode::Add(lhs, rhs, policy),
        ArithmeticOperator::Subtract => ExprNode::Sub(lhs, rhs, policy),
        ArithmeticOperator::Multiply => ExprNode::Mul(lhs, rhs, policy),
        ArithmeticOperator::Divide => ExprNode::Div(lhs, rhs, policy),
        ArithmeticOperator::Modulo => ExprNode::Mod(lhs, rhs, policy),
    };
    let expression = arena.push_typed(expression, result_type.data_type.clone());
    let legacy = arena.eval(expression, &chunk);
    let rows = (0..left.len())
        .map(|row| {
            recipe
                .evaluate_row(
                    EvaluatedArgument::Column(&left),
                    row,
                    row,
                    EvaluatedArgument::Column(&right),
                    row,
                    row,
                    &OriginalControl,
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    match &legacy {
        Ok(array) => {
            assert_eq!(array.data_type(), &result_type.data_type);
            for (row, value) in rows.iter().enumerate() {
                match value {
                    ArithmeticRowResult::Null => assert!(array.is_null(row)),
                    ArithmeticRowResult::Decimal128(value) => {
                        assert!(!array.is_null(row));
                        assert_eq!(
                            array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *value
                        );
                    }
                    ArithmeticRowResult::Decimal256(value) => {
                        assert!(!array.is_null(row));
                        assert_eq!(
                            array
                                .as_any()
                                .downcast_ref::<Decimal256Array>()
                                .unwrap()
                                .value(row),
                            *value
                        );
                    }
                    _ => panic!("foreign prepared decimal result: {value:?}"),
                }
            }
        }
        Err(diagnostic) => {
            // The old whole-batch failure is an independent diagnostic oracle;
            // it does not publish the new selected-row error journal.
            let mut errors = 0;
            for (row, value) in rows.iter().enumerate() {
                if let ArithmeticRowResult::RowError(error) = value {
                    assert_eq!(error.selected_ordinal(), row);
                    assert_eq!(diagnostic, error.message());
                    errors += 1;
                }
            }
            assert!(
                errors > 0,
                "legacy failure had no required prepared row error: {diagnostic}"
            );
        }
    }
    (legacy, rows)
}
fn compare_physical(
    operator: ArithmeticOperator,
    left: ArrayRef,
    right: ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (Result<ArrayRef, String>, Vec<ArithmeticRowResult>) {
    compare_legacy_rows(
        operator,
        left,
        ValueLogicalType::Physical,
        right,
        ValueLogicalType::Physical,
        policy,
        allow,
    )
}

#[test]
fn legacy_decimal128_oracle_matches_all_signed_pairs_scales_and_final_half_away_rounding() {
    use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for (left, right) in [
                (
                    decimal(vec![Some(700), Some(-700), None, Some(0)], 18, 2),
                    decimal(vec![Some(3000), Some(-3000), Some(3000), None], 18, 3),
                ),
                (
                    decimal(vec![Some(7), Some(-7), None, Some(0)], 2, -2),
                    decimal(vec![Some(3), Some(-3), Some(7), None], 2, -1),
                ),
            ] {
                for operator in [Add, Subtract, Multiply, Divide, Modulo] {
                    assert!(
                        compare_physical(operator, left.clone(), right.clone(), policy, allow)
                            .0
                            .is_ok()
                    );
                }
            }
            for ty in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                let left = decimal(vec![Some(700), Some(-700), None, Some(0)], 18, 2);
                let right = signed_array(&[Some(3), Some(-3), Some(7), None], &ty);
                for operator in [Add, Subtract, Multiply, Divide, Modulo] {
                    assert!(
                        compare_physical(operator, left.clone(), right.clone(), policy, allow)
                            .0
                            .is_ok()
                    );
                    assert!(
                        compare_physical(operator, right.clone(), left.clone(), policy, allow)
                            .0
                            .is_ok()
                    );
                }
            }
            let (output, _) = compare_physical(
                Divide,
                decimal(vec![Some(1), Some(-1), Some(1), Some(-1), None], 2, 0),
                decimal(
                    vec![Some(128), Some(128), Some(-128), Some(-128), Some(128)],
                    3,
                    0,
                ),
                policy,
                allow,
            );
            let output = output.unwrap();
            assert_eq!(output.data_type(), &DataType::Decimal128(38, 6));
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(7813), Some(-7813), Some(-7813), Some(7813), None]
            );
        }
    }
}

#[test]
fn legacy_decimal_fault_oracle_preserves_checked_intermediates_null_zero_and_independent_policies()
{
    use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
    let max = 10_i128.pow(38) - 1;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for (operator, left, right, right_scale) in [
                (Add, max, 1, 0),
                (Subtract, -max, 1, 0),
                (Multiply, max, 2, 0),
                (Divide, max, 1, 1),
                (Modulo, max, 1, 1),
            ] {
                let (legacy, rows) = compare_physical(
                    operator,
                    decimal(vec![None, Some(left)], 38, 0),
                    decimal(vec![Some(right), Some(right)], 2, right_scale),
                    policy,
                    allow,
                );
                assert_eq!(rows[0], ArithmeticRowResult::Null);
                assert_eq!(
                    legacy.is_err(),
                    policy == DecimalOverflowPolicy::ReportError || (operator == Multiply && allow)
                );
            }
            // The final quotient fits; the original i128 numerator overflows
            // before division. An i256 rescue would change the old algorithm.
            let (legacy, rows) = compare_physical(
                Divide,
                decimal(vec![Some(max)], 38, 0),
                decimal(vec![Some(10_i128.pow(37))], 38, 0),
                policy,
                allow,
            );
            assert_eq!(
                legacy.is_err(),
                policy == DecimalOverflowPolicy::ReportError
            );
            if policy == DecimalOverflowPolicy::OutputNull {
                assert_eq!(rows, vec![ArithmeticRowResult::Null]);
            }
            let (legacy, rows) = compare_physical(
                Add,
                decimal(vec![None, Some(0)], 1, -128),
                signed_array(&[Some(0), Some(0)], &DataType::Int64),
                policy,
                allow,
            );
            assert_eq!(rows[0], ArithmeticRowResult::Null);
            assert_eq!(
                legacy.is_err(),
                policy == DecimalOverflowPolicy::ReportError
            );
            for operator in [Divide, Modulo] {
                let (left, right) = if operator == Divide {
                    (
                        decimal(vec![Some(1), None], 38, 38),
                        decimal(vec![Some(0), Some(1)], 1, -128),
                    )
                } else {
                    (
                        decimal(vec![Some(1), None], 1, -128),
                        decimal(vec![Some(0), Some(1)], 38, 38),
                    )
                };
                let (legacy, rows) = compare_physical(operator, left, right, policy, allow);
                assert!(legacy.is_ok());
                assert_eq!(
                    rows,
                    vec![ArithmeticRowResult::Null, ArithmeticRowResult::Null]
                );
            }
            // Legacy stored-column coefficients are not limited to source p.
            let (legacy, rows) = compare_physical(
                Add,
                decimal(vec![Some(1000), None], 2, 0),
                signed_array(&[Some(1), Some(1)], &DataType::Int64),
                policy,
                allow,
            );
            assert!(legacy.is_ok());
            assert_eq!(
                rows,
                vec![
                    ArithmeticRowResult::Decimal128(1001),
                    ArithmeticRowResult::Null
                ]
            );
        }
    }
}

#[test]
fn legacy_mixed_decimal_largeint_oracle_preserves_wide_coefficients_both_orders_and_slices() {
    use ArithmeticOperator::{Add, Subtract};
    let wide = i256::from_string("200000000000000000000000000000000000000").unwrap();
    assert!(wide.to_i128().is_none());
    for decimal_source in [
        decimal(vec![Some(0), Some(7), Some(-7), None, Some(0)], 38, 15),
        decimal(vec![Some(0), Some(7), Some(-7), None, Some(0)], 38, 36),
        decimal(vec![Some(0), Some(7), Some(-7), None, Some(0)], 38, -36),
        wide_decimal(
            vec![
                Some(i256::ZERO),
                Some(wide),
                Some(-wide),
                None,
                Some(i256::ZERO),
            ],
            55,
            15,
        ),
    ] {
        let decimal_source = decimal_source.slice(1, 3);
        let integer = largeint::array_from_i128(&[
            Some(0),
            Some(i128::MIN),
            Some(i128::MAX),
            Some(7),
            Some(0),
        ])
        .unwrap()
        .slice(1, 3);
        for operator in [Add, Subtract] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    assert!(
                        compare_legacy_rows(
                            operator,
                            decimal_source.clone(),
                            ValueLogicalType::Physical,
                            integer.clone(),
                            ValueLogicalType::LargeInt,
                            policy,
                            allow
                        )
                        .0
                        .is_ok()
                    );
                    assert!(
                        compare_legacy_rows(
                            operator,
                            integer.clone(),
                            ValueLogicalType::LargeInt,
                            decimal_source.clone(),
                            ValueLogicalType::Physical,
                            policy,
                            allow
                        )
                        .0
                        .is_ok()
                    );
                }
            }
        }
    }
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for operator in [Add, Subtract] {
                // This is a licensed mixed profile, with actual raw Column data
                // beyond source p; result overflow is not an input-type error.
                let (legacy, rows) = compare_legacy_rows(
                    operator,
                    wide_decimal(vec![None, Some(i256::MAX)], 2, 0),
                    ValueLogicalType::Physical,
                    largeint::array_from_i128(&[Some(0), Some(0)]).unwrap(),
                    ValueLogicalType::LargeInt,
                    policy,
                    allow,
                );
                assert_eq!(rows[0], ArithmeticRowResult::Null);
                assert_eq!(
                    legacy.is_err(),
                    policy == DecimalOverflowPolicy::ReportError
                );
            }
        }
    }
}

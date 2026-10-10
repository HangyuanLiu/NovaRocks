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

//! Permanent equality against the original Float arithmetic public dispatcher.
use super::original_float_arithmetic_baseline_tests as raw;
use arrow::{
    array::{Array, ArrayRef, Float64Array},
    datatypes::DataType,
};
use novarocks_functions::{
    ArithmeticRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedArithmeticRecipe, SelectedValues, Selection,
};
use novarocks_type_contract::{
    ArithmeticOperator, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    FunctionValueType, PureCompileControl, arithmetic_result_value_type_with_op,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Float arithmetic never waits")
    }
}
fn recipe(
    op: ArithmeticOperator,
    left: &ArrayRef,
    right: &ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedArithmeticRecipe {
    let l = FunctionValueType::new(left.data_type().clone(), true);
    let r = FunctionValueType::new(right.data_type().clone(), true);
    let result = arithmetic_result_value_type_with_op(&l, &r, op).unwrap();
    PreparedArithmeticRecipe::try_new(op, &l, &r, &result, policy, allow, &Control).unwrap()
}
fn compare(row: ArithmeticRowResult, expected: &Float64Array, address: usize) {
    if expected.is_null(address) {
        assert!(matches!(row, ArithmeticRowResult::Null))
    } else {
        match row {
            ArithmeticRowResult::Float(value) => assert_eq!(
                value.to_bits(),
                expected.value(address).to_bits(),
                "source row {address}"
            ),
            other => panic!("original successful Float row differs: {other:?}"),
        }
    }
}
#[test]
fn float_arithmetic_required_original_equality_complete_float_numeric_axes() {
    for float in [raw::f32_values(), raw::f64_values()] {
        for peer in raw::peers() {
            for (left, right) in [(float.clone(), peer.clone()), (peer.clone(), float.clone())] {
                for op in raw::OPERATORS {
                    let original = raw::legacy(
                        op,
                        left.clone(),
                        right.clone(),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    )
                    .unwrap();
                    let original = original.as_any().downcast_ref::<Float64Array>().unwrap();
                    let prepared =
                        recipe(op, &left, &right, DecimalOverflowPolicy::OutputNull, false);
                    for row in 0..left.len() {
                        compare(
                            prepared
                                .evaluate_row(
                                    EvaluatedArgument::Column(&left),
                                    row,
                                    row,
                                    EvaluatedArgument::Column(&right),
                                    row,
                                    row,
                                    &Control,
                                )
                                .unwrap(),
                            original,
                            row,
                        )
                    }
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_required_original_equality_compact_scalar_null_and_policy_addresses() {
    let rows = [0, 2, 3, 6, 10, 11];
    let selection = Selection::try_sparse(12, &rows).unwrap();
    let left = raw::f64_values();
    let compact = Arc::new(Float64Array::from(
        rows.iter()
            .map(|row| {
                if left.is_null(*row) {
                    None
                } else {
                    Some(
                        left.as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(*row),
                    )
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let selected =
        SelectedValues::try_new(selection, &DataType::Float64, compact, Box::default()).unwrap();
    for op in raw::OPERATORS {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for rhs in [Some(0.0), Some(-0.0), Some(2.0), Some(f64::INFINITY), None] {
                    let scalar = Arc::new(Float64Array::from(vec![rhs])) as ArrayRef;
                    let broadcast = Arc::new(Float64Array::from(vec![rhs; 12])) as ArrayRef;
                    let original = raw::legacy(op, left.clone(), broadcast, policy, allow).unwrap();
                    let original = original.as_any().downcast_ref::<Float64Array>().unwrap();
                    let prepared = recipe(op, &left, &scalar, policy, allow);
                    for (ordinal, row) in rows.iter().copied().enumerate() {
                        compare(
                            prepared
                                .evaluate_row(
                                    EvaluatedArgument::SelectedColumn(&selected),
                                    ordinal,
                                    row,
                                    EvaluatedArgument::Scalar(&scalar),
                                    ordinal,
                                    row,
                                    &Control,
                                )
                                .unwrap(),
                            original,
                            row,
                        )
                    }
                }
            }
        }
    }
}

#[test]
fn float_arithmetic_required_original_equality_nonzero_constant_pool_and_hidden_null() {
    let left = raw::f64_values();
    let rows = [0, 2, 3, 6, 10, 11];
    for rhs in [Some(2.0), Some(-0.0), Some(f64::INFINITY), None] {
        // Unused pool element zero deliberately differs from the demanded value.
        let backing: ArrayRef = Arc::new(Float64Array::new(
            vec![71.0, rhs.unwrap_or(f64::from_bits(0x7ff0000000000001))].into(),
            Some(arrow_buffer::NullBuffer::from(vec![true, rhs.is_some()])),
        ));
        let ty = FunctionValueType::new(DataType::Float64, true);
        let pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(ty.try_to_field("original-float-pool").unwrap()),
            ty,
            backing.to_data(),
            super::pure_differential::constant_policy(),
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![rhs]));
        let broadcast: ArrayRef = Arc::new(Float64Array::from(vec![rhs; left.len()]));
        for op in raw::OPERATORS {
            for reversed in [false, true] {
                let (old_left, old_right) = if reversed {
                    (broadcast.clone(), left.clone())
                } else {
                    (left.clone(), broadcast.clone())
                };
                let expected = raw::legacy(
                    op,
                    old_left,
                    old_right,
                    DecimalOverflowPolicy::OutputNull,
                    false,
                )
                .unwrap();
                let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                let prepared = if reversed {
                    recipe(op, &scalar, &left, DecimalOverflowPolicy::OutputNull, false)
                } else {
                    recipe(op, &left, &scalar, DecimalOverflowPolicy::OutputNull, false)
                };
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    let (a, b) = if reversed {
                        (
                            EvaluatedArgument::Constant(&value),
                            EvaluatedArgument::Column(&left),
                        )
                    } else {
                        (
                            EvaluatedArgument::Column(&left),
                            EvaluatedArgument::Constant(&value),
                        )
                    };
                    compare(
                        prepared
                            .evaluate_row(a, ordinal, row, b, ordinal, row, &Control)
                            .unwrap(),
                        expected,
                        row,
                    );
                }
            }
        }
    }
}

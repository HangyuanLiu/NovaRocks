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

//! Permanent independent IEEE/carrier expectations for required Float arithmetic.
//! Historical test names retain the P12/P13 trace. The retired dual-author
//! arithmetic NaN payload comparator is not an IEEE payload contract; only
//! arithmetic NaN uses a semantic assertion. Stored payload bits remain exact.
use super::float_arithmetic_independent_fixture as raw;
use arrow::{
    array::{Array, ArrayRef, Float32Array, Float64Array},
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
    assert_eq!(result.data_type(), &DataType::Float64);
    assert!(
        result.nullable(),
        "strict nullable input declaration is retained"
    );
    PreparedArithmeticRecipe::try_new(op, &l, &r, &result, policy, allow, &Control).unwrap()
}
fn compare(row: ArithmeticRowResult, expected: &[raw::Expected; 12], address: usize) {
    match (row, expected[address]) {
        (ArithmeticRowResult::Null, raw::Expected::Null) => {}
        (ArithmeticRowResult::Float(value), raw::Expected::Bits(bits)) => {
            assert_eq!(value.to_bits(), bits, "source row {address}");
        }
        (ArithmeticRowResult::Float(value), raw::Expected::Nan) => {
            assert!(
                value.is_nan(),
                "independent IEEE NaN at source row {address}, actual bits={:016x}",
                value.to_bits()
            );
        }
        (actual, expected) => panic!(
            "independent Float result differs at source row {address}: actual={actual:?}, expected={expected:?}"
        ),
    }
}

fn assert_stored_float_payloads() {
    let f32 = raw::f32_values();
    let f64 = raw::f64_values();
    assert_eq!(
        f32.as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(6)
            .to_bits(),
        0x7fc0_0042
    );
    assert_eq!(
        f64.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(6)
            .to_bits(),
        0x7ff8_0000_0000_0042
    );
    assert_eq!(
        f32.as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(3)
            .to_bits(),
        0x8000_0000
    );
    assert_eq!(
        f64.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(3)
            .to_bits(),
        0x8000_0000_0000_0000
    );
    assert_eq!(
        f32.as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(9)
            .to_bits(),
        1
    );
    assert_eq!(
        f64.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(9)
            .to_bits(),
        1
    );
}

#[test]
fn float_arithmetic_required_original_equality_complete_float_numeric_axes() {
    assert_stored_float_payloads();
    let peers = raw::peers();
    assert_eq!(
        peers.len(),
        158,
        "all four integer widths, both floats and all 38 precisions x four scales"
    );
    for (float_index, float) in [raw::f32_values(), raw::f64_values()]
        .into_iter()
        .enumerate()
    {
        for (peer_index, peer) in peers.iter().enumerate() {
            for (reversed, (left, right)) in [
                (false, (float.clone(), peer.clone())),
                (true, (peer.clone(), float.clone())),
            ] {
                for (op_index, op) in raw::OPERATORS.into_iter().enumerate() {
                    let expected = raw::complete(float_index, peer_index, reversed, op_index);
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
                            expected,
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
    assert_stored_float_payloads();
    for (op_index, op) in raw::OPERATORS.into_iter().enumerate() {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for (rhs_index, rhs) in
                    [Some(0.0), Some(-0.0), Some(2.0), Some(f64::INFINITY), None]
                        .into_iter()
                        .enumerate()
                {
                    let scalar = Arc::new(Float64Array::from(vec![rhs])) as ArrayRef;
                    let expected = raw::scalar(rhs_index, false, op_index);
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
                            expected,
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
    assert_stored_float_payloads();
    // Indices address the independently fixed scalar table; no evaluator is
    // called to produce expected values for either constant operand order.
    for (rhs_index, rhs) in [
        (2, Some(2.0)),
        (1, Some(-0.0)),
        (3, Some(f64::INFINITY)),
        (4, None),
    ] {
        // Unused pool element zero deliberately differs from the demanded value.
        let backing: ArrayRef = Arc::new(Float64Array::new(
            vec![71.0, rhs.unwrap_or(f64::from_bits(0x7ff0000000000001))].into(),
            Some(arrow_buffer::NullBuffer::from(vec![true, rhs.is_some()])),
        ));
        let original_backing = backing.as_any().downcast_ref::<Float64Array>().unwrap();
        let demanded_bits = rhs.map(f64::to_bits);
        assert_eq!(original_backing.value(0).to_bits(), 71.0f64.to_bits());
        assert_eq!(
            original_backing.value(1).to_bits(),
            demanded_bits.unwrap_or(0x7ff0_0000_0000_0001)
        );
        assert_eq!(original_backing.is_valid(1), rhs.is_some());
        let ty = FunctionValueType::new(DataType::Float64, true);
        let pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(ty.try_to_field("original-float-pool").unwrap()),
            ty,
            backing.to_data(),
            super::test_support::constant_policy(),
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        assert_eq!(value.ordinal(), 1);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert_eq!(
            pool.value(0).unwrap().try_f64_bits().unwrap(),
            Some(71.0f64.to_bits())
        );
        assert_eq!(value.try_f64_bits().unwrap(), demanded_bits);
        // The source carrier's hidden NULL signaling payload is a storage fact,
        // separate from arithmetic NaN propagation. Pool construction cannot
        // mutate the immutable original backing.
        assert_eq!(
            original_backing.value(1).to_bits(),
            demanded_bits.unwrap_or(0x7ff0_0000_0000_0001)
        );
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![rhs]));
        for (op_index, op) in raw::OPERATORS.into_iter().enumerate() {
            for reversed in [false, true] {
                let expected = raw::scalar(rhs_index, reversed, op_index);
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

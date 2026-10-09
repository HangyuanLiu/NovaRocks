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

//! Original public ExprArena Float arithmetic witnesses, before selected support.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use novarocks_type_contract::{
    ArithmeticOperator, DecimalOverflowPolicy, FunctionValueType,
    arithmetic_result_value_type_with_op,
};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) const OPERATORS: [ArithmeticOperator; 5] = [
    ArithmeticOperator::Add,
    ArithmeticOperator::Subtract,
    ArithmeticOperator::Multiply,
    ArithmeticOperator::Divide,
    ArithmeticOperator::Modulo,
];
pub(super) fn f64_values() -> ArrayRef {
    Arc::new(Float64Array::from(vec![
        Some(1.5),
        Some(-2.25),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(f64::MAX),
        Some(f64::MIN_POSITIVE),
        Some(f64::from_bits(1)),
        None,
        Some(7.0),
    ]))
}
pub(super) fn f32_values() -> ArrayRef {
    Arc::new(Float32Array::from(vec![
        Some(1.5),
        Some(-2.25),
        Some(0.0),
        Some(-0.0),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(f32::from_bits(0x7fc0_0042)),
        Some(f32::MAX),
        Some(f32::MIN_POSITIVE),
        Some(f32::from_bits(1)),
        None,
        Some(7.0),
    ]))
}
pub(super) fn peers() -> Vec<ArrayRef> {
    let values = [
        Some(1),
        Some(-2),
        Some(0),
        Some(0),
        Some(3),
        Some(-3),
        Some(1),
        Some(7),
        Some(-7),
        None,
        Some(11),
        Some(2),
    ];
    let mut arrays = vec![
        Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i8))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i16))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        f32_values(),
        f64_values(),
    ];
    for precision in 1..=38u8 {
        for scale in [-128, -1, 0, precision as i8] {
            arrays.push(Arc::new(
                Decimal128Array::from(values.iter().map(|v| v.map(i128::from)).collect::<Vec<_>>())
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ));
        }
    }
    arrays
}
pub(super) fn legacy(
    op: ArithmeticOperator,
    left: ArrayRef,
    right: ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let lt = FunctionValueType::new(left.data_type().clone(), true);
    let rt = FunctionValueType::new(right.data_type().clone(), true);
    legacy_typed(op, left, right, lt, rt, policy, allow)
}
fn legacy_typed(
    op: ArithmeticOperator,
    left: ArrayRef,
    right: ArrayRef,
    lt: FunctionValueType,
    rt: FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let output = arithmetic_result_value_type_with_op(&lt, &rt, op).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            lt.try_to_field("left").unwrap(),
            rt.try_to_field("right").unwrap(),
        ])),
        vec![left, right],
    )
    .unwrap();
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1), SlotId::new(2)],
    )
    .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), lt.data_type);
    let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), rt.data_type);
    let expr = match op {
        ArithmeticOperator::Add => ExprNode::Add(left, right, policy),
        ArithmeticOperator::Subtract => ExprNode::Sub(left, right, policy),
        ArithmeticOperator::Multiply => ExprNode::Mul(left, right, policy),
        ArithmeticOperator::Divide => ExprNode::Div(left, right, policy),
        ArithmeticOperator::Modulo => ExprNode::Mod(left, right, policy),
    };
    let expr = arena.push_typed(expr, output.data_type);
    arena.eval(expr, &chunk)
}
fn shared(
    op: ArithmeticOperator,
    left: ArrayRef,
    right: ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    use novarocks_functions::legacy_arithmetic::*;
    match op {
        ArithmeticOperator::Add => eval_add_arrays(left, right, DataType::Float64, allow, policy),
        ArithmeticOperator::Subtract => {
            eval_sub_arrays(left, right, DataType::Float64, allow, policy)
        }
        ArithmeticOperator::Multiply => {
            eval_mul_arrays(left, right, DataType::Float64, allow, policy)
        }
        ArithmeticOperator::Divide => {
            eval_div_arrays(left, right, &DataType::Float64, allow, policy)
        }
        ArithmeticOperator::Modulo => {
            eval_mod_arrays(left, right, DataType::Float64, allow, policy)
        }
    }
}
pub(super) fn assert_bits(a: &ArrayRef, b: &ArrayRef) {
    assert_eq!(a.data_type(), b.data_type());
    assert_eq!(a.len(), b.len());
    let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
    let b = b.as_any().downcast_ref::<Float64Array>().unwrap();
    for row in 0..a.len() {
        assert_eq!(a.is_null(row), b.is_null(row));
        if !a.is_null(row) {
            assert_eq!(a.value(row).to_bits(), b.value(row).to_bits(), "row {row}")
        }
    }
}
#[test]
fn original_float_arithmetic_same_arrow_author_complete_carrier_axes() {
    for float in [f32_values(), f64_values()] {
        for peer in peers() {
            for (left, right) in [(float.clone(), peer.clone()), (peer.clone(), float.clone())] {
                for op in OPERATORS {
                    let legacy = legacy(
                        op,
                        left.clone(),
                        right.clone(),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    );
                    let author = shared(
                        op,
                        left.clone(),
                        right.clone(),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    );
                    match (legacy, author) {
                        (Ok(a), Ok(b)) => assert_bits(&a, &b),
                        (Err(a), Err(b)) => assert_eq!(a, b),
                        pair => panic!("original shared author differs: {pair:?}"),
                    }
                }
            }
        }
    }
}
#[test]
fn original_float_arithmetic_rhs_zero_width_and_ieee_remain_distinct() {
    let left = Arc::new(Float64Array::from(vec![
        Some(1.0),
        Some(-1.0),
        Some(0.0),
        None,
    ])) as ArrayRef;
    for right in [
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(0.0),
            Some(0.0),
        ])) as ArrayRef,
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(0.0),
            Some(0.0),
        ])) as ArrayRef,
    ] {
        let out = legacy(
            ArithmeticOperator::Divide,
            left.clone(),
            right.clone(),
            DecimalOverflowPolicy::ReportError,
            true,
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        if right.data_type() == &DataType::Float64 {
            assert_eq!(out.null_count(), 4)
        } else {
            assert_eq!(out.value(0), f64::INFINITY);
            assert_eq!(out.value(1), f64::INFINITY);
            assert!(out.value(2).is_nan());
            assert!(out.is_null(3))
        }
    }
    let out = legacy(
        ArithmeticOperator::Modulo,
        left,
        Arc::new(Float64Array::from(vec![Some(0.0); 4])),
        DecimalOverflowPolicy::OutputNull,
        false,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(out.value(0).is_nan());
    assert!(out.value(1).is_nan());
    assert!(out.value(2).is_nan());
    assert!(out.is_null(3));
}
#[test]
fn original_float_arithmetic_slice_empty_and_policy_axes_use_same_author() {
    for op in OPERATORS {
        for allow in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for (offset, len) in [(2, 8), (0, 0)] {
                    let a = f64_values().slice(offset, len);
                    let b = f32_values().slice(offset, len);
                    assert_bits(
                        &legacy(op, a.clone(), b.clone(), policy, allow).unwrap(),
                        &shared(op, a, b, policy, allow).unwrap(),
                    );
                }
            }
        }
    }
}
#[test]
fn original_float_arithmetic_child_error_order_and_nominal_arrow_refusal() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "v",
            DataType::Float64,
            true,
        )])),
        vec![f64_values()],
    )
    .unwrap();
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(), &[SlotId::new(1)],
    ).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let missing_left = ExprId(usize::MAX);
    let missing_right = ExprId(usize::MAX - 1);
    for op in OPERATORS {
        let node = match op {
            ArithmeticOperator::Add => ExprNode::Add(
                missing_left,
                missing_right,
                DecimalOverflowPolicy::OutputNull,
            ),
            ArithmeticOperator::Subtract => ExprNode::Sub(
                missing_left,
                missing_right,
                DecimalOverflowPolicy::OutputNull,
            ),
            ArithmeticOperator::Multiply => ExprNode::Mul(
                missing_left,
                missing_right,
                DecimalOverflowPolicy::OutputNull,
            ),
            ArithmeticOperator::Divide => ExprNode::Div(
                missing_left,
                missing_right,
                DecimalOverflowPolicy::OutputNull,
            ),
            ArithmeticOperator::Modulo => ExprNode::Mod(
                missing_left,
                missing_right,
                DecimalOverflowPolicy::OutputNull,
            ),
        };
        let id = arena.push_typed(node, DataType::Float64);
        assert_eq!(arena.eval(id, &chunk).unwrap_err(), "invalid ExprId");
    }
    let large = novarocks_functions::largeint::array_from_i128(&vec![Some(1); 12]).unwrap();
    for op in OPERATORS {
        let lt = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap();
        let error = legacy_typed(
            op,
            large.clone(),
            f64_values(),
            lt,
            FunctionValueType::new(DataType::Float64, true),
            DecimalOverflowPolicy::OutputNull,
            false,
        )
        .unwrap_err();
        let expected = arrow::compute::cast(&large, &DataType::Float64)
            .unwrap_err()
            .to_string();
        assert_eq!(error, expected);
    }
}

// FULL_FROZEN_BOUNDARY_SUPPLEMENT

#[test]
fn original_float_arithmetic_full_frozen_value_author_boundary() {
    use novarocks_type_contract::ValueLogicalType as L;
    let floats = [DataType::Float32, DataType::Float64];
    let rejected = [
        DataType::Boolean,
        DataType::Null,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal256(76, 2),
        DataType::Utf8,
        DataType::Binary,
        DataType::FixedSizeBinary(16),
    ];
    for float in floats {
        let f = FunctionValueType::new(float, false);
        for op in OPERATORS {
            for carrier in &rejected {
                let other = FunctionValueType::new(carrier.clone(), true);
                assert!(arithmetic_result_value_type_with_op(&f, &other, op).is_none());
                assert!(arithmetic_result_value_type_with_op(&other, &f, op).is_none());
            }
            for (carrier, logical) in [
                (DataType::Utf8, L::Json),
                (DataType::LargeBinary, L::Variant),
                (DataType::Binary, L::Hll),
                (DataType::Binary, L::Bitmap),
                (DataType::Binary, L::Object),
                (DataType::Binary, L::Percentile),
                (DataType::FixedSizeBinary(16), L::Uuid),
            ] {
                let other =
                    FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap();
                assert!(arithmetic_result_value_type_with_op(&f, &other, op).is_none());
                assert!(arithmetic_result_value_type_with_op(&other, &f, op).is_none());
            }
            // Full legitimate Decimal128 metadata domain, not just sampled scales.
            for precision in 1..=38u8 {
                for scale in i8::MIN..=precision as i8 {
                    let other =
                        FunctionValueType::new(DataType::Decimal128(precision, scale), true);
                    assert_eq!(
                        arithmetic_result_value_type_with_op(&f, &other, op),
                        Some(FunctionValueType::new(DataType::Float64, true))
                    );
                    assert_eq!(
                        arithmetic_result_value_type_with_op(&other, &f, op),
                        Some(FunctionValueType::new(DataType::Float64, true))
                    );
                }
            }
            let large = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                L::LargeInt,
            )
            .unwrap();
            assert_eq!(
                arithmetic_result_value_type_with_op(&f, &large, op),
                Some(FunctionValueType::new(DataType::Float64, true))
            );
            assert_eq!(
                arithmetic_result_value_type_with_op(&large, &f, op),
                Some(FunctionValueType::new(DataType::Float64, true))
            );
        }
    }
}
#[test]
fn original_float_arithmetic_nominal_largeint_setup_error_precedes_empty_and_null() {
    use novarocks_type_contract::ValueLogicalType;
    for len in [0, 3] {
        let large = novarocks_functions::largeint::array_from_i128(&vec![None; len]).unwrap();
        let float: ArrayRef = Arc::new(Float64Array::from(vec![None; len]));
        let large_type = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let float_type = FunctionValueType::new(DataType::Float64, true);
        let original_error = arrow::compute::cast(&large, &DataType::Float64)
            .unwrap_err()
            .to_string();
        for op in OPERATORS {
            for (left, right, lt, rt) in [
                (
                    large.clone(),
                    float.clone(),
                    large_type.clone(),
                    float_type.clone(),
                ),
                (
                    float.clone(),
                    large.clone(),
                    float_type.clone(),
                    large_type.clone(),
                ),
            ] {
                assert_eq!(
                    legacy_typed(
                        op,
                        left,
                        right,
                        lt,
                        rt,
                        DecimalOverflowPolicy::OutputNull,
                        false
                    )
                    .unwrap_err(),
                    original_error
                );
            }
        }
    }
}

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
//! Original native NEGATE projection: exact typed zero, then Sub(OutputNull).
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::{SlotId, largeint};
use std::sync::Arc;
fn zero(t: &DataType) -> LiteralValue {
    match t {
        DataType::Int8 => LiteralValue::Int8(0),
        DataType::Int16 => LiteralValue::Int16(0),
        DataType::Int32 => LiteralValue::Int32(0),
        DataType::Int64 => LiteralValue::Int64(0),
        DataType::Float32 => LiteralValue::Float32(0.0),
        DataType::Float64 => LiteralValue::Float64(0.0),
        DataType::Decimal128(precision, scale) => LiteralValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal256(precision, scale) => LiteralValue::Decimal256 {
            value: i256::ZERO,
            precision: *precision,
            scale: *scale,
        },
        t if largeint::is_largeint_data_type(t) => LiteralValue::LargeInt(0),
        _ => panic!("fixture only covers original native zero admission"),
    }
}
fn actual(input: ArrayRef, allow: bool) -> Result<ArrayRef, String> {
    let t = input.data_type().clone();
    let ty = FunctionValueType::new(t.clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![ty.try_to_field("operand").unwrap()])),
        vec![input],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let z = arena.push_typed(ExprNode::Literal(zero(&t)), t.clone());
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), t.clone());
    let root = arena.push_typed(
        ExprNode::Sub(z, source, DecimalOverflowPolicy::OutputNull),
        t,
    );
    arena.eval(root, &chunk)
}
#[test]
fn numeric_unary_raw_small_integer_minimum_casts_to_null_not_wrapping() {
    let cases: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
    ];
    for input in cases {
        let out = actual(input.clone(), false).unwrap();
        assert_eq!(out.data_type(), input.data_type());
        assert!(out.is_null(0));
        assert!(out.is_null(2));
        let vals = arrow::compute::cast(&out, &DataType::Int64).unwrap();
        let vals = vals.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(vals.value(1), 1);
        assert_eq!(vals.value(3), 0);
        assert_eq!(actual(input.slice(1, 2), true).unwrap().len(), 2);
        assert_eq!(actual(input.slice(0, 0), true).unwrap().len(), 0);
    }
}
#[test]
fn numeric_unary_raw_int64_minimum_original_arrow_full_error() {
    for allow in [false, true] {
        assert_eq!(
            actual(
                Arc::new(Int64Array::from(vec![None, Some(i64::MIN)])),
                allow
            )
            .unwrap_err(),
            "Arithmetic overflow: Overflow happened on: 0 - -9223372036854775808"
        );
        let out = actual(
            Arc::new(Int64Array::from(vec![Some(-42), None, Some(0)])),
            allow,
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 42);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 0);
    }
}
#[test]
fn numeric_unary_raw_float_exact_zero_subtraction_and_ieee_edges() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-101.9),
        None,
    ]));
    let b: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(-101.9),
        None,
    ]));
    let out = actual(a, false).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    for (row, v) in [0.0, 0.0, f64::NEG_INFINITY, f64::INFINITY, 101.9]
        .into_iter()
        .enumerate()
    {
        assert_eq!(out.value(row).to_bits(), v.to_bits());
    }
    assert!(out.is_null(5));
    let out = actual(b, true).unwrap();
    let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
    for (row, v) in [0.0f32, 0.0, f32::NEG_INFINITY, f32::INFINITY, 101.9]
        .into_iter()
        .enumerate()
    {
        assert_eq!(out.value(row).to_bits(), v.to_bits());
    }
    assert!(out.is_null(5));
    for input in [
        Arc::new(Float64Array::from(vec![Some(f64::NAN)])) as ArrayRef,
        Arc::new(Float32Array::from(vec![Some(f32::NAN)])),
    ] {
        let out = actual(input, false).unwrap();
        let out = arrow::compute::cast(&out, &DataType::Float64).unwrap();
        assert!(
            out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .is_nan()
        );
    }
}
#[test]
fn numeric_unary_raw_decimal_exact_precision_scale_and_outputnull() {
    for (p, s) in [(4, 0), (18, 2), (38, 6)] {
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(-123), None, Some(0)])
                .with_precision_and_scale(p, s)
                .unwrap(),
        );
        let out = actual(input, true).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.data_type(), &DataType::Decimal128(p, s));
        assert_eq!(out.value(0), 123);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 0);
    }
    for (p, s) in [(40, 0), (60, 2), (76, 6)] {
        let input: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(-123)), None, Some(i256::ZERO)])
                .with_precision_and_scale(p, s)
                .unwrap(),
        );
        let out = actual(input, true).unwrap();
        let out = out.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(out.data_type(), &DataType::Decimal256(p, s));
        assert_eq!(out.value(0), i256::from_i128(123));
        assert!(out.is_null(1));
    }
    let input: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    assert!(actual(input, true).unwrap().is_null(0));
    let input: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::MIN)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    assert!(actual(input, true).unwrap().is_null(0));
}
#[test]
fn numeric_unary_raw_largeint_minimum_original_wrapping_and_null() {
    let input = largeint::array_from_i128(&[Some(i128::MIN), Some(-1), None, Some(0)]).unwrap();
    let out = actual(input, false).unwrap();
    let out = largeint::as_fixed_size_binary_array(&out, "baseline").unwrap();
    assert_eq!(largeint::value_at(out, 0).unwrap(), i128::MIN);
    assert_eq!(largeint::value_at(out, 1).unwrap(), 1);
    assert!(out.is_null(2));
    assert_eq!(largeint::value_at(out, 3).unwrap(), 0);
}

#[cfg(test)]
#[path="numeric_unary_intrinsic_oracle_tests.rs"]
mod oracle_tests;

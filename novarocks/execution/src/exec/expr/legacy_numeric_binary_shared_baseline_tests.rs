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
//! Independent actual v1 binary-numeric values, raw profiles, errors and panics.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, math::eval_math_function};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int64Array, NullArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn evaluate(
    name: &'static str,
    columns: Vec<ArrayRef>,
    result: DataType,
) -> Result<ArrayRef, String> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns.clone()).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| arena.push_typed(ExprNode::SlotId(slots[i]), c.data_type().clone()))
        .collect::<Vec<_>>();
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Math(name),
            args: args.clone(),
        },
        result,
    );
    eval_math_function(name, &arena, call, &args, &chunk)
}
fn ints(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn floats(out: ArrayRef) -> Vec<Option<f64>> {
    assert_eq!(out.data_type(), &DataType::Float64);
    out.as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn decimal(v: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}

fn doubles(v: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(v))
}
fn names() -> [&'static str; 6] {
    ["atan2", "fmod", "pow", "fpow", "dpow", "power"]
}
#[test]
fn original_binary_atan2_raw_infinities_signed_zero_and_null_are_frozen() {
    let out = floats(
        evaluate(
            "atan2",
            vec![
                doubles(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(0.0),
                    Some(-0.0),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::NAN),
                    None,
                ]),
                doubles(vec![
                    Some(1.0),
                    Some(1.0),
                    Some(-1.0),
                    Some(-1.0),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(1.0),
                    Some(0.0),
                ]),
            ],
            DataType::Float64,
        )
        .unwrap(),
    );
    assert_eq!(out[0].unwrap().to_bits(), 0);
    assert_eq!(out[1].unwrap().to_bits(), 1u64 << 63);
    assert_eq!(
        &out[2..],
        &[
            Some(std::f64::consts::PI),
            Some(-std::f64::consts::PI),
            Some(std::f64::consts::FRAC_PI_4),
            Some(-std::f64::consts::FRAC_PI_4),
            None,
            None
        ]
    );
}
#[test]
fn original_binary_fmod_preserves_fractional_sign_and_evaluates_infinite_divisor() {
    let out = floats(
        evaluate(
            "fmod",
            vec![
                doubles(vec![
                    Some(5.5),
                    Some(-5.5),
                    Some(-0.0),
                    Some(5.5),
                    Some(f64::INFINITY),
                    Some(1.0),
                    Some(f64::NAN),
                    None,
                ]),
                doubles(vec![
                    Some(-2.0),
                    Some(2.0),
                    Some(2.0),
                    Some(f64::INFINITY),
                    Some(2.0),
                    Some(-0.0),
                    Some(0.0),
                    Some(2.0),
                ]),
            ],
            DataType::Float64,
        )
        .unwrap(),
    );
    assert_eq!(out[0], Some(1.5));
    assert_eq!(out[1], Some(-1.5));
    assert_eq!(out[2].unwrap().to_bits(), 1u64 << 63);
    assert_eq!(&out[3..], &[Some(5.5), None, None, None, None]);
}
#[test]
fn original_binary_all_pow_aliases_keep_nan_identity_signed_zero_overflow_and_null() {
    for name in ["pow", "fpow", "dpow", "power"] {
        let out = floats(
            evaluate(
                name,
                vec![
                    doubles(vec![
                        Some(f64::NAN),
                        Some(1.0),
                        Some(-0.0),
                        Some(-0.0),
                        Some(-0.0),
                        Some(-4.0),
                        Some(2.0),
                        None,
                    ]),
                    doubles(vec![
                        Some(0.0),
                        Some(f64::NAN),
                        Some(3.0),
                        Some(2.0),
                        Some(-1.0),
                        Some(0.5),
                        Some(1024.0),
                        Some(0.0),
                    ]),
                ],
                DataType::Float64,
            )
            .unwrap(),
        );
        assert_eq!(&out[..2], &[Some(1.0), Some(1.0)]);
        assert_eq!(out[2].unwrap().to_bits(), 1u64 << 63);
        assert_eq!(&out[3..], &[Some(0.0), None, None, None, None]);
    }
}
#[test]
fn original_binary_raw_decimal128_independent_scales_and_float32_widening() {
    let left = decimal(vec![Some(123), Some(-123), None], -2);
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(2_000_000); 3])
            .with_precision_and_scale(7, 3)
            .unwrap(),
    );
    assert_eq!(
        floats(evaluate("fmod", vec![left, right], DataType::Float64).unwrap()),
        vec![Some(300.0), Some(-300.0), None]
    );
    let f32: ArrayRef = Arc::new(Float32Array::from(vec![Some(f32::MAX), None]));
    let exponent = ints(vec![Some(2), Some(2)]);
    let out = floats(evaluate("pow", vec![f32, exponent], DataType::Float64).unwrap());
    assert_eq!(out[0].unwrap().to_bits(), 0x4fefffffc0000020);
    assert_eq!(out[1], None);
}
#[test]
fn original_binary_raw_largeint_decimal256_utf8_admission_errors_are_unchanged() {
    let fsb: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once([0u8; 16])).unwrap());
    let d256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::ONE)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    let utf8: ArrayRef = Arc::new(StringArray::from(vec!["7"]));
    for source in [fsb, d256, utf8] {
        for name in names() {
            let expected = format!("unsupported numeric type: {:?}", source.data_type());
            assert_eq!(
                evaluate(
                    name,
                    vec![source.clone(), ints(vec![Some(2)])],
                    DataType::Float64
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(
                evaluate(
                    name,
                    vec![ints(vec![Some(7)]), source.clone()],
                    DataType::Float64
                )
                .unwrap_err(),
                expected
            );
        }
    }
}
#[test]
fn original_binary_raw_output_projection_safe_cast_and_null_carrier_are_frozen() {
    let out = evaluate(
        "pow",
        vec![ints(vec![Some(2), Some(2)]), ints(vec![Some(7), Some(1)])],
        DataType::Int8,
    )
    .unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(2)]
    );
    let f32: ArrayRef = Arc::new(Float32Array::from(vec![Some(f32::MAX), None]));
    let out = evaluate(
        "pow",
        vec![f32, ints(vec![Some(2), Some(2)])],
        DataType::Float32,
    )
    .unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, None]
    );
    for name in names() {
        let null: ArrayRef = Arc::new(NullArray::new(2));
        assert_eq!(
            floats(
                evaluate(
                    name,
                    vec![null, ints(vec![Some(0), Some(3)])],
                    DataType::Float64
                )
                .unwrap()
            ),
            vec![None, None]
        );
    }
}
#[test]
fn original_binary_sliced_empty_ignored_tail_and_arity_panic_are_frozen() {
    for name in names() {
        let left = ints(vec![Some(99), Some(2), None, Some(2), Some(99)]).slice(1, 3);
        let right = ints(vec![Some(99), Some(3), Some(3), Some(2), Some(99)]).slice(1, 3);
        let base = evaluate(name, vec![left.clone(), right.clone()], DataType::Float64).unwrap();
        let ignored: ArrayRef = Arc::new(StringArray::from(vec!["unused"; 3]));
        assert_eq!(
            floats(
                evaluate(
                    name,
                    vec![left.clone(), right.clone(), ignored],
                    DataType::Float64
                )
                .unwrap()
            ),
            floats(base)
        );
        assert!(
            evaluate(
                name,
                vec![left.slice(0, 0), right.slice(0, 0)],
                DataType::Float64
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| evaluate(
                name,
                vec![ints(vec![Some(7)])],
                DataType::Float64
            )))
            .is_err()
        );
    }
}

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
//! Original dispatcher/ExprArena baselines; no pure owner or decoder replacement.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, object::eval_object_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use novarocks_functions::{ConstantPool, FunctionValueType};
use novarocks_type_contract::CompilePhase;
use novarocks_types::{SlotId, value::hll};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn setup(a: ArrayRef) -> (ExprArena, ExprId, Chunk) {
    let slot = SlotId::new(1);
    let ty = a.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("original", ty.clone(), true)])),
        vec![a],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let id = arena.push_typed(ExprNode::SlotId(slot), ty);
    (arena, id, chunk)
}
fn raw(a: ArrayRef, target: Option<DataType>, tails: usize) -> Result<ArrayRef, String> {
    let (mut arena, id, chunk) = setup(a);
    let mut args = vec![id];
    args.extend(std::iter::repeat_n(ExprId(usize::MAX), tails));
    let expr = target.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("hll_hash"),
                args: args.clone(),
            },
            ty,
        )
    });
    eval_object_function("hll_hash", &arena, expr, &args, &chunk)
}
fn payloads(a: ArrayRef) -> Vec<Vec<u8>> {
    assert_eq!(a.data_type(), &DataType::Binary);
    assert_eq!(a.null_count(), 0);
    a.as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_vec())
        .collect()
}
fn encoded(bytes: &[u8]) -> Vec<u8> {
    hll::encode_hll_single(hll::murmur_hash64a(bytes, hll::MURMUR_SEED))
}
fn verify(a: ArrayRef, expected: Vec<Vec<u8>>) {
    assert_eq!(payloads(raw(a.clone(), None, 0).unwrap()), expected);
    if a.len() >= 2 {
        assert_eq!(
            payloads(raw(a.slice(1, a.len() - 1), None, 0).unwrap()),
            expected[1..]
        );
    }
    assert!(raw(a.slice(0, 0), None, 0).unwrap().is_empty());
}
#[test]
fn legacy_hll_hash_original_integer_boolean_date_and_typed_null_bytes() {
    // Explicit bytes keep width visible; HLL does not normalize across widths.
    verify(
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(i8::MAX),
            Some(0),
        ])),
        vec![
            encoded(&i8::MIN.to_le_bytes()),
            vec![0],
            encoded(&i8::MAX.to_le_bytes()),
            encoded(&0i8.to_le_bytes()),
        ],
    );
    verify(
        Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
        vec![
            encoded(&i16::MIN.to_le_bytes()),
            vec![0],
            encoded(&i16::MAX.to_le_bytes()),
        ],
    );
    verify(
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
        vec![
            encoded(&i32::MIN.to_le_bytes()),
            vec![0],
            encoded(&i32::MAX.to_le_bytes()),
        ],
    );
    verify(
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        vec![
            encoded(&i64::MIN.to_le_bytes()),
            vec![0],
            encoded(&i64::MAX.to_le_bytes()),
        ],
    );
    verify(
        Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
        vec![encoded(&[0]), vec![0], encoded(&[1])],
    );
    verify(
        Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
        ])),
        vec![
            encoded(&i32::MIN.to_le_bytes()),
            vec![0],
            encoded(&i32::MAX.to_le_bytes()),
        ],
    );
}
#[test]
fn legacy_hll_hash_original_ieee_bits_are_not_ndv_canonical_bits() {
    let f32s = [
        0.,
        -0.,
        f32::from_bits(0x7fc0_0123),
        f32::INFINITY,
        f32::NEG_INFINITY,
    ];
    let f64s = [
        0.,
        -0.,
        f64::from_bits(0x7ff8_0000_0000_0123),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    for a in [
        Arc::new(Float32Array::from_iter_values(f32s)) as ArrayRef,
        Arc::new(Float64Array::from_iter_values(f64s)),
    ] {
        let out = payloads(raw(a, None, 0).unwrap());
        assert_ne!(out[0], out[1]);
    }
    verify(
        Arc::new(Float32Array::from_iter_values(f32s)),
        f32s.iter().map(|v| encoded(&v.to_le_bytes())).collect(),
    );
    verify(
        Arc::new(Float64Array::from_iter_values(f64s)),
        f64s.iter().map(|v| encoded(&v.to_le_bytes())).collect(),
    );
}
#[test]
fn legacy_hll_hash_original_timestamp_timezone_units_and_decimal_raw_payload() {
    macro_rules! timestamp {
        ($array:ident) => {
            for zone in [None, Some("UTC"), Some("America/New_York")] {
                let a = $array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])
                    .with_timezone_opt(zone);
                verify(
                    Arc::new(a),
                    vec![
                        encoded(&i64::MIN.to_le_bytes()),
                        vec![0],
                        encoded(&i64::MAX.to_le_bytes()),
                    ],
                );
            }
        };
    }
    timestamp!(TimestampSecondArray);
    timestamp!(TimestampMillisecondArray);
    timestamp!(TimestampMicrosecondArray);
    timestamp!(TimestampNanosecondArray);
    for p in 1..=38 {
        for s in [-128, -76, -38, -1, 0, p as i8] {
            let a = Decimal128Array::from(vec![Some(i128::MIN), None, Some(i128::MAX)])
                .with_precision_and_scale(p, s)
                .unwrap();
            verify(
                Arc::new(a),
                vec![
                    encoded(&i128::MIN.to_le_bytes()),
                    vec![0],
                    encoded(&i128::MAX.to_le_bytes()),
                ],
            );
        }
    }
}
#[test]
fn legacy_hll_hash_original_utf8_binary_and_all_fixed_widths() {
    let utf8 = [Some(""), None, Some("é雪\0"), Some("novarocks")];
    let expect = vec![
        encoded(b""),
        vec![0],
        encoded("é雪\0".as_bytes()),
        encoded(b"novarocks"),
    ];
    verify(Arc::new(StringArray::from(utf8.to_vec())), expect.clone());
    verify(
        Arc::new(LargeStringArray::from(utf8.to_vec())),
        expect.clone(),
    );
    let bytes = [
        Some(b"".as_slice()),
        None,
        Some("é雪\0".as_bytes()),
        Some(b"novarocks".as_slice()),
    ];
    verify(Arc::new(BinaryArray::from(bytes.to_vec())), expect.clone());
    verify(Arc::new(LargeBinaryArray::from(bytes.to_vec())), expect);
    for width in [0, 1, 15, 16, 17, 32] {
        let v = vec![0xff; width as usize];
        let a = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(v.as_slice()), None].into_iter(),
            width,
        )
        .unwrap();
        verify(Arc::new(a), vec![encoded(&v), vec![0]]);
    }
    let hidden = BinaryArray::new(
        OffsetBuffer::new(vec![0i32, 3, 4].into()),
        Buffer::from(b"bad\0".as_slice()),
        Some(NullBuffer::from(vec![false, true])),
    );
    verify(Arc::new(hidden), vec![vec![0], encoded(&[0])]);
}
#[test]
fn legacy_hll_hash_original_unsupported_carrier_full_error_precedes_empty_null() {
    let nested = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_long_field_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Null,
        DataType::UInt64,
        DataType::Decimal256(76, 2),
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        nested,
    ] {
        let expected = format!("hll_hash expects scalar input, got {ty:?}");
        for n in [0, 1, 3] {
            assert_eq!(raw(new_null_array(&ty, n), None, 0).unwrap_err(), expected);
        }
        if matches!(ty, DataType::Struct(_)) {
            assert!(expected.len() > 512);
        }
    }
}
#[test]
fn legacy_hll_hash_original_constant_broadcast_nonzero_pool_and_ignored_target() {
    let (_, _, chunk) = setup(Arc::new(Int32Array::from(vec![0, 0, 0, 0])));
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let backing = Arc::new(StringArray::from(vec!["unused", "é雪"])) as ArrayRef;
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("constant").unwrap()),
        ty,
        backing.to_data(),
        super::pure_differential::constant_policy(),
        CompilePhase::FunctionSpecialization,
        &super::pure_differential::HarnessControl,
    )
    .unwrap();
    for node in [
        ExprNode::Constant(pool.value(1).unwrap()),
        ExprNode::Literal(LiteralValue::Utf8("é雪".into())),
    ] {
        let mut arena = ExprArena::default();
        let id = arena.push_typed(node, DataType::Utf8);
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("hll_hash"),
                args: vec![id],
            },
            DataType::Binary,
        );
        assert_eq!(
            payloads(arena.eval(call, &chunk).unwrap()),
            vec![encoded("é雪".as_bytes()); 4]
        );
    }
    for target in [
        None,
        Some(DataType::Binary),
        Some(DataType::Float64),
        Some(DataType::Null),
    ] {
        assert_eq!(
            payloads(raw(Arc::new(Int32Array::from(vec![1])), target, 4).unwrap()),
            vec![encoded(&1i32.to_le_bytes())]
        );
    }
}
#[test]
fn legacy_hll_hash_original_raw_first_only_and_native_arity_are_distinct() {
    let (mut arena, id, chunk) = setup(Arc::new(Int32Array::from(vec![1])));
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("hll_hash"),
            args: vec![id, ExprId(usize::MAX)],
        },
        DataType::Binary,
    );
    assert_eq!(
        arena.eval(call, &chunk).unwrap_err(),
        "hll_hash expects 1 to 1 arguments, got 2"
    );
    assert_eq!(
        eval_object_function(
            "hll_hash",
            &arena,
            ExprId(usize::MAX),
            &[ExprId(usize::MAX), id],
            &chunk
        )
        .unwrap_err(),
        "invalid ExprId"
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_object_function(
            "hll_hash",
            &arena,
            ExprId(usize::MAX),
            &[],
            &chunk
        )))
        .is_err()
    );
}

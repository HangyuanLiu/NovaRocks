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
//! Original public scalar dispatcher, before the missing owner or row core is installed.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, object::eval_object_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
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
                kind: FunctionKind::Object("percentile_hash"),
                args: args.clone(),
            },
            ty,
        )
    });
    eval_object_function("percentile_hash", &arena, expr, &args, &chunk)
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
const EMPTY: &[u8] = &[0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0];
#[test]
fn legacy_percentile_hash_original_numeric_null_nan_and_exact_state_bytes() {
    let a = Arc::new(Float64Array::from(vec![
        Some(1.),
        None,
        Some(f64::NAN),
        Some(0.),
        Some(-0.),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(16777217.),
    ])) as ArrayRef;
    let out = payloads(raw(a, None, 0).unwrap());
    assert_eq!(out[0],hex::decode("a20400102700000000000000401c46ffff7f7fffff7fff204e0000000000008038010000000000000000000000803f00000000010000000000803f0000803f00000000").unwrap());
    assert_eq!(out[1], EMPTY);
    assert_eq!(out[2], EMPTY);
    for (i, bits) in [
        (3, 0f32.to_bits()),
        (4, (-0f32).to_bits()),
        (5, f32::INFINITY.to_bits()),
        (6, f32::NEG_INFINITY.to_bits()),
        (7, 16777216f32.to_bits()),
    ] {
        assert_eq!(out[i].len(), 67);
        assert_eq!(u32::from_le_bytes(out[i][55..59].try_into().unwrap()), bits);
    }
    // Original unprocessed singleton serialization retains its extreme sentinels.
    assert_eq!(&out[0][15..19], &f32::MAX.to_le_bytes());
    assert_eq!(&out[0][19..23], &f32::MIN.to_le_bytes());
}
#[test]
fn legacy_percentile_hash_original_all_numeric_readers_slice_null_and_empty() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(Int16Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(Float32Array::from(vec![Some(1.), None, Some(-1.)])),
        Arc::new(Float64Array::from(vec![Some(1.), None, Some(-1.)])),
        Arc::new(
            Decimal128Array::from(vec![Some(100), None, Some(-100)])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
        novarocks_functions::largeint::array_from_i128(&[Some(1), None, Some(-1)]).unwrap(),
    ];
    let reference = payloads(
        raw(
            Arc::new(Int32Array::from(vec![Some(1), None, Some(-1)])),
            None,
            0,
        )
        .unwrap(),
    );
    for a in arrays {
        assert_eq!(payloads(raw(a.clone(), None, 3).unwrap()), reference);
        assert_eq!(
            payloads(raw(a.slice(1, 2), None, 0).unwrap()),
            reference[1..]
        );
        assert!(raw(a.slice(0, 0), None, 0).unwrap().is_empty());
    }
}
#[test]
fn legacy_percentile_hash_original_decimal_signed_scale_and_largeint_extremes() {
    for precision in 1..=38 {
        for scale in [-128, -76, -38, -1, 0, precision as i8] {
            let a = Arc::new(
                Decimal128Array::from(vec![
                    Some(i128::MIN),
                    None,
                    Some(i128::MAX),
                    Some(-1),
                    Some(0),
                ])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
            ) as ArrayRef;
            let original = raw(a.clone(), None, 0).unwrap();
            let sliced = payloads(raw(a.slice(1, 3), None, 0).unwrap());
            assert_eq!(sliced, payloads(original)[1..4]);
            assert_eq!(sliced[0], EMPTY);
        }
    }
    let a = novarocks_functions::largeint::array_from_i128(&[
        Some(i128::MIN),
        Some(i128::MAX),
        None,
        Some(-1),
    ])
    .unwrap();
    let out = payloads(raw(a, None, 0).unwrap());
    assert_eq!(out[2], EMPTY);
    assert_eq!(out[0].len(), 67);
    assert_eq!(out[1].len(), 67);
}
#[test]
fn legacy_percentile_hash_original_full_carrier_error_null_mask_and_zero_rows() {
    let nested = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_long_field_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Null,
        DataType::UInt64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::Binary,
        DataType::Decimal256(76, 2),
        DataType::FixedSizeBinary(15),
        nested,
    ] {
        let a = new_null_array(&ty, 1);
        let expected = format!("percentile_hash: unsupported numeric input type {:?}", ty);
        assert_eq!(raw(a, None, 4).unwrap_err(), expected);
        if matches!(ty, DataType::Struct(_)) {
            assert!(expected.len() > 512);
        }
        // Original per-row admission does not inspect an unsupported empty carrier.
        assert!(raw(new_empty_array(&ty), None, 4).unwrap().is_empty());
    }
}
#[test]
fn legacy_percentile_hash_original_first_only_demand_ignored_target_and_raw_arity() {
    for target in [
        None,
        Some(DataType::Binary),
        Some(DataType::Float64),
        Some(DataType::Null),
    ] {
        assert_eq!(
            payloads(raw(Arc::new(Int32Array::from(vec![Some(1), None])), target, 5).unwrap())[1],
            EMPTY
        );
    }
    let (arena, id, chunk) = setup(Arc::new(Int32Array::from(vec![Some(1)])));
    assert_eq!(
        eval_object_function(
            "percentile_hash",
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
            "percentile_hash",
            &arena,
            ExprId(usize::MAX),
            &[],
            &chunk
        )))
        .is_err()
    );
}

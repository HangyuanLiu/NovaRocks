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
//! Immutable actual v1 SHA2 carrier, NULL, selector and admission-order baselines.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, encryption::eval_encryption_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        NullArray, StringArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn bits(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn raw(input: ArrayRef, length: ArrayRef, target: Option<DataType>) -> Result<ArrayRef, String> {
    let ids = [SlotId::new(1), SlotId::new(2)];
    let types = [input.data_type().clone(), length.data_type().clone()];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("bytes", types[0].clone(), true),
            Field::new("bits", types[1].clone(), true),
        ])),
        vec![input, length],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &ids).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = ids
        .into_iter()
        .zip(types)
        .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
        .collect::<Vec<_>>();
    let expr = target.map_or(ExprId(usize::MAX), |target| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Encryption("sha2"),
                args: args.clone(),
            },
            target,
        )
    });
    eval_encryption_function("sha2", &arena, expr, &args, &chunk)
}
fn values(out: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(out.data_type(), &DataType::Utf8);
    out.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}
const ABC256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
#[test]
fn legacy_sha2_all_original_selectors_targets_null_and_slices() {
    let text = strings(vec![
        Some("guard"),
        Some("abc"),
        Some("abc"),
        Some("abc"),
        Some("abc"),
        Some("abc"),
        Some("abc"),
        None,
        Some("abc"),
        Some("abc"),
        Some("guard"),
    ])
    .slice(1, 9);
    let selector = bits(vec![
        Some(999),
        Some(224),
        Some(0),
        Some(256),
        Some(384),
        Some(512),
        Some(-1),
        Some(256),
        None,
        Some(i64::MAX),
        Some(999),
    ])
    .slice(1, 9);
    let expected=vec![Some("23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7".into()),Some(ABC256.into()),Some(ABC256.into()),Some("cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7".into()),Some("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f".into()),None,None,None,None];
    for target in [
        None,
        Some(DataType::Utf8),
        Some(DataType::Binary),
        Some(DataType::Int64),
    ] {
        assert_eq!(
            values(raw(text.clone(), selector.clone(), target).unwrap()),
            expected
        );
    }
    assert_eq!(
        values(raw(strings(vec![Some("")]), bits(vec![Some(256)]), None).unwrap()),
        vec![Some(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into()
        )]
    );
}
#[test]
fn legacy_sha2_extra_raw_byte_carriers_and_original_int32_bigint_projection() {
    let length = Arc::new(Int32Array::from(vec![Some(256), Some(256), Some(-1)])) as ArrayRef;
    for input in [
        Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"x".as_slice()),
        ])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"x".as_slice()),
        ])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![Some("abc"), None, Some("x")])) as ArrayRef,
    ] {
        assert_eq!(
            values(raw(input, length.clone(), None).unwrap()),
            vec![Some(ABC256.into()), None, None]
        );
    }
    assert_eq!(
        values(
            raw(
                Arc::new(NullArray::new(2)),
                bits(vec![Some(256), None]),
                None
            )
            .unwrap()
        ),
        vec![None, None]
    );
    assert!(values(raw(strings(vec![]), bits(vec![]), None).unwrap()).is_empty());
}
#[test]
fn legacy_sha2_reader_error_order_full_text_precedes_null_mask() {
    let bad = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
    assert_eq!(
        raw(bad, strings(vec![None]), None).unwrap_err(),
        "sha2: arg0 must be VARCHAR or VARBINARY"
    );
    // Compare the exact original Arrow cast diagnostic, including its full carrier detail.
    let wrong = Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef;
    let expected = arrow::compute::cast(&wrong, &DataType::Int64).unwrap_err();
    assert_eq!(
        raw(strings(vec![None]), wrong, None).unwrap_err(),
        format!("sha2: failed to cast arg1 to BIGINT: {expected}")
    );
}
#[test]
fn legacy_sha2_index_panic_precedes_absent_child_and_long_stream_has_original_digest() {
    let mut arena = ExprArena::default();
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let expr = arena.push_typed(
        ExprNode::Literal(crate::exec::expr::LiteralValue::Null),
        DataType::Utf8,
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_encryption_function(
            "sha2",
            &arena,
            expr,
            &[],
            &chunk
        )))
        .is_err()
    );
    let long = "é中\0".repeat(200);
    // Independently frozen Python hashlib SHA256, not the production helper.
    assert_eq!(
        values(raw(strings(vec![Some(&long)]), bits(vec![Some(256)]), None).unwrap()),
        vec![Some(
            "89e628e761d207ac1428d1d008f368a72c0787f1961d84317aa0350836f6152c".into()
        )]
    );
}

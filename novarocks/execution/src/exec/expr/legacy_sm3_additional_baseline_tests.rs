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
//! Immutable original v1 SM3 bytes, grouped text, successful empty and error ordering.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, encryption::eval_encryption_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, LargeBinaryArray, LargeStringArray,
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
fn raw(input: ArrayRef, target: Option<DataType>) -> Result<ArrayRef, String> {
    let slot = SlotId::new(1);
    let ty = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("input", ty.clone(), true)])),
        vec![input],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(slot), ty);
    let args = vec![input];
    let expr = target.map_or(ExprId(usize::MAX), |target| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Encryption("sm3"),
                args: args.clone(),
            },
            target,
        )
    });
    eval_encryption_function("sm3", &arena, expr, &args, &chunk)
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
const ABC: &str = "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0";
#[test]
fn legacy_sm3_original_grouping_empty_not_standard_digest_null_slice_ignored_targets() {
    let block = "abcd".repeat(16);
    let input = strings(vec![
        Some("guard"),
        Some("abc"),
        Some(""),
        None,
        Some(&block),
        Some("guard"),
    ])
    .slice(1, 4);
    let expected = vec![
        Some(ABC.into()),
        Some("".into()),
        None,
        Some("debe9ff9 2275b8a1 38604889 c18e5a4d 6fdb70e5 387e5765 293dcba3 9c0c5732".into()),
    ];
    for target in [
        None,
        Some(DataType::Utf8),
        Some(DataType::Binary),
        Some(DataType::Int64),
    ] {
        assert_eq!(values(raw(input.clone(), target).unwrap()), expected);
    }
    let out = values(raw(strings(vec![Some("abc")]), None).unwrap())
        .pop()
        .unwrap()
        .unwrap();
    assert_eq!(out.len(), 71);
    assert_eq!(out.split(' ').count(), 8);
    assert!(out.split(' ').all(|word| {
        word.len() == 8
            && word
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }));
}
#[test]
fn legacy_sm3_extra_raw_byte_carriers_null_empty_and_non_utf8_bytes_remain_exact() {
    for input in [
        Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![Some("abc"), None, Some("")])) as ArrayRef,
    ] {
        assert_eq!(
            values(raw(input, None).unwrap()),
            vec![Some(ABC.into()), None, Some("".into())]
        );
    }
    assert_eq!(
        values(raw(Arc::new(NullArray::new(2)), None).unwrap()),
        vec![None, None]
    );
    assert!(values(raw(strings(vec![]), None).unwrap()).is_empty());
    let bytes = Arc::new(BinaryArray::from(vec![Some([0u8, 255, 128].as_slice())])) as ArrayRef;
    assert_eq!(
        values(raw(bytes, None).unwrap()),
        vec![Some(
            "119ce5ad 7e22eec2 f74a2069 79a88c36 43dbbd9b 11890a7a 18080ef2 70b7c566".into()
        )]
    );
}
#[test]
fn legacy_sm3_static_reader_error_full_text_precedes_sql_null_and_ignores_target() {
    for input in [
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![None])) as ArrayRef,
    ] {
        for target in [None, Some(DataType::Binary), Some(DataType::Int64)] {
            assert_eq!(
                raw(input.clone(), target).unwrap_err(),
                "sm3: arg0 must be VARCHAR or VARBINARY"
            );
        }
    }
}
#[test]
fn legacy_sm3_original_empty_args_index_panic_and_long_utf8_byte_stream_golden() {
    let mut arena = ExprArena::default();
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_encryption_function(
            "sm3",
            &arena,
            expr,
            &[],
            &chunk
        )))
        .is_err()
    );
    let long = "é中\0".repeat(200);
    // Independent Python hashlib byte-stream oracles, not the production recipe.
    assert_eq!(
        values(raw(strings(vec![Some(&long), Some("a\0b")]), None).unwrap()),
        vec![
            Some("5c254c6f f59b2d45 7a603933 6738ec1e e8e62ed5 6cd7305a 1e0ac59d 868527e3".into()),
            Some("35b867ed 6528bb46 099058ba f776e4ee fcf98d6d accc0f67 8541899d f16fd639".into())
        ]
    );
}

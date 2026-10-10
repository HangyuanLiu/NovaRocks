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
//! Unchanged v1 TO_BINARY bytes, eager input order and target-dependent projection.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, encryption::eval_to_binary};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{Array, ArrayRef, BinaryArray, Int64Array, LargeStringArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(crate) fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
pub(crate) fn setup(arrays: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots: Vec<_> = (0..arrays.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect();
    let fields: Vec<_> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.clone()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = arrays
        .iter()
        .zip(slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(s), a.data_type().clone()))
        .collect();
    (arena, args, chunk)
}
pub(crate) fn raw(arrays: Vec<ArrayRef>, target: DataType) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(arrays);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption("to_binary"),
            args: args.clone(),
        },
        target,
    );
    eval_to_binary(&arena, expr, &args, &chunk)
}
pub(crate) fn bytes(array: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect()
}
#[test]
fn legacy_to_binary_one_argument_hex_null_empty_invalid_slice() {
    let input = strings(vec![
        Some("guard"),
        Some("00fFaB"),
        Some(""),
        Some("F"),
        Some("xx"),
        None,
        Some("中"),
        Some("guard"),
    ])
    .slice(1, 6);
    assert_eq!(
        bytes(&raw(vec![input], DataType::Binary).unwrap()),
        vec![
            Some(vec![0, 255, 171]),
            Some(vec![]),
            None,
            None,
            None,
            None
        ]
    );
}
#[test]
fn legacy_to_binary_two_arguments_null_unknown_and_case_formats() {
    let input = strings(vec![
        Some("ff"),
        Some("ff"),
        Some("世界"),
        Some("YWJj"),
        Some(""),
        Some(""),
        None,
        Some("00"),
        Some("00"),
        Some("YQ=="),
        Some("YQ"),
        Some("!"),
    ]);
    let fmt = strings(vec![
        None,
        Some("unknown"),
        Some("UtF8"),
        Some("EnCoDe64"),
        Some("encode64"),
        Some("utf8"),
        Some("utf8"),
        Some(" utf8 "),
        Some(""),
        Some("encode64"),
        Some("encode64"),
        Some("encode64"),
    ]);
    assert_eq!(
        bytes(&raw(vec![input, fmt], DataType::Binary).unwrap()),
        vec![
            Some(vec![255]),
            Some(vec![255]),
            Some("世界".as_bytes().to_vec()),
            Some(b"abc".to_vec()),
            None,
            Some(vec![]),
            None,
            Some(vec![0]),
            Some(vec![0]),
            Some(b"a".to_vec()),
            None,
            None
        ]
    );
}
#[test]
fn legacy_to_binary_exact_arity_admission_and_eager_format_errors() {
    let (mut arena, args, chunk) = setup(vec![strings(vec![None])]);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption("to_binary"),
            args: args.clone(),
        },
        DataType::Binary,
    );
    assert_eq!(
        eval_to_binary(&arena, expr, &[], &chunk).unwrap_err(),
        "to_binary expects 1 or 2 arguments"
    );
    assert_eq!(
        eval_to_binary(&arena, expr, &[args[0], args[0], args[0]], &chunk).unwrap_err(),
        "to_binary expects 1 or 2 arguments"
    );
    assert_eq!(
        eval_to_binary(&arena, expr, &[args[0], ExprId(usize::MAX)], &chunk).unwrap_err(),
        "invalid ExprId"
    );
    assert_eq!(
        raw(
            vec![
                strings(vec![None]),
                Arc::new(Int64Array::from(vec![Some(1)]))
            ],
            DataType::Binary
        )
        .unwrap_err(),
        "to_binary expects VARCHAR format argument"
    );
    assert_eq!(
        raw(
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])),
                strings(vec![None])
            ],
            DataType::Binary
        )
        .unwrap_err(),
        "to_binary expects VARCHAR as first argument"
    );
    assert_eq!(
        raw(
            vec![Arc::new(LargeStringArray::from(vec![Some("00")]))],
            DataType::Binary
        )
        .unwrap_err(),
        "to_binary expects VARCHAR as first argument"
    );
}
#[test]
fn legacy_to_binary_latin1_projection_and_empty_batch_preserved() {
    let out = raw(
        vec![strings(vec![Some("00ff80"), None, Some("")])],
        DataType::Utf8,
    )
    .unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("\0ÿ\u{80}"), None, Some("")]
    );
    let out = raw(vec![strings(vec![])], DataType::Binary).unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(out.data_type(), &DataType::Binary);
}

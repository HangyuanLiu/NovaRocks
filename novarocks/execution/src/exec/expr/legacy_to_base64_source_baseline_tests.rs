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
//! Actual original source/data/arity/carrier oracles; no guessed provenance from values.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, encryption::eval_encryption_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
        StringArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
#[derive(Clone, Copy)]
pub(crate) enum Source {
    Slot,
    Literal,
    FromBase64,
    ToBinary,
    CastFromBase64,
    ConcatFromBase64,
    AesEncrypt,
}
pub(crate) fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn encryption(arena: &mut ExprArena, name: &'static str, args: Vec<ExprId>) -> ExprId {
    arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            args,
        },
        DataType::Utf8,
    )
}
fn setup(input: ArrayRef, source: Source) -> (ExprArena, ExprId, Chunk) {
    let slot = SlotId::new(1);
    let ty = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("input", ty.clone(), true)])),
        vec![input.clone()],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(slot), ty);

    let child = match source {
        Source::Slot => arg,
        Source::Literal => {
            let text = input.as_any().downcast_ref::<StringArray>().unwrap();
            let value = if text.is_null(0) {
                LiteralValue::Null
            } else {
                LiteralValue::Utf8(text.value(0).to_owned())
            };
            arena.push_typed(ExprNode::Literal(value), DataType::Utf8)
        }
        Source::FromBase64 => encryption(&mut arena, "from_base64", vec![arg]),
        Source::ToBinary => encryption(&mut arena, "to_binary", vec![arg]),
        Source::CastFromBase64 => {
            let decoded = encryption(&mut arena, "from_base64", vec![arg]);
            arena.push_typed(
                ExprNode::Cast(decoded, DecimalOverflowPolicy::OutputNull),
                DataType::Utf8,
            )
        }
        Source::ConcatFromBase64 => {
            let decoded = encryption(&mut arena, "from_base64", vec![arg]);
            arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::String("concat"),
                    args: vec![decoded],
                },
                DataType::Utf8,
            )
        }
        Source::AesEncrypt => {
            let key = arena.push_typed(
                ExprNode::Literal(LiteralValue::Utf8("0123456789abcdef".into())),
                DataType::Utf8,
            );
            arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::Encryption("aes_encrypt"),
                    args: vec![arg, key],
                },
                DataType::Utf8,
            )
        }
    };
    (arena, child, chunk)
}
pub(crate) fn raw(input: ArrayRef, source: Source, target: DataType) -> Result<ArrayRef, String> {
    let (mut arena, child, chunk) = setup(input, source);
    let args = vec![child];
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption("to_base64"),
            args: args.clone(),
        },
        target,
    );
    eval_encryption_function("to_base64", &arena, expr, &args, &chunk)
}
pub(crate) fn values(out: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(out.data_type(), &DataType::Utf8);
    out.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}
#[test]
fn legacy_to_base64_original_ordinary_full_utf8_null_empty_slice_target_ignored() {
    let input = strings(vec![
        Some("guard"),
        Some("ÿ"),
        Some("é"),
        Some("Ā"),
        Some("a\0b"),
        Some(""),
        None,
        Some("guard"),
    ])
    .slice(1, 6);
    for target in [DataType::Utf8, DataType::Binary, DataType::Int64] {
        assert_eq!(
            values(raw(input.clone(), Source::Slot, target).unwrap()),
            vec![
                Some("w78=".into()),
                Some("w6k=".into()),
                Some("xIA=".into()),
                Some("YQBi".into()),
                None,
                None
            ]
        );
    }
    for s in [Source::Slot, Source::Literal] {
        assert_eq!(
            values(raw(strings(vec![Some("ÿ")]), s, DataType::Utf8).unwrap()),
            vec![Some("w78=".into())]
        );
    }
    assert!(values(raw(strings(vec![]), Source::Slot, DataType::Utf8).unwrap()).is_empty());
}
#[test]
fn legacy_to_base64_original_immediate_latin1_vs_cast_and_other_calls_are_value_distinct() {
    let encoded = strings(vec![
        Some("/w=="),
        Some("/wA="),
        Some(""),
        None,
        Some("invalid"),
    ]);
    assert_eq!(
        values(raw(encoded.clone(), Source::FromBase64, DataType::Utf8).unwrap()),
        vec![Some("/w==".into()), Some("/wA=".into()), None, None, None]
    );
    for source in [Source::CastFromBase64, Source::ConcatFromBase64] {
        assert_eq!(
            values(raw(encoded.clone(), source, DataType::Utf8).unwrap()),
            vec![Some("w78=".into()), Some("w78A".into()), None, None, None]
        );
    }
    assert_eq!(
        values(
            raw(
                strings(vec![Some("FF"), Some("FF00"), Some(""), None, Some("GG")]),
                Source::ToBinary,
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some("/w==".into()), Some("/wA=".into()), None, None, None]
    );
    // AES bytes are pinned independently by OpenSSL's fixed-key AES-128-ECB result.
    let aes = raw(
        strings(vec![Some(""), None]),
        Source::AesEncrypt,
        DataType::Utf8,
    )
    .unwrap();
    assert_eq!(
        values(aes),
        vec![Some("N3Ii4GGpJMWRzZwn6hY+1A==".into()), None]
    );
}
#[test]
fn legacy_to_base64_original_byte_carriers_and_full_static_error_before_null() {
    for input in [
        Arc::new(BinaryArray::from(vec![
            Some([255u8, 0].as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef,
        Arc::new(LargeBinaryArray::from(vec![
            Some([255u8, 0].as_slice()),
            None,
            Some(b"".as_slice()),
        ])),
    ] {
        assert_eq!(
            values(raw(input, Source::Slot, DataType::Utf8).unwrap()),
            vec![Some("/wA=".into()), None, None]
        );
    }
    assert_eq!(
        values(
            raw(
                Arc::new(LargeStringArray::from(vec![Some("ÿ"), None, Some("")])),
                Source::Slot,
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some("w78=".into()), None, None]
    );
    assert_eq!(
        values(raw(Arc::new(NullArray::new(2)), Source::Slot, DataType::Utf8).unwrap()),
        vec![None, None]
    );
    assert_eq!(
        raw(
            Arc::new(Int64Array::from(vec![None])),
            Source::Slot,
            DataType::Utf8
        )
        .unwrap_err(),
        "to_base64: arg0 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn legacy_to_base64_original_extra_arguments_are_not_evaluated_and_empty_args_panic() {
    let (arena, child, chunk) = setup(strings(vec![Some("ÿ")]), Source::Slot);
    let args = [child, ExprId(usize::MAX)];
    assert_eq!(
        values(eval_encryption_function("to_base64", &arena, ExprId(0), &args, &chunk).unwrap()),
        vec![Some("w78=".into())]
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_encryption_function(
            "to_base64",
            &arena,
            ExprId(0),
            &[],
            &chunk
        )))
        .is_err()
    );
    let bad = [ExprId(usize::MAX), child];
    assert_eq!(
        eval_encryption_function("to_base64", &arena, ExprId(0), &bad, &chunk).unwrap_err(),
        "invalid ExprId"
    );
}

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

//! Original Binary, LargeBinary and FixedSizeBinary CAST boundary evidence.
use super::{ExprArena, ExprNode, cast_array_to_target};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) fn actual(
    input: &ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let slot = SlotId::new(1);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            input.data_type().clone(),
            true,
        )])),
        vec![input.clone()],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(slot), input.data_type().clone());
    let cast = arena.push_typed(ExprNode::Cast(child, policy), DataType::Utf8);
    arena.eval(cast, &chunk)
}
pub(super) fn input() -> ArrayRef {
    Arc::new(
        BinaryArray::from(vec![
            Some(b"pad".as_slice()),
            Some("你好\0🙂".as_bytes()),
            Some(b"".as_slice()),
            Some(b"\xffa".as_slice()),
            None,
            Some(b"tail".as_slice()),
        ])
        .slice(1, 4),
    )
}
fn strings(value: &ArrayRef) -> Vec<Option<&str>> {
    value
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_binary_text_cast_original_valid_invalid_null_empty_and_slice_all_policies() {
    let array = input();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let out = actual(&array, policy, allow).unwrap();
            assert_eq!(out.data_type(), &DataType::Utf8);
            assert_eq!(strings(&out), vec![Some("你好\0🙂"), Some(""), None, None]);
            assert_eq!(
                strings(&cast_array_to_target(&array, &DataType::Utf8).unwrap()),
                strings(&out)
            );
            assert_eq!(
                strings(&actual(&array.slice(0, 0), policy, allow).unwrap()),
                Vec::<Option<&str>>::new()
            );
            let all_bytes: ArrayRef = Arc::new(BinaryArray::from(vec![Some(
                (0u8..=255).collect::<Vec<_>>().as_slice(),
            )]));
            assert_eq!(
                strings(&actual(&all_bytes, policy, allow).unwrap()),
                vec![None]
            );
        }
    }
}
#[test]
fn legacy_binary_text_cast_original_largebinary_project_and_expression_are_distinct() {
    let array: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"plain text".as_slice()),
        Some(b"".as_slice()),
        Some(b"\xff".as_slice()),
        None,
    ]));
    assert_eq!(
        strings(&cast_array_to_target(&array, &DataType::Utf8).unwrap()),
        vec![Some("plain text"), Some(""), None, None]
    );
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            assert_eq!(
                strings(&actual(&array, policy, allow).unwrap()),
                vec![None, None, None, None]
            );
        }
    }
}
#[test]
fn legacy_binary_text_cast_original_fixedsize16_is_largeint_and_other_widths_error() {
    let data = novarocks_functions::largeint::array_from_i128(&[Some(7), Some(-2)]).unwrap();
    assert_eq!(
        strings(&cast_array_to_target(&data, &DataType::Utf8).unwrap()),
        vec![Some("7"), Some("-2")]
    );
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            assert_eq!(
                strings(&actual(&data, policy, allow).unwrap()),
                vec![Some("7"), Some("-2")]
            );
            for width in [0, 1, 2, 15, 17, 32] {
                let bytes = vec![b'a'; width];
                let array: ArrayRef =
                    Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap());
                let inner = arrow::compute::cast(array.as_ref(), &DataType::Utf8)
                    .unwrap_err()
                    .to_string();
                assert_eq!(
                    cast_array_to_target(&array, &DataType::Utf8).unwrap_err(),
                    inner
                );
                assert_eq!(
                    actual(&array, policy, allow).unwrap_err(),
                    format!(
                        "CAST failed: from {:?} to {:?}: {inner}",
                        array.data_type(),
                        DataType::Utf8
                    )
                );
            }
        }
    }
}

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
//! Independent pre-extraction original arena text-to-TIME(Microsecond) receipts.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, StringViewArray, Time64MicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) fn modes() -> impl Iterator<Item = (bool, DecimalOverflowPolicy)> {
    [false, true].into_iter().flat_map(|allow| {
        [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ]
        .into_iter()
        .map(move |policy| (allow, policy))
    })
}
pub(super) fn actual(
    input: ArrayRef,
    allow: bool,
    policy: DecimalOverflowPolicy,
    datetime: bool,
) -> Result<ArrayRef, String> {
    let ty = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("input", ty.clone(), true)])),
        vec![input],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(91)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, cs);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(91)), ty);
    let kind = if datetime {
        ExprNode::CastTimeFromDatetime(child, policy)
    } else {
        ExprNode::CastTime(child, policy)
    };
    let root = arena.push_typed(kind, DataType::Time64(TimeUnit::Microsecond));
    arena.eval(root, &chunk)
}
pub(super) fn values(array: &ArrayRef) -> Vec<Option<i64>> {
    assert_eq!(array.data_type(), &DataType::Time64(TimeUnit::Microsecond));
    array
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .iter()
        .collect()
}
pub(super) fn input(dtype: &DataType, strings: &[Option<&str>]) -> ArrayRef {
    match dtype {
        DataType::Utf8 => Arc::new(StringArray::from(strings.to_vec())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(strings.to_vec())),
        DataType::Utf8View => Arc::new(StringViewArray::from(strings.to_vec())),
        _ => panic!("fixture requires one actual text carrier"),
    }
}
pub(super) fn corpus(dtype: &DataType) -> ArrayRef {
    input(
        dtype,
        &[
            Some("00:00:00"),
            Some("25:00:00"),
            Some("+01:02:03"),
            Some("-01:02:03"),
            Some("01:02:03.456789"),
            Some("1"),
            Some("+1"),
            Some("-1"),
            Some("12:10"),
            Some("23:59:60"),
            Some("1970-01-01 01:01:01"),
            Some(" 12 : 34 : 56 "),
            Some(""),
            Some("9223372036854775807:00:00"),
            Some("2562047788015:00:00"),
            None,
        ],
    )
}
#[test]
fn text_time_original_utf8_duration_checked_overflow_signs_trim_and_null() {
    let input = corpus(&DataType::Utf8);
    let expected = vec![
        Some(0),
        Some(90_000_000_000),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(45_296_000_000),
        None,
        None,
        None,
        None,
    ];
    for (allow, policy) in modes() {
        assert_eq!(
            values(&actual(input.clone(), allow, policy, false).unwrap()),
            expected
        );
        assert_eq!(
            values(&actual(input.slice(1, 4), allow, policy, false).unwrap()),
            expected[1..5]
        );
        assert_eq!(
            values(&actual(input.slice(0, 0), allow, policy, false).unwrap()),
            vec![]
        );
    }
}
fn arrow_profile(dtype: DataType) {
    let input = corpus(&dtype);
    let expected = vec![
        Some(0),
        None,
        None,
        None,
        Some(3_723_456_789),
        Some(1),
        Some(1),
        Some(-1),
        Some(43_800_000_000),
        Some(86_400_000_000),
        None,
        None,
        None,
        None,
        None,
        None,
    ];
    for (allow, policy) in modes() {
        for datetime in [false, true] {
            assert_eq!(
                values(&actual(input.clone(), allow, policy, datetime).unwrap()),
                expected
            );
            assert_eq!(
                values(&actual(input.slice(4, 6), allow, policy, datetime).unwrap()),
                expected[4..10]
            );
            assert_eq!(
                values(&actual(input.slice(0, 0), allow, policy, datetime).unwrap()),
                vec![]
            );
        }
    }
}
#[test]
fn text_time_original_large_utf8_uses_original_arrow_parser_and_microsecond_identity() {
    arrow_profile(DataType::LargeUtf8);
}
#[test]
fn text_time_original_utf8_view_uses_original_arrow_parser_and_microsecond_identity() {
    arrow_profile(DataType::Utf8View);
}
#[test]
fn text_time_original_utf8_datetime_semantic_parameter_is_not_duration_parser() {
    let input = input(
        &DataType::Utf8,
        &[
            Some("1970-01-01 01:01:01"),
            Some("25:00:00"),
            Some("2020-01-02"),
            Some(" 1970-01-01 23:59:59 "),
            None,
        ],
    );
    for (allow, policy) in modes() {
        assert_eq!(
            values(&actual(input.clone(), allow, policy, true).unwrap()),
            vec![Some(3_661_000_000), None, None, Some(86_399_000_000), None]
        );
    }
}
#[test]
fn text_time_original_long_invalid_embedded_nul_and_hidden_null_payload_are_successful_nulls() {
    use arrow_buffer::{BooleanBuffer, NullBuffer};
    let invalid = "invalid".repeat(100);
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let source = input(
            &dtype,
            &[
                Some(&invalid),
                Some("01:02:03\0"),
                Some("25:00:00"),
                Some("12:34:56"),
            ],
        );
        let mut b = source.to_data().into_builder();
        b = b.nulls(Some(NullBuffer::new(BooleanBuffer::from(vec![
            true, true, false, true,
        ]))));
        let source = arrow::array::make_array(b.build().unwrap());
        for (allow, policy) in modes() {
            assert_eq!(
                values(&actual(source.clone(), allow, policy, false).unwrap()),
                vec![None, None, None, Some(45_296_000_000)]
            );
        }
    }
}
#[test]
fn text_time_original_wrong_target_full_data_text_is_before_child_evaluation() {
    let input: ArrayRef = Arc::new(StringArray::from(vec!["01:02:03"]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "input",
            DataType::Utf8,
            false,
        )])),
        vec![input],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(91)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, cs);
    for (allow, policy) in modes() {
        let mut arena = ExprArena::default();
        arena.set_allow_throw_exception(allow);
        let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(92)), DataType::Utf8);
        let root = arena.push_typed(
            ExprNode::CastTime(missing, policy),
            DataType::Time64(TimeUnit::Nanosecond),
        );
        assert_eq!(
            arena.eval(root, &chunk).unwrap_err(),
            "CAST failed: TIME target must be Time64(Microsecond), got Time64(Nanosecond)"
        );
    }
}

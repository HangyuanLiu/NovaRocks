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
//! Independent raw TIME baselines, recorded before core extraction.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(columns: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn raw(name: &str, columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(columns);
    eval_date_function(name, &arena, ExprId(usize::MAX), &args, &chunk)
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn strings(a: &ArrayRef) -> Vec<Option<String>> {
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
#[test]
fn original_time_to_sec_duration_grammar() {
    assert_eq!(
        ints(
            &raw(
                "time_to_sec",
                vec![text(vec![
                    Some("  +27:02:03  "),
                    Some("23:59:59"),
                    Some("-00:00:01"),
                    Some("01:02:03.1"),
                    Some("1:60:0"),
                    Some("1:2:60"),
                    None
                ])]
            )
            .unwrap()
        ),
        vec![Some(97323), Some(86399), None, None, None, None, None]
    );
}
#[test]
fn original_time_to_sec_datetime_carriers() {
    assert_eq!(
        ints(
            &raw(
                "time_to_sec",
                vec![Arc::new(Date32Array::from(vec![Some(0), None]))]
            )
            .unwrap()
        ),
        vec![Some(0), None]
    );
    assert_eq!(
        ints(
            &raw(
                "time_to_sec",
                vec![Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(45_296_000_000),
                    None
                ]))]
            )
            .unwrap()
        ),
        vec![Some(45296), None]
    );
}
#[test]
fn original_time_to_sec_roundtrip_bypasses_all_cast_kinds() {
    for variant in 0..3 {
        let (mut arena, args, chunk) = setup(vec![Arc::new(Int64Array::from(vec![
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ]))]);
        let sec = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Date("sec_to_time"),
                args: vec![args[0]],
            },
            DataType::Utf8,
        );
        let cast = match variant {
            0 => ExprNode::Cast(sec, DecimalOverflowPolicy::OutputNull),
            1 => ExprNode::CastTime(sec, DecimalOverflowPolicy::OutputNull),
            _ => ExprNode::CastTimeFromDatetime(sec, DecimalOverflowPolicy::OutputNull),
        };
        // The transformed child is deliberately inadmissible if it were evaluated.
        let cast = arena.push_typed(cast, DataType::Int64);
        let out = eval_date_function(
            "time_to_sec",
            &arena,
            ExprId(usize::MAX),
            &[cast, ExprId(usize::MAX)],
            &chunk,
        )
        .unwrap();
        assert_eq!(
            ints(&out),
            vec![Some(-1), Some(-3_023_999), Some(3_023_999), None]
        );
    }
}
#[test]
fn original_time_to_sec_source_type_error_precedes_wrappers() {
    let (mut arena, args, chunk) = setup(vec![text(vec![Some("1")])]);
    let sec = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("sec_to_time"),
            args: vec![args[0]],
        },
        DataType::Utf8,
    );
    let cast = arena.push_typed(
        ExprNode::Cast(sec, DecimalOverflowPolicy::OutputNull),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_eq!(
        eval_date_function("time_to_sec", &arena, ExprId(usize::MAX), &[cast], &chunk).unwrap_err(),
        "sec_to_time source for time_to_sec must be int"
    );
}
#[test]
fn original_time_to_sec_wrong_carrier_and_empty_type_admission() {
    for rows in [0, 1] {
        assert_eq!(
            raw(
                "time_to_sec",
                vec![Arc::new(Int64Array::from(vec![None; rows]))]
            )
            .unwrap_err(),
            "unsupported datetime input type: Int64"
        );
    }
}
#[test]
fn original_time_to_sec_arity_panic_and_ignored_tail() {
    let (arena, args, chunk) = setup(vec![text(vec![Some("01:02:03")])]);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_date_function(
            "time_to_sec",
            &arena,
            ExprId(0),
            &[],
            &chunk
        )))
        .is_err()
    );
    assert_eq!(
        ints(
            &eval_date_function(
                "time_to_sec",
                &arena,
                ExprId(0),
                &[args[0], ExprId(usize::MAX)],
                &chunk
            )
            .unwrap()
        ),
        vec![Some(3723)]
    );
}
#[test]
fn original_time_format_literal_tokens_and_unicode_nul() {
    let out = raw(
        "time_format",
        vec![
            text(vec![Some("12:34:56")]),
            text(vec![Some("%H:%i:%s/%S/%h/%f/%%/%q/é中\0%")]),
        ],
    )
    .unwrap();
    assert_eq!(
        strings(&out),
        vec![Some("00:00:00/00/12/045296/%/%q/é中\0%".into())]
    );
}
#[test]
fn original_time_format_fractional_invalid_and_null() {
    assert_eq!(
        strings(
            &raw(
                "time_format",
                vec![
                    text(vec![
                        Some("01:02:03.5"),
                        Some("-00:00:01"),
                        Some("24:00:00"),
                        None
                    ]),
                    text(vec![Some("%f"), Some("%f"), Some("%f"), Some("%f")])
                ]
            )
            .unwrap()
        ),
        vec![Some("003723".into()), None, None, None]
    );
}
#[test]
fn original_time_format_date_timestamp_and_null_format() {
    assert_eq!(
        strings(
            &raw(
                "time_format",
                vec![
                    Arc::new(Date32Array::from(vec![Some(0), None])),
                    text(vec![Some("%f"), Some("%f")])
                ]
            )
            .unwrap()
        ),
        vec![Some("000000".into()), None]
    );
    assert_eq!(
        strings(
            &raw(
                "time_format",
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        Some(45_296_000_000),
                        Some(0)
                    ])),
                    text(vec![Some("%f"), None])
                ]
            )
            .unwrap()
        ),
        vec![Some("045296".into()), None]
    );
}
#[test]
fn original_time_format_cast_source_differs_from_identical_value() {
    let (mut arena, args, chunk) = setup(vec![
        text(vec![Some("1970-01-01 12:34:56")]),
        text(vec![Some("%f")]),
    ]);
    let cast = arena.push_typed(
        ExprNode::Cast(args[0], DecimalOverflowPolicy::OutputNull),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_eq!(
        strings(
            &eval_date_function("time_format", &arena, ExprId(0), &[cast, args[1]], &chunk)
                .unwrap()
        ),
        vec![None]
    );
    assert_eq!(
        strings(
            &raw(
                "time_format",
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![45_296_000_000])),
                    text(vec![Some("%f")])
                ]
            )
            .unwrap()
        ),
        vec![Some("045296".into())]
    );
}
#[test]
fn original_time_format_raw_override_error_precedes_format_admission() {
    let (mut arena, args, chunk) = setup(vec![Arc::new(Int64Array::from(vec![1]))]);
    let invalid = arena.push_typed(ExprNode::Clone(ExprId(usize::MAX)), DataType::Utf8);
    let cast = arena.push_typed(
        ExprNode::Cast(invalid, DecimalOverflowPolicy::OutputNull),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_eq!(
        eval_date_function("time_format", &arena, ExprId(0), &[cast, args[0]], &chunk).unwrap_err(),
        "invalid ExprId"
    );
    assert_eq!(
        raw(
            "time_format",
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1]))
            ]
        )
        .unwrap_err(),
        "time_format expects string format"
    );
    assert_eq!(
        raw(
            "time_format",
            vec![Arc::new(Int64Array::from(vec![1])), text(vec![Some("%f")])]
        )
        .unwrap_err(),
        "time_format expects time"
    );
}
#[test]
fn original_time_format_arity_panic_and_ignored_tail() {
    let (arena, args, chunk) = setup(vec![text(vec![Some("01:02:03")]), text(vec![Some("%f")])]);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_date_function(
            "time_format",
            &arena,
            ExprId(0),
            &args[..1],
            &chunk
        )))
        .is_err()
    );
    assert_eq!(
        strings(
            &eval_date_function(
                "time_format",
                &arena,
                ExprId(0),
                &[args[0], args[1], ExprId(usize::MAX)],
                &chunk
            )
            .unwrap()
        ),
        vec![Some("003723".into())]
    );
}

#[test]
fn original_time_to_sec_if_invocation_domain_and_standalone_batch() {
    let (mut arena, args, chunk) = setup(vec![
        Arc::new(Date32Array::from(vec![Some(0), None])),
        Arc::new(BooleanArray::from(vec![true, false])),
        Arc::new(Int64Array::from(vec![-1, -1])),
    ]);
    let cast = arena.push_typed(
        ExprNode::Cast(args[0], DecimalOverflowPolicy::OutputNull),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("time_to_sec"),
            args: vec![cast],
        },
        DataType::Int64,
    );
    assert_eq!(
        ints(&arena.eval(call, &chunk).unwrap()),
        vec![Some(0), None]
    );
    let guarded = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::If,
            args: vec![args[1], call, args[2]],
        },
        DataType::Int64,
    );
    assert_eq!(
        ints(&arena.eval(guarded, &chunk).unwrap()),
        vec![Some(0), Some(-1)]
    );
}

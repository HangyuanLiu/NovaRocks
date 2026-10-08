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

//! Immutable pre-extraction v1 regexp_count behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
//! The actual two-argument Utf8 profile and raw typed-NULL projection are frozen.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{Array, ArrayRef, Int64Array, LargeStringArray, NullArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;

fn column(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{}", i + 1), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn eval(name: &str, columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                c.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    eval_string_function(name, &arena, args[0], &args, &chunk(columns))
}
fn assert_rows(output: ArrayRef, expected: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &DataType::Int64);
    assert_eq!(output.to_data(), Int64Array::from(expected).to_data());
}

#[test]
fn legacy_regexp_count_unicode_empty_pattern_nonoverlap_special_case_and_masks() {
    assert_rows(
        eval(
            "regexp_count",
            vec![
                column(vec![
                    Some("aaa"),
                    Some("ababa"),
                    Some("éé"),
                    Some(""),
                    None,
                    Some("aaa"),
                    Some("a\nb"),
                    Some("a\0b"),
                ]),
                column(vec![
                    Some("aa"),
                    Some("aba"),
                    Some("."),
                    Some(""),
                    Some("("),
                    Some("a{,}"),
                    Some("(?m)^"),
                    Some("\0"),
                ]),
            ],
        )
        .unwrap(),
        vec![
            Some(1),
            Some(1),
            Some(2),
            Some(1),
            None,
            Some(0),
            Some(2),
            Some(1),
        ],
    );
}
#[test]
fn legacy_regexp_count_literal_invalid_is_lazy_null_masked_and_empty_masked() {
    let pattern = "(bad";
    let expected = format!(
        "Invalid regex expression: {pattern}. Detail message: {}",
        regex::Regex::new(pattern).unwrap_err()
    );
    for values in [vec![None, None], vec![None, Some("a")], vec![]] {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(pattern.into())),
            DataType::Utf8,
        );
        let input = chunk(vec![column(values.clone())]);
        let result =
            eval_string_function("regexp_count", &arena, source, &[source, literal], &input);
        if values.iter().any(Option::is_some) {
            assert_eq!(result.unwrap_err(), expected);
        } else {
            assert_rows(result.unwrap(), vec![None; values.len()]);
        }
    }
}
#[test]
fn legacy_regexp_count_raw_null_carriers_and_static_downcasts_keep_full_message() {
    let null: ArrayRef = Arc::new(NullArray::new(2));
    let strings = column(vec![Some("a"), None]);
    for arrays in [
        vec![null.clone(), strings.clone()],
        vec![strings.clone(), null.clone()],
        vec![null.clone(), null.clone()],
    ] {
        assert_rows(eval("regexp_count", arrays).unwrap(), vec![None, None]);
    }
    let large: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("a"), None]));
    for arrays in [vec![large.clone(), null.clone()], vec![null, large]] {
        assert_eq!(
            eval("regexp_count", arrays).unwrap_err(),
            "regexp_count expects string"
        );
    }
}
#[test]
fn legacy_regexp_count_slices_many_patterns_and_dynamic_invalid_remain_successful_null() {
    let mut sources = (0..70).map(|i| Some(format!("p{i}"))).collect::<Vec<_>>();
    let mut patterns = (0..70).map(|i| Some(format!("^p{i}$"))).collect::<Vec<_>>();
    sources.extend([Some("x".into()), Some("x".into()), None]);
    patterns.extend([
        Some("(bad".into()),
        Some("(bad".into()),
        Some("(bad".into()),
    ]);
    let source = Arc::new(StringArray::from(sources)) as ArrayRef;
    let pattern = Arc::new(StringArray::from(patterns)) as ArrayRef;
    let mut expected = vec![Some(1); 70];
    expected.extend([None, None, None]);
    assert_rows(
        eval("regexp_count", vec![source.clone(), pattern.clone()]).unwrap(),
        expected.clone(),
    );
    assert_rows(
        eval(
            "regexp_count",
            vec![source.slice(3, 69), pattern.slice(3, 69)],
        )
        .unwrap(),
        expected[3..72].to_vec(),
    );
}
#[test]
fn legacy_regexp_count_raw_extra_argument_is_ignored_and_missing_argument_panics() {
    assert_rows(
        eval(
            "regexp_count",
            vec![
                column(vec![Some("aaa")]),
                column(vec![Some("a")]),
                Arc::new(NullArray::new(1)),
            ],
        )
        .unwrap(),
        vec![Some(3)],
    );
    let result = std::panic::catch_unwind(|| eval("regexp_count", vec![column(vec![Some("a")])]));
    let payload = result.unwrap_err();
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap();
    assert!(message.starts_with("index out of bounds:"), "{message}");
}

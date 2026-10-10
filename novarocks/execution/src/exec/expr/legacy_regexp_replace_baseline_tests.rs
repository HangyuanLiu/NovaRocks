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

//! Immutable pre-extraction v1 regexp_replace behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
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
fn eval(columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
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
    eval_string_function("regexp_replace", &arena, args[0], &args, &chunk(columns))
}
fn assert_rows(output: ArrayRef, expected: Vec<Option<&str>>) {
    assert_eq!(output.data_type(), &DataType::Utf8);
    assert_eq!(output.to_data(), StringArray::from(expected).to_data());
}
#[test]
fn legacy_regexp_replace_baseline_per_row_pattern_and_replacement() {
    assert_rows(
        eval(vec![
            column(vec![Some("xxxx"), Some("xxxx"), Some("xax"), Some("a12b3")]),
            column(vec![Some("x"), Some("xx"), Some("x"), Some("[0-9]+")]),
            column(vec![Some("-"), Some("+"), Some("*"), Some("#")]),
        ])
        .unwrap(),
        vec![Some("----"), Some("++"), Some("*a*"), Some("a#b#")],
    );
}
#[test]
fn legacy_regexp_replace_baseline_null_arguments_mask_invalid_pattern() {
    assert_rows(
        eval(vec![
            column(vec![None, Some("ab"), Some("ab"), Some("ab")]),
            column(vec![Some("(bad"), None, Some("(bad"), Some("a")]),
            column(vec![Some("-"), Some("-"), None, Some("-")]),
        ])
        .unwrap(),
        vec![None, None, None, Some("-b")],
    );
}
#[test]
fn legacy_regexp_replace_baseline_unicode_empty_pattern_and_no_match() {
    assert_rows(
        eval(vec![
            column(vec![Some("中a"), Some(""), Some("abc"), Some("中1中2")]),
            column(vec![Some(""), Some(""), Some("z+"), Some("中")]),
            column(vec![Some("-"), Some("-"), Some("X"), Some("文")]),
        ])
        .unwrap(),
        vec![Some("-中-a-"), Some("-"), Some("abc"), Some("文1文2")],
    );
}
#[test]
fn legacy_regexp_replace_baseline_capture_and_dollar_replacement_grammar() {
    assert_rows(
        eval(vec![
            column(vec![Some("abc123 xyz45"), Some("ab"), Some("ab")]),
            column(vec![
                Some("(?P<word>[a-z]+)([0-9]+)"),
                Some("(a)(b)"),
                Some("(a)(b)"),
            ]),
            column(vec![
                Some("${word}:$2:$$"),
                Some("$2$1"),
                Some("${missing}!"),
            ]),
        ])
        .unwrap(),
        vec![Some("abc:123:$ xyz:45:$"), Some("ba"), Some("!")],
    );
}
#[test]
fn legacy_regexp_replace_baseline_invalid_pattern_full_error_above_512_bytes() {
    let pattern = format!("{}(", "a".repeat(900));
    let expected = regex::Regex::new(&pattern).unwrap_err().to_string();
    assert!(expected.len() > 512);
    let actual = eval(vec![
        column(vec![Some("a")]),
        column(vec![Some(&pattern)]),
        column(vec![Some("-")]),
    ])
    .unwrap_err();
    assert_eq!(actual.as_bytes(), expected.as_bytes());
    println!(
        "regexp_replace legacy raw regex error bytes={}",
        actual.len()
    );
}
#[test]
fn legacy_regexp_replace_baseline_first_needed_error_order() {
    let first = "(first";
    let second = "[second";
    let actual = eval(vec![
        column(vec![None, Some("a"), Some("b")]),
        column(vec![Some("[masked"), Some(first), Some(second)]),
        column(vec![Some("-"), Some("-"), Some("-")]),
    ])
    .unwrap_err();
    assert_eq!(actual, regex::Regex::new(first).unwrap_err().to_string());
}
#[test]
fn legacy_regexp_replace_baseline_literal_and_column_patterns_same_semantics() {
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let replacement = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("#".into())),
        DataType::Utf8,
    );
    let source = chunk(vec![column(vec![Some("a1"), Some("b22"), None])]);
    for pattern in ["[0-9]+", "(bad"] {
        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(pattern.into())),
            DataType::Utf8,
        );
        let actual = eval_string_function(
            "regexp_replace",
            &arena,
            input,
            &[input, literal, replacement],
            &source,
        );
        let dynamic = eval(vec![
            column(vec![Some("a1"), Some("b22"), None]),
            column(vec![Some(pattern); 3]),
            column(vec![Some("#"); 3]),
        ]);
        match (actual, dynamic) {
            (Ok(a), Ok(b)) => assert_eq!(a.to_data(), b.to_data()),
            (Err(a), Err(b)) => assert_eq!(a.as_bytes(), b.as_bytes()),
            other => panic!("literal/column disagreement: {other:?}"),
        }
    }
}
#[test]
fn legacy_regexp_replace_baseline_many_distinct_patterns_and_sliced_carriers() {
    let patterns = (0..100).map(|i| format!("k{i}=")).collect::<Vec<_>>();
    let sources = (0..300)
        .map(|i| format!("k{}=v{i};k{}=w{i}", i % 100, i % 100))
        .collect::<Vec<_>>();
    let expected = (0..300).map(|i| format!("<v{i};<w{i}")).collect::<Vec<_>>();
    let columns = vec![
        Arc::new(StringArray::from(
            sources.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            (0..300)
                .map(|i| Some(patterns[i % 100].as_str()))
                .collect::<Vec<_>>(),
        )),
        column(vec![Some("<"); 300]),
    ];
    let actual = eval(columns.clone()).unwrap();
    assert_rows(actual, expected.iter().map(|s| Some(s.as_str())).collect());
    assert_rows(
        eval(columns.iter().map(|c| c.slice(91, 107)).collect()).unwrap(),
        expected[91..198].iter().map(|s| Some(s.as_str())).collect(),
    );
}
#[test]
fn legacy_regexp_replace_baseline_empty_batch_does_not_compile_pattern() {
    assert_rows(
        eval(vec![column(vec![]), column(vec![]), column(vec![])]).unwrap(),
        vec![],
    );
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let pattern = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("(bad".into())),
        DataType::Utf8,
    );
    let replacement = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("-".into())),
        DataType::Utf8,
    );
    assert_rows(
        eval_string_function(
            "regexp_replace",
            &arena,
            input,
            &[input, pattern, replacement],
            &chunk(vec![column(vec![])]),
        )
        .unwrap(),
        vec![],
    );
}
#[test]
fn legacy_regexp_replace_baseline_non_utf8_type_error_before_row_mask() {
    for argument in 0..3 {
        let mut columns = vec![column(vec![None]), column(vec![None]), column(vec![None])];
        columns[argument] = Arc::new(Int64Array::from(vec![None]));
        assert_eq!(eval(columns).unwrap_err(), "regexp_replace expects string");
    }
}
#[test]
fn legacy_regexp_replace_baseline_eager_arguments_even_when_source_is_null() {
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
    let replacement = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("-".into())),
        DataType::Utf8,
    );
    let source = chunk(vec![column(vec![None])]);
    let expected = arena.eval(missing, &source).unwrap_err();
    assert_eq!(
        eval_string_function(
            "regexp_replace",
            &arena,
            input,
            &[input, missing, replacement],
            &source
        )
        .unwrap_err(),
        expected
    );
}

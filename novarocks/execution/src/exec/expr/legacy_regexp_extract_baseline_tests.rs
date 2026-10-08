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

//! Immutable pre-extraction v1 regexp_extract family behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
//! SQL selects Int32 indices; raw legacy Int64 indices remain explicit oracles.
//! regexp_extract_all preserves its existing Utf8 JSON-text output.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
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
fn assert_rows(output: ArrayRef, expected: Vec<Option<&str>>) {
    assert_eq!(output.data_type(), &DataType::Utf8);
    assert_eq!(output.to_data(), StringArray::from(expected).to_data());
}
fn indices(values: Vec<Option<i64>>, wide: bool) -> ArrayRef {
    if wide {
        Arc::new(Int64Array::from(values))
    } else {
        Arc::new(Int32Array::from(
            values
                .into_iter()
                .map(|value| value.map(|i| i as i32))
                .collect::<Vec<_>>(),
        ))
    }
}
#[test]
fn legacy_regexp_extract_baseline_group_zero_missing_negative_and_null() {
    for wide in [false, true] {
        let columns = vec![
            column(vec![
                Some("a1b2"),
                Some("a1b2"),
                Some("a1b2"),
                Some("a1b2"),
                Some("a1b2"),
                None,
            ]),
            column(vec![Some("([0-9])"); 6]),
            indices(
                vec![Some(0), Some(1), Some(2), Some(-1), None, Some(1)],
                wide,
            ),
        ];
        assert_rows(
            eval("regexp_extract", columns.clone()).unwrap(),
            vec![Some("1"), Some("1"), Some(""), Some(""), None, None],
        );
        assert_rows(
            eval("regexp_extract_all", columns).unwrap(),
            vec![
                Some("[\"1\",\"2\"]"),
                Some("[\"1\",\"2\"]"),
                Some("[]"),
                Some("[]"),
                None,
                None,
            ],
        );
    }
}
#[test]
fn legacy_regexp_extract_baseline_negative_index_regex_error_order_differs() {
    for wide in [false, true] {
        let columns = vec![
            column(vec![Some("a")]),
            column(vec![Some("(bad")]),
            indices(vec![Some(-1)], wide),
        ];
        assert_eq!(
            eval("regexp_extract", columns.clone()).unwrap_err(),
            regex::Regex::new("(bad").unwrap_err().to_string()
        );
        assert_rows(
            eval("regexp_extract_all", columns).unwrap(),
            vec![Some("[]")],
        );
    }
}
#[test]
fn legacy_regexp_extract_baseline_null_masks_invalid_regex() {
    for name in ["regexp_extract", "regexp_extract_all"] {
        assert_rows(
            eval(
                name,
                vec![
                    column(vec![None, Some("a"), Some("a")]),
                    column(vec![Some("(bad"), None, Some("(bad")]),
                    indices(vec![Some(0), Some(0), None], false),
                ],
            )
            .unwrap(),
            vec![None, None, None],
        );
    }
}
#[test]
fn legacy_regexp_extract_baseline_optional_capture_unicode_and_json_escaping() {
    let columns = vec![
        column(vec![
            Some("b ab"),
            Some("\n中\""),
            Some("中a"),
            Some("nomatch"),
        ]),
        column(vec![Some("(a)?b"), Some("(?s)."), Some(""), Some("z+")]),
        indices(vec![Some(1), Some(0), Some(0), Some(0)], false),
    ];
    assert_rows(
        eval("regexp_extract", columns.clone()).unwrap(),
        vec![Some(""), Some("\n"), Some(""), Some("")],
    );
    assert_rows(
        eval("regexp_extract_all", columns).unwrap(),
        vec![
            Some("[\"a\"]"),
            Some("[\"\\n\",\"中\",\"\\\"\"]"),
            Some("[\"\",\"\",\"\"]"),
            Some("[]"),
        ],
    );
}
#[test]
fn legacy_regexp_extract_baseline_full_regex_error_above_512_and_first_needed_row() {
    let pattern = format!("{}(", "a".repeat(900));
    let expected = regex::Regex::new(&pattern).unwrap_err().to_string();
    assert!(expected.len() > 512);
    for name in ["regexp_extract", "regexp_extract_all"] {
        let actual = eval(
            name,
            vec![
                column(vec![None, Some("a"), Some("b")]),
                column(vec![
                    Some("[masked"),
                    Some(pattern.as_str()),
                    Some("(later"),
                ]),
                indices(vec![Some(0); 3], false),
            ],
        )
        .unwrap_err();
        assert_eq!(actual.as_bytes(), expected.as_bytes());
    }
}
#[test]
fn legacy_regexp_extract_baseline_many_patterns_recur_and_slices() {
    let sources = (0..300)
        .map(|i| format!("k{}=v{i};k{}=v{i}", i % 100, i % 100))
        .collect::<Vec<_>>();
    let patterns = (0..100)
        .map(|i| format!("k{i}=(v[0-9]+)"))
        .collect::<Vec<_>>();
    let columns = vec![
        column(sources.iter().map(|s| Some(s.as_str())).collect()),
        column((0..300).map(|i| Some(patterns[i % 100].as_str())).collect()),
        indices(vec![Some(1); 300], false),
    ];
    for (name, all) in [("regexp_extract", false), ("regexp_extract_all", true)] {
        let expected = (0..300)
            .map(|i| {
                if all {
                    format!("[\"v{i}\",\"v{i}\"]")
                } else {
                    format!("v{i}")
                }
            })
            .collect::<Vec<_>>();
        assert_rows(
            eval(name, columns.clone()).unwrap(),
            expected.iter().map(|s| Some(s.as_str())).collect(),
        );
        assert_rows(
            eval(name, columns.iter().map(|c| c.slice(91, 107)).collect()).unwrap(),
            expected[91..198].iter().map(|s| Some(s.as_str())).collect(),
        );
    }
}
#[test]
fn legacy_regexp_extract_baseline_empty_batch_and_exact_type_errors() {
    for name in ["regexp_extract", "regexp_extract_all"] {
        assert_rows(
            eval(
                name,
                vec![column(vec![]), column(vec![]), indices(vec![], false)],
            )
            .unwrap(),
            vec![],
        );
        for argument in 0..3 {
            let mut columns = vec![
                column(vec![None]),
                column(vec![None]),
                indices(vec![None], false),
            ];
            columns[argument] = if argument == 2 {
                column(vec![None])
            } else {
                indices(vec![None], false)
            };
            let expected = format!(
                "{name} expects {}",
                if argument == 2 { "int" } else { "string" }
            );
            assert_eq!(eval(name, columns).unwrap_err(), expected);
        }
    }
}
#[test]
fn legacy_regexp_extract_baseline_literal_pattern_matches_column_pattern() {
    for name in ["regexp_extract", "regexp_extract_all"] {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let index = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let source = chunk(vec![
            column(vec![Some("a1b2"), None]),
            indices(vec![Some(1); 2], false),
        ]);
        for pattern in ["([0-9])", "(bad"] {
            let literal = arena.push_typed(
                ExprNode::Literal(LiteralValue::Utf8(pattern.into())),
                DataType::Utf8,
            );
            let actual =
                eval_string_function(name, &arena, input, &[input, literal, index], &source);
            let dynamic = eval(
                name,
                vec![
                    column(vec![Some("a1b2"), None]),
                    column(vec![Some(pattern); 2]),
                    indices(vec![Some(1); 2], false),
                ],
            );
            match (actual, dynamic) {
                (Ok(a), Ok(b)) => assert_eq!(a.to_data(), b.to_data()),
                (Err(a), Err(b)) => assert_eq!(a.as_bytes(), b.as_bytes()),
                other => panic!("literal/column disagreement: {other:?}"),
            }
        }
    }
}

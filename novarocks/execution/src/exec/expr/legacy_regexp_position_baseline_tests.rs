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

//! Immutable pre-extraction v1 regexp_position behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
//! All three SQL overloads and both original raw integer carriers are frozen.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, NullArray, StringArray,
};
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
fn assert_rows(output: ArrayRef, expected: Vec<Option<i32>>) {
    assert_eq!(output.data_type(), &DataType::Int32);
    assert_eq!(output.to_data(), Int32Array::from(expected).to_data());
}
fn indices(values: Vec<Option<i64>>, wide: bool) -> ArrayRef {
    if wide {
        Arc::new(Int64Array::from(values))
    } else {
        Arc::new(Int32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|i| i as i32))
                .collect::<Vec<_>>(),
        ))
    }
}

#[test]
fn legacy_regexp_position_baseline_all_three_arities_unicode_nul_and_missing() {
    let source = column(vec![
        Some("中a中a"),
        Some("a\0b"),
        Some("👩‍💻xy"),
        Some(""),
        Some("nomatch"),
        None,
    ]);
    let pattern = column(vec![
        Some("a"),
        Some("\0"),
        Some("x"),
        Some("z"),
        Some("z"),
        Some("(bad"),
    ]);
    assert_rows(
        eval("regexp_position", vec![source.clone(), pattern.clone()]).unwrap(),
        vec![Some(2), Some(2), Some(4), Some(-1), Some(-1), None],
    );
    for wide in [false, true] {
        assert_rows(
            eval(
                "regexp_position",
                vec![
                    source.clone(),
                    pattern.clone(),
                    indices(
                        vec![Some(3), Some(1), Some(1), Some(1), Some(1), Some(1)],
                        wide,
                    ),
                ],
            )
            .unwrap(),
            vec![Some(4), Some(2), Some(4), Some(-1), Some(-1), None],
        );
        assert_rows(
            eval(
                "regexp_position",
                vec![
                    source.clone(),
                    pattern.clone(),
                    indices(vec![Some(1); 6], wide),
                    indices(
                        vec![Some(2), Some(1), Some(1), Some(1), Some(1), Some(1)],
                        wide,
                    ),
                ],
            )
            .unwrap(),
            vec![Some(4), Some(2), Some(4), Some(-1), Some(-1), None],
        );
    }
}
#[test]
fn legacy_regexp_position_baseline_empty_pattern_end_position_and_nonpositive() {
    for wide in [false, true] {
        assert_rows(
            eval(
                "regexp_position",
                vec![
                    column(vec![
                        Some("中a"),
                        Some("中a"),
                        Some("中a"),
                        Some("中a"),
                        Some(""),
                        Some(""),
                        Some("a"),
                        Some("a"),
                    ]),
                    column(vec![Some(""); 8]),
                    indices(
                        vec![
                            Some(1),
                            Some(2),
                            Some(3),
                            Some(4),
                            Some(1),
                            Some(2),
                            Some(0),
                            Some(1),
                        ],
                        wide,
                    ),
                    indices(
                        vec![
                            Some(3),
                            Some(2),
                            Some(1),
                            Some(1),
                            Some(1),
                            Some(1),
                            Some(1),
                            Some(0),
                        ],
                        wide,
                    ),
                ],
            )
            .unwrap(),
            vec![
                Some(3),
                Some(3),
                Some(3),
                Some(-1),
                Some(1),
                Some(-1),
                Some(-1),
                Some(-1),
            ],
        );
    }
}
#[test]
fn legacy_regexp_position_baseline_position_masks_regex_errors_before_compile() {
    for wide in [false, true] {
        assert_rows(
            eval(
                "regexp_position",
                vec![
                    column(vec![Some("中a"); 5]),
                    column(vec![Some("(invalid"); 5]),
                    indices(vec![Some(-1), Some(0), Some(4), Some(1), Some(1)], wide),
                    indices(vec![Some(1), Some(1), Some(1), Some(0), Some(-1)], wide),
                ],
            )
            .unwrap(),
            vec![Some(-1); 5],
        );
        let expected = format!(
            "Invalid regex expression: (invalid. Detail message: {}",
            regex::Regex::new("(invalid").unwrap_err()
        );
        assert_eq!(
            eval(
                "regexp_position",
                vec![
                    column(vec![Some("中a")]),
                    column(vec![Some("(invalid")]),
                    indices(vec![Some(3)], wide)
                ]
            )
            .unwrap_err(),
            expected
        );
    }
}
#[test]
fn legacy_regexp_position_baseline_typed_and_untyped_null_masks_all_payloads() {
    assert_rows(
        eval(
            "regexp_position",
            vec![
                column(vec![None, Some("a"), Some("a"), Some("a")]),
                column(vec![Some("(bad"), None, Some("(bad"), Some("(bad")]),
                indices(vec![Some(1), Some(1), None, Some(1)], true),
                indices(vec![Some(1), Some(1), Some(1), None], true),
            ],
        )
        .unwrap(),
        vec![None; 4],
    );
    for count in 2..=4 {
        for argument in 0..count {
            let mut columns = vec![column(vec![Some("a")]), column(vec![Some("(bad")])];
            for _ in 2..count {
                columns.push(indices(vec![Some(1)], true));
            }
            columns[argument] = Arc::new(NullArray::new(1));
            assert_rows(eval("regexp_position", columns).unwrap(), vec![None]);
        }
    }
}
#[test]
fn legacy_regexp_position_baseline_full_error_first_needed_row_and_literal_agree() {
    let long = format!("{}(", "a".repeat(900));
    let expected = format!(
        "Invalid regex expression: {long}. Detail message: {}",
        regex::Regex::new(&long).unwrap_err()
    );
    assert!(expected.len() > 512);
    assert_eq!(
        eval(
            "regexp_position",
            vec![
                column(vec![None, Some("a"), Some("b")]),
                column(vec![Some("(masked"), Some(&long), Some("(later")])
            ]
        )
        .unwrap_err()
        .as_bytes(),
        expected.as_bytes()
    );
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let pattern = arena.push_typed(ExprNode::Literal(LiteralValue::Utf8(long)), DataType::Utf8);
    assert_eq!(
        eval_string_function(
            "regexp_position",
            &arena,
            input,
            &[input, pattern],
            &chunk(vec![column(vec![Some("a")])])
        )
        .unwrap_err(),
        expected
    );
}
#[test]
fn legacy_regexp_position_baseline_recurrent_patterns_slices_and_empty() {
    let source = (0..300)
        .map(|i| format!("中k{}=v{i}", i % 100))
        .collect::<Vec<_>>();
    let patterns = (0..100).map(|i| format!("k{i}=")).collect::<Vec<_>>();
    let columns = vec![
        column(source.iter().map(|s| Some(s.as_str())).collect()),
        column((0..300).map(|i| Some(patterns[i % 100].as_str())).collect()),
    ];
    for (offset, len) in [(0, 300), (91, 107), (0, 0)] {
        assert_rows(
            eval(
                "regexp_position",
                columns.iter().map(|c| c.slice(offset, len)).collect(),
            )
            .unwrap(),
            vec![Some(2); len],
        );
    }
}
#[test]
fn legacy_regexp_position_baseline_downcast_before_null_and_later_child_errors() {
    for count in 2..=4 {
        for argument in 0..count {
            let mut columns = vec![column(vec![None]), column(vec![None])];
            for _ in 2..count {
                columns.push(indices(vec![None], true));
            }
            columns[argument] = if argument < 2 {
                indices(vec![None], true)
            } else {
                column(vec![None])
            };
            assert_eq!(
                eval("regexp_position", columns).unwrap_err(),
                format!(
                    "regexp_position expects {}",
                    if argument < 2 { "string" } else { "int" }
                )
            );
        }
    }
    let large = Arc::new(LargeStringArray::from(vec![Some("a")])) as ArrayRef;
    assert_eq!(
        eval("regexp_position", vec![large, column(vec![Some("a")])]).unwrap_err(),
        "regexp_position expects string"
    );
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let pattern = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int64);
    assert_eq!(
        eval_string_function(
            "regexp_position",
            &arena,
            input,
            &[input, pattern, missing],
            &chunk(vec![indices(vec![Some(1)], true), column(vec![Some("a")])])
        )
        .unwrap_err(),
        "regexp_position expects string"
    );
}
#[test]
fn legacy_regexp_position_baseline_result_metadata_is_ignored() {
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let pattern = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
    let output = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
    assert_rows(
        eval_string_function(
            "regexp_position",
            &arena,
            output,
            &[input, pattern],
            &chunk(vec![column(vec![Some("中a")]), column(vec![Some("a")])]),
        )
        .unwrap(),
        vec![Some(2)],
    );
}

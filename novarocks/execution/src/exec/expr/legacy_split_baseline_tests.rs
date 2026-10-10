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
//! Independent original raw string-family baselines; no pure owner needed.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeStringArray, ListArray, NullArray, StringArray,
    StructArray, new_null_array,
};
use arrow::array::{Int32Builder, ListBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(columns: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
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
fn raw(name: &str, columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(columns);
    eval_string_function(name, &arena, ExprId(usize::MAX), &args, &chunk)
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn parts(output: &ArrayRef) -> Vec<Option<Vec<String>>> {
    let list = output.as_any().downcast_ref::<ListArray>().unwrap();
    (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                None
            } else {
                let child = list.value(row);
                Some(
                    child
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .map(|s| s.unwrap().to_owned())
                        .collect(),
                )
            }
        })
        .collect()
}
fn bools(output: &ArrayRef) -> Vec<Option<bool>> {
    output
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn original_split_unicode_empty_delimiter_and_nul() {
    let a = text(vec![Some("é中👩\u{200d}🔬a\0"), Some(""), None, Some("ab")]);
    let b = text(vec![Some(""), Some(""), Some(""), None]);
    let output = raw("split", vec![a, b]).unwrap();
    assert_eq!(
        output.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
    assert_eq!(
        parts(&output),
        vec![
            Some(
                vec!["é", "中", "👩", "\u{200d}", "🔬", "a", "\0"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            ),
            Some(vec![]),
            None,
            None
        ]
    );
}
#[test]
fn original_split_nonoverlap_empty_fields_and_multibyte_delimiter() {
    let output = raw(
        "split",
        vec![
            text(vec![
                Some("aaaaa"),
                Some(",a,,"),
                Some("中é中"),
                Some(""),
                Some("a\0b\0"),
            ]),
            text(vec![
                Some("aa"),
                Some(","),
                Some("中"),
                Some(","),
                Some("\0"),
            ]),
        ],
    )
    .unwrap();
    assert_eq!(
        parts(&output),
        vec![
            Some(vec!["", "", "a"]),
            Some(vec!["", "a", "", ""]),
            Some(vec!["", "é", ""]),
            Some(vec![""]),
            Some(vec!["a", "b", ""])
        ]
        .into_iter()
        .map(|v| v.map(|p| p.into_iter().map(str::to_owned).collect()))
        .collect::<Vec<_>>()
    );
}
#[test]
fn original_split_nulls_slices_and_empty_output_shape() {
    let a = text(vec![Some("bad"), Some("a:b"), None, Some(""), Some("bad")]);
    let b = text(vec![None, Some(":"), Some(":"), Some(":"), Some("")]);
    assert_eq!(
        parts(&raw("split", vec![a.slice(1, 3), b.slice(1, 3)]).unwrap()),
        vec![
            Some(vec!["a".into(), "b".into()]),
            None,
            Some(vec!["".into()])
        ]
    );
    let out = raw("split", vec![a.slice(0, 0), b.slice(0, 0)]).unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(
        out.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
}
#[test]
fn original_split_exact_type_errors_even_all_null_and_empty() {
    for rows in [0, 3] {
        assert_eq!(
            raw(
                "split",
                vec![
                    new_null_array(&DataType::Int64, rows),
                    new_null_array(&DataType::Utf8, rows)
                ]
            )
            .unwrap_err(),
            "split: first argument must be a string array"
        );
        assert_eq!(
            raw(
                "split",
                vec![
                    new_null_array(&DataType::Utf8, rows),
                    new_null_array(&DataType::Null, rows)
                ]
            )
            .unwrap_err(),
            "split: second argument must be a string array"
        );
    }
}
#[test]
fn original_split_child_evaluation_precedes_type_admission() {
    let (mut arena, args, chunk) = setup(vec![Arc::new(Int64Array::from(vec![Some(1)]))]);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(99)), DataType::Utf8);
    let expected = arena.eval(missing, &chunk).unwrap_err();
    assert_eq!(
        eval_string_function(
            "split",
            &arena,
            ExprId(usize::MAX),
            &[args[0], missing],
            &chunk
        )
        .unwrap_err(),
        expected
    );
}
#[test]
fn original_split_tail_ignored_and_zero_one_arity_panics() {
    let (mut arena, args, chunk) = setup(vec![text(vec![Some("a:b")]), text(vec![Some(":")])]);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(99)), DataType::Utf8);
    assert_eq!(
        parts(
            &eval_string_function(
                "split",
                &arena,
                ExprId(usize::MAX),
                &[args[0], args[1], missing],
                &chunk
            )
            .unwrap()
        ),
        vec![Some(vec!["a".into(), "b".into()])]
    );
    for short in [vec![], vec![args[0]]] {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_string_function(
                "split",
                &arena,
                ExprId(usize::MAX),
                &short,
                &chunk
            )))
            .is_err()
        );
    }
}

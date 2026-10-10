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
//! Independent original SPLIT_PART values, carrier and demand witnesses.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, NullArray, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(columns: &[ArrayRef]) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots: Vec<_> = (1..=columns.len()).map(|i| SlotId::new(i as u32)).collect();
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("original-{i}"), a.data_type().clone(), true))
        .collect();
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .zip(&slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(*s), a.data_type().clone()))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns.to_vec()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}
fn call(name: &str, arena: &ExprArena, args: &[ExprId], chunk: &Chunk) -> Result<ArrayRef, String> {
    super::function::string::eval_string_function(name, arena, ExprId(usize::MAX), args, chunk)
}
fn raw(name: &str, columns: &[ArrayRef]) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(columns);
    call(name, &arena, &args, &chunk)
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn ints(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn values(a: ArrayRef) -> Vec<Option<String>> {
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}

#[test]
fn original_split_part_directional_delimiters_overlap_and_unicode() {
    for (text, delim, index, expected) in [
        ("a,,b,", ",", 1, "a"),
        ("a,,b,", ",", 2, ""),
        ("a,,b,", ",", 3, "b"),
        ("a,,b,", ",", 4, ""),
        ("a,,b,", ",", 5, ""),
        ("a,,b,", ",", -1, ""),
        ("a,,b,", ",", -2, "b"),
        ("a,,b,", ",", -3, ""),
        ("a,,b,", ",", -4, "a"),
        ("a,,b,", ",", -5, ""),
        ("aaaaa", "aa", 1, ""),
        ("aaaaa", "aa", 2, ""),
        ("aaaaa", "aa", 3, "a"),
        ("aaaaa", "aa", -1, ""),
        ("aaaaa", "aa", -2, ""),
        ("aaaaa", "aa", -3, "a"),
        ("é中é中x", "é中", 3, "x"),
        ("é中é中x", "é中", -1, "x"),
        ("é中", "z", 1, "é中"),
        ("é中", "z", -1, "é中"),
        ("é中", "z", 2, ""),
        ("é中", "z", -2, ""),
    ] {
        assert_eq!(
            values(
                raw(
                    "split_part",
                    &[text_array(text), text_array(delim), ints(vec![Some(index)])]
                )
                .unwrap()
            ),
            vec![Some(expected.into())],
            "{text:?}/{delim:?}/{index}"
        );
    }
}
fn text_array(value: &str) -> ArrayRef {
    text(vec![Some(value)])
}
#[test]
fn original_split_part_empty_delimiter_negative_first_character_and_null_empty_result() {
    for (index, expected) in [
        (i64::MIN, "é"),
        (-1, "é"),
        (0, ""),
        (1, "é"),
        (2, "中"),
        (3, ""),
        (i64::MAX, ""),
    ] {
        assert_eq!(
            values(
                raw(
                    "split_part",
                    &[text_array("é中"), text_array(""), ints(vec![Some(index)])]
                )
                .unwrap()
            ),
            vec![Some(expected.into())]
        );
    }
    assert_eq!(
        values(
            raw(
                "split_part",
                &[
                    text(vec![None, Some("hidden"), Some("hidden")]),
                    text(vec![Some(","), None, Some(",")]),
                    ints(vec![Some(1), Some(1), None])
                ]
            )
            .unwrap()
        ),
        vec![Some("".into()); 3]
    );
}
#[test]
fn original_split_part_i32_and_i64_reader_slice_empty_and_minimum_panic() {
    let source = text(vec![Some("guard"), Some("aé中"), None, Some("guard")]).slice(1, 2);
    let delimiter = text(vec![Some("guard"), Some("é"), Some("é"), Some("guard")]).slice(1, 2);
    let ordinal: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(-1),
        Some(1),
        Some(99),
    ]));
    assert_eq!(
        values(raw("split_part", &[source, delimiter, ordinal.slice(1, 2)]).unwrap()),
        vec![Some("中".into()), Some("".into())]
    );
    assert!(
        values(raw("split_part", &[text(vec![]), text(vec![]), ints(vec![])]).unwrap()).is_empty()
    );
    let (arena, args, chunk) = setup(&[
        text_array("a,b"),
        text_array(","),
        ints(vec![Some(i64::MIN)]),
    ]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        call("split_part", &arena, &args, &chunk)
    }));
    if cfg!(debug_assertions) {
        assert!(result.is_err());
    } else {
        assert_eq!(values(result.unwrap().unwrap()), vec![Some("".into())]);
    }
}
#[test]
fn original_split_part_original_carrier_errors_before_empty_or_null() {
    for rows in [0, 1] {
        let wrong: ArrayRef = Arc::new(NullArray::new(rows));
        let s = text(vec![None; rows]);
        let n = ints(vec![None; rows]);
        assert_eq!(
            raw("split_part", &[wrong.clone(), s.clone(), n.clone()]).unwrap_err(),
            "split_part expects string"
        );
        assert_eq!(
            raw("split_part", &[s.clone(), wrong.clone(), n]).unwrap_err(),
            "split_part expects string"
        );
        assert_eq!(
            raw("split_part", &[s.clone(), s, wrong]).unwrap_err(),
            "split_part expects int"
        );
    }
}
#[test]
fn original_split_part_children_before_carriers_extra_ignored_missing_arity_panic() {
    let (arena, args, chunk) = setup(&[text_array("a,b"), text_array(","), ints(vec![Some(1)])]);
    let missing = ExprId(usize::MAX);
    let expected = values(call("split_part", &arena, &args, &chunk).unwrap());
    let mut extra = args.clone();
    extra.push(missing);
    assert_eq!(
        values(call("split_part", &arena, &extra, &chunk).unwrap()),
        expected
    );
    for position in 0..3 {
        let mut bad = args.clone();
        bad[position] = missing;
        assert_eq!(
            call("split_part", &arena, &bad, &chunk).unwrap_err(),
            "invalid ExprId"
        );
    }
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call(
            "split_part",
            &arena,
            &[],
            &chunk
        )))
        .is_err()
    );
    let (arena, args, chunk) = setup(&[
        Arc::new(NullArray::new(1)) as ArrayRef,
        text_array(","),
        ints(vec![Some(1)]),
    ]);
    assert_eq!(
        call("split_part", &arena, &[args[0], args[1], missing], &chunk).unwrap_err(),
        "invalid ExprId"
    );
}

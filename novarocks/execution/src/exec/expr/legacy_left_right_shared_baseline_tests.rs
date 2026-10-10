// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Independent original four-alias Unicode, carrier, and evaluation-order witnesses.
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
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
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
        .map(|s| s.map(str::to_owned))
        .collect()
}
const NAMES: [&str; 4] = ["left", "strleft", "right", "strright"];
#[test]
fn original_left_right_unicode_full_i64_and_null_values() {
    for name in NAMES {
        let result = values(
            raw(
                name,
                &[
                    strings(vec![
                        Some("aé中🙂"),
                        Some(""),
                        Some("e\u{301}x"),
                        Some("abc"),
                        Some("x"),
                        None,
                        Some("\0é"),
                    ]),
                    ints(vec![
                        Some(2),
                        Some(i64::MAX),
                        Some(2),
                        Some(i64::MIN),
                        Some(0),
                        Some(3),
                        None,
                    ]),
                ],
            )
            .unwrap(),
        );
        let expected = if name == "left" || name == "strleft" {
            vec![
                Some("aé".into()),
                Some("".into()),
                Some("e\u{301}".into()),
                Some("".into()),
                Some("".into()),
                None,
                None,
            ]
        } else {
            vec![
                Some("中🙂".into()),
                Some("".into()),
                Some("\u{301}x".into()),
                Some("".into()),
                Some("".into()),
                None,
                None,
            ]
        };
        assert_eq!(result, expected);
    }
}
#[test]
fn original_left_right_i32_reader_empty_and_slice() {
    for name in NAMES {
        let source = strings(vec![Some("guard"), Some("é中🙂"), None, Some("guard")]).slice(1, 2);
        let lengths: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(99), Some(2), Some(1), Some(99)]));
        let expected = if name == "left" || name == "strleft" {
            "é中"
        } else {
            "中🙂"
        };
        assert_eq!(
            values(raw(name, &[source, lengths.slice(1, 2)]).unwrap()),
            vec![Some(expected.into()), None]
        );
        assert!(values(raw(name, &[strings(vec![]), ints(vec![])]).unwrap()).is_empty());
    }
}
#[test]
fn original_left_right_carrier_errors_before_empty_and_null_masks() {
    for rows in [0, 1] {
        for name in NAMES {
            let wrong: ArrayRef = Arc::new(NullArray::new(rows));
            let s = strings(vec![None; rows]);
            let n = ints(vec![None; rows]);
            assert_eq!(
                raw(name, &[wrong.clone(), n]).unwrap_err(),
                "left expects string"
            );
            assert_eq!(raw(name, &[s, wrong]).unwrap_err(), "left expects int");
        }
    }
}
#[test]
fn original_left_right_required_children_before_types_extra_ignored_and_missing_arity_panic() {
    let (arena, args, chunk) = setup(&[strings(vec![Some("é中")]), ints(vec![Some(1)])]);
    let missing = ExprId(usize::MAX);
    for name in NAMES {
        let expected = values(call(name, &arena, &args, &chunk).unwrap());
        assert_eq!(
            values(call(name, &arena, &[args[0], args[1], missing], &chunk).unwrap()),
            expected
        );
        for position in 0..2 {
            let mut bad = args.clone();
            bad[position] = missing;
            assert_eq!(
                call(name, &arena, &bad, &chunk).unwrap_err(),
                "invalid ExprId"
            );
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call(
                name,
                &arena,
                &[],
                &chunk
            )))
            .is_err()
        );
    }
    let (arena, args, chunk) =
        setup(&[Arc::new(NullArray::new(1)) as ArrayRef, ints(vec![Some(1)])]);
    for name in NAMES {
        assert_eq!(
            call(name, &arena, &[args[0], missing], &chunk).unwrap_err(),
            "invalid ExprId"
        );
    }
}

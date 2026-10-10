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
//! Independent original raw reader, value, and demand witnesses. Expectations
//! remain unchanged after the sole row-program extraction.
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
fn original_repeat_pad_shared_repeat_space_value_and_i32_readers() {
    assert_eq!(
        values(
            raw(
                "repeat",
                &[
                    text(vec![Some("é中"), Some(""), Some("x"), Some("x"), None]),
                    ints(vec![
                        Some(2),
                        Some(i64::MAX),
                        Some(-1),
                        Some(1_048_577),
                        Some(2)
                    ])
                ]
            )
            .unwrap()
        ),
        vec![
            Some("é中é中".into()),
            Some("".into()),
            Some("".into()),
            None,
            None
        ]
    );
    assert_eq!(
        values(
            raw(
                "space",
                &[Arc::new(Int32Array::from(vec![
                    Some(-1),
                    Some(0),
                    Some(3),
                    None
                ]))]
            )
            .unwrap()
        ),
        vec![None, Some("".into()), Some("   ".into()), None]
    );
    assert_eq!(
        values(
            raw(
                "repeat",
                &[
                    text(vec![Some("e\u{301}"), Some("x")]),
                    Arc::new(Int32Array::from(vec![Some(2), None]))
                ]
            )
            .unwrap()
        ),
        vec![Some("e\u{301}e\u{301}".into()), None]
    );
}
#[test]
fn original_repeat_pad_shared_unicode_caps_empty_pad_and_raw_null() {
    let source = text(vec![
        Some("é中"),
        Some("ab"),
        Some("ab"),
        Some("ab"),
        Some("ab"),
        None,
    ]);
    let counts = ints(vec![
        Some(5),
        Some(0),
        Some(-1),
        Some(1_048_577),
        Some(9),
        Some(1),
    ]);
    let pads = text(vec![
        Some("🙂x"),
        Some("x"),
        Some("x"),
        Some("x"),
        Some(""),
        Some("x"),
    ]);
    assert_eq!(
        values(raw("lpad", &[source.clone(), counts.clone(), pads.clone()]).unwrap()),
        vec![
            Some("🙂x🙂é中".into()),
            Some("".into()),
            None,
            None,
            Some("ab".into()),
            None
        ]
    );
    assert_eq!(
        values(raw("rpad", &[source, counts, pads]).unwrap()),
        vec![
            Some("é中🙂x🙂".into()),
            Some("".into()),
            None,
            None,
            Some("ab".into()),
            None
        ]
    );
    let long = "🙂".repeat(1_048_576 / 4 + 1);
    for name in ["lpad", "rpad"] {
        assert_eq!(
            values(
                raw(
                    name,
                    &[
                        text(vec![Some(&long), Some("x")]),
                        ints(vec![Some(262_145), Some(524_289)]),
                        text(vec![Some(""), Some("é")])
                    ]
                )
                .unwrap()
            ),
            vec![None, None]
        );
    }
}
#[test]
fn original_repeat_pad_shared_original_carrier_errors_before_empty_or_null_rows() {
    for rows in [0, 1] {
        let wrong: ArrayRef = Arc::new(NullArray::new(rows));
        let s: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; rows]));
        let n: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; rows]));
        assert_eq!(
            raw("repeat", &[wrong.clone(), n.clone()]).unwrap_err(),
            "repeat expects string"
        );
        assert_eq!(
            raw("repeat", &[s.clone(), wrong.clone()]).unwrap_err(),
            "repeat expects int"
        );
        assert_eq!(
            raw("space", &[wrong.clone()]).unwrap_err(),
            "space expects int"
        );
        for name in ["lpad", "rpad"] {
            assert_eq!(
                raw(name, &[wrong.clone(), n.clone(), s.clone()]).unwrap_err(),
                "pad expects string"
            );
            assert_eq!(
                raw(name, &[s.clone(), wrong.clone(), wrong.clone()]).unwrap_err(),
                "pad expects int"
            );
            assert_eq!(
                raw(name, &[s.clone(), n.clone(), wrong.clone()]).unwrap_err(),
                "pad expects string"
            );
        }
    }
}
#[test]
fn original_repeat_pad_shared_children_eager_required_extra_ignored_and_arity_panic() {
    let (arena, args, chunk) = setup(&[
        text(vec![Some("a")]),
        ints(vec![Some(3)]),
        text(vec![Some("x")]),
    ]);
    let missing = ExprId(usize::MAX);
    for (name, arity) in [("repeat", 2), ("space", 1), ("lpad", 3), ("rpad", 3)] {
        let actual: Vec<_> = if name == "space" {
            vec![args[1]]
        } else {
            args[..arity].to_vec()
        };
        let expected = values(call(name, &arena, &actual, &chunk).unwrap());
        let mut extra = actual.clone();
        extra.push(missing);
        assert_eq!(
            values(call(name, &arena, &extra, &chunk).unwrap()),
            expected
        );
        for position in 0..arity {
            let mut bad = actual.clone();
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
    // All required children are evaluated before the first carrier check.
    assert_eq!(
        call("repeat", &arena, &[args[1], missing], &chunk).unwrap_err(),
        "invalid ExprId"
    );
    assert_eq!(
        call("lpad", &arena, &[args[1], args[1], missing], &chunk).unwrap_err(),
        "invalid ExprId"
    );
}

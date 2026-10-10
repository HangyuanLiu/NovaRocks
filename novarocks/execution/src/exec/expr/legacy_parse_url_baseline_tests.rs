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
//! Original PARSE_URL dispatch oracles, independent of the new owner.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::string::eval_string_function;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{Array, ArrayRef, Int64Array, LargeStringArray, NullArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn setup(inputs: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let fields: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("a{i}"), a.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs.clone()).unwrap();
    let slots: Vec<_> = (0..inputs.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = inputs
        .iter()
        .zip(slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(s), a.data_type().clone()))
        .collect();
    (arena, args, chunk)
}
fn raw(inputs: Vec<ArrayRef>, target: DataType) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(inputs);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: crate::exec::expr::function::FunctionKind::String("parse_url"),
            args: args.clone(),
        },
        target,
    );
    eval_string_function("parse_url", &arena, expr, &args, &chunk)
}
fn values(a: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(a.data_type(), &DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}
#[test]
fn legacy_parse_url_original_parts_case_raw_query_empty_and_ignored_target() {
    let url = "https://Example.COM:8443/a%20b?q=a+b&q=second&Q=UPPER#Frag";
    let parts = [
        "host",
        "PATH",
        "Protocol",
        "ref",
        "query",
        "authority",
        "QUERY",
        "HOST",
    ];
    let expected = vec![
        Some("example.com".into()),
        Some("/a%20b".into()),
        Some("https".into()),
        Some("Frag".into()),
        Some("q=a+b&q=second&Q=UPPER".into()),
        None,
        None,
        None,
    ];
    let urls = strings(vec![
        Some(url),
        Some(url),
        Some(url),
        Some(url),
        Some(url),
        Some(url),
        Some("/relative"),
        None,
    ]);
    for target in [DataType::Utf8, DataType::Int64, DataType::Binary] {
        assert_eq!(
            values(
                raw(
                    vec![
                        urls.clone(),
                        strings(parts.iter().map(|s| Some(*s)).collect())
                    ],
                    target
                )
                .unwrap()
            ),
            expected
        );
    }
    assert!(
        values(raw(vec![strings(vec![]), strings(vec![])], DataType::Utf8).unwrap()).is_empty()
    );
    assert_eq!(
        values(
            raw(
                vec![
                    strings(vec![Some("https://x/?")]),
                    strings(vec![Some("QUERY")])
                ],
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some("".into())]
    );
}
#[test]
fn legacy_parse_url_original_query_key_decoding_first_match_and_key_null_only_masks_query() {
    let url = "https://example.com/?q=a+b&q=second&Q=UPPER&%E4%B8%AD=%FF&empty=#Frag";
    let parts = strings(vec![
        Some("QUERY"),
        Some("QUERY"),
        Some("QUERY"),
        Some("QUERY"),
        Some("QUERY"),
        Some("HOST"),
        Some("PATH"),
        Some("REF"),
    ]);
    let keys = strings(vec![
        Some("q"),
        Some("Q"),
        Some("中"),
        Some("empty"),
        None,
        None,
        None,
        None,
    ]);
    assert_eq!(
        values(
            raw(
                vec![strings(vec![Some(url); 8]), parts, keys],
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![
            Some("a b".into()),
            Some("UPPER".into()),
            Some("�".into()),
            Some("".into()),
            None,
            Some("example.com".into()),
            Some("/".into()),
            Some("Frag".into())
        ]
    );
}
#[test]
fn legacy_parse_url_original_null_slice_typed_errors_and_evaluation_order() {
    let a = strings(vec![
        Some("guard"),
        Some("https://x/a"),
        None,
        Some("not a URL"),
        Some("guard"),
    ])
    .slice(1, 3);
    let p = strings(vec![
        Some("guard"),
        Some("PATH"),
        Some("HOST"),
        None,
        Some("guard"),
    ])
    .slice(1, 3);
    assert_eq!(
        values(raw(vec![a, p], DataType::Utf8).unwrap()),
        vec![Some("/a".into()), None, None]
    );
    for bad in [
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
        Arc::new(NullArray::new(1)),
        Arc::new(LargeStringArray::from(vec![Some("x")])),
    ] {
        for pos in 0..3 {
            let mut a = vec![
                strings(vec![None]),
                strings(vec![Some("HOST")]),
                strings(vec![None]),
            ];
            a[pos] = bad.clone();
            assert_eq!(
                raw(a, DataType::Utf8).unwrap_err(),
                "parse_url expects string"
            );
        }
    }
    let (arena, mut args, chunk) = setup(vec![
        Arc::new(Int64Array::from(vec![None])),
        strings(vec![Some("HOST")]),
    ]);
    args[1] = ExprId(usize::MAX);
    assert_eq!(
        eval_string_function("parse_url", &arena, ExprId(0), &args, &chunk).unwrap_err(),
        "invalid ExprId"
    );
    // Both normal channels are evaluated before type admission; the key is evaluated only afterwards.
    let (arena, mut args, chunk) = setup(vec![
        Arc::new(Int64Array::from(vec![None])),
        strings(vec![Some("HOST")]),
        strings(vec![None]),
    ]);
    args[2] = ExprId(usize::MAX);
    assert_eq!(
        eval_string_function("parse_url", &arena, ExprId(0), &args, &chunk).unwrap_err(),
        "parse_url expects string"
    );
}
#[test]
fn legacy_parse_url_original_extra_arg_gate_missing_argument_panic_and_unicode_bytes() {
    let (arena, mut args, chunk) = setup(vec![
        strings(vec![Some("https://x/?q=a%2Bb")]),
        strings(vec![Some("query")]),
    ]);
    let expected = vec![Some("q=a%2Bb".into())];
    args.extend([ExprId(usize::MAX), ExprId(usize::MAX)]);
    assert_eq!(
        values(eval_string_function("parse_url", &arena, ExprId(0), &args, &chunk).unwrap()),
        expected
    );
    for arity in [0, 1] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval_string_function(
                "parse_url",
                &arena,
                ExprId(0),
                &args[..arity],
                &chunk
            )))
            .is_err()
        );
    }
    let long = "x".repeat(700);
    assert_eq!(
        values(
            raw(
                vec![
                    strings(vec![Some("https://x/a%00b"), Some("https://x/?q=a%2Bb")]),
                    strings(vec![Some("PATH"), Some("QUERY")]),
                    strings(vec![Some(&long), Some("q")])
                ],
                DataType::Utf8
            )
            .unwrap()
        ),
        vec![Some("/a%00b".into()), Some("a+b".into())]
    );
}

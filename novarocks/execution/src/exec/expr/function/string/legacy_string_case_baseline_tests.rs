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
//! Independent immutable raw-v1 string case oracles before sharing calculation.
use super::super::eval_upper;
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::{ExprNode, LiteralValue};
use arrow::array::{Int64Array, LargeStringArray, NullArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
fn chunk(input: ArrayRef) -> Chunk {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            input.data_type().clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn invoke(
    name: &str,
    arena: &ExprArena,
    output: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    match name {
        "upper" => eval_upper(arena, args[0], chunk),
        "lower" => eval_lower(arena, output, args, chunk),
        "lcase" => eval_lcase(arena, output, args, chunk),
        _ => unreachable!(),
    }
}
fn evaluate(name: &str, input: ArrayRef, target: DataType) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input.data_type().clone());
    let output = arena.push_typed(ExprNode::Literal(LiteralValue::Null), target);
    invoke(name, &arena, output, &[arg], &chunk(input))
}
fn assert_rows(output: ArrayRef, expected: Vec<Option<&str>>) {
    assert_eq!(output.data_type(), &DataType::Utf8);
    assert_eq!(output.to_data(), StringArray::from(expected).to_data());
}
#[test]
fn legacy_string_case_baseline_unicode_context_turkish_expansion_and_nul() {
    let source = vec![
        Some("Iİıi"),
        Some("Straße"),
        Some("ﬃ"),
        Some("ΟΣ"),
        Some("ΟΣΑ"),
        Some("Σ"),
        Some("ΟΣ\u{301}"),
        Some("AbC\0é"),
        Some("你好👩‍💻"),
        Some(""),
        None,
    ];
    for name in ["lower", "lcase"] {
        assert_rows(
            evaluate(
                name,
                Arc::new(StringArray::from(source.clone())),
                DataType::Utf8,
            )
            .unwrap(),
            vec![
                Some("ii\u{307}ıi"),
                Some("straße"),
                Some("ﬃ"),
                Some("ος"),
                Some("οσα"),
                Some("σ"),
                Some("ος\u{301}"),
                Some("abc\0é"),
                Some("你好👩‍💻"),
                Some(""),
                None,
            ],
        );
    }
    assert_rows(
        evaluate("upper", Arc::new(StringArray::from(source)), DataType::Utf8).unwrap(),
        vec![
            Some("IİII"),
            Some("STRASSE"),
            Some("FFI"),
            Some("ΟΣ"),
            Some("ΟΣΑ"),
            Some("Σ"),
            Some("ΟΣ\u{301}"),
            Some("ABC\0É"),
            Some("你好👩‍💻"),
            Some(""),
            None,
        ],
    );
}
#[test]
fn legacy_string_case_baseline_slice_null_and_zero_rows() {
    let source = Arc::new(StringArray::from(vec![
        Some("unused"),
        Some("ΟΣ"),
        None,
        Some("Straße"),
        Some("unused"),
    ])) as ArrayRef;
    for name in ["lower", "lcase", "upper"] {
        assert_rows(
            evaluate(name, source.slice(1, 3), DataType::Utf8).unwrap(),
            if name == "upper" {
                vec![Some("ΟΣ"), None, Some("STRASSE")]
            } else {
                vec![Some("ος"), None, Some("straße")]
            },
        );
        assert_rows(
            evaluate(name, source.slice(0, 0), DataType::Utf8).unwrap(),
            vec![],
        );
    }
}
#[test]
fn legacy_string_case_baseline_output_type_is_ignored_and_source_is_not_cast() {
    for name in ["lower", "lcase", "upper"] {
        let expected = if name == "upper" { "ABC" } else { "abc" };
        assert_rows(
            evaluate(
                name,
                Arc::new(StringArray::from(vec!["AbC"])),
                DataType::Int64,
            )
            .unwrap(),
            vec![Some(expected)],
        );
        let expected_error = if name == "upper" {
            "upper: argument must be a string array"
        } else {
            "lower expects string"
        };
        for input in [
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(NullArray::new(1)),
            Arc::new(LargeStringArray::from(vec!["AbC"])),
        ] {
            assert_eq!(
                evaluate(name, input, DataType::Utf8).unwrap_err(),
                expected_error
            );
        }
    }
}
#[test]
fn legacy_string_case_baseline_raw_lower_arity_and_unused_tail() {
    let input = Arc::new(StringArray::from(vec!["AbC"])) as ArrayRef;
    let chunk = chunk(input);
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8);
    for name in ["lower", "lcase"] {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| invoke(
                name,
                &arena,
                arg,
                &[],
                &chunk
            )))
            .is_err()
        );
        assert_rows(
            invoke(name, &arena, arg, &[arg, missing], &chunk).unwrap(),
            vec![Some("abc")],
        );
    }
}
#[test]
fn legacy_string_case_baseline_child_error_precedes_carrier_validation() {
    let chunk = chunk(Arc::new(StringArray::from(vec!["AbC"])));
    let mut arena = ExprArena::default();
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int64);
    let expected = arena.eval(missing, &chunk).unwrap_err();
    for name in ["lower", "lcase", "upper"] {
        assert_eq!(
            invoke(name, &arena, missing, &[missing], &chunk).unwrap_err(),
            expected
        );
    }
}
#[test]
fn legacy_string_case_baseline_long_context_and_utf8_expansion() {
    let mixed = "aİ".repeat(1025);
    let sigma = "Ο".to_string() + &"A".repeat(1025) + "Σ";
    let input = Arc::new(StringArray::from(vec![mixed.as_str(), sigma.as_str()])) as ArrayRef;
    let lower = "ai\u{307}".repeat(1025);
    let lower_sigma = "ο".to_string() + &"a".repeat(1025) + "ς";
    let upper = "Aİ".repeat(1025);
    for name in ["lower", "lcase"] {
        assert_rows(
            evaluate(name, input.clone(), DataType::Utf8).unwrap(),
            vec![Some(&lower), Some(&lower_sigma)],
        );
    }
    assert_rows(
        evaluate("upper", input, DataType::Utf8).unwrap(),
        vec![Some(&upper), Some(&sigma)],
    );
}

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

//! Immutable pre-extraction v1 crc32 behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
//! Pure public Utf8 and all four original raw byte carriers are distinguished.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use crate::runtime::fragment::ExecutionResult;
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_failure_contract::{FailureClass, FailureProjection};
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
fn eval(name: &str, columns: Vec<ArrayRef>) -> ExecutionResult<ArrayRef> {
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
fn legacy_crc32_shared_baseline_ieee_unsigned_unicode_nul_and_four_carriers() {
    let values = vec![
        Some(""),
        Some("123456789"),
        Some("hello"),
        Some("你好"),
        Some("\0"),
        Some("a\0b"),
        None,
    ];
    let bytes = values
        .iter()
        .map(|v| v.map(str::as_bytes))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.clone())),
        Arc::new(LargeStringArray::from(values)),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeBinaryArray::from(bytes)),
    ];
    let expected = vec![
        Some(0),
        Some(3421780262),
        Some(907060870),
        Some(1352841281),
        Some(3523407757),
        Some(367556721),
        None,
    ];
    for array in arrays {
        assert_rows(
            eval("crc32", vec![array.clone()]).unwrap(),
            expected.clone(),
        );
        assert_rows(
            eval("crc32", vec![array.slice(1, 5)]).unwrap(),
            expected[1..6].to_vec(),
        );
        assert_rows(eval("crc32", vec![array.slice(0, 0)]).unwrap(), vec![]);
    }
}
#[test]
fn legacy_crc32_shared_baseline_binary_bytes_are_not_utf8_converted() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(vec![
            Some(b"\xff\0\xfe".as_slice()),
            None,
            Some(b"".as_slice()),
        ])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"\xff\0\xfe".as_slice()),
            None,
            Some(b"".as_slice()),
        ])),
    ];
    for array in arrays {
        assert_rows(
            eval("crc32", vec![array]).unwrap(),
            vec![Some(467415780), None, Some(0)],
        );
    }
}
#[test]
fn legacy_crc32_shared_baseline_long_byte_boundaries_have_independent_zlib_oracles() {
    let lengths = [255, 256, 257, 512, 513, 10000];
    let expected = vec![
        Some(1030153177),
        Some(1944984080),
        Some(2434328829),
        Some(990604637),
        Some(2570924199),
        Some(223716515),
    ];
    let texts = lengths.iter().map(|n| "x".repeat(*n)).collect::<Vec<_>>();
    let array = column(texts.iter().map(|v| Some(v.as_str())).collect());
    assert_rows(
        eval("crc32", vec![array.clone()]).unwrap(),
        expected.clone(),
    );
    for row in 0..lengths.len() {
        assert_rows(
            eval("crc32", vec![array.slice(row, 1)]).unwrap(),
            vec![expected[row]],
        );
    }
}
#[test]
fn crc32_exact_unary_shape_precedes_execution_and_valid_child_error_is_preserved() {
    let mut arena = ExprArena::default();
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8);
    let source_chunk = chunk(vec![column(vec![Some("hello")])]);
    for arguments in [&[][..], &[source, missing][..]] {
        let failure =
            eval_string_function("crc32", &arena, source, arguments, &source_chunk).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 430);
    }
    assert_rows(
        eval_string_function("crc32", &arena, source, &[source], &source_chunk).unwrap(),
        vec![Some(907060870)],
    );
    let expected = arena.eval(missing, &source_chunk).unwrap_err();
    let failure =
        eval_string_function("crc32", &arena, source, &[missing], &source_chunk).unwrap_err();
    assert_eq!(failure.failure_fact(), expected.failure_fact());
    assert_eq!(failure.to_string(), expected.to_string());
    assert!(!failure.same_occurrence(&expected));
}
#[test]
fn legacy_crc32_shared_baseline_literal_pool_and_result_metadata_ignore_match() {
    let mut arena = ExprArena::default();
    let literal = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("123456789".into())),
        DataType::Utf8,
    );
    let output = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Binary);
    let source_chunk = chunk(vec![column(vec![Some("ignored"); 3])]);
    assert_rows(
        eval_string_function("crc32", &arena, output, &[literal], &source_chunk).unwrap(),
        vec![Some(3421780262); 3],
    );
    let pool = super::pure_differential::constant(
        novarocks_functions::FunctionValueType::new(DataType::Utf8, false),
        column(vec![Some("123456789")]),
    );
    let pooled = arena.push_typed(ExprNode::Constant(pool), DataType::Utf8);
    assert_rows(
        eval_string_function("crc32", &arena, output, &[pooled], &source_chunk).unwrap(),
        vec![Some(3421780262); 3],
    );
}

#[test]
fn legacy_crc32_shared_baseline_unsupported_null_and_long_dtype_errors_are_full_layout_boundary_and_complete_diagnostic()
 {
    // Keep the original oversized input as a real construction refusal.
    let oversized = DataType::Struct(
        vec![Arc::new(Field::new(
            "long_field_".repeat(128),
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    assert_eq!(
        novarocks_type_contract::ValueLayout::try_new(
            &oversized,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap_err(),
        novarocks_type_contract::ExecutionValueTypeError::Invalid(
            "value layout field name exceeds the byte limit"
        ),
    );

    for array in [
        Arc::new(NullArray::new(1)) as ArrayRef,
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
        Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
    ] {
        let expected = format!(
            "crc32 expects VARCHAR/BINARY input, got {:?}",
            array.data_type()
        );
        assert_eq!(
            eval("crc32", vec![array]).unwrap_err().to_string(),
            expected
        );
    }
    let field = Arc::new(Field::new("long_field_".repeat(90), DataType::Int64, true));
    let array = Arc::new(arrow::array::StructArray::from(vec![(
        field,
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
    )])) as ArrayRef;
    let expected = format!(
        "crc32 expects VARCHAR/BINARY input, got {:?}",
        array.data_type()
    );
    assert!(expected.len() > 512);
    let failure = eval("crc32", vec![array]).unwrap_err();
    assert_eq!(
        failure.to_string(),
        novarocks_failure_contract::BoundedDetail::truncating_display(&expected).to_string()
    );
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(failure.failure_fact().reason().code(), 360);
}

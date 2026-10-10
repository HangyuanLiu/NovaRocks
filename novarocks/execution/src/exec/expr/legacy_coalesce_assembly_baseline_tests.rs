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
//! Additional immutable legacy conditional assembly oracles before extraction.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{self, FunctionKind};
use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, Int32Array, Int64Array,
    LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn eval(name: &str, columns: Vec<ArrayRef>, output: DataType) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let mut args = Vec::new();
    let mut fields = Vec::new();
    let mut slots = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let slot = SlotId::new(i as u32 + 1);
        args.push(arena.push_typed(ExprNode::SlotId(slot), column.data_type().clone()));
        fields.push(Field::new(
            format!("c{i}"),
            column.data_type().clone(),
            true,
        ));
        slots.push(slot);
    }
    let kind = if name == "ifnull" {
        FunctionKind::IfNull
    } else {
        FunctionKind::Coalesce
    };
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: args.clone(),
        },
        output,
    );
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let input = Chunk::new_with_chunk_schema(batch, schema);
    if name == "ifnull" {
        function::eval_ifnull(&arena, args[0], args[1], &input)
    } else {
        function::eval_coalesce(&arena, root, &args, &input)
    }
}
fn assert_display(actual: &ArrayRef, expected: &ArrayRef) {
    assert_eq!(actual.data_type(), expected.data_type());
    assert_eq!(actual.len(), expected.len());
    let opts = arrow::util::display::FormatOptions::default().with_null("\\N");
    let a = arrow::util::display::ArrayFormatter::try_new(actual.as_ref(), &opts).unwrap();
    let b = arrow::util::display::ArrayFormatter::try_new(expected.as_ref(), &opts).unwrap();
    for i in 0..actual.len() {
        assert_eq!(a.value(i).to_string(), b.value(i).to_string());
    }
}
fn list(values: Vec<Option<i32>>, large: bool) -> ArrayRef {
    let values = values
        .into_iter()
        .map(|v| v.map(|v| vec![Some(v)]))
        .collect::<Vec<_>>();
    if large {
        Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
            values,
        ))
    } else {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values))
    }
}
fn map(values: Vec<Option<i32>>) -> ArrayRef {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for value in values {
        if let Some(value) = value {
            builder.keys().append_value("a");
            builder.values().append_value(value);
            builder.append(true).unwrap();
        } else {
            builder.append(false).unwrap();
        }
    }
    Arc::new(builder.finish())
}
fn structure(values: Vec<Option<i32>>) -> ArrayRef {
    let valid = values.iter().map(Option::is_some).collect::<Vec<_>>();
    let values = values
        .into_iter()
        .map(|v| v.unwrap_or(0))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new("value", DataType::Int32, false))].into(),
        vec![Arc::new(Int32Array::from(values))],
        Some(NullBuffer::from(valid)),
    ))
}
#[test]
fn legacy_coalesce_assembly_baseline_nested_carriers_and_nonzero_slices() {
    let left = vec![Some(1), Some(2), None, Some(3)];
    let right = vec![Some(1), Some(9), Some(4), None];
    let expected = vec![Some(1), Some(2), Some(4), Some(3)];
    for (left, right, expected) in [
        (
            list(left.clone(), false),
            list(right.clone(), false),
            list(expected.clone(), false),
        ),
        (
            list(left.clone(), true),
            list(right.clone(), true),
            list(expected.clone(), true),
        ),
        (map(left.clone()), map(right.clone()), map(expected.clone())),
        (structure(left), structure(right), structure(expected)),
    ] {
        for name in ["ifnull", "coalesce"] {
            for (offset, len) in [(0, 4), (1, 3)] {
                let ty = left.data_type().clone();
                let actual = eval(
                    name,
                    vec![left.slice(offset, len), right.slice(offset, len)],
                    ty,
                )
                .unwrap();
                assert_display(&actual, &expected.slice(offset, len));
            }
        }
    }
}
#[test]
fn legacy_coalesce_assembly_baseline_timestamp_timezone_is_retained() {
    let left: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1), None, None, Some(3)])
            .with_timezone("Asia/Shanghai"),
    );
    let right: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(9), Some(2), None, None])
            .with_timezone("Asia/Shanghai"),
    );
    let expected: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1), Some(2), None, Some(3)])
            .with_timezone("Asia/Shanghai"),
    );
    for name in ["ifnull", "coalesce"] {
        let output = eval(
            name,
            vec![left.clone(), right.clone()],
            left.data_type().clone(),
        )
        .unwrap();
        assert_eq!(output.data_type(), left.data_type());
        assert_eq!(output.to_data(), expected.to_data());
    }
}
#[test]
fn legacy_coalesce_assembly_baseline_decimal_targets_preserve_distinct_rules() {
    let left: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(100), None, None])
            .with_precision_and_scale(9, 2)
            .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(1000), Some(3333), None])
            .with_precision_and_scale(18, 3)
            .unwrap(),
    );
    let declared = DataType::Decimal128(18, 3);
    let output = eval(
        "ifnull",
        vec![left.clone(), right.clone()],
        declared.clone(),
    )
    .unwrap();
    let expected = Decimal128Array::from(vec![Some(100), Some(333), None])
        .with_precision_and_scale(9, 2)
        .unwrap();
    assert_eq!(output.to_data(), expected.to_data());
    let output = eval("coalesce", vec![left, right], declared).unwrap();
    let expected = Decimal128Array::from(vec![Some(1000), Some(3333), None])
        .with_precision_and_scale(18, 3)
        .unwrap();
    assert_eq!(output.to_data(), expected.to_data());
}
#[test]
fn legacy_coalesce_assembly_baseline_dictionary_uses_logical_null_mask() {
    let left: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec![None, Some("a")])),
        )
        .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(0), None]),
            Arc::new(StringArray::from(vec![Some("b")])),
        )
        .unwrap(),
    );
    for name in ["ifnull", "coalesce"] {
        let output = eval(
            name,
            vec![left.clone(), right.clone()],
            left.data_type().clone(),
        )
        .unwrap();
        assert_eq!(output.data_type(), left.data_type());
        let opts = arrow::util::display::FormatOptions::default().with_null("\\N");
        let formatted =
            arrow::util::display::ArrayFormatter::try_new(output.as_ref(), &opts).unwrap();
        assert_eq!(
            (0..3)
                .map(|i| formatted.value(i).to_string())
                .collect::<Vec<_>>(),
            vec!["b", "b", "a"]
        );
    }
}
#[test]
fn legacy_coalesce_assembly_baseline_bytes_large_utf8_and_unobserved_payloads() {
    let left: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"a\0b"[..]), None, None]));
    let right: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"unused"[..]),
        Some(&b"\xff"[..]),
        None,
    ]));
    let expected: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"a\0b"[..]),
        Some(&b"\xff"[..]),
        None,
    ]));
    for name in ["ifnull", "coalesce"] {
        let output = eval(name, vec![left.clone(), right.clone()], DataType::Binary).unwrap();
        assert_eq!(output.to_data(), expected.to_data());
    }
    let left: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("中"), None, None]));
    let right: ArrayRef = Arc::new(LargeStringArray::from(vec![
        Some("unused"),
        Some("a\0b"),
        None,
    ]));
    let expected: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("中"), Some("a\0b"), None]));
    for name in ["ifnull", "coalesce"] {
        let output = eval(name, vec![left.clone(), right.clone()], DataType::LargeUtf8).unwrap();
        assert_eq!(output.to_data(), expected.to_data());
    }
}
#[test]
fn legacy_coalesce_assembly_baseline_direct_return_preserves_first_array_identity() {
    let first: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
    let later: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; 2]));
    let output = eval("coalesce", vec![first.clone(), later], DataType::Int64).unwrap();
    assert!(Arc::ptr_eq(&first, &output));
}

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

//! Independent arena and Types decimal text receipts before core extraction.

use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use novarocks_types::SlotId;
use std::sync::Arc;

fn input() -> ArrayRef {
    let bytes = [
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(0),
        Some(42),
        None,
    ]
    .map(|v| v.map(i128::to_be_bytes));
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            bytes.iter().map(|v| v.as_ref().map(|b| b.as_slice())),
            16,
        )
        .unwrap(),
    )
}
fn arena_text(input: ArrayRef, nominal: bool) -> ArrayRef {
    let ty = if nominal {
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap()
    } else {
        FunctionValueType::new(DataType::FixedSizeBinary(16), true)
    };
    let field = ty.try_to_field("source").unwrap();
    assert_eq!(FunctionValueType::try_from_field(&field).unwrap(), ty);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![input]).unwrap();
    let layout =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, layout);
    let mut arena = ExprArena::default();
    let child = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        DataType::FixedSizeBinary(16),
    );
    let cast = arena.push_typed(
        ExprNode::Cast(child, DecimalOverflowPolicy::ReportError),
        DataType::Utf8,
    );
    arena.eval(cast, &chunk).unwrap()
}
#[test]
fn original_largeint_text_types_and_actual_arena_physical_and_nominal_fields() {
    let input = input();
    let expected = [
        Some("-170141183460469231731687303715884105728"),
        Some("170141183460469231731687303715884105727"),
        Some("-1"),
        Some("0"),
        Some("42"),
        None,
    ];
    for (offset, len) in [(0, 6), (1, 4), (0, 0)] {
        let source = input.slice(offset, len);
        let expected = StringArray::from(expected[offset..offset + len].to_vec());
        for result in [
            novarocks_types::arrow_cast::cast_scalar_with_special_rules(&source, &DataType::Utf8)
                .unwrap(),
            arena_text(source.clone(), false),
            arena_text(source, true),
        ] {
            assert_eq!(result.to_data(), expected.to_data());
        }
    }
}
#[test]
fn original_largeint_reader_keeps_complete_error_text_and_byte_order() {
    assert_eq!(
        novarocks_types::largeint::i128_from_be_bytes(&[0; 15]).unwrap_err(),
        "invalid LARGEINT byte length: expected 16, got 15"
    );
    let wrong: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
    assert_eq!(
        novarocks_types::largeint::as_fixed_size_binary_array(&wrong, "cast LARGEINT to VARCHAR")
            .unwrap_err(),
        "cast LARGEINT to VARCHAR: expected FixedSizeBinaryArray"
    );
    let wrong: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(&[0_u8; 15][..])].into_iter(),
            15,
        )
        .unwrap(),
    );
    assert_eq!(
        novarocks_types::largeint::as_fixed_size_binary_array(&wrong, "cast LARGEINT to VARCHAR")
            .unwrap_err(),
        "cast LARGEINT to VARCHAR: expected FixedSizeBinary(16), got FixedSizeBinary(15)"
    );
    assert_eq!(
        novarocks_types::largeint::i128_from_be_bytes(&[255; 16]).unwrap(),
        -1
    );
}

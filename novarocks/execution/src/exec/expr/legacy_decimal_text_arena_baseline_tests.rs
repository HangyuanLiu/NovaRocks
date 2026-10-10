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
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;

fn arena_text(input: ArrayRef) -> ArrayRef {
    let source_type = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            source_type.clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap();
    let layout =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, layout);
    let mut arena = ExprArena::default();
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type);
    let call = arena.push_typed(
        ExprNode::Cast(child, DecimalOverflowPolicy::ReportError),
        DataType::Utf8,
    );
    arena.eval(call, &chunk).unwrap()
}

fn assert_both(input: ArrayRef, expected: Vec<Option<&str>>) {
    let expected = StringArray::from(expected);
    for output in [
        arena_text(input.clone()),
        novarocks_types::arrow_cast::cast_scalar_with_special_rules(&input, &DataType::Utf8)
            .unwrap(),
    ] {
        assert_eq!(output.data_type(), &DataType::Utf8);
        assert_eq!(output.to_data(), expected.to_data());
    }
}

#[test]
fn original_decimal_text_arena_and_types_keep_scale_min_null_slice_empty() {
    for scale in [-3, 0, 2] {
        let expected = if scale > 0 {
            vec![Some("12.34"), Some("-0.05"), None, Some("0.00")]
        } else {
            vec![Some("1234"), Some("-5"), None, Some("0")]
        };
        let input128: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1234), Some(-5), None, Some(0)])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        let input256: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(1234)),
                Some(i256::from_i128(-5)),
                None,
                Some(i256::ZERO),
            ])
            .with_precision_and_scale(76, scale)
            .unwrap(),
        );
        for input in [input128, input256] {
            assert_both(input.clone(), expected.clone());
            assert_both(input.slice(1, 2), expected[1..3].to_vec());
            assert_both(input.slice(0, 0), vec![]);
        }
    }
    let minimum128: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN)])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    // Original debug abs panics; an overflow-check-free build wraps the same MIN.
    for raw_types in [false, true] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if raw_types {
                novarocks_types::arrow_cast::cast_scalar_with_special_rules(
                    &minimum128,
                    &DataType::Utf8,
                )
                .unwrap()
            } else {
                arena_text(minimum128.clone())
            }
        }));
        if cfg!(debug_assertions) {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result.unwrap().to_data(),
                StringArray::from(vec![Some("--1701411834604692317316873037158841057.28")])
                    .to_data()
            );
        }
    }
    let minimum256 = i256::from_string(
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
    )
    .unwrap();
    let minimum256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(minimum256), None])
            .with_precision_and_scale(76, 2)
            .unwrap(),
    );
    assert_both(
        minimum256,
        vec![
            Some(
                "--578960446186580977117854925043439539266349923328202820197287920039565648199.68",
            ),
            None,
        ],
    );
}

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

//! Independent original date/to_date dispatcher receipts before shared extraction.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, NullArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
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
fn eval(name: &'static str, columns: Vec<ArrayRef>, result: DataType) -> Result<ArrayRef, String> {
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
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date(name),
            args: args.clone(),
        },
        result,
    );
    eval_date_function(name, &arena, call, &args, &chunk(columns))
}
fn assert_dates(output: ArrayRef, expected: Vec<Option<i32>>) {
    assert_eq!(output.data_type(), &DataType::Date32);
    assert_eq!(output.to_data(), Date32Array::from(expected).to_data());
}
#[test]
fn legacy_to_date_baseline_all_aliases_text_grammar_nulls_and_invalids() {
    for name in ["date", "to_date", "to_tera_date"] {
        assert_dates(
            eval(
                name,
                vec![Arc::new(StringArray::from(vec![
                    Some("1970-01-01"),
                    Some("1969-12-31 23:59:59.999999"),
                    Some("20240229"),
                    Some("2024-02-29T12:34:56.123456789"),
                    Some(" 2024-02-29 "),
                    Some("bad"),
                    None,
                ]))],
                DataType::Date32,
            )
            .unwrap(),
            vec![
                Some(0),
                Some(-1),
                Some(19782),
                Some(19782),
                Some(19782),
                None,
                None,
            ],
        );
    }
}
#[test]
fn legacy_to_date_baseline_date32_ranges_and_result_carrier_ignore_requested_type() {
    for name in ["date", "to_date"] {
        assert_dates(
            eval(
                name,
                vec![Arc::new(Date32Array::from(vec![
                    Some(i32::MIN),
                    Some(-1),
                    Some(0),
                    Some(19782),
                    Some(i32::MAX),
                    None,
                ]))],
                DataType::Utf8,
            )
            .unwrap(),
            vec![None, Some(-1), Some(0), Some(19782), None, None],
        );
    }
}
#[test]
fn legacy_to_date_baseline_raw_timestamp_units_ignore_zone_and_extreme_ranges() {
    for name in ["date", "to_date"] {
        for zone in ["UTC", "Asia/Shanghai"] {
            let inputs: Vec<ArrayRef> = vec![
                Arc::new(
                    TimestampSecondArray::from(vec![
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(i64::MIN),
                        Some(i64::MAX),
                        None,
                    ])
                    .with_timezone(zone),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(i64::MIN),
                        Some(i64::MAX),
                        None,
                    ])
                    .with_timezone(zone),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(i64::MIN),
                        Some(i64::MAX),
                        None,
                    ])
                    .with_timezone(zone),
                ),
                Arc::new(
                    TimestampNanosecondArray::from(vec![
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(i64::MIN),
                        Some(i64::MAX),
                        None,
                    ])
                    .with_timezone(zone),
                ),
            ];
            for (index, input) in inputs.into_iter().enumerate() {
                let mut expected = vec![Some(-1), Some(0), Some(0), None, None, None];
                if index == 3 {
                    expected[3] = Some(-106752);
                    expected[4] = Some(106751);
                }
                assert_dates(eval(name, vec![input], DataType::Date32).unwrap(), expected);
            }
        }
    }
}
#[test]
fn legacy_to_date_baseline_declared_int64_drift_and_complete_unsupported_type_errors() {
    for name in ["date", "to_date"] {
        for input in [
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            Arc::new(NullArray::new(1)),
            Arc::new(BooleanArray::from(vec![None])),
        ] {
            let expected = format!("unsupported date input type: {:?}", input.data_type());
            assert_eq!(
                eval(name, vec![input], DataType::Date32).unwrap_err(),
                expected
            );
        }
        let fields = Fields::from(vec![Arc::new(Field::new(
            "long_FIELD_é".repeat(180),
            DataType::Int64,
            true,
        ))]);
        let input = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        )) as ArrayRef;
        let expected = format!("unsupported date input type: {:?}", input.data_type());
        assert!(expected.len() > 1024);
        assert_eq!(
            eval(name, vec![input], DataType::Date32).unwrap_err(),
            expected
        );
    }
}

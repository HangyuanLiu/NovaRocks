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
//! Independent receipts from the real, unmodified v1 date dispatcher.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, NullArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
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

#[test]
fn legacy_sec_to_time_baseline_saturated_extremes_sign_and_unit_boundaries() {
    let values = vec![
        i64::MIN,
        -3024000,
        -3023999,
        -3600,
        -61,
        -60,
        -59,
        -1,
        0,
        1,
        59,
        60,
        61,
        3599,
        3600,
        3023999,
        3024000,
        i64::MAX,
    ];
    let expected = vec![
        "-839:59:59",
        "-839:59:59",
        "-839:59:59",
        "-01:00:00",
        "-00:01:01",
        "-00:01:00",
        "-00:00:59",
        "-00:00:01",
        "00:00:00",
        "00:00:01",
        "00:00:59",
        "00:01:00",
        "00:01:01",
        "00:59:59",
        "01:00:00",
        "839:59:59",
        "839:59:59",
        "839:59:59",
    ];
    let result = eval(
        "sec_to_time",
        vec![Arc::new(Int64Array::from(values))],
        DataType::Utf8,
    )
    .unwrap();
    assert_eq!(result.to_data(), StringArray::from(expected).to_data());
}
#[test]
fn legacy_sec_to_time_baseline_nulls_empty_and_ignored_requested_carrier() {
    for requested in [DataType::Utf8, DataType::Date32, DataType::Int64] {
        let result = eval(
            "sec_to_time",
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(i64::MIN),
            ]))],
            requested,
        )
        .unwrap();
        assert_eq!(
            result.to_data(),
            StringArray::from(vec![Some("00:00:01"), None, Some("-839:59:59")]).to_data()
        );
    }
    let result = eval(
        "sec_to_time",
        vec![Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))],
        DataType::Utf8,
    )
    .unwrap();
    assert_eq!(result.data_type(), &DataType::Utf8);
    assert_eq!(result.len(), 0);
}
#[test]
fn legacy_sec_to_time_baseline_raw_wrong_carriers_fail_even_when_null() {
    for array in [
        Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        Arc::new(NullArray::new(1)),
        Arc::new(BooleanArray::from(vec![None])),
        Arc::new(Date32Array::from(vec![None])),
        Arc::new(StringArray::from(vec![Some("00:00:01")])),
    ] {
        assert_eq!(
            eval("sec_to_time", vec![array], DataType::Utf8).unwrap_err(),
            "sec_to_time expects int"
        );
    }
}
#[test]
fn legacy_sec_to_time_baseline_extra_child_is_ignored_and_missing_child_panics() {
    let result = eval(
        "sec_to_time",
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(BooleanArray::from(vec![true])),
        ],
        DataType::Utf8,
    )
    .unwrap();
    assert_eq!(
        result.to_data(),
        StringArray::from(vec!["00:00:01"]).to_data()
    );
    let mut arena = ExprArena::default();
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("sec_to_time"),
            args: vec![],
        },
        DataType::Utf8,
    );
    let data = chunk(vec![Arc::new(Int64Array::from(vec![1]))]);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_date_function(
            "sec_to_time",
            &arena,
            call,
            &[],
            &data
        )))
        .is_err()
    );
}

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

//! Real original scalar state producer baselines before any owner or extraction.
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprNode, LiteralValue};
use arrow::array::{Array, BinaryArray, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;

fn setup(arrays: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots = (0..arrays.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("f{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.clone()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let args = arrays
        .iter()
        .zip(slots)
        .map(|(a, s)| arena.push_typed(ExprNode::SlotId(s), a.data_type().clone()))
        .collect();
    (arena, args, chunk)
}
fn evaluate(arrays: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(arrays);
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("ds_hll_count_distinct_state"),
            args: args.clone(),
        },
        DataType::Binary,
    );
    eval_ds_hll_count_distinct_state(&arena, expr, &args, &chunk)
}
#[test]
fn legacy_ds_hll_baseline_scalar_three_declared_arities_payload_and_null() {
    for arity in 1..=3 {
        let mut arrays =
            vec![Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef];
        if arity >= 2 {
            arrays.push(Arc::new(Int64Array::from(vec![10; 3])));
        }
        if arity >= 3 {
            arrays.push(Arc::new(StringArray::from(vec!["HLL_8"; 3])));
        }
        let out = evaluate(arrays).unwrap();
        let bytes = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(bytes.is_null(1));
        for row in [0, 2] {
            assert_eq!(bytes.value(row)[3], if arity == 1 { 17 } else { 10 });
            assert_eq!(
                (bytes.value(row)[7] >> 2) & 3,
                if arity == 3 { 2 } else { 1 }
            );
            assert_eq!(
                HllHandle::from_payload_unreserved(bytes.value(row))
                    .unwrap()
                    .estimate()
                    .unwrap(),
                1
            );
        }
    }
}
#[test]
fn legacy_ds_hll_baseline_scalar_null_skips_bad_tuning_but_value_type_still_matters() {
    let values = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
    let unsupported = Arc::new(StringArray::from(vec!["not-a-number"])) as ArrayRef;
    let out = evaluate(vec![
        values,
        unsupported,
        Arc::new(Int64Array::from(vec![99])),
    ])
    .unwrap();
    assert!(out.is_null(0));
    let values = Arc::new(UInt32Array::from(vec![None])) as ArrayRef;
    assert_eq!(
        evaluate(vec![values]).unwrap_err(),
        "ds_hll_count_distinct_state: unsupported sketch hash input type UInt32"
    );
}
#[test]
fn legacy_ds_hll_baseline_scalar_checked_integer_float_and_type_full_errors() {
    let value = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    for (log, message) in [
        (-1, "ds_hll_count_distinct_state log_k out of range: -1"),
        (
            0,
            "ds_hll_count_distinct_state log_k must be in [4, 21], got 0",
        ),
        (266, "ds_hll_count_distinct_state log_k out of range: 266"),
    ] {
        assert_eq!(
            evaluate(vec![value.clone(), Arc::new(Int64Array::from(vec![log]))]).unwrap_err(),
            message
        );
    }
    let out = evaluate(vec![
        value.clone(),
        Arc::new(Float64Array::from(vec![10.9])),
        Arc::new(StringArray::from(vec!["unknown"])),
    ])
    .unwrap();
    let bytes = out.as_any().downcast_ref::<BinaryArray>().unwrap().value(0);
    assert_eq!(bytes[3], 10);
    assert_eq!((bytes[7] >> 2) & 3, 1);
    assert_eq!(
        evaluate(vec![
            value,
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![7]))
        ])
        .unwrap_err(),
        "ds_hll_count_distinct_state target type expects string input, got Int64(7)"
    );
}
#[test]
fn legacy_ds_hll_baseline_scalar_child_evaluation_precedes_row_hash_and_tuning() {
    let (mut arena, mut args, chunk) =
        setup(vec![Arc::new(UInt32Array::from(vec![1])) as ArrayRef]);
    args.push(ExprId(usize::MAX));
    let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Binary);
    assert_eq!(
        eval_ds_hll_count_distinct_state(&arena, expr, &args, &chunk).unwrap_err(),
        "invalid ExprId"
    );
    let (mut arena, mut args, chunk) = setup(vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef]);
    args.extend([
        arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64),
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("HLL_6".to_owned())),
            DataType::Utf8,
        ),
        ExprId(usize::MAX),
    ]);
    let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Binary);
    assert!(
        eval_ds_hll_count_distinct_state(&arena, expr, &args, &chunk).is_ok(),
        "raw fourth child is ignored, not a bound overload"
    );
}

#[cfg(test)]
#[path = "original_ds_scalar_domain_probes.rs"]
mod original_ds_scalar_domain_probes;

#[cfg(test)]
#[path = "legacy_ds_scalar_reader_baseline_tests.rs"]
mod legacy_ds_scalar_reader_baseline_tests;

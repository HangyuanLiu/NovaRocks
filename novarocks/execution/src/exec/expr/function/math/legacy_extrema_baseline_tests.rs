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
//! Additional independent v1 oracles before extracting greatest/least.
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{DecimalOverflowPolicy, ExprNode};
use arrow::array::{Date32Array, Int64Array, NullArray, new_null_array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
fn evaluate(
    name: &'static str,
    inputs: Vec<ArrayRef>,
    result_type: DataType,
    child_policy: Option<DecimalOverflowPolicy>,
) -> Result<ArrayRef, String> {
    let fields = inputs
        .iter()
        .enumerate()
        .map(|(i, array)| Field::new(format!("v{i}"), array.data_type().clone(), true))
        .collect::<Vec<_>>();
    let types = inputs
        .iter()
        .map(|array| array.data_type().clone())
        .collect::<Vec<_>>();
    let slots = (1..=inputs.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(child_policy == Some(DecimalOverflowPolicy::ReportError));
    arena.set_session_time_zone(Some("America/New_York".into()));
    let args: Vec<ExprId> = slots
        .into_iter()
        .zip(types)
        .map(|(slot, ty)| {
            let source = arena.push_typed(ExprNode::SlotId(slot), ty.clone());
            match child_policy {
                Some(policy) => {
                    let cast = arena.push_typed(ExprNode::Cast(source, policy), ty);
                    assert_eq!(arena.decimal_overflow_policy(cast), Some(policy));
                    cast
                }
                None => source,
            }
        })
        .collect();
    // The legacy function ABI has canonical Math kind and an exact output
    // carrier, but no new dynamic binding receipt or call-policy slot.
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Math(name),
            args: args.clone(),
        },
        result_type.clone(),
    );
    assert_eq!(arena.decimal_overflow_policy(call), None);
    let output = if name == "greatest" {
        eval_greatest(&arena, call, &args, &chunk)?
    } else {
        eval_least(&arena, call, &args, &chunk)?
    };
    assert_eq!(output.data_type(), &result_type);
    Ok(output)
}

#[test]
fn legacy_extrema_baseline_single_argument_signed_zero_bits() {
    for name in ["greatest", "least"] {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0), None]));
        let out = evaluate(name, vec![values], DataType::Float64, None).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0).to_bits(), 0x8000_0000_0000_0000);
        assert_eq!(out.value(1).to_bits(), 0);
        assert!(out.is_null(2));
    }
}
#[test]
fn legacy_extrema_baseline_zero_rows_still_rejects_late_numeric_carrier() {
    let bad = DataType::Struct(vec![Field::new("bad", DataType::Int64, true)].into());
    for name in ["greatest", "least"] {
        for rows in [0, 2] {
            let error = evaluate(
                name,
                vec![Arc::new(NullArray::new(rows)), new_null_array(&bad, rows)],
                DataType::Float64,
                None,
            )
            .unwrap_err();
            assert_eq!(error, format!("unsupported numeric type: {bad:?}"));
        }
    }
}
#[test]
fn legacy_extrema_baseline_full_source_and_target_diagnostics() {
    let name = "very_long_field_".to_string() + &"x".repeat(900);
    let bad = DataType::Struct(vec![Field::new(&name, DataType::Int64, true)].into());
    for operation in ["greatest", "least"] {
        let error = evaluate(
            operation,
            vec![new_null_array(&bad, 1)],
            DataType::Float64,
            None,
        )
        .unwrap_err();
        assert_eq!(error, format!("unsupported numeric type: {bad:?}"));
        assert!(error.len() > 512);
        let error = evaluate(
            operation,
            vec![Arc::new(Float64Array::from(vec![1.0]))],
            bad.clone(),
            None,
        )
        .unwrap_err();
        assert!(error.starts_with("math: failed to cast output:"));
        assert!(error.contains(&name));
        assert!(error.len() > 512);
    }
}
#[test]
fn legacy_extrema_baseline_finite_f64_to_f32_overflow_is_null() {
    for name in ["greatest", "least"] {
        let out = evaluate(
            name,
            vec![Arc::new(Float64Array::from(vec![
                Some(f64::MAX),
                Some(-f64::MAX),
                Some(1.5),
                None,
            ]))],
            DataType::Float32,
            Some(DecimalOverflowPolicy::ReportError),
        )
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![None, None, Some(1.5), None]
        );
    }
}
#[test]
fn legacy_extrema_baseline_raw_numeric_short_panic_date_short_null_and_scalar_broadcast() {
    let raw: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    let view = NumericArrayView::new(&raw).unwrap();
    assert_eq!(value_at_f64(&view, 1, 3), Some(8.0));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| value_at_f64(&view, 2, 3)))
            .is_err()
    );
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let view = NumericArrayView::new(&scalar).unwrap();
    assert_eq!(value_at_f64(&view, 2, 3), Some(7.0));
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    assert_eq!(datetime_value_at(&[Some(date), None], 2, 3), None);
    assert_eq!(datetime_value_at(&[Some(date)], 2, 3), Some(date));
}
#[test]
fn legacy_extrema_baseline_extreme_date32_invalid_and_null_rows() {
    for name in ["greatest", "least"] {
        let out = evaluate(
            name,
            vec![Arc::new(Date32Array::from(vec![
                Some(i32::MAX),
                Some(i32::MIN),
                Some(0),
                None,
            ]))],
            DataType::Timestamp(TimeUnit::Microsecond, None),
            None,
        )
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![None, None, Some(0), None]
        );
    }
}

#[test]
fn legacy_extrema_baseline_exact_null_target_retains_full_batch_cast_failure_even_when_empty() {
    let expected =
        "math: failed to cast output: Cast error: Casting from Timestamp(µs) to Null not supported";
    for name in ["greatest", "least"] {
        for rows in [0, 5] {
            assert_eq!(
                evaluate(
                    name,
                    vec![Arc::new(NullArray::new(rows))],
                    DataType::Null,
                    None
                )
                .unwrap_err(),
                expected
            );
        }
    }
}

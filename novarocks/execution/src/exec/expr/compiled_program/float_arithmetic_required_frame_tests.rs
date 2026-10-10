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

//! Float arithmetic uses the existing full source/effects compiler and Frame owner.
use super::*;
use crate::exec::expr::original_float_arithmetic_baseline_tests as raw;
use arrow::array::{Array, BooleanArray, Float64Array};
#[test]
fn float_arithmetic_required_actual_compiler_frame_sparse_bits_and_empty_selection() {
    for op in [
        BinaryOperator::Add,
        BinaryOperator::Subtract,
        BinaryOperator::Multiply,
        BinaryOperator::Divide,
        BinaryOperator::Modulo,
    ] {
        let program = build_program(op, DataType::Float64, DataType::Float64, Wrap::Bare, false);
        let left = raw::f64_values();
        let right = raw::f64_values();
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            vec![
                left.clone(),
                right.clone(),
                Arc::new(BooleanArray::from(vec![None; 12])),
            ],
        )
        .unwrap();
        let original = raw::legacy(
            operation(op),
            left,
            right,
            DecimalOverflowPolicy::ReportError,
            false,
        )
        .unwrap();
        let original = original.as_any().downcast_ref::<Float64Array>().unwrap();
        for rows in [&[0, 2, 3, 6, 10, 11][..], &[][..]] {
            let selection = Selection::try_sparse(12, rows).unwrap();
            let out = instance(&program)
                .evaluate(&batch, selection, &Control)
                .unwrap();
            assert_eq!(out.selection(), selection);
            assert!(out.errors().is_empty());
            let values = out
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for (ordinal, row) in rows.iter().copied().enumerate() {
                assert_eq!(values.is_null(ordinal), original.is_null(row));
                if !values.is_null(ordinal) {
                    assert_eq!(
                        values.value(ordinal).to_bits(),
                        original.value(row).to_bits()
                    );
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_required_actual_frame_seven_causes_prefix_and_failed_latch() {
    let program = build_program(
        BinaryOperator::Divide,
        DataType::Float64,
        DataType::Float64,
        Wrap::Bare,
        false,
    );
    let values = (0..320)
        .map(|row| if row % 5 == 0 { None } else { Some(row as f64) })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(Float64Array::from(values)),
            Arc::new(Float64Array::from(vec![Some(2.0); 320])),
            Arc::new(BooleanArray::from(vec![None; 320])),
        ],
    )
    .unwrap();
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let out = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert!(out.errors().is_empty());
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for stop in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop);
            assert!(
                matches!(evaluator.evaluate(&batch,Selection::all(320),&control),Err(actual) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

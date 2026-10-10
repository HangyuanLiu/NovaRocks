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
//! Actual LocalCompiler recipe + Frame equality against the original arena TIME shell.
use super::*;
use crate::exec::expr::legacy_text_time_cast_baseline_tests::{actual, corpus, values};
use arrow::array::{BooleanArray, Int64Array};
fn batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn check(dtype: DataType) {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let program = compiled(
                FunctionValueType::new(dtype.clone(), true),
                FunctionValueType::new(
                    DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    true,
                ),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            for source in [
                corpus(&dtype),
                corpus(&dtype).slice(1, 7),
                corpus(&dtype).slice(0, 0),
            ] {
                let expected = values(&actual(source.clone(), allow, policy, false).unwrap());
                let batch = batch(&program, source.clone());
                for rows in [
                    (0..source.len()).collect::<Vec<_>>(),
                    (0..source.len()).filter(|row| row % 3 == 0).collect(),
                ] {
                    let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                    let output = instance(&program)
                        .evaluate(&batch, selection, &Control)
                        .unwrap();
                    assert!(output.errors().is_empty());
                    assert_eq!(
                        values(output.values()),
                        rows.iter().map(|row| expected[*row]).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
#[test]
fn text_time_actual_compiler_utf8_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::Utf8);
}
#[test]
fn text_time_actual_compiler_large_utf8_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::LargeUtf8);
}
#[test]
fn text_time_actual_compiler_utf8_view_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::Utf8View);
}

#[test]
fn text_time_actual_compiler_all_seven_runtime_causes_stop_and_latch_without_replay() {
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                true,
            ),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let long = format!("{}:00:00", "0".repeat(777));
        let source = crate::exec::expr::legacy_text_time_cast_baseline_tests::input(
            &dtype,
            &[Some(long.as_str()), None, Some("+01:02:03")],
        );
        let batch = batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert!(
            instance(&program)
                .evaluate(&batch, Selection::all(3), &recorder)
                .unwrap()
                .errors()
                .is_empty()
        );
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|units| *units <= 256));
        for at in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = instance(&program);
                let control = CallbackControl::new(cause.clone(), at);
                assert!(
                    matches!(evaluator.evaluate(&batch, Selection::all(3), &control),
                    Err(actual) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(&batch, Selection::all(3), &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[path = "cast_calendar_time_tests.rs"]
mod calendar_time_tests;

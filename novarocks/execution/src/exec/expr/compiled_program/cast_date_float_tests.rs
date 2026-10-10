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

//! Real LocalCompiler Date32 float profiles, native literal projection and host first-failure evidence.
use super::*;
use crate::exec::expr::legacy_date_float_cast_baseline_tests::{actual, day, original};
use arrow::array::{Date32Array, Float32Array, Float64Array, UInt32Array};
fn date_batch(program: &LocalProgram, dates: ArrayRef) -> RecordBatch {
    let count = dates.len();
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            dates,
            Arc::new(Int64Array::from(vec![42; count])),
            Arc::new(BooleanArray::from(vec![true; count])),
        ],
    )
    .unwrap()
}
#[test]
fn date_float_actual_compiler_profiles_sparse_slice_empty_and_native_constants() {
    for target in [DataType::Float32, DataType::Float64] {
        for nullable in [false, true] {
            let source: ArrayRef = Arc::new(Date32Array::from(if nullable {
                vec![
                    Some(-7),
                    Some(0),
                    None,
                    Some(day(2024, 2, 29)),
                    Some(-1),
                    Some(day(-1, 12, 31)),
                ]
            } else {
                vec![
                    Some(-7),
                    Some(0),
                    Some(0),
                    Some(day(2024, 2, 29)),
                    Some(-1),
                    Some(day(-1, 12, 31)),
                ]
            }));
            let source = source.slice(1, 5);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(DataType::Date32, nullable),
                        FunctionValueType::new(target.clone(), nullable),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let batch = date_batch(&program, source.clone());
                    let rows = [0, 1, 3, 4];
                    let selection = Selection::try_sparse(5, &rows).unwrap();
                    let mut evaluator = instance(&program);
                    let snapshot = program
                        .checked()
                        .channels()
                        .expressions()
                        .resolved_calls()
                        .snapshot();
                    let occurrence = ProgramUseRef {
                        arena: root().arena(),
                        use_id: snapshot.bindings()[&root()],
                    };
                    let summary = evaluator.effects[&occurrence];
                    assert!(
                        summary
                            .for_use(summary.context())
                            .unwrap()
                            .may_raise_row_error
                    );
                    let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
                    let old = actual(&source, target.clone(), policy, allow).unwrap();
                    let expected = arrow::compute::take(
                        old.as_ref(),
                        &UInt32Array::from(rows.map(|r| r as u32).to_vec()),
                        None,
                    )
                    .unwrap();
                    assert_eq!(output.values().to_data(), expected.to_data());
                    assert!(output.errors().is_empty());
                    let empty = [];
                    let output = instance(&program)
                        .evaluate(&batch, Selection::try_sparse(5, &empty).unwrap(), &Control)
                        .unwrap();
                    assert_eq!(output.values().data_type(), &target);
                    assert_eq!(output.values().len(), 0);
                    assert!(output.errors().is_empty());
                    let constant = compiled(
                        FunctionValueType::new(DataType::Date32, false),
                        FunctionValueType::new(target.clone(), false),
                        Source::Constant,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let batch = date_batch(&constant, Arc::new(Date32Array::from(vec![0; 5])));
                    let output = instance(&constant)
                        .evaluate(&batch, selection, &Control)
                        .unwrap();
                    let expanded: ArrayRef = Arc::new(Date32Array::from(vec![71; 4]));
                    assert_eq!(
                        output.values().to_data(),
                        original(&expanded, target.clone()).unwrap().to_data()
                    );
                    assert!(output.errors().is_empty());
                }
            }
        }
    }
}
#[test]
fn date_float_actual_compiler_selected_error_origin_null_placeholder_and_unselected_panic() {
    for target in [DataType::Float32, DataType::Float64] {
        let program = compiled(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let source: ArrayRef = Arc::new(Date32Array::new(
            vec![i32::MAX, 0, i32::MIN, i32::MAX, -1].into(),
            Some(arrow_buffer::NullBuffer::from(vec![
                true, true, true, false, true,
            ])),
        ));
        let batch = date_batch(&program, source);
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let result = instance(&program)
            .evaluate(&batch, selection, &Control)
            .unwrap();
        assert_eq!(result.errors().len(), 1);
        assert_eq!(result.errors()[0].selected_ordinal(), 1);
        assert_eq!(
            result.errors()[0].message(),
            format!("CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648")
        );
        let expected: ArrayRef = if target == DataType::Float32 {
            Arc::new(Float32Array::from(vec![
                Some(19700101_i32 as f32),
                None,
                None,
                Some(19691231_i32 as f32),
            ]))
        } else {
            Arc::new(Float64Array::from(vec![
                Some(19700101.0),
                None,
                None,
                Some(19691231.0),
            ]))
        };
        assert_eq!(result.values().to_data(), expected.to_data());
    }
}
#[test]
fn date_float_actual_compiler_every_runtime_cause_keeps_first_failure_without_replay() {
    for target in [DataType::Float32, DataType::Float64] {
        let program = compiled(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let source: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), Some(i32::MIN), None]));
        let batch = date_batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert_eq!(
            instance(&program)
                .evaluate(&batch, Selection::all(3), &recorder)
                .unwrap()
                .errors()
                .len(),
            1
        );
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for at in 1..=trace.len() {
            for cause in causes() {
                let control = CallbackControl::new(cause.clone(), at);
                let mut evaluator = instance(&program);
                assert!(
                    matches!(evaluator.evaluate(&batch,Selection::all(3),&control),Err(error) if error==cause)
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
#[test]
fn date_float_actual_compiler_every_compile_control_refusal_keeps_typed_cause() {
    for target in [DataType::Float32, DataType::Float64] {
        let (functions, package) = fixture(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        compile(&functions, package.clone(), &recorder).unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileCallbacks::new(cause, at);
                assert!(
                    matches!(compile(&functions,package.clone(),&control),Err(FragmentCompileError::Control(error)) if error==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}

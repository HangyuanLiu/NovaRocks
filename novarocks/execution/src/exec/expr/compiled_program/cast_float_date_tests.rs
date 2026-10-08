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

//! Actual LocalCompiler strict float-DATE profiles and native source/first-failure contracts.
use super::*;
use crate::exec::expr::legacy_float_date_cast_baseline_tests::{actual, input};
use arrow::array::Date32Array;
fn float_batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn source_type(wide: bool) -> DataType {
    if wide {
        DataType::Float64
    } else {
        DataType::Float32
    }
}
#[test]
fn float_date_actual_compiler_all_profiles_root_nullability_allow_policies_slice_selection_and_empty()
 {
    for wide in [false, true] {
        for source_nullable in [false, true] {
            for result_nullable in [true, false] {
                if source_nullable && !result_nullable {
                    continue;
                }
                let mut values = vec![
                    Some(f64::NAN),
                    Some(19700102.0),
                    Some(20240230.0),
                    Some(20240229.0),
                    Some(0.0),
                    Some(f64::INFINITY),
                ];
                if source_nullable {
                    values.push(None);
                }
                let padded = input(wide, [vec![Some(999999.0)], values.clone()].concat());
                let source = padded.slice(1, values.len());
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let program = compiled(
                            FunctionValueType::new(source_type(wide), source_nullable),
                            FunctionValueType::new(DataType::Date32, result_nullable),
                            Source::Column,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let batch = float_batch(&program, source.clone());
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
                        let rows = [1, 2, 3, 5];
                        let selection = Selection::try_sparse(values.len(), &rows).unwrap();
                        let result = evaluator.evaluate(&batch, selection, &Control).unwrap();
                        let mut expected = Vec::new();
                        let mut error_ordinals = Vec::new();
                        for (ordinal, row) in selection.iter().enumerate() {
                            match actual(&source.slice(row, 1), policy, allow) {
                                Ok(old) => {
                                    let old = old.as_any().downcast_ref::<Date32Array>().unwrap();
                                    expected.push(if old.is_null(0) {
                                        None
                                    } else {
                                        Some(old.value(0))
                                    });
                                }
                                Err(message) => {
                                    expected.push(None);
                                    error_ordinals.push(ordinal);
                                    assert_eq!(
                                        result
                                            .errors()
                                            .iter()
                                            .find(|e| e.selected_ordinal() == ordinal)
                                            .unwrap()
                                            .message(),
                                        message
                                    );
                                }
                            }
                        }
                        assert_eq!(
                            result
                                .errors()
                                .iter()
                                .map(|e| e.selected_ordinal())
                                .collect::<Vec<_>>(),
                            error_ordinals
                        );
                        assert_eq!(
                            result.values().to_data(),
                            Date32Array::from(expected).to_data()
                        );
                        let empty = [];
                        let result = instance(&program)
                            .evaluate(
                                &batch,
                                Selection::try_sparse(values.len(), &empty).unwrap(),
                                &Control,
                            )
                            .unwrap();
                        assert_eq!(result.values().data_type(), &DataType::Date32);
                        assert_eq!(result.values().len(), 0);
                        assert!(result.errors().is_empty());
                        if source_nullable {
                            let rows = [values.len() - 1];
                            let result = instance(&program)
                                .evaluate(
                                    &batch,
                                    Selection::try_sparse(values.len(), &rows).unwrap(),
                                    &Control,
                                )
                                .unwrap();
                            assert!(result.values().is_null(0));
                            assert!(result.errors().is_empty());
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn float_date_actual_compiler_native_float_constant_negative_zero_is_original_error_not_null() {
    for wide in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                // Existing actual fixture projects Literal(-0 F64) through the native constant author;
                // F32 also consumes its original explicit narrowing cast, never a guessed scalar ordinal.
                let program = compiled(
                    FunctionValueType::new(source_type(wide), false),
                    FunctionValueType::new(DataType::Date32, true),
                    Source::Constant,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = float_batch(&program, input(wide, vec![Some(19700102.0); 4]));
                let rows = [1, 3];
                let selection = Selection::try_sparse(4, &rows).unwrap();
                let result = instance(&program)
                    .evaluate(&batch, selection, &Control)
                    .unwrap();
                let original_source = input(wide, vec![Some(-0.0)]);
                let message = actual(&original_source, policy, allow).unwrap_err();
                assert_eq!(
                    result.values().to_data(),
                    Date32Array::from(vec![None, None]).to_data()
                );
                assert_eq!(result.errors().len(), 2);
                for (ordinal, error) in result.errors().iter().enumerate() {
                    assert_eq!(error.selected_ordinal(), ordinal);
                    assert_eq!(error.message(), message);
                }
            }
        }
    }
}
#[test]
fn float_date_actual_compiler_all_seven_runtime_causes_stop_and_latch_without_replay() {
    for wide in [false, true] {
        let program = compiled(
            FunctionValueType::new(source_type(wide), true),
            FunctionValueType::new(DataType::Date32, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let source = input(wide, vec![Some(19700102.0), Some(f64::NAN), None]);
        let batch = float_batch(&program, source);
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
                let mut evaluator = instance(&program);
                let control = CallbackControl::new(cause.clone(), at);
                assert!(
                    matches!(evaluator.evaluate(&batch,Selection::all(3),&control),Err(actual) if actual==cause)
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
fn float_date_actual_compiler_all_three_compile_causes_preserve_real_callback_prefix() {
    for wide in [false, true] {
        let (functions, package) = fixture(
            FunctionValueType::new(source_type(wide), false),
            FunctionValueType::new(DataType::Date32, false),
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
                    matches!(compile(&functions,package.clone(),&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}

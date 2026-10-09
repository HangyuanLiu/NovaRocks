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

//! Permanent actual LocalCompiler/Frame differential. The two observed distinct
//! profiles are RED before preparation; the identity profile exposes the old
//! pure recipe's omission of original declared precision enforcement.
use super::*;
use crate::exec::expr::legacy_decimal128_rescale_baseline_tests::{actual, input, modes, values};
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
fn compare(p: u8, s: i8, target: DataType, raw: Vec<Option<i128>>) {
    for (policy, allow) in modes() {
        let full = input(p, s, raw.clone());
        let program = compiled(
            FunctionValueType::new(full.data_type().clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for source in [
            full.clone(),
            full.slice(1, full.len() - 1),
            full.slice(0, 0),
        ] {
            let data = batch(&program, source.clone());
            for rows in [
                (0..source.len()).collect::<Vec<_>>(),
                (0..source.len()).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                let out = instance(&program)
                    .evaluate(&data, selection, &Control)
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                let mut expected = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    match actual(source.slice(row, 1), target.clone(), policy, allow) {
                        Ok(array) => expected.push(values(&array)[0]),
                        Err(message) => {
                            expected.push(None);
                            errors.push((ordinal, message));
                        }
                    }
                }
                assert_eq!(values(out.values()), expected);
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
        let constant = compiled(
            FunctionValueType::new(full.data_type().clone(), false),
            FunctionValueType::new(target.clone(), true),
            Source::Constant,
            Wrap::Bare,
            policy,
            allow,
        );
        let cb = batch(&constant, input(p, s, vec![Some(0); 5]));
        let out = instance(&constant)
            .evaluate(&cb, Selection::try_sparse(5, &[1, 4]).unwrap(), &Control)
            .unwrap();
        match actual(input(p, s, vec![Some(1); 2]), target.clone(), policy, allow) {
            Ok(expected) => {
                assert!(out.errors().is_empty());
                assert_eq!(values(out.values()), values(&expected));
            }
            Err(message) => {
                assert_eq!(values(out.values()), vec![None, None]);
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|error| (error.selected_ordinal(), error.message().to_owned()))
                        .collect::<Vec<_>>(),
                    vec![(0, message.clone()), (1, message)]
                );
            }
        }
    }
}
#[test]
fn decimal128_rescale_actual_compiler_observed_7_2_to_9_3_full_policy_and_selection() {
    compare(
        7,
        2,
        DataType::Decimal128(9, 3),
        vec![Some(12000), Some(9000), None, Some(-1)],
    );
}
#[test]
fn decimal128_rescale_actual_compiler_observed_14_13_to_38_13_full_policy_and_selection() {
    compare(
        14,
        13,
        DataType::Decimal128(38, 13),
        vec![Some(1), Some(2), None, Some(-2)],
    );
}
#[test]
fn decimal128_rescale_actual_compiler_identical_metadata_full_carrier_precision_is_not_identity() {
    compare(
        1,
        0,
        DataType::Decimal128(1, 0),
        vec![
            Some(i128::MIN),
            Some(i128::MAX),
            None,
            Some(9),
            Some(10),
            Some(-10),
        ],
    );
}

#[test]
fn decimal128_rescale_actual_compiler_full_rescale_rounding_overflow_scale_and_null_profiles() {
    for (p, s, tp, ts) in [
        (38, 0, 38, 1),
        (10, 4, 10, 2),
        (18, -2, 20, 0),
        (18, 2, 20, -2),
        (38, 0, 38, -38),
        (38, 38, 38, -38),
        (1, -38, 38, 38),
    ] {
        compare(
            p,
            s,
            DataType::Decimal128(tp, ts),
            vec![
                Some(i128::MIN),
                Some(i128::MAX),
                None,
                Some(3185),
                Some(-3185),
                Some(1),
            ],
        );
    }
}
#[test]
fn decimal128_rescale_actual_compiler_every_seven_callback_causes_stop_and_latch_without_replay() {
    for (p, s, tp, ts, policy, allow) in [
        (7, 2, 9, 3, DecimalOverflowPolicy::OutputNull, false),
        (1, 0, 1, 0, DecimalOverflowPolicy::ReportError, true),
        (38, 38, 38, -38, DecimalOverflowPolicy::OutputNull, false),
    ] {
        let source = input(p, s, vec![Some(1), Some(i128::MAX), None, Some(-1)]);
        let program = compiled(
            FunctionValueType::new(source.data_type().clone(), true),
            FunctionValueType::new(DataType::Decimal128(tp, ts), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        let data = batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        instance(&program)
            .evaluate(&data, Selection::all(4), &recorder)
            .unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.iter().all(|n| *n <= 256));
        for stop in 1..=trace.len() {
            for cause in causes() {
                let mut frame = instance(&program);
                let control = CallbackControl::new(cause.clone(), stop);
                assert!(
                    matches!(frame.evaluate(&data,Selection::all(4),&control),Err(actual) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    frame.evaluate(&data, Selection::all(4), &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

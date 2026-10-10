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

//! Full admitted successful-value shape matrix and real Frame fault/latch probes.
use super::*;
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use std::{sync::Mutex, time::Duration};
fn compare_profile(dtype: DataType, p: u8, s: i8) {
    let (min, max) = bound(&dtype);
    let target = DataType::Decimal128(p, s);
    for (policy, allow) in modes() {
        let full = integral_input(&dtype, vec![Some(min), None, Some(max), Some(1), Some(-1)]);
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for source in [full.clone(), full.slice(1, 4), full.slice(0, 0)] {
            let n = source.len();
            let b = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                vec![
                    source.clone(),
                    Arc::new(Int64Array::from(vec![42; n])),
                    Arc::new(BooleanArray::from(vec![true; n])),
                ],
            )
            .unwrap();
            for rows in [
                (0..n).collect::<Vec<_>>(),
                (0..n).filter(|r| r % 2 == 0).collect(),
                // Overflowing inactive extrema cannot leak into this invocation.
                (0..n).filter(|r| r % 2 == 1).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(n, &rows).unwrap();
                let output = instance(&program)
                    .evaluate(&b, selection, &Control)
                    .unwrap();
                assert_eq!(output.selection(), selection);
                assert_eq!(output.values().data_type(), &target);
                let mut expected = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    match actual(source.slice(row, 1), target.clone(), policy, allow) {
                        Ok(a) => expected.push(values(&a)[0]),
                        Err(message) => {
                            expected.push(None);
                            errors.push((ordinal, message));
                        }
                    }
                }
                assert_eq!(values(output.values()), expected);
                assert_eq!(
                    output
                        .errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}
#[test]
fn integral_decimal128_actual_after_full_width_precision_scale_policy_sparse_and_empty() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for (p, s) in [
            (3, 0),
            (4, 0),
            (7, 2),
            (7, -2),
            (18, 1),
            (19, 1),
            (38, 1),
            (38, 38),
            (38, -38),
        ] {
            compare_profile(dtype.clone(), p, s);
        }
    }
}
#[test]
fn integral_decimal128_actual_after_original_i64_constant_broadcast_exact_source() {
    for (policy, allow) in modes() {
        let target = DataType::Decimal128(4, 0);
        let program = compiled(
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(target.clone(), true),
            Source::Constant,
            Wrap::Bare,
            policy,
            allow,
        );
        let b = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            vec![
                integral_input(&DataType::Int64, vec![Some(9999); 5]),
                Arc::new(Int64Array::from(vec![42; 5])),
                Arc::new(BooleanArray::from(vec![true; 5])),
            ],
        )
        .unwrap();
        let output = instance(&program)
            .evaluate(&b, Selection::try_sparse(5, &[1, 4]).unwrap(), &Control)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(values(output.values()), vec![Some(71), Some(71)]);
    }
}
struct Callback {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        assert!(t.len() < self.stop, "no callback after first refusal");
        t.push(n);
        if t.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integral Decimal CAST never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("integral-frame-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("integral-frame-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("integral-frame-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn integral_decimal128_actual_after_every_frame_callback_keeps_seven_causes_and_no_replay() {
    let program = compiled(
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Decimal128(3, 0), true),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let b = RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            integral_input(&DataType::Int64, vec![Some(1000), None, Some(99)]),
            Arc::new(Int64Array::from(vec![42; 3])),
            Arc::new(BooleanArray::from(vec![true; 3])),
        ],
    )
    .unwrap();
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        instance(&program).evaluate(&b, selection, &record).unwrap();
        let trace = record.trace.lock().unwrap().clone();
        for stop in 1..=trace.len() {
            for cause in causes() {
                let c = Callback::new(stop, cause.clone());
                let mut frame = instance(&program);
                assert!(matches!(frame.evaluate(&b,selection,&c),Err(actual) if actual==cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                let after = Callback::new(usize::MAX, KernelFailure::Cancelled);
                assert!(matches!(
                    frame.evaluate(&b, selection, &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

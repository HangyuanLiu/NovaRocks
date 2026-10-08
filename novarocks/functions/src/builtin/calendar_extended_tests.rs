// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::calendar_extended_owner::prepared_for_test_with_policy;
use super::*;
use crate::{EvaluatedArgument, ScalarEvaluationInstance, Selection};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::{sync::Mutex, time::Duration as StdDuration};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal.as_ref() {
            assert!(at <= *stop, "callback after refusal");
        }
        trace.push(units);
        match self.refusal.as_ref() {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: StdDuration) -> Result<(), KernelFailure> {
        panic!("calendar owner never waits")
    }
}
fn prepared(name: &str, arrays: &[ArrayRef]) -> Arc<dyn crate::PreparedScalarKernel> {
    let types: Vec<_> = arrays
        .iter()
        .map(|array| FunctionValueType::new(array.data_type().clone(), true))
        .collect();
    prepared_for_test_with_policy(name, &types, DecimalOverflowPolicy::OutputNull).unwrap()
}
struct OwnedOutput {
    values: ArrayRef,
    errors: Box<[RowDataError]>,
}
impl OwnedOutput {
    fn values(&self) -> &ArrayRef {
        &self.values
    }
    fn errors(&self) -> &[RowDataError] {
        &self.errors
    }
}
fn run(
    prepared: Arc<dyn crate::PreparedScalarKernel>,
    arrays: &[ArrayRef],
    selection: Selection<'_>,
    control: &Control,
) -> Result<OwnedOutput, KernelFailure> {
    let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let output = instance.evaluate(selection, &arguments, control)?;
    let (_, values, errors) = output.into_parts();
    Ok(OwnedOutput { values, errors })
}

#[test]
fn calendar_extended_sparse_truncation_preserves_original_batch_error_responsibility() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some("bad"),
            Some("year"),
            Some("bad"),
            None,
            Some("millisecond"),
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(-1),
        ])),
    ];
    let kernel = prepared("date_trunc", &arrays);
    let selected = [0usize, 1, 3, 4];
    let output = run(
        kernel,
        &arrays,
        Selection::try_sparse(5, &selected).unwrap(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert_eq!(output.errors()[0].message(), TRUNC_ERROR);
    let values = output
        .values()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![None, Some(0), None, Some(-1000)]
    );
}
#[test]
fn calendar_extended_control_refusal_keeps_exact_cause_and_actual_prefix() {
    for invalid_unit in [false, true] {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                if invalid_unit {
                    "bad"
                } else {
                    "year"
                };
                2
            ])),
            Arc::new(StringArray::from(vec![Some("2024-02-29 12:34:56"), None])),
        ];
        let kernel = prepared("date_trunc", &arrays);
        let baseline = Control::default();
        run(kernel.clone(), &arrays, Selection::all(2), &baseline).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.iter().any(|units| *units > 0));
        for stop in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(
                    matches!(run(kernel.clone(), &arrays, Selection::all(2), &control), Err(error) if error == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn calendar_extended_large_selected_loop_observes_actual_quantum() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![Some(0); 320])),
        Arc::new(Date32Array::from(vec![Some(1); 320])),
    ];
    let control = Control::default();
    let output = run(
        prepared("hours_diff", &arrays),
        &arrays,
        Selection::all(320),
        &control,
    )
    .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -24
    );
    // Each opaque date conversion flushes its own genuine small work tail;
    // a full quantum is therefore not mandatory on this profile.
    let trace = control.trace.lock().unwrap();
    assert!(trace.iter().map(|units| *units as usize).sum::<usize>() >= 320);
}

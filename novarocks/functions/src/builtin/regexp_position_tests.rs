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
use super::*;
use crate::{EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance};

use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    reject: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("XXH3 calculation never waits")
    }

    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.reject {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = &self.reject {
            if at == *stop {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
}

fn prepared(sources: &[FunctionValueType]) -> Arc<dyn crate::PreparedScalarKernel> {
    super::super::regexp_position_owner::tests::prepared_for_test("regexp_position", sources)
        .unwrap()
}
#[test]
fn regexp_position_actual_instance_every_checkpoint_preserves_seven_causes_and_latch() {
    let text = "é中a\0".repeat(129);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            None,
            Some("中a中a"),
            Some("skip"),
        ])),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("(masked"),
            Some("(invalid"),
            Some("(outside"),
        ])),
        Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1), Some(1)])),
        Arc::new(Int64Array::from(vec![Some(128), Some(1), Some(1), Some(1)])),
    ];
    let sources = arrays
        .iter()
        .map(|a| FunctionValueType::new(a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let prepared = prepared(&sources);
    let rows = [0, 1, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let probe = Control::default();
    let actual = ScalarEvaluationInstance::instantiate(prepared.clone())
        .unwrap()
        .evaluate(selection, &arguments, &probe)
        .unwrap();
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        511
    );
    assert_eq!(actual.errors().len(), 1);
    assert_eq!(actual.errors()[0].selected_ordinal(), 2);
    let trace = probe.trace.into_inner().unwrap();
    assert!(trace.len() > 8);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ] {
        for at in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                reject: Some((at, cause.clone())),
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn regexp_position_shared_legacy_keeps_raw_int32_null_and_full_error() {
    let long = format!("{}(", "a".repeat(900));
    let arrays = vec![
        Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some(long.as_str())])),
        Arc::new(Int32Array::from(vec![1])),
    ];
    let expected = format!(
        "Invalid regex expression: {long}. Detail message: {}",
        Regex::new(&long).unwrap_err()
    );
    assert!(expected.len() > 512);
    assert_eq!(evaluate_legacy(&arrays, 1).unwrap_err(), expected);
    let arrays = vec![
        arrays[0].clone(),
        arrays[1].clone(),
        Arc::new(Int32Array::from(vec![-1])),
    ];
    assert_eq!(
        evaluate_legacy(&arrays, 1).unwrap().to_data(),
        Int32Array::from(vec![-1]).to_data()
    );
}
#[test]
fn regexp_position_each_carrier_has_its_own_compact_scalar_and_constant_address() {
    let source = Arc::new(StringArray::from(vec![
        "outside", "中a中a", "outside", "中a中a",
    ])) as ArrayRef;
    let pattern = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
    let starts = Arc::new(Int64Array::from(vec![1, 3])) as ArrayRef;
    let occurrence = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact =
        SelectedValues::try_new(selection, &DataType::Int64, starts, Box::default()).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&pattern),
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Scalar(&occurrence),
    ];
    let sources = arguments
        .iter()
        .map(|a| FunctionValueType::new(a.array().data_type().clone(), false))
        .collect::<Vec<_>>();
    let out = ScalarEvaluationInstance::instantiate(prepared(&sources))
        .unwrap()
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(
        out.values().to_data(),
        Int32Array::from(vec![2, 4]).to_data()
    );
}

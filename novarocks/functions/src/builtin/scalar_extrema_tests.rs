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
use crate::kernel_control::{internal, invalid};
use std::sync::atomic::{AtomicUsize, Ordering};
struct Control {
    calls: AtomicUsize,
    reject: Option<usize>,
    cause: KernelFailure,
}
impl KernelEvaluationControl for Control {
    fn wait(&self, _duration: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("extrema evaluation must not request a wait")
    }

    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if self.reject == Some(call) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
fn run(arrays: &[ArrayRef], control: &Control) -> Result<ArrayRef, KernelFailure> {
    let args = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let mut work = EvaluationCheckpoints::new(control);
    let result = evaluate_values(
        ExtremaOperation::Greatest,
        &args,
        Selection::all(arrays[0].len()),
        Some(arrays[0].data_type()),
        false,
        &mut work,
        None,
    );
    work.finish_result(result)
}
#[test]
fn extrema_selected_all_seven_control_causes_preserve_first_refusal() {
    let rows = (0..1025).map(|i| Some(i as f64)).collect::<Vec<_>>();
    let text = "invalid_datetime_".to_string() + &"x".repeat(4097);
    let inputs = vec![
        vec![Arc::new(Float64Array::from(rows)) as ArrayRef],
        vec![Arc::new(StringArray::from(vec![text.as_str(), "20260115"])) as ArrayRef],
    ];
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("first invalid"),
        internal("first internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("first operational")),
        KernelFailure::InstanceFailed,
    ];
    for arrays in inputs {
        let probe = Control {
            calls: AtomicUsize::new(0),
            reject: None,
            cause: KernelFailure::Cancelled,
        };
        run(&arrays, &probe).unwrap();
        let checkpoints = probe.calls.load(Ordering::Relaxed);
        assert!(checkpoints > 4);
        for cause in &causes {
            for reject in 0..checkpoints {
                let control = Control {
                    calls: AtomicUsize::new(0),
                    reject: Some(reject),
                    cause: cause.clone(),
                };
                assert_eq!(run(&arrays, &control).unwrap_err(), *cause);
                assert_eq!(control.calls.load(Ordering::Relaxed), reject + 1);
            }
        }
    }
}
#[test]
fn extrema_core_legacy_error_callback_preserves_complete_source_type() {
    let field = "long_field_".to_string() + &"x".repeat(1025);
    let ty = DataType::Struct(vec![arrow_schema::Field::new(field, DataType::Int64, true)].into());
    let arrays = [arrow_array::new_null_array(&ty, 1)];
    for operation in [ExtremaOperation::Greatest, ExtremaOperation::Least] {
        let error = evaluate_legacy(operation, &arrays, 1, Some(&DataType::Float64)).unwrap_err();
        assert_eq!(error, format!("unsupported numeric type: {ty:?}"));
        assert!(error.len() > 512);
    }
}

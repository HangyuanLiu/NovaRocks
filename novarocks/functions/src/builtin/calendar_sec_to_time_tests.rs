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
//! Exact installed formatting profile and originating control failures.
use super::super::calendar_sec_to_time_owner::prepared_for_test_with_policy;
use super::*;
use crate::{ScalarEvaluationInstance, kernel_control::KernelDiagnostic};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Mutex;
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
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("sec_to_time never waits")
    }
}

fn instance(nullable: bool) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "sec_to_time",
            &FunctionValueType::new(DataType::Int64, nullable),
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn sec_to_time_sparse_actual_profile_has_no_environment_or_row_errors() {
    let prepared = prepared_for_test_with_policy(
        "sec_to_time",
        &FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    assert!(prepared.contract().effects().environment.is_empty());
    let values = Arc::new(Int64Array::from(vec![
        Some(1),
        None,
        Some(i64::MIN),
        Some(i64::MAX),
    ])) as ArrayRef;
    let args = [EvaluatedArgument::Column(&values)];
    let rows = [1, 2, 3];
    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let result = kernel
        .evaluate(
            Selection::try_sparse(4, &rows).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
    assert!(result.errors().is_empty());
    assert_eq!(
        result.values().to_data(),
        StringArray::from(vec![None, Some("-839:59:59"), Some("839:59:59")]).to_data()
    );
}
#[test]
fn sec_to_time_every_actual_checkpoint_keeps_first_cause_and_permanent_latch() {
    let values = Arc::new(Int64Array::from(
        (0..257)
            .map(|x| if x % 13 == 0 { None } else { Some(x - 128) })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let args = [EvaluatedArgument::Column(&values)];
    let good = Control::default();
    instance(true)
        .evaluate(Selection::all(257), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational")),
            KernelFailure::InstanceFailed,
        ] {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut kernel = instance(true);
            assert_eq!(
                kernel
                    .evaluate(Selection::all(257), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(257), &args, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

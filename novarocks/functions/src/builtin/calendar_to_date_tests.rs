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

//! Exact added to_date owner profiles and first-control-failure behavior.
use super::super::date_owner::prepared_for_test_with_policy;
use super::*;
use crate::{
    EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance, Selection,
    kernel_control::KernelDiagnostic,
};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::{sync::Mutex, time::Duration};
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
        panic!("date extraction never waits")
    }
}
fn instance(ty: FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("to_date", &ty, DecimalOverflowPolicy::OutputNull).unwrap(),
    )
    .unwrap()
}
#[test]
fn to_date_supported_profiles_keep_ranges_nulls_sparse_and_no_environment() {
    for values in [
        Arc::new(StringArray::from(vec![
            Some("bad"),
            None,
            Some("20240229"),
            Some("1969-12-31 23:59:59.999999"),
        ])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(i64::MAX),
            None,
            Some(0),
            Some(-1),
        ])) as ArrayRef,
    ] {
        let ty = FunctionValueType::new(values.data_type().clone(), true);
        let prepared =
            prepared_for_test_with_policy("to_date", &ty, DecimalOverflowPolicy::OutputNull)
                .unwrap();
        assert_eq!(
            prepared.contract().function_id().as_str(),
            "builtin.scalar/to_date/v1"
        );
        assert!(prepared.contract().effects().environment.is_empty());
        let args = [EvaluatedArgument::Column(&values)];
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let rows = [1, 3];
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
            Date32Array::from(vec![None, Some(-1)]).to_data()
        );
    }
}
#[test]
fn to_date_every_actual_parser_checkpoint_preserves_seven_typed_causes_and_latch() {
    let separators = format!("1970{}-01-01", " ".repeat(320));
    let values = Arc::new(StringArray::from(vec![
        Some(separators.as_str()),
        None,
        Some("bad"),
    ])) as ArrayRef;
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let args = [EvaluatedArgument::Column(&values)];
    let good = Control::default();
    instance(ty.clone())
        .evaluate(Selection::all(3), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
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
            let mut kernel = instance(ty.clone());
            assert_eq!(
                kernel
                    .evaluate(Selection::all(3), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(3), &args, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
#[test]
fn to_date_int64_declared_overload_is_explicitly_refused_during_prepare() {
    let refusal = prepared_for_test_with_policy(
        "to_date",
        &FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
    )
    .err()
    .expect("the admitted Int64 signature has no legacy reader");
    assert!(
        refusal
            .to_string()
            .contains("to_date declared Int64 profile has no legacy date reader"),
        "{refusal}"
    );
}

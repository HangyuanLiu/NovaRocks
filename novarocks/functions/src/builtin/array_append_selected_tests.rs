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

//! Exact array selected addressing and actual original-control refusal points.
use super::super::array_append_owner::tests::prepared_with_control;
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, StringArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop)
        }
        t.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("collection kernels never wait")
    }
}
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop)
        }
        t.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::new(a.data_type().clone(), true)
}
fn list(values: ArrayRef, offsets: Vec<i32>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}
fn prepared(
    types: &[FunctionValueType],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control("array_append", types, control)
}
#[test]
fn array_append_independent_sparse_scalar_compact_mappings_keep_parent_null_and_target_null() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let a: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()),
        values,
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    let target: ArrayRef = Arc::new(Int32Array::from(vec![Some(99)]));
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Scalar(&target),
    ];
    let out = ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let output = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 0, 2]);
    assert!(output.is_null(0));
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4), Some(99)]
    );
    let compact: ArrayRef = Arc::new(Int32Array::from(vec![Some(8), None]));
    let selected =
        SelectedValues::try_new(selection, &DataType::Int32, compact, Box::new([])).unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::SelectedColumn(&selected),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let output = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 0, 2]);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4), None]
    );
    assert!(out.errors().is_empty());
    let one = a.slice(0, 1);
    let target: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), None, Some(8), Some(7)]));
    let p = prepared(
        &[source(&one), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Scalar(&one),
        EvaluatedArgument::Column(&target),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let output = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), None, Some(1), Some(7)]
    );
}
#[test]
fn array_append_actual_owned_work_over_256_and_nested_shared_copy_are_observed() {
    let child = list(Arc::new(Int32Array::from(vec![1, 2])), vec![0, 1, 2]);
    let a = list(child.clone(), vec![0, 1, 2]);
    let target = child.slice(1, 1);
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Scalar(&target),
    ];
    let c = Control::default();
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(2), &args, &c)
        .unwrap();
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0, 2, 4]
    );
    assert!(!c.trace.lock().unwrap().is_empty());
    let a = list(
        Arc::new(Int32Array::from(vec![1; 257])),
        (0..=257).collect(),
    );
    let target: ArrayRef = Arc::new(Int32Array::from(vec![Some(2)]));
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Scalar(&target),
    ];
    let c = Control::default();
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(257), &args, &c)
        .unwrap();
    assert_eq!(out.values().len(), 257);
    assert!(c.trace.lock().unwrap().len() >= 257);
}
#[test]
fn array_append_every_actual_runtime_checkpoint_preserves_seven_causes_and_failed_latch() {
    let a = list(
        Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("long payload"),
        ])),
        vec![0, 1, 2, 3],
    );
    let target: ArrayRef = Arc::new(StringArray::from(vec![None, Some("y"), Some("z")]));
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&target),
    ];
    let good = Control::default();
    ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(Selection::all(3), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
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
            let c = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
            assert_eq!(k.evaluate(Selection::all(3), &args, &c).unwrap_err(), cause);
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                k.evaluate(Selection::all(3), &args, &after).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
#[test]
fn array_append_every_actual_compile_callback_preserves_three_causes_and_named_refusal_tail() {
    let a = list(Arc::new(Int32Array::from(vec![1])), vec![0, 1]);
    for types in [
        vec![source(&a), FunctionValueType::new(DataType::Int32, true)],
        vec![source(&a), FunctionValueType::new(DataType::Int32, false)],
        vec![FunctionValueType::new(DataType::Int32, true); 2],
    ] {
        let good = Compile::default();
        let success = types[0].data_type == *a.data_type();
        assert_eq!(prepared(&types, &good).is_ok(), success);
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Compile {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause)),
                };
                let error = prepared(&types, &c).err().unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(c) => Some(c),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                };
                assert_eq!(actual, Some(cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[path = "array_append_multi_tests.rs"]
mod multi;

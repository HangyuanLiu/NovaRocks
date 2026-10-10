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
use super::super::array_element_at_owner::tests::prepared_with_control;
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray};
use arrow_buffer::OffsetBuffer;
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
    prepared_with_control("__array_element_at", types, control)
}
#[test]
fn array_element_selected_independent_column_scalar_compact_index_and_scalar_list() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
    let a = list(values, vec![0, 2, 4, 6, 8]);
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![2]));
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let p = prepared(
        &[source(&a), source(&scalar)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Scalar(&scalar),
    ];
    let out = ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4), Some(8)]
    );
    let compact: ArrayRef = Arc::new(Int64Array::from(vec![1, 0]));
    let selected =
        SelectedValues::try_new(selection, &DataType::Int64, compact, Box::new([])).unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::SelectedColumn(&selected),
    ];
    let out = ScalarEvaluationInstance::instantiate(p.clone())
        .unwrap()
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), None]
    );
    let scalar_list = a.slice(0, 1);
    let index: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 0, -1]));
    let args = [
        EvaluatedArgument::Scalar(&scalar_list),
        EvaluatedArgument::Column(&index),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(4), &args, &Control::default())
        .unwrap();
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), None, None]
    );
}
#[test]
fn array_element_original_safe_i64_projection_and_utf8_quote_rules_have_no_row_errors() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        "\"abc\"", "\"a\\b\"", "\"\"", "plain", "\"é\"",
    ]));
    let a = list(values, vec![0, 1, 2, 3, 4, 5]);
    let idx: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, i64::MAX, i64::MIN, 1]));
    let p = prepared(&[source(&a), source(&idx)], crate::binding_test_control()).unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&idx),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(5), &args, &Control::default())
        .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("abc"), Some("\"a\\b\""), None, None, Some("é")]
    );
}
#[test]
fn array_element_every_actual_runtime_checkpoint_preserves_seven_causes_and_failed_latch() {
    let long = format!("\"{}\"", "é".repeat(400));
    let values: ArrayRef = Arc::new(StringArray::from(
        (0..257)
            .map(|r| {
                if r % 7 == 0 {
                    None
                } else {
                    Some(long.as_str())
                }
            })
            .collect::<Vec<_>>(),
    ));
    let a = list(values, (0..=257).collect());
    for index_type in [DataType::Int32, DataType::Int64] {
        let idx = arrow_cast::cast(
            &(Arc::new(Int32Array::from(vec![1; 257])) as ArrayRef),
            &index_type,
        )
        .unwrap();
        let p = prepared(&[source(&a), source(&idx)], crate::binding_test_control()).unwrap();
        let args = [
            EvaluatedArgument::Column(&a),
            EvaluatedArgument::Column(&idx),
        ];
        let good = Control::default();
        ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(Selection::all(257), &args, &good)
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
                let c = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                assert_eq!(
                    k.evaluate(Selection::all(257), &args, &c).unwrap_err(),
                    cause
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    k.evaluate(Selection::all(257), &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn array_element_every_actual_compile_callback_preserves_three_causes_and_named_refusal_tail() {
    let a = list(Arc::new(Int32Array::from(vec![1])), vec![0, 1]);
    for types in [
        vec![source(&a), FunctionValueType::new(DataType::Int32, true)],
        vec![source(&a), FunctionValueType::new(DataType::Int64, true)],
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

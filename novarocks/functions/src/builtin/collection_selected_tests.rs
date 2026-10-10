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

//! Actual selected ABI mapping, required row errors and first-cause controls.
use super::super::{array_literal_owner::tests as literal, map_element_at_owner::tests as access};
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray, UInt64Array,
};
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
fn prepared(
    op: Operation,
    types: &[FunctionValueType],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, FunctionSpecializationFailure> {
    match op {
        Operation::ArrayLiteral => {
            literal::prepared_with_control("__array_literal", types, control)
        }
        Operation::MapLookup => access::prepared_with_control("__map_element_at", types, control),
    }
}
fn map(keys: ArrayRef, values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    let fs = vec![
        Arc::new(Field::new("key", keys.data_type().clone(), true)),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ];
    let e = StructArray::new(fs.into(), vec![keys, values], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", e.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        e,
        valid.map(NullBuffer::from),
        false,
    ))
}
fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::new(a.data_type().clone(), true)
}
#[test]
fn collection_independent_column_scalar_compact_mappings_preserve_null_key_and_constructor_order() {
    let column: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)]));
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![Some(8)]));
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(40)]));
    let selected =
        SelectedValues::try_new(selection, &DataType::Int32, compact, Box::new([])).unwrap();
    let p = prepared(
        Operation::ArrayLiteral,
        &[
            source(&column),
            source(&scalar),
            FunctionValueType::new(DataType::Int32, true),
        ],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&column),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::SelectedColumn(&selected),
    ];
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(out.errors().is_empty());
    let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 3, 6]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(8), Some(10), Some(4), Some(8), Some(40)]
    );
    let m = map(
        Arc::new(Int32Array::from(vec![Some(1), None, None, Some(4), None])),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        vec![0, 1, 3, 4, 5],
        None,
    );
    let key: ArrayRef = Arc::new(Int32Array::from(vec![None]));
    let args = [
        EvaluatedArgument::Column(&m),
        EvaluatedArgument::Scalar(&key),
    ];
    let p = prepared(
        Operation::MapLookup,
        &[source(&m), source(&key)],
        crate::binding_test_control(),
    )
    .unwrap();
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), Some(5)]
    );
}
#[test]
fn map_lookup_original_unsupported_domain_is_bounded_rowdata_and_undemanded_errors_eliminate() {
    let m = map(
        Arc::new(UInt64Array::from(vec![Some(1), None])),
        Arc::new(Int32Array::from(vec![7, 8])),
        vec![0, 1, 2, 2],
        Some(vec![true, true, false]),
    );
    let key: ArrayRef = Arc::new(UInt64Array::from(vec![Some(1), None, Some(2)]));
    let args = [
        EvaluatedArgument::Column(&m),
        EvaluatedArgument::Column(&key),
    ];
    let p = prepared(
        Operation::MapLookup,
        &[source(&m), source(&key)],
        crate::binding_test_control(),
    )
    .unwrap();
    let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
    let out = k
        .evaluate(Selection::all(3), &args, &Control::default())
        .unwrap();
    assert_eq!(out.errors().len(), 1);
    assert_eq!(out.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        out.errors()[0].message(),
        "map key compare unsupported type: UInt64"
    );
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(8), None]
    );
    let rows = [1, 2];
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let out = k
        .evaluate(
            Selection::try_sparse(3, &rows).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
    assert!(out.errors().is_empty());
}
#[test]
fn collection_every_actual_runtime_checkpoint_preserves_seven_causes_quantum_and_failed_latch() {
    let text: ArrayRef = Arc::new(StringArray::from(
        (0..257)
            .map(|r| {
                if r % 7 == 0 {
                    None
                } else {
                    Some("é\0payload")
                }
            })
            .collect::<Vec<_>>(),
    ));
    let m = map(
        Arc::new(Int32Array::from_iter_values(0..300)),
        Arc::new(Int32Array::from_iter_values(0..300)),
        vec![0, 300],
        None,
    );
    let miss: ArrayRef = Arc::new(Int32Array::from(vec![-1]));
    let unsupported = map(
        Arc::new(UInt64Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![2])),
        vec![0, 1],
        None,
    );
    let uk: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
    for (op, columns, rows) in [
        (Operation::ArrayLiteral, vec![text], 257),
        (Operation::MapLookup, vec![m, miss], 1),
        (Operation::MapLookup, vec![unsupported, uk], 1),
    ] {
        let types = columns.iter().map(source).collect::<Vec<_>>();
        let p = prepared(op, &types, crate::binding_test_control()).unwrap();
        let args = columns
            .iter()
            .map(EvaluatedArgument::Column)
            .collect::<Vec<_>>();
        let good = Control::default();
        ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(Selection::all(rows), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if rows > 1 {
            assert!(trace.contains(&256));
        }
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
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                assert_eq!(
                    k.evaluate(Selection::all(rows), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    k.evaluate(Selection::all(rows), &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn collection_every_actual_compile_checkpoint_preserves_three_causes_and_ordinary_refusal_tail() {
    let scalar = FunctionValueType::new(DataType::Int32, true);
    let m = map(
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![2])),
        vec![0, 1],
        None,
    );
    for (op, types, success) in [
        (Operation::ArrayLiteral, vec![], true),
        (Operation::ArrayLiteral, vec![scalar.clone(); 3], true),
        (Operation::MapLookup, vec![source(&m), scalar.clone()], true),
        (Operation::MapLookup, vec![scalar.clone(), scalar], false),
    ] {
        let good = Compile::default();
        assert_eq!(prepared(op, &types, &good).is_ok(), success);
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
                let error = prepared(op, &types, &c).err().unwrap();
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

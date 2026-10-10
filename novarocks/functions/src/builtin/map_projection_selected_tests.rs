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

//! Exact map projection selected addressing and actual original-control refusal points.
use super::super::map_projection_owner::tests::prepared_with_control;
use super::*;
use crate::{
    EvaluatedArgument, FunctionSpecializationFailure, KernelDiagnostic, ScalarEvaluationInstance,
    SelectedValues, Selection,
};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray, UInt32Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
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

fn fixture(unsupported: bool) -> ArrayRef {
    let keys: ArrayRef = if unsupported {
        Arc::new(UInt32Array::from(vec![2, 1, 3, 2, 1]))
    } else {
        Arc::new(Int32Array::from(vec![2, 1, 3, 2, 1]))
    };
    let vals: ArrayRef = Arc::new(StringArray::from(vec![
        Some("two"),
        Some("one"),
        None,
        Some("two-again"),
        Some("one-again"),
    ]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", keys.data_type().clone(), false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into(),
        vec![keys, vals],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 2, 3, 3, 5].into()),
        entries,
        Some(NullBuffer::from(vec![true, true, false, true])),
        true,
    ))
}
fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::new(a.data_type().clone(), true)
}
fn prepared(
    name: &str,
    t: &FunctionValueType,
    c: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control(name, std::slice::from_ref(t), c)
}
#[test]
fn map_parts_actual_generic_unsupported_key_only_demanded_comparison_is_row_error() {
    let a = fixture(true);
    let column_argument = EvaluatedArgument::Column(&a);
    for name in ["map_keys", "map_values"] {
        let p = prepared(name, &source(&a), crate::binding_test_control()).unwrap();
        let rows = [0, 1, 2];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let out = ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(
                selection,
                std::slice::from_ref(&column_argument),
                &Control::default(),
            )
            .unwrap();
        assert_eq!(
            out.errors(),
            &[crate::RowDataError::new(
                0,
                "map key ordered compare unsupported type: UInt32"
            )]
        );
        let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 0, 1, 1]);
        assert!(list.is_null(0));
        assert!(!list.is_null(1));
        assert!(list.is_null(2));
        let good = [1, 2];
        let selection = Selection::try_sparse(4, &good).unwrap();
        let out = ScalarEvaluationInstance::instantiate(p)
            .unwrap()
            .evaluate(
                selection,
                std::slice::from_ref(&column_argument),
                &Control::default(),
            )
            .unwrap();
        assert!(out.errors().is_empty());
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_offsets(),
            &[0, 1, 1]
        );
    }
}
#[test]
fn map_parts_sparse_compact_slices_scalar_and_empty_consume_real_row_mapping() {
    let a = fixture(false);
    let column_argument = EvaluatedArgument::Column(&a);
    for name in ["map_keys", "map_values"] {
        let p = prepared(name, &source(&a), crate::binding_test_control()).unwrap();
        let rows = [0, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let taken =
            arrow_select::take::take(a.as_ref(), &UInt32Array::from(vec![0, 2, 3]), None).unwrap();
        let compact =
            SelectedValues::try_new(selection, a.data_type(), taken, Box::default()).unwrap();
        for arg in [
            EvaluatedArgument::Column(&a),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let out = ScalarEvaluationInstance::instantiate(p.clone())
                .unwrap()
                .evaluate(selection, std::slice::from_ref(&arg), &Control::default())
                .unwrap();
            let out = out.values().as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(out.value_offsets(), &[0, 2, 2, 4]);
            assert!(out.is_null(1));
        }
        let scalar = a.slice(1, 1);
        let scalar_argument = EvaluatedArgument::Scalar(&scalar);
        let out = ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(
                selection,
                std::slice::from_ref(&scalar_argument),
                &Control::default(),
            )
            .unwrap();
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_offsets(),
            &[0, 1, 2, 3]
        );
        assert!(
            ScalarEvaluationInstance::instantiate(p)
                .unwrap()
                .evaluate(
                    Selection::try_sparse(4, &[]).unwrap(),
                    std::slice::from_ref(&column_argument),
                    &Control::default()
                )
                .unwrap()
                .values()
                .is_empty()
        );
    }
}
#[test]
fn map_parts_every_actual_owned_callback_seven_causes_preserve_prefix_latch() {
    for unsupported in [false, true] {
        let a = fixture(unsupported);
        for name in ["map_keys", "map_values"] {
            let p = prepared(name, &source(&a), crate::binding_test_control()).unwrap();
            let args = [EvaluatedArgument::Column(&a)];
            let good = Control::default();
            ScalarEvaluationInstance::instantiate(p.clone())
                .unwrap()
                .evaluate(Selection::all(4), &args, &good)
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
                    let c = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                    assert_eq!(k.evaluate(Selection::all(4), &args, &c).unwrap_err(), cause);
                    assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        k.evaluate(Selection::all(4), &args, &after).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.trace.lock().unwrap().is_empty());
                }
            }
        }
    }
}
#[test]
fn map_parts_every_actual_compile_callback_three_causes_preserve_prefix() {
    let a = fixture(true);
    for name in ["map_keys", "map_values"] {
        let source = source(&a);
        let good = Compile::default();
        prepared(name, &source, &good).unwrap();
        let trace = good.trace.lock().unwrap().clone();
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
                let error = prepared(name, &source, &c).err().unwrap();
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

#[test]
fn map_parts_real_constant_pool_ordinal_and_required_row_error_domain() {
    let a = fixture(true);
    let source = source(&a);
    let policy = crate::ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1048576,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4194304,
        max_library_validation_bytes: 4194304,
    };
    let pool = crate::ConstantPool::try_new(
        Arc::new(source.try_to_field("actual").unwrap()),
        source.clone(),
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let rows = [1, 256, 512];
    let selection = Selection::try_sparse(513, &rows).unwrap();
    for name in ["map_keys", "map_values"] {
        let p = prepared(name, &source, crate::binding_test_control()).unwrap();
        for ordinal in [0, 1, 2] {
            let value = pool.value(ordinal).unwrap();
            let constant_argument = EvaluatedArgument::Constant(&value);
            let out = ScalarEvaluationInstance::instantiate(p.clone())
                .unwrap()
                .evaluate(
                    selection,
                    std::slice::from_ref(&constant_argument),
                    &Control::default(),
                )
                .unwrap();
            if ordinal == 0 {
                assert_eq!(
                    out.errors(),
                    &(0..3)
                        .map(|r| crate::RowDataError::new(
                            r,
                            "map key ordered compare unsupported type: UInt32"
                        ))
                        .collect::<Vec<_>>()
                );
                assert_eq!(out.values().null_count(), 3);
            } else {
                assert!(out.errors().is_empty());
                assert_eq!(out.values().null_count(), if ordinal == 2 { 3 } else { 0 });
            }
        }
    }
}

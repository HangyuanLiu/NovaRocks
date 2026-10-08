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

use super::*;
use crate::kernel_control::internal;
use crate::{ConstantPolicy, ConstantPool, EvaluatedArgument, ScalarEvaluationInstance, Selection};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray, new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
};
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
        panic!("array difference never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn policy() -> ConstantPolicy {
    // Finite fixture admission of already allocated real Arrow arrays, not a MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

fn instance(a: &ArrayRef) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_control(
            "array_difference",
            &[FunctionValueType::new(a.data_type().clone(), true)],
            DecimalOverflowPolicy::OutputNull,
            crate::binding_test_control(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(
            Field::new("source-child", values.data_type().clone(), true)
                .with_metadata([("nested-proof".into(), "retained".into())].into()),
        ),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn array_difference_selected_origin_null_slice_empty_nonzero_pool_and_full_target_metadata() {
    let a = list(
        Arc::new(Int64Array::from(vec![
            Some(99),
            Some(2),
            None,
            Some(4),
            Some(88),
            Some(-7),
            Some(5),
        ])),
        vec![0, 1, 4, 5, 7],
        Some(vec![true, true, false, true]),
    );
    let rows = [1usize, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    let mut k = instance(&a);
    let expected_type = k.contract().result_type().data_type.clone();
    let result = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(result.errors().is_empty());
    assert_eq!(result.values().data_type(), &expected_type);
    assert_eq!(
        ints(result.values()),
        vec![Some(0), None, None, Some(0), Some(12)]
    );
    assert!(result.values().is_null(1));
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0, 3, 3, 5]
    );
    let ty = FunctionValueType::new(a.data_type().clone(), true);
    let prepared = prepared_for_test_with_policy(
        "array_difference",
        &[ty.clone()],
        DecimalOverflowPolicy::ReportError,
    )
    .unwrap();
    assert_eq!(
        prepared.contract().selected().argument_types[0],
        FunctionArgumentType::Value(ty.clone())
    );
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("pool-source").unwrap()),
        ty,
        a.to_data(),
        policy(),
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let args = [EvaluatedArgument::Constant(&constant)];
    let result = instance(&a)
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    assert_eq!(
        ints(result.values()),
        vec![Some(0), None, None, Some(0), None, None]
    );
    let sliced = a.slice(1, 3);
    let args = [EvaluatedArgument::Column(&sliced)];
    assert_eq!(
        ints(
            instance(&sliced)
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap()
                .values()
        ),
        vec![Some(0), None, None, Some(0), Some(12)]
    );
    let empty = a.slice(0, 0);
    let args = [EvaluatedArgument::Column(&empty)];
    assert_eq!(
        instance(&empty)
            .evaluate(Selection::all(0), &args, &Control::default())
            .unwrap()
            .values()
            .len(),
        0
    );
}
#[test]
fn array_difference_selected_original_decimal_float_cast_and_no_admission_widening() {
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(125), Some(150), None, Some(-25)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let a = list(values, vec![0, 2, 4], None);
    let args = [EvaluatedArgument::Column(&a)];
    let result = instance(&a)
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    let output = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(output.value_type(), DataType::Float64);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0.0), Some(0.25), None, None]
    );
    // These are actual resolver refusals, not new preparation-time restrictions.
    for item in [
        DataType::UInt64,
        DataType::Utf8,
        DataType::Date32,
        DataType::Decimal256(76, 0),
        DataType::FixedSizeBinary(16),
        DataType::Null,
    ] {
        let source = FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", item, true))),
            true,
        );
        assert!(
            prepared_for_test_with_policy(
                "array_difference",
                &[source],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
}
#[test]
fn array_difference_selected_original_extreme_panic_or_wrapping_core_and_prepared() {
    use super::super::array_difference_core;
    let a = list(
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)])),
        vec![0, 2],
        None,
    );
    let args = [EvaluatedArgument::Column(&a)];
    let mut k = instance(&a);
    let target = k.contract().result_type().data_type.clone();
    let core = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        array_difference_core::difference(&a, Some(&target), 1)
    }));
    let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        k.evaluate(Selection::all(1), &args, &Control::default())
    }));
    if cfg!(debug_assertions) {
        assert!(core.is_err());
        assert!(prepared.is_err());
    } else {
        assert_eq!(ints(&core.unwrap().unwrap()), vec![Some(0), Some(-1)]);
        assert_eq!(
            ints(prepared.unwrap().unwrap().values()),
            vec![Some(0), Some(-1)]
        );
    }
    // A hidden parent must retain the original skip-before-subtraction behavior.
    let a = list(
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)])),
        vec![0, 2],
        Some(vec![false]),
    );
    let args = [EvaluatedArgument::Column(&a)];
    let result = instance(&a)
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    assert!(result.values().is_null(0));
}
#[test]
fn array_difference_selected_all_seven_causes_each_observation_failed_latch_and_real_quanta() {
    let a = list(
        Arc::new(Int64Array::from(vec![Some(1); 700])),
        vec![0, 700],
        None,
    );
    let args = [EvaluatedArgument::Column(&a)];
    let good = Control::default();
    instance(&a)
        .evaluate(Selection::all(1), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for cause in causes() {
        for stop in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            let mut k = instance(&a);
            assert_eq!(
                k.evaluate(Selection::all(1), &args, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            let after = Control::default();
            assert_eq!(
                k.evaluate(Selection::all(1), &args, &after).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
    let a = list(new_empty_array(&DataType::Int64), vec![0; 322], None);
    let args = [EvaluatedArgument::Column(&a)];
    let control = Control::default();
    instance(&a)
        .evaluate(Selection::all(321), &args, &control)
        .unwrap();
    assert!(control.trace.lock().unwrap().contains(&256));
}

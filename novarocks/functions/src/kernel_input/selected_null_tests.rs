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

use super::{validate_argument_observed, visit_selected_nulls};
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, KernelDiagnostic,
    KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues, Selection,
};
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int16Array, Int64Array, RunArray, StringArray,
    UnionArray,
};
use arrow_schema::{DataType, Field, UnionFields};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, failure)) = &self.refusal
            && index == *at
        {
            return Err(failure.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("NULL inspection must not wait");
    }
}
struct ConstructionControl;
impl PureCompileControl for ConstructionControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn refusals() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid-program refusal")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("selected-null-input").unwrap()),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 32,
            max_array_nodes: 512,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4 * 1024 * 1024,
            max_library_validation_work: 128 * 1024 * 1024,
            max_library_validation_bytes: 64 * 1024 * 1024,
        },
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap()
}
fn inspect(argument: EvaluatedArgument<'_>, selection: Selection<'_>) -> Vec<(usize, usize, bool)> {
    let mut rows = Vec::new();
    visit_selected_nulls(
        argument,
        selection,
        &Control::default(),
        |ordinal, row, null| {
            rows.push((ordinal, row, null));
            Ok(())
        },
    )
    .unwrap();
    rows
}

#[test]
fn constant_inspection_broadcasts_exact_nonzero_pool_ordinal() {
    let p = pool(Arc::new(Int64Array::from(vec![
        None,
        Some(41),
        None,
        Some(99),
    ])));
    let selection = Selection::try_sparse(100, &[2, 51, 99]).unwrap();
    let nonnull = p.value(1).unwrap();
    let null = p.value(2).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&nonnull), selection),
        [(0, 2, false), (1, 51, false), (2, 99, false)]
    );
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&null), selection),
        [(0, 2, true), (1, 51, true), (2, 99, true)]
    );
    assert!(Arc::ptr_eq(nonnull.pool().array(), p.array()));
}

#[test]
fn dictionary_inspection_uses_selected_keys_and_not_unselected_pool_nulls() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec![
                Some("present"),
                None,
                Some("unused"),
            ])),
        )
        .unwrap(),
    );
    let selection = Selection::try_sparse(4, &[1, 2, 3]).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Column(&dictionary), selection),
        [(0, 1, false), (1, 2, true), (2, 3, true)]
    );
    // The first pool row is NULL, while the constant ordinal selects key 0.
    let p = pool(dictionary);
    let value = p.value(1).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&value), Selection::all(2)),
        [(0, 0, false), (1, 1, false)]
    );
}

#[test]
fn union_inspection_uses_actual_dense_tags_offsets_and_slice() {
    let union = UnionArray::try_new(
        UnionFields::try_new(
            [7, 2],
            [
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, true),
            ],
        )
        .unwrap(),
        vec![7_i8, 2, 7, 2].into(),
        Some(vec![0_i32, 1, 1, 0].into()),
        vec![
            Arc::new(StringArray::from(vec![Some("unused"), None])),
            Arc::new(Int64Array::from(vec![None, Some(42)])),
        ],
    )
    .unwrap();
    let sliced: ArrayRef = Arc::new(union.slice(1, 3));
    assert_eq!(
        inspect(EvaluatedArgument::Column(&sliced), Selection::all(3)),
        [(0, 0, false), (1, 1, true), (2, 2, true)]
    );
}

#[test]
fn run_end_inspection_uses_logical_offsets_of_sliced_runs() {
    let runs = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 5, 7]),
        &Int64Array::from(vec![Some(11), None, Some(77)]),
    )
    .unwrap();
    let sliced: ArrayRef = Arc::new(runs.slice(1, 5));
    assert_eq!(
        inspect(
            EvaluatedArgument::Column(&sliced),
            Selection::try_sparse(5, &[0, 2, 4]).unwrap()
        ),
        [(0, 0, false), (1, 2, true), (2, 4, false)]
    );
}

#[test]
fn selected_error_nulls_can_be_inspected_but_cannot_enter_ordinary_kernel_arguments() {
    let rows = [1, 4, 8];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(9)]));
    let output = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        values,
        vec![RowDataError::new(1, "required child failed")].into_boxed_slice(),
    )
    .unwrap();
    let argument = EvaluatedArgument::SelectedColumn(&output);
    assert_eq!(
        inspect(argument, selection),
        [(0, 1, false), (1, 4, true), (2, 8, false)]
    );
    assert!(matches!(
        validate_argument_observed(
            argument,
            selection,
            &FunctionValueType::new(DataType::Int64, true),
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let wrong = Selection::try_sparse(10, &[1, 5, 8]).unwrap();
    let mut visits = 0;
    assert!(matches!(
        visit_selected_nulls(argument, wrong, &Control::default(), |_, _, _| {
            visits += 1;
            Ok(())
        }),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(visits, 0);
}

#[test]
fn callback_refusal_is_primary_and_has_no_later_visit_or_checkpoint() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    for failure in refusals() {
        let control = Control::default();
        let mut visits = Vec::new();
        let result = visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(3),
            &control,
            |ordinal, _, null| {
                visits.push((ordinal, null));
                if ordinal == 1 {
                    Err(failure.clone())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err(failure));
        assert_eq!(visits, [(0, false), (1, true)]);
        assert_eq!(*control.checks.lock().unwrap(), [0]);
    }
}

#[test]
fn original_controls_refuse_entry_quantum_and_completed_tail_without_retry() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1); 257]));
    let baseline = Control::default();
    visit_selected_nulls(
        EvaluatedArgument::Column(&values),
        Selection::all(257),
        &baseline,
        |_, _, _| Ok(()),
    )
    .unwrap();
    let trace = [0, 256, 256, 2];
    assert_eq!(*baseline.checks.lock().unwrap(), trace);
    for failure in refusals() {
        for (check, expected_visits) in [(0, 0), (1, 128), (2, 256), (3, 257)] {
            let control = Control {
                checks: Mutex::default(),
                refusal: Some((check, failure.clone())),
            };
            let mut visits = 0;
            let result = visit_selected_nulls(
                EvaluatedArgument::Column(&values),
                Selection::all(257),
                &control,
                |_, _, _| {
                    visits += 1;
                    Ok(())
                },
            );
            assert_eq!(result, Err(failure.clone()));
            assert_eq!(visits, expected_visits);
            assert_eq!(*control.checks.lock().unwrap(), trace[..=check]);
        }
    }
}

#[test]
fn ordinary_shape_rejection_observes_completed_comparison_tail() {
    let selection = Selection::try_sparse(10, &[1, 4, 8]).unwrap();
    let wrong = Selection::try_sparse(10, &[1, 5, 8]).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let output =
        SelectedValues::try_new(selection, &DataType::Int64, values, Box::default()).unwrap();
    let baseline = Control::default();
    let mut visits = 0;
    assert!(matches!(
        visit_selected_nulls(
            EvaluatedArgument::SelectedColumn(&output),
            wrong,
            &baseline,
            |_, _, _| {
                visits += 1;
                Ok(())
            }
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(visits, 0);
    assert_eq!(*baseline.checks.lock().unwrap(), [0, 2]);
    for refusal in refusals() {
        let control = Control {
            checks: Mutex::default(),
            refusal: Some((1, refusal.clone())),
        };
        let result = visit_selected_nulls(
            EvaluatedArgument::SelectedColumn(&output),
            wrong,
            &control,
            |_, _, _| panic!("shape rejection must precede any visitor"),
        );
        assert_eq!(result, Err(refusal));
        assert_eq!(*control.checks.lock().unwrap(), [0, 2]);
    }
}

#[test]
fn ordinary_address_rejection_observes_nested_work_tail() {
    // Valid Arrow dictionaries can exceed the kernel's checked addressing depth.
    // The inspector must observe this ordinary internal rejection, not return
    // early merely because it has the same category as a control callback.
    let mut values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    for _ in 0..novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        values = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values).unwrap(),
        );
    }
    let baseline = Control::default();
    assert!(matches!(
        visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(1),
            &baseline,
            |_, _, _| panic!("invalid depth cannot reach visitor")
        ),
        Err(KernelFailure::Internal(_))
    ));
    let tail = u32::try_from(novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 1).unwrap();
    assert_eq!(*baseline.checks.lock().unwrap(), [0, tail]);
    for refusal in refusals() {
        let control = Control {
            checks: Mutex::default(),
            refusal: Some((1, refusal.clone())),
        };
        assert_eq!(
            visit_selected_nulls(
                EvaluatedArgument::Column(&values),
                Selection::all(1),
                &control,
                |_, _, _| panic!("invalid depth cannot reach visitor")
            ),
            Err(refusal)
        );
        assert_eq!(*control.checks.lock().unwrap(), [0, tail]);
    }
}

#[test]
fn ordinary_length_rejection_observes_zero_completed_tail() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let baseline = Control::default();
    assert!(matches!(
        visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(2),
            &baseline,
            |_, _, _| panic!("invalid length cannot reach visitor")
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(*baseline.checks.lock().unwrap(), [0, 0]);
    let control = Control {
        checks: Mutex::default(),
        refusal: Some((1, KernelFailure::DeadlineExceeded)),
    };
    assert_eq!(
        visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(2),
            &control,
            |_, _, _| panic!("invalid length cannot reach visitor")
        ),
        Err(KernelFailure::DeadlineExceeded)
    );
    assert_eq!(*control.checks.lock().unwrap(), [0, 0]);
}

#[test]
fn bounded_row_projection_shares_null_traversal_observation_instead_of_a_parallel_counter() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct ProjectionControl {
        visited: AtomicUsize,
        positions: Mutex<Vec<usize>>,
    }
    impl KernelEvaluationControl for ProjectionControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            self.positions
                .lock()
                .unwrap()
                .push(self.visited.load(Ordering::Relaxed));
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("NULL projection must not wait");
        }
    }
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(17); 320]));
    let control = ProjectionControl::default();
    visit_selected_nulls(
        EvaluatedArgument::Column(&array),
        Selection::all(320),
        &control,
        |_, _, null| {
            assert!(!null);
            control.visited.fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(control.visited.load(Ordering::Relaxed), 320);
    let mut previous = 0;
    for &position in control.positions.lock().unwrap().iter() {
        // Each shallow source visit and its completed projection share the
        // 256-unit limit. A separate 256-row counter would exceed this gap.
        assert!(position - previous <= crate::MAX_UNOBSERVED_KERNEL_WORK as usize / 2);
        previous = position;
    }
    assert_eq!(previous, 320);
}

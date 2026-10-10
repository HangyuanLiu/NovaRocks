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
//! Actual MAP_ENTRIES exact binding, selected addressing and first-cause tests.
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionSpecializationFailure,
    FunctionValueType, ScalarEvaluationInstance, Selection,
};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray, new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{CompileControlError, CompilePhase, DecimalOverflowPolicy};
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
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(n);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("map_entries never waits")
    }
}
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, phase: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, n));
        match self.refusal {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn fixture(sorted: bool) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(
                Field::new("authored-key", DataType::Int32, false)
                    .with_metadata([("opaque".into(), "key".into())].into()),
            ),
            Arc::new(Field::new("authored-value", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![9, 1, 9, 4, 7])),
            Arc::new(StringArray::from(vec![
                Some("九\0"),
                None,
                Some("last"),
                Some("hidden"),
                Some("七"),
            ])),
        ],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new(
            "authored-entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(vec![0, 3, 3, 4, 5].into()),
        entries,
        Some(NullBuffer::from(vec![true, true, false, true])),
        sorted,
    ))
}
fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::new(a.data_type().clone(), true)
}
fn instance(a: &ArrayRef) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "map_entries",
            &[source(a)],
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn keys(a: &ArrayRef, row: usize) -> Vec<Option<i32>> {
    let list = a.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.value(row);
    let values = values.as_any().downcast_ref::<StructArray>().unwrap();
    values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn map_entries_owner_exact_original_order_duplicates_nulls_slices_empty_and_canonical_fields() {
    for sorted in [false, true] {
        let a = fixture(sorted);
        let args = [EvaluatedArgument::Column(&a)];
        let rows = [0, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let out = instance(&a)
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(keys(out.values(), 0), vec![Some(9), Some(1), Some(9)]);
        assert!(out.values().is_null(1));
        assert_eq!(keys(out.values(), 2), vec![Some(7)]);
        let DataType::List(item) = out.values().data_type() else {
            panic!("actual List");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("actual Struct");
        };
        assert_eq!(item.name(), "item");
        assert_eq!(fields[0].name(), "key");
        assert_eq!(fields[1].name(), "value");
        assert!(fields[0].is_nullable());
        assert!(out.errors().is_empty());
        let sliced = a.slice(1, 3);
        let args = [EvaluatedArgument::Column(&sliced)];
        let out = instance(&sliced)
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert_eq!(keys(out.values(), 0), Vec::<Option<i32>>::new());
        assert!(out.values().is_null(1));
        assert_eq!(keys(out.values(), 2), vec![Some(7)]);
        let empty = new_empty_array(a.data_type());
        let args = [EvaluatedArgument::Column(&empty)];
        let out = instance(&empty)
            .evaluate(Selection::all(0), &args, &Control::default())
            .unwrap();
        assert!(out.values().is_empty());
    }
}
#[test]
fn map_entries_owner_constant_nonzero_pool_null_and_selected_column_use_actual_addresses() {
    let a = fixture(false);
    let ty = source(&a);
    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 24,
        max_library_validation_bytes: 1 << 24,
    };
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-pool").unwrap()),
        ty,
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    for ordinal in [2, 3] {
        let v = pool.value(ordinal).unwrap();
        let args = [EvaluatedArgument::Constant(&v)];
        let out = instance(&a)
            .evaluate(Selection::all(257), &args, &Control::default())
            .unwrap();
        for row in 0..257 {
            if ordinal == 2 {
                assert!(out.values().is_null(row));
            } else {
                assert_eq!(keys(out.values(), row), vec![Some(7)]);
            }
        }
    }
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let projected = arrow_select::take::take(
        a.as_ref(),
        &arrow_array::UInt64Array::from(vec![1, 3]),
        None,
    )
    .unwrap();
    let prior = crate::SelectedValues::try_new(selection, a.data_type(), projected, Box::default())
        .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&prior)];
    let out = instance(&a)
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(keys(out.values(), 0), Vec::<Option<i32>>::new());
    assert_eq!(keys(out.values(), 1), vec![Some(7)]);
}
#[test]
fn map_entries_owner_full_nested_profiles_preserve_no_ordered_decoder_or_nominal_guess() {
    for item in [
        DataType::Null,
        DataType::UInt64,
        DataType::List(Arc::new(Field::new("nested", DataType::Utf8, true))),
        DataType::Map(
            Arc::new(Field::new(
                "nested-entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("nested-key", DataType::Int32, false)),
                        Arc::new(Field::new("nested-value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            true,
        ),
    ] {
        let values = arrow_array::new_null_array(&item, 3);
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", item, true)),
            ]
            .into(),
            vec![Arc::new(StringArray::from(vec!["z", "a", "z"])), values],
            None,
        );
        let a: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 3].into()),
            entries,
            None,
            false,
        ));
        let args = [EvaluatedArgument::Column(&a)];
        let out = instance(&a)
            .evaluate(Selection::all(1), &args, &Control::default())
            .unwrap();
        let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let entries = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("z"), Some("a"), Some("z")]
        );
        assert_eq!(entries.column(1).logical_null_count(), 3);
    }
}
#[test]
fn map_entries_owner_all_seven_actual_causes_exact_prefix_failed_latch_and_real_row_quanta() {
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
        None,
    );
    let a: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0; 322].into()),
        entries,
        None,
        false,
    ));
    for a in [a, fixture(false)] {
        let args = [EvaluatedArgument::Column(&a)];
        let good = Control::default();
        instance(&a)
            .evaluate(Selection::all(a.len()), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if a.len() > 256 {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                invalid("original invalid cause"),
                crate::kernel_control::internal("original internal cause"),
                KernelFailure::Operational(crate::KernelDiagnostic::new(
                    "original operational cause",
                )),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&a);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn map_entries_owner_compile_success_and_original_admission_rejection_all_actual_causes() {
    let a = fixture(false);
    for ty in [source(&a), FunctionValueType::new(DataType::Int32, true)] {
        let good = Compile::default();
        assert_eq!(
            prepared_for_test_with_control(
                "map_entries",
                &[ty.clone()],
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            matches!(ty.data_type, DataType::Map(..))
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Compile {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause)),
                };
                let error = prepared_for_test_with_control(
                    "map_entries",
                    &[ty.clone()],
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .and_then(|e| match e {
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
                });
                assert_eq!(error, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

fn check_all_projection_causes(
    a: &ArrayRef,
    selection: Selection<'_>,
    args: &[EvaluatedArgument<'_>],
) {
    let good = Control::default();
    instance(a).evaluate(selection, args, &good).unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            invalid("original invalid cause"),
            crate::kernel_control::internal("original internal cause"),
            KernelFailure::Operational(crate::KernelDiagnostic::new("original operational cause")),
            KernelFailure::InstanceFailed,
        ] {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut kernel = instance(a);
            assert_eq!(
                kernel.evaluate(selection, args, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                kernel.evaluate(selection, args, &after).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
fn wide_dictionary(rows: usize, dictionary_len: usize) -> ArrayRef {
    use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
    let dictionary: ArrayRef = Arc::new(StringArray::from(
        (0..dictionary_len)
            .map(|i| format!("entry-{i}"))
            .collect::<Vec<_>>(),
    ));
    let values: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0; rows]), dictionary).unwrap(),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", values.data_type().clone(), true)),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1; rows])), values],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new((0..=rows).map(|i| i as i32).collect::<Vec<_>>().into()),
        entries,
        None,
        false,
    ))
}
fn assert_dictionary_extent(a: &ArrayRef, expected: usize) {
    use arrow_array::{DictionaryArray, types::Int8Type};
    let list = a.as_any().downcast_ref::<ListArray>().unwrap();
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let dictionary = entries
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dictionary.values().len(), expected);
}
#[test]
fn map_entries_owner_contiguous_actual_address_view_preserves_original_dictionary128() {
    let a = wide_dictionary(4, 128);
    for a in [a.clone(), a.slice(1, 3)] {
        let args = [EvaluatedArgument::Column(&a)];
        let out = instance(&a)
            .evaluate(Selection::all(a.len()), &args, &Control::default())
            .unwrap();
        assert_eq!(out.values().len(), a.len());
        assert_dictionary_extent(out.values(), 128);
        check_all_projection_causes(&a, Selection::all(a.len()), &args);
    }
    let rows = [1, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    let out = instance(&a)
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_dictionary_extent(out.values(), 128);
    check_all_projection_causes(&a, selection, &args);
}
#[test]
fn map_entries_owner_noncontiguous_addresses_keep_original_shared_copy_dictionary_guard() {
    let rows = [0, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let a = wide_dictionary(4, 127);
    let args = [EvaluatedArgument::Column(&a)];
    let out = instance(&a)
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_dictionary_extent(out.values(), 127);
    check_all_projection_causes(&a, selection, &args);
    let a = wide_dictionary(4, 128);
    let args = [EvaluatedArgument::Column(&a)];
    let mut kernel = instance(&a);
    assert_eq!(
        kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(selection, &args, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}
#[test]
fn map_entries_owner_single_constant_ordinal_and_selected_column_use_same_contiguous_proof() {
    let a = wide_dictionary(4, 128);
    let ty = source(&a);
    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 24,
        max_library_validation_bytes: 1 << 24,
    };
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-pool").unwrap()),
        ty,
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let v = pool.value(3).unwrap();
    let args = [EvaluatedArgument::Constant(&v)];
    let out = instance(&a)
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    assert_dictionary_extent(out.values(), 128);
    check_all_projection_causes(&a, Selection::all(1), &args);
    // Every source row has the same payload. This real public selected value
    // represents batch rows 0/2 with compact ordinals 0/1, without an Arrow copy.
    let rows = [0, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let prior =
        crate::SelectedValues::try_new(selection, a.data_type(), a.slice(0, 2), Box::default())
            .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&prior)];
    let out = instance(&a)
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(out.values().len(), 2);
    assert_dictionary_extent(out.values(), 128);
    check_all_projection_causes(&a, selection, &args);
}

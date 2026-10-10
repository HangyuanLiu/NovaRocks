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

use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarEvaluationInstance, SelectedValues, Selection,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{CompilePhase, DecimalOverflowPolicy, ValueLogicalType};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

// Independent IEEE CRC32 fixtures produced by Python's standard zlib.crc32,
// not by this kernel or the legacy implementation. These are not CRC32C.
const FIXTURES: [(&str, i64); 6] = [
    ("", 0),
    ("123456789", 3_421_780_262),
    ("hello", 907_060_870),
    ("你好", 1_352_841_281),
    ("\0", 3_523_407_757),
    ("a\0b", 367_556_721),
];

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Control {
    fn refusing(index: usize, error: KernelFailure) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refusal: Some((index, error)),
        }
    }
    fn calls(&self) -> Vec<u32> {
        self.calls.lock().unwrap().clone()
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push(units);
        if let Some((at, error)) = &self.refusal
            && *at == index
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("crc32 kernels must not wait");
    }
}
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn instance(nullable: bool) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::crc32_owner::prepared_for_test(&source(nullable)).unwrap(),
    )
    .unwrap()
}
fn output(array: &ArrayRef) -> Vec<Option<i64>> {
    assert_eq!(array.data_type(), &DataType::Int64);
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = source(true);
    let rows = array.len() as u64;
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("crc32").unwrap()),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: rows,
            max_array_nodes: 1,
            max_logical_elements: rows,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 8192,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

#[test]
fn ieee_zlib_vectors_preserve_utf8_nul_and_unsigned_bit31_results() {
    let input = strings(FIXTURES.iter().map(|(text, _)| Some(*text)).collect());
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let prepared =
            super::super::crc32_owner::prepared_for_test_with_policy(&source(false), policy)
                .unwrap();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert_eq!(kernel.contract().decimal_overflow_policy(), policy);
        assert_eq!(kernel.contract().result_type().data_type, DataType::Int64);
        assert_eq!(
            kernel.contract().result_type().logical_type,
            ValueLogicalType::Physical
        );
        assert!(kernel.contract().result_type().nullable);
        let args = [EvaluatedArgument::Column(&input)];
        let result = kernel
            .evaluate(Selection::all(FIXTURES.len()), &args, &Control::default())
            .unwrap();
        assert!(result.errors().is_empty());
        assert_eq!(
            output(result.values()),
            FIXTURES.map(|(_, checksum)| Some(checksum))
        );
    }
}

#[test]
fn sliced_column_sparse_and_compact_rows_keep_their_exact_mapping() {
    let backing = strings(vec![
        Some("discard"),
        Some("hello"),
        None,
        Some("123456789"),
        Some("discard"),
    ]);
    let sliced = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact_values = strings(vec![Some("hello"), Some("123456789")]);
    let compact =
        SelectedValues::try_new(selection, &DataType::Utf8, compact_values, Box::default())
            .unwrap();
    for argument in [
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let mut kernel = instance(false);
        let args = [argument];
        let result = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            output(result.values()),
            [Some(907_060_870), Some(3_421_780_262)]
        );
    }
}

#[test]
fn scalar_and_checked_pool_ordinal_broadcast_only_the_selected_value() {
    let input = strings(vec![Some("你好")]);
    let mut kernel = instance(false);
    let args = [EvaluatedArgument::Scalar(&input)];
    let result = kernel
        .evaluate(Selection::all(3), &args, &Control::default())
        .unwrap();
    assert_eq!(output(result.values()), [Some(1_352_841_281); 3]);
    let pool = pool(strings(vec![None, Some("hello"), Some("123456789")]));
    for (ordinal, expected) in [(1, 907_060_870), (2, 3_421_780_262)] {
        let value = pool.value(ordinal).unwrap();
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
        let mut kernel = instance(true);
        let args = [EvaluatedArgument::Constant(&value)];
        let result = kernel
            .evaluate(Selection::all(4), &args, &Control::default())
            .unwrap();
        assert_eq!(output(result.values()), [Some(expected); 4]);
    }
    let value = pool.value(0).unwrap();
    let mut kernel = instance(true);
    let args = [EvaluatedArgument::Constant(&value)];
    assert_eq!(
        output(
            kernel
                .evaluate(Selection::all(2), &args, &Control::default())
                .unwrap()
                .values()
        ),
        [None; 2]
    );
}

#[test]
fn nullable_null_is_successful_but_selected_nonnull_contradiction_poison_is_fatal() {
    let input = strings(vec![None, Some(""), Some("hello")]);
    let mut kernel = instance(true);
    let args = [EvaluatedArgument::Column(&input)];
    let result = kernel
        .evaluate(Selection::all(3), &args, &Control::default())
        .unwrap();
    assert!(result.errors().is_empty());
    assert_eq!(output(result.values()), [None, Some(0), Some(907_060_870)]);
    let mut kernel = instance(false);
    assert!(matches!(
        kernel.evaluate(Selection::all(3), &args, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(Selection::all(0), &[], &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.calls().is_empty());
}

#[test]
fn real_owner_and_runtime_refuse_foreign_nominal_carrier_and_shape() {
    for bad in [
        FunctionValueType::new(DataType::LargeUtf8, false),
        FunctionValueType::new(DataType::Binary, false),
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
    ] {
        // The preparation ABI requires already-coerced exact arguments. Legacy
        // carrier support does not install additional pure selected profiles.
        assert!(super::super::crc32_owner::prepared_for_test(&bad).is_err());
    }
    let short = strings(vec![Some("hello")]);
    let wrong: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    for array in [&short, &wrong] {
        let mut kernel = instance(false);
        let args = [EvaluatedArgument::Column(array)];
        assert!(matches!(
            kernel.evaluate(Selection::all(2), &args, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let mut kernel = instance(false);
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &[], &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn foreign_or_poisoned_compact_selection_cannot_be_consumed() {
    let rows = [0];
    let other = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let foreign = Selection::try_sparse(2, &other).unwrap();
    let values = strings(vec![None]);
    let poisoned = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        values.clone(),
        vec![crate::RowDataError::new(0, "child failed")].into_boxed_slice(),
    )
    .unwrap();
    let wrong = SelectedValues::try_new(foreign, &DataType::Utf8, values, Box::default()).unwrap();
    for compact in [&poisoned, &wrong] {
        let mut kernel = instance(true);
        let args = [EvaluatedArgument::SelectedColumn(compact)];
        assert!(matches!(
            kernel.evaluate(selection, &args, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn byte_quantum_tail_and_publication_refusals_preserve_typed_cause_and_latch() {
    let text = "x".repeat(600);
    let input = strings(vec![Some(&text)]);
    let args = [EvaluatedArgument::Scalar(&input)];
    let trace = Control::default();
    let mut kernel = instance(false);
    let result = kernel.evaluate(Selection::all(1), &args, &trace).unwrap();
    assert_eq!(output(result.values()), [Some(3_751_744_722)]);
    let calls = trace.calls();
    assert_eq!(calls.iter().filter(|units| **units == 256).count(), 2);
    let second_quantum = calls.iter().rposition(|units| *units == 256).unwrap();
    assert!(
        calls[second_quantum + 1..]
            .iter()
            .any(|units| *units > 0 && *units < 256)
    );
    assert_eq!(calls.last(), Some(&0));
    for error in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        // Replay a fresh real instance for every observed boundary, including
        // entry, both byte quanta, the positive tail, and publication.
        for at in 0..calls.len() {
            let control = Control::refusing(at, error.clone());
            let mut kernel = instance(false);
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &args, &control)
                    .unwrap_err(),
                error
            );
            assert_eq!(control.calls(), calls[..=at]);
            let after = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &args, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.calls().is_empty());
        }
    }
}

#[test]
fn selected_row_and_byte_work_is_bounded_and_batch_partition_invariant() {
    let text = "x".repeat(10_000);
    let input = strings(vec![Some(&text); 3]);
    let args = [EvaluatedArgument::Column(&input)];
    let trace = Control::default();
    let mut kernel = instance(false);
    let output_all = kernel.evaluate(Selection::all(3), &args, &trace).unwrap();
    assert_eq!(output(output_all.values()), [Some(223_716_515); 3]);
    assert!(trace.calls().iter().filter(|n| **n == 256).count() >= 117);
    let mut partitioned = instance(false);
    for _ in 0..3 {
        let one = input.slice(0, 1);
        let args = [EvaluatedArgument::Column(&one)];
        assert_eq!(
            output(
                partitioned
                    .evaluate(Selection::all(1), &args, &Control::default())
                    .unwrap()
                    .values()
            ),
            [Some(223_716_515)]
        );
    }
    let empty = strings(vec![Some(""); 257]);
    let args = [EvaluatedArgument::Column(&empty)];
    let trace = Control::default();
    let mut kernel = instance(true);
    assert_eq!(
        output(
            kernel
                .evaluate(Selection::all(257), &args, &trace)
                .unwrap()
                .values()
        ),
        vec![Some(0); 257]
    );
    assert!(trace.calls().contains(&256));
}

#[test]
fn empty_demand_skips_checksum_and_capacity_refusal_precedes_selected_work() {
    let null = strings(vec![None]);
    let args = [EvaluatedArgument::Scalar(&null)];
    let trace = Control::default();
    let mut kernel = instance(false);
    let result = kernel.evaluate(Selection::all(0), &args, &trace).unwrap();
    assert!(result.values().is_empty());
    assert!(result.errors().is_empty());
    // Wrapper entry and argument entry only: the body has its own entry zero.
    assert_eq!(trace.calls().iter().filter(|n| **n == 0).count(), 2);
    let mut kernel = instance(true);
    let trace = Control::default();
    assert_eq!(
        kernel
            .evaluate(Selection::all(usize::MAX), &args, &trace)
            .unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    assert!(!trace.calls().contains(&256));
    assert!(super::output_capacity(usize::MAX).is_err());
}

#[test]
fn unselected_long_values_and_nulls_do_not_add_checksum_work() {
    let long = "x".repeat(10_000);
    let wide = strings(vec![Some(&long), Some("hello"), None]);
    let short = strings(vec![Some(""), Some("hello"), Some("")]);
    let rows = [1];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let mut observed = Vec::new();
    for input in [&wide, &short] {
        let mut kernel = instance(false);
        let control = Control::default();
        let args = [EvaluatedArgument::Column(input)];
        let result = kernel.evaluate(selection, &args, &control).unwrap();
        assert_eq!(output(result.values()), [Some(907_060_870)]);
        observed.push(control.calls());
    }
    assert_eq!(observed[0], observed[1]);
    assert!(!observed[0].contains(&256));
}

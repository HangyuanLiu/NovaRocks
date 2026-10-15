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
use std::sync::atomic::{AtomicBool, Ordering};

struct Control(AtomicBool);
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "control after first host refusal"
        );
        Ok(())
    }
}

#[test]
fn input_derived_null_rows_are_independent_of_buffer_bytes() {
    let control = Control(AtomicBool::new(false));
    let field = Field::new("source", DataType::Null, true);
    let ty = FunctionValueType::new(DataType::Null, true);
    let input = FlatConstantResourceInput {
        rows: 1 << 40,
        buffer_count_upper_bound: 0,
        buffer_visits_bytes_upper_bound: 0,
        retained_buffer_capacity_bytes_upper_bound: 0,
        view_validation_bytes_upper_bound: 0,
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (facts, policy) = preflight_flat_pool_resources_input_derived_observed(
        &field,
        &ty,
        input,
        &mut |_| Ok::<(), ()>(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(policy.max_rows, input.rows);
    assert_eq!(policy.max_logical_elements, input.rows);
    assert_eq!(policy.max_retained_buffer_bytes, 0);
    assert_eq!(
        preflight_flat_pool_resources(&field, &ty, input, policy, CompilePhase::Decode, &control,)
            .unwrap(),
        facts
    );
    assert!(matches!(
        preflight_flat_pool_resources(
            &field,
            &ty,
            input,
            ConstantPolicy {
                max_rows: input.rows - 1,
                ..policy
            },
            CompilePhase::Decode,
            &control,
        ),
        Err(ConstantError::Limit("constant row limit exceeded"))
    ));
    work.finish().unwrap();
}

#[test]
fn input_derived_recursive_profile_admits_actual_checked_values_without_a_second_formula() {
    use arrow_array::{Int8Array, ListArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    let control = Control(AtomicBool::new(false));
    let item = Arc::new(Field::new("item", DataType::Int8, true));
    let field = Arc::new(Field::new("source", DataType::List(item.clone()), true));
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let input = RecursiveConstantResourceInput {
        buffer_count_upper_bound: 4,
        buffer_visits_bytes_upper_bound: 100,
        retained_buffer_capacity_bytes_upper_bound: 512,
        view_validation_bytes_upper_bound: 0,
        utf8_fallback_validation_bytes_upper_bound: 0,
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (facts, policy) = preflight_recursive_pool_resources_input_derived_observed(
        &field,
        &ty,
        [2, 5],
        input,
        &mut |_| Ok::<(), ()>(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(facts.array_nodes, 2);
    assert_eq!(facts.logical_elements_upper_bound, 12);
    assert_eq!(policy.max_type_depth, 2);
    assert_eq!(policy.max_type_nodes, 2);
    assert_eq!(
        preflight_recursive_pool_resources(
            &field,
            &ty,
            [2, 5],
            input,
            policy,
            CompilePhase::Decode,
            &control,
        )
        .unwrap(),
        facts
    );
    let array = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 5])),
        Arc::new(Int8Array::from(vec![1i8, 2, 3, 4, 5])),
        None,
    )
    .unwrap();
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy,
        CompilePhase::Decode,
        &control,
    )
    .unwrap();
    assert_eq!(pool.resource_facts().rows, 2);
    work.finish().unwrap();
}

#[test]
fn input_derived_preflight_keeps_first_host_payload_and_has_no_completed_tail() {
    let control = Control(AtomicBool::new(false));
    let field = Field::new("source", DataType::Null, true);
    let ty = FunctionValueType::new(DataType::Null, true);
    let cause = Arc::new("original resource refusal");
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let error = preflight_flat_pool_resources_input_derived_observed(
        &field,
        &ty,
        FlatConstantResourceInput {
            rows: 1,
            buffer_count_upper_bound: 0,
            buffer_visits_bytes_upper_bound: 0,
            retained_buffer_capacity_bytes_upper_bound: 0,
            view_validation_bytes_upper_bound: 0,
        },
        &mut |_| {
            assert!(!control.0.swap(true, Ordering::SeqCst));
            Err(cause.clone())
        },
        &mut work,
    )
    .unwrap_err();
    match error {
        ConstantCreationFailure::Creator(actual) => assert!(Arc::ptr_eq(&actual, &cause)),
        other => panic!("unexpected failure: {other:?}"),
    }
}

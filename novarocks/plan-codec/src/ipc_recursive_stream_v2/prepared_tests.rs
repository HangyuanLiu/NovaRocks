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

fn positive(control: &Control) -> Vec<u32> {
    control
        .trace
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, units)| (*units > 0).then_some(*units))
        .collect()
}
fn every_boundary<T>(call: impl Fn(&Control) -> Result<T, RecursiveReaderError>, successful: bool) {
    let good = Control::default();
    assert_eq!(call(&good).is_ok(), successful);
    let trace = good.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refusing = Control {
                trace: Mutex::new(Vec::new()),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(call(&refusing), Err(RecursiveReaderError::Projection(FlatPoolResourceError::Control(actual))) if actual == cause)
            );
            assert_eq!(*refusing.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn prepared_recursive_moves_original_geometry_and_runs_numerical_model_once() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let value_type = FunctionValueType::new(field.data_type().clone(), true);
    let bytes = fixture(array, &field);
    let baseline = Control::default();
    let original = stream(&bytes, &field)
        .materialize_pool_borrowed(
            Arc::clone(&field),
            &value_type,
            source(&bytes),
            policy(),
            reader_limits(),
            &baseline,
        )
        .unwrap();
    let checked = stream(&bytes, &field);
    let nodes = checked.checked.nodes.as_ptr();
    let scratch_bytes = checked.checked.scratch_request_bytes;
    let scratch_count = checked.checked.scratch_request_count;
    let preparation = Control::default();
    let prepared = checked
        .prepare_pool_borrowed(
            Arc::clone(&field),
            &value_type,
            source(&bytes),
            policy(),
            reader_limits(),
            &preparation,
        )
        .unwrap();
    assert_eq!(prepared.stream.checked.nodes.as_ptr(), nodes);
    assert_eq!(prepared.geometry_scratch_request_bytes(), scratch_bytes);
    assert_eq!(prepared.geometry_scratch_request_count(), scratch_count);
    assert!(scratch_bytes > 0);
    assert!(prepared.facts().new_allocation_request_bytes_upper_bound >= scratch_bytes);
    let consumption = Control::default();
    let decoded = prepared.materialize(&consumption).unwrap();
    assert!(Arc::ptr_eq(decoded.field_ref(), &field));
    assert!(
        original
            .value(1)
            .unwrap()
            .equals_observed(
                &decoded.value(1).unwrap(),
                CompilePhase::Decode,
                &Control::default()
            )
            .unwrap()
    );
    let mut split = positive(&preparation);
    split.extend(positive(&consumption));
    assert_eq!(split, positive(&baseline));
}
#[test]
fn prepared_recursive_preparation_success_and_ordinary_failure_keep_every_original_boundary() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let value_type = FunctionValueType::new(field.data_type().clone(), true);
    let bytes = fixture(array, &field);
    for successful in [true, false] {
        let limits = if successful {
            reader_limits()
        } else {
            RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 0,
                ..reader_limits()
            }
        };
        every_boundary(
            |control| {
                stream(&bytes, &field).prepare_pool_borrowed(
                    Arc::clone(&field),
                    &value_type,
                    source(&bytes),
                    policy(),
                    limits,
                    control,
                )
            },
            successful,
        );
    }
    let foreign = Arc::new(field.as_ref().clone());
    every_boundary(
        |control| {
            stream(&bytes, &field).prepare_pool_borrowed(
                Arc::clone(&foreign),
                &value_type,
                source(&bytes),
                policy(),
                reader_limits(),
                control,
            )
        },
        false,
    );
}
#[test]
fn prepared_recursive_consumption_success_and_data_failure_keep_every_original_boundary() {
    for successful in [true, false] {
        let child: ArrayRef = Arc::new(
            Decimal128Array::from(vec![if successful { 7 } else { 99 }])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        );
        let child_field = Arc::new(Field::new("value", child.data_type().clone(), true));
        let array: ArrayRef = Arc::new(StructArray::new(
            vec![child_field].into(),
            vec![child],
            None,
        ));
        let field = Arc::new(Field::new("original", array.data_type().clone(), true));
        let value_type = FunctionValueType::new(field.data_type().clone(), true);
        let bytes = fixture(array, &field);
        every_boundary(
            |control| {
                let prepared = stream(&bytes, &field)
                    .prepare_pool_borrowed(
                        Arc::clone(&field),
                        &value_type,
                        source(&bytes),
                        policy(),
                        reader_limits(),
                        &Control::default(),
                    )
                    .unwrap();
                prepared.materialize(control)
            },
            successful,
        );
    }
}

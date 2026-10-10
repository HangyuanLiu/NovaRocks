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

fn positive(trace: Vec<(CompilePhase, u32)>) -> Vec<u32> {
    trace
        .into_iter()
        .filter_map(|(_, units)| (units > 0).then_some(units))
        .collect()
}
fn every_boundary<T>(call: impl Fn(&Control) -> Result<T, FlatReaderError>, successful: bool) {
    let good = Control::good();
    assert_eq!(call(&good).is_ok(), successful);
    let trace = good.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refusing = Control::refusing(at, cause);
            assert!(
                matches!(call(&refusing), Err(FlatReaderError::Projection(FlatPoolResourceError::Control(actual))) if actual == cause)
            );
            assert_eq!(refusing.trace(), trace[..=at]);
        }
    }
}
#[test]
fn prepared_flat_retains_original_snapshot_and_runs_numerical_model_once() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)]));
    let field = Arc::new(Field::new("original", DataType::Int64, true));
    let value_type = FunctionValueType::new(DataType::Int64, true);
    let bytes = writer_stream(array, &field);
    let baseline = Control::good();
    let original = checked(&bytes, &field)
        .materialize_pool_borrowed(
            Arc::clone(&field),
            &value_type,
            retained(&bytes, &field),
            policy(),
            reader_limits(),
            &baseline,
        )
        .unwrap();
    let preparation = Control::good();
    let prepared = checked(&bytes, &field)
        .prepare_pool_borrowed(
            Arc::clone(&field),
            &value_type,
            retained(&bytes, &field),
            policy(),
            reader_limits(),
            &preparation,
        )
        .unwrap();
    assert_eq!(prepared.geometry_scratch_request_bytes(), 0);
    assert_eq!(prepared.geometry_scratch_request_count(), 0);
    assert!(prepared.facts().new_allocation_request_bytes_upper_bound > 0);
    let materialization = Control::good();
    let decoded = prepared.materialize(&materialization).unwrap();
    assert!(Arc::ptr_eq(decoded.field_ref(), &field));
    assert_eq!(decoded.value(2).unwrap().try_i64().unwrap(), Some(-9));
    assert!(
        original
            .value(1)
            .unwrap()
            .equals_observed(
                &decoded.value(1).unwrap(),
                CompilePhase::Decode,
                &Control::good()
            )
            .unwrap()
    );
    // Only the two legitimate phase boundaries differ. Re-running the model
    // would duplicate its completed positive work in the consuming phase.
    let mut split = positive(preparation.trace());
    split.extend(positive(materialization.trace()));
    assert_eq!(split, positive(baseline.trace()));
}
#[test]
fn prepared_flat_preparation_success_and_ordinary_failure_keep_every_original_boundary() {
    let field = Arc::new(Field::new("original", DataType::Int64, true));
    let value_type = FunctionValueType::new(DataType::Int64, true);
    let bytes = writer_stream(Arc::new(Int64Array::from(vec![7])), &field);
    for successful in [true, false] {
        let limits = if successful {
            reader_limits()
        } else {
            FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 0,
                ..reader_limits()
            }
        };
        every_boundary(
            |control| {
                checked(&bytes, &field).prepare_pool_borrowed(
                    Arc::clone(&field),
                    &value_type,
                    retained(&bytes, &field),
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
            checked(&bytes, &field).prepare_pool_borrowed(
                Arc::clone(&foreign),
                &value_type,
                retained(&bytes, &field),
                policy(),
                reader_limits(),
                control,
            )
        },
        false,
    );
}
#[test]
fn prepared_flat_consumption_success_and_data_failure_keep_every_original_boundary() {
    for successful in [true, false] {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![if successful { 7 } else { 99 }])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        );
        let field = Arc::new(Field::new("original", DataType::Decimal128(1, 0), true));
        let value_type = FunctionValueType::new(field.data_type().clone(), true);
        let bytes = writer_stream(array, &field);
        every_boundary(
            |control| {
                let prepared = checked(&bytes, &field)
                    .prepare_pool_borrowed(
                        Arc::clone(&field),
                        &value_type,
                        retained(&bytes, &field),
                        policy(),
                        reader_limits(),
                        &Control::good(),
                    )
                    .unwrap();
                prepared.materialize(control)
            },
            successful,
        );
    }
}

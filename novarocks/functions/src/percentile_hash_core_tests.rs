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
use crate::{KernelDiagnostic, KernelFailure};
use arrow_array::{ArrayRef, Float64Array, Int32Array, StringArray};
use std::sync::Arc;
const EMPTY: &[u8] = &[0xa2, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0];
#[test]
fn percentile_hash_core_original_null_nan_f32_and_signed_zero() {
    let a = Arc::new(Float64Array::from(vec![
        None,
        Some(f64::NAN),
        Some(16777217.),
        Some(-0.),
    ])) as ArrayRef;
    assert_eq!(row(&a, 0).unwrap(), EMPTY);
    assert_eq!(row(&a, 1).unwrap(), EMPTY);
    let bytes = row(&a, 2).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[55..59].try_into().unwrap()),
        16777216f32.to_bits()
    );
    let bytes = row(&a, 3).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[55..59].try_into().unwrap()),
        (-0f32).to_bits()
    );
}
#[test]
fn percentile_hash_core_original_long_error_has_no_success_footer() {
    let a = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
    let mut trace = vec![];
    let error = row_observed(&a, 0, |what| {
        trace.push(what);
        Ok::<_, ()>(())
    })
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error,
        "percentile_hash: unsupported numeric input type Utf8"
    );
    assert_eq!(trace, vec![Observation::ReadBoundary]);
}
#[test]
fn percentile_hash_core_observes_all_seven_causes_without_tail() {
    let a = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let mut success = vec![];
    row_observed(&a, 0, |what| {
        success.push(what);
        Ok::<_, KernelFailure>(())
    })
    .unwrap()
    .unwrap();
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("input")),
        KernelFailure::Internal(KernelDiagnostic::new("internal")),
        KernelFailure::Operational(KernelDiagnostic::new("operational")),
        KernelFailure::InstanceFailed,
    ];
    for stop in 0..success.len() {
        for cause in &causes {
            let mut trace = vec![];
            assert_eq!(
                row_observed(&a, 0, |what| {
                    let at = trace.len();
                    trace.push(what);
                    if at == stop {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err(),
                *cause
            );
            assert_eq!(trace, success[..=stop]);
        }
    }
}

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

//! Storage order and strict state validation are inherited legacy contracts.
use super::*;
use crate::KernelFailure;
use std::{sync::Mutex, time::Duration};
struct OrderedKeys(Vec<Vec<u8>>);
impl NumericDistinctSet for OrderedKeys {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.0.iter().map(Vec::as_slice)
    }
}
#[derive(Default)]
struct Buffer(Vec<u8>);
impl NumericDistinctBuffer for Buffer {
    fn reserve_exact(&mut self, size: usize) -> Result<(), String> {
        self.0.try_reserve_exact(size).map_err(|e| e.to_string())
    }
    fn append(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    failure: Option<KernelFailure>,
}
impl crate::KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        self.trace.lock().unwrap().push(n);
        if n > 0 {
            if let Some(e) = &self.failure {
                return Err(e.clone());
            }
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric distinct never waits")
    }
}
#[test]
fn floating_results_and_wire_bytes_keep_the_storage_iteration_order() {
    let a = OrderedKeys(
        [1e16f64, 1.0, -1e16]
            .map(|v| v.to_le_bytes().to_vec())
            .into(),
    );
    let b = OrderedKeys(
        [1e16f64, -1e16, 1.0]
            .map(|v| v.to_le_bytes().to_vec())
            .into(),
    );
    let control = Control::default();
    let mut w = EvaluationCheckpoints::new(&control);
    for (set, sum, mean) in [(&a, 0.0, 0.0), (&b, 1.0, 1.0 / 3.0)] {
        let out = sum_from_set(set, &DataType::Float64, &DataType::Float64, &mut w).unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            sum
        );
        let out = avg_from_set(set, &DataType::Float64, &DataType::Float64, &mut w).unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            mean
        );
    }
    let mut out = Buffer::default();
    serialize_set_into(&a, &mut out, &mut w).unwrap();
    assert_eq!(out.0.len(), 40);
    assert_eq!(&out.0[..4], &3u32.to_le_bytes());
    assert_eq!(&out.0[20..28], &1.0f64.to_le_bytes());
    let mut other = Buffer::default();
    serialize_set_into(&b, &mut other, &mut w).unwrap();
    assert_eq!(&other.0[20..28], &(-1e16f64).to_le_bytes());
    assert_ne!(out.0, other.0);
    // A corrupt final width must be refused before the first insertion.
    let mut malformed = out.0.clone();
    malformed[28..32].copy_from_slice(&4u32.to_le_bytes());
    let mut inserts = 0;
    let error = visit_serialized_keys(&malformed, 8, &mut w, |_| {
        inserts += 1;
        Ok(())
    })
    .unwrap_err()
    .into_legacy_message();
    assert_eq!(inserts, 0);
    assert_eq!(
        error,
        "distinct set key width differs from selected input type"
    );
    let mut malformed = out.0;
    malformed.push(0);
    assert_eq!(
        visit_serialized_keys(&malformed, 8, &mut w, |_| {
            inserts += 1;
            Ok(())
        })
        .unwrap_err()
        .into_legacy_message(),
        "invalid distinct set payload length"
    );
    assert_eq!(inserts, 0);
}
#[test]
fn shared_distinct_numeric_control_failures_remain_typed() {
    let keys = OrderedKeys((0..513i64).map(|v| v.to_le_bytes().to_vec()).collect());
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InstanceFailed,
        crate::kernel_control::invalid("original refusal"),
        crate::kernel_control::internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
    ] {
        let c = Control {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        let mut w = EvaluationCheckpoints::new(&c);
        assert_eq!(
            sum_from_set(&keys, &DataType::Int64, &DataType::Int64, &mut w)
                .unwrap_err()
                .into_kernel_failure(),
            failure
        );
        assert_eq!(c.trace.lock().unwrap().as_slice(), &[256]);
    }
}

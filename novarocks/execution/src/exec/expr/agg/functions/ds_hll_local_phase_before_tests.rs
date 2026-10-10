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

//! Original aggregate call chronology over the exact native witness value set.
use super::*;
use arrow::array::{Array, BinaryArray, Int64Array};

#[test]
fn ds_hll_local_phase_before_original_100k() {
    let tracker = MemTracker::new_root("DsHllLocalPhaseOriginal");
    let source = Arc::new(Int64Array::from_iter_values(1..=100_000)) as ArrayRef;
    let input = spec(
        "ds_hll_count_distinct",
        &DataType::Int64,
        false,
        DataType::Int64,
    );
    let merge = spec(
        "ds_hll_count_distinct",
        &DataType::Binary,
        true,
        DataType::Int64,
    );
    {
        let mut single = DsHllState::new(tracker.clone());
        update(&input, &source, &mut single).unwrap();
        let final_array = build(&input, &single, false);
        let payload = build(&input, &single, true);
        let bytes = payload
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        let value = final_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(bytes[3], 17, "original default lg_k");
        println!(
            "DS_PHASE ORIGINAL_RAW estimate={value} flags={} length={}",
            bytes[5],
            bytes.len()
        );
        for reverse in [false, true] {
            let mut left = DsHllState::new(tracker.clone());
            let mut right = DsHllState::new(tracker.clone());
            update(&input, &source.slice(0, 50_000), &mut left).unwrap();
            update(&input, &source.slice(50_000, 50_000), &mut right).unwrap();
            let left_payload = build(&input, &left, true);
            let right_payload = build(&input, &right, true);
            let left_bytes = left_payload
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            let right_bytes = right_payload
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            let payloads = if reverse {
                vec![right_bytes, left_bytes]
            } else {
                vec![left_bytes, right_bytes]
            };
            let states = Arc::new(BinaryArray::from_iter_values(payloads)) as ArrayRef;
            let mut result = DsHllState::new(tracker.clone());
            let pointer = &mut result as *mut DsHllState as AggStatePtr;
            DsHllAgg
                .merge_batch(&merge, 0, &[pointer, pointer], &AggInputView::Any(&states))
                .unwrap();
            let final_array = build(&merge, &result, false);
            let payload = build(&merge, &result, true);
            let bytes = payload
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            let value = final_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            println!(
                "DS_PHASE ORIGINAL_TWO reverse={reverse} estimate={value} flags={} length={} left_flags={} right_flags={}",
                bytes[5],
                bytes.len(),
                left_bytes[5],
                right_bytes[5]
            );
        }
    }
    assert_eq!(tracker.current(), 0, "all actual retained charges drop");
}

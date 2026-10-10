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

//! Original state chronology of every key in the exact native grouped witness.
use super::*;
#[test]
fn ds_hll_grouped_local_before_original_native_100k() {
    let tracker = MemTracker::new_root("DsHllGroupedOriginal");
    {
        for group in 0..8 {
            let ids = (1..=100_000_i64)
                .filter(|id| id % 8 == group)
                .collect::<Vec<_>>();
            let array = packed(
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(vec![10; ids.len()])),
                Some(Arc::new(StringArray::from(vec!["HLL_6"; ids.len()]))),
            );
            let current = spec(
                "ds_hll_count_distinct",
                array.data_type(),
                false,
                DataType::Int64,
            );
            let mut state = DsHllState::new(tracker.clone());
            update(&current, &array, &mut state).unwrap();
            let out = build(&current, &state, false);
            let payload = build(&current, &state, true);
            let bytes = payload
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            println!(
                "DS_GROUPED ORIGINAL_RAW grp={group} estimate={} flags={}",
                out.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
                bytes[5]
            );
            let split = ids.partition_point(|id| *id <= 50_000);
            let mut left = DsHllState::new(tracker.clone());
            let mut right = DsHllState::new(tracker.clone());
            update(&current, &array.slice(0, split), &mut left).unwrap();
            update(&current, &array.slice(split, ids.len() - split), &mut right).unwrap();
            let left_payload = build(&current, &left, true);
            let right_payload = build(&current, &right, true);
            for reverse in [false, true] {
                let a = left_payload
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0);
                let b = right_payload
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0);
                let input = Arc::new(BinaryArray::from_iter_values(if reverse {
                    vec![b, a]
                } else {
                    vec![a, b]
                })) as ArrayRef;
                let merge = spec(
                    "ds_hll_count_distinct",
                    &DataType::Binary,
                    true,
                    DataType::Int64,
                );
                let mut final_state = DsHllState::new(tracker.clone());
                let ptr = &mut final_state as *mut DsHllState as AggStatePtr;
                DsHllAgg
                    .merge_batch(&merge, 0, &[ptr, ptr], &AggInputView::Any(&input))
                    .unwrap();
                let out = build(&merge, &final_state, false);
                let payload = build(&merge, &final_state, true);
                let bytes = payload
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0);
                println!(
                    "DS_GROUPED ORIGINAL_TWO grp={group} reverse={reverse} estimate={} flags={}",
                    out.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
                    bytes[5]
                );
            }
        }
    }
    assert_eq!(tracker.current(), 0);
}

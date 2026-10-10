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
//! Independent original analytic COUNT frame receipts; child of analytic_shared.
use super::*;
use arrow::array::{Int64Array, StringArray};
use arrow::array::{DictionaryArray, Int8Array, NullArray};
use arrow::datatypes::Int8Type;
fn values(output: ArrayRef) -> Vec<i64> {
    assert_eq!(output.data_type(), &DataType::Int64);
    assert_eq!(output.null_count(), 0);
    output
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
#[test]
fn legacy_analytic_count_original_star_expr_and_empty_frames() {
    let ctx = PartitionWindowContext {
        partitions: vec![(0, 3), (3, 5)],
        peer_groups_by_partition: vec![vec![(0, 3)], vec![(3, 5)]],
        frames_by_partition: vec![vec![(0, 0), (0, 2), (1, 3)], vec![(3, 5), (4, 4)]],
    };
    assert_eq!(
        values(compute_count(&[], &ctx, 5).unwrap()),
        [0, 2, 2, 2, 0]
    );
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        None,
        Some(3),
        None,
        Some(5),
    ]));
    assert_eq!(
        values(compute_count(&[source.clone()], &ctx, 5).unwrap()),
        [0, 1, 1, 1, 0]
    );
    assert_eq!(
        values(compute_count(&[source, Arc::new(NullArray::new(0))], &ctx, 5).unwrap()),
        [0, 1, 1, 1, 0]
    );
    let empty = PartitionWindowContext::new(&[], &[], None).unwrap();
    assert!(values(compute_count(&[], &empty, 0).unwrap()).is_empty());
}
#[test]
fn legacy_analytic_count_original_bare_null_and_dictionary_root_nulls() {
    let ctx = PartitionWindowContext::new(&[(0, 5)], &[], None).unwrap();
    let bare: ArrayRef = Arc::new(NullArray::new(5));
    assert_eq!(values(compute_count(&[bare], &ctx, 5).unwrap()), [5; 5]);
    let dict: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        )
        .unwrap(),
    );
    assert_eq!(values(compute_count(&[dict], &ctx, 5).unwrap()), [4; 5]);
}
#[test]
fn legacy_analytic_count_original_frame_errors_stay_whole_and_exact() {
    let ctx = PartitionWindowContext {
        partitions: vec![(0, 1)],
        peer_groups_by_partition: vec![],
        frames_by_partition: vec![],
    };
    assert_eq!(
        compute_count(&[], &ctx, 1).unwrap_err(),
        "window partition index out of range: 0"
    );
}

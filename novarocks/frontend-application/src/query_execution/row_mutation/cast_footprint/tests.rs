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
use arrow::{
    array::{DictionaryArray, Int32Array, Int64Array, StringArray},
    datatypes::Int32Type,
};
use std::sync::Arc;
fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}
#[test]
fn layout_preflight_counts_independent_owner_copies_of_shared_backing() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7; 128]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let input = RecordBatch::try_new(schema.clone(), vec![array.clone(), array]).unwrap();
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    assert_eq!(p.output_bytes, 0);
    assert!(p.copy_peak_bytes >= 2 * 128 * 8);
    assert!(p.peak_bytes >= p.input_bytes + p.copy_peak_bytes + p.temporary_bytes);
}
#[test]
fn layout_preflight_dictionary_matches_actual_expansion_and_copy() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 0, 1]),
            Arc::new(StringArray::from(vec!["abc", "longer"])),
        )
        .unwrap(),
    );
    let input = batch(dictionary);
    let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    let out = super::super::signed_layout::normalize(&Arc::new(schema), &input).unwrap();
    assert!(p.output_bytes >= out.column(0).get_buffer_memory_size());
    assert!(p.copy_peak_bytes >= 18 + 5 * 4);
    assert!(p.output_bytes > 0);
}
#[test]
fn layout_preflight_rejects_value_conversion_before_normalization() {
    let input = batch(Arc::new(Int64Array::from(vec![7])));
    let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
    let e = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
    assert_eq!(e.kind(), ConnectorErrorKind::InvalidRequest);
}
#[test]
fn layout_preflight_fixed_peak_is_enforced_with_retained_and_other_owners() {
    let input = batch(Arc::new(Int64Array::from(vec![7])));
    let schema = input.schema();
    let p = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap();
    assert!(
        CowSignedLayoutFootprint::with_limit(&schema, &input, 0, 0, 0, p.peak_bytes - 1).is_err()
    );
    assert!(
        CowSignedLayoutFootprint::for_batch(&schema, &input, SELECTION_BYTES + 1, 0, 0).is_err()
    );
    assert!(CowSignedLayoutFootprint::for_batch(&schema, &input, 0, INPUT_BYTES, 0).is_err());
    assert!(CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, TRANSFORM_BYTES).is_err());
}

#[test]
fn layout_preflight_rejects_large_dictionary_expansion_without_allocating_it() {
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; MAX_ROWS]),
            Arc::new(StringArray::from(vec!["x".repeat(512)])),
        )
        .unwrap(),
    );
    let input = batch(array);
    let schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
    let error = CowSignedLayoutFootprint::for_batch(&schema, &input, 0, 0, 0).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
}

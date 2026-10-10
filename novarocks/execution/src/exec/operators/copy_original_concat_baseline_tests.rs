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

//! Raw original Arrow concat authors used by ONE analytic gather. These
//! probes distinguish concat resources from MutableArrayData semantics.
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int16Array, Int64Array, StringArray,
    ListViewArray, RunArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Int8Type, Int16Type};
use std::sync::Arc;

#[test]
fn by_original_concat_dictionary_overflow_domain_can_merge_successfully() {
    let make = |prefix: &str| {
        let values = Arc::new(StringArray::from_iter_values(
            (0..70).map(|i| format!("{prefix}{i}")),
        ));
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0_i8]), values).unwrap()
    };
    let a = make("first-");
    let b = make("second-");
    // 140 > Int8::MAX, but referenced merged values need only keys 0 and 1.
    let actual = arrow::compute::concat(&[&a, &b]).unwrap();
    let actual = actual
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(actual.keys().values().as_ref(), &[0_i8, 1]);
    let values = actual
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), "first-0");
    assert_eq!(values.value(1), "second-0");
}

#[test]
fn by_original_concat_list_view_copies_entire_unused_child_domain() {
    let mut field = Field::new("original_items", DataType::Int64, false);
    field.set_metadata(std::collections::HashMap::from([(
        "PARQUET:field_id".into(),
        "7".into(),
    )]));
    let field = Arc::new(field);
    let a = ListViewArray::try_new(
        field.clone(),
        ScalarBuffer::from(vec![2_i32]),
        ScalarBuffer::from(vec![0_i32]),
        Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
        None,
    )
    .unwrap();
    let b = ListViewArray::try_new(
        field.clone(),
        ScalarBuffer::from(vec![1_i32]),
        ScalarBuffer::from(vec![0_i32]),
        Arc::new(Int64Array::from(vec![20, 21, 22])),
        None,
    )
    .unwrap();
    let actual = arrow::compute::concat(&[&a, &b]).unwrap();
    let actual = actual.as_any().downcast_ref::<ListViewArray>().unwrap();
    assert_eq!(actual.len(), 2);
    assert_eq!(actual.value_offsets(), &[2_i32, 5]);
    assert_eq!(actual.value_sizes(), &[0_i32, 0]);
    assert_eq!(
        actual.values().len(),
        7,
        "zero selected view sizes do not eliminate original child concat"
    );
    assert_eq!(actual.data_type(), &DataType::ListView(field));
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10_i64, 11, 12, 13, 20, 21, 22]
    );
}

#[test]
fn by_original_concat_run_logical_rows_and_physical_buffers_are_distinct() {
    let values_a: ArrayRef = Arc::new(Int64Array::from(vec![10, 11]));
    let values_b: ArrayRef = Arc::new(Int64Array::from(vec![20, 21]));
    let a =
        RunArray::<Int16Type>::try_new(&Int16Array::from(vec![500_i16, 1000]), values_a.as_ref())
            .unwrap();
    let b =
        RunArray::<Int16Type>::try_new(&Int16Array::from(vec![600_i16, 1000]), values_b.as_ref())
            .unwrap();
    let actual = arrow::compute::concat(&[&a, &b]).unwrap();
    let actual = actual
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(actual.len(), 2000);
    assert_eq!(actual.run_ends().values(), &[500_i16, 1000, 1600, 2000]);
    assert_eq!(actual.values().len(), 4);
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10_i64, 11, 20, 21]
    );
}

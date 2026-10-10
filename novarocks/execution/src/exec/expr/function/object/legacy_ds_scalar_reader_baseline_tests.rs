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

//! Original recursive scalar-reader observations before the generic sink extraction.
use crate::exec::expr::agg::{AggScalarValue as V, agg_scalar_from_array};
use arrow::array::types::Int64Type;
use arrow::array::{ArrayRef, Int64Array, ListArray, StringArray, StructArray, UInt32Array};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;
#[test]
fn legacy_ds_scalar_reader_baseline_nested_slice_and_null() {
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(3), None, Some(-7)]),
        None,
        Some(vec![]),
    ]);
    let array: ArrayRef = Arc::new(list);
    assert_eq!(
        format!("{:?}", agg_scalar_from_array(&array, 0).unwrap()),
        "Some(List([Some(Int64(3)), None, Some(Int64(-7))]))"
    );
    assert!(agg_scalar_from_array(&array, 1).unwrap().is_none());
    let sliced = array.slice(2, 1);
    assert_eq!(
        format!("{:?}", agg_scalar_from_array(&sliced, 0).unwrap()),
        "Some(List([]))"
    );
}
#[test]
fn legacy_ds_scalar_reader_baseline_struct_field_order_and_owned_bytes() {
    let fields = vec![
        Arc::new(Field::new("first", DataType::Utf8, true)),
        Arc::new(Field::new("second", DataType::Int64, true)),
    ];
    let array: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![
            Arc::new(StringArray::from(vec![Some("é\0"), None])),
            Arc::new(Int64Array::from(vec![Some(-5), None])),
        ],
        None,
    ));
    let Some(V::Struct(fields)) = agg_scalar_from_array(&array, 0).unwrap() else {
        panic!("expected original Struct");
    };
    assert!(matches!(&fields[0],Some(V::Utf8(v)) if v == "é\0"));
    assert!(matches!(&fields[1], Some(V::Int64(-5))));
    let Some(V::Struct(fields)) = agg_scalar_from_array(&array, 1).unwrap() else {
        panic!("expected original Struct");
    };
    assert!(fields.iter().all(Option::is_none));
}
#[test]
fn legacy_ds_scalar_reader_baseline_lazy_nested_unsupported_actual_carrier() {
    use arrow::buffer::OffsetBuffer;
    use arrow::buffer::ScalarBuffer;
    let child: ArrayRef = Arc::new(UInt32Array::from(vec![Some(8)]));
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("value", DataType::UInt32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0, 1])),
        child,
        None,
    ));
    assert!(matches!(agg_scalar_from_array(&array,0).unwrap(),Some(V::List(v)) if v.is_empty()));
    assert_eq!(
        agg_scalar_from_array(&array, 1).unwrap_err(),
        "unsupported scalar type: UInt32"
    );
    let null_unsupported: ArrayRef = Arc::new(UInt32Array::from(vec![None]));
    assert_eq!(
        agg_scalar_from_array(&null_unsupported, 0).unwrap_err(),
        "unsupported scalar type: UInt32"
    );
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Every admitted concrete flat NULLIF profile uses the v1 differential oracle.
use super::*;
use arrow::array::{
    BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::TimeUnit;

#[test]
fn pure_differential_nullif_all_flat_numeric_date_and_timestamp_profiles() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(3),
        Some(5),
        Some(0),
        None,
    ]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(9),
        Some(2),
        None,
        Some(5),
        Some(1),
        None,
    ]));
    let profiles = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ];
    for (i, ty) in profiles.into_iter().enumerate() {
        let (left, right) = if ty == DataType::Date32 {
            (
                Arc::new(arrow::array::Date32Array::from(vec![
                    Some(1),
                    Some(2),
                    None,
                    Some(3),
                    Some(5),
                    Some(0),
                    None,
                ])) as ArrayRef,
                Arc::new(arrow::array::Date32Array::from(vec![
                    Some(1),
                    Some(9),
                    Some(2),
                    None,
                    Some(5),
                    Some(1),
                    None,
                ])) as ArrayRef,
            )
        } else {
            (
                arrow::compute::cast(left.as_ref(), &ty).unwrap(),
                arrow::compute::cast(right.as_ref(), &ty).unwrap(),
            )
        };
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("nullif")
                .column(left.clone())
                .column(right.clone())
                .sparse_selections(12, 171 + i as u64),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("nullif")
                .column(left.slice(1, 5))
                .column(right.slice(1, 5))
                .sparse_selections(12, 190 + i as u64),
        );
    }
}
fn assert_bound_coerced_inputs(left: ArrayRef, right: ArrayRef, seed: u64) {
    let arguments = [left, right].map(|values| DiffArgument::Column {
        value_type: FunctionValueType::new(values.data_type().clone(), true),
        values,
    });
    let binding = resolve_like_sql(
        builtin_engine_function_catalog(),
        "nullif",
        CatalogKind::Scalar,
        &arguments,
    )
    .unwrap();
    let mut spec = ScalarDiffSpec::new("nullif").sparse_selections(12, seed);
    for (argument, selected) in arguments.iter().zip(binding.selected.argument_types.iter()) {
        let FunctionArgumentType::Value(value_type) = selected else {
            panic!("NULLIF selected a non-value argument");
        };
        let DiffArgument::Column { values, .. } = argument else {
            unreachable!();
        };
        let coerced = arrow::compute::cast(values.as_ref(), &value_type.data_type).unwrap();
        spec = spec.typed_column(value_type.clone(), coerced);
    }
    assert_scalar_matches_v1(spec);
}

#[test]
fn pure_differential_nullif_decimal_metadata_and_mixed_width_coercion() {
    for (precision, scale) in [(9, 0), (18, 2), (38, -3)] {
        let left: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, Some(3), Some(5)])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        );
        let right: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(9), Some(2), None, Some(5)])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("nullif")
                .column(left)
                .column(right)
                .sparse_selections(12, 220),
        );
    }
    let left: ArrayRef = Arc::new(arrow::array::Int8Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(3),
    ]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(9), Some(2), None]));
    assert_bound_coerced_inputs(left, right, 221);
    let left: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(100), Some(201), None])
            .with_precision_and_scale(9, 2)
            .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(10), Some(20), None])
            .with_precision_and_scale(9, 1)
            .unwrap(),
    );
    assert_bound_coerced_inputs(left, right, 222);
}
#[test]
fn pure_differential_nullif_boolean_utf8_broadcast_and_zero_rows() {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
            ])))
            .column(Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                None,
            ])))
            .sparse_selections(12, 230),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(StringArray::from(vec![
                Some(""),
                Some("中"),
                None,
                Some("keep"),
                Some("equal"),
                Some("a\0b"),
            ])))
            .column(Arc::new(StringArray::from(vec![
                Some(""),
                Some("different"),
                Some("b"),
                None,
                Some("equal"),
                Some("a\0b"),
            ])))
            .sparse_selections(12, 231),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(StringArray::from(vec![
                Some("same"),
                None,
                Some("other"),
            ])))
            .constant_array(Arc::new(StringArray::from(vec!["same"])))
            .sparse_selections(12, 232),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])))
            .constant_array(Arc::new(Int64Array::from(vec![1])))
            .sparse_selections(12, 233),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(Int64Array::from(Vec::<Option<i64>>::new())))
            .column(Arc::new(Int64Array::from(Vec::<Option<i64>>::new())))
            .sparse_selections(12, 234),
    );
}
#[test]
fn pure_differential_nullif_float_special_native_equalities() {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(-0.0),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(-0.0),
                None,
            ])))
            .column(Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(0.0),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                None,
                Some(1.0),
            ])))
            .sparse_selections(12, 240),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("nullif")
            .column(Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(-0.0),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(-0.0),
                None,
            ])))
            .column(Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(0.0),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                None,
                Some(1.0),
            ])))
            .sparse_selections(12, 241),
    );
}

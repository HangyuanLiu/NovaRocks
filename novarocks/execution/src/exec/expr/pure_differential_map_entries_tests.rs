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
//! Permanent actual MAP_ENTRIES dynamic-v1 profile matrix.
use super::super::legacy_map_entries_baseline_tests::{fixture, map};
use super::generate::{InputGenerator, InputProfile};
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray, new_empty_array};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::OffsetBuffer;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
fn check(a: ArrayRef, nullable: bool) {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("map_entries")
            .typed_column(FunctionValueType::new(a.data_type().clone(), nullable), a)
            .float_comparison(super::FloatComparison::Exact)
            .sparse_selections(7, 7712),
    );
}
#[test]
fn pure_differential_map_entries_dynamic_v1_all_flat_child_domains() {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Decimal128(38, -2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 6),
    ];
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(u, None));
    }
    for ty in types {
        for sorted in [false, true] {
            for nullable in [false, true] {
                let n = 514;
                let v = InputGenerator::new(7713).column(
                    &FunctionValueType::new(ty.clone(), true),
                    n,
                    &InputProfile::default(),
                );
                let k: ArrayRef = Arc::new(Int32Array::from(
                    (0..n).map(|i| (i % 11) as i32).collect::<Vec<_>>(),
                ));
                let a = map(
                    k,
                    v,
                    (0..=n / 2).map(|i| (i * 2) as i32).collect(),
                    nullable.then(|| (0..n / 2).map(|i| i % 7 != 0).collect()),
                    sorted,
                );
                check(a, nullable);
            }
        }
    }
}
#[test]
fn pure_differential_map_entries_nested_struct_list_and_sliced_carriers() {
    let child: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("actual-item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()),
        Arc::new(Int32Array::from(vec![None, Some(7), Some(2), None])),
        None,
    ));
    let value: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "nested",
            child.data_type().clone(),
            true,
        ))]
        .into(),
        vec![child],
        None,
    ));
    let a = map(
        Arc::new(Int32Array::from(vec![8, 1, 8, 3])),
        value,
        vec![0, 2, 4],
        Some(vec![true, false]),
        false,
    );
    check(a.slice(1, 1), true);
    check(a, true);
}
#[test]
fn pure_differential_map_entries_pool_constant_null_empty_and_nonzero_ordinal() {
    let a = fixture(true);
    let ty = FunctionValueType::new(a.data_type().clone(), true);
    for ordinal in [0, 1, 2, 3] {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("map_entries")
                    .constant_array(a.slice(ordinal, 1))
                    .legacy_constants(form)
                    .constant_rows(257)
                    .sparse_selections(7, 7714),
            );
        }
    }
    check(new_empty_array(&ty.data_type), true);
}
#[test]
fn pure_differential_map_entries_registered_nested_nominal_domains_preserve_tags() {
    for logical in [
        ValueLogicalType::Json,
        ValueLogicalType::Variant,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
        ValueLogicalType::Uuid,
    ] {
        let (dtype, values): (DataType, ArrayRef) = match logical {
            ValueLogicalType::Json => (
                DataType::Utf8,
                Arc::new(arrow::array::StringArray::from(vec![Some("{}"), None])),
            ),
            ValueLogicalType::Variant => (
                DataType::LargeBinary,
                Arc::new(arrow::array::LargeBinaryArray::from(vec![
                    Some(&b"raw"[..]),
                    None,
                ])),
            ),
            ValueLogicalType::LargeInt | ValueLogicalType::Uuid => (
                DataType::FixedSizeBinary(16),
                novarocks_types::largeint::array_from_i128(&[Some(i128::MIN), None]).unwrap(),
            ),
            _ => (
                DataType::Binary,
                Arc::new(arrow::array::BinaryArray::from(vec![
                    Some(&b"opaque"[..]),
                    None,
                ])),
            ),
        };
        let vf = FunctionValueType::try_with_logical_type(dtype, true, logical)
            .unwrap()
            .try_to_field("nominal-value")
            .unwrap();
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(vf),
            ]
            .into(),
            vec![Arc::new(Int32Array::from(vec![1, 1])), values],
            None,
        );
        let a: ArrayRef = Arc::new(arrow::array::MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        ));
        check(a, false);
    }
}

#[test]
fn pure_differential_map_entries_full_nested_carrier_closure_without_key_restriction() {
    use arrow::array::{new_empty_array, new_null_array};
    use arrow::datatypes::{IntervalUnit, UnionFields, UnionMode};
    let field = |ty| {
        Arc::new(
            Field::new("authored-child", ty, true)
                .with_metadata([("opaque".into(), "retained".into())].into()),
        )
    };
    let mut types = vec![
        DataType::Date64,
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::FixedSizeBinary(0),
        DataType::FixedSizeBinary(3),
        DataType::FixedSizeBinary(16),
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::List(field(DataType::Utf8)),
        DataType::LargeList(field(DataType::Int64)),
        DataType::FixedSizeList(field(DataType::Int32), 3),
        DataType::ListView(field(DataType::Utf8)),
        DataType::LargeListView(field(DataType::Utf8)),
        DataType::Struct(
            vec![
                field(DataType::Decimal128(38, -38)),
                field(DataType::List(field(DataType::Utf8))),
            ]
            .into(),
        ),
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        ),
    ];
    let inner_entries = Arc::new(Field::new(
        "inner",
        DataType::Struct(
            vec![
                Arc::new(Field::new("inner-key", DataType::Int32, false)),
                field(DataType::Utf8),
            ]
            .into(),
        ),
        false,
    ));
    types.push(DataType::Map(inner_entries, false));
    for mode in [UnionMode::Sparse, UnionMode::Dense] {
        types.push(DataType::Union(
            UnionFields::try_new(
                [0, 3],
                [
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ],
            )
            .unwrap(),
            mode,
        ));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Duration(unit));
        types.push(DataType::Timestamp(unit, Some("UTC".into())));
    }
    for ty in types {
        for sorted in [false, true] {
            let a = map(
                Arc::new(Int32Array::from(vec![3, 1, 3, 2])),
                new_null_array(&ty, 4),
                vec![0, 2, 3, 3, 4],
                Some(vec![true, false, true, true]),
                sorted,
            );
            check(a.clone(), true);
            check(a.slice(1, 3), true);
            check(new_empty_array(a.data_type()), false);
            // Any valid key type is admitted: no ordered-compare decoder belongs here.
            let key = new_empty_array(&ty);
            let value = new_empty_array(&DataType::Utf8);
            check(map(key, value, vec![0], None, sorted), false);
        }
    }
}

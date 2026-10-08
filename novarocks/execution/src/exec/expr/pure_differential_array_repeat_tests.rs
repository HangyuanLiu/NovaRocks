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
//! Permanent ARRAY_REPEAT accurate dynamic source/count profiles, before and after owner installation.
use super::generate::{InputGenerator, InputProfile};
use super::{
    FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant,
};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt32Array,
    new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 257;
fn flat_profiles() -> Vec<FunctionValueType> {
    let mut p = vec![
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
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 6),
    ]
    .into_iter()
    .map(|t| FunctionValueType::new(t, true))
    .collect::<Vec<_>>();
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        p.push(FunctionValueType::new(DataType::Timestamp(u, None), true))
    }
    for precision in [9u8, 18, 38] {
        for scale in [-2i8, 0, 2, precision as i8] {
            p.push(FunctionValueType::new(
                DataType::Decimal128(precision, scale),
                true,
            ));
        }
    }
    for precision in [40u8, 60, 76] {
        for scale in [-2i8, 0, 2, 6] {
            p.push(FunctionValueType::new(
                DataType::Decimal256(precision, scale),
                true,
            ));
        }
    }
    p.push(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    p.push(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    p.push(
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Variant,
        )
        .unwrap(),
    );
    for l in [
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        p.push(FunctionValueType::try_with_logical_type(DataType::Binary, true, l).unwrap())
    }
    p.push(FunctionValueType::new(DataType::FixedSizeBinary(16), true));
    p
}
fn values(t: &FunctionValueType, rows: usize, seed: u64) -> ArrayRef {
    if t.data_type == DataType::FixedSizeBinary(16) {
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|r| {
                    if t.nullable && r % 7 == 0 {
                        None
                    } else {
                        Some([i128::MIN, i128::MAX, -1, 0, 1][r % 5])
                    }
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    if t.logical_type != ValueLogicalType::Physical {
        let physical = FunctionValueType::new(t.data_type.clone(), t.nullable);
        return InputGenerator::new(seed).column(&physical, rows, &InputProfile::default());
    }
    InputGenerator::new(seed).column(t, rows, &InputProfile::default())
}
fn nested() -> Vec<(FunctionValueType, ArrayRef)> {
    let mut result = Vec::new();
    for logical in [ValueLogicalType::Physical, ValueLogicalType::LargeInt] {
        let item = if logical == ValueLogicalType::Physical {
            FunctionValueType::new(DataType::Int32, true)
        } else {
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap()
        };
        let field = Arc::new(item.try_to_field("item").unwrap());
        let a: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(
                (0..=ROWS)
                    .map(|r| (r * 2) as i32)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            values(&item, ROWS * 2, 41),
            Some(NullBuffer::from(
                (0..ROWS).map(|r| r % 11 != 0).collect::<Vec<_>>(),
            )),
        ));
        result.push((FunctionValueType::new(a.data_type().clone(), true), a));
    }
    let fs = vec![
        Arc::new(Field::new("x", DataType::Int32, true)),
        Arc::new(Field::new("y", DataType::Utf8, true)),
    ];
    let a: ArrayRef = Arc::new(StructArray::new(
        fs.into(),
        vec![
            values(&FunctionValueType::new(DataType::Int32, true), ROWS, 43),
            values(&FunctionValueType::new(DataType::Utf8, true), ROWS, 44),
        ],
        Some(NullBuffer::from(
            (0..ROWS).map(|r| r % 13 != 0).collect::<Vec<_>>(),
        )),
    ));
    result.push((FunctionValueType::new(a.data_type().clone(), true), a));
    result
}

fn counts(rows: usize) -> ArrayRef {
    Arc::new(Int64Array::from(
        (0..rows)
            .map(|r| {
                if r % 11 == 0 {
                    None
                } else {
                    Some([-3, 0, 1, 2, 3][r % 5])
                }
            })
            .collect::<Vec<_>>(),
    ))
}
fn check(source: FunctionValueType, array: ArrayRef, count: FunctionValueType, c: ArrayRef) {
    let out = assert_scalar_matches_v1(
        ScalarDiffSpec::new("array_repeat")
            .typed_column(source.clone(), array)
            .typed_column(count, c)
            .float_comparison(FloatComparison::Exact)
            .sparse_selections(7, 4811),
    );
    assert_eq!(out.attributed_row_errors, 0);
    assert_eq!(out.legacy_batch_errors, 0);
    let DataType::List(item) = out.result_type.data_type else {
        panic!("real ARRAY_REPEAT result must be List")
    };
    let actual = FunctionValueType::try_from_field(&item).unwrap();
    assert_eq!(actual.data_type, source.data_type);
    assert_eq!(actual.logical_type, source.logical_type);
}
#[test]
fn pure_differential_repeat_full_source_and_nominal_profiles() {
    for mut source in flat_profiles() {
        for nullable in [false, true] {
            if source.data_type == DataType::Null && !nullable {
                continue;
            }
            source.nullable = nullable;
            check(
                source.clone(),
                values(&source, ROWS, 4801),
                FunctionValueType::new(DataType::Int64, true),
                counts(ROWS),
            );
        }
    }
}
#[test]
fn pure_differential_repeat_actual_count_shapes_are_not_guessed_bigint() {
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
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 6),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Microsecond),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
    }
    for t in types {
        if !arrow::compute::can_cast_types(&t, &DataType::Int64) {
            continue;
        }
        let c = if t == DataType::Null {
            new_null_array(&t, ROWS)
        } else if t == DataType::Utf8 {
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|r| Some(["2", "invalid", "-1", "0"][r % 4]))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        } else {
            arrow::compute::cast(&counts(ROWS), &t).unwrap()
        };
        check(
            FunctionValueType::new(DataType::Int32, true),
            values(&FunctionValueType::new(DataType::Int32, true), ROWS, 4802),
            FunctionValueType::new(t, true),
            c,
        );
    }
}
#[test]
fn pure_differential_repeat_nested_metadata_slices_and_empty() {
    for (source, array) in nested() {
        check(
            source,
            array.slice(1, ROWS - 1).clone(),
            FunctionValueType::new(DataType::Int64, true),
            counts(ROWS - 1),
        );
    }
    for source in flat_profiles() {
        for rows in [0, ROWS] {
            check(
                source.clone(),
                new_null_array(&source.data_type, rows),
                FunctionValueType::new(DataType::Int64, true),
                new_null_array(&DataType::Int64, rows),
            );
        }
    }
}
#[test]
fn pure_differential_repeat_constant_source_count_and_sparse_mappings() {
    for source in flat_profiles() {
        let a = values(&source, ROWS, 4803);
        let c = counts(ROWS);
        for mask in 1..4 {
            let mut spec = ScalarDiffSpec::new("array_repeat")
                .constant_rows(ROWS)
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 4804);
            spec = if mask & 1 != 0 {
                spec.constant(constant(source.clone(), a.slice(1, 1)))
            } else {
                spec.typed_column(source.clone(), a.clone())
            };
            spec = if mask & 2 != 0 {
                spec.constant(constant(
                    FunctionValueType::new(DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![2])),
                ))
            } else {
                spec.column(c.clone())
            };
            assert_scalar_matches_v1(spec);
        }
    }
}

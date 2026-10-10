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

//! Actual two ARRAY element overloads, with typed child identity preserved.
use super::generate::{InputGenerator, InputProfile};
use super::{
    FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant,
};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray, new_empty_array,
    new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 513;
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

fn list(item: &FunctionValueType, values: ArrayRef, rows: usize, nullable: bool) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(item.try_to_field("item").unwrap()),
        OffsetBuffer::new(
            (0..=rows)
                .map(|r| (r * 2) as i32)
                .collect::<Vec<_>>()
                .into(),
        ),
        values,
        nullable.then(|| NullBuffer::from((0..rows).map(|r| r % 17 != 0).collect::<Vec<_>>())),
    ))
}
fn index(ty: &DataType, rows: usize, nullable: bool) -> ArrayRef {
    let pattern = [
        Some(1),
        Some(2),
        Some(3),
        Some(0),
        Some(-1),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ];
    let a: ArrayRef = Arc::new(Int64Array::from(
        (0..rows)
            .map(|r| pattern[r % pattern.len()].or_else(|| (!nullable).then_some(1)))
            .collect::<Vec<_>>(),
    ));
    if ty == &DataType::Int32 {
        return Arc::new(Int32Array::from(
            (0..rows)
                .map(|r| match pattern[r % pattern.len()] {
                    Some(v) => Some(v.clamp(i32::MIN as i64, i32::MAX as i64) as i32),
                    None if !nullable => Some(1),
                    None => None,
                })
                .collect::<Vec<_>>(),
        ));
    }
    a
}
fn check(
    item: &FunctionValueType,
    values: ArrayRef,
    index_type: &DataType,
    map_nullable: bool,
    index_nullable: bool,
) {
    let a = list(item, values, ROWS, map_nullable);
    let source = FunctionValueType::new(a.data_type().clone(), map_nullable);
    let idx = index(index_type, ROWS, index_nullable);
    let idx_type = FunctionValueType::new(index_type.clone(), index_nullable);
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new("__array_element_at")
            .typed_column(source, a)
            .typed_column(idx_type, idx)
            .sparse_selections(7, 891)
            .float_comparison(FloatComparison::Exact),
    );
    assert_eq!(s.attributed_row_errors, 0);
    assert_eq!(s.legacy_batch_errors, 0);
    assert!(s.result_type.nullable);
    assert_eq!(s.result_type.logical_type, item.logical_type);
}
#[test]
fn pure_differential_array_element_at_both_overloads_every_flat_and_logical_element_domain() {
    for index_type in [DataType::Int32, DataType::Int64] {
        for item in flat_profiles() {
            for list_nullable in [false, true] {
                for index_nullable in [false, true] {
                    check(
                        &item,
                        values(&item, ROWS * 2, 901),
                        &index_type,
                        list_nullable,
                        index_nullable,
                    );
                }
            }
        }
    }
}
#[test]
fn pure_differential_array_element_at_both_overloads_nested_elements_and_sliced_list_offsets() {
    for (index_type, (item, base)) in [DataType::Int32, DataType::Int64]
        .into_iter()
        .flat_map(|t| nested().into_iter().map(move |n| (t.clone(), n)))
    {
        let indices =
            arrow::array::UInt32Array::from_iter_values((0..ROWS * 2).map(|r| (r % ROWS) as u32));
        let vals = arrow::compute::take(base.as_ref(), &indices, None).unwrap();
        let a = list(&item, vals, ROWS, true);
        let idx = index(&index_type, ROWS, true);
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("__array_element_at")
                .column(a.slice(1, ROWS - 1))
                .typed_column(
                    FunctionValueType::new(index_type, true),
                    idx.slice(1, ROWS - 1),
                )
                .sparse_selections(7, 903),
        );
        assert_eq!(s.attributed_row_errors, 0);
    }
}
#[test]
fn pure_differential_array_element_at_int64_overflow_null_and_unquoting_original_bytes() {
    let strings = [
        Some("\"abc\""),
        Some("\"\""),
        Some("\"a\\b\""),
        Some("\"a\"b\""),
        Some("\"é\""),
        Some("plain"),
        None,
    ];
    for logical in [ValueLogicalType::Physical, ValueLogicalType::Json] {
        let item = FunctionValueType::try_with_logical_type(DataType::Utf8, true, logical).unwrap();
        let vals: ArrayRef = Arc::new(StringArray::from(
            (0..ROWS * 2)
                .map(|r| strings[r % strings.len()])
                .collect::<Vec<_>>(),
        ));
        for t in [DataType::Int32, DataType::Int64] {
            check(&item, vals.clone(), &t, true, true)
        }
    }
}
#[test]
fn pure_differential_array_element_at_empty_entries_all_null_parent_and_constant_index() {
    for t in [DataType::Int32, DataType::Int64] {
        for item in flat_profiles() {
            let idx = constant(
                FunctionValueType::new(t.clone(), false),
                arrow::compute::cast(&(Arc::new(Int32Array::from(vec![1])) as ArrayRef), &t)
                    .unwrap(),
            );
            for rows in [0, ROWS] {
                let a: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(item.try_to_field("item").unwrap()),
                    OffsetBuffer::new(vec![0; rows + 1].into()),
                    new_empty_array(&item.data_type),
                    None,
                ));
                let s = assert_scalar_matches_v1(
                    ScalarDiffSpec::new("__array_element_at")
                        .column(a)
                        .constant(idx.clone())
                        .legacy_constants(LegacyConstantForm::Pool)
                        .sparse_selections(7, 905),
                );
                assert!(s.null_results >= rows);
                assert_eq!(s.attributed_row_errors, 0);
            }
            let ty = DataType::List(Arc::new(item.try_to_field("item").unwrap()));
            let a = new_null_array(&ty, ROWS);
            let s = assert_scalar_matches_v1(
                ScalarDiffSpec::new("__array_element_at")
                    .column(a)
                    .constant(idx)
                    .legacy_constants(LegacyConstantForm::Pool),
            );
            assert!(s.null_results >= ROWS);
        }
    }
}

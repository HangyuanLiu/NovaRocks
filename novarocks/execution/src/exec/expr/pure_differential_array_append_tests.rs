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

//! Permanent array_append differential matrix for its actual generic signature.
use super::generate::{InputGenerator, InputProfile};
use super::{
    FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant,
};
use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, StructArray, UInt32Array, new_empty_array,
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
fn check(
    item: &FunctionValueType,
    children: ArrayRef,
    targets: ArrayRef,
    list_nullable: bool,
    target_nullable: bool,
) {
    let a = list(item, children, ROWS, list_nullable);
    let list_type = FunctionValueType::new(a.data_type().clone(), list_nullable);
    let mut target_type = item.clone();
    target_type.nullable = target_nullable;
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new("array_append")
            .typed_column(list_type, a)
            .typed_column(target_type, targets)
            .float_comparison(FloatComparison::Exact)
            .sparse_selections(7, 921),
    );
    assert_eq!(s.attributed_row_errors, 0);
    assert_eq!(s.legacy_batch_errors, 0);
    let DataType::List(field) = &s.result_type.data_type else {
        panic!("append result must be List")
    };
    let expected = FunctionValueType::try_from_field(field).unwrap();
    assert_eq!(expected.logical_type, item.logical_type);
    assert_eq!(expected.data_type, item.data_type);
}
#[test]
fn pure_differential_array_append_all_flat_and_logical_domains_independent_root_nullability() {
    for item in flat_profiles() {
        for list_nullable in [false, true] {
            for target_nullable in [false, true] {
                if item.data_type == DataType::Null && !target_nullable {
                    continue;
                }
                let mut target_type = item.clone();
                target_type.nullable = target_nullable;
                check(
                    &item,
                    values(&item, ROWS * 2, 923),
                    values(&target_type, ROWS, 924),
                    list_nullable,
                    target_nullable,
                );
            }
        }
    }
}
#[test]
fn pure_differential_array_append_nested_children_and_sliced_offsets() {
    for (item, base) in nested() {
        let indices = UInt32Array::from_iter_values((0..ROWS * 2).map(|r| (r % ROWS) as u32));
        let children = arrow::compute::take(base.as_ref(), &indices, None).unwrap();
        let a = list(&item, children, ROWS, true);
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("array_append")
                .column(a.slice(1, ROWS - 1))
                .typed_column(item, base.slice(1, ROWS - 1))
                .sparse_selections(7, 925),
        );
        assert_eq!(s.attributed_row_errors, 0);
    }
}
#[test]
fn pure_differential_array_append_empty_chunks_empty_parents_and_null_parents() {
    for item in flat_profiles() {
        for rows in [0, ROWS] {
            let dtype = DataType::List(Arc::new(item.try_to_field("item").unwrap()));
            let empty: ArrayRef = Arc::new(ListArray::new(
                Arc::new(item.try_to_field("item").unwrap()),
                OffsetBuffer::new(vec![0; rows + 1].into()),
                new_empty_array(&item.data_type),
                None,
            ));
            let null = new_null_array(&dtype, rows);
            let targets = new_null_array(&item.data_type, rows);
            for a in [empty, null] {
                let s = assert_scalar_matches_v1(
                    ScalarDiffSpec::new("array_append")
                        .column(a)
                        .typed_column(item.clone(), targets.clone())
                        .sparse_selections(7, 926),
                );
                assert_eq!(s.attributed_row_errors, 0);
            }
        }
    }
}
#[test]
fn pure_differential_array_append_constant_list_and_target_are_real_original_domains() {
    for item in flat_profiles() {
        let child = values(&item, ROWS * 2, 927);
        let a = list(&item, child, ROWS, true);
        let t = values(&item, ROWS, 928);
        let list_type = FunctionValueType::new(a.data_type().clone(), true);
        for mask in 1..4 {
            let mut spec = ScalarDiffSpec::new("array_append")
                .constant_rows(ROWS)
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 929);
            spec = if mask & 1 != 0 {
                spec.constant(constant(list_type.clone(), a.slice(1, 1)))
            } else {
                spec.typed_column(list_type.clone(), a.clone())
            };
            spec = if mask & 2 != 0 {
                spec.constant(constant(item.clone(), t.slice(1, 1)))
            } else {
                spec.typed_column(item.clone(), t.clone())
            };
            let s = assert_scalar_matches_v1(spec);
            assert_eq!(s.attributed_row_errors, 0);
        }
    }
}

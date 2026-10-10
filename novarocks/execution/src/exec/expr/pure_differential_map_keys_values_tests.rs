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
//! Permanent MAP_KEYS/MAP_VALUES matrices; raw per-row data failures use E-D13 oracle.
use super::generate::{InputGenerator, InputProfile};
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant};
use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray, new_empty_array, new_null_array,
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

fn map(keys: ArrayRef, value: ArrayRef, nullable: bool, sorted: bool) -> ArrayRef {
    let key = FunctionValueType::new(keys.data_type().clone(), false);
    let val = FunctionValueType::new(value.data_type().clone(), true);
    map_typed(keys, value, &key, &val, nullable, sorted)
}
fn map_typed(
    keys: ArrayRef,
    value: ArrayRef,
    key: &FunctionValueType,
    val: &FunctionValueType,
    nullable: bool,
    sorted: bool,
) -> ArrayRef {
    let rows = keys.len() / 2;
    assert_eq!(keys.len(), value.len());
    let mut kf = key.try_to_field("authored-key").unwrap();
    let mut vf = val.try_to_field("authored-value").unwrap();
    let mut km = kf.metadata().clone();
    km.insert("key-fact".into(), "preserved".into());
    kf = kf.with_metadata(km);
    let mut vm = vf.metadata().clone();
    vm.insert("value-fact".into(), "preserved".into());
    vf = vf.with_metadata(vm);
    let entries = StructArray::new(
        vec![Arc::new(kf), Arc::new(vf)].into(),
        vec![keys, value],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(
            Field::new("authored-entries", entries.data_type().clone(), false)
                .with_metadata([("entry-fact".into(), "preserved".into())].into()),
        ),
        OffsetBuffer::new(
            (0..=rows)
                .map(|r| (r * 2) as i32)
                .collect::<Vec<_>>()
                .into(),
        ),
        entries,
        nullable.then(|| NullBuffer::from((0..rows).map(|r| r % 11 != 0).collect::<Vec<_>>())),
        sorted,
    ))
}
fn check(name: &str, a: ArrayRef, nullable: bool) {
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new(name)
            .typed_column(FunctionValueType::new(a.data_type().clone(), nullable), a)
            .float_comparison(super::FloatComparison::Exact)
            .sparse_selections(7, 6031),
    );
    assert!(s.result_type.nullable);
}
fn supported(t: &FunctionValueType) -> bool {
    matches!(
        t.data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Timestamp(_, None)
            | DataType::FixedSizeBinary(16)
    )
}
#[test]
fn pure_differential_map_parts_every_generic_flat_key_value_and_error_domain() {
    for name in ["map_keys", "map_values"] {
        for t in flat_profiles() {
            for nullable in [false, true] {
                for sorted in [false, true] {
                    let key: ArrayRef = Arc::new(Int32Array::from(
                        (0..ROWS * 2)
                            .map(|r| ((ROWS * 2 - r) % 11) as i32)
                            .collect::<Vec<_>>(),
                    ));
                    check(
                        name,
                        map_typed(
                            key,
                            values(&t, ROWS * 2, 6032),
                            &FunctionValueType::new(DataType::Int32, false),
                            &t,
                            nullable,
                            sorted,
                        ),
                        nullable,
                    );
                    if t.data_type != DataType::Null {
                        let mut k = t.clone();
                        k.nullable = false;
                        check(
                            name,
                            map_typed(
                                values(&k, ROWS * 2, 6033),
                                values(&t, ROWS * 2, 6034),
                                &k,
                                &t,
                                nullable,
                                sorted,
                            ),
                            nullable,
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn pure_differential_map_parts_unsupported_key_single_entry_and_null_demand_succeed() {
    for name in ["map_keys", "map_values"] {
        for t in flat_profiles() {
            if supported(&t) || t.data_type == DataType::Null {
                continue;
            }
            let mut k = t.clone();
            k.nullable = false;
            let keys = values(&k, ROWS, 6035);
            let val: ArrayRef = Arc::new(Int32Array::from(
                (0..ROWS).map(|r| r as i32).collect::<Vec<_>>(),
            ));
            let entries = StructArray::new(
                vec![
                    Arc::new(k.try_to_field("key").unwrap()),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ]
                .into(),
                vec![keys, val],
                None,
            );
            let a: ArrayRef = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new((0..=ROWS as i32).collect::<Vec<_>>().into()),
                entries,
                Some(NullBuffer::from(
                    (0..ROWS).map(|r| r % 7 != 0).collect::<Vec<_>>(),
                )),
                false,
            ));
            check(name, a, true);
        }
    }
}

#[test]
fn pure_differential_map_parts_nested_children_complete_fields_and_slices() {
    let nested: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("nested-item", DataType::Int32, true)),
        OffsetBuffer::new((0..=ROWS * 2).map(|r| r as i32).collect::<Vec<_>>().into()),
        Arc::new(Int32Array::from(
            (0..ROWS * 2)
                .map(|r| if r % 7 == 0 { None } else { Some(r as i32) })
                .collect::<Vec<_>>(),
        )),
        None,
    ));
    let structure: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "nested-x",
            nested.data_type().clone(),
            true,
        ))]
        .into(),
        vec![nested.clone()],
        None,
    ));
    let keys: ArrayRef = Arc::new(Int32Array::from(
        (0..ROWS * 2)
            .map(|r| ((ROWS * 2 - r) % 19) as i32)
            .collect::<Vec<_>>(),
    ));
    for name in ["map_keys", "map_values"] {
        for sorted in [false, true] {
            for a in [
                map(keys.clone(), structure.clone(), true, sorted),
                map(nested.clone(), structure.clone(), true, sorted),
            ] {
                check(name, a.slice(1, ROWS - 1), true);
            }
        }
    }
}
#[test]
fn pure_differential_map_parts_empty_null_parents_and_null_key_empty_domain() {
    for name in ["map_keys", "map_values"] {
        for t in flat_profiles() {
            let entries = StructArray::new(
                vec![
                    Arc::new(Field::new("key", t.data_type.clone(), false)),
                    Arc::new(Field::new("value", t.data_type.clone(), true)),
                ]
                .into(),
                vec![new_empty_array(&t.data_type), new_empty_array(&t.data_type)],
                None,
            );
            let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
            for rows in [0, ROWS] {
                for sorted in [false, true] {
                    let a: ArrayRef = Arc::new(MapArray::new(
                        field.clone(),
                        OffsetBuffer::new(vec![0; rows + 1].into()),
                        entries.clone(),
                        None,
                        sorted,
                    ));
                    check(name, a.clone(), false);
                    check(name, new_null_array(a.data_type(), rows), true);
                }
            }
        }
    }
}
#[test]
fn pure_differential_map_parts_actual_pool_constants_and_sparse() {
    let keys: ArrayRef = Arc::new(Int32Array::from(
        (0..ROWS * 2)
            .map(|r| ((ROWS * 2 - r) % 19) as i32)
            .collect::<Vec<_>>(),
    ));
    let a = map(keys.clone(), keys, true, false);
    let source = FunctionValueType::new(a.data_type().clone(), true);
    for name in ["map_values", "map_keys"] {
        for row in [0, 1] {
            let s = assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .constant(constant(source.clone(), a.slice(row, 1)))
                    .constant_rows(ROWS)
                    .legacy_constants(LegacyConstantForm::Pool)
                    .sparse_selections(7, 6036),
            );
            assert_eq!(s.attributed_row_errors, 0);
        }
    }
}

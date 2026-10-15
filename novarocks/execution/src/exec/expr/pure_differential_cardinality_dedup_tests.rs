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
//! Permanent CARDINALITY generic declaration matrix; before extraction it exercises the installed original-54 owner.
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
    if *t.data_type() == DataType::FixedSizeBinary(16) {
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|r| {
                    if t.nullable() && r % 7 == 0 {
                        None
                    } else {
                        Some([i128::MIN, i128::MAX, -1, 0, 1][r % 5])
                    }
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    if t.logical_type() != ValueLogicalType::Physical {
        let physical = FunctionValueType::new(t.data_type().clone(), t.nullable());
        return InputGenerator::new(seed).column(&physical, rows, &InputProfile::default());
    }
    InputGenerator::new(seed).column(t, rows, &InputProfile::default())
}

type DeclaredArray = (FunctionValueType, ArrayRef);

fn sliced(source: &DeclaredArray, offset: usize, len: usize) -> DeclaredArray {
    (source.0.clone(), source.1.slice(offset, len))
}
fn null_like(source: &DeclaredArray, rows: usize) -> DeclaredArray {
    (
        source.0.with_nullable(true).unwrap(),
        new_null_array(source.0.data_type(), rows),
    )
}
fn declared_map(
    key: &FunctionValueType,
    val: &FunctionValueType,
    nullable: bool,
    sorted: bool,
) -> FunctionValueType {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let key = key.with_nullable(false).unwrap();
    let val = val.with_nullable(true).unwrap();
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", key.data_type().clone(), false)),
                Arc::new(Field::new("value", val.data_type().clone(), true)),
            ]
            .into(),
        ),
        false,
    );
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Map {
                key: key.value_type().clone(),
                value: val.value_type().clone(),
            },
            nullable,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
        ValueLayout::try_new(
            &DataType::Map(Arc::new(entries), sorted),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn map_typed(
    keys: ArrayRef,
    value: ArrayRef,
    key: &FunctionValueType,
    val: &FunctionValueType,
    nullable: bool,
    sorted: bool,
) -> DeclaredArray {
    let rows = keys.len() / 2;
    assert_eq!(keys.len(), value.len());
    // Canonicalize representation before either consumer. The independently
    // supplied complete declarations remain the sole semantic source.
    let keys = crate::exec::chunk::normalize_execution_array(&keys).unwrap();
    let value = crate::exec::chunk::normalize_execution_array(&value).unwrap();
    assert_eq!(keys.data_type(), key.data_type());
    assert_eq!(value.data_type(), val.data_type());
    let declared = declared_map(key, val, nullable, sorted);
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", key.data_type().clone(), false)),
            Arc::new(Field::new("value", val.data_type().clone(), true)),
        ]
        .into(),
        vec![keys, value],
        None,
    );
    let array = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(
            (0..=rows)
                .map(|r| (r * 2) as i32)
                .collect::<Vec<_>>()
                .into(),
        ),
        entries,
        nullable.then(|| NullBuffer::from((0..rows).map(|r| r % 11 != 0).collect::<Vec<_>>())),
        sorted,
    )) as ArrayRef;
    assert_eq!(array.data_type(), declared.data_type());
    (declared, array)
}
fn check(a: DeclaredArray, nullable: bool) {
    assert_eq!(a.0.nullable(), nullable);
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new("cardinality")
            .typed_column(a.0, a.1)
            .sparse_selections(7, 5901),
    );
    assert_eq!(s.attributed_row_errors, 0);
    assert_eq!(s.legacy_batch_errors, 0);
    assert_eq!(s.result_type, FunctionValueType::new(DataType::Int32, true));
}
#[test]
fn pure_differential_cardinality_all_flat_child_domains_preserve_generic_metadata() {
    for t in flat_profiles() {
        for nullable in [false, true] {
            for sorted in [false, true] {
                let key: ArrayRef = Arc::new(Int32Array::from(
                    (0..ROWS * 2).map(|r| r as i32).collect::<Vec<_>>(),
                ));
                check(
                    map_typed(
                        key,
                        values(&t, ROWS * 2, 5902),
                        &FunctionValueType::new(DataType::Int32, false),
                        &t,
                        nullable,
                        sorted,
                    ),
                    nullable,
                );
                if *t.data_type() != DataType::Null {
                    let mut k = t.clone();
                    k = k.with_nullable(false).expect("valid test root nullability");
                    check(
                        map_typed(
                            values(&k, ROWS * 2, 5903),
                            values(&t, ROWS * 2, 5904),
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
#[test]
fn pure_differential_cardinality_nested_key_value_and_sliced_offsets() {
    let item = Arc::new(
        Field::new("nested-item", DataType::Int32, true)
            .with_metadata([("nested-fact".into(), "preserved".into())].into()),
    );
    let nested_type = super::array_append_tests::declared_list(
        &FunctionValueType::new(DataType::Int32, true),
        true,
    );
    let structure_type =
        super::array_append_tests::declared_struct(&[("nested-x", nested_type.clone())], true);
    let nested: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new((0..=ROWS * 2).map(|r| r as i32).collect::<Vec<_>>().into()),
        Arc::new(Int32Array::from(
            (0..ROWS * 2)
                .map(|r| if r % 7 == 0 { None } else { Some(r as i32) })
                .collect::<Vec<_>>(),
        )),
        None,
    ));
    let fields = vec![Arc::new(Field::new(
        "nested-x",
        nested.data_type().clone(),
        true,
    ))]
    .into();
    let structure: ArrayRef = Arc::new(StructArray::new(fields, vec![nested.clone()], None));
    for sorted in [false, true] {
        for a in [
            map_typed(
                nested.clone(),
                structure.clone(),
                &nested_type,
                &structure_type,
                true,
                sorted,
            ),
            map_typed(
                structure.clone(),
                nested.clone(),
                &structure_type,
                &nested_type,
                false,
                sorted,
            ),
        ] {
            let nullable = a.0.nullable();
            check(sliced(&a, 1, ROWS - 1), nullable);
        }
    }
}
#[test]
fn pure_differential_cardinality_empty_all_null_empty_key_domains() {
    for t in flat_profiles() {
        for sorted in [false, true] {
            let entries = StructArray::new(
                vec![
                    Arc::new(Field::new("key", t.data_type().clone(), false)),
                    Arc::new(Field::new("value", t.data_type().clone(), true)),
                ]
                .into(),
                vec![
                    new_empty_array(&t.data_type()),
                    new_empty_array(&t.data_type()),
                ],
                None,
            );
            let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
            for rows in [0, ROWS] {
                let a: ArrayRef = Arc::new(MapArray::new(
                    field.clone(),
                    OffsetBuffer::new(vec![0; rows + 1].into()),
                    entries.clone(),
                    None,
                    sorted,
                ));
                check((declared_map(&t, &t, false, sorted), a.clone()), false);
                check(
                    (
                        declared_map(&t, &t, true, sorted),
                        new_null_array(a.data_type(), rows),
                    ),
                    true,
                );
            }
        }
    }
}
#[test]
fn pure_differential_cardinality_real_constant_pool_and_sparse_domain() {
    let keys: ArrayRef = Arc::new(Int32Array::from(
        (0..ROWS * 2).map(|r| r as i32).collect::<Vec<_>>(),
    ));
    let key_type = FunctionValueType::new(DataType::Int32, false);
    let a = map_typed(keys.clone(), keys, &key_type, &key_type, true, false);
    let source = a.0.clone();
    for row in [0, 1] {
        let s = assert_scalar_matches_v1(
            ScalarDiffSpec::new("cardinality")
                .constant(constant(source.clone(), a.1.slice(row, 1)))
                .constant_rows(ROWS)
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 5905),
        );
        assert_eq!(s.attributed_row_errors, 0);
        assert_eq!(s.legacy_batch_errors, 0);
    }
}

fn list_typed(item: &FunctionValueType, values: ArrayRef, nullable: bool) -> DeclaredArray {
    let rows = values.len() / 2;
    let values = crate::exec::chunk::normalize_execution_array(&values).unwrap();
    assert_eq!(values.data_type(), item.data_type());
    let declared = super::array_append_tests::declared_list(item, nullable);
    let array = Arc::new(ListArray::new(
        super::array_append_tests::execution_list_field(item),
        OffsetBuffer::new(
            (0..=rows)
                .map(|i| (i * 2) as i32)
                .collect::<Vec<_>>()
                .into(),
        ),
        values,
        nullable.then(|| NullBuffer::from((0..rows).map(|i| i % 13 != 0).collect::<Vec<_>>())),
    )) as ArrayRef;
    assert_eq!(array.data_type(), declared.data_type());
    (declared, array)
}

#[test]
fn pure_differential_cardinality_list_actual_overload_flat_nominal_and_constant() {
    for item in flat_profiles() {
        for nullable in [false, true] {
            let a = list_typed(&item, values(&item, ROWS * 2, 7931), nullable);
            check(a.clone(), nullable);
            check(sliced(&a, 1, ROWS - 1), nullable);
            for form in [LegacyConstantForm::Pool, LegacyConstantForm::Literal] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("cardinality")
                        .constant_rows(ROWS)
                        .constant(constant(a.0.clone(), a.1.slice(1, 1)))
                        .legacy_constants(form)
                        .sparse_selections(3, 7939),
                );
            }
            let empty = list_typed(&item, new_empty_array(&item.data_type()), nullable);
            check(empty, nullable);
        }
    }
}
#[test]
fn pure_differential_cardinality_both_overloads_nested_fields_and_root_null() {
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(
            Field::new("payload", DataType::Utf8, true)
                .with_metadata([("nested-key".into(), "retained".into())].into()),
        )]
        .into(),
        vec![Arc::new(arrow::array::StringArray::from(
            (0..ROWS * 2)
                .map(|i| if i % 7 == 0 { None } else { Some("payload") })
                .collect::<Vec<_>>(),
        ))],
        None,
    ));
    let item = super::array_append_tests::declared_struct(
        &[("payload", FunctionValueType::new(DataType::Utf8, true))],
        true,
    );
    let a = list_typed(&item, values.clone(), true);
    check(a.clone(), true);
    check(null_like(&a, ROWS), true);
    for sorted in [false, true] {
        let m = map_typed(
            Arc::new(Int32Array::from(
                (0..ROWS * 2).map(|i| i as i32).collect::<Vec<_>>(),
            )),
            values.clone(),
            &FunctionValueType::new(DataType::Int32, false),
            &item,
            true,
            sorted,
        );
        let nested = list_typed(&m.0, m.1.clone(), true);
        check(nested, true);
        check(m, true);
    }
}

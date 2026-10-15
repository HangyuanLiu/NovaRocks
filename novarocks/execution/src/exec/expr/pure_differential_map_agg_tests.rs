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
//! Permanent full MAP_AGG declaration differential; RED Missing before installation.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::constant;
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    UInt32Array, new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::OffsetBuffer;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 37;
fn check(keys: ArrayRef, values: ArrayRef, seed: u64) {
    assert_eq!(keys.len(), values.len());
    let rows = keys.len();
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("map_agg")
            .column(keys)
            .column(values)
            .grouped((0..rows).map(|row| row % 3).collect(), 5)
            .partitions(5, seed),
    );
}
#[test]
fn pure_differential_map_agg_original_native_utf8_int32_first_wins_nulls() {
    check(
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some("c"),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(99),
            Some(77),
            None,
        ])),
        101,
    );
}
#[test]
fn pure_differential_map_agg_all_original_scalar_carriers_and_physical_fsb16() {
    let mut types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            types.push(DataType::Timestamp(unit, zone));
        }
    }
    for (precision, scale) in [(9, 0), (18, 2), (38, -2), (38, 6)] {
        types.push(DataType::Decimal128(precision, scale));
    }
    for (precision, scale) in [(40, 0), (60, 2), (76, 6)] {
        types.push(DataType::Decimal256(precision, scale));
    }
    for nullable in [false, true] {
        for (i, ty) in types.iter().enumerate() {
            let value_type = FunctionValueType::new(ty.clone(), nullable);
            let values = InputGenerator::new(102 + i as u64).column(
                &value_type,
                ROWS,
                &InputProfile::default().with_boundary_ratio(0.0),
            );
            let stable_keys: ArrayRef = Arc::new(Int64Array::from(
                (0..ROWS).map(|row| (row % 7) as i64).collect::<Vec<_>>(),
            ));
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("map_agg")
                    .typed_column(
                        FunctionValueType::new(DataType::Int64, false),
                        stable_keys.clone(),
                    )
                    .typed_column(value_type.clone(), values.clone())
                    .partitions(5, 103),
            );
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("map_agg")
                    .typed_column(value_type, values)
                    .typed_column(FunctionValueType::new(DataType::Int64, false), stable_keys)
                    .partitions(5, 104),
            );
        }
        for logical_type in [ValueLogicalType::Physical] {
            let ty = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                logical_type,
            )
            .unwrap();
            let values = novarocks_types::largeint::array_from_i128(
                &(0..ROWS)
                    .map(|r| {
                        if nullable && r % 5 == 0 {
                            None
                        } else {
                            Some([i128::MIN, i128::MAX, 0, -1][r % 4])
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("map_agg")
                    .typed_column(ty.clone(), values.clone())
                    .typed_column(ty, values)
                    .partitions(5, 105),
            );
        }
    }
}
#[test]
fn pure_differential_map_agg_nested_full_fields_metadata_and_slice() {
    let item = Arc::new(
        Field::new("original-item", DataType::Int64, true)
            .with_metadata([(String::from("provider.source"), String::from("original"))].into()),
    );
    let list: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0i32, 2, 2, 3, 4].into()),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(1)])),
        None,
    ));
    let row: ArrayRef = Arc::new(StructArray::new(
        vec![
            Field::new("actual-list", list.data_type().clone(), true),
            Field::new("label", DataType::Utf8, true),
        ]
        .into(),
        vec![
            list.clone(),
            Arc::new(StringArray::from(vec![
                Some("x"),
                None,
                Some("x"),
                Some("y"),
            ])),
        ],
        None,
    ));
    let entries = StructArray::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("b"),
                Some("c"),
            ])) as ArrayRef,
        ],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0i32, 1, 2, 3, 4].into()),
        entries,
        None,
        false,
    ));
    for source in [list, row, map] {
        check(source.slice(1, 3), source.slice(1, 3), 106);
    }
}
#[test]
fn pure_differential_map_agg_empty_null_keys_null_values_and_generic_data_domains() {
    for ty in [
        DataType::Null,
        DataType::Int64,
        DataType::Utf8,
        DataType::UInt32,
    ] {
        for rows in [0, 37] {
            check(
                new_null_array(&ty, rows),
                new_null_array(&DataType::Int64, rows),
                107,
            );
            check(
                new_null_array(&DataType::Int64, rows),
                new_null_array(&ty, rows),
                108,
            );
        }
    }
    check(
        new_empty_array(&DataType::Utf8),
        new_empty_array(&DataType::Int32),
        109,
    );
    check(
        Arc::new(UInt32Array::from(vec![None, Some(1), Some(2)])),
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        110,
    );
    check(
        Arc::new(Int64Array::from(vec![None, Some(1), Some(1)])),
        Arc::new(UInt32Array::from(vec![Some(7), None, Some(99)])),
        111,
    );
}
#[test]
fn pure_differential_map_agg_original_pool_constants_and_sparse_phase_selection() {
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("map_agg")
            .constant(constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["key"])),
            ))
            .constant(constant(
                FunctionValueType::new(DataType::Int32, true),
                Arc::new(Int32Array::from(vec![None])),
            ))
            .constant_rows(321)
            .partitions(7, 112),
    );
}

// Complete argument declarations preserve nominal children in both aggregate
// owners. Both real phases must agree on values; an equivalent failure/panic
// is not a successful replacement for the retired metadata-drift witness.
#[test]
fn pure_differential_map_agg_nominal_largeint_full_state_and_result() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};

    let required = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let nullable = required.with_nullable(true).unwrap();
    let expected_layout = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::FixedSizeBinary(16), false)),
                    Arc::new(Field::new("value", DataType::FixedSizeBinary(16), true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let expected = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Map {
                key: required.value_type().clone(),
                value: nullable.value_type().clone(),
            },
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
        ValueLayout::try_new(&expected_layout, ValueTypeStorageLifetime::pure()).unwrap(),
    )
    .unwrap();
    let assert_map_metadata_free = |ty: &DataType| {
        let DataType::Map(entries, false) = ty else {
            panic!("the original MAP_AGG carrier must remain an unordered map");
        };
        assert_eq!(entries.metadata().capacity(), 0);
        assert_eq!(entries.name(), "entries");
        assert!(!entries.is_nullable());
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("map entries must retain the exact key/value scaffold");
        };
        assert_eq!(fields.len(), 2);
        for (field, name, nullable) in [(&fields[0], "key", false), (&fields[1], "value", true)] {
            assert_eq!(field.name(), name);
            assert_eq!(field.is_nullable(), nullable);
            assert_eq!(field.data_type(), &DataType::FixedSizeBinary(16));
            assert_eq!(field.metadata().capacity(), 0);
        }
    };
    let finite = || {
        novarocks_types::largeint::array_from_i128(&[
            Some(i128::MIN),
            Some(i128::MAX),
            Some(0),
            Some(-1),
        ])
        .unwrap()
    };
    let check = |keys: ArrayRef,
                 key_type: FunctionValueType,
                 values: ArrayRef,
                 value_type: FunctionValueType| {
        assert_eq!(keys.len(), values.len());
        let rows = keys.len();
        for grouped in [false, true] {
            let mut spec = AggregateDiffSpec::new("map_agg")
                .typed_column(key_type.clone(), keys.clone())
                .typed_column(value_type.clone(), values.clone())
                .partitions(5, 105);
            if grouped {
                // Preserve sparse groups, including groups with no input.
                spec = spec.grouped((0..rows).map(|row| row % 3).collect(), 5);
            }
            let summary = assert_aggregate_matches_v1(spec);
            assert_eq!(summary.rows, rows);
            assert_eq!(summary.groups, if grouped { 5 } else { 1 });
            assert_eq!(summary.partitions, 5);
            assert_eq!(summary.matched_failures, 0);
            assert_eq!(summary.matched_panics, 0);
            // The original algorithm emits a non-NULL empty map for empty
            // states and skips NULL keys in both single and merged phases.
            assert_eq!(summary.null_results, 0);
            assert_eq!(summary.result_type, expected);
            assert_eq!(summary.pure_state_type, expected);
            assert_map_metadata_free(summary.result_type.data_type());
            assert_map_metadata_free(summary.pure_state_type.data_type());
            assert_map_metadata_free(&summary.legacy_intermediate_type);
        }
    };
    // Both original root-nullability declarations remain independent of
    // observed data. Empty and all-NULL input preserve the same nominal type.
    for ty in [required.clone(), nullable.clone()] {
        check(finite(), ty.clone(), finite(), ty.clone());
        check(
            new_empty_array(&DataType::FixedSizeBinary(16)),
            ty.clone(),
            new_empty_array(&DataType::FixedSizeBinary(16)),
            ty,
        );
    }
    check(
        new_null_array(&DataType::FixedSizeBinary(16), 4),
        nullable.clone(),
        new_null_array(&DataType::FixedSizeBinary(16), 4),
        nullable.clone(),
    );
    // NULL values with real MIN/MAX/0/-1 keys test the value promise separately
    // from NULL-key skipping; both phases still compare every original value.
    check(
        finite(),
        required,
        new_null_array(&DataType::FixedSizeBinary(16), 4),
        nullable,
    );
}

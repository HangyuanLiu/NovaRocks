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
            let ty = FunctionValueType {
                logical_type,
                ..FunctionValueType::new(DataType::FixedSizeBinary(16), nullable)
            };
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

// Original registered v1 cannot re-derive top-level nominal input tags from
// its DataType-only signature arguments. This is an admission witness, not
// an aggregate value comparison or a pure-owner rejection claim.
#[test]
fn pure_differential_map_agg_nominal_largeint_preserves_original_binding_refusal() {
    const ORIGINAL_REASON: &str = r##"bind aggregate `map_agg`: aggregate `map_agg` resolved signature drift: planned=ResolvedAggregateSignature { overload: AggregateOverloadIdentity("builtin.aggregate/map_agg/derived-v1"), argument_types: [FixedSizeBinary(16), FixedSizeBinary(16)], intermediate_type: Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: FixedSizeBinary(16), nullable: true, metadata: {"nr_logical_type": "largeint"} }, Field { name: "value", data_type: FixedSizeBinary(16), nullable: true, metadata: {"nr_logical_type": "largeint"} }]) }, false), output_type: Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: FixedSizeBinary(16), nullable: true, metadata: {"nr_logical_type": "largeint"} }, Field { name: "value", data_type: FixedSizeBinary(16), nullable: true, metadata: {"nr_logical_type": "largeint"} }]) }, false), state_format: AggregateStateFormatId("novarocks/map_agg/state-v1") }, local=ResolvedAggregateSignature { overload: AggregateOverloadIdentity("builtin.aggregate/map_agg/derived-v1"), argument_types: [FixedSizeBinary(16), FixedSizeBinary(16)], intermediate_type: Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: FixedSizeBinary(16), nullable: true }, Field { name: "value", data_type: FixedSizeBinary(16), nullable: true }]) }, false), output_type: Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: FixedSizeBinary(16), nullable: true }, Field { name: "value", data_type: FixedSizeBinary(16), nullable: true }]) }, false), state_format: AggregateStateFormatId("novarocks/map_agg/state-v1") }"##;
    for nullable in [false, true] {
        for values in [
            novarocks_types::largeint::array_from_i128(&[
                Some(i128::MIN),
                Some(i128::MAX),
                Some(0),
                Some(-1),
            ])
            .unwrap(),
            new_empty_array(&DataType::FixedSizeBinary(16)),
            new_null_array(&DataType::FixedSizeBinary(16), 4),
        ] {
            if !nullable && values.null_count() != 0 {
                continue;
            }
            let ty = FunctionValueType {
                logical_type: ValueLogicalType::LargeInt,
                ..FunctionValueType::new(DataType::FixedSizeBinary(16), nullable)
            };
            let spec = AggregateDiffSpec::new("map_agg")
                .typed_column(ty.clone(), values.clone())
                .typed_column(ty, values)
                .partitions(5, 105);
            match super::aggregate::run_aggregate_differential(&spec) {
                Err(super::DifferentialFailure::LegacyUnavailable { name, reason }) => {
                    assert_eq!(name, "map_agg");
                    assert_eq!(reason, ORIGINAL_REASON);
                }
                other => panic!("original nominal binding route changed: {other:?}"),
            }
        }
    }
}

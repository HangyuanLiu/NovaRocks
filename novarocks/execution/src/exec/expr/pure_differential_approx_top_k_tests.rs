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
//! Permanent full original arity and recursive carrier equality, before owner installation.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn profiles() -> Vec<DataType> {
    let mut v = vec![
        DataType::Null,
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
        DataType::FixedSizeBinary(16),
    ];
    for u in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for z in [None, Some("UTC".into())] {
            v.push(DataType::Timestamp(u, z));
        }
    }
    for (wide, p) in [(false, 1), (false, 38), (true, 1), (true, 76)] {
        for scale in [-(p as i8), 0, p as i8] {
            v.push(if wide {
                DataType::Decimal256(p, scale)
            } else {
                DataType::Decimal128(p, scale)
            });
        }
    }
    v.push(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int32,
        true,
    ))));
    v.push(DataType::Struct(
        vec![Field::new("nested", DataType::Utf8, true)].into(),
    ));
    v.push(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    ));
    v
}
fn values(ty: &DataType, rows: usize) -> ArrayRef {
    // The shared generator explicitly has no composite author. Preserve the
    // declared fields and build actual nested/null/empty payloads here.
    let validity = || NullBuffer::from((0..rows).map(|r| r % 5 != 0).collect::<Vec<_>>());
    let lengths: Vec<usize> = (0..rows).map(|r| if r % 4 == 1 { 0 } else { 2 }).collect();
    let composite: Option<ArrayRef> = match ty {
        DataType::List(field) => {
            assert_eq!(field.data_type(), &DataType::Int32);
            let child = Arc::new(Int32Array::from(
                lengths
                    .iter()
                    .enumerate()
                    .flat_map(|(r, n)| (0..*n).map(move |i| (i == 0).then_some((r % 3) as i32)))
                    .collect::<Vec<_>>(),
            )) as ArrayRef;
            Some(Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::from_lengths(lengths.iter().copied()),
                child,
                Some(validity()),
            )))
        }
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].data_type(), &DataType::Utf8);
            let child = Arc::new(StringArray::from(
                (0..rows)
                    .map(|r| match r % 3 {
                        0 => None,
                        1 => Some("λ"),
                        _ => Some(""),
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef;
            Some(Arc::new(StructArray::new(
                fields.clone(),
                vec![child],
                Some(validity()),
            )))
        }
        DataType::Map(field, sorted) => {
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].data_type(), &DataType::Int32);
            assert_eq!(fields[1].data_type(), &DataType::Utf8);
            let total: usize = lengths.iter().sum();
            let keys = Arc::new(Int32Array::from(vec![7; total])) as ArrayRef;
            let values = Arc::new(StringArray::from(
                (0..total)
                    .map(|i| (i % 2 == 0).then_some("λ"))
                    .collect::<Vec<_>>(),
            )) as ArrayRef;
            let entries = StructArray::new(fields.clone(), vec![keys, values], None);
            Some(Arc::new(MapArray::new(
                field.clone(),
                OffsetBuffer::from_lengths(lengths.iter().copied()),
                entries,
                Some(validity()),
                *sorted,
            )))
        }
        _ => None,
    };
    if let Some(array) = composite {
        assert_eq!(array.data_type(), ty);
        assert_eq!(array.len(), rows);
        return array;
    }
    if *ty == DataType::FixedSizeBinary(16) {
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|r| match r % 5 {
                    0 => None,
                    1 => Some(i128::MIN),
                    2 => Some(i128::MAX),
                    3 => Some(-1),
                    _ => Some(0),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    InputGenerator::new(9301).column(
        &FunctionValueType::new(ty.clone(), true),
        rows,
        &InputProfile::default().with_boundary_ratio(0.0),
    )
}
fn run(arity: usize) {
    for ty in profiles() {
        let a = values(&ty, 17);
        let mut spec = AggregateDiffSpec::new("approx_top_k")
            .typed_column(FunctionValueType::new(ty, true), a);
        if arity >= 2 {
            spec = spec.column(Arc::new(Int64Array::from(vec![3; 17])));
        }
        if arity >= 3 {
            spec = spec.column(Arc::new(Int64Array::from(vec![100; 17])));
        }
        assert_aggregate_matches_v1(
            spec.grouped((0..17).map(|i| i % 3).collect(), 5)
                .partitions(3, 9302),
        );
    }
}
#[test]
fn pure_differential_approx_top_k_full_declaration_one_argument() {
    run(1);
}
#[test]
fn pure_differential_approx_top_k_full_declaration_two_arguments() {
    run(2);
}
#[test]
fn pure_differential_approx_top_k_full_declaration_three_arguments() {
    run(3);
}
#[test]
fn pure_differential_approx_top_k_empty_and_all_null_all_arities() {
    for n in 1..=3 {
        for a in [
            new_empty_array(&DataType::Int32),
            new_null_array(&DataType::Int32, 7),
        ] {
            let len = a.len();
            let mut spec = AggregateDiffSpec::new("approx_top_k").column(a);
            for _ in 1..n {
                spec = spec.column(Arc::new(Int64Array::from(vec![3; len])));
            }
            let result = assert_aggregate_matches_v1(spec);
            assert_eq!(result.null_results, 0);
        }
    }
}

#[test]
fn pure_differential_approx_top_k_original_parameter_carriers_and_distinct_receipt() {
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray, UInt32Array};
    for parameter in [
        Arc::new(Int32Array::from(vec![3; 17])) as ArrayRef,
        Arc::new(Float64Array::from(vec![2.9; 17])),
        Arc::new(BooleanArray::from(vec![true; 17])),
        Arc::new(StringArray::from(vec!["ignored"; 17])),
        Arc::new(UInt32Array::from(vec![1; 17])),
    ] {
        let value = Arc::new(Int32Array::from(
            (0..17)
                .map(|r| if r % 5 == 0 { None } else { Some(r % 3) })
                .collect::<Vec<_>>(),
        )) as ArrayRef;
        for distinct in [false, true] {
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("approx_top_k")
                    .column(value.clone())
                    .column(parameter.clone())
                    .original_state_interpretation(distinct, Vec::new())
                    .grouped((0..17).map(|r| r % 2).collect(), 3),
            );
        }
    }
}
#[test]
fn pure_differential_approx_top_k_full_nominal_carriers_preserve_original_projection() {
    use arrow::array::{BinaryArray, LargeBinaryArray, StringArray};
    use novarocks_type_contract::ValueLogicalType;
    for (dtype, logical) in [
        (DataType::Utf8, ValueLogicalType::Json),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::LargeBinary, ValueLogicalType::Hll),
        (DataType::Binary, ValueLogicalType::Bitmap),
        (DataType::LargeBinary, ValueLogicalType::Bitmap),
        (DataType::Binary, ValueLogicalType::Object),
        (DataType::LargeBinary, ValueLogicalType::Object),
        (DataType::Binary, ValueLogicalType::Percentile),
        (DataType::LargeBinary, ValueLogicalType::Percentile),
        (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
    ] {
        let ty = FunctionValueType::try_with_logical_type(dtype.clone(), true, logical).unwrap();
        let a: ArrayRef = match dtype {
            DataType::Utf8 => Arc::new(StringArray::from(vec![
                Some("{\"v\":1}"),
                None,
                Some("{\"v\":1}"),
            ])),
            DataType::Binary => Arc::new(BinaryArray::from(vec![
                Some(b"raw".as_slice()),
                None,
                Some(b"raw".as_slice()),
            ])),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![
                Some(b"raw".as_slice()),
                None,
                Some(b"raw".as_slice()),
            ])),
            DataType::FixedSizeBinary(16) => novarocks_types::largeint::array_from_i128(&[
                Some(i128::MIN),
                None,
                Some(i128::MIN),
            ])
            .unwrap(),
            _ => unreachable!(),
        };
        assert_aggregate_matches_v1(AggregateDiffSpec::new("approx_top_k").typed_column(ty, a));
    }
}

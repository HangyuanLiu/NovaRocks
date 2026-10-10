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

//! Permanent complete original admitted update carriers for the fixed derived aggregate overload.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use super::*;
use arrow::array::*;
use novarocks_type_contract::FunctionValueType;
fn check(ty: FunctionValueType, a: ArrayRef, distinct: bool) {
    let rows = a.len();
    let mut spec = AggregateDiffSpec::new("bitmap_union_int")
        .typed_column(ty, a)
        .grouped((0..rows).map(|i| i % 3).collect(), 4)
        .partitions(3, 551389);
    if distinct {
        spec = spec.original_state_interpretation(true, vec![]);
    }
    assert_aggregate_matches_v1(spec);
}
#[test]
fn pure_differential_bitmap_union_int_complete_original_integer_boolean_domain() {
    for dtype in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        for nullable in [true, false] {
            let ty = FunctionValueType::new(dtype.clone(), nullable);
            let a = InputGenerator::new(79122).column(&ty, 17, &InputProfile::default());
            for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
                for distinct in [false, true] {
                    check(ty.clone(), a.clone(), distinct);
                }
            }
        }
    }
    check(
        FunctionValueType::new(DataType::Int64, true),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(i64::MAX),
            None,
            Some(-1),
        ])),
        false,
    );
    check(
        FunctionValueType::new(DataType::UInt64, true),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(u64::MAX),
            Some(u64::MAX),
            None,
            Some(1),
        ])),
        false,
    );
}
#[test]
fn pure_differential_bitmap_union_int_full_text_binary_decode_fallback_null_empty_shapes() {
    let text = vec![
        Some(" -1 "),
        Some("7"),
        None,
        Some("bad"),
        Some("18446744073709551615"),
        Some("-9223372036854775809"),
        Some("18446744073709551616"),
        Some("é雪"),
        Some("1,7"),
        Some(""),
    ];
    let small = Arc::new(StringArray::from(text.clone())) as ArrayRef;
    let large = Arc::new(LargeStringArray::from(text)) as ArrayRef;
    let payloads = vec![
        Some(vec![0]),
        None,
        Some(b"bad".to_vec()),
        Some(vec![0xff]),
        Some(b"-1".to_vec()),
        Some(b"7,1,7".to_vec()),
        Some(
            novarocks_functions::bitmap_value::encode_internal_bitmap(
                &std::collections::BTreeSet::from([0, 7, u64::MAX]),
            )
            .unwrap(),
        ),
        Some(
            novarocks_functions::bitmap_value::encode_external_bitmap(&(0..96).collect()).unwrap(),
        ),
    ];
    let binary = Arc::new(BinaryArray::from_iter(
        payloads.iter().map(|v| v.as_deref()),
    )) as ArrayRef;
    let large_binary = Arc::new(LargeBinaryArray::from_iter(
        payloads.iter().map(|v| v.as_deref()),
    )) as ArrayRef;
    for a in [small, large, binary, large_binary] {
        let ty = FunctionValueType::new(a.data_type().clone(), true);
        for a in [a.clone(), a.slice(1, 4), a.slice(0, 0)] {
            for distinct in [false, true] {
                check(ty.clone(), a.clone(), distinct);
            }
        }
    }
    for dtype in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
    ] {
        check(
            FunctionValueType::new(dtype.clone(), true),
            new_null_array(&dtype, 5),
            false,
        );
    }
}
#[test]
fn pure_differential_bitmap_union_int_constant_columns_and_empty_final_groups() {
    for a in [
        Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef,
        Arc::new(Int32Array::from(vec![None])),
        Arc::new(StringArray::from(vec!["7"])),
    ] {
        let ty = FunctionValueType::new(a.data_type().clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("constant").unwrap()),
            ty,
            a.to_data(),
            constant_policy(),
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap();
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new("bitmap_union_int")
                .constant(pool.value(0).unwrap())
                .constant_rows(7)
                .grouped(vec![0, 1, 0, 1, 0, 1, 0], 3)
                .partitions(4, 71931),
        );
    }
}
#[test]
fn pure_differential_bitmap_union_int_original_full_nominal_carrier_domains() {
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
    ] {
        for nullable in [false, true] {
            let ty =
                FunctionValueType::try_with_logical_type(dtype.clone(), nullable, logical).unwrap();
            let last = if nullable { None } else { Some("1") };
            let a: ArrayRef = match dtype {
                DataType::Utf8 => Arc::new(StringArray::from(vec![
                    Some("7"),
                    Some("-1"),
                    Some("bad"),
                    last,
                ])),
                DataType::Binary => Arc::new(BinaryArray::from(vec![
                    Some(b"7".as_slice()),
                    Some([0u8].as_slice()),
                    Some(b"-1".as_slice()),
                    last.map(str::as_bytes),
                ])),
                DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![
                    Some(b"7".as_slice()),
                    Some([0u8].as_slice()),
                    Some(b"-1".as_slice()),
                    last.map(str::as_bytes),
                ])),
                _ => unreachable!("closed exact original nominal carrier facts"),
            };
            check(ty.clone(), a.clone(), false);
            check(ty, a.slice(1, 2), true);
        }
    }
}

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

//! Permanent equality across the three original fixed payload aggregate declarations.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::*;
use arrow::array::*;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
fn matrix(name: &str) {
    let mut full = vec![3];
    full.extend(std::iter::repeat_n(1, 16384));
    let payloads = [
        None,
        Some(vec![0]),
        Some(vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0]),
        Some(vec![2, 1, 0, 0, 0, 1, 0, 51]),
        Some(b"opaque".to_vec()),
        Some(full),
        Some(vec![99, 1, 2]),
    ];
    let text: Vec<Option<&str>> = payloads
        .iter()
        .map(|v| v.as_ref().map(|v| std::str::from_utf8(v).unwrap()))
        .collect();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from_iter(
            payloads.iter().map(|v| v.as_deref()),
        )),
        Arc::new(LargeBinaryArray::from_iter(
            payloads.iter().map(|v| v.as_deref()),
        )),
        Arc::new(StringArray::from(text.clone())),
        Arc::new(LargeStringArray::from(text)),
    ];
    for a in arrays {
        let physical = FunctionValueType::new(a.data_type().clone(), true);
        let mut types = vec![physical.clone()];
        for logical in [
            ValueLogicalType::Json,
            ValueLogicalType::Variant,
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
        ] {
            if let Ok(ty) =
                FunctionValueType::try_with_logical_type(a.data_type().clone(), true, logical)
            {
                types.push(ty);
            }
        }
        for ty in types {
            for a in [
                a.clone(),
                a.slice(1, 4),
                a.slice(0, 0),
                new_null_array(a.data_type(), 3),
            ] {
                for distinct in [false, true] {
                    let rows = a.len();
                    let mut spec = AggregateDiffSpec::new(name)
                        .typed_column(ty.clone(), a.clone())
                        .grouped((0..rows).map(|i| i % 3).collect(), 4)
                        .partitions(3, 662801);
                    if distinct {
                        spec = spec.original_state_interpretation(true, vec![])
                    }
                    assert_aggregate_matches_v1(spec);
                }
            }
        }
        let ty = FunctionValueType::new(a.data_type().clone(), false);
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .typed_column(ty, a.slice(1, 3))
                .grouped(vec![0, 1, 0], 3)
                .partitions(3, 77201),
        );
    }
    for bytes in [vec![], vec![1], vec![2, 0]] {
        let a = Arc::new(BinaryArray::from_iter([Some(bytes.as_slice()), None])) as ArrayRef;
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .typed_column(FunctionValueType::new(DataType::Binary, true), a)
                .grouped(vec![0, 1], 2)
                .partitions(2, 6628),
        );
    }
    let a = Arc::new(BinaryArray::from_iter([
        Some(&[0][..]),
        Some(&[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..]),
    ])) as ArrayRef;
    let ty = FunctionValueType::new(DataType::Binary, false);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-pool").unwrap()),
        ty,
        a.to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .constant(pool.value(1).unwrap())
            .constant_rows(7)
            .grouped(vec![0, 1, 0, 1, 0, 1, 0], 3)
            .partitions(3, 191991),
    );
}
#[test]
fn pure_differential_hll_payload_union_full_original_domain() {
    matrix("hll_union")
}
#[test]
fn pure_differential_hll_payload_raw_agg_full_original_domain() {
    matrix("hll_raw_agg")
}
#[test]
fn pure_differential_hll_payload_union_agg_full_original_domain() {
    matrix("hll_union_agg")
}

#[test]
fn pure_differential_hll_payload_empty_unsupported_carrier_original_setup() {
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        for ty in [
            DataType::Null,
            DataType::Int32,
            DataType::Float64,
            DataType::Decimal128(38, 2),
            DataType::FixedSizeBinary(16),
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
        ] {
            for rows in [0usize, 3] {
                assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .typed_column(
                            FunctionValueType::new(ty.clone(), true),
                            new_null_array(&ty, rows),
                        )
                        .grouped(vec![0; rows], 1)
                        .partitions(2, 449922),
                );
            }
        }
    }
}
